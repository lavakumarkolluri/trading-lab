#!/usr/bin/env python3
"""
signal_agent.py — Signal Explanation Agent

Runs once per day after confidence_scorer (scheduler triggers at 13:10 UTC / 18:40 IST).
Reads today's scores from analysis.confidence_scores, parses the features_json,
and sends a plain-English Telegram summary explaining WHY each symbol's
confidence score is what it is.

Read-only: never writes to ClickHouse.
"""

import os
import json
import urllib.request
import zoneinfo
import argparse
from datetime import date

from ch_utils import ch_client
from logging_utils import get_logger

log = get_logger(__name__)

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# ── Telegram ─────────────────────────────────────────────────────────────────

def _send(msg: str) -> None:
    if not (_TG_TOKEN and _TG_CHAT_ID):
        print(msg)
        return
    try:
        payload = json.dumps({"chat_id": _TG_CHAT_ID, "text": msg}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log.warning("Telegram send failed: %s", e)


# ── Feature interpretation ────────────────────────────────────────────────────

def _explain_features(f: dict) -> list[str]:
    """
    Return a list of one-line factor explanations, ordered by importance.
    Each line starts with ✅ (bullish for sellers), ❌ (bearish), or ⚠️ (caution).
    """
    lines = []

    # IV Rank — primary premium richness signal
    iv_r = f.get("iv_rank", 0)
    if iv_r > 70:
        lines.append(f"✅ IV Rank {iv_r:.0f} — options priced rich, high premium income")
    elif iv_r > 50:
        lines.append(f"✅ IV Rank {iv_r:.0f} — above average premium")
    elif iv_r > 0 and iv_r < 25:
        lines.append(f"❌ IV Rank {iv_r:.0f} — cheap options, thin premium edge")
    elif iv_r > 0:
        lines.append(f"⚠️ IV Rank {iv_r:.0f} — below average, moderate edge")

    # India VIX — market calm vs stressed
    vix = f.get("vix", 0)
    if vix > 0:
        if vix < 13:
            lines.append(f"✅ VIX {vix:.1f} — very calm, low realised vol risk")
        elif vix < 17:
            lines.append(f"✅ VIX {vix:.1f} — moderate, within comfortable range")
        elif vix < 22:
            lines.append(f"⚠️ VIX {vix:.1f} — elevated, wider expected moves")
        elif vix < 28:
            lines.append(f"❌ VIX {vix:.1f} — high volatility, dangerous for short gamma")
        else:
            lines.append(f"❌ VIX {vix:.1f} — extreme, avoid short straddle")

    # ATR Percentile — price range regime
    atr_p = f.get("atr_percentile", 50)
    if atr_p < 30:
        lines.append(f"✅ ATR {atr_p:.0f}th pct — narrow daily ranges, range-bound market")
    elif atr_p > 70:
        lines.append(f"❌ ATR {atr_p:.0f}th pct — wide daily swings, directional risk")

    # HV Ratio (HV5/HV20) — volatility acceleration
    hv_r = f.get("hv_ratio", 1.0)
    if hv_r > 1.3:
        lines.append(f"❌ HV5/HV20 {hv_r:.2f}x — vol accelerating (short-term > long-term)")
    elif hv_r < 0.8:
        lines.append(f"✅ HV5/HV20 {hv_r:.2f}x — vol decelerating, mean-reversion favourable")

    # IV/HV5 Ratio — options priced vs realised vol
    ivhv = f.get("iv_hv5_ratio", 0)
    if ivhv > 1.5:
        lines.append(f"✅ IV/HV5 {ivhv:.2f}x — options richly priced vs realised vol")
    elif 0 < ivhv < 0.8:
        lines.append(f"❌ IV/HV5 {ivhv:.2f}x — options cheap vs realised vol, no edge")

    # Put-Call Ratio (chain OI)
    pcr = f.get("pcr_oi", 0)
    if pcr > 1.5:
        lines.append(f"⚠️ PCR {pcr:.2f} — heavy put hedging (crowded bearish positioning)")
    elif 0.7 < pcr < 1.3 and pcr > 0:
        lines.append(f"✅ PCR {pcr:.2f} — balanced options market")

    # FII net positioning
    fii_call = f.get("fii_call_net", 0)
    fii_put  = f.get("fii_put_net", 0)
    if abs(fii_call) > 50000 or abs(fii_put) > 50000:
        bias = "bullish" if fii_put > fii_call else "bearish"
        lines.append(
            f"⚠️ FII opts: call_net={fii_call/1000:.0f}K put_net={fii_put/1000:.0f}K ({bias} bias)"
        )

    # Event calendar
    ev = f.get("event_in_window", 0)
    d2e = f.get("days_to_event", 99)
    if ev:
        lines.append(f"✅ Event in window ({d2e:.0f}d away) — elevated IV expected")

    # Supertrend direction
    st = f.get("supertrend_dir", 0)
    if st == 1:
        lines.append("✅ Supertrend: uptrend — momentum aligned")
    elif st == -1:
        lines.append("⚠️ Supertrend: downtrend — market in sell mode")

    # RSI14 — overbought/oversold
    rsi = f.get("rsi14", 50)
    if rsi > 70:
        lines.append(f"⚠️ RSI {rsi:.0f} — overbought, mean-reversion risk")
    elif rsi < 30:
        lines.append(f"⚠️ RSI {rsi:.0f} — oversold, bounce risk")

    return lines


def _verdict(confidence: float) -> str:
    if confidence >= 75:
        return "STRONG BUY signal — high conviction to enter"
    if confidence >= 60:
        return "BUY signal — favourable conditions"
    if confidence >= 50:
        return "BORDERLINE — marginal edge, trade small or skip"
    return "SKIP — conditions unfavourable for short premium"


# ── ClickHouse query ──────────────────────────────────────────────────────────

def fetch_today_scores(ch, score_date: date) -> list[dict]:
    rows = ch.query(f"""
        SELECT symbol, next_expiry, confidence, expected_pnl_pct, features_json
        FROM analysis.confidence_scores FINAL
        WHERE score_date = '{score_date}'
        ORDER BY confidence DESC
    """).result_rows
    results = []
    for symbol, next_expiry, confidence, expected_pnl_pct, features_json in rows:
        try:
            features = json.loads(features_json)
        except Exception:
            features = {}
        results.append({
            "symbol":          symbol,
            "next_expiry":     str(next_expiry)[:10],
            "confidence":      float(confidence),
            "expected_pnl_pct": float(expected_pnl_pct),
            "features":        features,
        })
    return results


# ── Report formatting ─────────────────────────────────────────────────────────

def format_report(scores: list[dict], score_date: date) -> str:
    if not scores:
        return f"📊 Signal Report {score_date}\nNo scores available — scorer may not have run yet."

    lines = [f"📊 Signal Report — {score_date}"]
    for s in sorted(scores, key=lambda x: x["confidence"], reverse=True):
        sym  = s["symbol"]
        conf = s["confidence"]
        exp  = s["expected_pnl_pct"]
        expiry = s["next_expiry"]
        factors = _explain_features(s["features"])

        bar_filled = int(conf / 10)
        bar = "█" * bar_filled + "░" * (10 - bar_filled)

        lines.append("")
        lines.append(f"━━━ {sym} (expiry {expiry}) ━━━")
        lines.append(f"Confidence: {conf:.0f}/100  [{bar}]")
        lines.append(f"Expected P&L: {exp:+.1f}%  →  {_verdict(conf)}")
        if factors:
            lines.append("Factors:")
            lines.extend(f"  {fl}" for fl in factors)
        else:
            lines.append("  (no feature data available)")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Signal explanation agent")
    parser.add_argument("--date", default=str(date.today()),
                        help="Score date to explain (YYYY-MM-DD, default: today)")
    args = parser.parse_args()

    score_date = date.fromisoformat(args.date)
    log.info("Signal Agent: fetching scores for %s", score_date)

    ch = ch_client()
    scores = fetch_today_scores(ch, score_date)

    if not scores:
        log.warning("No scores found for %s — confidence_scorer may not have run", score_date)
    else:
        log.info("Found %d symbol scores", len(scores))

    report = format_report(scores, score_date)
    log.info("Sending signal report via Telegram")
    _send(report)
    log.info("Signal Agent complete")


if __name__ == "__main__":
    main()
