#!/usr/bin/env python3
"""
analysis_agent.py — Post-Trade Analysis Agent

Runs daily Mon–Fri at 13:15 UTC (18:45 IST), after market close and EOD pipeline.

Daily mode: compact Telegram summary of the last 30 days' trades.
  • Win rate, avg P&L, streak
  • Today's trades (if any)
  • Breakdown by symbol and exit reason

Weekly mode (--weekly, triggered Sundays): same stats + writes a full
  Markdown report to agents/analysis/YYYY-MM-DD.md covering the past 30 days.

Usage:
    python analysis_agent.py              # daily Telegram summary
    python analysis_agent.py --weekly     # full report + Telegram + write Markdown
    python analysis_agent.py --days 7     # use last 7 days instead of 30
"""

import os
import json
import argparse
import urllib.request
import zoneinfo
from datetime import date, datetime, timedelta
from pathlib import Path

from ch_utils import ch_client
from logging_utils import get_logger

log = get_logger(__name__)

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

REPORT_DIR = Path(os.getenv("REPORT_DIR", "/trading-lab/agents/analysis"))


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


# ── ClickHouse query ──────────────────────────────────────────────────────────

def fetch_trades(ch, since: date) -> list[dict]:
    rows = ch.query(f"""
        SELECT trade_id, symbol, expiry, entry_time, exit_time,
               strike, entry_premium, exit_premium, pnl_pts, pnl_inr,
               lot_size, lots, exit_reason, scorecard_conf,
               wing_ce_strike, net_premium
        FROM trades.trade_outcomes FINAL
        WHERE toDate(exit_time) >= '{since}'
        ORDER BY exit_time
    """).result_rows
    cols = [
        "trade_id", "symbol", "expiry", "entry_time", "exit_time",
        "strike", "entry_premium", "exit_premium", "pnl_pts", "pnl_inr",
        "lot_size", "lots", "exit_reason", "scorecard_conf",
        "wing_ce_strike", "net_premium",
    ]
    return [dict(zip(cols, r)) for r in rows]


# ── Statistics ────────────────────────────────────────────────────────────────

def _win(t: dict) -> bool:
    return float(t["pnl_inr"]) > 0


def compute_stats(trades: list[dict]) -> dict:
    if not trades:
        return {"count": 0}

    pnls = [float(t["pnl_inr"]) for t in trades]
    wins = [t for t in trades if _win(t)]
    losses = [t for t in trades if not _win(t)]

    # Current streak
    streak = 0
    streak_type = ""
    for t in reversed(trades):
        is_win = _win(t)
        if streak == 0:
            streak_type = "W" if is_win else "L"
            streak = 1
        elif (streak_type == "W") == is_win:
            streak += 1
        else:
            break

    # By symbol
    symbols = sorted({t["symbol"] for t in trades})
    by_symbol = {}
    for sym in symbols:
        sym_trades = [t for t in trades if t["symbol"] == sym]
        sym_wins = [t for t in sym_trades if _win(t)]
        by_symbol[sym] = {
            "count": len(sym_trades),
            "wins":  len(sym_wins),
            "avg_pnl": sum(float(t["pnl_inr"]) for t in sym_trades) / len(sym_trades),
        }

    # By exit reason
    reasons = sorted({t["exit_reason"] for t in trades})
    by_reason = {}
    for r in reasons:
        r_trades = [t for t in trades if t["exit_reason"] == r]
        r_wins = [t for t in r_trades if _win(t)]
        by_reason[r] = {"count": len(r_trades), "wins": len(r_wins)}

    # Confidence buckets
    hi_conf  = [t for t in trades if float(t["scorecard_conf"]) >= 70]
    lo_conf  = [t for t in trades if float(t["scorecard_conf"]) < 50]
    hi_wins  = [t for t in hi_conf if _win(t)]
    lo_wins  = [t for t in lo_conf if _win(t)]

    best  = max(trades, key=lambda t: float(t["pnl_inr"]))
    worst = min(trades, key=lambda t: float(t["pnl_inr"]))

    return {
        "count":       len(trades),
        "win_count":   len(wins),
        "loss_count":  len(losses),
        "win_rate":    len(wins) / len(trades) * 100,
        "total_pnl":   sum(pnls),
        "avg_pnl":     sum(pnls) / len(pnls),
        "best_pnl":    float(best["pnl_inr"]),
        "worst_pnl":   float(worst["pnl_inr"]),
        "best_trade":  best,
        "worst_trade": worst,
        "streak":      streak,
        "streak_type": streak_type,
        "by_symbol":   by_symbol,
        "by_reason":   by_reason,
        "hi_conf":     {"count": len(hi_conf), "wins": len(hi_wins)},
        "lo_conf":     {"count": len(lo_conf), "wins": len(lo_wins)},
    }


# ── Today's trades ────────────────────────────────────────────────────────────

def _today_trades(trades: list[dict]) -> list[dict]:
    today = date.today()
    return [t for t in trades if _exit_date(t) == today]


def _exit_date(t: dict) -> date:
    et = t["exit_time"]
    if isinstance(et, datetime):
        return et.date()
    if isinstance(et, date):
        return et
    return date.fromisoformat(str(et)[:10])


# ── Telegram message ──────────────────────────────────────────────────────────

def _fmt_inr(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}₹{v:.0f}"


def _wr(wins: int, total: int) -> str:
    if total == 0:
        return "—"
    return f"{wins}/{total} ({wins/total*100:.0f}%)"


def build_telegram_msg(stats: dict, today: list[dict], since: date, as_of: date) -> str:
    if stats["count"] == 0:
        return f"📉 Analysis ({since} → {as_of})\nNo trades in this period."

    lines = [
        f"📉 Trade Analysis ({since} → {as_of}  ·  {stats['count']} trades)",
        "",
    ]

    # Today's summary first
    if today:
        today_pnl = sum(float(t["pnl_inr"]) for t in today)
        today_wins = sum(1 for t in today if _win(t))
        lines.append(f"Today: {_wr(today_wins, len(today))}  total {_fmt_inr(today_pnl)}")

    # Overall
    streak_str = f"{stats['streak']}×{stats['streak_type']}"
    lines.append(
        f"Win rate : {_wr(stats['win_count'], stats['count'])}"
    )
    lines.append(f"Avg P&L  : {_fmt_inr(stats['avg_pnl'])}  total {_fmt_inr(stats['total_pnl'])}")
    lines.append(f"Streak   : {streak_str}")
    lines.append(f"Best     : {_fmt_inr(stats['best_pnl'])}")
    lines.append(f"Worst    : {_fmt_inr(stats['worst_pnl'])}")

    # By symbol
    if stats["by_symbol"]:
        lines.append("")
        lines.append("By symbol:")
        for sym, s in stats["by_symbol"].items():
            lines.append(
                f"  {sym:12s} {_wr(s['wins'], s['count'])}  avg {_fmt_inr(s['avg_pnl'])}"
            )

    # By exit reason
    if stats["by_reason"]:
        lines.append("")
        lines.append("By exit:")
        for reason, s in stats["by_reason"].items():
            lines.append(f"  {reason:8s} {_wr(s['wins'], s['count'])}")

    # Confidence buckets
    hi = stats["hi_conf"]
    lo = stats["lo_conf"]
    if hi["count"] or lo["count"]:
        lines.append("")
        lines.append("Confidence edge:")
        if hi["count"]:
            lines.append(f"  Conf ≥70  {_wr(hi['wins'], hi['count'])}")
        if lo["count"]:
            lines.append(f"  Conf <50  {_wr(lo['wins'], lo['count'])}")

    return "\n".join(lines)


# ── Weekly Markdown report ────────────────────────────────────────────────────

def build_markdown_report(trades: list[dict], stats: dict, since: date, as_of: date) -> str:
    lines = [
        f"# Weekly Trade Analysis — {as_of}",
        f"Period: {since} → {as_of}  ({stats['count']} trades)",
        "",
        "## Summary",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Win rate | {_wr(stats['win_count'], stats['count'])} |",
        f"| Total P&L | {_fmt_inr(stats['total_pnl'])} |",
        f"| Avg P&L / trade | {_fmt_inr(stats['avg_pnl'])} |",
        f"| Best trade | {_fmt_inr(stats['best_pnl'])} |",
        f"| Worst trade | {_fmt_inr(stats['worst_pnl'])} |",
        f"| Current streak | {stats['streak']}×{stats['streak_type']} |",
        "",
        "## By Symbol",
        "| Symbol | Trades | Win Rate | Avg P&L |",
        "|--------|--------|----------|---------|",
    ]
    for sym, s in stats["by_symbol"].items():
        lines.append(
            f"| {sym} | {s['count']} | {_wr(s['wins'], s['count'])} | {_fmt_inr(s['avg_pnl'])} |"
        )

    lines += [
        "",
        "## By Exit Reason",
        "| Exit Reason | Trades | Win Rate |",
        "|-------------|--------|----------|",
    ]
    for reason, s in stats["by_reason"].items():
        lines.append(f"| {reason} | {s['count']} | {_wr(s['wins'], s['count'])} |")

    hi = stats["hi_conf"]
    lo = stats["lo_conf"]
    lines += [
        "",
        "## Confidence Buckets",
        "| Bucket | Trades | Win Rate |",
        "|--------|--------|----------|",
        f"| Conf ≥70 | {hi['count']} | {_wr(hi['wins'], hi['count'])} |",
        f"| Conf <50 | {lo['count']} | {_wr(lo['wins'], lo['count'])} |",
    ]

    lines += ["", "## All Trades", "| Date | Symbol | Exit | P&L | Conf |", "|------|--------|------|-----|------|"]
    for t in sorted(trades, key=_exit_date, reverse=True):
        exit_d = _exit_date(t)
        lines.append(
            f"| {exit_d} | {t['symbol']} | {t['exit_reason']} "
            f"| {_fmt_inr(float(t['pnl_inr']))} | {float(t['scorecard_conf']):.0f} |"
        )

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Post-trade analysis agent")
    parser.add_argument("--weekly", action="store_true",
                        help="Full weekly report: stats + write Markdown file")
    parser.add_argument("--days", type=int, default=30,
                        help="Lookback window in calendar days (default: 30)")
    args = parser.parse_args()

    as_of = date.today()
    since = as_of - timedelta(days=args.days)

    log.info("Analysis Agent: fetching trades from %s to %s", since, as_of)
    ch = ch_client()
    trades = fetch_trades(ch, since)
    log.info("Found %d trades", len(trades))

    stats = compute_stats(trades)
    today = _today_trades(trades)

    # Always send Telegram summary
    msg = build_telegram_msg(stats, today, since, as_of)
    _send(msg)
    log.info("Telegram summary sent")

    # Weekly: also write Markdown report
    if args.weekly:
        md = build_markdown_report(trades, stats, since, as_of)
        try:
            REPORT_DIR.mkdir(parents=True, exist_ok=True)
            report_path = REPORT_DIR / f"{as_of}.md"
            report_path.write_text(md)
            log.info("Weekly report written to %s", report_path)
        except Exception as e:
            log.error("Failed to write report: %s", e)

    log.info("Analysis Agent complete — %d trades, win rate %.0f%%",
             stats.get("count", 0), stats.get("win_rate", 0))


if __name__ == "__main__":
    main()
