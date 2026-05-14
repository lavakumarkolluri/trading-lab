#!/usr/bin/env python3
"""
monitor_agent.py — Trading Monitor Agent

Polls open positions every 5 minutes during market hours (Mon–Fri 09:00–16:00 IST)
and sends Telegram alerts for key events:
  • New position entered
  • Target hit  (unrealized P&L ≥ target_inr)
  • Stop hit    (unrealized P&L ≤ -stoploss_inr)
  • Trailing stop activated
  • EOD warning at 15:15 IST (positions still open)
  • Position closed (trade resolved)
  • Heartbeat every 30 min with open position summary

Read-only: never writes to ClickHouse.
"""

import os
import time
import json
import urllib.request
import zoneinfo
from datetime import datetime, time as dtime, timedelta

from ch_utils import ch_client
from logging_utils import get_logger

log = get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

LOOP_INTERVAL_S   = 5 * 60         # poll every 5 min
HEARTBEAT_TICKS   = 6              # heartbeat every 30 min (6 × 5 min)
MONITOR_START     = dtime(9, 0)
MONITOR_END       = dtime(16, 0)
EOD_WARN_TIME     = dtime(15, 15)  # warn about open positions


# ── Telegram ─────────────────────────────────────────────────────────────────

def _send(msg: str) -> None:
    if not (_TG_TOKEN and _TG_CHAT_ID):
        log.info("Telegram not configured — would send: %s", msg[:80])
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


# ── ClickHouse queries ────────────────────────────────────────────────────────

def _fetch_positions(ch) -> list[dict]:
    """Return all rows from trades.open_positions FINAL."""
    rows = ch.query("""
        SELECT trade_id, symbol, strike, expiry, entry_time,
               entry_ce_ltp, entry_pe_ltp, entry_premium,
               lot_size, lots, target_inr, stoploss_inr,
               trailing_active, peak_pnl_inr, trail_stop_inr,
               scorecard_conf, status,
               wing_ce_strike, wing_pe_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.open_positions FINAL
        WHERE status = 'open'
    """).result_rows
    cols = [
        "trade_id", "symbol", "strike", "expiry", "entry_time",
        "entry_ce_ltp", "entry_pe_ltp", "entry_premium",
        "lot_size", "lots", "target_inr", "stoploss_inr",
        "trailing_active", "peak_pnl_inr", "trail_stop_inr",
        "scorecard_conf", "status",
        "wing_ce_strike", "wing_pe_strike", "wing_ce_ltp", "wing_pe_ltp", "net_premium",
    ]
    return [dict(zip(cols, r)) for r in rows]


def _fetch_mark(ch, sym: str, strike: float, expiry: str) -> float | None:
    """Return latest straddle mark (CE + PE) for ATM strike."""
    rows = ch.query(f"""
        SELECT sumIf(ltp, option_type='CE') + sumIf(ltp, option_type='PE') AS curr
        FROM market.options_chain
        WHERE symbol='{sym}' AND strike={strike} AND expiry='{expiry}'
          AND toDate(timestamp) = today()
          AND timestamp = (
              SELECT max(timestamp) FROM market.options_chain
              WHERE symbol='{sym}' AND toDate(timestamp) = today()
          )
        GROUP BY symbol HAVING curr > 0
    """).result_rows
    return float(rows[0][0]) if rows else None


def _fetch_wing_marks(ch, sym: str, expiry: str,
                      wce: float, wpe: float) -> tuple[float, float] | None:
    """Return (wing_ce_ltp, wing_pe_ltp) at latest timestamp."""
    rows = ch.query(f"""
        SELECT
            sumIf(ltp, option_type='CE' AND strike={wce}) AS wce_ltp,
            sumIf(ltp, option_type='PE' AND strike={wpe}) AS wpe_ltp
        FROM market.options_chain
        WHERE symbol='{sym}' AND expiry='{expiry}'
          AND toDate(timestamp) = today()
          AND timestamp = (
              SELECT max(timestamp) FROM market.options_chain
              WHERE symbol='{sym}' AND toDate(timestamp) = today()
          )
    """).result_rows
    if not rows:
        return None
    wce_ltp, wpe_ltp = float(rows[0][0]), float(rows[0][1])
    return (wce_ltp, wpe_ltp)


def _compute_pnl(pos: dict, ch) -> float | None:
    """Compute unrealized P&L in INR for one position. Returns None if no mark."""
    sym    = pos["symbol"]
    strike = float(pos["strike"])
    expiry = str(pos["expiry"])[:10]
    wce    = float(pos.get("wing_ce_strike") or 0)
    wpe    = float(pos.get("wing_pe_strike") or 0)
    is_fly = wce > 0

    straddle_mark = _fetch_mark(ch, sym, strike, expiry)
    if straddle_mark is None:
        return None

    curr_net = straddle_mark
    if is_fly:
        wings = _fetch_wing_marks(ch, sym, expiry, wce, wpe)
        if wings:
            curr_net = straddle_mark - wings[0] - wings[1]

    net_prem = float(pos.get("net_premium") or 0)
    entry_value = net_prem if (is_fly and net_prem > 0) else float(pos["entry_premium"])
    lot_size = int(pos["lot_size"])
    lots     = int(pos.get("lots") or 1)

    return (entry_value - curr_net) * lot_size * lots


# ── Alert formatting ──────────────────────────────────────────────────────────

def _fmt_pos(pos: dict, pnl_inr: float | None) -> str:
    sym    = pos["symbol"]
    strike = int(float(pos["strike"]))
    expiry = str(pos["expiry"])[:10]
    lots   = int(pos.get("lots") or 1)
    conf   = float(pos.get("scorecard_conf") or 0)
    wce    = float(pos.get("wing_ce_strike") or 0)
    fly_tag = f" | Iron Fly {int(wce)}↑" if wce > 0 else ""
    pnl_tag = f" | P&L: ₹{pnl_inr:+.0f}" if pnl_inr is not None else ""
    return f"{sym} {strike} {expiry} ×{lots}{fly_tag} | Conf: {conf:.0f}{pnl_tag}"


# ── Main loop ─────────────────────────────────────────────────────────────────

def _ist_now() -> datetime:
    return datetime.now(IST)


def _is_market_hours() -> bool:
    now = _ist_now()
    if now.weekday() >= 5:  # Sat/Sun
        return False
    t = now.time()
    return MONITOR_START <= t <= MONITOR_END


def run():
    log.info("Monitor Agent started — polling every %d min during market hours", LOOP_INTERVAL_S // 60)

    ch = ch_client()

    # State: trade_id → set of alerted event names
    alerted: dict[str, set[str]] = {}
    # Last known open trade_ids (to detect closures)
    last_open: set[str] = set()
    tick = 0

    while True:
        if not _is_market_hours():
            log.info("Outside market hours — sleeping 5 min")
            time.sleep(LOOP_INTERVAL_S)
            continue

        tick += 1
        now = _ist_now()

        try:
            positions = _fetch_positions(ch)
        except Exception as e:
            log.error("CH query failed: %s", e)
            time.sleep(LOOP_INTERVAL_S)
            continue

        current_ids = {str(p["trade_id"]) for p in positions}

        # ── Detect closed positions ───────────────────────────────────────────
        for tid in last_open - current_ids:
            log.info("Position %s closed", tid)
            _send(f"🔴 Position closed\nTrade ID: {tid[:8]}")
            alerted.pop(tid, None)

        last_open = current_ids

        # ── Check each open position ──────────────────────────────────────────
        heartbeat_lines = []
        for pos in positions:
            tid = str(pos["trade_id"])
            if tid not in alerted:
                alerted[tid] = set()
            seen = alerted[tid]

            try:
                pnl_inr = _compute_pnl(pos, ch)
            except Exception as e:
                log.warning("P&L compute error for %s: %s", tid[:8], e)
                pnl_inr = None

            summary = _fmt_pos(pos, pnl_inr)

            # New position alert
            if "entered" not in seen:
                seen.add("entered")
                _send(f"📥 Position entered\n{summary}")
                log.info("ALERT: entered %s", tid[:8])

            # Target hit
            target = float(pos["target_inr"])
            if pnl_inr is not None and pnl_inr >= target and "target" not in seen:
                seen.add("target")
                _send(f"🎯 Target hit!\n{summary}\nTarget: ₹{target:.0f}")
                log.info("ALERT: target %s P&L=₹%.0f", tid[:8], pnl_inr)

            # Stop hit
            stop = float(pos["stoploss_inr"])
            if pnl_inr is not None and pnl_inr <= -stop and "stop" not in seen:
                seen.add("stop")
                _send(f"🛑 Stop loss hit!\n{summary}\nStop: ₹{stop:.0f}")
                log.info("ALERT: stop %s P&L=₹%.0f", tid[:8], pnl_inr)

            # Trailing activated
            if pos["trailing_active"] and "trailing" not in seen:
                seen.add("trailing")
                peak = float(pos.get("peak_pnl_inr") or 0)
                trail_stop = float(pos.get("trail_stop_inr") or 0)
                _send(
                    f"🔒 Trailing stop activated\n{summary}\n"
                    f"Peak: ₹{peak:.0f} | Trail stop: ₹{trail_stop:.0f}"
                )
                log.info("ALERT: trailing %s peak=₹%.0f", tid[:8], peak)

            # EOD warning — 15:15 IST
            if now.time() >= EOD_WARN_TIME and "eod" not in seen:
                seen.add("eod")
                _send(f"⏰ EOD approaching — position still open\n{summary}")
                log.info("ALERT: EOD warning %s", tid[:8])

            heartbeat_lines.append(f"  • {summary}")

        # ── Heartbeat every 30 min ────────────────────────────────────────────
        if tick % HEARTBEAT_TICKS == 0 and positions:
            body = "\n".join(heartbeat_lines)
            _send(f"📊 Position update ({now.strftime('%H:%M IST')})\n{body}")
            log.info("Heartbeat: %d open position(s)", len(positions))

        if not positions:
            log.info("No open positions — watching")

        time.sleep(LOOP_INTERVAL_S)


if __name__ == "__main__":
    run()
