#!/usr/bin/env python3
"""
intraday_monitor.py — Paper trading intraday straddle monitor

Runs Mon–Fri 09:20–15:25 IST. Every 15 minutes:
  • If no open position: evaluate entry conditions and enter if favorable
  • If open position: check stop loss, trailing stop, end-of-day exit

Strategy: sell ATM straddle (CE + PE) on nearest weekly expiry.

P&L targets (per lot, 1 lot):
  Target  : ₹2000 → activates trailing stop at 75% of peak profit
  Stop    : ₹1000 loss → hard exit
  Trailing: once target hit, trail at 75% of peak; exit if P&L falls back to trail
  EOD     : exit any open position at 15:20 IST

Lot sizes  : NIFTY=75, BANKNIFTY=35 (queried from market.fo_lot_sizes)
Symbols    : NIFTY, BANKNIFTY
Entry window: 09:30–14:00 IST (not before open settles, not too late)
Re-entry   : allowed after target/trail exit; 30-min cooling after stop loss

Usage:
    python intraday_monitor.py          # live paper trading loop
    python intraday_monitor.py --dry-run  # log signals, no DB writes
"""

import json
import logging
import os
import time
import uuid
import argparse
import zoneinfo
from datetime import datetime, time as dtime, date, timedelta

import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

SYMBOLS = ["NIFTY", "BANKNIFTY"]

DEFAULT_LOT_SIZES = {"NIFTY": 75, "BANKNIFTY": 35}

TARGET_INR   = 2000.0
STOPLOSS_INR = 1000.0
TRAIL_PCT    = 0.75        # trail stop = 75% of peak profit

LOOP_INTERVAL_S  = 15 * 60   # 15 minutes
ENTRY_START      = dtime(9, 30)
ENTRY_CUTOFF     = dtime(14, 0)
EOD_EXIT         = dtime(15, 20)
MONITOR_EXIT     = dtime(15, 25)
STOP_COOLDOWN_M  = 30         # minutes to wait before re-entry after stop hit

MIN_PREMIUM_NIFTY     = 40.0   # don't enter if straddle too cheap
MIN_PREMIUM_BANKNIFTY = 80.0

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")


# ── Connections ───────────────────────────────────────────────────────────────

def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


# ── Time helpers ──────────────────────────────────────────────────────────────

def ist_now() -> datetime:
    return datetime.now(IST)

def ist_time() -> dtime:
    return ist_now().time().replace(tzinfo=None)

def ist_naive() -> datetime:
    return ist_now().replace(tzinfo=None)


# ── Lot sizes ─────────────────────────────────────────────────────────────────

def load_lot_sizes(ch) -> dict:
    try:
        r = ch.query("""
            SELECT symbol, lot_size FROM market.fo_lot_sizes FINAL
            WHERE symbol IN ('NIFTY', 'BANKNIFTY')
        """)
        sizes = {row[0]: int(row[1]) for row in r.result_rows}
        return {**DEFAULT_LOT_SIZES, **sizes}
    except Exception as e:
        log.warning(f"Could not load lot sizes from DB: {e} — using defaults")
        return DEFAULT_LOT_SIZES


# ── Market data ───────────────────────────────────────────────────────────────

def get_latest_snapshot(ch, symbol: str) -> dict | None:
    """
    Get the most recent intraday option chain snapshot for symbol.
    Returns dict with spot, expiry, strike, ce_ltp, pe_ltp, straddle, iv, timestamp.
    """
    try:
        r = ch.query("""
            SELECT
                toDate(timestamp)     AS snap_date,
                max(timestamp)        AS latest_ts,
                expiry,
                strike,
                sumIf(ltp, option_type = 'CE') AS ce_ltp,
                sumIf(ltp, option_type = 'PE') AS pe_ltp,
                avgIf(iv,  option_type = 'CE') AS ce_iv,
                avgIf(iv,  option_type = 'PE') AS pe_iv
            FROM market.options_chain
            WHERE symbol = {sym:String}
              AND toDate(timestamp) = today()
              AND timestamp = (
                  SELECT max(timestamp)
                  FROM market.options_chain
                  WHERE symbol = {sym:String} AND toDate(timestamp) = today()
              )
            GROUP BY snap_date, expiry, strike
            HAVING ce_ltp > 0.5 AND pe_ltp > 0.5
            ORDER BY expiry ASC, abs(ce_ltp - pe_ltp) ASC
            LIMIT 1
        """, parameters={"sym": symbol})

        if not r.result_rows:
            return None

        row = r.result_rows[0]
        snap_date, ts, expiry, strike, ce_ltp, pe_ltp, ce_iv, pe_iv = row
        straddle = ce_ltp + pe_ltp
        return {
            "timestamp": ts,
            "expiry":    expiry,
            "strike":    float(strike),
            "ce_ltp":    float(ce_ltp),
            "pe_ltp":    float(pe_ltp),
            "straddle":  float(straddle),
            "ce_iv":     float(ce_iv),
            "pe_iv":     float(pe_iv),
            "atm_iv":    (float(ce_iv) + float(pe_iv)) / 2,
        }
    except Exception as e:
        log.warning(f"[{symbol}] snapshot query failed: {e}")
        return None


def get_current_straddle(ch, symbol: str, expiry: date, strike: float) -> float | None:
    """Get current straddle LTP for an existing position's strike."""
    try:
        r = ch.query("""
            SELECT
                sumIf(ltp, option_type = 'CE') AS ce_ltp,
                sumIf(ltp, option_type = 'PE') AS pe_ltp
            FROM market.options_chain
            WHERE symbol   = {sym:String}
              AND expiry    = {exp:Date}
              AND strike    = {strike:Float32}
              AND toDate(timestamp) = today()
              AND timestamp = (
                  SELECT max(timestamp)
                  FROM market.options_chain
                  WHERE symbol = {sym:String} AND toDate(timestamp) = today()
              )
            GROUP BY symbol
            HAVING ce_ltp > 0 AND pe_ltp > 0
        """, parameters={"sym": symbol, "exp": expiry, "strike": strike})

        if not r.result_rows:
            return None
        ce, pe = r.result_rows[0]
        return float(ce) + float(pe)
    except Exception as e:
        log.warning(f"[{symbol}] straddle price query failed: {e}")
        return None


def get_scorecard_confidence(ch, symbol: str) -> float:
    """Get latest confidence score for symbol, must be within last 7 days."""
    try:
        r = ch.query("""
            SELECT confidence FROM analysis.confidence_scores FINAL
            WHERE symbol = {sym:String}
              AND score_date >= today() - 7
            ORDER BY score_date DESC
            LIMIT 1
        """, parameters={"sym": symbol})
        return float(r.result_rows[0][0]) if r.result_rows else 50.0
    except Exception:
        return 50.0


# ── Position management ───────────────────────────────────────────────────────

def get_open_position(ch, symbol: str) -> dict | None:
    """Return open position for symbol, or None."""
    try:
        r = ch.query("""
            SELECT trade_id, symbol, expiry, entry_time, strike,
                   entry_ce_ltp, entry_pe_ltp, entry_premium,
                   lot_size, target_pts, stop_pts, target_inr, stoploss_inr,
                   scorecard_conf, trailing_active, peak_pnl_inr, trail_stop_inr,
                   entry_features
            FROM trades.open_positions FINAL
            WHERE symbol = {sym:String} AND status = 'open'
            ORDER BY entry_time DESC
            LIMIT 1
        """, parameters={"sym": symbol})

        if not r.result_rows:
            return None
        cols = ["trade_id","symbol","expiry","entry_time","strike",
                "entry_ce_ltp","entry_pe_ltp","entry_premium",
                "lot_size","target_pts","stop_pts","target_inr","stoploss_inr",
                "scorecard_conf","trailing_active","peak_pnl_inr","trail_stop_inr",
                "entry_features"]
        return dict(zip(cols, r.result_rows[0]))
    except Exception as e:
        log.warning(f"[{symbol}] get_open_position failed: {e}")
        return None


def last_stop_time(ch, symbol: str) -> datetime | None:
    """Return the exit_time of the last stop-loss trade for cooldown check."""
    try:
        r = ch.query("""
            SELECT max(exit_time) FROM trades.trade_outcomes FINAL
            WHERE symbol = {sym:String}
              AND exit_reason = 'stop'
              AND toDate(exit_time) = today()
        """, parameters={"sym": symbol})
        val = r.result_rows[0][0] if r.result_rows else None
        return val
    except Exception:
        return None


def record_entry(ch, symbol, snap, lot_size, scorecard_conf, dry_run=False) -> str:
    trade_id = str(uuid.uuid4())
    target_pts  = TARGET_INR / lot_size
    stop_pts    = STOPLOSS_INR / lot_size
    entry_time  = ist_naive()

    features = json.dumps({
        "strike":        snap["strike"],
        "ce_ltp":        snap["ce_ltp"],
        "pe_ltp":        snap["pe_ltp"],
        "straddle":      snap["straddle"],
        "ce_iv":         snap["ce_iv"],
        "pe_iv":         snap["pe_iv"],
        "expiry":        str(snap["expiry"]),
        "scorecard_conf": scorecard_conf,
    })

    log.info(f"[{symbol}] ENTER trade_id={trade_id[:8]} strike={snap['strike']:.0f} "
             f"premium={snap['straddle']:.1f} target={target_pts:.1f}pts "
             f"stop={stop_pts:.1f}pts conf={scorecard_conf:.0f}")

    if not dry_run:
        ch.insert(
            "trades.open_positions",
            [[trade_id, symbol, snap["expiry"], entry_time,
              snap["strike"], snap["ce_ltp"], snap["pe_ltp"], snap["straddle"],
              lot_size, target_pts, stop_pts, TARGET_INR, STOPLOSS_INR,
              scorecard_conf, "open",
              0, 0.0, 0.0, entry_time, features]],
            column_names=["trade_id","symbol","expiry","entry_time","strike",
                          "entry_ce_ltp","entry_pe_ltp","entry_premium",
                          "lot_size","target_pts","stop_pts","target_inr","stoploss_inr",
                          "scorecard_conf","status",
                          "trailing_active","peak_pnl_inr","trail_stop_inr","last_checked",
                          "entry_features"],
        )
    return trade_id


def record_exit(ch, pos, current_straddle, exit_reason, dry_run=False):
    pnl_pts = pos["entry_premium"] - current_straddle
    pnl_inr = pnl_pts * pos["lot_size"]
    exit_time = ist_naive()

    log.info(f"[{pos['symbol']}] EXIT {exit_reason.upper()} trade_id={str(pos['trade_id'])[:8]} "
             f"entry={pos['entry_premium']:.1f} exit={current_straddle:.1f} "
             f"pnl={pnl_pts:.1f}pts ₹{pnl_inr:.0f}")

    if not dry_run:
        ch.insert(
            "trades.trade_outcomes",
            [[pos["trade_id"], pos["symbol"], pos["expiry"],
              pos["entry_time"], exit_time,
              pos["strike"], pos["entry_premium"], current_straddle,
              pnl_pts, pnl_inr, pos["lot_size"], exit_reason,
              pos["scorecard_conf"], pos.get("entry_features", "{}")]],
            column_names=["trade_id","symbol","expiry","entry_time","exit_time",
                          "strike","entry_premium","exit_premium",
                          "pnl_pts","pnl_inr","lot_size","exit_reason",
                          "scorecard_conf","entry_features"],
        )
        # Close position by inserting updated row with status=closed
        ch.insert(
            "trades.open_positions",
            [[pos["trade_id"], pos["symbol"], pos["expiry"], pos["entry_time"],
              pos["strike"], pos["entry_ce_ltp"], pos["entry_pe_ltp"], pos["entry_premium"],
              pos["lot_size"], pos["target_pts"], pos["stop_pts"],
              pos["target_inr"], pos["stoploss_inr"],
              pos["scorecard_conf"], "closed",
              int(pos["trailing_active"]), float(pos["peak_pnl_inr"]),
              float(pos["trail_stop_inr"]), exit_time, pos.get("entry_features", "{}")]],
            column_names=["trade_id","symbol","expiry","entry_time","strike",
                          "entry_ce_ltp","entry_pe_ltp","entry_premium",
                          "lot_size","target_pts","stop_pts","target_inr","stoploss_inr",
                          "scorecard_conf","status",
                          "trailing_active","peak_pnl_inr","trail_stop_inr","last_checked",
                          "entry_features"],
        )


def update_trail(ch, pos, pnl_inr, dry_run=False):
    """Update peak_pnl_inr and trail_stop_inr for a position in trailing mode."""
    peak   = max(float(pos["peak_pnl_inr"]), pnl_inr, TARGET_INR)
    trail  = peak * TRAIL_PCT
    now    = ist_naive()

    log.info(f"[{pos['symbol']}] TRAIL peak=₹{peak:.0f} floor=₹{trail:.0f} "
             f"current=₹{pnl_inr:.0f}")

    if not dry_run:
        ch.insert(
            "trades.open_positions",
            [[pos["trade_id"], pos["symbol"], pos["expiry"], pos["entry_time"],
              pos["strike"], pos["entry_ce_ltp"], pos["entry_pe_ltp"], pos["entry_premium"],
              pos["lot_size"], pos["target_pts"], pos["stop_pts"],
              pos["target_inr"], pos["stoploss_inr"],
              pos["scorecard_conf"], "open",
              1, peak, trail, now, pos.get("entry_features", "{}")]],
            column_names=["trade_id","symbol","expiry","entry_time","strike",
                          "entry_ce_ltp","entry_pe_ltp","entry_premium",
                          "lot_size","target_pts","stop_pts","target_inr","stoploss_inr",
                          "scorecard_conf","status",
                          "trailing_active","peak_pnl_inr","trail_stop_inr","last_checked",
                          "entry_features"],
        )
    return peak, trail


# ── Per-symbol tick ───────────────────────────────────────────────────────────

def tick(ch, symbol: str, lot_sizes: dict, dry_run: bool):
    """One monitoring cycle for a symbol."""
    t = ist_time()
    lot_size = lot_sizes.get(symbol, DEFAULT_LOT_SIZES.get(symbol, 75))
    min_premium = MIN_PREMIUM_NIFTY if symbol == "NIFTY" else MIN_PREMIUM_BANKNIFTY

    pos = get_open_position(ch, symbol)

    # ── In-trade: check stop / trail / EOD ───────────────────────────────────
    if pos:
        current = get_current_straddle(ch, symbol, pos["expiry"], pos["strike"])
        if current is None:
            log.warning(f"[{symbol}] could not fetch current straddle — skipping tick")
            return

        pnl_pts = pos["entry_premium"] - current
        pnl_inr = pnl_pts * pos["lot_size"]

        log.info(f"[{symbol}] IN-TRADE strike={pos['strike']:.0f} "
                 f"entry={pos['entry_premium']:.1f} now={current:.1f} "
                 f"pnl={pnl_pts:.1f}pts ₹{pnl_inr:.0f} "
                 f"trailing={'yes' if pos['trailing_active'] else 'no'}")

        # EOD exit
        if t >= EOD_EXIT:
            record_exit(ch, pos, current, "eod", dry_run)
            return

        # Hard stop loss
        if pnl_inr <= -STOPLOSS_INR:
            record_exit(ch, pos, current, "stop", dry_run)
            return

        # Trailing logic
        if pos["trailing_active"]:
            peak, trail = update_trail(ch, pos, pnl_inr, dry_run)
            if pnl_inr <= trail:
                record_exit(ch, pos, current, "trail", dry_run)
            return

        # Target hit — activate trailing instead of exiting
        if pnl_inr >= TARGET_INR:
            log.info(f"[{symbol}] TARGET ₹{TARGET_INR:.0f} hit — activating trailing stop")
            update_trail(ch, pos, pnl_inr, dry_run)
            return

        return  # hold

    # ── No position: evaluate entry ───────────────────────────────────────────
    if t < ENTRY_START or t >= ENTRY_CUTOFF:
        log.info(f"[{symbol}] outside entry window ({t}) — no action")
        return

    # Cooldown after stop loss — exit_time written as ist_naive() so comparison is IST vs IST
    last_stop = last_stop_time(ch, symbol)
    if last_stop:
        if not isinstance(last_stop, datetime):
            last_stop = datetime.fromisoformat(str(last_stop))
        if last_stop.tzinfo is not None:
            last_stop = last_stop.astimezone(IST).replace(tzinfo=None)
        elapsed_m = (ist_naive() - last_stop).total_seconds() / 60
        if elapsed_m < STOP_COOLDOWN_M:
            log.info(f"[{symbol}] stop cooldown {elapsed_m:.0f}/{STOP_COOLDOWN_M} min — waiting")
            return

    snap = get_latest_snapshot(ch, symbol)
    if snap is None:
        log.warning(f"[{symbol}] no intraday snapshot available — skipping")
        return

    # ClickHouse returns UTC-naive datetime; convert to IST-naive before age check
    snap_ts = snap["timestamp"]
    if not isinstance(snap_ts, datetime):
        snap_ts = datetime.fromisoformat(str(snap_ts))
    snap_ts_ist = snap_ts + timedelta(hours=5, minutes=30)
    age_s = (ist_naive() - snap_ts_ist).total_seconds()
    if age_s > 20 * 60:  # snapshot older than 20 min is stale
        log.warning(f"[{symbol}] snapshot is {age_s/60:.0f} min old — skipping")
        return

    log.info(f"[{symbol}] EVAL strike={snap['strike']:.0f} "
             f"straddle={snap['straddle']:.1f} iv={snap['atm_iv']:.1f}%")

    # Entry gate: premium must be large enough relative to stop
    stop_pts = STOPLOSS_INR / lot_size
    if snap["straddle"] < min_premium:
        log.info(f"[{symbol}] premium {snap['straddle']:.1f} < min {min_premium} — skip")
        return

    if snap["straddle"] < stop_pts * 2:
        log.info(f"[{symbol}] premium {snap['straddle']:.1f} < 2× stop {stop_pts*2:.1f} — skip")
        return

    scorecard_conf = get_scorecard_confidence(ch, symbol)
    log.info(f"[{symbol}] scorecard={scorecard_conf:.0f} — ENTERING")
    record_entry(ch, symbol, snap, lot_size, scorecard_conf, dry_run)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Evaluate and log signals but do not write to DB")
    args = parser.parse_args()

    log.info("=== Intraday Straddle Monitor (paper trading) ===")
    log.info(f"Symbols        : {SYMBOLS}")
    log.info(f"Target         : ₹{TARGET_INR:.0f} → trailing at {TRAIL_PCT:.0%} of peak")
    log.info(f"Stop loss      : ₹{STOPLOSS_INR:.0f}")
    log.info(f"Entry window   : {ENTRY_START}–{ENTRY_CUTOFF} IST")
    log.info(f"EOD exit       : {EOD_EXIT} IST")
    log.info(f"Loop interval  : {LOOP_INTERVAL_S//60} min")
    log.info(f"Dry run        : {args.dry_run}")

    ch = get_ch()
    lot_sizes = load_lot_sizes(ch)
    log.info(f"Lot sizes      : {lot_sizes}")

    while ist_time() <= MONITOR_EXIT:
        t = ist_time()
        if t < ENTRY_START:
            wait = (datetime.combine(date.today(), ENTRY_START) - ist_naive()).total_seconds()
            log.info(f"Market not open yet — sleeping {wait/60:.1f} min")
            time.sleep(min(wait, LOOP_INTERVAL_S))
            continue

        log.info(f"--- Tick at {ist_now().strftime('%H:%M:%S')} IST ---")
        for symbol in SYMBOLS:
            try:
                tick(ch, symbol, lot_sizes, args.dry_run)
            except Exception as e:
                log.error(f"[{symbol}] tick failed: {e}", exc_info=True)

        if ist_time() >= MONITOR_EXIT:
            break

        log.info(f"Next tick in {LOOP_INTERVAL_S//60} min")
        time.sleep(LOOP_INTERVAL_S)

    log.info("=== Intraday monitor session complete ===")


if __name__ == "__main__":
    main()
