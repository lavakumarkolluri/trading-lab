#!/usr/bin/env python3
"""
validation_engine.py
────────────────────
Validates open predictions against actual OHLCV data and writes
outcomes to analysis.prediction_outcomes.

Run after market close each trading day (3:30 PM IST).

Validation logic:
  For each open prediction where target_date <= today:
    1. Fetch actual OHLCV for target_date from market.ohlcv_daily
    2. entry_price = open on target_date (DATA-001: avoids forward-look bias;
         prediction is made at EOD on T, trade enters at open on T+1)
    3. Compute actual returns:
         actual_pct_1d = (close[T+1] - entry_price) / entry_price * 100
         actual_pct_3d = (close[T+3] - entry_price) / entry_price * 100
         actual_pct_5d = (close[T+5] - entry_price) / entry_price * 100
    4. was_correct_Nd = 1 if direction matches AND |actual| >= predicted_min_pct
    5. hit_threshold  = 1 if ANY window was correct

After writing outcomes:
  Updates analysis.predictions.status to 'hit' | 'miss' | 'expired'
  expired = target_date passed but no OHLCV available (holiday/weekend gap)

Incremental:
  Only processes predictions where status = 'open' AND target_date <= today.
  Re-running is safe — ReplacingMergeTree deduplicates on
  (prediction_date, symbol, pattern_id).

Usage:
  python validation_engine.py              # validate all open predictions
  python validation_engine.py --date 2026-04-07  # validate specific target date
  python validation_engine.py --symbol RELIANCE.NS
  python validation_engine.py --dry-run   # compute but don't write outcomes
"""

import os
import re
import logging
import argparse
import threading
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import clickhouse_connect
from holidays_pipeline import is_trading_day, next_trading_day

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MAX_WORKERS         = 8
EXPIRY_GRACE_DAYS   = 5    # mark as 'expired' if no OHLCV N days after target

# ── SEC-003: input validation ──────────────────────────
_SAFE_ID     = re.compile(r'^[A-Za-z0-9_\-]{1,50}$')
_SAFE_SYMBOL = re.compile(r'^[A-Za-z0-9._\-&=]{1,30}$')
_VALID_STATUSES = frozenset({"hit", "miss", "expired"})

def _validate_id(value: str, label: str) -> None:
    if not _SAFE_ID.match(value):
        raise ValueError(f"SEC-003: unsafe {label} {value!r}")

def _validate_symbol(value: str) -> None:
    if not _SAFE_SYMBOL.match(value):
        raise ValueError(f"SEC-003: unsafe symbol {value!r}")

def _validate_status(value: str) -> None:
    if value not in _VALID_STATUSES:
        raise ValueError(f"SEC-003: invalid status {value!r}")

# ── Results tracker ────────────────────────────────────
results = {
    "validated":  0,
    "hits":       0,
    "misses":     0,
    "expired":    0,
    "failed":     [],
}
results_lock = threading.Lock()


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Data fetching
# ══════════════════════════════════════════════════════

def fetch_open_predictions(ch,
                           target_date: date = None,
                           symbol: str = None) -> pd.DataFrame:
    """
    Fetch predictions with status='open' and target_date <= today.
    Optionally filter by target_date or symbol.
    """
    conditions = ["status = 'open'", "target_date <= {today:Date}"]
    params     = {"today": date.today()}

    if target_date:
        conditions.append("target_date = {tdate:Date}")
        params["tdate"] = target_date

    if symbol:
        conditions.append("symbol = {sym:String}")
        params["sym"] = symbol

    where = "WHERE " + " AND ".join(conditions)

    result = ch.query(
        f"SELECT prediction_date, target_date, symbol, market, "
        f"       pattern_id, predicted_direction, predicted_min_pct, "
        f"       confidence_pct "
        f"FROM analysis.predictions FINAL "
        f"{where} "
        f"ORDER BY symbol, prediction_date",
        parameters=params
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(result.result_rows, columns=[
        "prediction_date", "target_date", "symbol", "market",
        "pattern_id", "predicted_direction", "predicted_min_pct",
        "confidence_pct"
    ])
    df["prediction_date"] = pd.to_datetime(df["prediction_date"]).dt.date
    df["target_date"]     = pd.to_datetime(df["target_date"]).dt.date
    return df


def fetch_ohlcv_window(ch, symbol: str, market: str,
                       from_date: date, to_date: date) -> pd.DataFrame:
    """
    Fetch OHLCV for symbol between from_date and to_date + 8 days buffer
    (to capture T+5 forward returns even near end of dataset).
    """
    buffer_to = to_date + timedelta(days=8)

    result = ch.query(
        "SELECT date, open, close "
        "FROM market.ohlcv_daily FINAL "
        "WHERE symbol = {sym:String} AND market = {mkt:String} "
        "  AND date >= {d_from:Date} AND date <= {d_to:Date} "
        "ORDER BY date",
        parameters={
            "sym":    symbol,
            "mkt":    market,
            "d_from": from_date,
            "d_to":   buffer_to,
        }
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(result.result_rows, columns=["date", "open", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values("date").reset_index(drop=True)


# ══════════════════════════════════════════════════════
# Forward return computation
# ══════════════════════════════════════════════════════

def get_nth_close_after(df_ohlcv: pd.DataFrame,
                        from_date: date, n: int) -> float | None:
    """
    Get close price N trading days after from_date.
    Returns None if insufficient data.
    """
    future = df_ohlcv[df_ohlcv["date"] > from_date]
    if len(future) < n:
        return None
    return float(future.iloc[n - 1]["close"])


def compute_actual_returns(df_ohlcv: pd.DataFrame,
                           prediction_date: date,
                           target_date: date,
                           predicted_direction: str,
                           predicted_min_pct: float) -> dict:
    """
    Compute actual forward returns using open on target_date as entry_price.
    DATA-001: entry at open[target_date] eliminates forward-look bias
    (prediction is made at EOD on T; earliest realistic entry is open on T+1).
    """
    entry_row = df_ohlcv[df_ohlcv["date"] == target_date]
    if entry_row.empty:
        return {}

    entry_price = float(entry_row.iloc[0]["open"])
    if entry_price <= 0:
        return {}

    def pct(close):
        return round((close - entry_price) / entry_price * 100, 4)

    def is_correct(ret: float) -> int:
        if predicted_direction == "UP":
            return 1 if ret >= predicted_min_pct else 0
        else:
            return 1 if ret <= -predicted_min_pct else 0

    r1 = get_nth_close_after(df_ohlcv, prediction_date, 1)
    r3 = get_nth_close_after(df_ohlcv, prediction_date, 3)
    r5 = get_nth_close_after(df_ohlcv, prediction_date, 5)

    actual_1d = pct(r1) if r1 else 0.0
    actual_3d = pct(r3) if r3 else 0.0
    actual_5d = pct(r5) if r5 else 0.0

    correct_1d = is_correct(actual_1d) if r1 else 0
    correct_3d = is_correct(actual_3d) if r3 else 0
    correct_5d = is_correct(actual_5d) if r5 else 0
    hit        = 1 if (correct_1d or correct_3d or correct_5d) else 0

    return {
        "entry_price":    entry_price,
        "actual_pct_1d":  actual_1d,
        "actual_pct_3d":  actual_3d,
        "actual_pct_5d":  actual_5d,
        "was_correct_1d": correct_1d,
        "was_correct_3d": correct_3d,
        "was_correct_5d": correct_5d,
        "hit_threshold":  hit,
    }


# ══════════════════════════════════════════════════════
# Insert / update helpers
# ══════════════════════════════════════════════════════

OUTCOME_COLS = [
    "prediction_date", "target_date", "symbol", "market", "pattern_id",
    "predicted_direction", "predicted_min_pct", "confidence_pct",
    "entry_price",
    "actual_pct_1d", "actual_pct_3d", "actual_pct_5d",
    "was_correct_1d", "was_correct_3d", "was_correct_5d",
    "hit_threshold", "outcome_notes",
    "validated_at", "version",
]


def insert_outcomes(ch, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)[OUTCOME_COLS]
    ch.insert_df("analysis.prediction_outcomes", df)


def update_prediction_status(ch, symbol: str,
                              prediction_date: date,
                              pattern_id: str,
                              status: str):
    """Update status of a prediction to 'hit', 'miss', or 'expired'."""
    # SEC-003: validate all user-derived values before SQL interpolation
    _validate_status(status)
    _validate_symbol(symbol)
    _validate_id(pattern_id, "pattern_id")
    safe_date = str(prediction_date)   # date.__str__ always yields YYYY-MM-DD
    ch.command(
        f"ALTER TABLE analysis.predictions UPDATE "
        f"status = '{status}' "
        f"WHERE symbol = '{symbol}' "
        f"  AND prediction_date = '{safe_date}' "
        f"  AND pattern_id = '{pattern_id}' "
        f"SETTINGS mutations_sync = 0"
    )


# ══════════════════════════════════════════════════════
# Per-symbol worker
# ══════════════════════════════════════════════════════

def process_symbol(symbol: str,
                   market: str,
                   predictions: pd.DataFrame,
                   dry_run: bool,
                   idx: int,
                   total: int):
    ch = get_ch_client()
    outcome_rows = []

    try:
        # Fetch OHLCV window covering all prediction dates + T+5 buffer
        from_date = predictions["prediction_date"].min()
        to_date   = predictions["target_date"].max()

        df_ohlcv = fetch_ohlcv_window(ch, symbol, market, from_date, to_date)

        close_dates = set(df_ohlcv["date"].tolist()) if not df_ohlcv.empty else set()
        now_ts      = datetime.now()
        version_ts  = int(now_ts.timestamp())

        for _, pred in predictions.iterrows():
            prediction_date   = pred["prediction_date"]
            target_date       = pred["target_date"]
            pattern_id        = pred["pattern_id"]
            direction         = pred["predicted_direction"]
            min_pct           = float(pred["predicted_min_pct"])
            confidence        = float(pred["confidence_pct"])

            # Check if target date OHLCV exists
            if target_date not in close_dates:
                # Check expiry — if target_date + EXPIRY_GRACE_DAYS < today,
                # declare expired (holiday / data gap)
                grace_end = target_date + timedelta(days=EXPIRY_GRACE_DAYS)
                if grace_end < date.today():
                    status = "expired"
                    notes  = f"No OHLCV data found for {target_date} after {EXPIRY_GRACE_DAYS}d grace"

                    outcome_rows.append({
                        "prediction_date":    prediction_date,
                        "target_date":        target_date,
                        "symbol":             symbol,
                        "market":             market,
                        "pattern_id":         pattern_id,
                        "predicted_direction": direction,
                        "predicted_min_pct":  min_pct,
                        "confidence_pct":     confidence,
                        "entry_price":        0.0,
                        "actual_pct_1d":      0.0,
                        "actual_pct_3d":      0.0,
                        "actual_pct_5d":      0.0,
                        "was_correct_1d":     0,
                        "was_correct_3d":     0,
                        "was_correct_5d":     0,
                        "hit_threshold":      0,
                        "outcome_notes":      notes,
                        "validated_at":       now_ts,
                        "version":            version_ts,
                    })

                    if not dry_run:
                        update_prediction_status(
                            ch, symbol, prediction_date, pattern_id, status
                        )

                    with results_lock:
                        results["expired"] += 1
                else:
                    # Too early — target date hasn't had OHLCV loaded yet
                    log.debug(f"  [{symbol}] {target_date} — OHLCV not yet available")
                continue

            # Compute actual returns
            actuals = compute_actual_returns(
                df_ohlcv, prediction_date, target_date, direction, min_pct
            )

            if not actuals:
                log.warning(f"  [{symbol}] Could not compute returns for {prediction_date}")
                continue

            hit    = actuals["hit_threshold"]
            status = "hit" if hit else "miss"
            notes  = (
                f"1d={actuals['actual_pct_1d']:+.2f}% "
                f"3d={actuals['actual_pct_3d']:+.2f}% "
                f"5d={actuals['actual_pct_5d']:+.2f}% | "
                f"correct={actuals['was_correct_1d']}/{actuals['was_correct_3d']}/{actuals['was_correct_5d']}"
            )

            outcome_rows.append({
                "prediction_date":    prediction_date,
                "target_date":        target_date,
                "symbol":             symbol,
                "market":             market,
                "pattern_id":         pattern_id,
                "predicted_direction": direction,
                "predicted_min_pct":  min_pct,
                "confidence_pct":     confidence,
                "entry_price":        actuals["entry_price"],
                "actual_pct_1d":      actuals["actual_pct_1d"],
                "actual_pct_3d":      actuals["actual_pct_3d"],
                "actual_pct_5d":      actuals["actual_pct_5d"],
                "was_correct_1d":     actuals["was_correct_1d"],
                "was_correct_3d":     actuals["was_correct_3d"],
                "was_correct_5d":     actuals["was_correct_5d"],
                "hit_threshold":      hit,
                "outcome_notes":      notes,
                "validated_at":       now_ts,
                "version":            version_ts,
            })

            if not dry_run:
                update_prediction_status(
                    ch, symbol, prediction_date, pattern_id, status
                )

            with results_lock:
                results["validated"] += 1
                if hit:
                    results["hits"] += 1
                else:
                    results["misses"] += 1

        if outcome_rows and not dry_run:
            insert_outcomes(ch, outcome_rows)

    except Exception as e:
        log.error(f"  FAILED [{symbol}]: {e}")
        with results_lock:
            results["failed"].append(f"{symbol} → {e}")

    return outcome_rows


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def print_summary(all_outcomes: list[dict], dry_run: bool):
    print("\n" + "=" * 65)
    print("VALIDATION ENGINE — SUMMARY")
    print("=" * 65)

    if dry_run:
        print("⚠️  DRY RUN — outcomes NOT written to DB")

    total = results["validated"] + results["expired"]
    hit_rate = (
        round(results["hits"] / results["validated"] * 100, 1)
        if results["validated"] > 0 else 0.0
    )

    print(f"📊 Predictions validated : {results['validated']}")
    print(f"✅ Hits                  : {results['hits']} ({hit_rate}%)")
    print(f"❌ Misses                : {results['misses']}")
    print(f"⏳ Expired               : {results['expired']}")
    print(f"💥 Failed                : {len(results['failed'])}")

    if all_outcomes:
        validated = [o for o in all_outcomes if o["entry_price"] > 0]
        if validated:
            print(f"\nDetailed outcomes:")
            print(f"{'Symbol':<20} {'Pat':<6} {'Dir':<5} "
                  f"{'1d%':>7} {'3d%':>7} {'5d%':>7} {'Result':<8}")
            print("-" * 65)
            for o in sorted(validated,
                           key=lambda x: x["actual_pct_1d"],
                           reverse=True):
                result_str = "✅ HIT" if o["hit_threshold"] else "❌ MISS"
                dir_arrow  = "↑" if o["predicted_direction"] == "UP" else "↓"
                print(
                    f"{o['symbol']:<20} "
                    f"{o['pattern_id']:<6} "
                    f"{dir_arrow:<5} "
                    f"{o['actual_pct_1d']:>+6.2f}% "
                    f"{o['actual_pct_3d']:>+6.2f}% "
                    f"{o['actual_pct_5d']:>+6.2f}% "
                    f"{result_str}"
                )

    print("=" * 65)


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Validation Engine — validates predictions against actual outcomes"
    )
    parser.add_argument("--date",    type=str, default=None,
                        help="Validate predictions with this target_date YYYY-MM-DD")
    parser.add_argument("--symbol",  type=str, default=None,
                        help="Validate predictions for a single symbol")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute outcomes but don't write to DB")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else None

    log.info("=== Validation Engine Starting ===")
    log.info(f"Target date filter : {target_date or 'all open'}")
    log.info(f"Dry run            : {args.dry_run}")
    log.info(f"Started            : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # ── Fetch open predictions ─────────────────────────
    log.info("Fetching open predictions...")
    df_predictions = fetch_open_predictions(
        ch, target_date=target_date, symbol=args.symbol
    )

    if df_predictions.empty:
        log.info("No open predictions due for validation today.")
        return

    log.info(f"Predictions to validate: {len(df_predictions)}")

    # ── Group by symbol ────────────────────────────────
    symbol_groups = {}
    for _, row in df_predictions.iterrows():
        key = (row["symbol"], row["market"])
        symbol_groups.setdefault(key, []).append(row)

    total = len(symbol_groups)
    log.info(f"Symbols to process : {total}")
    log.info(f"Workers            : {MAX_WORKERS}")

    all_outcomes = []
    lock         = threading.Lock()

    def _worker(item):
        idx, ((symbol, market), rows) = item
        df_sym = pd.DataFrame(rows)
        outcomes = process_symbol(
            symbol, market, df_sym,
            args.dry_run, idx, total
        )
        if outcomes:
            with lock:
                all_outcomes.extend(outcomes)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(_worker, enumerate(symbol_groups.items(), 1))

    print_summary(all_outcomes, args.dry_run)
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()