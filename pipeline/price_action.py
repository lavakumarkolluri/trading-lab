"""
price_action.py
───────────────
Shared price action feature computation for backtests.
Fetches OHLCV from market.ohlcv_daily and computes:

  1. prev_range_pct     — Prior day range as % of close
  2. range_vs_atr       — Prior day range / 20-day ATR (compression ratio)
  3. inside_bar         — Is current week's range inside prior week's range?
  4. ma20_dist_pct      — % distance of close from 20-day MA
  5. ma50_dist_pct      — % distance of close from 50-day MA
  6. above_ma20         — 1 if close > MA20
  7. week_range_pct     — Current week's high-low / week open (so far)
  8. consec_inside_days — How many consecutive days has range been < ATR

INDEX_SYMBOLS maps trading symbols to their ohlcv_daily tickers.
"""

from __future__ import annotations
from datetime import date, timedelta
import logging
import pandas as pd

log = logging.getLogger(__name__)

# Map options-chain symbol → ohlcv_daily symbol
INDEX_SYMBOLS = {
    "NIFTY":     "^NSEI",
    "BANKNIFTY": "^NSEBANK",
    "FINNIFTY":  None,   # not in ohlcv_daily — will return empty features
}

PA_DEFAULTS = {
    "prev_range_pct":     0.0,
    "range_vs_atr":       0.0,
    "inside_bar":         0,
    "ma20_dist_pct":      0.0,
    "ma50_dist_pct":      0.0,
    "above_ma20":         0,
    "week_range_pct":     0.0,
    "consec_inside_days": 0,
    "pa_available":       0,
}


def _load_ohlcv(ch, ohlcv_sym: str, as_of: date, lookback_days: int = 80) -> pd.DataFrame:
    """Fetch OHLCV for the index up to and including as_of date."""
    since = as_of - timedelta(days=lookback_days)
    result = ch.query(
        "SELECT date, open, high, low, close "
        "FROM market.ohlcv_daily FINAL "
        "WHERE symbol={sym:String} AND date >= {d_from:Date} AND date <= {d_to:Date} "
        "ORDER BY date",
        parameters={"sym": ohlcv_sym, "d_from": since, "d_to": as_of}
    )
    if not result.result_rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
    df = pd.DataFrame(result.result_rows, columns=["date", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df


def compute_price_action(ch, symbol: str, as_of: date) -> dict:
    """
    Compute price action features for `symbol` as of `as_of` date.
    Returns PA_DEFAULTS if index data is unavailable (e.g. FINNIFTY).
    """
    ohlcv_sym = INDEX_SYMBOLS.get(symbol)
    if ohlcv_sym is None:
        return PA_DEFAULTS.copy()

    df = _load_ohlcv(ch, ohlcv_sym, as_of)
    if len(df) < 22:
        log.debug("Insufficient OHLCV data for %s as of %s", symbol, as_of)
        return PA_DEFAULTS.copy()

    df = df.set_index("date")
    today = df.index[-1]   # last row = as_of date

    # ── 1. Prior day range as % of close ─────────────────────────────────────
    if len(df) >= 2:
        prev = df.iloc[-2]
        prev_range_pct = (prev["high"] - prev["low"]) / prev["close"] * 100
    else:
        prev_range_pct = 0.0

    # ── 2. 20-day ATR (true range, simplified as high-low/close) ─────────────
    df["range_pct"] = (df["high"] - df["low"]) / df["close"] * 100
    atr_20 = df["range_pct"].iloc[-21:-1].mean()   # prior 20 days, exclude today
    range_vs_atr = prev_range_pct / atr_20 if atr_20 > 0 else 0.0

    # ── 3. Inside bar (current week inside prior week) ────────────────────────
    df_reset = df.reset_index()
    df_reset["iso_year"] = df_reset["date"].dt.isocalendar().year.astype(int)
    df_reset["iso_week"] = df_reset["date"].dt.isocalendar().week.astype(int)
    weekly = df_reset.groupby(["iso_year", "iso_week"]).agg(
        w_high=("high", "max"), w_low=("low", "min")
    ).reset_index().sort_values(["iso_year", "iso_week"])

    inside_bar = 0
    if len(weekly) >= 2:
        cur_w  = weekly.iloc[-1]
        prev_w = weekly.iloc[-2]
        if cur_w["w_high"] < prev_w["w_high"] and cur_w["w_low"] > prev_w["w_low"]:
            inside_bar = 1

    # ── 4 & 5. Distance from 20-day and 50-day MA ────────────────────────────
    close_today = float(df["close"].iloc[-1])
    ma20 = float(df["close"].iloc[-20:].mean())
    ma50 = float(df["close"].iloc[-50:].mean()) if len(df) >= 50 else ma20
    ma20_dist_pct = (close_today - ma20) / ma20 * 100
    ma50_dist_pct = (close_today - ma50) / ma50 * 100
    above_ma20 = int(close_today > ma20)

    # ── 6. Current week's range so far ───────────────────────────────────────
    cur_week_mask = (
        (df_reset["iso_year"] == int(weekly.iloc[-1]["iso_year"])) &
        (df_reset["iso_week"] == int(weekly.iloc[-1]["iso_week"]))
    )
    cur_week = df_reset[cur_week_mask]
    if not cur_week.empty and float(cur_week["open"].iloc[0]) > 0:
        week_range_pct = (
            float(cur_week["high"].max()) - float(cur_week["low"].min())
        ) / float(cur_week["open"].iloc[0]) * 100
    else:
        week_range_pct = 0.0

    # ── 7. Consecutive inside days (daily range < ATR) ────────────────────────
    consec = 0
    for rng in reversed(df["range_pct"].iloc[:-1].values):
        if rng < atr_20:
            consec += 1
        else:
            break

    return {
        "prev_range_pct":     round(prev_range_pct, 4),
        "range_vs_atr":       round(range_vs_atr, 4),
        "inside_bar":         inside_bar,
        "ma20_dist_pct":      round(ma20_dist_pct, 4),
        "ma50_dist_pct":      round(ma50_dist_pct, 4),
        "above_ma20":         above_ma20,
        "week_range_pct":     round(week_range_pct, 4),
        "consec_inside_days": consec,
        "pa_available":       1,
    }
