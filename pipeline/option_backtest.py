#!/usr/bin/env python3
"""
option_backtest.py
──────────────────
Backtests the PCR + VIX-based index option strategy historically.

═══ NO-LOOKAHEAD GUARANTEE ═══════════════════════════════════════════════
Signal generation for date T uses ONLY data available at EOD of T:
  • VIX              — published by NSE during market hours; known at close
  • VIX rank         — rolling 252-day window ending on T (pandas .rolling())
  • PCR              — computed from NSE F&O bhavcopy EOD file for T
  • Entry price      — ATM option closing price from same bhavcopy file

The exit price (T+3 trading days) is the OUTCOME being measured.
It is never used in signal generation. This is not lookahead — it is
the forward window the strategy claims to predict.
═══════════════════════════════════════════════════════════════════════════

Data sources:
  • VIX + Nifty spot : market.nifty_live  (run: vix_pipeline --full first)
  • F&O bhavcopy     : NSE archives (nsearchives.nseindia.com)
                       Free, no auth, ~600 files for 2 years

Signal logic (mirrors option_chain_pipeline — same rules, historical replay):
  PCR > 1.3  →  bullish  →  buy ATM Call (CE)
  PCR < 0.7  →  bearish  →  buy ATM Put  (PE)
  PCR 0.7–1.3 + VIX regime 'low'/'normal' + Nifty uptrend → bullish
  PCR 0.7–1.3 + VIX regime 'elevated'/'extreme' → bearish
  Otherwise  →  no trade

Hold period: 3 trading days (closes before expiry if T+3 > expiry).
P&L = exit_close − entry_close per unit (Rs). Zero if expired worthless.

Usage:
  python option_backtest.py                      # 2-year backtest
  python option_backtest.py --days 365           # 1 year
  python option_backtest.py --from 2024-01-01 --to 2025-01-01
  python option_backtest.py --dry-run            # fetch + compute, no DB insert

Docker:
  docker compose run --rm option_backtest
"""

import io
import os
import sys
import time
import zipfile
import logging
import argparse
import threading
from datetime import date, timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import clickhouse_connect
from curl_cffi import requests as cffi_requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

LOOKBACK_DAYS  = 730
MAX_WORKERS    = 3      # concurrent bhavcopy downloads (NSE rate-limits aggressively)
REQUEST_DELAY  = 1.0    # seconds between requests per worker
NSE_HOME       = "https://www.nseindia.com"
NSE_ARCHIVE    = "https://nsearchives.nseindia.com"
SESSION_TIMEOUT = 30

# Transaction costs (Zerodha / NSE typical)
LOT_SIZE        = 75      # Nifty options lot size
BROKERAGE       = 20.0    # flat per order leg; round-trip = 2 legs
STT_PCT         = 0.0005  # 0.05% of premium on sell leg only
EXCHANGE_PCT    = 0.0005  # NSE + SEBI charges (both legs combined ~0.05%)
GST_RATE        = 0.18    # 18% GST on brokerage only

# Nifty strike step (index options in 50-point increments)
STRIKE_STEP = 50.0

# New bhavcopy URL (NSE archives, works from ~Jan 2023 onwards)
BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
)

# PCR thresholds (same as option_chain_pipeline)
PCR_BULLISH = 1.3
PCR_BEARISH = 0.7


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Bhavcopy download + parse
# ══════════════════════════════════════════════════════

def _parse_new_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse NSE F&O bhavcopy new format (2023+).
    Columns: TckrSymb, XpryDt, StrkPric, OptnTp, ClsPric/SttlmPric, OpnIntrst
    Returns DataFrame: expiry, strike, option_type, close, oi
    """
    # Filter to NIFTY index options
    mask = (
        df["TckrSymb"].str.strip().eq("NIFTY") &
        df["OptnTp"].isin(["CE", "PE"])
    )
    if "FinInstrmTp" in df.columns:
        # NSE changed instrument type from "IO" to "IDO" around 2025-2026
        mask &= df["FinInstrmTp"].str.strip().isin(["IO", "IDO"])
    df = df[mask].copy()
    if df.empty:
        return pd.DataFrame()

    # Close price: prefer ClsPric, fallback SttlmPric
    close_col = "ClsPric" if "ClsPric" in df.columns else "SttlmPric"

    out = pd.DataFrame({
        "expiry":      pd.to_datetime(df["XpryDt"], errors="coerce").dt.date,
        "strike":      pd.to_numeric(df["StrkPric"], errors="coerce"),
        "option_type": df["OptnTp"].str.strip(),
        "close":       pd.to_numeric(df[close_col], errors="coerce"),
        "oi":          pd.to_numeric(df["OpnIntrst"], errors="coerce").fillna(0),
    })
    return out.dropna(subset=["expiry", "strike", "close"])


def _parse_old_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse NSE F&O bhavcopy old format (pre-2023).
    Columns: INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP, CLOSE, OPEN_INT
    Returns DataFrame: expiry, strike, option_type, close, oi
    """
    mask = (
        df["INSTRUMENT"].str.strip().eq("OPTIDX") &
        df["SYMBOL"].str.strip().eq("NIFTY") &
        df["OPTION_TYP"].str.strip().isin(["CE", "PE"])
    )
    df = df[mask].copy()
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "expiry":      pd.to_datetime(df["EXPIRY_DT"], format="%d-%b-%Y",
                                      errors="coerce").dt.date,
        "strike":      pd.to_numeric(df["STRIKE_PR"], errors="coerce"),
        "option_type": df["OPTION_TYP"].str.strip(),
        "close":       pd.to_numeric(df["CLOSE"], errors="coerce"),
        "oi":          pd.to_numeric(df["OPEN_INT"], errors="coerce").fillna(0),
    })
    return out.dropna(subset=["expiry", "strike", "close"])


def build_nse_session() -> cffi_requests.Session:
    """Build a curl_cffi session that bypasses NSE bot protection."""
    session = cffi_requests.Session(impersonate="chrome110")
    log.info("Warming NSE session...")
    session.get(
        f"{NSE_HOME}/option-chain",
        timeout=SESSION_TIMEOUT,
        headers={
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 Chrome/110.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://www.nseindia.com/",
        },
    )
    log.info(f"NSE session ready (cookies: {len(session.cookies)})")
    time.sleep(2)
    return session


def fetch_bhavcopy(dt: date, session: cffi_requests.Session) -> pd.DataFrame | None:
    """
    Download and parse NSE F&O bhavcopy for one date.
    Returns DataFrame(expiry, strike, option_type, close, oi) with
    NIFTY index options only, or None on 404/parse failure.
    """
    url = BHAVCOPY_URL.format(date=dt.strftime("%Y%m%d"))
    try:
        resp = session.get(
            url, timeout=SESSION_TIMEOUT,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 Chrome/110.0.0.0 Safari/537.36",
                "Referer":    "https://www.nseindia.com/",
            },
        )
        if resp.status_code == 404:
            return None   # holiday / weekend — silent skip
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            raw = pd.read_csv(zf.open(csv_name), low_memory=False)

        # Detect format by column names
        if "TckrSymb" in raw.columns:
            df = _parse_new_format(raw)
        elif "SYMBOL" in raw.columns:
            df = _parse_old_format(raw)
        else:
            log.warning(f"  {dt}: unrecognised bhavcopy format — columns: "
                        f"{list(raw.columns[:5])}")
            return None

        return df if not df.empty else None

    except zipfile.BadZipFile:
        return None   # holiday / weekend
    except Exception as e:
        log.warning(f"  {dt}: fetch failed — {e}")
        return None


def download_bhavcopy_range(dates: list[date]) -> dict[date, pd.DataFrame]:
    """
    Download bhavcopy files for all dates.
    Each worker thread gets its own NSE session (curl_cffi is not thread-safe).
    Returns {date: DataFrame} for dates that have data.
    """
    _local = threading.local()
    cache  = {}
    lock   = threading.Lock()
    total  = len(dates)
    done   = [0]

    def _get_session():
        if not hasattr(_local, "session"):
            _local.session = build_nse_session()
        return _local.session

    def _fetch_one(dt: date):
        time.sleep(REQUEST_DELAY)
        df = fetch_bhavcopy(dt, _get_session())
        with lock:
            done[0] += 1
            if df is not None:
                cache[dt] = df
                if done[0] % 50 == 0:
                    log.info(f"  Downloaded {done[0]}/{total} dates "
                             f"({len(cache)} with data)")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(_fetch_one, dates))

    return cache


# ══════════════════════════════════════════════════════
# VIX + Nifty from DB
# ══════════════════════════════════════════════════════

def fetch_vix_series(ch, from_date: date, to_date: date) -> pd.DataFrame:
    """
    Fetch VIX + Nifty spot from market.nifty_live.
    Returns DataFrame: date, vix, nifty_spot (sorted ascending).
    """
    result = ch.query(
        "SELECT toDate(timestamp) AS d, avg(vix) AS avg_vix, avg(nifty_spot) AS avg_nifty "
        "FROM market.nifty_live FINAL "
        "WHERE toDate(timestamp) >= {f:Date} AND toDate(timestamp) <= {t:Date} "
        "  AND vix > 0 "
        "GROUP BY d ORDER BY d",
        parameters={"f": from_date, "t": to_date}
    )
    if not result.result_rows:
        return pd.DataFrame(columns=["date", "vix", "nifty_spot"])

    df = pd.DataFrame(result.result_rows, columns=["date", "vix", "nifty_spot"])
    df["date"]       = pd.to_datetime(df["date"]).dt.date
    df["vix"]        = df["vix"].astype(float)
    df["nifty_spot"] = df["nifty_spot"].astype(float)
    return df.sort_values("date").reset_index(drop=True)


# ══════════════════════════════════════════════════════
# Rolling VIX rank (NO LOOKAHEAD)
# ══════════════════════════════════════════════════════

def compute_rolling_vix_rank(vix_series: pd.Series,
                              window: int = 252) -> pd.Series:
    """
    For each date T: percentile rank of vix[T] within vix[T-window+1 … T].
    The window includes T itself — this is correct because VIX is known
    during market hours on day T, before the close.

    Returns Series (same index) with rank in [0, 100].
    0 = lowest VIX in past year (cheap vol), 100 = highest.
    """
    return vix_series.rolling(window=window, min_periods=20).apply(
        lambda x: float(pd.Series(x).rank(pct=True).iloc[-1]) * 100,
        raw=False,
    )


# ══════════════════════════════════════════════════════
# Signal helpers
# ══════════════════════════════════════════════════════

def get_near_expiry(df_oc: pd.DataFrame, signal_date: date) -> date | None:
    """
    Nearest expiry strictly after signal_date.
    Avoids entering trades on the expiry day itself (gamma instability).
    """
    future_expiries = sorted(
        e for e in df_oc["expiry"].unique() if e > signal_date
    )
    return future_expiries[0] if future_expiries else None


def derive_bias(pcr: float, vix: float, vix_rank: float) -> str:
    """
    Same logic as option_chain_pipeline.write_strike_recommendation.
    Returns 'bullish', 'bearish', or 'neutral'.
    """
    if pcr > PCR_BULLISH:
        return "bullish"
    if pcr < PCR_BEARISH:
        return "bearish"

    # PCR is neutral → fall back to VIX regime
    # Low/normal VIX and low rank → risk-on → bullish
    if vix < 18 and vix_rank < 50:
        return "bullish"
    # Elevated/extreme VIX → risk-off → bearish
    if vix > 22:
        return "bearish"

    return "neutral"


def find_atm_price(df_near: pd.DataFrame, spot: float,
                   option_type: str) -> tuple[float, float]:
    """
    Find ATM strike and its close price.
    Tries ATM, then ±1 step, then ±2 steps before giving up.
    Returns (atm_strike, close_price) or (0, 0) if not found.
    """
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    for offset in [0, STRIKE_STEP, -STRIKE_STEP,
                   2 * STRIKE_STEP, -2 * STRIKE_STEP]:
        strike = atm + offset
        rows = df_near[
            (df_near["strike"] == strike) &
            (df_near["option_type"] == option_type)
        ]
        if not rows.empty:
            close = float(rows["close"].iloc[0])
            if close > 0:
                return strike, close
    return 0.0, 0.0


def transaction_cost_per_unit(entry_price: float, exit_price: float,
                               strategy: str) -> float:
    """
    Round-trip transaction cost per unit (1 contract, not per lot).
    For BUYING  : you buy at entry, sell at exit → STT on exit leg
    For SELLING : you sell at entry, buy back at exit → STT on entry leg
    """
    brok  = (BROKERAGE * 2) * (1 + GST_RATE)   # buy + sell legs, with GST
    exch  = (entry_price + exit_price) * EXCHANGE_PCT
    if strategy == "buy":
        stt = exit_price * STT_PCT
    else:
        stt = entry_price * STT_PCT
    return round((brok + exch + stt) / LOT_SIZE, 4)   # per unit


# ══════════════════════════════════════════════════════
# Core backtest loop
# ══════════════════════════════════════════════════════

def run_backtest(vix_df: pd.DataFrame,
                 bhavcopy_cache: dict[date, pd.DataFrame],
                 strategy: str = "buy") -> list[dict]:  # strategy stored per row
    """
    Replay strategy signal day-by-day using only past data.

    ┌─ Signal date T (EOD) ─────────────────────────────────────────────┐
    │  VIX, vix_rank, PCR → bias (bullish/bearish/neutral)              │
    │  ATM option close → entry_price                                    │
    │  All inputs known at 3:30 PM IST on T. No future data used.       │
    └────────────────────────────────────────────────────────────────────┘
    ┌─ Exit date T+3 trading days ──────────────────────────────────────┐
    │  Same strike, same expiry, close price → exit_price (measured)    │
    │  If T+3 > expiry: use expiry-date close instead (settlement)      │
    └────────────────────────────────────────────────────────────────────┘
    """
    if vix_df.empty:
        log.error("VIX series is empty. Run: docker compose run --rm vix_pipeline --full")
        return []

    # Build indexed lookups
    vix_idx = vix_df.set_index("date")

    # Rolling VIX rank — computed on full series, NO LOOKAHEAD
    # (rolling includes current day T, which is fine — VIX is known at T)
    vix_rank_series = compute_rolling_vix_rank(vix_idx["vix"])
    log.info(f"VIX rank computed for {len(vix_rank_series.dropna())} dates")

    # Trading dates available in bhavcopy (actual market days)
    trading_dates = sorted(bhavcopy_cache.keys())
    td_set        = set(trading_dates)

    signals = []

    for i, today in enumerate(trading_dates):
        df_oc = bhavcopy_cache[today]

        # ── VIX + Nifty (must be available for signal) ───────────────
        if today not in vix_idx.index:
            continue
        vix        = float(vix_idx.loc[today, "vix"])
        nifty_spot = float(vix_idx.loc[today, "nifty_spot"])
        if nifty_spot <= 0 or vix <= 0:
            continue

        vix_rank = float(vix_rank_series.get(today) or 0.0)
        if vix_rank == 0.0 and i < 20:
            continue   # skip warm-up period before rolling window fills

        # ── PCR from bhavcopy (EOD, no lookahead) ────────────────────
        near_expiry = get_near_expiry(df_oc, today)
        if near_expiry is None:
            continue

        df_near   = df_oc[df_oc["expiry"] == near_expiry]
        total_ce  = df_near[df_near["option_type"] == "CE"]["oi"].sum()
        total_pe  = df_near[df_near["option_type"] == "PE"]["oi"].sum()
        pcr       = round(total_pe / total_ce, 4) if total_ce > 0 else 0.0
        if total_ce == 0:
            continue

        # ── Signal bias ───────────────────────────────────────────────
        bias = derive_bias(pcr, vix, vix_rank)
        if bias == "neutral":
            continue

        # Buy: follow bias (buy CE on bullish, buy PE on bearish)
        # Sell: fade bias (sell PE on bullish = it decays, sell CE on bearish)
        if strategy == "buy":
            opt_type = "CE" if bias == "bullish" else "PE"
        else:
            opt_type = "PE" if bias == "bullish" else "CE"

        # ── Entry price (ATM option close on signal date, no lookahead)
        atm_strike, entry_price = find_atm_price(df_near, nifty_spot, opt_type)
        if entry_price <= 0:
            continue

        # ── Exit date: T + 3 trading days, capped at expiry date ─────
        future_tds = [d for d in trading_dates if d > today]
        exit_date  = (future_tds[2]
                      if len(future_tds) >= 3
                      else future_tds[-1] if future_tds else None)
        if exit_date is None:
            continue
        # Cap at expiry (can't hold past expiry)
        exit_date = min(exit_date, near_expiry)

        # ── Exit price (future outcome being measured) ────────────────
        if exit_date not in bhavcopy_cache:
            continue

        df_exit = bhavcopy_cache[exit_date]
        df_exit_expiry = df_exit[df_exit["expiry"] == near_expiry]
        exit_rows = df_exit_expiry[
            (df_exit_expiry["strike"] == atm_strike) &
            (df_exit_expiry["option_type"] == opt_type)
        ]
        exit_price = float(exit_rows["close"].iloc[0]) \
                     if not exit_rows.empty else 0.0

        # ── Nifty return over hold period (context only, not used for signal)
        nifty_exit = float(vix_idx.loc[exit_date, "nifty_spot"]) \
                     if exit_date in vix_idx.index else 0.0
        nifty_ret  = round((nifty_exit - nifty_spot) / nifty_spot * 100, 4) \
                     if nifty_spot > 0 and nifty_exit > 0 else 0.0

        # Buyer P&L = exit - entry; Seller P&L = entry - exit (collected premium)
        gross_pnl = exit_price - entry_price if strategy == "buy" \
                    else entry_price - exit_price
        cost      = transaction_cost_per_unit(entry_price, exit_price, strategy)
        pnl       = round(gross_pnl - cost, 2)
        pnl_pct   = round(pnl / entry_price * 100, 4) if entry_price > 0 else 0.0

        signals.append({
            "signal_date":     today,
            "expiry":          near_expiry,
            "nifty_spot":      nifty_spot,
            "vix":             vix,
            "vix_rank":        round(vix_rank, 2),
            "pcr":             pcr,
            "bias":            bias,
            "strategy":        strategy,
            "atm_strike":      atm_strike,
            "entry_type":      opt_type,
            "entry_price":     entry_price,
            "exit_date":       exit_date,
            "exit_price":      exit_price,
            "nifty_return_3d": nifty_ret,
            "pnl":             pnl,
            "pnl_pct":         pnl_pct,
            "hit":             int(pnl > 0),
            "version":         int(datetime.now().timestamp()),
        })

    return signals


# ══════════════════════════════════════════════════════
# Insert + Summary
# ══════════════════════════════════════════════════════

def insert_results(ch, signals: list[dict]):
    if not signals:
        return
    df = pd.DataFrame(signals)
    # Drop version before checking cols (added for insert)
    cols = [c for c in df.columns if c != "version"]
    df = df[cols + ["version"]]
    ch.insert_df("analysis.option_backtest_results", df)
    log.info(f"Inserted {len(signals)} rows → analysis.option_backtest_results")


def print_summary(signals: list[dict], from_date: date, to_date: date,
                  strategy: str = "buy"):
    if not signals:
        print("\nNo signals generated — check VIX data and bhavcopy downloads.")
        return

    df = pd.DataFrame(signals)

    total    = len(df)
    bull     = df[df["bias"] == "bullish"]
    bear     = df[df["bias"] == "bearish"]
    win_rate = df["hit"].mean() * 100
    avg_pnl  = df["pnl"].mean()
    avg_pct  = df["pnl_pct"].mean()

    # Sharpe: annualised on per-trade P&L % returns
    # (assumes ~252 trading signals per year as base — scale by actual density)
    signals_per_year = total / max(1, (to_date - from_date).days / 365)
    pnl_std  = df["pnl_pct"].std()
    sharpe   = (avg_pct / pnl_std * (signals_per_year ** 0.5)) \
               if pnl_std > 0 else 0.0

    # Max drawdown on cumulative P&L
    cumulative  = df["pnl"].cumsum()
    rolling_max = cumulative.cummax()
    drawdown    = (cumulative - rolling_max)
    max_dd      = drawdown.min()

    # Max consecutive losses
    runs   = (df["hit"] == 0).astype(int)
    consec = max(
        (sum(1 for _ in g) for k, g in
         __import__("itertools").groupby(runs) if k == 1),
        default=0
    )

    best  = df.loc[df["pnl"].idxmax()]
    worst = df.loc[df["pnl"].idxmin()]

    w = 58
    action = "BUY ATM (CE/PE)" if strategy == "buy" else "SELL ATM (PE/CE)"
    print(f"\n{'═'*w}")
    print(f"  OPTION STRATEGY BACKTEST RESULTS")
    print(f"  Strategy : {action}  [P&L after transaction costs]")
    print(f"  Period   : {from_date} → {to_date}")
    print(f"{'─'*w}")
    print(f"  Total signals      : {total:>6}  "
          f"({total / max(1,(to_date-from_date).days)*100:.0f}% of calendar days)")
    print(f"  → Bullish signals  : {len(bull):>6}  "
          f"({len(bull)/total*100:.0f}%)")
    print(f"  → Bearish signals  : {len(bear):>6}  "
          f"({len(bear)/total*100:.0f}%)")
    print(f"{'─'*w}")
    print(f"  DIRECTIONAL ACCURACY (hold = 3 trading days)")
    print(f"  Overall win rate   : {win_rate:>6.1f}%")
    if not bull.empty:
        print(f"  Bullish win rate   : {bull['hit'].mean()*100:>6.1f}%  "
              f"({bull['hit'].sum()}/{len(bull)})")
    if not bear.empty:
        print(f"  Bearish win rate   : {bear['hit'].mean()*100:>6.1f}%  "
              f"({bear['hit'].sum()}/{len(bear)})")
    print(f"{'─'*w}")
    print(f"  P&L SUMMARY  (entry = ATM option close, 1 lot = 1 unit)")
    print(f"  Average P&L / trade: ₹{avg_pnl:>+8.2f}")
    print(f"  Average P&L %      : {avg_pct:>+7.1f}%")
    print(f"  Total P&L (×1 unit): ₹{df['pnl'].sum():>+10.2f}")
    print(f"  Best trade         : ₹{best['pnl']:>+8.2f}  "
          f"({best['signal_date']} {best['bias']})")
    print(f"  Worst trade        : ₹{worst['pnl']:>+8.2f}  "
          f"({worst['signal_date']} {worst['bias']})")
    print(f"{'─'*w}")
    print(f"  RISK METRICS")
    print(f"  Sharpe ratio       : {sharpe:>+7.2f}  (annualised, per-trade)")
    print(f"  Max drawdown       : ₹{max_dd:>+8.2f}  (cumulative)")
    print(f"  Max consec. losses : {consec:>6}")
    print(f"{'─'*w}")
    print(f"  NIFTY CONTEXT (avg Nifty 3d return on signal days)")
    print(f"  Avg Nifty 3d return: {df['nifty_return_3d'].mean():>+7.2f}%")
    if not bull.empty:
        print(f"  Bull signals avg   : {bull['nifty_return_3d'].mean():>+7.2f}%  Nifty move")
    if not bear.empty:
        print(f"  Bear signals avg   : {bear['nifty_return_3d'].mean():>+7.2f}%  Nifty move")
    print(f"{'═'*w}\n")


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Option Strategy Backtest — historical PCR+VIX signal replay"
    )
    parser.add_argument("--days",    type=int, default=LOOKBACK_DAYS,
                        help=f"Lookback in calendar days (default: {LOOKBACK_DAYS})")
    parser.add_argument("--from",    dest="from_date", type=str, default=None,
                        help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--to",      dest="to_date",   type=str, default=None,
                        help="End date YYYY-MM-DD (default: today-3)")
    parser.add_argument("--strategy", choices=["buy", "sell"], default="buy",
                        help="buy = long ATM option; sell = short ATM option (default: buy)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and print without inserting to DB")
    args = parser.parse_args()

    today    = date.today()
    to_date  = (date.fromisoformat(args.to_date)
                if args.to_date else today - timedelta(days=3))
    from_date = (date.fromisoformat(args.from_date)
                 if args.from_date
                 else to_date - timedelta(days=args.days))

    log.info("═" * 55)
    log.info("  OPTION STRATEGY BACKTEST")
    log.info(f"  Period  : {from_date} → {to_date}")
    log.info(f"  Strategy: {args.strategy.upper()}")
    log.info(f"  Mode    : {'DRY RUN' if args.dry_run else 'LIVE (will insert to DB)'}")
    log.info("═" * 55)

    # ── Step 1: Fetch VIX from DB ────────────────────────
    log.info("Step 1/3: Fetching VIX + Nifty from market.nifty_live...")
    ch      = get_ch_client()
    vix_df  = fetch_vix_series(ch, from_date, to_date)

    if vix_df.empty:
        log.error(
            "market.nifty_live is empty!\n"
            "  Run first: docker compose run --rm vix_pipeline --full\n"
            "  Then retry this backtest."
        )
        sys.exit(1)

    log.info(f"VIX data loaded: {len(vix_df)} dates "
             f"({vix_df['date'].min()} → {vix_df['date'].max()})")

    # ── Step 2: Download bhavcopy files ─────────────────
    # Build candidate dates (all calendar days — weekends/holidays = 404 skip)
    all_dates = [from_date + timedelta(days=i)
                 for i in range((to_date - from_date).days + 4)]

    log.info(f"Step 2/3: Downloading {len(all_dates)} bhavcopy files "
             f"(with {MAX_WORKERS} workers)...")
    log.info("  Holidays and weekends return 404 and are skipped automatically.")

    bhavcopy_cache = download_bhavcopy_range(all_dates)

    if not bhavcopy_cache:
        log.error("No bhavcopy data downloaded. Check network / NSE archives.")
        sys.exit(1)

    trading_days = sorted(bhavcopy_cache.keys())
    log.info(f"Bhavcopy loaded: {len(bhavcopy_cache)} trading days "
             f"({trading_days[0]} → {trading_days[-1]})")

    # ── Step 3: Run backtest ─────────────────────────────
    log.info("Step 3/3: Replaying signals (no-lookahead)...")
    signals = run_backtest(vix_df, bhavcopy_cache, strategy=args.strategy)
    log.info(f"Signals generated: {len(signals)}")

    if not args.dry_run and signals:
        insert_results(ch, signals)

    print_summary(signals, from_date, to_date, strategy=args.strategy)

    if args.dry_run:
        log.info("[DRY RUN] Results not inserted to DB.")


if __name__ == "__main__":
    main()
