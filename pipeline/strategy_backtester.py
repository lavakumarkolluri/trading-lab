#!/usr/bin/env python3
"""
strategy_backtester.py — Defined-risk option selling strategy backtester

Strategies: Iron Condor, Bull Put Spread, Bear Call Spread
All positions are hedged — no naked legs.

For each symbol × expiry × strategy variant (short_n, wing_m):
  Entry: previous trading day EOD prices (approximates 0DTE morning entry)
  Exit:  expiry day EOD settlement prices

P&L accounting (all hedge costs included):
  net_credit = sell_legs - buy_legs   (what you receive after paying for wings)
  exit_cost  = settle(sell_legs) - settle(buy_legs)
  pnl_pts    = net_credit - exit_cost
  max_loss   = wing_width - net_credit   (defined risk)

Parameter grid:
  short_n : 1–4  steps from ATM to place short leg
  wing_m  : 1–4  steps from short to protective long leg

Usage:
  python strategy_backtester.py                    # all symbols
  python strategy_backtester.py --symbol NIFTY
  python strategy_backtester.py --symbol NIFTY --from 2025-01-01
"""

import os
import logging
import argparse
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GIT_SHA = os.getenv("GIT_SHA", "unknown")


def _record_run(ch, status: str, started_at: datetime, error_msg: str = ""):
    try:
        ch.command(
            """INSERT INTO system_meta.pipeline_runs
               (service, started_at, finished_at, status, git_sha, error_msg)
               VALUES ({svc:String},{start:DateTime},{end:DateTime},{st:String},{sha:String},{err:String})""",
            parameters={"svc": "strategy_backtester", "start": started_at,
                        "end": datetime.utcnow(), "st": status,
                        "sha": GIT_SHA, "err": error_msg},
        )
    except Exception as e:
        log.warning("pipeline_runs write failed: %s", e)


CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

SYMBOLS     = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
SHORT_N_MAX = 4   # max steps from ATM for short leg
WING_M_MAX  = 4   # max wing width in steps


# ── DB ────────────────────────────────────────────────────────────────────────

def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_chain(ch, symbol: str, from_date: Optional[date] = None) -> pd.DataFrame:
    """Load full options chain for a symbol (all strikes, both dates: prev + expiry)."""
    date_filter = f"AND toDate(timestamp) >= '{from_date}'" if from_date else ""
    r = ch.query(f"""
        SELECT toDate(timestamp) AS snap_date,
               expiry, strike, option_type, ltp, oi
        FROM market.options_chain FINAL
        WHERE symbol = '{symbol}'
          AND ltp > 0.05
          {date_filter}
        ORDER BY snap_date, expiry, strike, option_type
    """)
    df = pd.DataFrame(r.result_rows,
                      columns=["snap_date", "expiry", "strike", "option_type", "ltp", "oi"])
    df["snap_date"] = pd.to_datetime(df["snap_date"]).dt.date
    df["expiry"]    = pd.to_datetime(df["expiry"]).dt.date
    return df


def build_index(chain: pd.DataFrame) -> dict:
    """
    Build O(1) price lookup dict.
    Key: (snap_date, expiry, strike, option_type) → ltp
    Also build per-(snap_date, expiry) CE/PE series for ATM detection.
    """
    idx = {}
    for row in chain.itertuples(index=False):
        idx[(row.snap_date, row.expiry, row.strike, row.option_type)] = row.ltp
    return idx


def detect_strike_step(chain: pd.DataFrame, symbol: str, expiry: date) -> float:
    """Infer strike interval from traded CE strikes for this expiry."""
    snap = chain[
        (chain.expiry == expiry) & (chain.option_type == "CE") & (chain.oi > 1000)
    ].sort_values("strike")
    if len(snap) < 3:
        return {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25}.get(symbol, 50)
    diffs = snap["strike"].diff().dropna()
    diffs = diffs[diffs > 0]
    return float(diffs.mode().iloc[0]) if not diffs.empty else 50.0


# ── Strategy P&L ─────────────────────────────────────────────────────────────

def get_ltp(idx: dict, snap_date: date, expiry: date,
            strike: float, option_type: str) -> Optional[float]:
    return idx.get((snap_date, expiry, strike, option_type))


def find_atm(chain: pd.DataFrame, snap_date: date, expiry: date) -> Optional[float]:
    """ATM = strike where |CE_ltp - PE_ltp| is minimised (both > 0.5)."""
    snap = chain[(chain.snap_date == snap_date) & (chain.expiry == expiry)]
    ce = snap[snap.option_type == "CE"].set_index("strike")["ltp"]
    pe = snap[snap.option_type == "PE"].set_index("strike")["ltp"]
    common = ce.index.intersection(pe.index)
    common = common[(ce.loc[common] > 0.5) & (pe.loc[common] > 0.5)]
    if len(common) < 2:
        return None
    return float((ce.loc[common] - pe.loc[common]).abs().idxmin())



def compute_iron_condor(idx, entry_date, expiry, atm,
                        step, short_n, wing_m) -> Optional[dict]:
    """
    IC: sell (atm + short_n*step) CE + sell (atm - short_n*step) PE
        buy  (atm + (short_n+wing_m)*step) CE + buy (atm - (short_n+wing_m)*step) PE
    net_credit = sell legs - buy legs  (hedge cost already deducted)
    """
    sce_k = atm + short_n * step
    spe_k = atm - short_n * step
    lce_k = atm + (short_n + wing_m) * step
    lpe_k = atm - (short_n + wing_m) * step

    sce = get_ltp(idx, entry_date, expiry, sce_k, "CE")
    spe = get_ltp(idx, entry_date, expiry, spe_k, "PE")
    lce = get_ltp(idx, entry_date, expiry, lce_k, "CE")
    lpe = get_ltp(idx, entry_date, expiry, lpe_k, "PE")
    if any(p is None for p in [sce, spe, lce, lpe]):
        return None
    if any(p < 0.05 for p in [sce, spe, lce, lpe]):
        return None

    net_credit = (sce + spe) - (lce + lpe)
    if net_credit <= 0:
        return None  # debit structure — skip

    max_loss = wing_m * step - net_credit  # single-side worst case
    pnl_pct_denom = wing_m * step

    # Settlement — all 4 legs must be present; missing settlement = skip trade
    xsce = get_ltp(idx, expiry, expiry, sce_k, "CE")
    xspe = get_ltp(idx, expiry, expiry, spe_k, "PE")
    xlce = get_ltp(idx, expiry, expiry, lce_k, "CE")
    xlpe = get_ltp(idx, expiry, expiry, lpe_k, "PE")
    if any(v is None for v in (xsce, xspe, xlce, xlpe)):
        return None
    exit_cost = (xsce + xspe) - (xlce + xlpe)
    # Cap at theoretical max: bhavcopy LTP ≠ final settlement in edge cases
    pnl = max(net_credit - exit_cost, -max_loss) if max_loss > 0 else net_credit - exit_cost

    return dict(
        strategy="iron_condor", short_n=short_n, wing_m=wing_m,
        short_ce_strike=sce_k, short_pe_strike=spe_k,
        long_ce_strike=lce_k, long_pe_strike=lpe_k,
        short_ce_entry=sce, short_pe_entry=spe,
        long_ce_entry=lce, long_pe_entry=lpe,
        short_ce_settle=xsce, short_pe_settle=xspe,
        long_ce_settle=xlce, long_pe_settle=xlpe,
        net_credit=net_credit, max_loss=max_loss,
        exit_cost=exit_cost, pnl_pts=pnl,
        pnl_pct=pnl / pnl_pct_denom * 100,
        target=int(pnl > 0),
    )


def compute_bull_put(idx, entry_date, expiry, atm,
                     step, short_n, wing_m) -> Optional[dict]:
    """
    Bull Put Spread (bullish): sell (atm - short_n*step) PE, buy (atm - (short_n+wing_m)*step) PE
    Profits if spot stays above the short PE strike.
    Hedge cost (long PE) is deducted from net_credit.
    """
    spe_k = atm - short_n * step
    lpe_k = atm - (short_n + wing_m) * step

    spe = get_ltp(idx, entry_date, expiry, spe_k, "PE")
    lpe = get_ltp(idx, entry_date, expiry, lpe_k, "PE")
    if spe is None or lpe is None:
        return None
    if spe < 0.05 or lpe < 0.05:
        return None

    net_credit = spe - lpe   # hedge cost deducted
    if net_credit <= 0:
        return None
    max_loss = wing_m * step - net_credit

    xspe = get_ltp(idx, expiry, expiry, spe_k, "PE")
    xlpe = get_ltp(idx, expiry, expiry, lpe_k, "PE")
    if xspe is None or xlpe is None:
        return None
    exit_cost = xspe - xlpe
    pnl = max(net_credit - exit_cost, -max_loss) if max_loss > 0 else net_credit - exit_cost

    return dict(
        strategy="bull_put", short_n=short_n, wing_m=wing_m,
        short_ce_strike=0.0, short_pe_strike=spe_k,
        long_ce_strike=0.0, long_pe_strike=lpe_k,
        short_ce_entry=0.0, short_pe_entry=spe,
        long_ce_entry=0.0, long_pe_entry=lpe,
        short_ce_settle=0.0, short_pe_settle=xspe,
        long_ce_settle=0.0, long_pe_settle=xlpe,
        net_credit=net_credit, max_loss=max_loss,
        exit_cost=exit_cost, pnl_pts=pnl,
        pnl_pct=pnl / (wing_m * step) * 100,
        target=int(pnl > 0),
    )


def compute_bear_call(idx, entry_date, expiry, atm,
                      step, short_n, wing_m) -> Optional[dict]:
    """
    Bear Call Spread (bearish): sell (atm + short_n*step) CE, buy (atm + (short_n+wing_m)*step) CE
    Profits if spot stays below the short CE strike.
    Hedge cost (long CE) is deducted from net_credit.
    """
    sce_k = atm + short_n * step
    lce_k = atm + (short_n + wing_m) * step

    sce = get_ltp(idx, entry_date, expiry, sce_k, "CE")
    lce = get_ltp(idx, entry_date, expiry, lce_k, "CE")
    if sce is None or lce is None:
        return None
    if sce < 0.05 or lce < 0.05:
        return None

    net_credit = sce - lce
    if net_credit <= 0:
        return None
    max_loss = wing_m * step - net_credit

    xsce = get_ltp(idx, expiry, expiry, sce_k, "CE")
    xlce = get_ltp(idx, expiry, expiry, lce_k, "CE")
    if xsce is None or xlce is None:
        return None
    exit_cost = xsce - xlce
    pnl = max(net_credit - exit_cost, -max_loss) if max_loss > 0 else net_credit - exit_cost

    return dict(
        strategy="bear_call", short_n=short_n, wing_m=wing_m,
        short_ce_strike=sce_k, short_pe_strike=0.0,
        long_ce_strike=lce_k, long_pe_strike=0.0,
        short_ce_entry=sce, short_pe_entry=0.0,
        long_ce_entry=lce, long_pe_entry=0.0,
        short_ce_settle=xsce, short_pe_settle=0.0,
        long_ce_settle=xlce, long_pe_settle=0.0,
        net_credit=net_credit, max_loss=max_loss,
        exit_cost=exit_cost, pnl_pts=pnl,
        pnl_pct=pnl / (wing_m * step) * 100,
        target=int(pnl > 0),
    )


# ── Main Processing ───────────────────────────────────────────────────────────

def process_symbol(ch, symbol: str, from_date: Optional[date] = None) -> pd.DataFrame:
    log.info(f"[{symbol}] loading chain…")
    chain = load_chain(ch, symbol, from_date)

    # Build O(1) lookup index once
    idx = build_index(chain)

    expiries      = sorted(chain["expiry"].unique())
    expiry_snaps  = set(chain["snap_date"].unique())
    log.info(f"[{symbol}] {len(expiries)} expiry dates, building index done")

    rows = []
    for expiry in expiries:
        # Previous trading day (latest snap before expiry)
        candidates = sorted(chain[
            (chain.expiry == expiry) & (chain.snap_date < expiry)
        ]["snap_date"].unique())
        if not candidates:
            continue
        entry_date = candidates[-1]

        # Need expiry-day settlement data
        if expiry not in expiry_snaps:
            continue

        atm = find_atm(chain, entry_date, expiry)
        if atm is None:
            continue

        step = detect_strike_step(chain, symbol, expiry)
        base = dict(symbol=symbol, expiry=expiry, entry_date=entry_date,
                    atm_strike=atm, strike_step=step)

        # Hedged strategies only — no naked positions
        for sn in range(1, SHORT_N_MAX + 1):
            for wm in range(1, WING_M_MAX + 1):
                for fn in [compute_iron_condor, compute_bull_put, compute_bear_call]:
                    r = fn(idx, entry_date, expiry, atm, step, sn, wm)
                    if r:
                        rows.append({**base, **r})

    df = pd.DataFrame(rows)
    if df.empty:
        log.warning(f"[{symbol}] no valid trades found")
        return df

    log.info(f"[{symbol}] {len(df)} strategy×expiry combinations")
    return df


def insert_backtest(ch, df: pd.DataFrame):
    cols = [
        "symbol", "expiry", "entry_date", "strategy", "short_n", "wing_m",
        "strike_step", "atm_strike",
        "short_ce_strike", "short_pe_strike", "long_ce_strike", "long_pe_strike",
        "short_ce_entry", "short_pe_entry", "long_ce_entry", "long_pe_entry",
        "short_ce_settle", "short_pe_settle", "long_ce_settle", "long_pe_settle",
        "net_credit", "max_loss", "exit_cost", "pnl_pts", "pnl_pct", "target",
    ]
    df = df[cols].copy()
    df["pnl_pct"] = df["pnl_pct"].fillna(0.0)
    rows = [list(row) for row in df.itertuples(index=False)]
    ch.insert("analysis.spread_backtest", rows, column_names=cols)
    log.info(f"  inserted {len(rows)} rows into spread_backtest")


def compute_and_insert_optimal(ch, df: pd.DataFrame):
    """Compute summary stats per symbol × strategy × params and insert."""
    rows = []
    for (symbol, strategy, sn, wm), grp in df.groupby(
        ["symbol", "strategy", "short_n", "wing_m"], observed=True
    ):
        if len(grp) < 5:
            continue
        pnl_pts = grp["pnl_pts"].values
        pnl_pct = grp["pnl_pct"].replace(0, float("nan")).dropna().values
        step = float(grp["strike_step"].iloc[0])

        sharpe = float(np.mean(pnl_pts) / (np.std(pnl_pts) + 1e-9))
        avg_pnl_pct = float(np.nanmean(pnl_pct)) if len(pnl_pct) else 0.0
        avg_credit = float(grp["net_credit"].mean())
        avg_maxloss = float(grp["max_loss"].mean())
        p2r = avg_credit / (wm * step) * 100 if wm > 0 and step > 0 else 0.0

        rows.append([
            symbol, strategy, int(sn), int(wm), len(grp),
            float(grp["target"].mean()),
            float(grp["pnl_pts"].mean()),
            avg_pnl_pct, sharpe,
            avg_credit, avg_maxloss, p2r,
        ])

    ch.insert(
        "analysis.spread_optimal",
        rows,
        column_names=[
            "symbol", "strategy", "short_n", "wing_m", "n_trades",
            "win_rate", "avg_pnl_pts", "avg_pnl_pct", "sharpe_pct",
            "avg_net_credit", "avg_max_loss", "premium_to_risk",
        ],
    )
    log.info(f"  inserted {len(rows)} rows into spread_optimal")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Single symbol (default: all)")
    parser.add_argument("--from", dest="from_date",
                        help="Start date YYYY-MM-DD (default: all history)")
    args = parser.parse_args()

    ch = get_ch()
    from_date = date.fromisoformat(args.from_date) if args.from_date else None
    symbols   = [args.symbol] if args.symbol else SYMBOLS

    started_at = datetime.utcnow()
    try:
        all_frames = []
        for sym in symbols:
            try:
                df = process_symbol(ch, sym, from_date)
                if df.empty:
                    continue
                insert_backtest(ch, df)
                all_frames.append(df)
            except Exception as e:
                log.error(f"[{sym}] failed: {e}", exc_info=True)

        if all_frames:
            combined = pd.concat(all_frames, ignore_index=True)
            try:
                compute_and_insert_optimal(ch, combined)
            except Exception as e:
                log.error(f"optimal computation failed: {e}", exc_info=True)

        log.info("strategy_backtester done")
        _record_run(ch, "success", started_at)
    except Exception as e:
        log.error(f"strategy_backtester fatal: {e}", exc_info=True)
        _record_run(ch, "failed", started_at, str(e))


if __name__ == "__main__":
    main()
