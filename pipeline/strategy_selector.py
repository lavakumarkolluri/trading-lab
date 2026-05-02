#!/usr/bin/env python3
"""
strategy_selector.py
────────────────────
Step 5 — picks the best defined-risk strategy for each expiry day, sizes
the position, and (optionally) runs a compounding backtest simulation.

Modes
-----
  python strategy_selector.py --recommend   # today's recommendation (daily run)
  python strategy_selector.py --backtest    # historical compounding simulation

Expiry schedule (NSE weekly options)
-------------------------------------
  Monday    → MIDCPNIFTY
  Tuesday   → FINNIFTY
  Wednesday → BANKNIFTY
  Thursday  → NIFTY

Only the symbol whose expiry falls on/after today is recommended on a given day.
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta

import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── ClickHouse ────────────────────────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

# ── Strategy parameters ───────────────────────────────────────────────────────
CONFIDENCE_THRESHOLD = 55.0     # minimum confidence to trade
INITIAL_CAPITAL     = 500_000.0 # ₹5 lakh starting capital
CAPITAL_FLOOR       = 50_000.0  # halt if capital falls below this
MAX_RISK_PCT        = 0.02      # 2% of capital per trade max loss

# PCR thresholds for directional bias
PCR_BULLISH = 1.2   # PCR > this → bullish sentiment → bull_put preferred
PCR_BEARISH = 0.8   # PCR < this → bearish sentiment → bear_call preferred

# Weekday → symbol mapping (0=Mon … 6=Sun)
WEEKDAY_SYMBOL = {
    0: "MIDCPNIFTY",
    1: "FINNIFTY",
    2: "BANKNIFTY",
    3: "NIFTY",
}


def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_lot_size(ch, symbol: str) -> int:
    rows = ch.query(
        "SELECT lot_size FROM market.fo_lot_sizes WHERE symbol = {symbol:String} "
        "ORDER BY effective_date DESC LIMIT 1",
        parameters={"symbol": symbol},
    ).result_rows
    return int(rows[0][0]) if rows else 75


def get_current_capital(ch) -> float:
    """Return latest capital_after from strategy_simulation, or INITIAL_CAPITAL if no history."""
    rows = ch.query(
        "SELECT capital_after FROM analysis.strategy_simulation FINAL "
        "ORDER BY sim_date DESC LIMIT 1"
    ).result_rows
    return float(rows[0][0]) if rows else INITIAL_CAPITAL


def get_confidence(ch, score_date: date, symbol: str):
    """Returns (confidence, pcr_bias, iv_skew, next_expiry) or None.

    Looks back up to 7 days to handle the case where scoring ran Sunday
    and recommendation is requested Mon–Thu.
    """
    rows = ch.query(
        "SELECT confidence, next_expiry, features_json "
        "FROM analysis.confidence_scores FINAL "
        "WHERE score_date >= {d:Date} - INTERVAL 7 DAY "
        "  AND score_date <= {d:Date} "
        "  AND symbol = {sym:String} "
        "ORDER BY score_date DESC, version DESC LIMIT 1",
        parameters={"d": score_date, "sym": symbol},
    ).result_rows
    if not rows:
        return None
    conf, expiry, features_json = rows[0]
    feats = json.loads(features_json) if features_json else {}
    pcr_bias = feats.get("pcr_oi", 1.0)
    iv_skew  = feats.get("iv_skew", 0.0)
    return conf, pcr_bias, iv_skew, expiry


def pick_strategy(confidence: float, pcr_bias: float, iv_skew: float) -> str:
    """Choose spread type from confidence + market bias."""
    if pcr_bias > PCR_BULLISH:
        return "bull_put"
    if pcr_bias < PCR_BEARISH:
        return "bear_call"
    return "iron_condor"


def get_optimal_params(ch, symbol: str, strategy: str):
    """Best (short_n, wing_m) by Sharpe from spread_optimal. Returns None if missing."""
    rows = ch.query(
        "SELECT short_n, wing_m, sharpe_pct, avg_net_credit, avg_max_loss "
        "FROM analysis.spread_optimal "
        "WHERE symbol = {sym:String} AND strategy = {strat:String} "
        "  AND n_trades >= 10 "
        "ORDER BY sharpe_pct DESC LIMIT 1",
        parameters={"sym": symbol, "strat": strategy},
    ).result_rows
    if not rows:
        return None
    sn, wm, sharpe, avg_credit, avg_max_loss = rows[0]
    return {"short_n": sn, "wing_m": wm, "sharpe": sharpe,
            "avg_net_credit": avg_credit, "avg_max_loss": avg_max_loss}


def get_latest_chain_snapshot(ch, symbol: str, expiry: date):
    """Return latest date with chain data for this symbol/expiry."""
    rows = ch.query(
        "SELECT max(toDate(timestamp)) FROM market.options_chain "
        "WHERE symbol = {sym:String} AND expiry = {exp:Date} AND ltp > 0.05",
        parameters={"sym": symbol, "exp": expiry},
    ).result_rows
    return rows[0][0] if rows and rows[0][0] else None


def get_chain_prices(ch, symbol: str, snap_date: date, expiry: date):
    """Return {(strike, option_type): ltp} for this snap."""
    rows = ch.query(
        "SELECT strike, option_type, ltp "
        "FROM market.options_chain "
        "WHERE symbol = {sym:String} AND toDate(timestamp) = {sd:Date} "
        "  AND expiry = {exp:Date} AND ltp > 0.05",
        parameters={"sym": symbol, "sd": snap_date, "exp": expiry},
    ).result_rows
    return {(float(r[0]), r[1]): float(r[2]) for r in rows}


def find_atm(chain_idx: dict, expiry: date):
    """Find ATM strike by minimising |CE_ltp - PE_ltp|."""
    ce_strikes = {s for (s, ot) in chain_idx if ot == "CE"}
    pe_strikes = {s for (s, ot) in chain_idx if ot == "PE"}
    common = sorted(ce_strikes & pe_strikes)
    if not common:
        return None
    best_strike, best_diff = None, float("inf")
    for s in common:
        diff = abs(chain_idx[(s, "CE")] - chain_idx[(s, "PE")])
        if diff < best_diff:
            best_diff = diff
            best_strike = s
    return best_strike


def get_strike_step(ch, symbol: str, expiry: date) -> float:
    rows = ch.query(
        "SELECT strike FROM market.options_chain "
        "WHERE symbol = {sym:String} AND expiry = {exp:Date} "
        "  AND option_type = 'CE' AND ltp > 0.05 "
        "ORDER BY strike LIMIT 30",
        parameters={"sym": symbol, "exp": expiry},
    ).result_rows
    if len(rows) < 2:
        return {"NIFTY": 50.0, "BANKNIFTY": 100.0, "FINNIFTY": 50.0, "MIDCPNIFTY": 25.0}.get(symbol, 50.0)
    strikes = sorted(set(float(r[0]) for r in rows))
    diffs = [strikes[i+1] - strikes[i] for i in range(len(strikes)-1) if strikes[i+1] > strikes[i]]
    from statistics import mode
    try:
        return float(mode(diffs))
    except Exception:
        return diffs[0] if diffs else 50.0


def build_legs(strategy: str, atm: float, step: float, short_n: int, wing_m: int):
    """Return (short_ce, short_pe, long_ce, long_pe) strikes."""
    short_ce = atm + short_n * step
    short_pe = atm - short_n * step
    if strategy in ("iron_condor", "bear_call"):
        long_ce = short_ce + wing_m * step
    else:
        long_ce = 0.0
    if strategy in ("iron_condor", "bull_put"):
        long_pe = short_pe - wing_m * step
    else:
        long_pe = 0.0
    return short_ce, short_pe, long_ce, long_pe


def price_leg(idx: dict, strike: float, ot: str) -> float:
    return idx.get((strike, ot), 0.0)


def build_recommendation(
    rec_date: date, symbol: str, expiry: date, ch,
    confidence: float, pcr_bias: float, iv_skew: float,
    params: dict, lots: int, lot_size: int,
):
    """Assemble full recommendation dict with leg prices."""
    snap_date = get_latest_chain_snapshot(ch, symbol, expiry)
    if snap_date is None:
        return None

    step = get_strike_step(ch, symbol, expiry)
    idx  = get_chain_prices(ch, symbol, snap_date, expiry)
    if not idx:
        return None

    atm = find_atm(idx, expiry)
    if atm is None:
        return None

    strategy = pick_strategy(confidence, pcr_bias, iv_skew)
    sn = params["short_n"]
    wm = params["wing_m"]
    sce, spe, lce, lpe = build_legs(strategy, atm, step, sn, wm)

    sce_price = price_leg(idx, sce, "CE")
    spe_price = price_leg(idx, spe, "PE")
    lce_price = price_leg(idx, lce, "CE") if lce else 0.0
    lpe_price = price_leg(idx, lpe, "PE") if lpe else 0.0

    net_credit = (sce_price + spe_price) - (lce_price + lpe_price)
    max_loss   = wm * step - net_credit if wm > 0 else 0.0
    capital_at_risk = max_loss * lots * lot_size

    return dict(
        rec_date=rec_date, symbol=symbol, expiry=expiry,
        strategy=strategy, short_n=sn, wing_m=wm,
        strike_step=step, atm_strike=atm,
        short_ce_strike=sce, short_pe_strike=spe,
        long_ce_strike=lce, long_pe_strike=lpe,
        short_ce_entry=sce_price, short_pe_entry=spe_price,
        long_ce_entry=lce_price, long_pe_entry=lpe_price,
        net_credit=net_credit, max_loss=max_loss,
        lots=lots, capital_at_risk=capital_at_risk,
        confidence=confidence, pcr_bias=pcr_bias, iv_skew=iv_skew,
        pnl_pts=0.0, pnl_amount=0.0, outcome="pending",
    )


def insert_recommendation(ch, rec: dict):
    ch.insert(
        "analysis.trade_recommendations",
        [[
            rec["rec_date"], rec["symbol"], rec["expiry"],
            rec["strategy"], rec["short_n"], rec["wing_m"],
            rec["strike_step"], rec["atm_strike"],
            rec["short_ce_strike"], rec["short_pe_strike"],
            rec["long_ce_strike"], rec["long_pe_strike"],
            rec["short_ce_entry"], rec["short_pe_entry"],
            rec["long_ce_entry"], rec["long_pe_entry"],
            rec["net_credit"], rec["max_loss"],
            rec["lots"], rec["capital_at_risk"],
            rec["confidence"], rec["pcr_bias"], rec["iv_skew"],
            rec["pnl_pts"], rec["pnl_amount"], rec["outcome"],
        ]],
        column_names=[
            "rec_date", "symbol", "expiry",
            "strategy", "short_n", "wing_m",
            "strike_step", "atm_strike",
            "short_ce_strike", "short_pe_strike",
            "long_ce_strike", "long_pe_strike",
            "short_ce_entry", "short_pe_entry",
            "long_ce_entry", "long_pe_entry",
            "net_credit", "max_loss",
            "lots", "capital_at_risk",
            "confidence", "pcr_bias", "iv_skew",
            "pnl_pts", "pnl_amount", "outcome",
        ],
    )


# ── Modes ─────────────────────────────────────────────────────────────────────

def is_trading_holiday(ch, d: date) -> bool:
    rows = ch.query(
        "SELECT 1 FROM market.trading_holidays "
        "WHERE holiday_date = {d:Date} LIMIT 1",
        parameters={"d": d},
    ).result_rows
    return len(rows) > 0


def run_recommend(ch):
    today = date.today()
    weekday = today.weekday()

    if weekday not in WEEKDAY_SYMBOL:
        log.info("No expiry today (weekday=%d). Nothing to recommend.", weekday)
        return

    if is_trading_holiday(ch, today):
        log.info("Today %s is a trading holiday — no recommendation.", today)
        return

    symbol = WEEKDAY_SYMBOL[weekday]
    log.info("Recommending for %s expiry on %s", symbol, today)

    result = get_confidence(ch, today, symbol)
    if result is None:
        log.warning("No confidence score for %s on %s — skipping", symbol, today)
        return

    confidence, pcr_bias, iv_skew, expiry = result

    if confidence < CONFIDENCE_THRESHOLD:
        log.info("Confidence %.1f < threshold %.1f — no trade for %s",
                 confidence, CONFIDENCE_THRESHOLD, symbol)
        return

    strategy = pick_strategy(confidence, pcr_bias, iv_skew)
    params   = get_optimal_params(ch, symbol, strategy)
    if params is None:
        log.warning("No optimal params for %s/%s — skipping", symbol, strategy)
        return

    lot_size     = get_lot_size(ch, symbol)
    max_loss     = params["avg_max_loss"]
    capital      = get_current_capital(ch)
    risk_budget  = capital * MAX_RISK_PCT
    lots = max(1, int(risk_budget / (max_loss * lot_size))) if max_loss > 0 else 1
    log.info("Current capital: ₹%.0f | risk budget: ₹%.0f", capital, risk_budget)

    rec = build_recommendation(
        today, symbol, expiry, ch,
        confidence, pcr_bias, iv_skew,
        params, lots, lot_size,
    )

    if rec is None:
        log.warning("Could not price legs for %s/%s — skipping", symbol, expiry)
        return

    insert_recommendation(ch, rec)
    log.info(
        "Recommendation: %s %s | strategy=%s sn=%d wm=%d | "
        "net_credit=%.2f max_loss=%.2f lots=%d | confidence=%.1f",
        symbol, expiry, strategy, params["short_n"], params["wing_m"],
        rec["net_credit"], rec["max_loss"], lots, confidence,
    )


def run_backtest(ch):
    """Compounding simulation over historical OOS predictions."""
    log.info("Running compounding backtest simulation")

    # Load OOS confidence predictions joined with actual spread_backtest outcomes
    rows = ch.query("""
        SELECT
            cb.expiry,
            cb.symbol,
            cb.entry_date,
            cb.confidence,
            cb.pnl_pts   AS straddle_pnl,
            so.strategy,
            so.short_n,
            so.wing_m,
            sb.pnl_pts   AS spread_pnl,
            sb.max_loss
        FROM analysis.confidence_backtest FINAL cb
        -- pick best strategy params for each symbol
        JOIN (
            SELECT symbol, strategy, short_n, wing_m, sharpe_pct,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY sharpe_pct DESC) AS rn
            FROM analysis.spread_optimal FINAL
            WHERE n_trades >= 10
        ) so ON so.symbol = cb.symbol AND so.rn = 1
        -- get actual P&L for those params
        LEFT JOIN analysis.spread_backtest FINAL sb
            ON sb.symbol = cb.symbol
           AND sb.expiry = cb.expiry
           AND sb.strategy = so.strategy
           AND sb.short_n  = so.short_n
           AND sb.wing_m   = so.wing_m
        ORDER BY cb.entry_date
    """).result_rows

    if not rows:
        log.warning("No OOS data to simulate")
        return

    capital = INITIAL_CAPITAL
    sim_rows = []

    # Cache lot sizes to avoid one DB round-trip per row
    symbols_seen = {r[1] for r in rows}
    lot_size_cache = {sym: get_lot_size(ch, sym) for sym in symbols_seen}

    for (expiry, symbol, entry_date, confidence, straddle_pnl,
         strategy, short_n, wing_m, spread_pnl, max_loss) in rows:

        confidence_pct = float(confidence) * 100 if float(confidence) <= 1.0 else float(confidence)
        lot_size = lot_size_cache[symbol]

        skipped = 0
        pnl_pts = 0.0
        pnl_amount = 0.0
        lots = 1

        if confidence_pct < CONFIDENCE_THRESHOLD:
            skipped = 1
        else:
            if max_loss and float(max_loss) > 0:
                risk_budget = capital * MAX_RISK_PCT
                lots = max(1, int(risk_budget / (float(max_loss) * lot_size)))
            pnl_pts   = float(spread_pnl or straddle_pnl or 0.0)
            pnl_amount = pnl_pts * lots * lot_size

        capital_before = capital
        capital += pnl_amount
        capital = max(capital, 0)

        sim_rows.append([
            entry_date, symbol, expiry,
            strategy or "straddle", short_n or 0, wing_m or 0,
            confidence_pct, lots, lot_size,
            pnl_pts, pnl_amount,
            capital_before, capital,
            skipped,
        ])

        if capital < CAPITAL_FLOOR:
            log.warning("Capital %.0f fell below floor on %s — halting simulation", capital, entry_date)
            break

    if sim_rows:
        # Truncate before re-insert so re-running --backtest doesn't append duplicates.
        # ReplacingMergeTree deduplication is async and not guaranteed before next read.
        ch.command("TRUNCATE TABLE analysis.strategy_simulation")
        ch.insert(
            "analysis.strategy_simulation",
            sim_rows,
            column_names=[
                "sim_date", "symbol", "expiry",
                "strategy", "short_n", "wing_m",
                "confidence", "lots", "lot_size",
                "pnl_pts", "pnl_amount",
                "capital_before", "capital_after",
                "skipped",
            ],
        )
        final_capital = sim_rows[-1][12]
        n_traded = sum(1 for r in sim_rows if not r[13])
        n_skipped = sum(1 for r in sim_rows if r[13])
        log.info(
            "Simulation complete: %d trades, %d skipped | "
            "capital ₹%.0f → ₹%.0f (%.1f%%)",
            n_traded, n_skipped,
            INITIAL_CAPITAL, final_capital,
            (final_capital / INITIAL_CAPITAL - 1) * 100,
        )


def run_fill_outcomes(ch):
    """
    Fill pnl_pts/outcome on past trade_recommendations where outcome='pending'
    and the expiry has already passed.  Matches against spread_backtest on
    (symbol, expiry, strategy, short_n, wing_m).
    """
    rows = ch.query("""
        SELECT rec_date, symbol, expiry, strategy, short_n, wing_m
        FROM analysis.trade_recommendations FINAL
        WHERE outcome = 'pending' AND expiry < today()
    """).result_rows

    if not rows:
        log.info("No pending recommendations to fill.")
        return

    filled = 0
    for rec_date, symbol, expiry, strategy, short_n, wing_m in rows:
        res = ch.query(
            "SELECT pnl_pts, target FROM analysis.spread_backtest FINAL "
            "WHERE symbol = {sym:String} AND expiry = {exp:Date} "
            "  AND strategy = {strat:String} "
            "  AND short_n = {sn:UInt8} AND wing_m = {wm:UInt8} "
            "LIMIT 1",
            parameters={
                "sym": symbol, "exp": expiry,
                "strat": strategy, "sn": short_n, "wm": wing_m,
            },
        ).result_rows

        if not res:
            continue

        pnl_pts, target = res[0]
        # Fetch lot size + lots from the existing recommendation
        lot_res = ch.query(
            "SELECT lots FROM analysis.trade_recommendations FINAL "
            "WHERE rec_date = {rd:Date} AND symbol = {sym:String} LIMIT 1",
            parameters={"rd": rec_date, "sym": symbol},
        ).result_rows
        lots = int(lot_res[0][0]) if lot_res else 1
        lot_size = get_lot_size(ch, symbol)
        pnl_amount = float(pnl_pts) * lots * lot_size
        outcome = "win" if target else "loss"

        # ReplacingMergeTree: re-insert with updated fields + new version
        ch.command(
            "ALTER TABLE analysis.trade_recommendations UPDATE "
            "pnl_pts = {pnl_pts:Float32}, "
            "pnl_amount = {pnl_amount:Float32}, "
            "outcome = {outcome:String}, "
            "version = toUnixTimestamp(now()) "
            "WHERE rec_date = {rd:Date} AND symbol = {sym:String}",
            parameters={
                "pnl_pts": float(pnl_pts), "pnl_amount": pnl_amount,
                "outcome": outcome, "rd": rec_date, "sym": symbol,
            },
        )
        log.info("Filled %s %s %s: pnl=%.2f pts (%.0f ₹) → %s",
                 rec_date, symbol, expiry, pnl_pts, pnl_amount, outcome)
        filled += 1

    log.info("fill_outcomes: updated %d/%d recommendations", filled, len(rows))


def main():
    parser = argparse.ArgumentParser()
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--recommend",     action="store_true")
    grp.add_argument("--backtest",      action="store_true")
    grp.add_argument("--fill-outcomes", action="store_true")
    args = parser.parse_args()

    ch = ch_client()

    if args.recommend:
        run_recommend(ch)
    elif args.backtest:
        run_backtest(ch)
    else:
        run_fill_outcomes(ch)


if __name__ == "__main__":
    main()
