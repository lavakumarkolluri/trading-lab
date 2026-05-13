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
CONFIDENCE_THRESHOLD = 55.0     # minimum confidence to trade (used in --backtest only)
TRADE_SCORE_THRESHOLD = 65.0    # composite score gate for --recommend
INITIAL_CAPITAL     = 500_000.0 # ₹5 lakh starting capital
CAPITAL_FLOOR       = 50_000.0  # halt if capital falls below this
MAX_RISK_PCT        = 0.02      # 2% of capital per trade max loss

# PCR thresholds for directional bias
PCR_BULLISH = 1.2   # PCR > this → bullish sentiment → bull_put preferred
PCR_BEARISH = 0.8   # PCR < this → bearish sentiment → bear_call preferred

# Phase 1 signal filters
VIX_MIN = 11.0          # skip if VIX too low (no premium worth collecting)
VIX_MAX = 28.0          # skip if VIX too high (tail-risk events)
IV_SKEW_THRESH = 2.0    # % — iv_skew = avg(PE_IV_OTM) − avg(CE_IV_OTM); +ve = fear/put premium

# Vol surface signal thresholds
VOL_SKEW_BULL_PUT_THRESH = 0.02   # skew_2pct > 2% → puts expensive → bias toward bull_put
TERM_BACKW_BONUS_MIN     = -0.04  # term_slope > -4% (not panic) → eligible for bonus pts
TERM_BACKW_BONUS_MAX     = -0.01  # term_slope < -1% → backwardation confirmed
TERM_BACKW_BONUS_PTS     = 3.0    # bonus added to trade_score in moderate backwardation

# Phase 2 trade_score weights (sum = 100)
SCORE_W_CONFIDENCE = 40   # confidence model quality
SCORE_W_VIX        = 20   # VIX regime: sweet spot 14-22 = full, 11-13/23-28 = half
SCORE_W_IV_RANK    = 20   # IV rank: higher = more premium to sell
SCORE_W_ALIGNMENT  = 20   # PCR + iv_skew agreement on direction
VIX_SWEET_LO       = 14.0
VIX_SWEET_HI       = 22.0

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

def get_vix(ch, snap_date: date) -> float | None:
    rows = ch.query(
        "SELECT vix FROM market.nifty_live FINAL "
        "WHERE toDate(timestamp) <= {d:Date} ORDER BY timestamp DESC LIMIT 1",
        parameters={"d": snap_date},
    ).result_rows
    return float(rows[0][0]) if rows and rows[0][0] else None


def get_vol_surface_signals(ch, symbol: str, snap_date: date) -> dict:
    """Fetch term_slope and skew from vol_term_structure for snap_date.
    Returns zeros if no surface data available (e.g. pre-2019 or not yet computed).
    """
    _default = {"term_slope": 0.0, "skew_2pct": 0.0, "skew_3pct": 0.0}
    if snap_date is None:
        return _default
    rows = ch.query(
        "SELECT term_slope, skew_2pct, skew_3pct "
        "FROM market.vol_term_structure FINAL "
        "WHERE symbol={sym:String} AND date={d:Date}",
        parameters={"sym": symbol, "d": snap_date},
    ).result_rows
    if not rows:
        return _default
    r = rows[0]
    return {
        "term_slope": float(r[0] or 0),
        "skew_2pct":  float(r[1] or 0),
        "skew_3pct":  float(r[2] or 0),
    }


def get_eod_signals(ch, expiry: date, snap_date: date) -> dict:
    """Fetch iv_rank, max_pain, OI walls for a specific expiry from EOD summary."""
    _default = {"iv_rank": 50.0, "max_pain_strike": 0.0, "ce_wall_strike": 0.0, "pe_wall_strike": 0.0}
    try:
        rows = ch.query(
            "SELECT iv_rank, max_pain_strike, "
            "       ifNull(ce_wall_strike, 0), "
            "       ifNull(pe_wall_strike, 0) "
            "FROM market.options_eod_summary FINAL "
            "WHERE expiry = {exp:Date} AND date <= {d:Date} "
            "ORDER BY date DESC LIMIT 1",
            parameters={"exp": expiry, "d": snap_date},
        ).result_rows
    except Exception as e:
        log.warning("get_eod_signals failed (columns may not exist yet): %s", e)
        return _default
    if rows:
        iv_rank, max_pain, ce_wall, pe_wall = rows[0]
        return {
            "iv_rank":         float(iv_rank or 50.0),
            "max_pain_strike": float(max_pain or 0.0),
            "ce_wall_strike":  float(ce_wall or 0.0),
            "pe_wall_strike":  float(pe_wall or 0.0),
        }
    return _default


def get_lot_size(ch, symbol: str) -> int:
    rows = ch.query(
        "SELECT lot_size FROM market.fo_lot_sizes WHERE symbol = {symbol:String} "
        "ORDER BY effective_from DESC LIMIT 1",
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


def get_avg_win_pnl(ch, lookback: int = 30) -> float | None:
    """Average P&L (₹) of winning trades from the last `lookback` non-skipped sim rows.

    Returns None if insufficient history (< 10 trades).
    Used as a drawdown cap: max loss per trade ≤ avg win.
    """
    rows = ch.query(
        "SELECT avg(pnl_amount) FROM ("
        "  SELECT pnl_amount FROM analysis.strategy_simulation FINAL "
        "  WHERE skipped = 0 AND pnl_amount > 0 "
        "  ORDER BY sim_date DESC LIMIT {n:UInt32}"
        ")",
        parameters={"n": lookback},
    ).result_rows
    val = float(rows[0][0]) if rows and rows[0][0] else None
    # Only return once we have at least 10 trades in history
    count_rows = ch.query(
        "SELECT count() FROM analysis.strategy_simulation FINAL WHERE skipped = 0"
    ).result_rows
    n_trades = int(count_rows[0][0]) if count_rows else 0
    return val if (val and n_trades >= 10) else None


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


def pick_strategy(confidence: float, pcr_bias: float, iv_skew: float,
                  skew_from_surface: float = 0.0) -> str:
    """Choose spread type from confidence + market bias.

    Both PCR and iv_skew must agree for a directional trade:
      pcr > PCR_BULLISH AND iv_skew > IV_SKEW_THRESH  → bull_put  (fear premium, puts expensive)
      pcr < PCR_BEARISH AND iv_skew < -IV_SKEW_THRESH → bear_call (upside fear, calls expensive)
      skew_from_surface > VOL_SKEW_BULL_PUT_THRESH     → bias bull_put (puts structurally expensive)
      any conflict or neutral                          → iron_condor
    """
    pcr_bull  = pcr_bias > PCR_BULLISH
    pcr_bear  = pcr_bias < PCR_BEARISH
    skew_bull = iv_skew > IV_SKEW_THRESH   # PE IV > CE IV — put premium elevated
    skew_bear = iv_skew < -IV_SKEW_THRESH  # CE IV > PE IV — call premium elevated

    if pcr_bull and not skew_bear:
        return "bull_put"
    if pcr_bear and not skew_bull:
        return "bear_call"
    # Vol surface tiebreaker: structurally elevated put skew favours bull_put
    if skew_from_surface > VOL_SKEW_BULL_PUT_THRESH and not skew_bear:
        return "bull_put"
    return "iron_condor"


def compute_trade_score(
    confidence: float, vix: float | None,
    iv_rank: float, pcr_bias: float, iv_skew: float,
    term_slope: float = 0.0,
) -> tuple[float, dict]:
    """Composite trade quality score 0-100. Returns (score, breakdown).

    Components (weights sum to 100):
      confidence  40pts — model conviction
      vix_zone    20pts — 14-22 = sweet spot (full), 11-13/23-28 = half, else 0
      iv_rank     20pts — proportional (more premium → higher score)
      alignment   20pts — PCR + iv_skew both agree on direction (full),
                          only PCR signal (half), conflict or neutral (0)
    Adjustment:
      term_slope  +3pts bonus if moderate backwardation (-4% to -1%): IV is elevated
                  but not panic — good environment to sell premium
    """
    # Confidence component
    conf_pct  = min(confidence, 100.0) / 100.0
    s_conf    = conf_pct * SCORE_W_CONFIDENCE

    # VIX zone component
    if vix is None:
        s_vix = SCORE_W_VIX * 0.5   # unknown → assume partial credit
    elif VIX_SWEET_LO <= vix <= VIX_SWEET_HI:
        s_vix = SCORE_W_VIX * 1.0
    elif VIX_MIN <= vix <= VIX_MAX:
        s_vix = SCORE_W_VIX * 0.5
    else:
        s_vix = 0.0

    # IV rank component
    s_ivrank = (min(iv_rank, 100.0) / 100.0) * SCORE_W_IV_RANK

    # Signal alignment component
    pcr_bull  = pcr_bias > PCR_BULLISH
    pcr_bear  = pcr_bias < PCR_BEARISH
    skew_bull = iv_skew > IV_SKEW_THRESH
    skew_bear = iv_skew < -IV_SKEW_THRESH
    if (pcr_bull and skew_bull) or (pcr_bear and skew_bear):
        s_align = SCORE_W_ALIGNMENT * 1.0   # both agree
    elif pcr_bull or pcr_bear:
        s_align = SCORE_W_ALIGNMENT * 0.5   # only PCR signal
    else:
        s_align = 0.0                        # neutral or conflict

    # Term slope bonus: moderate backwardation → IV elevated but not panic
    s_term = 0.0
    if TERM_BACKW_BONUS_MIN <= term_slope <= TERM_BACKW_BONUS_MAX:
        s_term = TERM_BACKW_BONUS_PTS

    score = s_conf + s_vix + s_ivrank + s_align + s_term
    breakdown = {
        "score": round(score, 1),
        "s_confidence": round(s_conf, 1),
        "s_vix": round(s_vix, 1),
        "s_iv_rank": round(s_ivrank, 1),
        "s_alignment": round(s_align, 1),
        "s_term_slope": round(s_term, 1),
    }
    return score, breakdown


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
    signals: dict | None = None,
    term_slope: float = 0.0, skew_2pct: float = 0.0,
):
    """Assemble full recommendation dict with leg prices.

    signals — optional dict from get_eod_signals(); used for OI wall guard.
    """
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

    # OI wall guard: short strike must be beyond the OI wall (wall buffers us from spot)
    if signals:
        ce_wall = signals.get("ce_wall_strike", 0.0)
        pe_wall = signals.get("pe_wall_strike", 0.0)
        if strategy in ("iron_condor", "bear_call") and ce_wall > 0 and sce < ce_wall:
            log.info(
                "OI wall guard: skipping %s %s — short_ce %.0f is inside CE wall %.0f",
                symbol, strategy, sce, ce_wall,
            )
            return None
        if strategy in ("iron_condor", "bull_put") and pe_wall > 0 and spe > pe_wall:
            log.info(
                "OI wall guard: skipping %s %s — short_pe %.0f is inside PE wall %.0f",
                symbol, strategy, spe, pe_wall,
            )
            return None

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
        term_slope=term_slope, skew_2pct=skew_2pct,
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
            rec["term_slope"], rec["skew_2pct"],
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
            "term_slope", "skew_2pct",
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

    # Fetch chain snapshot date early — needed for VIX and EOD signals
    snap_date = get_latest_chain_snapshot(ch, symbol, expiry)
    vix = get_vix(ch, snap_date) if snap_date else None
    signals = get_eod_signals(ch, expiry, snap_date) if snap_date else {}
    iv_rank = signals.get("iv_rank", 50.0)
    vol_signals = get_vol_surface_signals(ch, symbol, snap_date)
    term_slope = vol_signals["term_slope"]
    skew_2pct  = vol_signals["skew_2pct"]

    # Phase 2: composite trade_score gate
    trade_score, score_breakdown = compute_trade_score(
        confidence, vix, iv_rank, pcr_bias, iv_skew, term_slope=term_slope
    )
    log.info(
        "Trade score: %.1f/100 (conf=%.1f vix=%.1f ivrank=%.1f align=%.1f term=%.1f) | "
        "VIX=%s iv_rank=%.1f pcr=%.2f iv_skew=%.2f term_slope=%.3f skew_2pct=%.3f",
        trade_score,
        score_breakdown["s_confidence"], score_breakdown["s_vix"],
        score_breakdown["s_iv_rank"], score_breakdown["s_alignment"],
        score_breakdown["s_term_slope"],
        f"{vix:.1f}" if vix is not None else "n/a",
        iv_rank, pcr_bias, iv_skew, term_slope, skew_2pct,
    )
    if trade_score < TRADE_SCORE_THRESHOLD:
        log.info("Trade score %.1f < threshold %.1f — no trade for %s",
                 trade_score, TRADE_SCORE_THRESHOLD, symbol)
        return

    # Hard VIX gate still applies (catches extreme regimes regardless of score)
    if vix is not None and (vix < VIX_MIN or vix > VIX_MAX):
        log.info("VIX %.2f outside [%.1f, %.1f] — no trade for %s",
                 vix, VIX_MIN, VIX_MAX, symbol)
        return

    strategy = pick_strategy(confidence, pcr_bias, iv_skew, skew_from_surface=skew_2pct)
    log.info("Signal: pcr=%.2f iv_skew=%.2f skew_2pct=%.3f → strategy=%s",
             pcr_bias, iv_skew, skew_2pct, strategy)
    params   = get_optimal_params(ch, symbol, strategy)
    if params is None:
        log.warning("No optimal params for %s/%s — skipping", symbol, strategy)
        return

    iv_rank_mult = 0.5 + iv_rank / 100.0   # 0.5x at iv_rank=0, 1.5x at iv_rank=100
    log.info("iv_rank=%.1f → lot_mult=%.2f | max_pain=%.0f | CE_wall=%.0f | PE_wall=%.0f",
             iv_rank, iv_rank_mult,
             signals.get("max_pain_strike", 0), signals.get("ce_wall_strike", 0),
             signals.get("pe_wall_strike", 0))

    lot_size    = get_lot_size(ch, symbol)
    max_loss    = params["avg_max_loss"]
    capital     = get_current_capital(ch)
    risk_budget = capital * MAX_RISK_PCT
    base_lots   = int(risk_budget / (max_loss * lot_size)) if max_loss > 0 else 1
    lots        = max(1, int(base_lots * iv_rank_mult))

    # Drawdown cap: max loss per trade ≤ avg winning trade P&L
    avg_win = get_avg_win_pnl(ch)
    if avg_win and max_loss > 0:
        cap_lots = max(1, int(avg_win / (max_loss * lot_size)))
        if cap_lots < lots:
            log.info("Drawdown cap: lots %d → %d (avg_win=₹%.0f / max_loss=₹%.0f per lot)",
                     lots, cap_lots, avg_win, max_loss * lot_size)
            lots = cap_lots

    log.info("Capital: ₹%.0f | risk budget: ₹%.0f | lots=%d (base=%d × iv_mult=%.2f)",
             capital, risk_budget, lots, base_lots, iv_rank_mult)

    rec = build_recommendation(
        today, symbol, expiry, ch,
        confidence, pcr_bias, iv_skew,
        params, lots, lot_size,
        signals=signals,
        term_slope=term_slope, skew_2pct=skew_2pct,
    )

    if rec is None:
        log.warning("Could not price legs for %s/%s — skipping", symbol, expiry)
        return

    insert_recommendation(ch, rec)
    log.info(
        "Recommendation: %s %s | strategy=%s sn=%d wm=%d | "
        "net_credit=%.2f max_loss=%.2f lots=%d | score=%.1f confidence=%.1f",
        symbol, expiry, strategy, params["short_n"], params["wing_m"],
        rec["net_credit"], rec["max_loss"], lots, trade_score, confidence,
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
        FROM analysis.confidence_backtest AS cb FINAL
        -- pick best strategy params for each symbol (top sharpe per symbol)
        JOIN (
            SELECT symbol, strategy, short_n, wing_m
            FROM (
                SELECT symbol, strategy, short_n, wing_m,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY sharpe_pct DESC) AS rn
                FROM analysis.spread_optimal FINAL
                WHERE n_trades >= 10
            )
            WHERE rn = 1
        ) AS so ON so.symbol = cb.symbol
        -- get actual P&L for those params
        LEFT JOIN analysis.spread_backtest AS sb FINAL
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

    # Pre-fetch VIX and EOD signals for all entry_dates to avoid per-row DB queries
    all_dates  = sorted({r[2] for r in rows})   # entry_date is index 2
    all_expiry = sorted({r[0] for r in rows})   # expiry is index 0

    d_min, d_max = all_dates[0], all_dates[-1]

    vix_rows = ch.query(
        "SELECT toDate(timestamp) AS d, argMax(vix, timestamp) AS vix "
        "FROM market.nifty_live FINAL "
        "WHERE toDate(timestamp) >= {d1:Date} AND toDate(timestamp) <= {d2:Date} "
        "GROUP BY d",
        parameters={"d1": d_min, "d2": d_max},
    ).result_rows
    vix_by_date = {r[0]: float(r[1]) for r in vix_rows if r[1]}

    try:
        eod_rows = ch.query(
            "SELECT expiry, date, ifNull(iv_rank, 50), ifNull(pcr, 1.0), "
            "       ifNull(iv_skew, 0) "
            "FROM market.options_eod_summary FINAL "
            "WHERE date >= {d1:Date} AND date <= {d2:Date}",
            parameters={"d1": d_min, "d2": d_max},
        ).result_rows
    except Exception as e:
        log.warning("EOD signals query failed (iv_skew may not exist): %s — using defaults", e)
        eod_rows = []
    eod_by_key = {(r[0], r[1]): (float(r[2]), float(r[3]), float(r[4])) for r in eod_rows}

    # Pre-fetch vol surface term structure for all symbols × dates
    try:
        vts_rows = ch.query(
            "SELECT symbol, date, term_slope, skew_2pct "
            "FROM market.vol_term_structure FINAL "
            "WHERE date >= {d1:Date} AND date <= {d2:Date}",
            parameters={"d1": d_min, "d2": d_max},
        ).result_rows
    except Exception as e:
        log.warning("vol_term_structure query failed: %s — using defaults", e)
        vts_rows = []
    vts_by_key = {(r[0], r[1]): (float(r[2] or 0), float(r[3] or 0)) for r in vts_rows}
    log.info("Loaded %d vol surface rows for backtest", len(vts_by_key))

    capital = INITIAL_CAPITAL
    sim_rows = []
    win_pnls: list[float] = []   # track winning trade amounts for drawdown cap

    # Cache lot sizes to avoid one DB round-trip per row
    symbols_seen = {r[1] for r in rows}
    lot_size_cache = {sym: get_lot_size(ch, sym) for sym in symbols_seen}

    for (expiry, symbol, entry_date, confidence, straddle_pnl,
         strategy, short_n, wing_m, spread_pnl, max_loss) in rows:

        confidence_pct = float(confidence) * 100 if float(confidence) <= 1.0 else float(confidence)
        lot_size = lot_size_cache[symbol]

        # Resolve signals for this row
        vix = vix_by_date.get(entry_date)
        iv_rank, pcr_bias, iv_skew = eod_by_key.get((expiry, entry_date), (50.0, 1.0, 0.0))
        term_slope, skew_2pct = vts_by_key.get((symbol, entry_date), (0.0, 0.0))

        # Hard VIX gate
        if vix is not None and (vix < VIX_MIN or vix > VIX_MAX):
            sim_rows.append([
                entry_date, symbol, expiry,
                strategy or "iron_condor", short_n or 0, wing_m or 0,
                confidence_pct, 0, lot_size, 0.0, 0.0, capital, capital, 1,
            ])
            continue

        trade_score, _ = compute_trade_score(
            confidence_pct, vix, iv_rank, pcr_bias, iv_skew, term_slope=term_slope
        )

        skipped = 0
        pnl_pts = 0.0
        pnl_amount = 0.0
        lots = 1

        if trade_score < TRADE_SCORE_THRESHOLD:
            skipped = 1
        else:
            ml = float(max_loss) if max_loss else 0.0
            if ml > 0:
                iv_rank_mult = 0.5 + iv_rank / 100.0
                risk_budget  = capital * MAX_RISK_PCT
                base_lots    = int(risk_budget / (ml * lot_size))
                lots         = max(1, int(base_lots * iv_rank_mult))

                # Drawdown cap: max loss per trade ≤ rolling avg winning P&L
                if len(win_pnls) >= 10:
                    avg_win = sum(win_pnls[-30:]) / len(win_pnls[-30:])
                    cap_lots = max(1, int(avg_win / (ml * lot_size)))
                    lots = min(lots, cap_lots)

            pnl_pts    = float(spread_pnl or straddle_pnl or 0.0)
            pnl_amount = pnl_pts * lots * lot_size
            if pnl_amount > 0:
                win_pnls.append(pnl_amount)

        capital_before = capital
        capital += pnl_amount
        capital = max(capital, 0)

        sim_rows.append([
            entry_date, symbol, expiry,
            strategy or "iron_condor", short_n or 0, wing_m or 0,
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
