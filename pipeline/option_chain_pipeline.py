#!/usr/bin/env python3
"""
option_chain_pipeline.py
─────────────────────────
Fetches NSE NIFTY/BANKNIFTY option chain at EOD (run after 15:30 IST).
Computes key derivatives signals and writes to:
  market.options_chain        — row-per-strike EOD snapshot
  market.options_eod_summary  — daily summary: PCR, max pain, IV rank
  market.nifty_live           — PCR field updated for today's date
  analysis.strike_recommendations — suggested index option strategy

Key signals:
  PCR (Put-Call Ratio)  — total PE OI / total CE OI
                          >1.3 = excessive fear (contrarian bullish)
                          <0.7 = excessive greed (contrarian bearish)
  Max Pain              — strike minimising total option-seller loss;
                          Nifty gravitates toward this near expiry
  IV Rank               — (ATM_IV − 52w_low) / (52w_high − 52w_low) × 100
                          >70 = sell premium; <25 = buy cheap options
  IV Percentile         — % of past-year days with lower ATM IV

Strategy logic (written to analysis.strike_recommendations):
  PCR > 1.3 or regime bullish + IV_rank > 60  → Bull Put Spread (credit)
  PCR > 1.3 or regime bullish + IV_rank ≤ 60  → Buy ATM Call (debit)
  PCR < 0.7 or regime bearish + IV_rank > 60  → Bear Call Spread (credit)
  PCR < 0.7 or regime bearish + IV_rank ≤ 60  → Buy ATM Put (debit)

Usage:
  python option_chain_pipeline.py
  python option_chain_pipeline.py --symbol BANKNIFTY
  python option_chain_pipeline.py --dry-run

Docker:
  docker compose run --rm option_chain_pipeline
"""

import os
import time
import logging
import argparse
from datetime import datetime, date, timedelta

import pandas as pd
import clickhouse_connect
from curl_cffi import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

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

NSE_HOME   = "https://www.nseindia.com"
NSE_OC_URL = "https://www.nseindia.com/api/option-chain-indices"

SESSION_TIMEOUT = 20
REQUEST_DELAY   = 2.5

# Nifty options are in 50-point increments; BankNifty in 100
_STRIKE_STEP = {"NIFTY": 50.0, "BANKNIFTY": 100.0}


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# NSE session
# ══════════════════════════════════════════════════════

def build_nse_session() -> requests.Session:
    session = requests.Session(impersonate="chrome110")
    log.info("Warming up NSE session (option-chain page)...")
    resp = session.get(
        f"{NSE_HOME}/option-chain",
        timeout=SESSION_TIMEOUT,
        headers={
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 Chrome/110.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://www.nseindia.com/",
        },
    )
    resp.raise_for_status()
    log.info(f"NSE session ready (cookies: {len(session.cookies)})")
    time.sleep(REQUEST_DELAY)
    return session


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def fetch_option_chain(session: requests.Session, symbol: str) -> dict:
    resp = session.get(
        NSE_OC_URL,
        params={"symbol": symbol},
        timeout=SESSION_TIMEOUT,
        headers={
            "Referer":         "https://www.nseindia.com/option-chain",
            "X-Requested-With": "XMLHttpRequest",
            "Accept":           "application/json",
        },
    )
    resp.raise_for_status()
    return resp.json()


# ══════════════════════════════════════════════════════
# Parsing
# ══════════════════════════════════════════════════════

def parse_chain(data: dict) -> tuple[pd.DataFrame, float, list[str]]:
    """
    Parse NSE option chain JSON.
    Returns (df_strikes, spot, expiry_list).

    df_strikes columns:
      strike, expiry, ce_oi, ce_iv, ce_ltp, ce_oi_chg,
                      pe_oi, pe_iv, pe_ltp, pe_oi_chg
    """
    records  = data.get("records", {})
    spot     = float(records.get("underlyingValue", 0))
    expiries = records.get("expiryDates", [])

    rows = []
    for item in data.get("filtered", {}).get("data", []):
        strike = float(item.get("strikePrice", 0))
        expiry = item.get("expiryDate", "")
        ce     = item.get("CE", {})
        pe     = item.get("PE", {})
        rows.append({
            "strike":    strike,
            "expiry":    expiry,
            "ce_oi":     int(ce.get("openInterest", 0)),
            "ce_iv":     float(ce.get("impliedVolatility", 0)),
            "ce_ltp":    float(ce.get("lastPrice", 0)),
            "ce_oi_chg": int(ce.get("changeinOpenInterest", 0)),
            "pe_oi":     int(pe.get("openInterest", 0)),
            "pe_iv":     float(pe.get("impliedVolatility", 0)),
            "pe_ltp":    float(pe.get("lastPrice", 0)),
            "pe_oi_chg": int(pe.get("changeinOpenInterest", 0)),
        })

    return pd.DataFrame(rows), spot, expiries


def find_atm(df: pd.DataFrame, spot: float) -> float:
    if df.empty or spot == 0:
        return 0.0
    idx = (df["strike"] - spot).abs().idxmin()
    return float(df.loc[idx, "strike"])


def compute_max_pain(df: pd.DataFrame) -> float:
    """
    Strike K where total pain to all option sellers is minimum.
    call_pain(K) = Σ max(0, K - strike_i) × CE_OI_i   for all strikes
    put_pain(K)  = Σ max(0, strike_i - K) × PE_OI_i
    """
    if df.empty:
        return 0.0

    strikes  = sorted(df["strike"].unique())
    min_pain = float("inf")
    mp_strike = strikes[0]

    for k in strikes:
        cp = sum(max(0.0, k - r["strike"]) * r["ce_oi"]
                 for _, r in df.iterrows())
        pp = sum(max(0.0, r["strike"] - k) * r["pe_oi"]
                 for _, r in df.iterrows())
        total = cp + pp
        if total < min_pain:
            min_pain  = total
            mp_strike = k

    return mp_strike


# ══════════════════════════════════════════════════════
# IV Rank
# ══════════════════════════════════════════════════════

def compute_iv_rank(ch, today_iv: float,
                    lookback_days: int = 252) -> tuple[float, float]:
    """
    IV Rank      = (today − 52w_low) / (52w_high − 52w_low) × 100
    IV Percentile = % of past-year days with lower ATM IV
    Both in [0, 100]. Returns (0, 0) when history is insufficient.
    """
    if today_iv <= 0:
        return 0.0, 0.0

    from_date = date.today() - timedelta(days=lookback_days)
    try:
        result = ch.query(
            "SELECT atm_ce_iv FROM market.options_eod_summary FINAL "
            "WHERE date >= {d:Date} AND atm_ce_iv > 0 ORDER BY date",
            parameters={"d": from_date}
        )
        hist = [float(r[0]) for r in result.result_rows]
    except Exception:
        hist = []

    if not hist:
        return 0.0, 0.0

    iv_min  = min(hist)
    iv_max  = max(hist)
    iv_rank = round((today_iv - iv_min) / (iv_max - iv_min) * 100, 2) \
              if iv_max > iv_min else 50.0
    iv_pct  = round(sum(1 for v in hist if v < today_iv) / len(hist) * 100, 2)

    return iv_rank, iv_pct


# ══════════════════════════════════════════════════════
# Insert helpers
# ══════════════════════════════════════════════════════

def insert_chain_rows(ch, df_near: pd.DataFrame,
                      expiry_date: date, now_ts: datetime):
    """Write per-strike rows to market.options_chain."""
    version = int(now_ts.timestamp())
    rows    = []

    for _, r in df_near.iterrows():
        if r["ce_oi"] > 0 or r["ce_ltp"] > 0:
            rows.append({
                "timestamp": now_ts, "expiry": expiry_date,
                "strike": r["strike"], "option_type": "CE",
                "ltp": r["ce_ltp"], "bid": 0.0, "ask": 0.0,
                "iv": r["ce_iv"], "delta": 0.0, "theta": 0.0,
                "oi": r["ce_oi"], "oi_change": r["ce_oi_chg"],
                "volume": 0, "version": version,
            })
        if r["pe_oi"] > 0 or r["pe_ltp"] > 0:
            rows.append({
                "timestamp": now_ts, "expiry": expiry_date,
                "strike": r["strike"], "option_type": "PE",
                "ltp": r["pe_ltp"], "bid": 0.0, "ask": 0.0,
                "iv": r["pe_iv"], "delta": 0.0, "theta": 0.0,
                "oi": r["pe_oi"], "oi_change": r["pe_oi_chg"],
                "volume": 0, "version": version,
            })

    if rows:
        ch.insert_df("market.options_chain", pd.DataFrame(rows))
        log.info(f"Inserted {len(rows)} option chain rows "
                 f"→ market.options_chain")


def insert_eod_summary(ch, today: date, expiry_date: date, spot: float,
                       df_near: pd.DataFrame, atm_strike: float,
                       max_pain: float, iv_rank: float, iv_pct: float):
    total_ce = int(df_near["ce_oi"].sum())
    total_pe = int(df_near["pe_oi"].sum())
    pcr      = round(total_pe / total_ce, 4) if total_ce > 0 else 0.0

    atm_rows  = df_near[df_near["strike"] == atm_strike]
    atm_ce_iv = float(atm_rows["ce_iv"].iloc[0]) if not atm_rows.empty else 0.0
    atm_pe_iv = float(atm_rows["pe_iv"].iloc[0]) if not atm_rows.empty else 0.0

    row = {
        "date": today, "expiry": expiry_date, "nifty_spot": spot,
        "total_ce_oi": total_ce, "total_pe_oi": total_pe, "pcr": pcr,
        "max_pain_strike": max_pain, "atm_strike": atm_strike,
        "atm_ce_iv": atm_ce_iv, "atm_pe_iv": atm_pe_iv,
        "iv_rank": iv_rank, "iv_percentile": iv_pct,
        "version": int(datetime.now().timestamp()),
    }
    ch.insert_df("market.options_eod_summary", pd.DataFrame([row]))
    log.info(f"EOD summary → PCR={pcr:.2f}  max_pain={max_pain:.0f}  "
             f"ATM_IV={atm_ce_iv:.1f}%  IV_rank={iv_rank:.0f}")
    return pcr, atm_ce_iv


def update_nifty_live_pcr(ch, today: date, pcr: float):
    """Re-insert today's market.nifty_live row with PCR filled in."""
    try:
        result = ch.query(
            "SELECT timestamp, nifty_spot, vix, advance_decline "
            "FROM market.nifty_live FINAL "
            "WHERE toDate(timestamp) = {d:Date}",
            parameters={"d": today}
        )
        if not result.result_rows:
            log.warning("No nifty_live row for today — run vix_pipeline first")
            return
        r = result.result_rows[0]
        ch.insert_df("market.nifty_live", pd.DataFrame([{
            "timestamp":       r[0],
            "nifty_spot":      float(r[1]),
            "vix":             float(r[2]),
            "pcr":             pcr,
            "advance_decline": float(r[3]),
            "version":         int(datetime.now().timestamp()),
        }]))
        log.info(f"Updated market.nifty_live PCR={pcr:.2f} for {today}")
    except Exception as e:
        log.warning(f"Could not update PCR in nifty_live: {e}")


# ══════════════════════════════════════════════════════
# Strike recommendation
# ══════════════════════════════════════════════════════

def write_strike_recommendation(ch, today: date, spot: float,
                                expiry_date: date, pcr: float,
                                iv_rank: float, atm_ce_iv: float,
                                symbol: str):
    """
    Derive a single daily index-option strategy and insert into
    analysis.strike_recommendations.

    Bias logic:
      PCR > 1.3  → excessive put buying → contrarian bullish
      PCR < 0.7  → excessive call buying → contrarian bearish
      otherwise  → follow analysis.market_regime trend
    """
    if pcr > 1.3:
        bias = "bullish"
    elif pcr < 0.7:
        bias = "bearish"
    else:
        try:
            result = ch.query(
                "SELECT trend FROM analysis.market_regime FINAL "
                "WHERE date = {d:Date}",
                parameters={"d": today}
            )
            trend = result.result_rows[0][0] if result.result_rows else "flat"
            bias  = ("bullish" if trend == "up"
                     else "bearish" if trend == "down"
                     else "neutral")
        except Exception:
            bias = "neutral"

    if bias == "neutral":
        log.info("Regime is neutral — no strike recommendation today")
        return

    step = _STRIKE_STEP.get(symbol, 50.0)
    atm  = round(spot / step) * step   # nearest ATM strike

    if bias == "bullish":
        if iv_rank > 60:
            # Sell put at ATM-1step, hedge at ATM-2steps (Bull Put Spread)
            rec_put  = atm - step
            rec_call = 0.0
            strategy = "bull_put_spread"
        else:
            # Buy ATM call outright
            rec_call = atm
            rec_put  = 0.0
            strategy = "buy_call"
    else:  # bearish
        if iv_rank > 60:
            # Sell call at ATM+1step, hedge at ATM+2steps (Bear Call Spread)
            rec_call = atm + step
            rec_put  = 0.0
            strategy = "bear_call_spread"
        else:
            # Buy ATM put outright
            rec_put  = atm
            rec_call = 0.0
            strategy = "buy_put"

    # Approximate ATM premium: S × (IV/100) × sqrt(DTE/252) × 0.4
    dte            = max(1, (expiry_date - today).days)
    approx_premium = round(spot * (atm_ce_iv / 100) * ((dte / 252) ** 0.5) * 0.4, 2)

    is_credit   = "spread" in strategy
    # Credit strategies: high IV_rank = good to sell; Debit: low IV_rank = cheap
    confidence  = int(iv_rank if is_credit else 100 - iv_rank)
    probability = (min(80.0, 45 + iv_rank * 0.3)
                   if is_credit
                   else max(35.0, 55 - iv_rank * 0.2))

    row = {
        "timestamp":             datetime.now(),
        "expiry":                expiry_date,
        "nifty_spot":            spot,
        "vix":                   atm_ce_iv,
        "recommended_call":      rec_call,
        "recommended_put":       rec_put,
        "expected_premium_call": approx_premium if rec_call > 0 else 0.0,
        "expected_premium_put":  approx_premium if rec_put  > 0 else 0.0,
        "total_premium":         approx_premium,
        "breakeven_upper":       spot + approx_premium * 1.5,
        "breakeven_lower":       spot - approx_premium * 1.5,
        "probability_profit":    round(probability, 2),
        "confidence_score":      min(100, max(0, confidence)),
        "version":               int(datetime.now().timestamp()),
    }
    ch.insert_df("analysis.strike_recommendations", pd.DataFrame([row]))
    log.info(
        f"Strategy: {strategy} | bias={bias} | "
        f"call={rec_call or '-'} put={rec_put or '-'} | "
        f"premium≈{approx_premium} | confidence={confidence}"
    )


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Option Chain Pipeline — NSE EOD option chain fetch"
    )
    parser.add_argument("--symbol",  default="NIFTY",
                        choices=["NIFTY", "BANKNIFTY"],
                        help="Index symbol (default: NIFTY)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch, compute and print — do not insert to DB")
    args = parser.parse_args()

    log.info(f"=== Option Chain Pipeline [{args.symbol}] ===")

    session = build_nse_session()
    log.info("Fetching option chain...")
    data = fetch_option_chain(session, args.symbol)

    df_all, spot, expiries = parse_chain(data)

    if df_all.empty or spot == 0:
        log.error("Empty option chain or spot=0 — aborting")
        return

    near_expiry = expiries[0] if expiries else ""
    log.info(f"Spot={spot:.0f}  Near expiry={near_expiry}  "
             f"Strikes parsed={len(df_all)}")

    df_near    = df_all[df_all["expiry"] == near_expiry].copy()
    atm_strike = find_atm(df_near, spot)
    max_pain   = compute_max_pain(df_near)

    total_ce = int(df_near["ce_oi"].sum())
    total_pe = int(df_near["pe_oi"].sum())
    pcr      = round(total_pe / total_ce, 4) if total_ce > 0 else 0.0

    atm_rows  = df_near[df_near["strike"] == atm_strike]
    atm_ce_iv = float(atm_rows["ce_iv"].iloc[0]) if not atm_rows.empty else 0.0

    log.info(f"ATM={atm_strike:.0f}  Max_pain={max_pain:.0f}  "
             f"PCR={pcr:.2f}  ATM_IV={atm_ce_iv:.1f}%")
    log.info(f"Total CE OI={total_ce:,}  PE OI={total_pe:,}")

    if args.dry_run:
        log.info("[DRY RUN] Skipping DB writes")
        return

    ch    = get_ch_client()
    today = date.today()

    try:
        expiry_date = datetime.strptime(near_expiry, "%d-%b-%Y").date()
    except ValueError:
        log.error(f"Cannot parse expiry date string: {near_expiry!r}")
        return

    iv_rank, iv_pct = compute_iv_rank(ch, atm_ce_iv)
    log.info(f"IV rank={iv_rank:.0f}  IV percentile={iv_pct:.0f}")

    now_ts = datetime.now()
    insert_chain_rows(ch, df_near, expiry_date, now_ts)
    pcr, atm_ce_iv = insert_eod_summary(
        ch, today, expiry_date, spot, df_near,
        atm_strike, max_pain, iv_rank, iv_pct
    )
    update_nifty_live_pcr(ch, today, pcr)
    write_strike_recommendation(
        ch, today, spot, expiry_date, pcr, iv_rank, atm_ce_iv, args.symbol
    )

    print(f"\n{'='*55}")
    print(f"  Symbol      : {args.symbol}")
    print(f"  Spot        : {spot:.0f}")
    print(f"  Near expiry : {near_expiry}")
    print(f"  PCR         : {pcr:.2f}  "
          f"({'fear' if pcr > 1.2 else 'greed' if pcr < 0.8 else 'neutral'})")
    print(f"  Max pain    : {max_pain:.0f}")
    print(f"  ATM IV      : {atm_ce_iv:.1f}%")
    print(f"  IV rank     : {iv_rank:.0f} / 100")
    print(f"{'='*55}\n")

    log.info("Option chain pipeline complete ✅")


if __name__ == "__main__":
    main()
