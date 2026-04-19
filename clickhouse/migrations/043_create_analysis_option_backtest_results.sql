-- Migration : 043
-- Table     : analysis.option_backtest_results
-- Purpose   : Stores per-trade results from option_backtest.py.
--             One row per (signal_date, bias). Populated by option_backtest.py.
--
-- Lookahead guarantee:
--   signal_date data (vix, vix_rank, pcr, bias, entry_price) uses ONLY
--   information available at EOD of signal_date.
--   exit_price/pnl are the measured future outcomes — not inputs to the signal.

CREATE TABLE IF NOT EXISTS analysis.option_backtest_results
(
    signal_date     Date,
    expiry          Date,
    nifty_spot      Float64,
    vix             Float64,
    vix_rank        Float64,        -- rolling 52w rank at signal_date; no lookahead
    pcr             Float64,
    bias            String,         -- 'bullish' | 'bearish'
    atm_strike      Float64,
    entry_type      String,         -- 'CE' | 'PE'
    entry_price     Float64,        -- ATM option close on signal_date
    exit_date       Date,           -- signal_date + 3 trading days (or pre-expiry)
    exit_price      Float64,        -- same strike close on exit_date (0 if expired)
    nifty_return_3d Float64,        -- % Nifty move over the hold period
    pnl             Float64,        -- exit_price - entry_price (Rs per unit)
    pnl_pct         Float64,        -- pnl / entry_price * 100
    hit             UInt8,          -- 1 if pnl > 0
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (signal_date, bias)
