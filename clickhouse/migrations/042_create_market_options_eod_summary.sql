-- Migration : 042
-- Table     : market.options_eod_summary
-- Purpose   : Daily summary of key options-market signals for Nifty/BankNifty.
--             One row per (date, expiry). Written by option_chain_pipeline.
--
-- Key columns:
--   pcr              — Put-Call Ratio by OI (>1.2 fear, <0.8 greed)
--   max_pain_strike  — Strike minimising total option seller loss (gravitational pull near expiry)
--   atm_ce_iv/pe_iv  — Implied volatility at the money
--   iv_rank          — Where today's ATM IV sits vs past 52w (0=lowest, 100=highest)
--   iv_percentile    — % of past-year days with lower IV than today

CREATE TABLE IF NOT EXISTS market.options_eod_summary
(
    date            Date,
    expiry          Date,
    nifty_spot      Float64,
    total_ce_oi     UInt64  DEFAULT 0,
    total_pe_oi     UInt64  DEFAULT 0,
    pcr             Float64 DEFAULT 0,
    max_pain_strike Float64 DEFAULT 0,
    atm_strike      Float64 DEFAULT 0,
    atm_ce_iv       Float64 DEFAULT 0,
    atm_pe_iv       Float64 DEFAULT 0,
    iv_rank         Float64 DEFAULT 0,
    iv_percentile   Float64 DEFAULT 0,
    version         UInt64  DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (date, expiry)
