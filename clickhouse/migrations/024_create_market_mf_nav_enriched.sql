-- Migration : 024
-- Description: MF NAV enriched table — weekly/monthly rollups,
--              technical indicators (SMA, EMA, RSI, ATR proxy),
--              and per-symbol summary stats as separate columns
-- Created    : 2026-03-21
-- Depends on : market.mf_nav (migrations 022, 023)

CREATE TABLE IF NOT EXISTS market.mf_nav_enriched
(
    date                Date,
    scheme_code         UInt32,
    nav                 Float64,

    week_start          Date,
    week_open           Float64 DEFAULT 0,
    week_high           Float64 DEFAULT 0,
    week_low            Float64 DEFAULT 0,
    week_close          Float64 DEFAULT 0,
    week_return_pct     Float64 DEFAULT 0,

    month_start         Date,
    month_open          Float64 DEFAULT 0,
    month_high          Float64 DEFAULT 0,
    month_low           Float64 DEFAULT 0,
    month_close         Float64 DEFAULT 0,
    month_return_pct    Float64 DEFAULT 0,

    sma_20              Float64 DEFAULT 0,
    sma_50              Float64 DEFAULT 0,
    sma_200             Float64 DEFAULT 0,

    ema_20              Float64 DEFAULT 0,
    ema_50              Float64 DEFAULT 0,

    rsi_14              Float64 DEFAULT 0,
    atr_14              Float64 DEFAULT 0,

    high_52w            Float64 DEFAULT 0,
    low_52w             Float64 DEFAULT 0,

    ytd_return_pct      Float64 DEFAULT 0,

    all_time_high       Float64 DEFAULT 0,
    drawdown_pct        Float64 DEFAULT 0,

    computed_at         DateTime DEFAULT now(),
    version             UInt64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (scheme_code, date)
