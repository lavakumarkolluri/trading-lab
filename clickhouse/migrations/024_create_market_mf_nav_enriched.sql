-- Migration : 024
-- Description: MF NAV enriched table — weekly/monthly rollups,
--              technical indicators (SMA, EMA, RSI, ATR proxy),
--              and per-symbol summary stats as separate columns
-- Created    : 2026-03-21
-- Depends on : 022_mf_scheme_master, 023_MF_NAV_History (market.mf_nav)

CREATE TABLE IF NOT EXISTS market.mf_nav_enriched
(
    -- ── Identity ──────────────────────────────────────
    date                Date,
    scheme_code         UInt32,
    nav                 Float64,

    -- ── Weekly Rollup ─────────────────────────────────
    -- ISO week: Monday = week_start
    week_start          Date    DEFAULT toMonday(date),
    week_open           Float64 DEFAULT 0,   -- first NAV of the week
    week_high           Float64 DEFAULT 0,   -- highest NAV in the week
    week_low            Float64 DEFAULT 0,   -- lowest NAV in the week
    week_close          Float64 DEFAULT 0,   -- last NAV of the week (= nav on last day)
    week_return_pct     Float64 DEFAULT 0,   -- (week_close - week_open) / week_open * 100

    -- ── Monthly Rollup ────────────────────────────────
    month_start         Date    DEFAULT toStartOfMonth(date),
    month_open          Float64 DEFAULT 0,   -- first NAV of the month
    month_high          Float64 DEFAULT 0,   -- highest NAV in the month
    month_low           Float64 DEFAULT 0,   -- lowest NAV in the month
    month_close         Float64 DEFAULT 0,   -- last NAV of the month
    month_return_pct    Float64 DEFAULT 0,   -- (month_close - month_open) / month_open * 100

    -- ── Simple Moving Averages ────────────────────────
    sma_20              Float64 DEFAULT 0,   -- 20-day SMA of NAV
    sma_50              Float64 DEFAULT 0,   -- 50-day SMA
    sma_200             Float64 DEFAULT 0,   -- 200-day SMA (golden/death cross signal)

    -- ── Exponential Moving Averages ───────────────────
    ema_20              Float64 DEFAULT 0,   -- 20-day EMA (span=20, adjust=False)
    ema_50              Float64 DEFAULT 0,   -- 50-day EMA

    -- ── RSI (Relative Strength Index) ─────────────────
    -- Wilder's RSI-14; values 0–100
    -- >70 = overbought (NAV stretched), <30 = oversold (dip opportunity)
    rsi_14              Float64 DEFAULT 0,

    -- ── ATR Proxy (Volatility) ────────────────────────
    -- MF has no intraday high/low; ATR is proxied as
    -- 14-day rolling mean of |daily NAV change| (absolute rupee volatility)
    atr_14              Float64 DEFAULT 0,

    -- ── 52-Week Range ─────────────────────────────────
    high_52w            Float64 DEFAULT 0,   -- max NAV over trailing 252 trading days
    low_52w             Float64 DEFAULT 0,   -- min NAV over trailing 252 trading days

    -- ── YTD Return ────────────────────────────────────
    -- (current NAV - first NAV of current calendar year) / first NAV * 100
    ytd_return_pct      Float64 DEFAULT 0,

    -- ── All-Time High & Drawdown ──────────────────────
    all_time_high       Float64 DEFAULT 0,   -- max NAV from inception to this date
    drawdown_pct        Float64 DEFAULT 0,   -- (all_time_high - nav) / all_time_high * 100

    -- ── Housekeeping ──────────────────────────────────
    computed_at         DateTime DEFAULT now(),
    version             UInt64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (scheme_code, date)
