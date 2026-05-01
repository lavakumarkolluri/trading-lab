-- Daily valuation metrics per symbol (from yfinance ticker.info)
CREATE TABLE IF NOT EXISTS market.fundamental_snapshot
(
    symbol             LowCardinality(String),
    date               Date,
    market_cap         Float64,
    pe_ratio           Float64,
    forward_pe         Float64,
    pb_ratio           Float64,
    ev_ebitda          Float64,
    ev_revenue         Float64,
    eps_ttm            Float64,
    book_value         Float64,
    revenue_ttm        Float64,
    gross_margins      Float64,
    operating_margins  Float64,
    profit_margins     Float64,
    roe                Float64,
    roa                Float64,
    free_cashflow      Float64,
    operating_cashflow Float64,
    total_debt         Float64,
    cash               Float64,
    dividend_yield     Float64,
    computed_at        DateTime DEFAULT now(),
    version            UInt64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(date)
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- Quarterly income statement / balance sheet / cashflow per symbol
CREATE TABLE IF NOT EXISTS market.fundamental_quarterly
(
    symbol             LowCardinality(String),
    period             Date,
    period_type        LowCardinality(String),
    revenue            Float64,
    gross_profit       Float64,
    operating_income   Float64,
    net_income         Float64,
    ebitda             Float64,
    eps                Float64,
    total_assets       Float64,
    total_debt         Float64,
    cash               Float64,
    equity             Float64,
    operating_cashflow Float64,
    capex              Float64,
    free_cashflow      Float64,
    computed_at        DateTime DEFAULT now(),
    version            UInt64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYear(period)
ORDER BY (symbol, period_type, period)
SETTINGS index_granularity = 8192;
