-- Strike-level volatility surface: one row per (date, symbol, expiry, strike, option_type)
CREATE TABLE IF NOT EXISTS market.vol_surface (
    date        Date,
    symbol      LowCardinality(String),
    expiry      Date,
    dte         Int32,
    strike      Float64,
    moneyness   Float64,       -- K / spot; 1.0 = ATM
    option_type LowCardinality(String),
    iv          Float64,
    delta       Float64,
    ltp         Float64,
    oi          UInt64,
    version     UInt64 DEFAULT toUnixTimestamp(now())
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(date)
ORDER BY (date, symbol, expiry, strike, option_type);

-- Aggregated term structure + skew: one row per (date, symbol)
CREATE TABLE IF NOT EXISTS market.vol_term_structure (
    date        Date,
    symbol      LowCardinality(String),
    spot        Float64,
    atm_iv_7d   Float64,       -- interpolated ATM IV at 7 DTE
    atm_iv_14d  Float64,
    atm_iv_30d  Float64,
    atm_iv_60d  Float64,
    atm_iv_90d  Float64,
    term_slope  Float64,       -- (atm_iv_30d - atm_iv_7d); negative = backwardation (fear)
    skew_1pct   Float64,       -- PE_IV(0.99) - CE_IV(1.01)
    skew_2pct   Float64,       -- PE_IV(0.98) - CE_IV(1.02)
    skew_3pct   Float64,       -- PE_IV(0.97) - CE_IV(1.03)
    skew_5pct   Float64,       -- PE_IV(0.95) - CE_IV(1.05)
    version     UInt64 DEFAULT toUnixTimestamp(now())
) ENGINE = ReplacingMergeTree(version)
ORDER BY (date, symbol);
