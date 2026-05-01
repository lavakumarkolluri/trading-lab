-- Migration : 045
-- Table     : market.options_chain
-- Purpose   : Add symbol column (NIFTY/BANKNIFTY), PARTITION BY month,
--             and reorder ORDER BY to (symbol, expiry, strike, option_type, timestamp).
--             Table was empty so it is dropped and recreated.

DROP TABLE IF EXISTS market.options_chain;

CREATE TABLE market.options_chain
(
    symbol      String,
    timestamp   DateTime,
    expiry      Date,
    strike      Float64,
    option_type String,
    ltp         Float64,
    bid         Float64 DEFAULT 0,
    ask         Float64 DEFAULT 0,
    iv          Float64 DEFAULT 0,
    delta       Float64 DEFAULT 0,
    theta       Float64 DEFAULT 0,
    oi          UInt64  DEFAULT 0,
    oi_change   Int64   DEFAULT 0,
    volume      UInt64  DEFAULT 0,
    version     UInt64  DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, expiry, strike, option_type, timestamp)
SETTINGS index_granularity = 8192
