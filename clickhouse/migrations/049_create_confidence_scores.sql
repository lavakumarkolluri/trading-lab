-- 049_create_confidence_scores.sql
-- Walk-forward backtest results + current production confidence scores

CREATE TABLE IF NOT EXISTS analysis.confidence_backtest
(
    symbol        LowCardinality(String),
    expiry        Date,
    entry_date    Date,
    atm_strike    Float32,
    entry_premium Float32,
    exit_value    Float32,
    pnl_pts       Float32,
    pnl_pct       Float32,
    target        UInt8,           -- 1 if pnl_pts > 0
    confidence    Float32,         -- model predicted probability (0-1)
    fold          Int16,           -- walk-forward fold index
    version       UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, expiry);

CREATE TABLE IF NOT EXISTS analysis.confidence_scores
(
    score_date      Date,
    symbol          LowCardinality(String),
    next_expiry     Date,
    confidence      Float32,        -- 0-100
    expected_pnl_pct Float32,       -- predicted P&L as % of premium
    features_json   String,
    version         UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (score_date, symbol);
