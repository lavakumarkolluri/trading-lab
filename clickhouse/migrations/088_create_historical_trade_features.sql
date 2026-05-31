CREATE TABLE IF NOT EXISTS analysis.historical_trade_features
(
    symbol        LowCardinality(String),
    entry_date    Date,
    expiry        Date,
    strategy      LowCardinality(String),
    pnl_pts       Float32,
    target        UInt8,
    features_json String DEFAULT '{}',
    version       UInt64 DEFAULT toUnixTimestamp(now())
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, entry_date, expiry, strategy);
