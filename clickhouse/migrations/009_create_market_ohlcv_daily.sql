CREATE TABLE IF NOT EXISTS market.ohlcv_daily
(
    date            Date,
    symbol          String,
    market          String,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          UInt64,
    created_at      DateTime DEFAULT now(),
    loaded_by       String DEFAULT '',
    source_file     String DEFAULT '',
    batch_id        UUID,
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (market, symbol, date)