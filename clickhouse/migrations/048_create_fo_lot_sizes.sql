-- F&O lot size change history — one row per (symbol, effective_from) when lot_size changes
-- Derived from NSE bhavcopy NewBrdLotQty column; covers all F&O symbols
CREATE TABLE IF NOT EXISTS market.fo_lot_sizes
(
    symbol         LowCardinality(String),
    effective_from Date,
    lot_size       UInt32,
    computed_at    DateTime DEFAULT now(),
    version        UInt64   DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, effective_from)
SETTINGS index_granularity = 8192;
