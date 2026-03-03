CREATE TABLE IF NOT EXISTS market.fii_dii
(
    date            Date,
    entity          String,
    buy_value       Float64 DEFAULT 0,
    sell_value      Float64 DEFAULT 0,
    net_value       Float64 DEFAULT 0,
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (date, entity)