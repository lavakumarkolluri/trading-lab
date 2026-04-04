-- Migration : 038
-- Description: Formally adopt trades.ohlcv — table existed before migration tracking
-- Created    : 2026-04-04
--
-- This table was created manually outside the migration system.
-- It contains 753 rows of OHLCV data (2025-03-03 to 2026-03-02).
-- CREATE TABLE IF NOT EXISTS ensures this is a no-op if table already exists.
-- DO NOT modify the schema here — create 039 for any future changes.

CREATE TABLE IF NOT EXISTS trades.ohlcv
(
    `date`      Date,
    `open`      Float64,
    `high`      Float64,
    `low`       Float64,
    `close`     Float64,
    `volume`    UInt64,
    `symbol`    String
)
ENGINE = MergeTree
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192
