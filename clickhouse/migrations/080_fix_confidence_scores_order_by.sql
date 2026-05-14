-- Fix: add strategy_type to the ORDER BY key so all 4 strategy scores per
-- (date, symbol) are preserved as distinct rows. MODIFY ORDER BY cannot add
-- an existing column in ClickHouse, so we recreate the table.
-- confidence_scores holds scoring outputs only (no training data) — safe to drop.
DROP TABLE IF EXISTS analysis.confidence_scores;

CREATE TABLE analysis.confidence_scores
(
    `score_date`       Date,
    `symbol`           LowCardinality(String),
    `next_expiry`      Date,
    `confidence`       Float32,
    `expected_pnl_pct` Float32,
    `features_json`    String,
    `version`          UInt64 DEFAULT toUnixTimestamp(now()),
    `strategy_type`    LowCardinality(String) DEFAULT 'iron_fly'
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (score_date, symbol, strategy_type)
SETTINGS index_granularity = 8192;
