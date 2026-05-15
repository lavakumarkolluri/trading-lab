-- Fix options_eod_summary ORDER BY to include symbol.
-- Previous ORDER BY (date, expiry) caused NIFTY and FINNIFTY rows (same Tuesday expiry)
-- to collide, with the later-inserted symbol silently replacing the earlier one.
-- Solution: DROP + CREATE with ORDER BY (symbol, date, expiry).
-- Data must be re-backfilled via options_eod_summary_pipeline after this migration.

DROP TABLE IF EXISTS market.options_eod_summary;

CREATE TABLE market.options_eod_summary
(
    `date`             Date,
    `symbol`           LowCardinality(String) DEFAULT 'NIFTY',
    `expiry`           Date,
    `nifty_spot`       Float64,
    `total_ce_oi`      UInt64  DEFAULT 0,
    `total_pe_oi`      UInt64  DEFAULT 0,
    `pcr`              Float64 DEFAULT 0,
    `max_pain_strike`  Float64 DEFAULT 0,
    `atm_strike`       Float64 DEFAULT 0,
    `atm_ce_iv`        Float64 DEFAULT 0,
    `atm_pe_iv`        Float64 DEFAULT 0,
    `iv_rank`          Float64 DEFAULT 0,
    `iv_percentile`    Float64 DEFAULT 0,
    `version`          UInt64  DEFAULT 0,
    `ce_wall_strike`   Float64 DEFAULT 0,
    `pe_wall_strike`   Float64 DEFAULT 0,
    `iv_skew`          Float64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, date, expiry)
SETTINGS index_granularity = 8192;
