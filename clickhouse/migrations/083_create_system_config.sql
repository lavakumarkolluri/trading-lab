CREATE DATABASE IF NOT EXISTS system_meta;

-- ── Mutable operational config ─────────────────────────────────────────────────
-- Tunable at runtime via INSERT. Change min_confidence, risk limits, cleanup
-- frequency — anything operational. Use:
--   INSERT INTO system_meta.config VALUES ('min_confidence', '65', now());
CREATE TABLE IF NOT EXISTS system_meta.config
(
    key        String,
    value      String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key;

INSERT INTO system_meta.config (key, value, updated_at) VALUES
    ('min_confidence',               '60',  now()),
    ('target_pnl_inr',               '2000',now()),
    ('stop_pnl_inr',                 '1000',now()),
    ('trail_floor_pct',              '75',  now()),
    ('cleanup_interval_hours',       '6',   now()),
    ('paper_trade_future_min_months','3',   now());

-- ── IMMUTABLE stage boundaries — NEVER written by any pipeline ─────────────────
-- These dates define the data split for the 5-stage trading lifecycle.
-- Changing them retroactively would violate the no-look-ahead guarantee and
-- corrupt Stage 3 validation results.
--
-- HOW TO READ ONLY:
--   SELECT value FROM system_meta.stage_boundaries WHERE key = 'stage4_holdout_start'
--
-- WHO WRITES:
--   Only this migration. No pipeline, scheduler, or dashboard code may INSERT here.
--   If a date ever needs correction, create a NEW migration (084+) that explains why,
--   documents the impact on previous Stage 3 results, and requires explicit human review.
CREATE TABLE IF NOT EXISTS system_meta.stage_boundaries
(
    key        String,
    value      String,
    note       String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY key;

INSERT INTO system_meta.stage_boundaries (key, value, note) VALUES
    ('backtest_start_date',
     '2019-01-01',
     'Stage 2: earliest date used for backtest parameter identification'),
    ('walkforward_start_date',
     '2019-01-01',
     'Stage 3: walk-forward CV uses data from this date up to stage4_holdout_start'),
    ('stage4_holdout_start',
     '2024-05-15',
     'CRITICAL: Stage 3 training never uses data on or after this date. '
     'Stage 4 Paper Trade Historical covers this date → stage4_holdout_end.'),
    ('stage4_holdout_end',
     '2026-05-15',
     'Stage 4 Paper Trade Historical ends here. Stage 4 Paper Trade Future begins here.');
