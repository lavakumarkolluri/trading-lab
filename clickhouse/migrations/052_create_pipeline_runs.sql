-- 052_create_pipeline_runs.sql
-- Execution log for every scheduled pipeline run.
-- Used for: dependency gating, git-SHA drift detection, dashboard health grid.

CREATE DATABASE IF NOT EXISTS system_meta;

CREATE TABLE IF NOT EXISTS system_meta.pipeline_runs
(
    run_id        UUID            DEFAULT generateUUIDv4(),
    service       LowCardinality(String),
    run_date      Date            DEFAULT toDate(started_at),
    started_at    DateTime,
    finished_at   DateTime        DEFAULT toDateTime(0),
    status        LowCardinality(String),  -- running|success|failed|skipped
    rows_written  UInt64          DEFAULT 0,
    git_sha       LowCardinality(String)   DEFAULT 'unknown',
    error_msg     String          DEFAULT '',
    version       UInt64          DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (service, run_date, run_id)
PARTITION BY toYYYYMM(run_date);
