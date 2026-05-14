-- cleanup_log: full history of every maintenance / cleanup task run.
-- Written by scheduler.py job_cleanup(); read by System Health dashboard.
CREATE TABLE IF NOT EXISTS system_meta.cleanup_log (
    task_name    LowCardinality(String),
    status       LowCardinality(String),  -- ok | skipped | error
    items_freed  Int64 DEFAULT 0,         -- objects / files / images removed
    bytes_freed  Int64 DEFAULT 0,         -- bytes freed (0 when unknown)
    detail       String DEFAULT '',       -- JSON blob with extra context
    ran_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (task_name, ran_at);
