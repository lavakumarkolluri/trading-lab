CREATE TABLE IF NOT EXISTS logs.agent_runs
(
    run_id          UUID DEFAULT generateUUIDv4(),
    agent_name      String,
    started_at      DateTime DEFAULT now(),
    ended_at        Nullable(DateTime),
    status          String,
    total_symbols   UInt32 DEFAULT 0,
    success_count   UInt32 DEFAULT 0,
    failure_count   UInt32 DEFAULT 0,
    warning_count   UInt32 DEFAULT 0,
    total_rows      UInt32 DEFAULT 0,
    duration_ms     UInt32 DEFAULT 0,
    error_summary   String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (agent_name, started_at)