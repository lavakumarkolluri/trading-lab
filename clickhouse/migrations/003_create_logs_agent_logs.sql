CREATE TABLE IF NOT EXISTS logs.agent_logs
(
    id              UUID DEFAULT generateUUIDv4(),
    timestamp       DateTime DEFAULT now(),
    agent_name      String,
    run_id          UUID,
    level           String,
    category        String,
    message         String,
    details         String DEFAULT '',
    symbol          String DEFAULT '',
    file_path       String DEFAULT '',
    rows_affected   UInt32 DEFAULT 0,
    duration_ms     UInt32 DEFAULT 0,
    is_resolved     UInt8 DEFAULT 0,
    resolution      String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (agent_name, timestamp)