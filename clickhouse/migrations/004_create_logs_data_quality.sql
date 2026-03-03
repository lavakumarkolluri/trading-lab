CREATE TABLE IF NOT EXISTS logs.data_quality
(
    id              UUID DEFAULT generateUUIDv4(),
    timestamp       DateTime DEFAULT now(),
    agent_name      String,
    run_id          UUID,
    symbol          String DEFAULT '',
    file_path       String DEFAULT '',
    check_name      String,
    check_status    String,
    expected        String DEFAULT '',
    actual          String DEFAULT '',
    rows_affected   UInt32 DEFAULT 0,
    action_taken    String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (timestamp, agent_name)