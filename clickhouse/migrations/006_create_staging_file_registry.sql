CREATE TABLE IF NOT EXISTS staging.file_registry
(
    id              UUID DEFAULT generateUUIDv4(),
    created_at      DateTime DEFAULT now(),
    agent_name      String,
    file_path       String,
    file_date       Date,
    status          String DEFAULT 'pending',
    row_count       UInt32 DEFAULT 0,
    loaded_at       Nullable(DateTime),
    error_message   String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (agent_name, file_date)