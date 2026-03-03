CREATE TABLE IF NOT EXISTS market.events
(
    event_date      Date,
    event_time      String DEFAULT '',
    event_name      String,
    impact          String,
    description     String DEFAULT '',
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (event_date, event_name)