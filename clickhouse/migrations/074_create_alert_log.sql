-- 074_create_alert_log.sql
-- Stores all Telegram alerts sent by agents and scheduler for dashboard visibility

CREATE TABLE IF NOT EXISTS system_meta.alert_log
(
    alert_time  DateTime,
    source      LowCardinality(String),  -- monitor_agent / signal_agent / data_freshness_check / scheduler
    level       LowCardinality(String),  -- INFO / WARN / CRIT
    message     String,
    version     UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (alert_time, source);
