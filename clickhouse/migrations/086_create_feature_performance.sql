-- 086_create_feature_performance.sql
-- Rolling feature-to-win correlation and drift detection.
-- Populated weekly by drift_detector.py.

CREATE TABLE IF NOT EXISTS analysis.feature_performance
(
    computed_at   Date,
    symbol        LowCardinality(String),
    feature_name  LowCardinality(String),
    window_days   UInt8,         -- 30, 60, or 90
    n_trades      UInt16,        -- trades in window (need >= 10 for meaningful correlation)
    correlation   Float32,       -- Pearson(feature_value, win_label) in this window
    baseline_corr Float32,       -- 180-day baseline correlation (reference)
    drift_detected UInt8 DEFAULT 0,   -- 1 if |corr - baseline| > 0.15
    version       UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (computed_at, symbol, feature_name, window_days);

-- Retrain triggers emitted by drift_detector when 2+ features drift for same symbol.
-- Scheduler picks up acted_on=0 rows and runs confidence_scorer --compare.
CREATE TABLE IF NOT EXISTS analysis.retrain_triggers
(
    trigger_date  Date,
    symbol        LowCardinality(String),
    reason        String,          -- human-readable: "drift: iv_rank, vix (2 features)"
    drifted_features String,       -- comma-separated feature names
    triggered_at  DateTime DEFAULT now(),
    acted_on      UInt8 DEFAULT 0, -- 1 after confidence_scorer --compare completes
    version       UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (trigger_date, symbol);
