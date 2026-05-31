-- 085_create_trade_attributions.sql
-- Per-feature attribution scores for every closed loss.
-- Populated by loss_attributor.py after trades settle.

CREATE TABLE IF NOT EXISTS analysis.trade_attributions
(
    trade_id      String,
    symbol        LowCardinality(String),
    exit_date     Date,
    exit_reason   LowCardinality(String),    -- target|stop|eod|trail
    pnl_pts       Float32,
    feature_name  LowCardinality(String),
    feature_value Float32,
    win_mean      Float32,    -- population mean for winning trades (reference point)
    attribution   Float32,    -- (feature_val - win_mean) / win_std; positive = worsened outcome
    version       UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (trade_id, feature_name);
