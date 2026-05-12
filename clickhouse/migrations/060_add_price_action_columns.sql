-- Add price action feature columns to backtest result tables
ALTER TABLE analysis.edge_analysis
    ADD COLUMN IF NOT EXISTS prev_range_pct     Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS range_vs_atr       Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS inside_bar         Int8    DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ma20_dist_pct      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ma50_dist_pct      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS above_ma20         Int8    DEFAULT 0,
    ADD COLUMN IF NOT EXISTS week_range_pct     Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS consec_inside_days Int32   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pa_available       Int8    DEFAULT 0;

ALTER TABLE analysis.weekend_theta_trades
    ADD COLUMN IF NOT EXISTS prev_range_pct     Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS range_vs_atr       Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS inside_bar         Int8    DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ma20_dist_pct      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ma50_dist_pct      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS above_ma20         Int8    DEFAULT 0,
    ADD COLUMN IF NOT EXISTS week_range_pct     Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS consec_inside_days Int32   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pa_available       Int8    DEFAULT 0;
