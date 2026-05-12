-- Add liquidity check columns to edge_analysis and weekend_theta_trades
ALTER TABLE analysis.edge_analysis
    ADD COLUMN IF NOT EXISTS oi_ce        Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS oi_pe        Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS liquidity_ok Int8    DEFAULT 0;

ALTER TABLE analysis.weekend_theta_trades
    ADD COLUMN IF NOT EXISTS oi_ce        Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS oi_pe        Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS liquidity_ok Int8    DEFAULT 0;
