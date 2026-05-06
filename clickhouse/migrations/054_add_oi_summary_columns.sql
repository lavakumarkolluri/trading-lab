-- 054_add_oi_summary_columns.sql
-- Add OI wall + IV skew columns to options_eod_summary (used by compute_oi_features.py)

ALTER TABLE market.options_eod_summary
    ADD COLUMN IF NOT EXISTS iv_skew         Float32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ce_wall_strike  Float32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pe_wall_strike  Float32 DEFAULT 0;
