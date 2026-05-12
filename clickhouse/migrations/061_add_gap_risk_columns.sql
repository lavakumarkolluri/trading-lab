-- Add gap risk filter columns to weekend_theta_trades
ALTER TABLE analysis.weekend_theta_trades
    ADD COLUMN IF NOT EXISTS vix_friday  Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS event_week  Int8    DEFAULT 0;
