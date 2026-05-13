ALTER TABLE analysis.trade_recommendations
    ADD COLUMN IF NOT EXISTS term_slope Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS skew_2pct  Float64 DEFAULT 0;
