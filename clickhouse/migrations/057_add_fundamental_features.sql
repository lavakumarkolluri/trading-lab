-- Add fundamental quality features to pattern_features (v3)
ALTER TABLE analysis.pattern_features
    ADD COLUMN IF NOT EXISTS pe_ratio       Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pb_ratio       Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS roe            Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS profit_margins Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fcf_positive   Int8    DEFAULT 0;
