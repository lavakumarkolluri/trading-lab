-- Migration : 041
-- Table     : analysis.pattern_features
-- Purpose   : Add options-market context features (PCR, IV rank, max pain)
--             populated by option_chain_pipeline going forward.
--             Existing rows default to 0 — only new events carry real values.

ALTER TABLE analysis.pattern_features ADD COLUMN IF NOT EXISTS pcr Float64 DEFAULT 0;
ALTER TABLE analysis.pattern_features ADD COLUMN IF NOT EXISTS iv_rank Float64 DEFAULT 0;
ALTER TABLE analysis.pattern_features ADD COLUMN IF NOT EXISTS iv_percentile Float64 DEFAULT 0;
ALTER TABLE analysis.pattern_features ADD COLUMN IF NOT EXISTS max_pain_dist_pct Float64 DEFAULT 0;
