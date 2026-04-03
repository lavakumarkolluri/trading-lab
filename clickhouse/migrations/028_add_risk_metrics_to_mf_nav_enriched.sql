-- Migration : 028
-- Description: Add risk metric columns to market.mf_nav_enriched
-- Created    : 2026-04-03

ALTER TABLE market.mf_nav_enriched
    ADD COLUMN IF NOT EXISTS volatility_1y    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sharpe_1y        Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sortino_1y       Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_drawdown_pct Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_drawdown_days UInt32 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS beta_vs_nifty    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS consistency_score Float64 DEFAULT 0