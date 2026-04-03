-- Migration : 027
-- Description: Add CAGR and return columns to market.mf_nav_enriched
-- Created    : 2026-04-03

ALTER TABLE market.mf_nav_enriched
    ADD COLUMN IF NOT EXISTS return_1m    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_3m    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_6m    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_1y    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_3y    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_5y    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS return_10y   Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cagr_1y      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cagr_3y      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cagr_5y      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cagr_10y     Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sip_return_1y Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sip_return_3y Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sip_return_5y Float64 DEFAULT 0