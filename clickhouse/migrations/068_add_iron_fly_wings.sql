-- Iron fly: short ATM straddle + long OTM wings
ALTER TABLE trades.open_positions
    ADD COLUMN IF NOT EXISTS wing_ce_strike Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wing_pe_strike Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wing_ce_ltp    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wing_pe_ltp    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS net_premium    Float64 DEFAULT 0;  -- straddle ltp - wing cost

ALTER TABLE trades.trade_outcomes
    ADD COLUMN IF NOT EXISTS wing_ce_strike Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wing_pe_strike Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wing_ce_ltp    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wing_pe_ltp    Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS net_premium    Float64 DEFAULT 0;
