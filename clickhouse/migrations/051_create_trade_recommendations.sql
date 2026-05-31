-- 051_create_trade_recommendations.sql
-- Daily trade recommendations (production) + compounding backtest simulation

CREATE TABLE IF NOT EXISTS analysis.trade_recommendations
(
    rec_date         Date,
    symbol           LowCardinality(String),
    expiry           Date,
    strategy         LowCardinality(String),   -- iron_condor|bull_put|bear_call|straddle
    short_n          UInt8,
    wing_m           UInt8,
    strike_step      Float32,
    atm_strike       Float32,
    short_ce_strike  Float32,
    short_pe_strike  Float32,
    long_ce_strike   Float32,
    long_pe_strike   Float32,
    -- Pricing at recommendation time (prev EOD)
    short_ce_entry   Float32,
    short_pe_entry   Float32,
    long_ce_entry    Float32,
    long_pe_entry    Float32,
    net_credit       Float32,
    max_loss         Float32,
    -- Position sizing
    lots             UInt8,
    capital_at_risk  Float32,           -- max_loss * lots * lot_size
    -- Decision inputs
    confidence       Float32,           -- from confidence_scores (0-100)
    pcr_bias         Float32,           -- pcr_oi used for direction bias
    iv_skew          Float32,           -- iv_skew at entry
    -- Outcome (filled after expiry)
    pnl_pts          Float32   DEFAULT 0,
    pnl_amount       Float32   DEFAULT 0,   -- pnl_pts * lots * lot_size
    outcome          LowCardinality(String) DEFAULT '',  -- pending|win|loss|skipped
    version          UInt64    DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (rec_date, symbol);

-- Compounding simulation results (from --backtest mode)
CREATE TABLE IF NOT EXISTS analysis.strategy_simulation
(
    sim_date         Date,
    symbol           LowCardinality(String),
    expiry           Date,
    strategy         LowCardinality(String),
    short_n          UInt8,
    wing_m           UInt8,
    confidence       Float32,
    lots             UInt8,
    lot_size         UInt16,
    pnl_pts          Float32,
    pnl_amount       Float32,
    capital_before   Float32,
    capital_after    Float32,
    skipped          UInt8   DEFAULT 0,   -- 1 if confidence < threshold
    version          UInt64  DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (sim_date, symbol);
