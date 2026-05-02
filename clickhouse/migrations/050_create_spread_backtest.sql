-- 050_create_spread_backtest.sql
-- Per-trade results for IC / Bull Put / Bear Call / Straddle strategy variants
-- All P&L figures include hedge leg costs (net credit = sell - buy legs)

CREATE TABLE IF NOT EXISTS analysis.spread_backtest
(
    symbol           LowCardinality(String),
    expiry           Date,
    entry_date       Date,
    strategy         LowCardinality(String),  -- straddle|iron_condor|bull_put|bear_call
    short_n          UInt8,   -- steps from ATM where short leg is placed (0 = ATM for straddle)
    wing_m           UInt8,   -- wing width in steps (0 for straddle = no wing)
    strike_step      Float32, -- pt interval for this symbol (50/100/25)
    atm_strike       Float32,
    -- Leg strikes
    short_ce_strike  Float32,
    short_pe_strike  Float32,
    long_ce_strike   Float32, -- 0 if no wing
    long_pe_strike   Float32, -- 0 if no wing
    -- Entry prices (previous day EOD)
    short_ce_entry   Float32,
    short_pe_entry   Float32,
    long_ce_entry    Float32,
    long_pe_entry    Float32,
    -- Settlement prices (expiry day EOD)
    short_ce_settle  Float32,
    short_pe_settle  Float32,
    long_ce_settle   Float32,
    long_pe_settle   Float32,
    -- P&L (hedge costs already included in net_credit)
    net_credit       Float32, -- premium received net of hedge cost
    max_loss         Float32, -- wing_m * step - net_credit (0 = unlimited for straddle)
    exit_cost        Float32, -- net settlement cost to close all legs
    pnl_pts          Float32, -- net_credit - exit_cost
    pnl_pct          Float32, -- pnl_pts / abs(max_loss) * 100 (NaN for straddle)
    target           UInt8,   -- 1 if pnl_pts > 0
    version          UInt64   DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, expiry, strategy, short_n, wing_m);

-- Summary statistics per symbol x strategy x params (for optimal param selection)
CREATE TABLE IF NOT EXISTS analysis.spread_optimal
(
    symbol           LowCardinality(String),
    strategy         LowCardinality(String),
    short_n          UInt8,
    wing_m           UInt8,
    n_trades         UInt32,
    win_rate         Float32,
    avg_pnl_pts      Float32,
    avg_pnl_pct      Float32,
    sharpe_pct       Float32,  -- mean/std of pnl_pct
    avg_net_credit   Float32,
    avg_max_loss     Float32,
    premium_to_risk  Float32,  -- avg_net_credit / (step * wing_m) * 100
    computed_at      DateTime  DEFAULT now(),
    version          UInt64    DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, strategy, short_n, wing_m);
