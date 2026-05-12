-- Edge analysis results: per-expiry straddle edge statistics
CREATE TABLE IF NOT EXISTS analysis.edge_analysis
(
    expiry_date          Date,
    symbol               LowCardinality(String),
    day_of_week          String,
    t1_date              Date,
    spot_t1              Float64,
    spot_expiry          Float64,
    atm_strike           Float64,
    entry_1dte           Float64,
    exit_1dte            Float64,
    profit_1dte_pts      Float64,
    profit_1dte_pct      Float64,
    win_1dte             Int8,
    estimated_0dte_entry Float64,
    profit_0dte_pts      Float64,
    profit_0dte_pct      Float64,
    win_0dte             Int8,
    implied_move_pct     Float64,
    realized_move_pct    Float64,
    vrp                  Float64,
    best_strike          Float64,
    best_profit_pts      Float64,
    best_profit_pct      Float64,
    best_dist_from_atm   Float64,
    atm_profit_pts       Float64,
    iv_rank              Float64,
    iv_percentile        Float64,
    atm_iv               Float64,
    pcr                  Float64,
    fii_net_3d           Float64,
    run_date             Date DEFAULT today()
)
ENGINE = ReplacingMergeTree(run_date)
ORDER BY (symbol, expiry_date);
