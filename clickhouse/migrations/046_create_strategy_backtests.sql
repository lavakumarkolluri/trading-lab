-- Strategy backtest trades: one row per trade for any custom strategy
CREATE TABLE IF NOT EXISTS analysis.strategy_trades
(
    run_id        String,
    strategy      String,          -- e.g. 'breakout_3m', 'momentum_factor'
    universe      String,          -- e.g. 'nse', 'bse'
    symbol        String,
    entry_month   Date,
    exit_month    Date,
    hold_months   UInt16,
    entry_px      Float64,
    exit_px       Float64,
    raw_ret       Float64,
    net_ret       Float64,
    is_open       UInt8 DEFAULT 0,
    computed_at   DateTime DEFAULT now(),
    version       UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY strategy
ORDER BY (strategy, universe, run_id, symbol, entry_month)
SETTINGS index_granularity = 8192;

-- Strategy backtest run-level summary
CREATE TABLE IF NOT EXISTS analysis.strategy_runs
(
    run_id          String,
    strategy        String,
    universe        String,
    start_date      Date,
    end_date        Date,
    period          String,    -- 'full', 'train', 'test'
    n_trades        UInt32,
    win_rate        Float64,
    avg_net_ret     Float64,
    median_net_ret  Float64,
    best_trade      Float64,
    worst_trade     Float64,
    avg_hold_months Float64,
    sharpe          Float64,
    bm_monthly_ret  Float64,
    alpha_monthly   Float64,
    computed_at     DateTime DEFAULT now(),
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (strategy, universe, run_id, period)
SETTINGS index_granularity = 8192;
