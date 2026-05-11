-- Monthly breakout backtest results
-- Strategy: buy when monthly close > 3-month prior high; exit at 2x or daily close < prior week low
CREATE TABLE IF NOT EXISTS analysis.monthly_breakout_trades
(
    symbol          String,
    entry_date      Date,
    exit_date       Date,
    entry_price     Float64,
    exit_price      Float64,
    exit_reason     String,   -- 'target' | 'stop'
    holding_days    Int32,
    return_pct      Float64,  -- (exit/entry - 1) * 100
    xirr_annualized Float64,  -- annualized return as decimal (0.12 = 12%)
    run_date        Date DEFAULT today()
)
ENGINE = ReplacingMergeTree(run_date)
ORDER BY (symbol, entry_date);
