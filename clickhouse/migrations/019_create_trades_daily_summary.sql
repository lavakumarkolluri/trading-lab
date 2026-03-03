CREATE TABLE IF NOT EXISTS trades.daily_summary
(
    date                Date,
    trade_mode          String,
    total_trades        UInt16 DEFAULT 0,
    winning_trades      UInt16 DEFAULT 0,
    losing_trades       UInt16 DEFAULT 0,
    win_rate            Float64 DEFAULT 0,
    total_pnl           Float64 DEFAULT 0,
    avg_profit_capture  Float64 DEFAULT 0,
    best_trade_pnl      Float64 DEFAULT 0,
    worst_trade_pnl     Float64 DEFAULT 0,
    avg_vix             Float64 DEFAULT 0,
    notes               String DEFAULT '',
    version             UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (trade_mode, date)