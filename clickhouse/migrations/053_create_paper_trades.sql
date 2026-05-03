-- 053_create_paper_trades.sql
-- Paper trading tables for intraday straddle monitor

-- Open positions: one row per active paper trade
-- Closed by updating status via ReplacingMergeTree(version)
CREATE TABLE IF NOT EXISTS trades.open_positions
(
    trade_id        String,                          -- UUID, set at entry
    symbol          LowCardinality(String),          -- NIFTY / BANKNIFTY
    expiry          Date,
    entry_time      DateTime,
    strike          Float32,                         -- ATM strike at entry
    entry_ce_ltp    Float32,
    entry_pe_ltp    Float32,
    entry_premium   Float32,                         -- CE + PE at entry
    lot_size        UInt16,
    target_pts      Float32,                         -- premium drop to take profit
    stop_pts        Float32,                         -- premium rise to stop out
    target_inr      Float32,                         -- fixed: 2000
    stoploss_inr    Float32,                         -- fixed: 1000
    scorecard_conf  Float32,                         -- confidence score at entry (0-100)
    status          LowCardinality(String),          -- open / closed
    entry_features  String DEFAULT '{}',             -- JSON snapshot of signals at entry
    trailing_active UInt8 DEFAULT 0,                 -- 1 once target first hit
    peak_pnl_inr    Float32 DEFAULT 0,               -- highest INR profit seen
    trail_stop_inr  Float32 DEFAULT 0,               -- 75% of peak_pnl_inr
    last_checked    DateTime DEFAULT now(),
    version         UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (trade_id);

-- Trade outcomes: one row per completed trade (filled at exit)
CREATE TABLE IF NOT EXISTS trades.trade_outcomes
(
    trade_id        String,
    symbol          LowCardinality(String),
    expiry          Date,
    entry_time      DateTime,
    exit_time       DateTime,
    strike          Float32,
    entry_premium   Float32,
    exit_premium    Float32,                         -- straddle value at exit
    pnl_pts         Float32,                         -- entry_premium - exit_premium
    pnl_inr         Float32,                         -- pnl_pts * lot_size
    lot_size        UInt16,
    exit_reason     LowCardinality(String),          -- target / stop / eod
    scorecard_conf  Float32,
    entry_features  String,                          -- JSON snapshot of signals at entry
    version         UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (trade_id);
