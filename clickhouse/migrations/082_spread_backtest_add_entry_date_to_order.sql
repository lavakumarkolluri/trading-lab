-- 082_spread_backtest_add_entry_date_to_order.sql
-- Add entry_date to ORDER BY so multiple N-DTE entry dates per expiry don't collide.
-- Current ORDER BY (symbol, expiry, strategy, short_n, wing_m) causes ReplacingMergeTree
-- to keep only one row per expiry+strategy — all N-DTE entries for the same expiry
-- would overwrite each other. entry_date must be in the key.

CREATE TABLE IF NOT EXISTS analysis.spread_backtest_v2
(
    symbol           LowCardinality(String),
    expiry           Date,
    entry_date       Date,
    strategy         LowCardinality(String),
    short_n          UInt8,
    wing_m           UInt8,
    strike_step      Float32,
    atm_strike       Float32,
    short_ce_strike  Float32,
    short_pe_strike  Float32,
    long_ce_strike   Float32,
    long_pe_strike   Float32,
    short_ce_entry   Float32,
    short_pe_entry   Float32,
    long_ce_entry    Float32,
    long_pe_entry    Float32,
    short_ce_settle  Float32,
    short_pe_settle  Float32,
    long_ce_settle   Float32,
    long_pe_settle   Float32,
    net_credit       Float32,
    max_loss         Float32,
    exit_cost        Float32,
    pnl_pts          Float32,
    pnl_pct          Float32,
    target           UInt8,
    version          UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, expiry, entry_date, strategy, short_n, wing_m);

INSERT INTO analysis.spread_backtest_v2 SELECT * FROM analysis.spread_backtest;

RENAME TABLE analysis.spread_backtest TO analysis.spread_backtest_old;
RENAME TABLE analysis.spread_backtest_v2 TO analysis.spread_backtest;

DROP TABLE analysis.spread_backtest_old;
