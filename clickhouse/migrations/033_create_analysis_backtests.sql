-- Migration : 033
-- Description: Individual backtest results — one row per pattern × signal date × symbol
-- Created    : 2026-04-04
--
-- Populated by: backtest_engine.py
-- Key design:
--   • signal_date = the day the pattern was matched (before the event)
--   • entry_price = close on signal_date (simulated entry at next day open is a future option)
--   • return_Nd   = (close[signal_date + N] - entry_price) / entry_price * 100
--   • hit_2pct_Nd = 1 if return_Nd >= 2.0 (the detection threshold used for events)
--   • feature_snapshot: JSON copy of the feature vector at signal time — allows
--     reanalysis without re-joining pattern_features
--   • run_id groups all rows from a single backtest_engine.py run for easy invalidation

CREATE TABLE IF NOT EXISTS analysis.backtests
(
    pattern_id          String,
    symbol              String,
    market              String,
    signal_date         Date,           -- date pattern was matched
    entry_price         Float64,        -- close on signal_date
    return_1d           Float64 DEFAULT 0,
    return_3d           Float64 DEFAULT 0,
    return_5d           Float64 DEFAULT 0,
    hit_2pct_1d         UInt8 DEFAULT 0,
    hit_2pct_3d         UInt8 DEFAULT 0,
    hit_2pct_5d         UInt8 DEFAULT 0,
    max_adverse_3d      Float64 DEFAULT 0,  -- worst intraday low vs entry over 3d (drawdown)
    feature_snapshot    String DEFAULT '{}',-- JSON snapshot of pattern_features at signal
    run_id              String DEFAULT '',  -- groups rows from same backtest run
    computed_at         DateTime DEFAULT now(),
    version             UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (pattern_id, symbol, signal_date)
