-- Migration : 031
-- Description: Pattern library — named, versioned, reusable trading patterns
-- Created    : 2026-04-04
--
-- Populated by: pattern_builder.py (rule-based) + gap_analyzer.py (LLM-suggested)
-- Key design:
--   • pattern_id is a human-readable slug, e.g. 'rsi_oversold_vol_spike'
--   • conditions_json stores the rule thresholds as JSON, e.g.:
--       {"rsi_14": {"lt": 35}, "volume_z5": {"gt": 2.0}}
--     Supported operators: lt, lte, gt, gte, eq, between, in
--   • created_by: 'system' (seed patterns) | 'llm' | 'manual'
--   • win_rate_* and avg_return_* updated by backtest_engine.py after each run
--   • is_active = 0 to soft-delete a pattern without losing history

CREATE TABLE IF NOT EXISTS analysis.patterns
(
    pattern_id              String,             -- slug: 'rsi_oversold_vol_spike'
    label                   String,             -- human-readable: "RSI Oversold + Volume Surge"
    description             String DEFAULT '',
    hypothesis              String DEFAULT '',  -- LLM-generated causal reasoning
    feature_version         String DEFAULT 'v1',
    conditions_json         String DEFAULT '{}',-- JSON rule thresholds

    -- Backtest aggregate stats (updated by backtest_engine.py)
    total_matches           UInt32 DEFAULT 0,
    win_rate_1d             Float64 DEFAULT 0,  -- % of matches with +return next day
    win_rate_3d             Float64 DEFAULT 0,
    win_rate_5d             Float64 DEFAULT 0,
    avg_return_1d           Float64 DEFAULT 0,
    avg_return_3d           Float64 DEFAULT 0,
    avg_return_5d           Float64 DEFAULT 0,
    sharpe_1d               Float64 DEFAULT 0,
    max_drawdown_3d         Float64 DEFAULT 0,  -- worst 3d loss across all matches
    last_backtested         Date DEFAULT '1970-01-01',

    -- Metadata
    created_at              DateTime DEFAULT now(),
    created_by              String DEFAULT 'system',    -- 'system' | 'llm' | 'manual'
    is_active               UInt8 DEFAULT 1,
    version                 UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY pattern_id
