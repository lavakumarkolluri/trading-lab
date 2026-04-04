-- Migration : 032
-- Description: Maps which patterns matched which historical events
-- Created    : 2026-04-04
--
-- Populated by: pattern_builder.py and backtest_engine.py
-- Key design:
--   • One row per (pattern × symbol × event_date) match
--   • Used by backtest_engine to look up forward returns for matched events
--   • Also used by star_rater to compute recency_score (recent matches weighted higher)
--   • Join with analysis.pattern_features on (symbol, event_date) for full feature context
--   • Join with analysis.detected_events on (symbol, date=event_date) for outcome data

CREATE TABLE IF NOT EXISTS analysis.pattern_event_map
(
    pattern_id      String,
    symbol          String,
    market          String,
    event_date      Date,
    match_score     Float64 DEFAULT 1.0,    -- 1.0 for rule-based exact match;
                                            -- [0,1] for NN soft match (future)
    matched_by      String DEFAULT 'rule',  -- 'rule' | 'nn' | 'llm'
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (pattern_id, symbol, event_date)
