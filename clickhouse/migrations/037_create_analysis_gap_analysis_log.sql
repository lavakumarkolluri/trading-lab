-- Migration : 037
-- Description: Log of LLM-driven gap analysis runs — missed events and false positives
-- Created    : 2026-04-04
--
-- Populated by: gap_analyzer.py (weekly, or triggered when false_negative_rate > 40%)
-- Key design:
--   • run_type: 'false_negative' — events we missed (no prediction existed)
--               'false_positive' — predictions that did not materialise
--               'full'           — both passes in one run
--   • suggested_features: LLM output listing new feature ideas to add to
--     pattern_feature_extractor.py (raw text, reviewed by developer before acting)
--   • suggested_pattern_updates: LLM output suggesting changes to conditions_json
--     for existing patterns (reviewed before applying)
--   • action_taken: developer fills this after reviewing the LLM suggestions,
--     e.g. "Added volume_z20 feature in v1.1" or "Tightened rsi_14 < 30 in pattern X"
--   • model_used / tokens_used: cost tracking per run
--   • This table is append-only (MergeTree, not ReplacingMergeTree) because
--     every gap analysis run is a distinct historical record worth keeping

CREATE TABLE IF NOT EXISTS analysis.gap_analysis_log
(
    id                          UUID DEFAULT generateUUIDv4(),
    analysis_date               Date,
    run_type                    String,             -- 'false_negative' | 'false_positive' | 'full'
    date_range_start            Date,               -- events reviewed from this date
    date_range_end              Date,               -- events reviewed to this date
    symbols_reviewed            UInt32 DEFAULT 0,
    missed_events               UInt32 DEFAULT 0,   -- false negatives in window
    false_positives             UInt32 DEFAULT 0,
    false_negative_rate         Float64 DEFAULT 0,  -- missed / total_events_in_window
    false_positive_rate         Float64 DEFAULT 0,  -- wrong / total_predictions_in_window
    llm_summary                 String DEFAULT '',  -- LLM's top-level diagnosis
    suggested_features          String DEFAULT '',  -- raw LLM text: new feature ideas
    suggested_pattern_updates   String DEFAULT '',  -- raw LLM text: pattern refinements
    action_taken                String DEFAULT '',  -- developer fills after review
    model_used                  String DEFAULT '',  -- e.g. 'claude-sonnet-4-6'
    tokens_used                 UInt32 DEFAULT 0,
    llm_cost_usd                Float64 DEFAULT 0,
    created_at                  DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (analysis_date, run_type)
