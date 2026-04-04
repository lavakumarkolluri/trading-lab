-- Migration : 036
-- Description: Actual outcomes for each prediction — the ground truth validation layer
-- Created    : 2026-04-04
--
-- Populated by: validation_engine.py (runs T+1, T+3, T+5 after prediction_date)
-- Key design:
--   • One row per prediction (matches analysis.predictions 1:1 on prediction_date × symbol × pattern_id)
--   • actual_pct_Nd = forward return from entry_price (close on prediction_date)
--   • was_correct_1d = 1 if actual_pct_1d direction matches predicted_direction
--     AND abs(actual_pct_1d) >= predicted_min_pct
--   • hit_threshold = 1 if any of 1d/3d/5d windows crossed predicted_min_pct in
--     the correct direction (generous scoring — at least one window succeeded)
--   • This table drives gap_analyzer.py:
--     - False negatives: events in detected_events with NO matching prediction row
--     - False positives: rows here where hit_threshold = 0
--   • Also drives star_rater.py recency scoring via rated_at vs prediction_date

CREATE TABLE IF NOT EXISTS analysis.prediction_outcomes
(
    prediction_date         Date,
    target_date             Date,
    symbol                  String,
    market                  String,
    pattern_id              String,
    predicted_direction     String,
    predicted_min_pct       Float64,
    confidence_pct          Float64,

    -- Actual forward returns from close on prediction_date
    entry_price             Float64 DEFAULT 0,
    actual_pct_1d           Float64 DEFAULT 0,
    actual_pct_3d           Float64 DEFAULT 0,
    actual_pct_5d           Float64 DEFAULT 0,

    -- Correctness flags
    was_correct_1d          UInt8 DEFAULT 0,    -- direction + magnitude both correct at T+1
    was_correct_3d          UInt8 DEFAULT 0,
    was_correct_5d          UInt8 DEFAULT 0,
    hit_threshold           UInt8 DEFAULT 0,    -- 1 if any window met predicted_min_pct

    outcome_notes           String DEFAULT '',
    validated_at            DateTime DEFAULT now(),
    version                 UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (prediction_date, symbol, pattern_id)
