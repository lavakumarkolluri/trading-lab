-- 087_add_model_version_to_confidence_scores.sql
-- Tracks which model version produced each score.
-- Enables before/after AUC comparison across retrain events.

ALTER TABLE analysis.confidence_scores
    ADD COLUMN IF NOT EXISTS model_version String DEFAULT '';
