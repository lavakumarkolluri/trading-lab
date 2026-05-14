ALTER TABLE analysis.confidence_scores
    ADD COLUMN IF NOT EXISTS strategy_type LowCardinality(String) DEFAULT 'iron_fly';
