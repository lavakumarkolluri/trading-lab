ALTER TABLE analysis.scorecard
    ADD COLUMN IF NOT EXISTS strategy_type LowCardinality(String) DEFAULT 'iron_fly';
