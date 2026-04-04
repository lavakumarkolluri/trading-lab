-- Migration : 034
-- Description: Star ratings per pattern (and optionally per pattern × symbol)
-- Created    : 2026-04-04
--
-- Populated by: star_rater.py (runs after each backtest_engine update)
-- Key design:
--   • symbol = '*' for the aggregate (all-symbol) rating of a pattern
--   • symbol = 'RELIANCE.NS' for a symbol-specific override
--   • stars: 1–5  (minimum 3 required for prediction_engine to surface a signal)
--
--   Rating formula:
--     score = (win_rate_1d        × 0.35)
--           + (sample_adequacy    × 0.20)   -- log(n)/log(100), capped at 1.0
--           + (recency_score      × 0.20)   -- % of matches in last 90 days
--           + (risk_adj_return    × 0.25)   -- normalised Sharpe proxy
--     stars = CEIL(score / 0.20)            -- maps [0,1] → 1–5
--
--   needs_more_data = 1 when total_matches < 10 (backtest_engine sets this flag)

CREATE TABLE IF NOT EXISTS analysis.pattern_ratings
(
    pattern_id          String,
    symbol              String,         -- '*' = all-symbol aggregate
    stars               UInt8,          -- 1–5
    win_rate            Float64,        -- decimal, e.g. 0.72 = 72%
    sample_size         UInt32,
    recency_score       Float64,        -- [0,1] — fraction of matches in last 90d
    risk_adj_return     Float64,        -- Sharpe proxy (normalised to [0,1])
    needs_more_data     UInt8 DEFAULT 0,-- 1 when sample_size < 10
    rating_reason       String DEFAULT '',  -- brief human-readable explanation
    rated_at            DateTime DEFAULT now(),
    version             UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (pattern_id, symbol)
