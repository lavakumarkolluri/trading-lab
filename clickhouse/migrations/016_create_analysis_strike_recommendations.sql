CREATE TABLE IF NOT EXISTS analysis.strike_recommendations
(
    timestamp               DateTime,
    expiry                  Date,
    nifty_spot              Float64,
    vix                     Float64,
    recommended_call        Float64 DEFAULT 0,
    recommended_put         Float64 DEFAULT 0,
    expected_premium_call   Float64 DEFAULT 0,
    expected_premium_put    Float64 DEFAULT 0,
    total_premium           Float64 DEFAULT 0,
    breakeven_upper         Float64 DEFAULT 0,
    breakeven_lower         Float64 DEFAULT 0,
    probability_profit      Float64 DEFAULT 0,
    confidence_score        UInt8 DEFAULT 0,
    version                 UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY timestamp