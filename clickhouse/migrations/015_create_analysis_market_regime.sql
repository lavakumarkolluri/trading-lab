CREATE TABLE IF NOT EXISTS analysis.market_regime
(
    date                Date,
    vix_regime          String,
    trend               String DEFAULT '',
    momentum            String DEFAULT '',
    fii_stance          String DEFAULT '',
    opportunity_score   UInt8 DEFAULT 0,
    trade_recommended   UInt8 DEFAULT 0,
    reason              String DEFAULT '',
    version             UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY date