CREATE TABLE IF NOT EXISTS market.nifty_live
(
    timestamp       DateTime,
    nifty_spot      Float64,
    vix             Float64,
    pcr             Float64 DEFAULT 0,
    advance_decline Float64 DEFAULT 0,
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY timestamp