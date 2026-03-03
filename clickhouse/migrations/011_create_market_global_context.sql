CREATE TABLE IF NOT EXISTS market.global_context
(
    timestamp       DateTime,
    sgx_nifty       Float64 DEFAULT 0,
    dow_futures     Float64 DEFAULT 0,
    crude_oil       Float64 DEFAULT 0,
    usd_inr         Float64 DEFAULT 0,
    us_vix          Float64 DEFAULT 0,
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY timestamp