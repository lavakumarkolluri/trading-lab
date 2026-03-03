CREATE TABLE IF NOT EXISTS market.options_chain
(
    timestamp       DateTime,
    expiry          Date,
    strike          Float64,
    option_type     String,
    ltp             Float64,
    bid             Float64 DEFAULT 0,
    ask             Float64 DEFAULT 0,
    iv              Float64 DEFAULT 0,
    delta           Float64 DEFAULT 0,
    theta           Float64 DEFAULT 0,
    oi              UInt64 DEFAULT 0,
    oi_change       Int64 DEFAULT 0,
    volume          UInt64 DEFAULT 0,
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (timestamp, expiry, strike, option_type)