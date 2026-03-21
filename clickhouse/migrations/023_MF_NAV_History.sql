CREATE TABLE market.mf_nav
(
    date        Date,
    scheme_code UInt32,
    nav         Float64,
    version     UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (scheme_code, date)