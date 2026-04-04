CREATE TABLE IF NOT EXISTS market.mf_schemes
(
    scheme_code     UInt32,
    scheme_name     String,
    fund_house      String,
    scheme_type     String,   -- Equity, Debt, Hybrid, etc
    scheme_category String,   -- Large Cap, ELSS, Liquid, etc
    is_active       UInt8,
    added_on        Date
)
ENGINE = ReplacingMergeTree
ORDER BY scheme_code