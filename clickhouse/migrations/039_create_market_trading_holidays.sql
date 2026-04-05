-- Migration : 039
-- Description: NSE/BSE trading holiday calendar
-- Created    : 2026-04-05
--
-- Populated by: holidays_pipeline.py
-- Used by    : prediction_engine.py (next_trading_day)
--              validation_engine.py (expiry logic)
--              meta_pipeline.py (skip non-trading days)
--
-- exchange values: 'NSE' | 'BSE' | 'ALL'
-- holiday_type  : 'full_closure' | 'muhurat' | 'early_close'
-- source        : 'hardcoded' | 'nse_api' | 'manual'

CREATE TABLE IF NOT EXISTS market.trading_holidays
(
    holiday_date    Date,
    exchange        String DEFAULT 'NSE',
    description     String,
    holiday_type    String DEFAULT 'full_closure',
    source          String DEFAULT 'hardcoded',
    added_on        DateTime DEFAULT now(),
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (exchange, holiday_date)