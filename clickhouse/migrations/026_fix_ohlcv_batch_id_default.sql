-- Migration : 026
-- Description: Add DEFAULT generateUUIDv4() to batch_id in market.ohlcv_daily
-- Created    : 2026-03-21

ALTER TABLE market.ohlcv_daily
    MODIFY COLUMN batch_id UUID DEFAULT generateUUIDv4()