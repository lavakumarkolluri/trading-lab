-- Migration 055: Add TTL to market.options_chain to prevent unbounded disk growth.
-- Intraday scraper inserts ~130 snapshots/day; without TTL this grows indefinitely.
-- Retains 90 days of intraday data (enough for backtests), drops older rows automatically.
ALTER TABLE market.options_chain
    MODIFY TTL timestamp + INTERVAL 90 DAY DELETE;
