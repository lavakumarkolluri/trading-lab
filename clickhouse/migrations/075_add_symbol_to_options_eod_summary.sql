-- 075_add_symbol_to_options_eod_summary.sql
-- CRIT-003: options_eod_summary was NIFTY-only; adding symbol column
-- enables per-symbol IV rank/percentile/PCR features for all 4 instruments.
--
-- Using DEFAULT 'NIFTY' so all existing rows remain valid.
-- (date, expiry) pairs are unique per symbol in practice because each symbol
-- has a different weekly expiry day, so no key conflicts arise.

ALTER TABLE market.options_eod_summary
    ADD COLUMN IF NOT EXISTS symbol LowCardinality(String) DEFAULT 'NIFTY';
