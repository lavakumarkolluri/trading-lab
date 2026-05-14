ALTER TABLE trades.open_positions
    ADD COLUMN IF NOT EXISTS kite_ce_order_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS kite_pe_order_id String DEFAULT '';

ALTER TABLE trades.trade_outcomes
    ADD COLUMN IF NOT EXISTS kite_ce_order_id String DEFAULT '',
    ADD COLUMN IF NOT EXISTS kite_pe_order_id String DEFAULT '';
