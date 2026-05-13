ALTER TABLE trades.open_positions
    ADD COLUMN IF NOT EXISTS lots Int8 DEFAULT 1;

ALTER TABLE trades.trade_outcomes
    ADD COLUMN IF NOT EXISTS lots Int8 DEFAULT 1;
