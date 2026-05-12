-- Track delta hedge actions for paper trading positions
CREATE TABLE IF NOT EXISTS trades.delta_hedges (
    trade_id        String,
    symbol          LowCardinality(String),
    hedge_time      DateTime,
    net_delta       Float64,
    hedge_lots      Float64,
    hedge_direction LowCardinality(String),  -- 'buy' or 'sell' futures
    spot_at_hedge   Float64,
    cumulative_lots Float64,
    version         UInt64 DEFAULT toUnixTimestamp(now())
) ENGINE = ReplacingMergeTree(version)
ORDER BY (trade_id, hedge_time);

-- Add net_delta tracking to open_positions
ALTER TABLE trades.open_positions
    ADD COLUMN IF NOT EXISTS net_delta      Float64 DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hedge_lots_cum Float64 DEFAULT 0;
