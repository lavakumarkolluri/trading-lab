-- Migration : 040
-- Description: NSE F&O participant wise open interest (no. of contracts)
-- Created    : 2026-04-10
--
-- Source     : https://archives.nseindia.com/content/nsccl/fao_participant_oi_DDMMYYYY.csv
-- Populated  : participant_oi_pipeline.py (daily after market close)
-- Entities   : Client, DII, FII, Pro, TOTAL
--
-- All values are number of contracts (not rupees).
-- Net columns are pre-computed at insert time: Long - Short.
-- Always query with FINAL for strict dedup.

CREATE TABLE IF NOT EXISTS market.participant_oi
(
    date                        Date,
    entity                      String,     -- 'Client' | 'DII' | 'FII' | 'Pro' | 'TOTAL'

    -- Index Futures
    fut_index_long              Int64 DEFAULT 0,
    fut_index_short             Int64 DEFAULT 0,
    fut_index_net               Int64 DEFAULT 0,   -- long - short

    -- Stock Futures
    fut_stock_long              Int64 DEFAULT 0,
    fut_stock_short             Int64 DEFAULT 0,
    fut_stock_net               Int64 DEFAULT 0,

    -- Index Options
    opt_index_call_long         Int64 DEFAULT 0,
    opt_index_call_short        Int64 DEFAULT 0,
    opt_index_call_net          Int64 DEFAULT 0,

    opt_index_put_long          Int64 DEFAULT 0,
    opt_index_put_short         Int64 DEFAULT 0,
    opt_index_put_net           Int64 DEFAULT 0,

    -- Stock Options
    opt_stock_call_long         Int64 DEFAULT 0,
    opt_stock_call_short        Int64 DEFAULT 0,
    opt_stock_call_net          Int64 DEFAULT 0,

    opt_stock_put_long          Int64 DEFAULT 0,
    opt_stock_put_short         Int64 DEFAULT 0,
    opt_stock_put_net           Int64 DEFAULT 0,

    -- Totals
    total_long                  Int64 DEFAULT 0,
    total_short                 Int64 DEFAULT 0,
    total_net                   Int64 DEFAULT 0,

    version                     UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (date, entity)