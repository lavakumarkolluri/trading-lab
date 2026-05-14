-- Strategy graduation tracking table.
-- Stores the current stage and gate metrics for each strategy.
-- Stage 1=Backtest, 2=Paper, 3=Micro-live, 4=Full-live.
-- graduation_gate.py recomputes and upserts this daily.

CREATE TABLE IF NOT EXISTS analysis.strategy_graduation
(
    strategy_id        LowCardinality(String),   -- e.g. 'iron_fly_0dte'
    strategy_name      String,
    stage              UInt8,                    -- 1/2/3/4
    stage_label        LowCardinality(String),   -- 'BACKTEST'/'PAPER'/'MICRO_LIVE'/'FULL_LIVE'
    stage_since        Date,

    -- Stage 1 gate metrics (from backtest tables)
    bt_oos_trades      UInt32,
    bt_win_rate        Float32,
    bt_sharpe          Float32,
    bt_years           Float32,

    -- Stage 1 gate pass flags
    bt_gate_trades     UInt8,    -- 1=pass, 0=fail
    bt_gate_win_rate   UInt8,
    bt_gate_sharpe     UInt8,
    bt_gate_years      UInt8,

    -- Stage 2 gate metrics (from paper trade tables)
    paper_trades       UInt32,
    paper_win_rate     Float32,
    paper_net_pnl      Float32,
    paper_vs_bt_delta  Float32,  -- paper_win_rate - bt_win_rate (negative = drift)

    -- Stage 2 gate pass flags
    paper_gate_trades  UInt8,
    paper_gate_win_rate UInt8,
    paper_gate_pnl     UInt8,

    -- Stage 3 gate metrics (from micro-live trade tables)
    micro_trades       UInt32,
    micro_win_rate     Float32,
    micro_net_pnl      Float32,

    -- Stage 3 gate pass flags
    micro_gate_trades  UInt8,
    micro_gate_win_rate UInt8,
    micro_gate_pnl     UInt8,

    -- Gate thresholds (stored for auditing)
    gate_bt_min_trades UInt32    DEFAULT 50,
    gate_bt_min_wr     Float32   DEFAULT 0.55,
    gate_bt_min_sharpe Float32   DEFAULT 0.50,
    gate_bt_min_years  Float32   DEFAULT 2.0,
    gate_paper_min_trades UInt32 DEFAULT 30,
    gate_paper_max_drift  Float32 DEFAULT -0.10,
    gate_micro_min_trades UInt32 DEFAULT 20,
    gate_micro_max_drift  Float32 DEFAULT -0.15,

    updated_at         DateTime  DEFAULT now(),
    version            UInt64    DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY strategy_id;
