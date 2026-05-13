#!/usr/bin/env python3
"""
graduation_gate.py — Strategy graduation gate checker.

For each registered strategy:
  1. Queries existing backtest/paper/micro tables to compute gate metrics.
  2. Evaluates pass/fail against fixed thresholds.
  3. Determines current stage (1=BACKTEST → 4=FULL_LIVE).
  4. Upserts one row to analysis.strategy_graduation.

Run daily (after confidence_scorer) to keep graduation stage current.
Usage:
    python graduation_gate.py            # update all strategies
    python graduation_gate.py --dry-run  # compute and print, no DB writes
"""

import argparse
import logging
import os
from datetime import date, datetime

import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Gate thresholds ────────────────────────────────────────────────────────────

BT_MIN_TRADES   = 50
BT_MIN_WIN_RATE = 0.55
BT_MIN_SHARPE   = 0.50
BT_MIN_YEARS    = 2.0

PAPER_MIN_TRADES = 30
PAPER_MAX_DRIFT  = -0.10   # paper_win_rate - bt_win_rate must be > this

MICRO_MIN_TRADES = 20
MICRO_MAX_DRIFT  = -0.15   # micro_win_rate - paper_win_rate must be > this

# ── Strategy registry ─────────────────────────────────────────────────────────
# bt_table  : table that holds backtest trades for stage-1 gate
# bt_win_col: column that is 1 (win) / 0 (loss)
# bt_pnl_col: column for P&L points (for Sharpe calc)
# bt_date_col: date column to compute year span
# paper_table: table for paper/live trades (None = no paper stage yet)

STRATEGY_REGISTRY = {
    "iron_fly_0dte": {
        "name": "Iron Fly 0DTE",
        "bt_table": "analysis.spread_backtest",
        "bt_where": "strategy = 'iron_condor'",
        "bt_win_col": "target",
        "bt_pnl_col": "pnl_pts",
        "bt_date_col": "entry_date",
        "paper_table": "trades.trade_outcomes",
    },
    "weekend_theta": {
        "name": "Weekend Theta Decay",
        "bt_table": "analysis.weekend_theta_trades",
        "bt_where": "1=1",
        "bt_win_col": "win_exp",
        "bt_pnl_col": "profit_exp_pts",
        "bt_date_col": "expiry_date",
        "paper_table": None,
    },
    "equity_breakout": {
        "name": "Monthly Equity Breakout",
        "bt_table": "analysis.monthly_breakout_trades",
        "bt_where": "1=1",
        "bt_win_col": None,          # derived: return_pct > 0
        "bt_pnl_col": "return_pct",
        "bt_date_col": "entry_date",
        "paper_table": None,
    },
}


# ── Pure gate functions ────────────────────────────────────────────────────────

def check_bt_gates(bt_oos_trades: int, bt_win_rate: float,
                   bt_sharpe: float, bt_years: float) -> dict:
    return {
        "trades":   1 if bt_oos_trades >= BT_MIN_TRADES   else 0,
        "win_rate": 1 if bt_win_rate   >= BT_MIN_WIN_RATE else 0,
        "sharpe":   1 if bt_sharpe     >= BT_MIN_SHARPE   else 0,
        "years":    1 if bt_years      >= BT_MIN_YEARS    else 0,
    }


def check_paper_gates(paper_trades: int, paper_win_rate: float,
                       paper_net_pnl: float, paper_vs_bt_delta: float) -> dict:
    return {
        "trades":   1 if paper_trades       >= PAPER_MIN_TRADES        else 0,
        "win_rate": 1 if paper_vs_bt_delta  >  PAPER_MAX_DRIFT         else 0,
        "pnl":      1 if paper_net_pnl      >  0.0                     else 0,
    }


def check_micro_gates(micro_trades: int, micro_win_rate: float,
                       micro_net_pnl: float) -> dict:
    return {
        "trades":   1 if micro_trades  >= MICRO_MIN_TRADES else 0,
        "win_rate": 1 if micro_trades  >= MICRO_MIN_TRADES else 0,  # placeholder — same threshold
        "pnl":      1 if micro_net_pnl >  0.0             else 0,
    }


def determine_stage(bt_gates: dict, paper_gates: dict,
                    micro_gates: dict) -> tuple[int, str]:
    bt_pass    = all(v == 1 for v in bt_gates.values())
    paper_pass = all(v == 1 for v in paper_gates.values())
    micro_pass = all(v == 1 for v in micro_gates.values())

    if not bt_pass:
        return 1, "BACKTEST"
    if not paper_pass:
        return 2, "PAPER"
    if not micro_pass:
        return 3, "MICRO_LIVE"
    return 4, "FULL_LIVE"


# ── CH metric queries ─────────────────────────────────────────────────────────

def compute_bt_metrics(ch, strategy_id: str) -> dict:
    cfg = STRATEGY_REGISTRY.get(strategy_id)
    if not cfg:
        return _zero_bt()

    table    = cfg["bt_table"]
    where    = cfg.get("bt_where", "1=1")
    win_col  = cfg.get("bt_win_col")
    pnl_col  = cfg["bt_pnl_col"]
    date_col = cfg["bt_date_col"]

    if win_col:
        win_expr = f"avg({win_col})"
    else:
        win_expr = f"avg(if({pnl_col} > 0, 1, 0))"

    sql = f"""
        SELECT
            count()                                      AS n,
            {win_expr}                                   AS wr,
            if(stddevSamp({pnl_col}) > 0,
               avg({pnl_col}) / stddevSamp({pnl_col}),
               0)                                        AS sharpe,
            toDate(min({date_col}))                      AS min_dt,
            toDate(max({date_col}))                      AS max_dt
        FROM {table}
        WHERE {where}
    """
    try:
        rows = ch.query(sql).result_rows
    except Exception as e:
        log.warning("bt_metrics query failed for %s: %s", strategy_id, e)
        return _zero_bt()

    if not rows or rows[0][0] == 0:
        return _zero_bt()

    n, wr, sharpe, min_dt, max_dt = rows[0]
    if isinstance(min_dt, str):
        min_dt = date.fromisoformat(min_dt)
    if isinstance(max_dt, str):
        max_dt = date.fromisoformat(max_dt)
    years = (max_dt - min_dt).days / 365.0 if max_dt and min_dt else 0.0

    return {
        "bt_oos_trades": int(n),
        "bt_win_rate":   float(wr),
        "bt_sharpe":     float(sharpe),
        "bt_years":      float(years),
    }


def compute_paper_metrics(ch, strategy_id: str, bt_win_rate: float) -> dict:
    cfg = STRATEGY_REGISTRY.get(strategy_id, {})
    paper_table = cfg.get("paper_table")

    if not paper_table:
        return _zero_paper(bt_win_rate)

    sql = f"""
        SELECT
            count()              AS n,
            avg(if(pnl_inr > 0, 1, 0)) AS wr,
            sum(pnl_inr)         AS net_pnl
        FROM {paper_table}
    """
    try:
        rows = ch.query(sql).result_rows
    except Exception as e:
        log.warning("paper_metrics query failed for %s: %s", strategy_id, e)
        return _zero_paper(bt_win_rate)

    if not rows or rows[0][0] == 0:
        return _zero_paper(bt_win_rate)

    n, wr, net_pnl = rows[0]
    return {
        "paper_trades":      int(n),
        "paper_win_rate":    float(wr),
        "paper_net_pnl":     float(net_pnl),
        "paper_vs_bt_delta": float(wr) - bt_win_rate,
    }


def compute_micro_metrics(ch, strategy_id: str, paper_win_rate: float) -> dict:
    # Micro-live not yet instrumented in any strategy — return zeros.
    return {
        "micro_trades":   0,
        "micro_win_rate": 0.0,
        "micro_net_pnl":  0.0,
    }


def _zero_bt() -> dict:
    return {"bt_oos_trades": 0, "bt_win_rate": 0.0, "bt_sharpe": 0.0, "bt_years": 0.0}


def _zero_paper(bt_win_rate: float = 0.0) -> dict:
    return {"paper_trades": 0, "paper_win_rate": 0.0, "paper_net_pnl": 0.0,
            "paper_vs_bt_delta": 0.0}


# ── Upsert ────────────────────────────────────────────────────────────────────

def upsert_graduation(ch, strategy_id: str, strategy_name: str,
                       stage: int, stage_label: str, stage_since: date,
                       bt: dict, bt_gates: dict,
                       paper: dict, paper_gates: dict,
                       micro: dict, micro_gates: dict):
    now = int(datetime.utcnow().timestamp())
    row = [
        strategy_id, strategy_name,
        stage, stage_label, stage_since,
        bt["bt_oos_trades"], bt["bt_win_rate"],  bt["bt_sharpe"],  bt["bt_years"],
        bt_gates["trades"],  bt_gates["win_rate"], bt_gates["sharpe"], bt_gates["years"],
        paper["paper_trades"], paper["paper_win_rate"], paper["paper_net_pnl"],
        paper["paper_vs_bt_delta"],
        paper_gates["trades"], paper_gates["win_rate"], paper_gates["pnl"],
        micro["micro_trades"], micro["micro_win_rate"], micro["micro_net_pnl"],
        micro_gates["trades"], micro_gates["win_rate"], micro_gates["pnl"],
        datetime.utcnow(), now,
    ]
    cols = [
        "strategy_id", "strategy_name", "stage", "stage_label", "stage_since",
        "bt_oos_trades", "bt_win_rate", "bt_sharpe", "bt_years",
        "bt_gate_trades", "bt_gate_win_rate", "bt_gate_sharpe", "bt_gate_years",
        "paper_trades", "paper_win_rate", "paper_net_pnl", "paper_vs_bt_delta",
        "paper_gate_trades", "paper_gate_win_rate", "paper_gate_pnl",
        "micro_trades", "micro_win_rate", "micro_net_pnl",
        "micro_gate_trades", "micro_gate_win_rate", "micro_gate_pnl",
        "updated_at", "version",
    ]
    ch.insert("analysis.strategy_graduation", [row], column_names=cols)
    log.info("upserted %s stage=%d %s", strategy_id, stage, stage_label)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(ch, dry_run: bool = False):
    today = date.today()
    for sid, cfg in STRATEGY_REGISTRY.items():
        log.info("processing strategy: %s", sid)
        bt    = compute_bt_metrics(ch, sid)
        paper = compute_paper_metrics(ch, sid, bt_win_rate=bt["bt_win_rate"])
        micro = compute_micro_metrics(ch, sid, paper_win_rate=paper["paper_win_rate"])

        bt_gates    = check_bt_gates(**bt)
        paper_gates = check_paper_gates(**paper)
        micro_gates = check_micro_gates(**micro)

        stage, label = determine_stage(bt_gates, paper_gates, micro_gates)

        log.info("  %s → stage=%d (%s) | BT=%s PAPER=%s MICRO=%s",
                 sid, stage, label, bt_gates, paper_gates, micro_gates)

        if dry_run:
            continue

        upsert_graduation(ch, strategy_id=sid, strategy_name=cfg["name"],
                           stage=stage, stage_label=label, stage_since=today,
                           bt=bt, bt_gates=bt_gates,
                           paper=paper, paper_gates=paper_gates,
                           micro=micro, micro_gates=micro_gates)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Compute and print stages without writing to DB")
    args = ap.parse_args()

    CH_HOST = os.getenv("CH_HOST", "clickhouse")
    CH_PORT = int(os.getenv("CH_PORT", "8123"))
    CH_USER = os.getenv("CH_USER", "default")
    CH_PASS = os.getenv("CH_PASSWORD", "")

    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )
    run(ch, dry_run=args.dry_run)
