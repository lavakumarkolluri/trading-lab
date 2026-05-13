"""
Tests for pipeline/graduation_gate.py — offline, no ClickHouse required.
TDD: written before implementation to define expected behaviour.

Gate thresholds (from graduation schema):
  Stage 1→2 (BT gate): ≥50 OOS trades, win_rate ≥0.55, Sharpe ≥0.50, ≥2 years data
  Stage 2→3 (paper gate): ≥30 paper trades, paper_win_rate within -10% of bt, net_pnl ≥0
  Stage 3→4 (micro gate): ≥20 micro-live trades, within -15% of paper, net_pnl ≥0
"""
import sys
import os
from unittest.mock import MagicMock, call
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import graduation_gate as m


# ── Gate threshold constants ──────────────────────────────────────────────────

def test_bt_gate_thresholds_present():
    assert m.BT_MIN_TRADES   == 50
    assert m.BT_MIN_WIN_RATE == 0.55
    assert m.BT_MIN_SHARPE   == 0.10   # raw per-trade; weekly freq → ~0.72 annualised
    assert m.BT_MIN_YEARS    == 2.0

def test_paper_gate_thresholds_present():
    assert m.PAPER_MIN_TRADES   == 30
    assert m.PAPER_MAX_DRIFT    == -0.10

def test_micro_gate_thresholds_present():
    assert m.MICRO_MIN_TRADES   == 20
    assert m.MICRO_MAX_DRIFT    == -0.15


# ── check_bt_gates() — pure function ─────────────────────────────────────────

def test_bt_gates_all_pass():
    gates = m.check_bt_gates(bt_oos_trades=100, bt_win_rate=0.60,
                              bt_sharpe=0.80, bt_years=3.0)
    assert gates["trades"] == 1
    assert gates["win_rate"] == 1
    assert gates["sharpe"] == 1
    assert gates["years"] == 1

def test_bt_gates_fail_trades():
    gates = m.check_bt_gates(bt_oos_trades=49, bt_win_rate=0.60,
                              bt_sharpe=0.80, bt_years=3.0)
    assert gates["trades"] == 0

def test_bt_gates_fail_win_rate():
    gates = m.check_bt_gates(bt_oos_trades=60, bt_win_rate=0.54,
                              bt_sharpe=0.80, bt_years=3.0)
    assert gates["win_rate"] == 0

def test_bt_gates_fail_sharpe():
    gates = m.check_bt_gates(bt_oos_trades=60, bt_win_rate=0.60,
                              bt_sharpe=0.05, bt_years=3.0)
    assert gates["sharpe"] == 0

def test_bt_gates_fail_years():
    gates = m.check_bt_gates(bt_oos_trades=60, bt_win_rate=0.60,
                              bt_sharpe=0.80, bt_years=1.5)
    assert gates["years"] == 0

def test_bt_gates_exactly_on_threshold_pass():
    gates = m.check_bt_gates(bt_oos_trades=50, bt_win_rate=0.55,
                              bt_sharpe=0.50, bt_years=2.0)
    assert all(v == 1 for v in gates.values())


# ── check_paper_gates() — pure function ──────────────────────────────────────

def test_paper_gates_all_pass():
    gates = m.check_paper_gates(paper_trades=40, paper_win_rate=0.60,
                                 paper_net_pnl=5000.0, paper_vs_bt_delta=-0.05)
    assert gates["trades"] == 1
    assert gates["win_rate"] == 1
    assert gates["pnl"] == 1

def test_paper_gates_fail_trades():
    gates = m.check_paper_gates(paper_trades=29, paper_win_rate=0.60,
                                 paper_net_pnl=5000.0, paper_vs_bt_delta=-0.05)
    assert gates["trades"] == 0

def test_paper_gates_fail_drift():
    """Drift of -0.11 (more negative than -0.10 threshold) → fail."""
    gates = m.check_paper_gates(paper_trades=40, paper_win_rate=0.49,
                                 paper_net_pnl=5000.0, paper_vs_bt_delta=-0.11)
    assert gates["win_rate"] == 0

def test_paper_gates_fail_pnl():
    gates = m.check_paper_gates(paper_trades=40, paper_win_rate=0.60,
                                 paper_net_pnl=-100.0, paper_vs_bt_delta=-0.05)
    assert gates["pnl"] == 0

def test_paper_gates_zero_pnl_fails():
    """Net P&L must be strictly positive."""
    gates = m.check_paper_gates(paper_trades=40, paper_win_rate=0.60,
                                 paper_net_pnl=0.0, paper_vs_bt_delta=0.0)
    assert gates["pnl"] == 0


# ── check_micro_gates() — pure function ──────────────────────────────────────

def test_micro_gates_all_pass():
    gates = m.check_micro_gates(micro_trades=25, micro_win_rate=0.60,
                                 micro_net_pnl=3000.0)
    assert gates["trades"] == 1
    assert gates["win_rate"] == 1
    assert gates["pnl"] == 1

def test_micro_gates_fail_trades():
    gates = m.check_micro_gates(micro_trades=19, micro_win_rate=0.60,
                                 micro_net_pnl=3000.0)
    assert gates["trades"] == 0

def test_micro_gates_fail_pnl():
    gates = m.check_micro_gates(micro_trades=25, micro_win_rate=0.60,
                                 micro_net_pnl=-1.0)
    assert gates["pnl"] == 0


# ── determine_stage() — pure function ────────────────────────────────────────

def _all_pass():
    return {"trades": 1, "win_rate": 1, "sharpe": 1, "years": 1}

def _all_fail():
    return {"trades": 0, "win_rate": 0, "sharpe": 0, "years": 0}

def _paper_pass():
    return {"trades": 1, "win_rate": 1, "pnl": 1}

def _paper_fail():
    return {"trades": 0, "win_rate": 0, "pnl": 0}

def _micro_pass():
    return {"trades": 1, "win_rate": 1, "pnl": 1}

def _micro_fail():
    return {"trades": 0, "win_rate": 0, "pnl": 0}


def test_determine_stage_1_when_bt_fails():
    stage, label = m.determine_stage(bt_gates=_all_fail(),
                                      paper_gates=_paper_pass(),
                                      micro_gates=_micro_pass())
    assert stage == 1
    assert label == "BACKTEST"

def test_determine_stage_2_when_bt_passes_paper_fails():
    stage, label = m.determine_stage(bt_gates=_all_pass(),
                                      paper_gates=_paper_fail(),
                                      micro_gates=_micro_pass())
    assert stage == 2
    assert label == "PAPER"

def test_determine_stage_3_when_bt_paper_pass_micro_fails():
    stage, label = m.determine_stage(bt_gates=_all_pass(),
                                      paper_gates=_paper_pass(),
                                      micro_gates=_micro_fail())
    assert stage == 3
    assert label == "MICRO_LIVE"

def test_determine_stage_4_when_all_pass():
    stage, label = m.determine_stage(bt_gates=_all_pass(),
                                      paper_gates=_paper_pass(),
                                      micro_gates=_micro_pass())
    assert stage == 4
    assert label == "FULL_LIVE"

def test_determine_stage_1_when_no_gates():
    """No backtest data at all → Stage 1."""
    stage, label = m.determine_stage(bt_gates=_all_fail(),
                                      paper_gates=_paper_fail(),
                                      micro_gates=_micro_fail())
    assert stage == 1
    assert label == "BACKTEST"


# ── compute_bt_metrics() — mocked CH ─────────────────────────────────────────

def _bt_ch(rows):
    """Mock CH client returning given rows for bt query."""
    ch = MagicMock()
    ch.query.return_value.result_rows = rows
    return ch


def test_compute_bt_metrics_iron_fly():
    """For iron_fly_0dte, bt metrics come from analysis.spread_backtest."""
    # (n_trades, win_rate, sharpe, min_date, max_date)
    ch = _bt_ch([(120, 0.62, 0.75, "2021-01-01", "2025-12-31")])
    result = m.compute_bt_metrics(ch, "iron_fly_0dte")
    assert result["bt_oos_trades"] == 120
    assert abs(result["bt_win_rate"] - 0.62) < 1e-4
    assert abs(result["bt_sharpe"] - 0.75) < 1e-4
    assert result["bt_years"] > 4.5   # ~5 years

def test_compute_bt_metrics_empty_table_returns_zeros():
    """Empty backtest table → zero metrics, all gates fail → Stage 1."""
    ch = _bt_ch([])
    result = m.compute_bt_metrics(ch, "iron_fly_0dte")
    assert result["bt_oos_trades"] == 0
    assert result["bt_win_rate"] == 0.0
    assert result["bt_sharpe"] == 0.0
    assert result["bt_years"] == 0.0

def test_compute_bt_metrics_weekend_theta():
    """For weekend_theta, bt metrics come from analysis.weekend_theta_trades."""
    ch = _bt_ch([(80, 0.70, 0.90, "2022-06-01", "2025-12-31")])
    result = m.compute_bt_metrics(ch, "weekend_theta")
    assert result["bt_oos_trades"] == 80
    assert abs(result["bt_win_rate"] - 0.70) < 1e-4


# ── compute_paper_metrics() — mocked CH ──────────────────────────────────────

def _paper_ch(rows):
    ch = MagicMock()
    ch.query.return_value.result_rows = rows
    return ch


def test_compute_paper_metrics_iron_fly():
    """For iron_fly_0dte, paper metrics come from trades.trade_outcomes."""
    # (n_trades, win_rate, net_pnl_inr)
    ch = _paper_ch([(35, 0.60, 12000.0)])
    result = m.compute_paper_metrics(ch, "iron_fly_0dte", bt_win_rate=0.62)
    assert result["paper_trades"] == 35
    assert abs(result["paper_win_rate"] - 0.60) < 1e-4
    assert abs(result["paper_net_pnl"] - 12000.0) < 1e-2
    assert abs(result["paper_vs_bt_delta"] - (0.60 - 0.62)) < 1e-4

def test_compute_paper_metrics_empty_returns_zeros():
    ch = _paper_ch([])
    result = m.compute_paper_metrics(ch, "iron_fly_0dte", bt_win_rate=0.60)
    assert result["paper_trades"] == 0
    assert result["paper_win_rate"] == 0.0
    assert result["paper_net_pnl"] == 0.0
    assert result["paper_vs_bt_delta"] == 0.0

def test_compute_paper_metrics_no_paper_table():
    """Strategies with no paper table get zero metrics."""
    ch = MagicMock()
    result = m.compute_paper_metrics(ch, "weekend_theta", bt_win_rate=0.70)
    assert result["paper_trades"] == 0
    ch.query.assert_not_called()


# ── STRATEGY_REGISTRY coverage ────────────────────────────────────────────────

def test_strategy_registry_contains_iron_fly():
    assert "iron_fly_0dte" in m.STRATEGY_REGISTRY

def test_strategy_registry_contains_weekend_theta():
    assert "weekend_theta" in m.STRATEGY_REGISTRY

def test_all_strategies_have_required_keys():
    required = {"name", "bt_table"}
    for sid, cfg in m.STRATEGY_REGISTRY.items():
        missing = required - cfg.keys()
        assert not missing, f"Strategy '{sid}' missing keys: {missing}"


# ── upsert_graduation() — mocked CH ──────────────────────────────────────────

def test_upsert_graduation_calls_insert():
    """upsert_graduation must call ch.insert() with analysis.strategy_graduation."""
    ch = MagicMock()
    from datetime import date
    m.upsert_graduation(ch, strategy_id="iron_fly_0dte",
                         strategy_name="Iron Fly 0DTE",
                         stage=2, stage_label="PAPER",
                         stage_since=date.today(),
                         bt={"bt_oos_trades": 80, "bt_win_rate": 0.60,
                              "bt_sharpe": 0.70, "bt_years": 3.0},
                         bt_gates={"trades": 1, "win_rate": 1,
                                    "sharpe": 1, "years": 1},
                         paper={"paper_trades": 10, "paper_win_rate": 0.55,
                                 "paper_net_pnl": 3000.0, "paper_vs_bt_delta": -0.05},
                         paper_gates={"trades": 0, "win_rate": 1, "pnl": 1},
                         micro={"micro_trades": 0, "micro_win_rate": 0.0,
                                 "micro_net_pnl": 0.0},
                         micro_gates={"trades": 0, "win_rate": 0, "pnl": 0})
    ch.insert.assert_called_once()
    args = ch.insert.call_args
    table = args[0][0]
    assert table == "analysis.strategy_graduation"
