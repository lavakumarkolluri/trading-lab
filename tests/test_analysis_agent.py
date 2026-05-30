"""
Tests for analysis_agent.py.
All tests run offline — no ClickHouse, Telegram, or filesystem writes required.
"""
import sys
import os
import uuid
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import analysis_agent as m


# ── helpers ───────────────────────────────────────────────────────────────────

def _trade(symbol="NIFTY", pnl_inr=500.0, exit_reason="target",
           scorecard_conf=72.0, days_ago=1):
    exit_dt = datetime.today() - timedelta(days=days_ago)
    return {
        "trade_id":      str(uuid.uuid4()),
        "symbol":        symbol,
        "expiry":        date.today(),
        "entry_time":    exit_dt - timedelta(hours=3),
        "exit_time":     exit_dt,
        "strike":        22500.0,
        "entry_premium": 200.0,
        "exit_premium":  200.0 - pnl_inr / 65,
        "pnl_pts":       pnl_inr / 65,
        "pnl_inr":       pnl_inr,
        "lot_size":      65,
        "lots":          1,
        "exit_reason":   exit_reason,
        "scorecard_conf": scorecard_conf,
        "wing_ce_strike": 0.0,
        "net_premium":    0.0,
    }


# ── compute_stats ─────────────────────────────────────────────────────────────

def test_compute_stats_empty_returns_zero_count():
    s = m.compute_stats([])
    assert s["count"] == 0


def test_compute_stats_win_rate():
    trades = [_trade(pnl_inr=500), _trade(pnl_inr=-300), _trade(pnl_inr=800)]
    s = m.compute_stats(trades)
    assert s["count"] == 3
    assert s["win_count"] == 2
    assert abs(s["win_rate"] - 66.67) < 0.1


def test_compute_stats_total_and_avg_pnl():
    trades = [_trade(pnl_inr=600), _trade(pnl_inr=400)]
    s = m.compute_stats(trades)
    assert s["total_pnl"] == 1000.0
    assert s["avg_pnl"] == 500.0


def test_compute_stats_best_worst():
    trades = [_trade(pnl_inr=2000), _trade(pnl_inr=-1000), _trade(pnl_inr=500)]
    s = m.compute_stats(trades)
    assert s["best_pnl"] == 2000.0
    assert s["worst_pnl"] == -1000.0


def test_compute_stats_streak_wins():
    trades = [_trade(pnl_inr=-200, days_ago=3),
              _trade(pnl_inr=500, days_ago=2),
              _trade(pnl_inr=400, days_ago=1)]
    s = m.compute_stats(trades)
    assert s["streak"] == 2
    assert s["streak_type"] == "W"


def test_compute_stats_streak_loss():
    trades = [_trade(pnl_inr=500, days_ago=2),
              _trade(pnl_inr=-300, days_ago=1)]
    s = m.compute_stats(trades)
    assert s["streak"] == 1
    assert s["streak_type"] == "L"


def test_compute_stats_by_symbol():
    trades = [
        _trade(symbol="NIFTY", pnl_inr=500),
        _trade(symbol="NIFTY", pnl_inr=-300),
        _trade(symbol="BANKNIFTY", pnl_inr=800),
    ]
    s = m.compute_stats(trades)
    assert s["by_symbol"]["NIFTY"]["count"] == 2
    assert s["by_symbol"]["NIFTY"]["wins"] == 1
    assert s["by_symbol"]["BANKNIFTY"]["count"] == 1
    assert s["by_symbol"]["BANKNIFTY"]["wins"] == 1


def test_compute_stats_by_exit_reason():
    trades = [
        _trade(exit_reason="target", pnl_inr=500),
        _trade(exit_reason="target", pnl_inr=300),
        _trade(exit_reason="stop",   pnl_inr=-1000),
    ]
    s = m.compute_stats(trades)
    assert s["by_reason"]["target"]["count"] == 2
    assert s["by_reason"]["target"]["wins"] == 2
    assert s["by_reason"]["stop"]["count"] == 1
    assert s["by_reason"]["stop"]["wins"] == 0


def test_compute_stats_confidence_buckets():
    trades = [
        _trade(pnl_inr=500,  scorecard_conf=80),  # hi_conf win
        _trade(pnl_inr=-300, scorecard_conf=75),  # hi_conf loss
        _trade(pnl_inr=200,  scorecard_conf=40),  # lo_conf win
        _trade(pnl_inr=-100, scorecard_conf=45),  # lo_conf loss
    ]
    s = m.compute_stats(trades)
    assert s["hi_conf"]["count"] == 2
    assert s["hi_conf"]["wins"] == 1
    assert s["lo_conf"]["count"] == 2
    assert s["lo_conf"]["wins"] == 1


# ── _today_trades ─────────────────────────────────────────────────────────────

def test_today_trades_filters_correctly():
    today_t = _trade(days_ago=0)
    old_t   = _trade(days_ago=3)
    result  = m._today_trades([today_t, old_t])
    assert len(result) == 1
    assert result[0]["trade_id"] == today_t["trade_id"]


def test_today_trades_empty_when_none_today():
    trades = [_trade(days_ago=2), _trade(days_ago=5)]
    assert m._today_trades(trades) == []


# ── build_telegram_msg ────────────────────────────────────────────────────────

def test_telegram_msg_no_trades():
    msg = m.build_telegram_msg({"count": 0}, [], date(2026, 5, 1), date(2026, 5, 14))
    assert "No trades" in msg


def test_telegram_msg_includes_win_rate():
    trades = [_trade(pnl_inr=500), _trade(pnl_inr=-300)]
    s = m.compute_stats(trades)
    msg = m.build_telegram_msg(s, [], date(2026, 5, 1), date(2026, 5, 14))
    assert "Win rate" in msg
    assert "1/2" in msg


def test_telegram_msg_includes_today_section_when_trades():
    today_t = _trade(days_ago=0, pnl_inr=600)
    trades = [_trade(days_ago=5, pnl_inr=500), today_t]
    s = m.compute_stats(trades)
    msg = m.build_telegram_msg(s, [today_t], date(2026, 5, 1), date(2026, 5, 14))
    assert "Today" in msg


def test_telegram_msg_no_today_section_when_no_trades_today():
    trades = [_trade(days_ago=5, pnl_inr=500)]
    s = m.compute_stats(trades)
    msg = m.build_telegram_msg(s, [], date(2026, 5, 1), date(2026, 5, 14))
    assert "Today" not in msg


def test_telegram_msg_includes_streak():
    trades = [_trade(pnl_inr=500, days_ago=2), _trade(pnl_inr=400, days_ago=1)]
    s = m.compute_stats(trades)
    msg = m.build_telegram_msg(s, [], date(2026, 5, 1), date(2026, 5, 14))
    assert "Streak" in msg
    assert "2×W" in msg


# ── build_markdown_report ─────────────────────────────────────────────────────

def test_markdown_report_has_summary_table():
    trades = [_trade(pnl_inr=500), _trade(pnl_inr=-300)]
    s = m.compute_stats(trades)
    md = m.build_markdown_report(trades, s, date(2026, 5, 1), date(2026, 5, 14))
    assert "## Summary" in md
    assert "Win rate" in md


def test_markdown_report_has_all_trades():
    trades = [_trade(pnl_inr=500), _trade(pnl_inr=-300)]
    s = m.compute_stats(trades)
    md = m.build_markdown_report(trades, s, date(2026, 5, 1), date(2026, 5, 14))
    assert "## All Trades" in md
    assert "NIFTY" in md


def test_markdown_report_has_confidence_section():
    trades = [_trade(pnl_inr=500, scorecard_conf=80)]
    s = m.compute_stats(trades)
    md = m.build_markdown_report(trades, s, date(2026, 5, 1), date(2026, 5, 14))
    assert "Confidence" in md


# ── fetch_trades ──────────────────────────────────────────────────────────────

def test_fetch_trades_calls_ch_query():
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    result = m.fetch_trades(ch, date(2026, 5, 1))
    assert result == []
    ch.query.assert_called_once()
    assert "trade_outcomes" in ch.query.call_args[0][0]


def test_fetch_trades_maps_columns():
    ch = MagicMock()
    ch.query.return_value.result_rows = [(
        "tid1", "NIFTY", date(2026, 5, 15),
        datetime(2026, 5, 14, 9, 35), datetime(2026, 5, 14, 14, 0),
        22500.0, 200.0, 180.0, 20.0, 1300.0, 65, 1, "target", 72.0, 0.0, 0.0,
    )]
    result = m.fetch_trades(ch, date(2026, 5, 1))
    assert len(result) == 1
    assert result[0]["symbol"] == "NIFTY"
    assert result[0]["pnl_inr"] == 1300.0
    assert result[0]["exit_reason"] == "target"


# ── docker-compose + scheduler wiring ────────────────────────────────────────

def test_analysis_agent_service_in_compose():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    with open(compose_path) as f:
        content = f.read()
    assert "analysis_agent:" in content
    assert "analysis_agent.py" in content


def test_analysis_agent_has_report_dir_env():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    import yaml
    with open(compose_path) as f:
        data = yaml.safe_load(f)
    env = data["services"]["analysis_agent"]["environment"]
    assert any("REPORT_DIR" in str(e) for e in env)


def test_analysis_agent_daily_in_scheduler():
    sched_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "scheduler.py")
    with open(sched_path) as f:
        content = f.read()
    assert "job_analysis_agent_daily" in content
    assert "job_analysis_agent_weekly" in content
    assert "17:15" in content   # daily trigger time (moved from 13:15 with C4 fix)
    assert "analysis_agent" in content
