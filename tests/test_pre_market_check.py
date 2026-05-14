"""
Tests for pre_market_check.py.
All tests run offline — no ClickHouse or Docker required.
"""
import sys
import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import pre_market_check as m


def _mock_ch(rows_by_fragment: dict):
    ch = MagicMock()
    def _query(sql, parameters=None, **kw):
        result = MagicMock()
        for frag, rows in rows_by_fragment.items():
            if frag in sql:
                result.result_rows = rows
                return result
        result.result_rows = []
        return result
    ch.query.side_effect = _query
    return ch


# ── _check_clickhouse ─────────────────────────────────────────────────────────

def test_check_clickhouse_returns_true_when_reachable():
    ch = _mock_ch({"SELECT 1": [(1,)]})
    ok, detail = m._check_clickhouse(ch)
    assert ok is True
    assert "reachable" in detail


def test_check_clickhouse_returns_false_when_query_raises():
    ch = MagicMock()
    ch.query.side_effect = Exception("connection refused")
    ok, detail = m._check_clickhouse(ch)
    assert ok is False
    assert "unreachable" in detail


# ── _check_dashboard ──────────────────────────────────────────────────────────

def test_check_dashboard_returns_true_when_running():
    result = MagicMock()
    result.stdout = "running\n"
    with patch("subprocess.run", return_value=result):
        ok, detail = m._check_dashboard()
    assert ok is True
    assert detail == "running"


def test_check_dashboard_returns_false_when_exited():
    result = MagicMock()
    result.stdout = "exited\n"
    with patch("subprocess.run", return_value=result):
        ok, detail = m._check_dashboard()
    assert ok is False
    assert "exited" in detail


def test_check_dashboard_returns_false_when_not_found():
    result = MagicMock()
    result.stdout = "\n"
    with patch("subprocess.run", return_value=result):
        ok, detail = m._check_dashboard()
    assert ok is False


# ── _check_options_chain ──────────────────────────────────────────────────────

def test_check_options_chain_returns_true_when_all_fresh():
    fresh = date.today() - timedelta(days=1)
    rows = [(sym, fresh) for sym in m.SYMBOLS]
    ch = _mock_ch({"options_chain": rows})
    ok, detail = m._check_options_chain(ch)
    assert ok is True
    assert "fresh" in detail


def test_check_options_chain_returns_false_when_any_stale():
    stale = date.today() - timedelta(days=5)
    ch = _mock_ch({"options_chain": [("NIFTY", stale)]})
    ok, detail = m._check_options_chain(ch)
    assert ok is False
    assert "NIFTY" in detail
    assert "stale" in detail


def test_check_options_chain_returns_false_when_empty():
    ch = _mock_ch({"options_chain": []})
    ok, detail = m._check_options_chain(ch)
    assert ok is False


# ── _check_vix ────────────────────────────────────────────────────────────────

def test_check_vix_returns_true_when_fresh():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"nifty_live": [(fresh,)]})
    ok, detail = m._check_vix(ch)
    assert ok is True
    assert "fresh" in detail


def test_check_vix_returns_false_when_stale():
    stale = date.today() - timedelta(days=5)
    ch = _mock_ch({"nifty_live": [(stale,)]})
    ok, detail = m._check_vix(ch)
    assert ok is False
    assert "stale" in detail


def test_check_vix_returns_false_when_empty():
    ch = _mock_ch({"nifty_live": []})
    ok, detail = m._check_vix(ch)
    assert ok is False
    assert "no data" in detail


# ── _check_ohlcv ─────────────────────────────────────────────────────────────

def test_check_ohlcv_returns_true_when_fresh():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"ohlcv_daily": [("NSE", fresh)]})
    ok, detail = m._check_ohlcv(ch)
    assert ok is True


def test_check_ohlcv_returns_false_when_stale():
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch({"ohlcv_daily": [("NSE", stale)]})
    ok, detail = m._check_ohlcv(ch)
    assert ok is False
    assert "NSE" in detail


def test_check_ohlcv_returns_false_when_empty():
    ch = _mock_ch({"ohlcv_daily": []})
    ok, detail = m._check_ohlcv(ch)
    assert ok is False


# ── _check_confidence_scores ──────────────────────────────────────────────────

def test_check_confidence_scores_returns_true_when_all_present():
    rows = [(sym, 65) for sym in m.SYMBOLS]
    ch = _mock_ch({"scorecard": rows})
    ok, detail = m._check_confidence_scores(ch)
    assert ok is True
    assert "NIFTY" in detail


def test_check_confidence_scores_returns_false_when_missing_symbol():
    ch = _mock_ch({"scorecard": [("NIFTY", 70)]})  # only NIFTY present
    ok, detail = m._check_confidence_scores(ch)
    assert ok is False
    assert "missing" in detail
    assert "BANKNIFTY" in detail


def test_check_confidence_scores_returns_false_when_empty():
    ch = _mock_ch({"scorecard": []})
    ok, detail = m._check_confidence_scores(ch)
    assert ok is False


# ── _check_yesterday_pipelines ────────────────────────────────────────────────

def test_check_yesterday_pipelines_returns_true_when_all_success():
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        r.result_rows = [("success",)]
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    ok, detail = m._check_yesterday_pipelines(ch)
    assert ok is True
    assert "all success" in detail


def test_check_yesterday_pipelines_returns_false_when_any_failed():
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        svc = (parameters or {}).get("service", "")
        r.result_rows = [("failed",)] if svc == "vix_pipeline" else [("success",)]
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    ok, detail = m._check_yesterday_pipelines(ch)
    assert ok is False
    assert "vix_pipeline" in detail


def test_check_yesterday_pipelines_returns_false_when_not_run():
    ch = _mock_ch({"pipeline_runs": []})
    ok, detail = m._check_yesterday_pipelines(ch)
    assert ok is False


# ── _check_open_positions ─────────────────────────────────────────────────────

def test_check_open_positions_returns_true_when_none():
    ch = _mock_ch({"open_positions": [(0,)]})
    ok, detail = m._check_open_positions(ch)
    assert ok is True
    assert "none" in detail


def test_check_open_positions_returns_false_when_open():
    ch = _mock_ch({"open_positions": [(3,)]})
    ok, detail = m._check_open_positions(ch)
    assert ok is False
    assert "3" in detail


# ── run_all_checks ────────────────────────────────────────────────────────────

def test_run_all_checks_all_pass():
    fresh = date.today() - timedelta(days=1)

    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        if "SELECT 1" in sql:
            r.result_rows = [(1,)]
        elif "options_chain" in sql:
            r.result_rows = [(sym, fresh) for sym in m.SYMBOLS]
        elif "nifty_live" in sql:
            r.result_rows = [(fresh,)]
        elif "ohlcv_daily" in sql:
            r.result_rows = [("NSE", fresh)]
        elif "scorecard" in sql:
            r.result_rows = [(sym, 65) for sym in m.SYMBOLS]
        elif "pipeline_runs" in sql:
            r.result_rows = [("success",)]
        elif "open_positions" in sql:
            r.result_rows = [(0,)]
        else:
            r.result_rows = []
        return r

    ch = MagicMock()
    ch.query.side_effect = _side
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "running\n"
        results = m.run_all_checks(ch)

    assert all(r["ok"] for r in results)
    assert len(results) == len(m.CHECKS)


def test_run_all_checks_ch_down_marks_remaining_unavailable():
    ch = MagicMock()
    ch.query.side_effect = Exception("connection refused")
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "running\n"
        results = m.run_all_checks(ch)

    ch_result = next(r for r in results if r["name"] == "ClickHouse")
    assert ch_result["ok"] is False
    data_results = [r for r in results if r["name"] not in ("ClickHouse", "Dashboard")]
    assert all(not r["ok"] for r in data_results)
    assert all("unavailable" in r["detail"] for r in data_results)


def test_run_all_checks_dashboard_checked_even_when_ch_down():
    """Dashboard is CH-independent and should still be checked."""
    ch = MagicMock()
    ch.query.side_effect = Exception("connection refused")
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = "running\n"
        results = m.run_all_checks(ch)

    dash = next(r for r in results if r["name"] == "Dashboard")
    assert dash["ok"] is True


# ── format_report ─────────────────────────────────────────────────────────────

def test_format_report_go_when_all_pass():
    results = [{"name": "ClickHouse", "ok": True, "detail": "reachable"},
               {"name": "Dashboard",  "ok": True, "detail": "running"}]
    report = m.format_report(results, date(2026, 5, 14))
    assert "🟢" in report
    assert "SYSTEM GO" in report
    assert "14 May 2026" in report


def test_format_report_not_ready_when_any_fail():
    results = [{"name": "ClickHouse",   "ok": True,  "detail": "reachable"},
               {"name": "Options chain","ok": False, "detail": "NIFTY stale"}]
    report = m.format_report(results, date(2026, 5, 14))
    assert "🔴" in report
    assert "NOT READY" in report
    assert "1 issue" in report


def test_format_report_includes_check_names_and_details():
    results = [{"name": "VIX", "ok": True, "detail": "fresh (2026-05-13)"}]
    report = m.format_report(results, date(2026, 5, 14))
    assert "VIX" in report
    assert "fresh (2026-05-13)" in report


def test_format_report_counts_multiple_failures():
    results = [
        {"name": "A", "ok": False, "detail": "bad"},
        {"name": "B", "ok": False, "detail": "bad"},
        {"name": "C", "ok": True,  "detail": "ok"},
    ]
    report = m.format_report(results, date(2026, 5, 14))
    assert "2 issue" in report


# ── main() ────────────────────────────────────────────────────────────────────

def test_main_sends_info_level_when_all_ok():
    fresh = date.today() - timedelta(days=1)
    sent = []

    def fake_run_all(ch):
        return [{"name": n, "ok": True, "detail": "ok"} for n, _ in m.CHECKS]

    with patch("pre_market_check._ch_factory"), \
         patch("pre_market_check.run_all_checks", side_effect=fake_run_all), \
         patch("pre_market_check._send", side_effect=lambda msg, level="INFO": sent.append(level)):
        m.main()

    assert any(lvl == "INFO" for lvl in sent)
    assert not any(lvl == "CRIT" for lvl in sent)


def test_main_sends_crit_when_check_fails():
    sent = []

    def fake_run_all(ch):
        results = [{"name": n, "ok": True, "detail": "ok"} for n, _ in m.CHECKS]
        results[0]["ok"] = False  # ClickHouse fail
        return results

    with patch("pre_market_check._ch_factory"), \
         patch("pre_market_check.run_all_checks", side_effect=fake_run_all), \
         patch("pre_market_check._send", side_effect=lambda msg, level="INFO": sent.append(level)):
        m.main()

    assert any(lvl == "CRIT" for lvl in sent)


def test_main_sends_crit_when_ch_connection_fails():
    sent = []
    with patch("pre_market_check._ch_factory", side_effect=Exception("timeout")), \
         patch("pre_market_check._send", side_effect=lambda msg, level="INFO": sent.append(level)):
        m.main()
    assert any(lvl == "CRIT" for lvl in sent)


# ── Scheduler wiring ──────────────────────────────────────────────────────────

def test_scheduler_has_pre_market_check_job():
    sched_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "scheduler.py")
    with open(sched_path) as f:
        content = f.read()
    assert "job_pre_market_check" in content
    assert "pre_market_check" in content


def test_scheduler_schedules_pre_market_at_03_00():
    sched_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "scheduler.py")
    with open(sched_path) as f:
        content = f.read()
    assert '"03:00"' in content and "job_pre_market_check" in content
