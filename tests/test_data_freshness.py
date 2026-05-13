"""
Tests for data_freshness_check.py.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import data_freshness_check as m


def _mock_ch(rows_by_query: dict):
    """Build a mock CH client that returns configured rows for each query."""
    ch = MagicMock()
    def _query(sql, **kwargs):
        result = MagicMock()
        for fragment, rows in rows_by_query.items():
            if fragment in sql:
                result.result_rows = rows
                return result
        result.result_rows = []
        return result
    ch.query.side_effect = _query
    return ch


# ── _check_ohlcv ─────────────────────────────────────────────────────────────

def test_check_ohlcv_fresh_returns_empty():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"ohlcv_daily": [("NSE", fresh), ("BSE", fresh)]})
    assert m._check_ohlcv(ch) == []


def test_check_ohlcv_stale_returns_issue():
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch({"ohlcv_daily": [("NSE", stale)]})
    issues = m._check_ohlcv(ch)
    assert len(issues) == 1
    assert issues[0]["label"] == "ohlcv_daily[NSE]"
    assert issues[0]["fix"] == "meta_pipeline"
    assert issues[0]["days_stale"] == 10


def test_check_ohlcv_threshold_is_3_days():
    """Exactly 3-day-old data should be fresh (weekend gap tolerance)."""
    borderline = date.today() - timedelta(days=3)
    ch = _mock_ch({"ohlcv_daily": [("NSE", borderline)]})
    assert m._check_ohlcv(ch) == []


# ── _check_mf_nav ────────────────────────────────────────────────────────────

def test_check_mf_nav_fresh_returns_empty():
    fresh = date.today() - timedelta(days=2)
    ch = _mock_ch({"mf_nav": [(fresh,)]})
    assert m._check_mf_nav(ch) == []


def test_check_mf_nav_stale_returns_issue():
    stale = date.today() - timedelta(days=8)
    ch = _mock_ch({"mf_nav": [(stale,)]})
    issues = m._check_mf_nav(ch)
    assert len(issues) == 1
    assert issues[0]["label"] == "mf_nav"
    assert issues[0]["fix"] == "mf_pipeline"


def test_check_mf_nav_threshold_is_5_days():
    borderline = date.today() - timedelta(days=5)
    ch = _mock_ch({"mf_nav": [(borderline,)]})
    assert m._check_mf_nav(ch) == []


# ── _check_vix ───────────────────────────────────────────────────────────────

def test_check_vix_fresh_returns_empty():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"nifty_live": [(fresh,)]})
    assert m._check_vix(ch) == []


def test_check_vix_stale_returns_issue():
    stale = date.today() - timedelta(days=6)
    ch = _mock_ch({"nifty_live": [(stale,)]})
    issues = m._check_vix(ch)
    assert len(issues) == 1
    assert issues[0]["fix"] == "vix_pipeline"


# ── _check_options_chain ─────────────────────────────────────────────────────

def test_check_options_chain_fresh_returns_empty():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"options_chain": [("NIFTY", fresh), ("BANKNIFTY", fresh)]})
    assert m._check_options_chain(ch) == []


def test_check_options_chain_stale_returns_issue():
    stale = date.today() - timedelta(days=7)
    ch = _mock_ch({"options_chain": [("NIFTY", stale)]})
    issues = m._check_options_chain(ch)
    assert len(issues) == 1
    assert "NIFTY" in issues[0]["label"]
    assert issues[0]["fix"] == "option_chain_historical"


# ── empty-table safety (CRIT-005) ────────────────────────────────────────────

def test_check_mf_nav_empty_table_returns_empty():
    """result_rows=[] must not crash with IndexError."""
    ch = _mock_ch({"mf_nav": []})
    issues = m._check_mf_nav(ch)
    assert issues == []


def test_check_vix_empty_table_returns_empty():
    """result_rows=[] must not crash with IndexError."""
    ch = _mock_ch({"nifty_live": []})
    issues = m._check_vix(ch)
    assert issues == []


# ── _check_per_symbol_staleness ───────────────────────────────────────────────

def test_check_per_symbol_staleness_returns_report_only_issues():
    ch = _mock_ch({"days_behind": [("NSE", "RELIANCE", date(2026, 4, 1), date(2026, 5, 1), 30)]})
    issues = m._check_per_symbol_staleness(ch)
    assert len(issues) == 1
    assert issues[0]["fix"] is None  # report-only


# ── main: auto-fix logic ─────────────────────────────────────────────────────

def _mock_ch_main(ohlcv_rows, mf_rows, vix_rows, chain_rows, sym_rows=()):
    """Mock CH for main() — distinguishes per-symbol query by 'dateDiff' fragment."""
    ch = MagicMock()
    def _query(sql, **kwargs):
        result = MagicMock()
        if "dateDiff" in sql:
            result.result_rows = list(sym_rows)
        elif "ohlcv_daily" in sql:
            result.result_rows = list(ohlcv_rows)
        elif "mf_nav" in sql:
            result.result_rows = list(mf_rows)
        elif "nifty_live" in sql:
            result.result_rows = list(vix_rows)
        elif "options_chain" in sql:
            result.result_rows = list(chain_rows)
        else:
            result.result_rows = []
        return result
    ch.query.side_effect = _query
    return ch


def test_main_report_only_does_not_run_pipelines():
    """--report flag must not trigger any auto-fix pipelines."""
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch_main(
        ohlcv_rows=[("NSE", stale)], mf_rows=[(stale,)],
        vix_rows=[(stale,)], chain_rows=[], sym_rows=[],
    )
    with patch("data_freshness_check.get_ch", return_value=ch), \
         patch("data_freshness_check._run_pipeline") as mock_run, \
         patch("data_freshness_check._send_telegram"), \
         patch("sys.argv", ["data_freshness_check.py", "--report"]):
        m.main()
    mock_run.assert_not_called()


def test_main_auto_fix_triggers_pipeline_once_per_service():
    """Each unique fix service must be triggered exactly once."""
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch_main(
        ohlcv_rows=[("NSE", stale), ("BSE", stale)],  # both → meta_pipeline
        mf_rows=[(stale,)], vix_rows=[(stale,)], chain_rows=[], sym_rows=[],
    )
    with patch("data_freshness_check.get_ch", return_value=ch), \
         patch("data_freshness_check._run_pipeline", return_value=True) as mock_run, \
         patch("data_freshness_check._send_telegram"), \
         patch("sys.argv", ["data_freshness_check.py"]):
        m.main()

    called_services = [call.args[0] for call in mock_run.call_args_list]
    assert called_services.count("meta_pipeline") == 1
    assert called_services.count("mf_pipeline") == 1
    assert called_services.count("vix_pipeline") == 1


def test_main_no_issues_exits_cleanly():
    """When all data is fresh, no pipelines are triggered and no Telegram sent."""
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch_main(
        ohlcv_rows=[("NSE", fresh)], mf_rows=[(fresh,)],
        vix_rows=[(fresh,)], chain_rows=[("NIFTY", fresh)], sym_rows=[],
    )
    with patch("data_freshness_check.get_ch", return_value=ch), \
         patch("data_freshness_check._run_pipeline") as mock_run, \
         patch("data_freshness_check._send_telegram") as mock_tg, \
         patch("sys.argv", ["data_freshness_check.py"]):
        m.main()

    mock_run.assert_not_called()
    mock_tg.assert_not_called()


# ── docker-compose wiring ────────────────────────────────────────────────────

def test_data_freshness_service_in_compose():
    """data_freshness_check service must exist in docker-compose.yml."""
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    with open(compose_path) as f:
        content = f.read()
    assert "data_freshness_check:" in content, "data_freshness_check service missing from docker-compose.yml"
    assert "data_freshness_check.py" in content, "entrypoint missing"


def test_data_freshness_in_dockerfile():
    """Dockerfile must COPY data_freshness_check.py."""
    df_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "Dockerfile")
    with open(df_path) as f:
        content = f.read()
    assert "data_freshness_check.py" in content, "data_freshness_check.py not copied in Dockerfile"


def test_data_freshness_scheduled_in_scheduler():
    """scheduler.py must reference job_data_freshness_check."""
    sched_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "scheduler.py")
    with open(sched_path) as f:
        content = f.read()
    assert "job_data_freshness_check" in content
    assert "data_freshness_check" in content
