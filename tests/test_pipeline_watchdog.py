"""
Tests for pipeline_watchdog.py.
All tests run offline — no ClickHouse or Docker required.
"""
import sys
import os
from datetime import datetime, timedelta, date, timezone
from unittest.mock import MagicMock, patch, call
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import pipeline_watchdog as w


def _mock_ch(rows_by_fragment: dict):
    """Build a mock CH client that returns rows based on SQL fragments."""
    ch = MagicMock()
    def _query(sql, parameters=None, **kwargs):
        result = MagicMock()
        for frag, rows in rows_by_fragment.items():
            if frag in sql:
                result.result_rows = rows
                return result
        result.result_rows = []
        return result
    ch.query.side_effect = _query
    return ch


def _now():
    return datetime.now(timezone.utc)


# ── EXPECTED_JOBS / config ────────────────────────────────────────────────────

def test_expected_jobs_has_required_services():
    required = {"option_chain_historical", "compute_oi_features",
                 "confidence_scorer", "strategy_selector", "vix_pipeline"}
    assert required.issubset(set(w.EXPECTED_JOBS.keys()))


def test_pipeline_to_freshness_maps_all_expected_jobs():
    for svc in w.EXPECTED_JOBS:
        assert svc in w.PIPELINE_TO_FRESHNESS, f"{svc} missing from PIPELINE_TO_FRESHNESS"


# ── Detection: _query_pipeline_status ────────────────────────────────────────

def test_query_pipeline_status_returns_status():
    ch = _mock_ch({"pipeline_runs": [("success",)]})
    assert w._query_pipeline_status(ch, "vix_pipeline", "2026-05-14") == "success"


def test_query_pipeline_status_returns_none_when_no_row():
    ch = _mock_ch({"pipeline_runs": []})
    assert w._query_pipeline_status(ch, "vix_pipeline", "2026-05-14") is None


# ── Detection: _detect_failed_runs ───────────────────────────────────────────

def test_detect_failed_runs_returns_failed_services():
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        svc = (parameters or {}).get("service", "")
        if svc == "vix_pipeline" and "status" in sql and "error_msg" not in sql:
            r.result_rows = [("failed",)]
        else:
            r.result_rows = []
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    issues = w._detect_failed_runs(ch, "2026-05-14")
    services = [i["service"] for i in issues]
    assert "vix_pipeline" in services
    assert all(i["status"] == "failed" for i in issues)


def test_detect_failed_runs_excludes_success():
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        r.result_rows = [("success",)] if "error_msg" not in sql else []
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    issues = w._detect_failed_runs(ch, "2026-05-14")
    assert issues == []


# ── Detection: _detect_missing_runs ──────────────────────────────────────────

def test_detect_missing_runs_reports_missing_after_grace():
    # Set now_utc to well past all expected times + 30 min grace
    now_utc = datetime(2026, 5, 14, 20, 0, 0)  # 20:00 UTC — well after all deadlines
    ch = _mock_ch({"pipeline_runs": []})  # no rows → all missing
    issues = w._detect_missing_runs(ch, "2026-05-14", now_utc)
    services = [i["service"] for i in issues]
    assert "vix_pipeline" in services
    assert "option_chain_historical" in services
    assert all(i["status"] == "missing" for i in issues)


def test_detect_missing_runs_skips_within_grace():
    # now_utc is before earliest expected time + grace (12:30 + 30min = 13:00)
    now_utc = datetime(2026, 5, 14, 12, 0, 0)  # 12:00 UTC
    ch = _mock_ch({"pipeline_runs": []})
    issues = w._detect_missing_runs(ch, "2026-05-14", now_utc)
    # All services should be skipped — grace period not expired
    assert issues == []


def test_detect_missing_runs_excludes_when_row_exists():
    now_utc = datetime(2026, 5, 14, 20, 0, 0)
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        svc = (parameters or {}).get("service", "")
        r.result_rows = [("success",)] if svc == "vix_pipeline" else []
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    issues = w._detect_missing_runs(ch, "2026-05-14", now_utc)
    services = [i["service"] for i in issues]
    assert "vix_pipeline" not in services


# ── Detection: detect_issues deduplication ───────────────────────────────────

def test_detect_issues_failed_wins_over_missing():
    """If a service has both failed row and is in missing scan, 'failed' should appear once."""
    now_utc = datetime(2026, 5, 14, 20, 0, 0)
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        svc = (parameters or {}).get("service", "")
        if svc == "vix_pipeline" and "error_msg" not in sql:
            r.result_rows = [("failed",)]
        else:
            r.result_rows = []
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    issues = w.detect_issues(ch, "2026-05-14", now_utc)
    vix_issues = [i for i in issues if i["service"] == "vix_pipeline"]
    assert len(vix_issues) == 1
    assert vix_issues[0]["status"] == "failed"


# ── Freshness checks ──────────────────────────────────────────────────────────

def test_is_ohlcv_fresh_returns_true_when_recent():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"ohlcv_daily": [(fresh,)]})
    assert w._is_ohlcv_fresh(ch) is True


def test_is_ohlcv_fresh_returns_false_when_stale():
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch({"ohlcv_daily": [(stale,)]})
    assert w._is_ohlcv_fresh(ch) is False


def test_is_ohlcv_fresh_empty_table_returns_false():
    ch = _mock_ch({"ohlcv_daily": [(None,)]})
    assert w._is_ohlcv_fresh(ch) is False


def test_is_mf_nav_fresh_returns_true_when_recent():
    fresh = date.today() - timedelta(days=2)
    ch = _mock_ch({"mf_nav": [(fresh,)]})
    assert w._is_mf_nav_fresh(ch) is True


def test_is_mf_nav_fresh_returns_false_when_stale():
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch({"mf_nav": [(stale,)]})
    assert w._is_mf_nav_fresh(ch) is False


def test_is_vix_fresh_returns_true_when_recent():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"nifty_live": [(fresh,)]})
    assert w._is_vix_fresh(ch) is True


def test_is_vix_fresh_returns_false_when_stale():
    stale = date.today() - timedelta(days=8)
    ch = _mock_ch({"nifty_live": [(stale,)]})
    assert w._is_vix_fresh(ch) is False


def test_is_vix_fresh_empty_table_returns_false():
    ch = _mock_ch({"nifty_live": []})
    assert w._is_vix_fresh(ch) is False


def test_is_options_chain_fresh_returns_true_when_recent():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"options_chain": [(fresh,)]})
    assert w._is_options_chain_fresh(ch) is True


def test_is_options_chain_fresh_returns_false_when_stale():
    stale = date.today() - timedelta(days=8)
    ch = _mock_ch({"options_chain": [(stale,)]})
    assert w._is_options_chain_fresh(ch) is False


# ── check_freshness_for_service ───────────────────────────────────────────────

def test_check_freshness_for_service_returns_none_for_no_key():
    """confidence_scorer maps to None → check via pipeline_runs instead."""
    ch = MagicMock()
    result = w.check_freshness_for_service(ch, "confidence_scorer")
    assert result is None


def test_check_freshness_for_service_returns_bool_for_mapped_key():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"options_chain": [(fresh,)]})
    result = w.check_freshness_for_service(ch, "option_chain_historical")
    assert result is True


def test_check_freshness_for_service_handles_unknown_service():
    ch = MagicMock()
    result = w.check_freshness_for_service(ch, "totally_unknown_service")
    assert result is None


# ── Pattern tracking ──────────────────────────────────────────────────────────

def test_count_recent_failures_returns_count():
    ch = _mock_ch({"pipeline_runs": [(5,)]})
    assert w._count_recent_failures(ch, "vix_pipeline") == 5


def test_count_recent_failures_returns_zero_on_empty():
    ch = _mock_ch({"pipeline_runs": []})
    assert w._count_recent_failures(ch, "vix_pipeline") == 0


# ── attempt_fix ───────────────────────────────────────────────────────────────

def test_attempt_fix_respects_max_retries():
    ch = _mock_ch({"pipeline_runs": [(0,)]})
    state = w.WatchdogState()
    state.retry_counts[("vix_pipeline", "2026-05-14")] = w.MAX_RETRIES
    issue = {"service": "vix_pipeline", "status": "failed", "error_msg": ""}
    with patch.object(w, "_run_fix") as mock_fix, \
         patch.object(w, "_send"):
        w.attempt_fix(ch, issue, state, "2026-05-14", _now())
    mock_fix.assert_not_called()


def test_attempt_fix_increments_retry_count():
    ch = _mock_ch({"pipeline_runs": [(1,)]})
    state = w.WatchdogState()
    issue = {"service": "vix_pipeline", "status": "failed", "error_msg": "timeout"}
    with patch.object(w, "_run_fix", return_value=(True, "")), \
         patch.object(w, "_send"):
        w.attempt_fix(ch, issue, state, "2026-05-14", _now())
    assert state.retry_counts[("vix_pipeline", "2026-05-14")] == 1


def test_attempt_fix_registers_pending_verification():
    ch = _mock_ch({"pipeline_runs": [(1,)]})
    state = w.WatchdogState()
    issue = {"service": "vix_pipeline", "status": "failed", "error_msg": ""}
    with patch.object(w, "_run_fix", return_value=(True, "")), \
         patch.object(w, "_send"):
        w.attempt_fix(ch, issue, state, "2026-05-14", _now())
    assert "vix_pipeline" in state.pending_verifications


def test_attempt_fix_sends_recurring_flag_when_many_failures():
    ch = _mock_ch({"pipeline_runs": [(w.RECURRING_THRESHOLD,)]})
    state = w.WatchdogState()
    issue = {"service": "vix_pipeline", "status": "failed", "error_msg": "disk full"}
    sent_msgs = []
    with patch.object(w, "_run_fix", return_value=(True, "")), \
         patch.object(w, "_send", side_effect=lambda m, level="WARN": sent_msgs.append((m, level))):
        w.attempt_fix(ch, issue, state, "2026-05-14", _now())
    assert any("Recurring" in m for m, lvl in sent_msgs)
    assert any(lvl == "CRIT" for m, lvl in sent_msgs)


def test_attempt_fix_sends_warn_when_few_failures():
    ch = _mock_ch({"pipeline_runs": [(1,)]})
    state = w.WatchdogState()
    issue = {"service": "vix_pipeline", "status": "missing", "error_msg": ""}
    sent_levels = []
    with patch.object(w, "_run_fix", return_value=(True, "")), \
         patch.object(w, "_send", side_effect=lambda m, level="WARN": sent_levels.append(level)):
        w.attempt_fix(ch, issue, state, "2026-05-14", _now())
    assert "WARN" in sent_levels
    assert "CRIT" not in sent_levels


# ── run_pending_verifications ─────────────────────────────────────────────────

def test_run_pending_verifications_skips_before_delay():
    ch = MagicMock()
    state = w.WatchdogState()
    triggered = _now() - timedelta(seconds=w.VERIFY_DELAY_S - 60)
    state.pending_verifications["vix_pipeline"] = w.PendingVerification(
        service="vix_pipeline", triggered_at=triggered,
        freshness_target="vix", error_msg="",
    )
    with patch.object(w, "_send") as mock_send:
        w.run_pending_verifications(ch, state, "2026-05-14", _now())
    mock_send.assert_not_called()
    assert "vix_pipeline" in state.pending_verifications  # still pending


def test_run_pending_verifications_sends_ok_when_fresh():
    fresh = date.today() - timedelta(days=1)
    ch = _mock_ch({"nifty_live": [(fresh,)]})
    state = w.WatchdogState()
    triggered = _now() - timedelta(seconds=w.VERIFY_DELAY_S + 60)
    state.pending_verifications["vix_pipeline"] = w.PendingVerification(
        service="vix_pipeline", triggered_at=triggered,
        freshness_target="vix", error_msg="",
    )
    sent = []
    with patch.object(w, "_send", side_effect=lambda m, level="WARN": sent.append((m, level))):
        w.run_pending_verifications(ch, state, "2026-05-14", _now())
    assert any("Fixed" in m for m, lvl in sent)
    assert any(lvl == "INFO" for m, lvl in sent)
    assert "vix_pipeline" not in state.pending_verifications


def test_run_pending_verifications_sends_crit_when_still_stale():
    stale = date.today() - timedelta(days=10)
    ch = _mock_ch({"nifty_live": [(stale,)]})
    state = w.WatchdogState()
    triggered = _now() - timedelta(seconds=w.VERIFY_DELAY_S + 60)
    state.pending_verifications["vix_pipeline"] = w.PendingVerification(
        service="vix_pipeline", triggered_at=triggered,
        freshness_target="vix", error_msg="",
    )
    sent = []
    with patch.object(w, "_send", side_effect=lambda m, level="WARN": sent.append((m, level))):
        w.run_pending_verifications(ch, state, "2026-05-14", _now())
    assert any(lvl == "CRIT" for m, lvl in sent)
    assert "vix_pipeline" not in state.pending_verifications  # removed regardless


def test_run_pending_verifications_fallback_to_pipeline_runs():
    """For services with freshness_target=None, fall back to pipeline_runs status."""
    def _side(sql, parameters=None, **kw):
        r = MagicMock()
        svc = (parameters or {}).get("service", "")
        if svc == "confidence_scorer":
            r.result_rows = [("success",)]
        else:
            r.result_rows = []
        return r
    ch = MagicMock()
    ch.query.side_effect = _side
    state = w.WatchdogState()
    triggered = _now() - timedelta(seconds=w.VERIFY_DELAY_S + 60)
    state.pending_verifications["confidence_scorer"] = w.PendingVerification(
        service="confidence_scorer", triggered_at=triggered,
        freshness_target=None, error_msg="",
    )
    sent = []
    with patch.object(w, "_send", side_effect=lambda m, level="WARN": sent.append((m, level))):
        w.run_pending_verifications(ch, state, "2026-05-14", _now())
    assert any("Fixed" in m for m, lvl in sent)


# ── _tick integration ─────────────────────────────────────────────────────────

def test_tick_skips_fix_for_pending_service():
    """If a service is already in pending_verifications, attempt_fix must not be called again."""
    state = w.WatchdogState()
    # triggered just now → elapsed ≈ 0 << VERIFY_DELAY_S → stays in pending
    state.pending_verifications["vix_pipeline"] = w.PendingVerification(
        service="vix_pipeline", triggered_at=datetime.now(timezone.utc),
        freshness_target="vix", error_msg="",
    )
    # detect_issues returns vix_pipeline as an issue
    vix_issue = {"service": "vix_pipeline", "status": "failed", "error_msg": ""}
    with patch.object(w, "attempt_fix") as mock_fix, \
         patch.object(w, "detect_issues", return_value=[vix_issue]):
        ch = MagicMock()
        w._tick(ch, state)
    calls = [ca[0][1]["service"] for ca in mock_fix.call_args_list]
    assert "vix_pipeline" not in calls


# ── Docker compose wiring ─────────────────────────────────────────────────────

def test_pipeline_watchdog_in_compose():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    with open(compose_path) as f:
        content = f.read()
    assert "pipeline_watchdog:" in content
    assert "pipeline_watchdog.py" in content
    assert "restart: unless-stopped" in content


def test_pipeline_watchdog_has_docker_socket():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    with open(compose_path) as f:
        content = f.read()
    assert "/var/run/docker.sock" in content


def test_pipeline_watchdog_in_dockerfile():
    df_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "Dockerfile")
    with open(df_path) as f:
        content = f.read()
    copied = "pipeline_watchdog.py" in content or "COPY *.py" in content
    assert copied, "pipeline_watchdog.py not copied in Dockerfile"


# ── alert_log wiring ──────────────────────────────────────────────────────────

def test_send_writes_to_alert_log_when_ch_set():
    ch = MagicMock()
    w._ch_ref = ch
    with patch("pipeline_watchdog.write_alert_log") as mock_wal, \
         patch("urllib.request.urlopen"):
        w._send("test alert", level="WARN")
    mock_wal.assert_called_once()
    args = mock_wal.call_args[0]
    assert args[0] is ch
    assert args[1] == "pipeline_watchdog"
    w._ch_ref = None


def test_send_skips_alert_log_when_ch_not_set():
    w._ch_ref = None
    with patch("pipeline_watchdog.write_alert_log") as mock_wal:
        w._send("test alert")
    mock_wal.assert_not_called()
