"""
Tests for scheduler.py dependency chain enforcement.
Offline — no ClickHouse or Docker required.

Catches:
  - UPSTREAM_DEPS missing a critical edge → job runs without its data ready
  - _upstream_ok returning True when upstream failed → bad data poisons model
  - _upstream_ok returning False incorrectly → jobs blocked for no reason
"""
import sys
import os
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))

# scheduler imports `schedule`, `kiteconnect`, and others not on the host.
# Stub them before importing scheduler so tests run fully offline.
for _mod in ("schedule", "kiteconnect", "jugaad_data", "curl_cffi",
             "bs4", "boto3", "minio", "tenacity"):
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()
# kite_orders / kite_auth also import kiteconnect
for _mod in ("kite_orders", "kite_auth"):
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()


# ── UPSTREAM_DEPS map structure ────────────────────────────────────────────────

def test_upstream_deps_is_dict():
    import scheduler as s
    assert isinstance(s.UPSTREAM_DEPS, dict)


def test_confidence_scorer_requires_compute_oi_features():
    """confidence_scorer must wait for compute_oi_features.
    Without OI features, model trains on stale/zero features → unreliable score."""
    import scheduler as s
    deps = s.UPSTREAM_DEPS.get("confidence_scorer", [])
    assert "compute_oi_features" in deps, (
        "confidence_scorer must declare compute_oi_features as upstream dep"
    )


def test_confidence_scorer_requires_strategy_backtester():
    """confidence_scorer --compare needs fresh backtest data for target labels."""
    import scheduler as s
    deps = s.UPSTREAM_DEPS.get("confidence_scorer", [])
    assert "strategy_backtester" in deps


def test_strategy_selector_requires_confidence_scorer():
    """strategy_selector reads from analysis.scorecard — must run after scorer."""
    import scheduler as s
    deps = s.UPSTREAM_DEPS.get("strategy_selector", [])
    assert "confidence_scorer" in deps


def test_compute_oi_features_declared_as_upstream():
    """compute_oi_features must be a declared upstream somewhere — not a leaf."""
    import scheduler as s
    all_upstreams = set()
    for deps in s.UPSTREAM_DEPS.values():
        all_upstreams.update(deps)
    assert "compute_oi_features" in all_upstreams


# ── _upstream_ok() logic ───────────────────────────────────────────────────────

def _mock_ch(rows_by_service: dict):
    """Return a CH mock that yields different rows per query based on service name."""
    ch = MagicMock()
    def side_effect(sql, parameters=None):
        svc = parameters.get("service", "") if parameters else ""
        result = MagicMock()
        result.result_rows = rows_by_service.get(svc, [])
        return result
    ch.query.side_effect = side_effect
    return ch


def test_upstream_ok_returns_true_when_no_deps():
    """Service with no declared upstreams is always OK."""
    import scheduler as s
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client") as mock_ch_factory:
            mock_ch_factory.return_value = _mock_ch({})
            ok, reason = s._upstream_ok("option_chain_intraday", "2026-05-13")
    assert ok is True
    assert reason == ""


def test_upstream_ok_false_when_upstream_failed():
    """If compute_oi_features status=failed, confidence_scorer must be blocked."""
    import scheduler as s
    ch = _mock_ch({"compute_oi_features": [("failed",)],
                   "strategy_backtester":  [("success",)]})
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is False
    assert "compute_oi_features" in reason


def test_upstream_ok_false_when_upstream_missing():
    """If compute_oi_features has no record today, block confidence_scorer."""
    import scheduler as s
    # no rows → upstream not run today
    ch = _mock_ch({"compute_oi_features": [],
                   "strategy_backtester":  [("success",)]})
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is False


def test_upstream_ok_true_when_all_upstreams_succeed():
    """All upstreams succeeded → confidence_scorer is cleared to run."""
    import scheduler as s
    ch = _mock_ch({"compute_oi_features": [("success",)],
                   "strategy_backtester":  [("success",)]})
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is True
    assert reason == ""


def test_upstream_ok_returns_true_when_tracking_disabled():
    """When CH is unreachable (_TRACKING_OK=False), skip the check (don't block)."""
    import scheduler as s
    with patch.object(s, "_TRACKING_OK", False):
        ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is True


# ── job_check_dashboard_health ────────────────────────────────────────────────

def test_dashboard_health_sends_alert_when_down():
    """Alert is sent when docker inspect returns non-running status."""
    import scheduler as s
    s._dashboard_was_down = False  # reset state
    result = MagicMock()
    result.stdout = "exited\n"
    result.returncode = 0
    with patch("subprocess.run", return_value=result), \
         patch.object(s, "_send_telegram") as mock_tg:
        s.job_check_dashboard_health()
    mock_tg.assert_called_once()
    assert "DOWN" in mock_tg.call_args[0][0]


def test_dashboard_health_no_alert_when_running():
    """No alert when dashboard is running."""
    import scheduler as s
    s._dashboard_was_down = False
    result = MagicMock()
    result.stdout = "running\n"
    with patch("subprocess.run", return_value=result), \
         patch.object(s, "_send_telegram") as mock_tg:
        s.job_check_dashboard_health()
    mock_tg.assert_not_called()


def test_dashboard_health_alert_only_once_while_down():
    """Alert fires once on transition to down, not on every poll."""
    import scheduler as s
    s._dashboard_was_down = True  # already alerted
    result = MagicMock()
    result.stdout = "exited\n"
    with patch("subprocess.run", return_value=result), \
         patch.object(s, "_send_telegram") as mock_tg:
        s.job_check_dashboard_health()
    mock_tg.assert_not_called()


def test_dashboard_health_recovers_state_when_up():
    """_dashboard_was_down resets to False when container comes back up."""
    import scheduler as s
    s._dashboard_was_down = True
    result = MagicMock()
    result.stdout = "running\n"
    with patch("subprocess.run", return_value=result), \
         patch.object(s, "_send_telegram"):
        s.job_check_dashboard_health()
    assert s._dashboard_was_down is False


# ── _is_intraday_market_hours ─────────────────────────────────────────────────

def test_market_hours_true_tuesday_0400_utc():
    """04:00 UTC on a Tuesday (weekday=1) is inside the 03:30-10:00 window."""
    import scheduler as s
    from datetime import datetime as real_dt
    now = real_dt(2026, 5, 19, 4, 0, 0)   # Tuesday
    assert s._is_intraday_market_hours(now) is True


def test_market_hours_false_tuesday_1030_utc():
    """10:30 UTC on Tuesday is outside the window (after 10:00)."""
    import scheduler as s
    from datetime import datetime as real_dt
    now = real_dt(2026, 5, 19, 10, 30, 0)   # Tuesday after window
    assert s._is_intraday_market_hours(now) is False


def test_market_hours_false_saturday():
    """Saturday is not a weekday — no market hours."""
    import scheduler as s
    from datetime import datetime as real_dt
    now = real_dt(2026, 5, 16, 5, 0, 0)   # Saturday 05:00 UTC
    assert s._is_intraday_market_hours(now) is False


def test_market_hours_false_at_midnight():
    """00:30 UTC on Monday is before the 03:30 window start."""
    import scheduler as s
    from datetime import datetime as real_dt
    now = real_dt(2026, 5, 18, 0, 30, 0)   # Monday 00:30 UTC
    assert s._is_intraday_market_hours(now) is False


# ── auto-deploy market-hours gate ─────────────────────────────────────────────

def test_auto_deploy_no_restart_during_market_hours():
    """sys.exit must NOT be called when pipeline changes during market hours."""
    import scheduler as s
    from datetime import datetime as real_dt

    market_now = real_dt(2026, 5, 19, 4, 0, 0)   # Tuesday 04:00 UTC

    git_result = MagicMock()
    git_result.returncode = 0
    git_result.stdout = "abc1234\n"

    diff_result = MagicMock()
    diff_result.returncode = 0
    diff_result.stdout = "pipeline/scheduler.py\n"

    def fake_git(args, **kwargs):
        if "fetch" in args:
            return git_result
        if "rev-parse" in args:
            return git_result
        if "diff" in args:
            return diff_result
        return MagicMock(returncode=0, stdout="")

    with patch.object(s, "_get_last_deployed_sha", return_value="old1234"), \
         patch.object(s, "_save_deployed_sha"), \
         patch.object(s, "_git", side_effect=fake_git), \
         patch.object(s, "_is_intraday_market_hours", return_value=True), \
         patch.object(s, "_run_tests_and_record"), \
         patch("sys.exit") as mock_exit:
        s.job_auto_deploy()

    mock_exit.assert_not_called()


def test_auto_deploy_restarts_outside_market_hours():
    """sys.exit(0) IS called when pipeline changes outside market hours."""
    import scheduler as s

    git_result = MagicMock()
    git_result.returncode = 0
    git_result.stdout = "abc1234\n"

    diff_result = MagicMock()
    diff_result.returncode = 0
    diff_result.stdout = "pipeline/scheduler.py\n"

    def fake_git(args, **kwargs):
        if "fetch" in args:
            return git_result
        if "rev-parse" in args:
            return git_result
        if "diff" in args:
            return diff_result
        return MagicMock(returncode=0, stdout="")

    with patch.object(s, "_get_last_deployed_sha", return_value="old1234"), \
         patch.object(s, "_save_deployed_sha"), \
         patch.object(s, "_git", side_effect=fake_git), \
         patch.object(s, "_is_intraday_market_hours", return_value=False), \
         patch.object(s, "_run_tests_and_record"), \
         patch.object(s, "_compose"), \
         patch("sys.exit") as mock_exit:
        s.job_auto_deploy()

    mock_exit.assert_called_once_with(0)


# ── _startup_recovery intraday recovery ──────────────────────────────────────

def _make_intraday_now():
    """Return a datetime within the intraday recovery window (05:00 UTC Tuesday)."""
    from datetime import datetime as real_dt
    return real_dt(2026, 5, 19, 5, 0, 0)   # Tuesday 05:00 UTC


def test_startup_recovery_launches_monitor_if_not_running():
    """If intraday_monitor is not running during market hours, job must be fired."""
    import scheduler as s

    def fake_container(name):
        return False   # nothing running

    with patch.object(s, "datetime") as mock_dt, \
         patch.object(s, "_container_is_running", side_effect=fake_container), \
         patch.object(s, "job_intraday_monitor") as mock_monitor, \
         patch.object(s, "job_option_chain_intraday") as mock_oc, \
         patch.object(s, "_TRACKING_OK", False), \
         patch.dict("os.environ", {"CH_PASSWORD": "secret"}):
        mock_dt.now.return_value = _make_intraday_now()
        s._startup_recovery()

    mock_monitor.assert_called_once()


def test_startup_recovery_skips_monitor_if_already_running():
    """If intraday_monitor is already running, do not launch a second instance."""
    import scheduler as s

    def fake_container(name):
        return True   # everything running

    with patch.object(s, "datetime") as mock_dt, \
         patch.object(s, "_container_is_running", side_effect=fake_container), \
         patch.object(s, "job_intraday_monitor") as mock_monitor, \
         patch.object(s, "_TRACKING_OK", False), \
         patch.dict("os.environ", {"CH_PASSWORD": "secret"}):
        mock_dt.now.return_value = _make_intraday_now()
        s._startup_recovery()

    mock_monitor.assert_not_called()


def test_startup_recovery_skips_when_no_ch_password():
    """If CH_PASSWORD is not set, _startup_recovery must return early (A3)."""
    import scheduler as s
    import os

    with patch.object(s, "job_intraday_monitor") as mock_monitor, \
         patch.object(s, "job_option_chain_intraday") as mock_oc, \
         patch.dict("os.environ", {}, clear=True):   # no CH_PASSWORD
        s._startup_recovery()

    mock_monitor.assert_not_called()
    mock_oc.assert_not_called()


# ── job_intraday_post_check ───────────────────────────────────────────────────

def test_post_check_alerts_when_monitor_not_running_and_no_trades():
    """Alert fires when intraday_monitor is not running and DB shows 0 trades."""
    import scheduler as s

    ch = MagicMock()
    ch.query.return_value.result_rows = [(0,)]

    with patch.object(s, "_container_is_running", return_value=False), \
         patch.object(s, "_ch_client", return_value=ch), \
         patch.object(s, "_send_telegram") as mock_tg:
        s.job_intraday_post_check()

    mock_tg.assert_called_once()
    assert "intraday_monitor" in mock_tg.call_args[0][0]


def test_post_check_no_alert_when_monitor_still_running():
    """No alert when intraday_monitor container is still running (session in progress)."""
    import scheduler as s

    with patch.object(s, "_container_is_running", return_value=True), \
         patch.object(s, "_send_telegram") as mock_tg:
        s.job_intraday_post_check()

    mock_tg.assert_not_called()


def test_post_check_no_alert_when_trades_recorded():
    """No alert when trades were recorded today even if container exited normally."""
    import scheduler as s

    ch = MagicMock()
    ch.query.return_value.result_rows = [(3,)]   # 3 trades recorded

    with patch.object(s, "_container_is_running", return_value=False), \
         patch.object(s, "_ch_client", return_value=ch), \
         patch.object(s, "_send_telegram") as mock_tg:
        s.job_intraday_post_check()

    mock_tg.assert_not_called()
