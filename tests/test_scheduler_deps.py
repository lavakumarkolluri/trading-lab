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


def test_strategy_backtester_in_upstream_row_check():
    """DAILY-006: confidence_scorer must not retrain when spread_backtest is empty.
    strategy_backtester must be in UPSTREAM_ROW_CHECK so a 'success' record with
    0 rows still blocks the downstream scorer."""
    import scheduler as s
    assert "strategy_backtester" in s.UPSTREAM_ROW_CHECK
    assert "spread_backtest" in s.UPSTREAM_ROW_CHECK["strategy_backtester"]


def test_confidence_scorer_blocked_when_spread_backtest_empty():
    """DAILY-006: If spread_backtest has 0 rows, confidence_scorer is blocked
    even if strategy_backtester recorded a success status."""
    import scheduler as s

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "pipeline_runs" in sql:
            result.result_rows = [("success", "2026-05-13")]
        else:
            result.result_rows = [(0,)]  # spread_backtest is empty
        return result

    ch = MagicMock()
    ch.query.side_effect = side_effect
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is False
    assert "0 rows" in reason


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
    """If compute_oi_features status=failed (within last 3 days), confidence_scorer must be blocked."""
    import scheduler as s
    ch = _mock_ch({"compute_oi_features": [("failed",  "2026-05-13")],
                   "strategy_backtester":  [("success", "2026-05-13")]})
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is False
    assert "compute_oi_features" in reason


def test_upstream_ok_false_when_upstream_missing():
    """If compute_oi_features has no record in the last 3 days, block confidence_scorer."""
    import scheduler as s
    ch = _mock_ch({"compute_oi_features": [],
                   "strategy_backtester":  [("success", "2026-05-13")]})
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-13")
    assert ok is False


def test_upstream_ok_true_when_all_upstreams_succeed():
    """All upstreams succeeded within last 3 days → confidence_scorer is cleared to run."""
    import scheduler as s
    ch = _mock_ch({"compute_oi_features": [("success", "2026-05-13")],
                   "strategy_backtester":  [("success", "2026-05-13")]})
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


def test_upstream_ok_false_when_row_count_is_zero():
    """C3: upstream recorded success but produced 0 rows — downstream must be blocked."""
    import scheduler as s

    def side_effect(sql, parameters=None):
        result = MagicMock()
        svc = (parameters or {}).get("service", "")
        if "pipeline_runs" in sql:
            result.result_rows = [("success", "2026-05-13")]
        else:
            # row-count query returns 0 — table is empty
            result.result_rows = [(0,)]
        return result

    ch = MagicMock()
    ch.query.side_effect = side_effect
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("options_eod_summary_pipeline", "2026-05-13")
    assert ok is False
    assert "0 rows" in reason


def test_upstream_ok_true_when_row_count_nonzero():
    """C3: upstream recorded success AND has rows — downstream is cleared."""
    import scheduler as s

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "pipeline_runs" in sql:
            result.result_rows = [("success", "2026-05-13")]
        else:
            result.result_rows = [(42,)]   # 42 rows written
        return result

    ch = MagicMock()
    ch.query.side_effect = side_effect
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("options_eod_summary_pipeline", "2026-05-13")
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


# ── _is_trading_holiday (C5) ──────────────────────────────────────────────────

def test_is_trading_holiday_true_when_holiday():
    """Returns True when trading_holidays table has a matching full_closure NSE row."""
    import scheduler as s
    ch = MagicMock()
    ch.query.return_value.result_rows = [(1,)]
    with patch.object(s, "_ch_client", return_value=ch):
        s._HOLIDAY_CACHE.clear()
        result = s._is_trading_holiday("2026-01-26")
    assert result is True


def test_is_trading_holiday_false_on_trading_day():
    """Returns False when no holiday row exists for the date."""
    import scheduler as s
    ch = MagicMock()
    ch.query.return_value.result_rows = [(0,)]
    with patch.object(s, "_ch_client", return_value=ch):
        s._HOLIDAY_CACHE.clear()
        result = s._is_trading_holiday("2026-05-19")
    assert result is False


def test_is_trading_holiday_fail_open_on_ch_error():
    """If CH is unreachable, assume trading day (fail-open) so jobs are not blocked."""
    import scheduler as s
    ch = MagicMock()
    ch.query.side_effect = RuntimeError("CH down")
    with patch.object(s, "_ch_client", return_value=ch):
        s._HOLIDAY_CACHE.clear()
        result = s._is_trading_holiday("2026-01-26")
    assert result is False


def test_intraday_jobs_skip_on_holiday():
    """option_chain_intraday and intraday_monitor must not start on a market holiday."""
    import scheduler as s
    with patch.object(s, "_is_trading_holiday", return_value=True), \
         patch.object(s, "_run_background") as mock_bg:
        s.job_option_chain_intraday()
        s.job_intraday_monitor()
    mock_bg.assert_not_called()


def test_eod_job_skips_on_holiday():
    """job_option_chain_eod must not run on a market holiday (no bhavcopy published)."""
    import scheduler as s
    with patch.object(s, "_is_trading_holiday", return_value=True), \
         patch.object(s, "_run") as mock_run:
        s.job_option_chain_eod()
    mock_run.assert_not_called()


# ── _run() retry logic (H6) ───────────────────────────────────────────────────

def test_run_retries_three_times_on_failure():
    """_run retries up to 3 attempts when the service exits non-zero."""
    import scheduler as s
    fail = MagicMock()
    fail.returncode = 1
    call_count = {"n": 0}

    def fake_subprocess_run(cmd, **kwargs):
        call_count["n"] += 1
        return fail

    with patch.object(s, "_upstream_ok", return_value=(True, "")), \
         patch.object(s, "_record_run"), \
         patch("subprocess.run", side_effect=fake_subprocess_run), \
         patch("time.sleep"):
        s._run("some_service")

    assert call_count["n"] == 3, f"Expected 3 attempts, got {call_count['n']}"


def test_run_stops_retrying_on_success():
    """_run records success and stops after the first successful attempt."""
    import scheduler as s
    fail = MagicMock(returncode=1)
    ok = MagicMock(returncode=0)

    with patch.object(s, "_upstream_ok", return_value=(True, "")), \
         patch.object(s, "_record_run") as mock_record, \
         patch("subprocess.run", side_effect=[fail, ok]), \
         patch("time.sleep"):
        s._run("some_service")

    statuses = [call[0][2] for call in mock_record.call_args_list]
    assert "success" in statuses
    assert "failed" not in statuses


def test_run_records_failed_after_all_retries_exhausted():
    """After 3 failed attempts, _run records exactly one 'failed' entry."""
    import scheduler as s
    fail = MagicMock(returncode=2)

    with patch.object(s, "_upstream_ok", return_value=(True, "")), \
         patch.object(s, "_record_run") as mock_record, \
         patch("subprocess.run", return_value=fail), \
         patch("time.sleep"):
        s._run("some_service")

    statuses = [call[0][2] for call in mock_record.call_args_list]
    assert statuses == ["failed"], f"Expected exactly one 'failed' record, got {statuses}"


# ── EOD startup recovery — multi-day gap fix (2026-05-31) ────────────────────

def _eod_recovery_now(hour=22, minute=0, weekday=0):
    """Return a weekday UTC datetime at the given hour (default Monday 22:00 UTC)."""
    from datetime import datetime as real_dt
    # Monday 2026-05-18 22:00 UTC (past the 16:10 window)
    return real_dt(2026, 5, 18, hour, minute, 0)


def test_eod_recovery_triggers_on_multi_day_gap():
    """If option_chain_historical has not run in >1 day, recovery fires even outside
    the old 16:10-20:10 window — preventing the 2-week data gap seen on 2026-05-31."""
    import scheduler as s
    from datetime import date as real_date

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "run_date" in (parameters or {}):
            result.result_rows = []          # no record for today
        else:
            # last success was 5 days ago
            result.result_rows = [(real_date(2026, 5, 13),)]
        return result

    ch = MagicMock()
    ch.query.side_effect = side_effect

    with patch.object(s, "datetime") as mock_dt, \
         patch.object(s, "_TRACKING_OK", True), \
         patch.object(s, "_ch_client", return_value=ch), \
         patch.object(s, "_container_is_running", return_value=True), \
         patch.object(s, "job_option_chain_eod") as mock_eod, \
         patch.dict("os.environ", {"CH_PASSWORD": "secret"}):
        mock_dt.now.return_value = _eod_recovery_now(hour=22)
        s._startup_recovery()

    mock_eod.assert_called_once()


def test_eod_recovery_skips_when_ran_today():
    """No recovery when option_chain_historical already succeeded today."""
    import scheduler as s
    from datetime import date as real_date

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "run_date" in (parameters or {}):
            result.result_rows = [("success",)]   # ran today
        else:
            result.result_rows = [(real_date(2026, 5, 18),)]
        return result

    ch = MagicMock()
    ch.query.side_effect = side_effect

    with patch.object(s, "datetime") as mock_dt, \
         patch.object(s, "_TRACKING_OK", True), \
         patch.object(s, "_ch_client", return_value=ch), \
         patch.object(s, "_container_is_running", return_value=True), \
         patch.object(s, "job_option_chain_eod") as mock_eod, \
         patch.dict("os.environ", {"CH_PASSWORD": "secret"}):
        mock_dt.now.return_value = _eod_recovery_now(hour=22)
        s._startup_recovery()

    mock_eod.assert_not_called()


# ── P0-4: upstream_ok ordering guarantee ─────────────────────────────────────

def test_upstream_fails_if_most_recent_run_failed():
    """Old success cannot override a more recent failure on the same date.
    Regression guard: when scheduler AND service both write pipeline_runs records,
    the query ordering must surface the MOST RECENTLY STARTED run, not an older insert."""
    import scheduler as s
    # Mock returns [("failed", today)] — the DB's LIMIT 1 result after correct ordering.
    # Simulates: service wrote "success" at T1, scheduler wrote "failed" at T2 (T2>T1).
    ch = _mock_ch({
        "compute_oi_features": [("failed", "2026-05-31")],
        "strategy_backtester":  [("success", "2026-05-31")],
    })
    with patch.object(s, "_TRACKING_OK", True):
        with patch.object(s, "_ch_client", return_value=ch):
            ok, reason = s._upstream_ok("confidence_scorer", "2026-05-31")
    assert ok is False, "Recent failure must block downstream even if an older success exists"
    assert "compute_oi_features" in reason
    assert "failed" in reason


def test_upstream_ok_query_orders_by_started_at():
    """_upstream_ok must ORDER BY started_at DESC, not version DESC.
    started_at is the actual run start time; version is the INSERT timestamp and can
    diverge when scheduler and service both write records for the same run."""
    import scheduler as s
    import inspect
    source = inspect.getsource(s._upstream_ok)
    assert "started_at" in source, (
        "_upstream_ok must use ORDER BY started_at DESC — "
        "started_at captures when the run BEGAN; version captures when the INSERT happened"
    )


def test_eod_recovery_skips_before_window_start():
    """Recovery must not trigger before 16:10 UTC — let schedule handle it."""
    import scheduler as s
    from datetime import date as real_date

    def side_effect(sql, parameters=None):
        result = MagicMock()
        if "run_date" in (parameters or {}):
            result.result_rows = []           # no record for today
        else:
            result.result_rows = [(real_date(2026, 5, 17),)]  # yesterday
        return result

    ch = MagicMock()
    ch.query.side_effect = side_effect

    with patch.object(s, "datetime") as mock_dt, \
         patch.object(s, "_TRACKING_OK", True), \
         patch.object(s, "_ch_client", return_value=ch), \
         patch.object(s, "_container_is_running", return_value=True), \
         patch.object(s, "job_option_chain_eod") as mock_eod, \
         patch.dict("os.environ", {"CH_PASSWORD": "secret"}):
        # 08:00 UTC — before the 16:10 EOD window
        mock_dt.now.return_value = _eod_recovery_now(hour=8, minute=0)
        s._startup_recovery()

    mock_eod.assert_not_called()
