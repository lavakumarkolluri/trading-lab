"""
Tests for pipeline/startup_validator.py.
Verifies that required env vars are checked before any work begins.
All tests run offline — no ClickHouse or Docker required.
"""
import sys
import os
import logging
from unittest.mock import patch
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import startup_validator as sv


def test_validate_passes_when_all_vars_set(monkeypatch):
    """No exit when all required vars have non-empty values."""
    monkeypatch.setenv("CH_PASSWORD", "secret123")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok:abc")
    sv.validate(["CH_PASSWORD", "TELEGRAM_BOT_TOKEN"])   # must not raise


def test_validate_exits_when_single_var_missing(monkeypatch):
    """sys.exit(1) when a required var is absent from the environment."""
    monkeypatch.delenv("CH_PASSWORD", raising=False)
    with pytest.raises(SystemExit) as exc:
        sv.validate(["CH_PASSWORD"])
    assert exc.value.code == 1


def test_validate_exits_when_var_is_empty_string(monkeypatch):
    """Empty string counts as missing — same as unset."""
    monkeypatch.setenv("CH_PASSWORD", "")
    with pytest.raises(SystemExit) as exc:
        sv.validate(["CH_PASSWORD"])
    assert exc.value.code == 1


def test_validate_exits_when_var_is_whitespace_only(monkeypatch):
    """Whitespace-only value counts as missing (stripped to empty)."""
    monkeypatch.setenv("CH_PASSWORD", "   ")
    with pytest.raises(SystemExit) as exc:
        sv.validate(["CH_PASSWORD"])
    assert exc.value.code == 1


def test_validate_exits_on_first_missing_reports_all(monkeypatch, caplog):
    """All missing vars are logged before exit — not just the first one."""
    monkeypatch.delenv("VAR_A", raising=False)
    monkeypatch.delenv("VAR_B", raising=False)
    monkeypatch.setenv("VAR_C", "present")
    with caplog.at_level(logging.ERROR, logger="startup_validator"):
        with pytest.raises(SystemExit):
            sv.validate(["VAR_A", "VAR_B", "VAR_C"])
    assert "VAR_A" in caplog.text
    assert "VAR_B" in caplog.text
    assert "VAR_C" not in caplog.text   # present — should not appear in error logs


def test_validate_empty_list_always_passes(monkeypatch):
    """validate([]) is a no-op — useful when a service has no required vars."""
    sv.validate([])   # must not raise


def test_validate_service_name_appears_in_log(monkeypatch, caplog):
    """service= kwarg prefixes all error log lines for easy filtering."""
    monkeypatch.delenv("MISSING_VAR", raising=False)
    with caplog.at_level(logging.ERROR, logger="startup_validator"):
        with pytest.raises(SystemExit):
            sv.validate(["MISSING_VAR"], service="confidence_scorer")
    assert "confidence_scorer" in caplog.text
    assert "MISSING_VAR" in caplog.text


def test_validate_ch_password_is_required_for_scheduler():
    """Regression: CH_PASSWORD must always be in the scheduler's required list."""
    required = sv.REQUIRED_VARS.get("scheduler", [])
    assert "CH_PASSWORD" in required, (
        "scheduler must declare CH_PASSWORD — without it, ClickHouse connections silently fail"
    )


def test_validate_ch_password_is_required_for_confidence_scorer():
    """confidence_scorer reads analysis.confidence_scores — needs CH_PASSWORD."""
    required = sv.REQUIRED_VARS.get("confidence_scorer", [])
    assert "CH_PASSWORD" in required


def test_validate_intraday_monitor_requires_kite_vars():
    """intraday_monitor places live orders — Kite credentials are mandatory."""
    required = sv.REQUIRED_VARS.get("intraday_monitor", [])
    assert "KITE_API_KEY" in required
    assert "KITE_ACCESS_TOKEN" in required


def test_validate_meta_pipeline_requires_host_project_dir():
    """meta_pipeline uses HOST_PROJECT_DIR for DooD project-directory (D028)."""
    required = sv.REQUIRED_VARS.get("meta_pipeline", [])
    assert "HOST_PROJECT_DIR" in required
