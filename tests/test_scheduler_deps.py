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
