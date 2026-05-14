"""
Tests for signal_agent.py.
All tests run offline — no ClickHouse or Telegram required.
"""
import sys
import os
import json
from datetime import date
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import signal_agent as m


# ── _explain_features ─────────────────────────────────────────────────────────

def test_explain_high_iv_rank():
    lines = m._explain_features({"iv_rank": 78})
    assert any("✅" in l and "IV Rank" in l and "78" in l for l in lines)


def test_explain_low_iv_rank():
    lines = m._explain_features({"iv_rank": 18})
    assert any("❌" in l and "IV Rank" in l for l in lines)


def test_explain_calm_vix():
    lines = m._explain_features({"vix": 11.5})
    assert any("✅" in l and "VIX" in l for l in lines)


def test_explain_high_vix():
    lines = m._explain_features({"vix": 25.0})
    assert any("❌" in l and "VIX" in l for l in lines)


def test_explain_low_atr():
    lines = m._explain_features({"atr_percentile": 22})
    assert any("✅" in l and "ATR" in l for l in lines)


def test_explain_high_atr():
    lines = m._explain_features({"atr_percentile": 80})
    assert any("❌" in l and "ATR" in l for l in lines)


def test_explain_hv_ratio_accelerating():
    lines = m._explain_features({"hv_ratio": 1.45})
    assert any("❌" in l and "HV5/HV20" in l for l in lines)


def test_explain_hv_ratio_decelerating():
    lines = m._explain_features({"hv_ratio": 0.72})
    assert any("✅" in l and "HV5/HV20" in l for l in lines)


def test_explain_ivhv_ratio_rich():
    lines = m._explain_features({"iv_hv5_ratio": 1.8})
    assert any("✅" in l and "IV/HV5" in l for l in lines)


def test_explain_ivhv_ratio_cheap():
    lines = m._explain_features({"iv_hv5_ratio": 0.6})
    assert any("❌" in l and "IV/HV5" in l for l in lines)


def test_explain_event_in_window():
    lines = m._explain_features({"event_in_window": 1, "days_to_event": 2})
    assert any("✅" in l and "Event" in l for l in lines)


def test_explain_empty_features_returns_empty():
    lines = m._explain_features({})
    assert lines == []


def test_explain_zero_vix_skipped():
    """vix=0 means missing data — should not emit a VIX line."""
    lines = m._explain_features({"vix": 0})
    assert not any("VIX" in l for l in lines)


# ── _verdict ──────────────────────────────────────────────────────────────────

def test_verdict_high_confidence():
    v = m._verdict(80)
    assert "STRONG BUY" in v


def test_verdict_moderate_confidence():
    v = m._verdict(62)
    assert "BUY" in v and "STRONG" not in v


def test_verdict_borderline():
    v = m._verdict(52)
    assert "BORDERLINE" in v


def test_verdict_skip():
    v = m._verdict(40)
    assert "SKIP" in v


# ── format_report ─────────────────────────────────────────────────────────────

def test_format_report_no_scores():
    report = m.format_report([], date(2026, 5, 14))
    assert "No scores available" in report


def test_format_report_includes_symbol():
    scores = [{
        "symbol": "NIFTY",
        "next_expiry": "2026-05-15",
        "confidence": 72.0,
        "expected_pnl_pct": 3.3,
        "features": {"iv_rank": 78, "vix": 11.5},
    }]
    report = m.format_report(scores, date(2026, 5, 14))
    assert "NIFTY" in report
    assert "72" in report
    assert "+3.3%" in report


def test_format_report_ordered_by_confidence():
    """Highest confidence symbol should appear first in report."""
    scores = [
        {"symbol": "BANKNIFTY", "next_expiry": "2026-05-14", "confidence": 55.0,
         "expected_pnl_pct": 1.0, "features": {}},
        {"symbol": "NIFTY", "next_expiry": "2026-05-15", "confidence": 80.0,
         "expected_pnl_pct": 5.0, "features": {}},
    ]
    report = m.format_report(scores, date(2026, 5, 14))
    nifty_pos = report.index("NIFTY")
    banknifty_pos = report.index("BANKNIFTY")
    assert nifty_pos < banknifty_pos


def test_format_report_includes_verdict():
    scores = [{
        "symbol": "NIFTY",
        "next_expiry": "2026-05-15",
        "confidence": 80.0,
        "expected_pnl_pct": 5.0,
        "features": {},
    }]
    report = m.format_report(scores, date(2026, 5, 14))
    assert "STRONG BUY" in report


def test_format_report_no_features_shows_fallback():
    scores = [{
        "symbol": "NIFTY",
        "next_expiry": "2026-05-15",
        "confidence": 60.0,
        "expected_pnl_pct": 2.0,
        "features": {},
    }]
    report = m.format_report(scores, date(2026, 5, 14))
    assert "no feature data" in report


# ── fetch_today_scores ────────────────────────────────────────────────────────

def test_fetch_today_scores_parses_features_json():
    features = {"iv_rank": 78.0, "vix": 11.5}
    ch = MagicMock()
    ch.query.return_value.result_rows = [
        ("NIFTY", date(2026, 5, 15), 72.0, 3.3, json.dumps(features))
    ]
    scores = m.fetch_today_scores(ch, date(2026, 5, 14))
    assert len(scores) == 1
    assert scores[0]["symbol"] == "NIFTY"
    assert scores[0]["features"]["iv_rank"] == 78.0


def test_fetch_today_scores_handles_bad_json():
    ch = MagicMock()
    ch.query.return_value.result_rows = [
        ("NIFTY", date(2026, 5, 15), 72.0, 3.3, "not-valid-json")
    ]
    scores = m.fetch_today_scores(ch, date(2026, 5, 14))
    assert scores[0]["features"] == {}


def test_fetch_today_scores_empty():
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    scores = m.fetch_today_scores(ch, date(2026, 5, 14))
    assert scores == []


# ── docker-compose + scheduler wiring ────────────────────────────────────────

def test_signal_agent_service_in_compose():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    with open(compose_path) as f:
        content = f.read()
    assert "signal_agent:" in content
    assert "signal_agent.py" in content


def test_signal_agent_in_scheduler():
    sched_path = os.path.join(os.path.dirname(__file__), "..", "pipeline", "scheduler.py")
    with open(sched_path) as f:
        content = f.read()
    assert "job_signal_agent" in content
    assert "signal_agent" in content
