"""
Unit tests for vol surface integration in edge_analysis.py.
Covers load_vol_surface_context() and migration 064.
All tests run offline — no ClickHouse connection required.
"""
import sys
import os
from unittest.mock import MagicMock
from datetime import date

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import edge_analysis as ea


# ── load_vol_surface_context ───────────────────────────────────────────────────

def _mock_ch_with_row(term_slope, skew_2pct, skew_3pct, atm_iv_30d):
    ch = MagicMock()
    ch.query.return_value.result_rows = [(term_slope, skew_2pct, skew_3pct, atm_iv_30d)]
    return ch

def _mock_ch_empty():
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    return ch


def test_load_vol_surface_context_returns_correct_values():
    ch = _mock_ch_with_row(term_slope=0.03, skew_2pct=0.015, skew_3pct=0.025, atm_iv_30d=0.18)
    ctx = ea.load_vol_surface_context(ch, "NIFTY", date(2024, 1, 24))
    assert ctx["term_slope"] == pytest.approx(0.03)
    assert ctx["skew_2pct"]  == pytest.approx(0.015)
    assert ctx["skew_3pct"]  == pytest.approx(0.025)
    assert ctx["atm_iv_30d"] == pytest.approx(0.18)


def test_load_vol_surface_context_returns_zeros_when_empty():
    ctx = ea.load_vol_surface_context(_mock_ch_empty(), "NIFTY", date(2024, 1, 24))
    assert ctx["term_slope"] == 0.0
    assert ctx["skew_2pct"]  == 0.0
    assert ctx["skew_3pct"]  == 0.0
    assert ctx["atm_iv_30d"] == 0.0


def test_load_vol_surface_context_handles_none_values():
    """None values in DB (e.g. freshly inserted rows) should default to 0."""
    ch = _mock_ch_with_row(None, None, None, None)
    ctx = ea.load_vol_surface_context(ch, "NIFTY", date(2024, 1, 24))
    assert ctx["term_slope"] == 0.0
    assert ctx["skew_2pct"]  == 0.0


def test_load_vol_surface_context_queries_correct_symbol_and_date():
    ch = _mock_ch_empty()
    ea.load_vol_surface_context(ch, "BANKNIFTY", date(2024, 3, 14))
    call_kwargs = ch.query.call_args[1]["parameters"]
    assert call_kwargs["sym"] == "BANKNIFTY"
    assert call_kwargs["d"] == date(2024, 3, 14)


# ── ensure_table includes vol surface columns ──────────────────────────────────

def test_ensure_table_includes_vol_surface_columns():
    """ensure_table CREATE TABLE DDL must include the 4 new vol surface columns."""
    ch = MagicMock()
    ea.ensure_table(ch)
    ddl = ch.command.call_args[0][0]
    for col in ("term_slope", "skew_2pct", "skew_3pct", "atm_iv_30d"):
        assert col in ddl, f"Column '{col}' missing from ensure_table DDL"


# ── Migration 064 sanity ───────────────────────────────────────────────────────

def test_migration_064_exists_and_has_correct_columns():
    path = os.path.join(os.path.dirname(__file__), "..",
                        "clickhouse", "migrations", "064_add_vol_surface_to_edge_analysis.sql")
    assert os.path.exists(path)
    with open(path) as f:
        sql = f.read()
    for col in ("term_slope", "skew_2pct", "skew_3pct", "atm_iv_30d"):
        assert col in sql, f"Column '{col}' missing from migration 064"
    assert "edge_analysis" in sql
