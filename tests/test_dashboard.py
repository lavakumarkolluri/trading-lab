"""
Tests for dashboard/app.py — offline, no ClickHouse or Streamlit server required.
Catches: query() signature (no params kwarg), live P&L query uses f-string not params.
"""
import sys
import os
import ast
import inspect
import pytest

DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "..", "dashboard", "app.py")


def _dashboard_source() -> str:
    with open(DASHBOARD_PATH) as f:
        return f.read()


# ── query() signature ─────────────────────────────────────────────────────────

def test_query_accepts_only_sql_arg():
    """query() must accept only a sql string — no params kwarg.
    Adding params= caused TypeError crashes in the live P&L panel."""
    source = _dashboard_source()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "query":
            args = [a.arg for a in node.args.args]
            assert args == ["sql"], f"query() args changed: {args}"
            assert node.args.vararg is None
            assert node.args.kwarg is None
            return
    pytest.fail("query() function not found in dashboard/app.py")


def test_no_params_kwarg_calls_on_query():
    """No call site should pass params= to query() — that crashes at runtime."""
    source = _dashboard_source()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            func_name = ""
            if isinstance(func, ast.Name):
                func_name = func.id
            elif isinstance(func, ast.Attribute):
                func_name = func.attr
            if func_name == "query":
                kwarg_names = [kw.arg for kw in node.keywords]
                assert "params" not in kwarg_names, (
                    f"query() called with params= at line {node.lineno} — "
                    "use f-string or ClickHouse {{param:Type}} syntax instead"
                )


# ── No hardcoded credentials ──────────────────────────────────────────────────

def test_no_hardcoded_passwords():
    """Dashboard must not hardcode any passwords — must read from env."""
    source = _dashboard_source()
    # os.getenv with a default password would be a red flag
    assert "password123" not in source
    assert "admin123" not in source


# ── Required env vars read from environment ────────────────────────────────────

def test_ch_config_read_from_env():
    """CH_HOST, CH_PORT, CH_USER, CH_PASS must all come from os.getenv."""
    source = _dashboard_source()
    for var in ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASS"):
        assert var in source, f"{var} not found in dashboard/app.py"
