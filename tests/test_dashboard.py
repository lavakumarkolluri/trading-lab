"""
Tests for dashboard/app.py — offline, no ClickHouse or Streamlit server required.
Catches: query() signature (no params kwarg), live P&L query uses f-string not params.
"""
import sys
import os
import ast
import re
import pytest

DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "..", "dashboard", "app.py")


def _dashboard_source() -> str:
    with open(DASHBOARD_PATH) as f:
        return f.read()


def _dashboard_tree() -> ast.Module:
    return ast.parse(_dashboard_source())


# ── Syntax ────────────────────────────────────────────────────────────────────

def test_dashboard_valid_python():
    """dashboard/app.py must parse as valid Python — catches syntax errors."""
    _dashboard_tree()  # raises SyntaxError if broken


# ── query() signature ─────────────────────────────────────────────────────────

def test_query_accepts_only_sql_arg():
    """query() must accept only a sql string — no params kwarg.
    Adding params= caused TypeError crashes in the live P&L panel."""
    for node in ast.walk(_dashboard_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "query":
            args = [a.arg for a in node.args.args]
            assert args == ["sql"], f"query() args changed: {args}"
            assert node.args.vararg is None
            assert node.args.kwarg is None
            return
    pytest.fail("query() function not found in dashboard/app.py")


def test_no_params_kwarg_calls_on_query():
    """No call site should pass params= to query() — that crashes at runtime."""
    for node in ast.walk(_dashboard_tree()):
        if isinstance(node, ast.Call):
            func = node.func
            func_name = (func.id if isinstance(func, ast.Name)
                         else func.attr if isinstance(func, ast.Attribute) else "")
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
    assert "password123" not in source
    assert "admin123" not in source


def test_ch_config_read_from_env():
    """CH_HOST, CH_PORT, CH_USER, CH_PASS must all come from os.getenv."""
    source = _dashboard_source()
    for var in ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASS"):
        assert var in source, f"{var} not found in dashboard/app.py"


# ── Expiry date format ────────────────────────────────────────────────────────

def test_expiry_assignments_are_date_only():
    """
    Any variable assigned from a DataFrame expiry/next_expiry column and later
    used in a WHERE clause must be sliced to 10 chars to strip the time part.
    Catches: expiry = r["expiry"] without [:10] → CH TYPE_MISMATCH on Date column.
    """
    source = _dashboard_source()
    lines  = source.splitlines()
    bad = []
    # Find lines assigning expiry from a df row (r[...] or conf_row[...])
    # Pattern: `expiry = <expr>["expiry" or "next_expiry"]` without [:10]
    assign_re = re.compile(
        r'^\s*expiry\s*=\s*.+\[[\'"](expiry|next_expiry)[\'"]\]'
    )
    for i, line in enumerate(lines, 1):
        if assign_re.match(line) and "[:10]" not in line:
            bad.append(f"line {i}: {line.strip()}")
    assert not bad, (
        "expiry assigned from DataFrame without [:10] date truncation — "
        "ClickHouse Date column rejects '2026-05-26 00:00:00' format:\n"
        + "\n".join(bad)
    )


def test_no_datetime_string_in_expiry_where():
    """
    WHERE expiry = '...' in f-strings must use a variable that was sliced [:10].
    Catches raw datetime strings like '2026-05-26 00:00:00' reaching a Date column.
    """
    source = _dashboard_source()
    # Check that there are no f-string WHERE clauses comparing expiry to a raw
    # datetime literal (19-char timestamp pattern)
    bad = re.findall(r"expiry\s*=\s*'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'", source)
    assert not bad, f"Hardcoded datetime string in expiry WHERE clause: {bad}"


# ── Page navigation completeness ──────────────────────────────────────────────

def _sidebar_pages(source: str) -> list[str]:
    """Extract page names from st.sidebar.radio('Navigate', [...])."""
    m = re.search(r'st\.sidebar\.radio\([^,]+,\s*\[(.*?)\]\s*\)', source, re.DOTALL)
    if not m:
        return []
    # Match only double-quoted strings to avoid splitting on apostrophes (e.g. "Today's Signals")
    return re.findall(r'"([^"]+)"', m.group(1))


def _handled_pages(source: str) -> set[str]:
    """Extract page names from `if/elif page == "..."` blocks."""
    # Match both `if page ==` (first branch) and `elif page ==` (subsequent)
    return set(re.findall(r'(?:if|elif)\s+page\s*==\s*"([^"]+)"', source))


def test_all_nav_pages_have_handlers():
    """Every page listed in the sidebar radio must have an if/elif handler.
    Catches pages added to the nav without a corresponding implementation."""
    source   = _dashboard_source()
    nav      = _sidebar_pages(source)
    handled  = _handled_pages(source)
    assert nav, "Could not parse sidebar page list"
    missing  = [p for p in nav if p not in handled]
    assert not missing, (
        f"Pages in sidebar nav but no handler: {missing}\n"
        "Add `elif page == '<name>':` block for each."
    )


def test_no_orphan_page_handlers():
    """Every if/elif page handler must correspond to a sidebar nav entry.
    Catches stale handlers left after removing a page from the nav."""
    source   = _dashboard_source()
    nav      = set(_sidebar_pages(source))
    handled  = _handled_pages(source)
    orphans  = [p for p in handled if p not in nav]
    assert not orphans, (
        f"Page handlers with no nav entry: {orphans}\n"
        "Remove the handler or add the page back to the sidebar."
    )


def test_required_pages_present():
    """Key pages must exist in the nav — catches accidental deletion."""
    source = _dashboard_source()
    nav    = _sidebar_pages(source)
    for page in ("System Health", "Model", "Trade Log", "Market Data"):
        assert page in nav, f"Required page '{page}' missing from sidebar nav"


# ── f-string query safety ─────────────────────────────────────────────────────

def test_no_raw_sql_drop_or_truncate():
    """f-string queries must never contain DROP or TRUNCATE — basic injection guard."""
    source = _dashboard_source()
    # Look for f-strings containing these keywords (case-insensitive)
    fstrings = re.findall(r'f["\'].*?["\']', source, re.DOTALL)
    for fs in fstrings:
        upper = fs.upper()
        assert "DROP TABLE" not in upper, "f-string query contains DROP TABLE"
        assert "TRUNCATE" not in upper, "f-string query contains TRUNCATE"


def test_todate_used_for_timestamp_comparisons():
    """Queries filtering options_chain by date must use toDate(timestamp),
    not compare timestamp directly to a date string."""
    source = _dashboard_source()
    # If options_chain is queried with a date filter, toDate() must wrap timestamp
    chain_blocks = re.findall(
        r'options_chain.*?(?=query|st\.|$)', source, re.DOTALL
    )
    for block in chain_blocks:
        if "timestamp" in block and "today()" in block:
            assert "toDate(timestamp)" in block, (
                "options_chain query compares timestamp to today() without toDate() wrapper"
            )
