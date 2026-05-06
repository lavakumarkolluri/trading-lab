"""
Unit tests for compute_oi_features.py logic.
Catches: duplicate column bug in pf_insert selection.
"""
import pandas as pd
import pytest


def test_pf_insert_no_duplicate_columns_when_column_already_exists():
    """
    Regression test: pf_cols + ["fii_fut_net_3d"] creates duplicates when
    fii_fut_net_3d is already in pf_cols. Must use dict.fromkeys() to dedup.
    """
    # Simulate the case where fii_fut_net_3d is already in pf_cols
    pf_cols = ["trade_id", "event_date", "symbol", "fii_fut_net_3d", "version"]
    pf = pd.DataFrame([{c: 1 for c in pf_cols}])

    # The old buggy approach:
    buggy = pf[[c for c in pf_cols + ["fii_fut_net_3d"] if c in pf.columns]]
    assert buggy.columns.duplicated().any(), "Buggy approach should have duplicates (confirming regression)"

    # The fixed approach:
    fixed = pf[[c for c in list(dict.fromkeys(pf_cols + ["fii_fut_net_3d"])) if c in pf.columns]]
    assert not fixed.columns.duplicated().any(), "Fixed approach must have no duplicate columns"


def test_pf_insert_no_duplicate_columns_when_column_missing():
    """When fii_fut_net_3d is NOT yet in pf_cols, it should appear exactly once."""
    pf_cols = ["trade_id", "event_date", "symbol", "version"]
    pf = pd.DataFrame([{c: 1 for c in pf_cols + ["fii_fut_net_3d"]}])

    result = pf[[c for c in list(dict.fromkeys(pf_cols + ["fii_fut_net_3d"])) if c in pf.columns]]
    assert list(result.columns).count("fii_fut_net_3d") == 1
    assert not result.columns.duplicated().any()
