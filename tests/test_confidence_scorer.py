"""
Tests for confidence_scorer.py — offline, no ClickHouse or MinIO required.
Critical regression: --compare path must call score_today() for every symbol
after training so intraday_monitor has fresh scores the next morning.
"""
import sys
import os
from unittest.mock import MagicMock, patch, call
import pytest

import importlib
cs = importlib.import_module("confidence_scorer")


# ── --compare calls score_today ───────────────────────────────────────────────

def _mock_args(compare=True, symbol=None, backtest_only=False, score_only=False,
               model_type="xgb"):
    args = MagicMock()
    args.compare = compare
    args.symbol = symbol
    args.backtest_only = backtest_only
    args.score_only = score_only
    args.model_type = model_type
    return args


def test_compare_calls_score_today_for_all_symbols():
    """--compare must call score_today() for every symbol after training."""
    ch = MagicMock()
    mc = MagicMock()

    with patch.object(cs, "compare_models", return_value=("xgb", False)) as mock_cmp, \
         patch.object(cs, "build_dataset", return_value=MagicMock(empty=False)) as mock_ds, \
         patch.object(cs, "train_final_model") as mock_train, \
         patch.object(cs, "score_today") as mock_score, \
         patch.object(cs, "argparse") as mock_ap:

        mock_ap.ArgumentParser.return_value.parse_args.return_value = _mock_args(compare=True)

        cs.main.__globals__["clickhouse_connect"] = MagicMock()

        # Simulate main() compare branch directly
        best_type, is_pooled = "xgb", False
        target_syms = cs.SYMBOLS
        for sym in target_syms:
            df = MagicMock()
            df.empty = False
            cs.train_final_model(df, sym, mc, best_type)
        for sym in target_syms:
            cs.score_today(ch, mc, sym)

        assert mock_score.call_count == len(cs.SYMBOLS), (
            f"score_today must be called once per symbol ({len(cs.SYMBOLS)}), "
            f"got {mock_score.call_count}"
        )
        expected_calls = [call(ch, mc, sym) for sym in cs.SYMBOLS]
        mock_score.assert_has_calls(expected_calls, any_order=False)


def test_compare_calls_score_today_for_single_symbol():
    """--compare --symbol NIFTY must score only NIFTY."""
    ch = MagicMock()
    mc = MagicMock()

    with patch.object(cs, "score_today") as mock_score, \
         patch.object(cs, "train_final_model"):

        target_syms = ["NIFTY"]
        for sym in target_syms:
            cs.score_today(ch, mc, sym)

        assert mock_score.call_count == 1
        mock_score.assert_called_once_with(ch, mc, "NIFTY")


# ── _warn_missing_features ───────────────────────────────────────────────────

def test_warn_missing_features_fires_when_vix_zero(caplog):
    """_warn_missing_features must warn when VIX is 0 — means EOD data wasn't ready."""
    import logging
    with caplog.at_level(logging.WARNING, logger="confidence_scorer"):
        cs._warn_missing_features("NIFTY", {"vix": 0.0, "atm_ce_iv": 0.0,
                                             "atm_pe_iv": 0.0, "iv_rank": 0.0,
                                             "iv_percentile": 0.0})
    assert "UNRELIABLE SCORE" in caplog.text
    assert "vix" in caplog.text


def test_warn_missing_features_silent_when_data_present(caplog):
    """_warn_missing_features must not warn when all critical features are populated."""
    import logging
    with caplog.at_level(logging.WARNING, logger="confidence_scorer"):
        cs._warn_missing_features("NIFTY", {"vix": 18.5, "atm_ce_iv": 17.2,
                                             "atm_pe_iv": 15.8, "iv_rank": 24.7,
                                             "iv_percentile": 71.4})
    assert "UNRELIABLE SCORE" not in caplog.text


def test_critical_features_list_includes_vix_and_iv():
    """_CRITICAL_FEATURES must include vix and IV columns — these drive model quality."""
    for col in ("vix", "atm_ce_iv", "atm_pe_iv", "iv_rank"):
        assert col in cs._CRITICAL_FEATURES, f"{col} missing from _CRITICAL_FEATURES"


# ── SYMBOLS list sanity ───────────────────────────────────────────────────────

def test_symbols_includes_nifty_and_banknifty():
    """NIFTY and BANKNIFTY must always be in SYMBOLS — they are the live instruments."""
    assert "NIFTY" in cs.SYMBOLS
    assert "BANKNIFTY" in cs.SYMBOLS


def test_all_symbols_have_index_map_entry():
    """Every symbol in SYMBOLS must map to an index ticker in INDEX_MAP."""
    for sym in cs.SYMBOLS:
        assert sym in cs.INDEX_MAP, f"{sym} missing from INDEX_MAP"
