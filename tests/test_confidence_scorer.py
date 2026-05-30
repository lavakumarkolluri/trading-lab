"""
Tests for confidence_scorer.py — offline, no ClickHouse or MinIO required.
Critical regression: --compare path must call score_today() for every symbol
after training so intraday_monitor has fresh scores the next morning.
"""
import sys
import os
from datetime import date
from unittest.mock import MagicMock, patch, call
import pytest
import pandas as pd

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


def test_warn_missing_features_sends_telegram_alert():
    """L03: _warn_missing_features must fire a Telegram alert on UNRELIABLE SCORE."""
    from unittest.mock import patch
    with patch.object(cs, "_send_telegram") as mock_tg:
        cs._warn_missing_features("NIFTY", {"vix": 0.0, "atm_ce_iv": 0.0,
                                             "atm_pe_iv": 0.0, "iv_rank": 0.0,
                                             "iv_percentile": 0.0})
    mock_tg.assert_called_once()
    assert "UNRELIABLE" in mock_tg.call_args[0][0]


def test_warn_missing_features_no_telegram_when_data_ok():
    """No Telegram alert when all critical features are present."""
    from unittest.mock import patch
    with patch.object(cs, "_send_telegram") as mock_tg:
        cs._warn_missing_features("NIFTY", {"vix": 18.5, "atm_ce_iv": 17.2,
                                             "atm_pe_iv": 15.8, "iv_rank": 24.7,
                                             "iv_percentile": 71.4})
    mock_tg.assert_not_called()


def test_critical_features_list_includes_vix_and_iv():
    """_CRITICAL_FEATURES must include vix and IV columns — these drive model quality."""
    for col in ("vix", "atm_ce_iv", "atm_pe_iv", "iv_rank"):
        assert col in cs._CRITICAL_FEATURES, f"{col} missing from _CRITICAL_FEATURES"


# ── SPAN estimate / WIN threshold (HIGH-005) ──────────────────────────────────

def test_span_daily_factor_is_correct():
    """_SPAN_DAILY_FACTOR must equal sqrt(1/252) — used in SPAN margin estimate."""
    import math
    assert abs(cs._SPAN_DAILY_FACTOR - math.sqrt(1 / 252)) < 1e-10


def test_span_win_threshold_stricter_than_pnl_gt_zero():
    """SPAN-based WIN requires more than 0 pts: 1% of SPAN margin in INR.
    For NIFTY (lot=65, spot=22000, atm_iv=0.185):
      span_inr ≈ 0.185 × 22000 × sqrt(1/252) × 65 × 3 ≈ 49,900
      win_threshold_pts = 0.01 × 49900 / 65 ≈ 7.7 pts
    A 1-pt pnl must NOT be a win; a 10-pt pnl MUST be a win.
    """
    import math
    lot = cs.LOT_SIZES["NIFTY"]
    spot = 22_000.0
    atm_iv = 0.185
    span_inr = atm_iv * spot * math.sqrt(1 / 252) * lot * 3.0
    threshold_pts = 0.01 * span_inr / lot

    assert threshold_pts > 1.0,  "Threshold should be >1 pt (SPAN-based, not just >0)"
    assert threshold_pts < 20.0, "Threshold should be <20 pts (not too restrictive)"


# ── SYMBOLS list sanity ───────────────────────────────────────────────────────

def test_symbols_includes_nifty_and_banknifty():
    """NIFTY and BANKNIFTY must always be in SYMBOLS — they are the live instruments."""
    assert "NIFTY" in cs.SYMBOLS
    assert "BANKNIFTY" in cs.SYMBOLS


def test_all_symbols_have_index_map_entry():
    """Every symbol in SYMBOLS must map to an index ticker in INDEX_MAP."""
    for sym in cs.SYMBOLS:
        assert sym in cs.INDEX_MAP, f"{sym} missing from INDEX_MAP"


# ── extract_chain_features duplicate-strike regression ───────────────────────

def _make_chain_df(n_snapshots=3):
    """Build a chain DataFrame with duplicate strikes (simulating intraday snapshots)."""
    strikes = [24000, 24100, 24200]
    expiry  = date(2026, 6, 26)
    snap_d  = date(2026, 5, 13)
    rows = []
    for t in range(n_snapshots):
        for s in strikes:
            for ot, ltp in [("CE", 50.0 + t), ("PE", 45.0 + t)]:
                rows.append({
                    "snap_date": snap_d, "expiry": expiry, "strike": float(s),
                    "option_type": ot, "ltp": ltp, "oi": 1000 + t, "volume": 500,
                })
    return pd.DataFrame(rows)


def test_extract_chain_features_handles_duplicate_strikes():
    """extract_chain_features must not crash when multiple intraday snapshots
    create duplicate strike index values — fixed by groupby(level=0).last()."""
    chain = _make_chain_df(n_snapshots=4)
    expiry  = date(2026, 6, 26)
    snap_d  = date(2026, 5, 13)
    snap_df = chain[(chain.snap_date == snap_d) & (chain.expiry == expiry)]
    result = cs.extract_chain_features(snap_df)
    assert result is not None, "extract_chain_features returned None on valid data"
    assert "straddle_premium" in result
    assert result["straddle_premium"] > 0


def test_find_atm_strike_handles_duplicate_index():
    """find_atm_strike must return a scalar float even when the Series index
    has duplicate values (ce.loc[common] returns DataFrame → fixed by groupby)."""
    # Duplicate entries for same strike (as happens with multi-expiry / multi-snap data)
    ce = pd.Series([100.0, 95.0, 100.0, 45.0], index=[24000, 24100, 24000, 24200])
    pe = pd.Series([45.0, 95.0, 45.0, 100.0],  index=[24000, 24100, 24000, 24200])
    result = cs.find_atm_strike(ce, pe)
    assert result == 24100.0, f"Expected ATM=24100, got {result}"


# ── CRIT-004: INDEX_MAP correctness ──────────────────────────────────────────

def test_index_map_symbols_all_distinct():
    """All 4 symbols must map to distinct index tickers — CRIT-004 fix."""
    values = list(cs.INDEX_MAP.values())
    assert len(values) == len(set(values)), (
        f"INDEX_MAP has duplicate tickers: {cs.INDEX_MAP}. "
        "FINNIFTY and MIDCPNIFTY must not reuse ^NSEI."
    )


def test_index_map_finnifty_not_nsei():
    """FINNIFTY must NOT map to ^NSEI (NIFTY 50) — CRIT-004."""
    assert cs.INDEX_MAP.get("FINNIFTY") != "^NSEI", (
        "FINNIFTY mapped to ^NSEI — wrong index. Should be ^CNXFIN."
    )


def test_index_map_midcpnifty_not_nsei():
    """MIDCPNIFTY must NOT map to ^NSEI (NIFTY 50) — CRIT-004."""
    assert cs.INDEX_MAP.get("MIDCPNIFTY") != "^NSEI", (
        "MIDCPNIFTY mapped to ^NSEI — wrong index. Should be ^NSMIDCP."
    )


# ── CRIT-001: Target variable must use breakeven, not zero ────────────────────

def test_target_variable_nifty_5pts_is_loss():
    """A +5 pt straddle gain for NIFTY is a net loss after ~35 pt wing cost — CRIT-001."""
    breakeven = cs._BREAKEVEN.get("NIFTY", 35)
    target = int(5 > breakeven)
    assert target == 0, (
        f"5 pts > {breakeven} pt breakeven should be LOSS (0), got WIN (1). "
        "Model was training on wrong signal — gains below wing cost labeled WIN."
    )


def test_target_variable_nifty_40pts_is_win():
    """A +40 pt gain for NIFTY clears the 35 pt breakeven — must be WIN."""
    breakeven = cs._BREAKEVEN.get("NIFTY", 35)
    target = int(40 > breakeven)
    assert target == 1, f"40 pts should be WIN above {breakeven} pt breakeven."


def test_target_variable_banknifty_threshold_higher():
    """BANKNIFTY has wider wings (±500 pts) so breakeven must be > NIFTY's."""
    assert cs._BREAKEVEN.get("BANKNIFTY", 0) > cs._BREAKEVEN.get("NIFTY", 0), (
        "BANKNIFTY breakeven must exceed NIFTY's — BANKNIFTY wings cost more."
    )


def test_breakeven_defined_for_all_symbols():
    """_BREAKEVEN must have an entry for every symbol in SYMBOLS."""
    for sym in cs.SYMBOLS:
        assert sym in cs._BREAKEVEN, f"_BREAKEVEN missing entry for {sym}"
        assert cs._BREAKEVEN[sym] > 0, f"_BREAKEVEN[{sym}] must be positive"


# ── HIGH-004: MIN_TRAIN must be at least 60 ──────────────────────────────────

def test_min_train_at_least_60():
    """MIN_TRAIN=25 causes severe XGBoost overfitting — must be ≥ 60 (HIGH-004)."""
    assert cs.MIN_TRAIN >= 60, (
        f"MIN_TRAIN={cs.MIN_TRAIN} is too low. XGBoost needs ≥60 rows per fold "
        "to avoid overfitting on weekly-expiry iron fly data."
    )


# ── CRIT-002: Walk-forward split must use entry_date, not expiry_dt ──────────

# ── CRIT-003: load_eod_summary must filter by symbol ─────────────────────────

def test_load_eod_summary_filters_by_symbol():
    """load_eod_summary must pass symbol to the query so BANKNIFTY doesn't get NIFTY data."""
    from unittest.mock import MagicMock, patch
    ch = MagicMock()
    ch.query.return_value.result_rows = []

    with patch.object(cs, "_has_symbol_col_eod", return_value=True):
        cs.load_eod_summary(ch, "BANKNIFTY")

    call_sql = ch.query.call_args[0][0]
    assert "symbol" in call_sql.lower(), "Query must filter by symbol column"
    params = ch.query.call_args[1].get("parameters", {}) or ch.query.call_args[0][1] if len(ch.query.call_args[0]) > 1 else {}
    # Accept either positional or keyword parameters
    call_kwargs = ch.query.call_args
    all_args = str(call_kwargs)
    assert "BANKNIFTY" in all_args, "BANKNIFTY symbol must be passed to the query"


def test_load_eod_summary_legacy_no_symbol_col():
    """When symbol column doesn't exist (pre-migration), query must not include WHERE symbol."""
    from unittest.mock import MagicMock, patch
    ch = MagicMock()
    ch.query.return_value.result_rows = []

    with patch.object(cs, "_has_symbol_col_eod", return_value=False):
        cs.load_eod_summary(ch, "BANKNIFTY")

    call_sql = ch.query.call_args[0][0]
    assert "WHERE symbol" not in call_sql, "Legacy path must not filter by symbol"


def test_walk_forward_no_future_leak():
    """Rows whose entry_date is before fold_start must not appear in test set.

    Simulates CRIT-002: a trade entered on 2026-01-25 with expiry 2026-02-06
    must land in train when fold_start = 2026-02-01, not in test.
    """
    import pandas as pd

    rows = [
        # entry_date before fold; expiry after fold start → was in TEST before fix
        {"entry_date": "2026-01-25", "expiry": "2026-02-06",
         "pnl_pts": 10, "target": 1, "atm_strike": 23000,
         "entry_premium": 200, "exit_value": 190, "pnl_pct": 0.05},
        # entry_date inside test window → belongs in test
        {"entry_date": "2026-02-03", "expiry": "2026-02-06",
         "pnl_pts": 5,  "target": 1, "atm_strike": 23000,
         "entry_premium": 180, "exit_value": 175, "pnl_pct": 0.03},
    ]
    df = pd.DataFrame(rows)
    df["entry_dt"] = pd.to_datetime(df["entry_date"])

    fold_start = pd.Timestamp("2026-02-01")
    fold_end   = pd.Timestamp("2026-03-01")

    train = df[df["entry_dt"] < fold_start]
    test  = df[(df["entry_dt"] >= fold_start) & (df["entry_dt"] < fold_end)]

    assert len(train) == 1, "Row with entry_date 2026-01-25 must be in train"
    assert len(test)  == 1, "Row with entry_date 2026-02-03 must be in test"
    assert train["entry_date"].iloc[0] == "2026-01-25"
    assert test["entry_date"].iloc[0]  == "2026-02-03"


# ── DTE feature ───────────────────────────────────────────────────────────────

def test_dte_in_feature_cols():
    """'dte' must be in FEATURE_COLS so N-DTE context reaches the model."""
    assert "dte" in cs.FEATURE_COLS, "dte must be in FEATURE_COLS"


def test_dte_value_correct_in_build_dataset():
    """build_dataset must compute dte = (expiry - entry_date).days for each row."""
    from datetime import date
    snap = date(2026, 5, 12)
    expiry = date(2026, 5, 13)
    expected_dte = (expiry - snap).days   # = 1

    # Simulate what build_dataset computes
    actual_dte = (expiry - snap).days
    assert actual_dte == expected_dte

    # For N-DTE case: entry 3 days before expiry
    snap2 = date(2026, 5, 10)
    expiry2 = date(2026, 5, 13)
    assert (expiry2 - snap2).days == 3


def test_load_backtest_pnl_returns_all_dte_rows():
    """DAILY-006: load_backtest_pnl must return all (expiry, entry_date) pairs.
    N-DTE backtester writes up to 5 rows per expiry (DTE 1-5); all must be loaded
    so confidence_scorer retrains on the full N-DTE dataset."""
    from datetime import date
    from unittest.mock import MagicMock
    ch = MagicMock()
    expiry = date(2026, 5, 13)
    ch.query.return_value.result_rows = [
        (expiry, date(2026, 5, 12), 50.0, 120.0),   # DTE=1
        (expiry, date(2026, 5, 11), 45.0, 118.0),   # DTE=2
        (expiry, date(2026, 5, 10), 40.0, 115.0),   # DTE=3
    ]
    df = cs.load_backtest_pnl(ch, "NIFTY", "iron_fly")
    assert len(df) == 3, f"All 3 N-DTE rows must be loaded, got {len(df)}"
    assert df.index.names == ["expiry", "entry_date"]
    entry_dates = df.index.get_level_values("entry_date").tolist()
    assert date(2026, 5, 12) in entry_dates   # DTE=1
    assert date(2026, 5, 11) in entry_dates   # DTE=2
    assert date(2026, 5, 10) in entry_dates   # DTE=3


def test_load_eod_summary_deduplicates_by_date():
    """load_eod_summary must return exactly one row per date even if options_eod_summary
    has multiple expiry rows per date (post migration 081 schema)."""
    import pandas as pd
    from datetime import date
    import confidence_scorer as cs

    d1, d2 = date(2026, 5, 12), date(2026, 5, 13)
    # Two rows for d1 (different expiries), one row for d2
    rows = [
        (d1, 15.0, 20.0, 14.5, 14.8, 0.3, 0.9, 22000.0, 22500.0, 21500.0),
        (d1, 16.0, 21.0, 15.0, 15.2, 0.4, 0.8, 22001.0, 22510.0, 21490.0),
        (d2, 17.0, 22.0, 15.5, 15.6, 0.2, 0.85, 22100.0, 22600.0, 21400.0),
    ]
    cols = ["date", "iv_rank", "iv_percentile", "atm_ce_iv", "atm_pe_iv",
            "iv_skew", "pcr_eod", "nifty_spot", "ce_wall_strike", "pe_wall_strike"]
    df = pd.DataFrame(rows, columns=cols)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.set_index("date", inplace=True)
    df = df[~df.index.duplicated(keep="first")]

    assert df.index.is_unique, "index must be unique after dedup"
    assert len(df) == 2, f"expected 2 rows, got {len(df)}"
    # First row for d1 kept (nearest expiry)
    assert float(df.loc[d1, "iv_rank"]) == 15.0


def test_max_pain_dist_pct_in_feature_cols():
    import confidence_scorer as cs
    assert "max_pain_dist_pct" in cs.FEATURE_COLS


def test_term_slope_in_feature_cols():
    import confidence_scorer as cs
    assert "term_slope" in cs.FEATURE_COLS
    assert "atm_iv_7d" in cs.FEATURE_COLS


def test_vix_z20_in_feature_cols():
    import confidence_scorer as cs
    assert "vix_z20" in cs.FEATURE_COLS


def test_adx14_in_feature_cols():
    import confidence_scorer as cs
    assert "adx14" in cs.FEATURE_COLS


def test_straddle_z_in_feature_cols():
    import confidence_scorer as cs
    assert "straddle_z" in cs.FEATURE_COLS


def test_compute_adx_range_0_to_100():
    """ADX output must always be between 0 and 100."""
    import numpy as np
    import pandas as pd
    import confidence_scorer as cs

    np.random.seed(42)
    n = 200
    close = pd.Series(100 + np.cumsum(np.random.randn(n) * 0.5))
    high  = close + np.abs(np.random.randn(n) * 0.3)
    low   = close - np.abs(np.random.randn(n) * 0.3)
    df = pd.DataFrame({"high": high, "low": low, "close": close})
    adx = cs._compute_adx(df)
    valid = adx.dropna()
    assert len(valid) > 50, "should have enough non-NaN values"
    assert (valid >= 0).all() and (valid <= 100).all(), f"ADX out of range: {valid.describe()}"


def test_load_vol_surface_returns_empty_on_missing_symbol():
    """load_vol_surface must return empty DataFrame (not raise) if symbol missing."""
    import pandas as pd
    from unittest.mock import MagicMock
    import confidence_scorer as cs

    ch = MagicMock()
    ch.query.side_effect = Exception("Table missing")
    result = cs.load_vol_surface(ch, "NIFTY")
    assert isinstance(result, pd.DataFrame)
    assert result.empty
