"""
Unit tests for intraday_monitor.py business logic.
All tests run offline — no ClickHouse connection required.
Catches: trailing stop gate, timezone offset, entry window, column schema.
"""
import sys
import os
from datetime import datetime, timedelta, time as dtime, timezone

UTC = timezone.utc
from unittest.mock import MagicMock, patch
import pytest

# Import without triggering ClickHouse connection
import importlib
monitor = importlib.import_module("intraday_monitor")

TARGET_INR   = monitor.TARGET_INR      # 2000.0
STOPLOSS_INR = monitor.STOPLOSS_INR    # 1000.0
TRAIL_PCT    = monitor.TRAIL_PCT       # 0.75


# ── Trailing stop ──────────────────────────────────────────────────────────────

def test_trail_peak_tracks_actual_pnl_not_clamped():
    """BUG-007: peak must be max(existing_peak, current_pnl) only — no TARGET_INR clamp.
    Removing TARGET_INR from max() ensures peak reflects reality."""
    pos = {
        "trade_id": "test-id", "symbol": "NIFTY", "expiry": "2026-05-07",
        "entry_time": datetime.now(UTC).replace(tzinfo=None), "strike": 24000.0,
        "entry_ce_ltp": 100.0, "entry_pe_ltp": 100.0, "entry_premium": 200.0,
        "lot_size": 75, "target_pts": 26.67, "stop_pts": 13.33,
        "target_inr": 2000.0, "stoploss_inr": 1000.0,
        "scorecard_conf": 70.0, "trailing_active": 0,
        "peak_pnl_inr": 0.0, "trail_stop_inr": 0.0,
        "entry_features": "{}",
    }
    ch = MagicMock()
    # P&L is exactly at target (2000), just activated
    peak, trail = monitor.update_trail(ch, pos, pnl_inr=2000.0, dry_run=True)
    assert peak == 2000.0
    assert trail == 2000.0 * TRAIL_PCT


def test_trail_peak_grows_with_pnl():
    """update_trail: peak must track the highest seen pnl_inr."""
    pos = {
        "trade_id": "x", "symbol": "NIFTY", "expiry": "2026-05-07",
        "entry_time": datetime.now(UTC).replace(tzinfo=None), "strike": 24000.0,
        "entry_ce_ltp": 100.0, "entry_pe_ltp": 100.0, "entry_premium": 200.0,
        "lot_size": 75, "target_pts": 26.67, "stop_pts": 13.33,
        "target_inr": 2000.0, "stoploss_inr": 1000.0, "scorecard_conf": 70.0,
        "trailing_active": 1, "peak_pnl_inr": 2000.0, "trail_stop_inr": 1500.0,
        "entry_features": "{}",
    }
    ch = MagicMock()
    peak, trail = monitor.update_trail(ch, pos, pnl_inr=3000.0, dry_run=True)
    assert peak == 3000.0
    assert abs(trail - 3000.0 * TRAIL_PCT) < 0.01


def test_trail_peak_does_not_shrink():
    """update_trail: if current pnl < peak, peak stays at previous peak."""
    pos = {
        "trade_id": "x", "symbol": "NIFTY", "expiry": "2026-05-07",
        "entry_time": datetime.now(UTC).replace(tzinfo=None), "strike": 24000.0,
        "entry_ce_ltp": 100.0, "entry_pe_ltp": 100.0, "entry_premium": 200.0,
        "lot_size": 75, "target_pts": 26.67, "stop_pts": 13.33,
        "target_inr": 2000.0, "stoploss_inr": 1000.0, "scorecard_conf": 70.0,
        "trailing_active": 1, "peak_pnl_inr": 3000.0, "trail_stop_inr": 2250.0,
        "entry_features": "{}",
    }
    ch = MagicMock()
    peak, trail = monitor.update_trail(ch, pos, pnl_inr=2500.0, dry_run=True)
    assert peak == 3000.0   # did not shrink
    assert abs(trail - 2250.0) < 0.01  # floor held


# ── Snapshot age / timezone ────────────────────────────────────────────────────

def test_snapshot_age_utc_to_ist_conversion():
    """Snapshot timestamps from ClickHouse are UTC-naive; must add 5.5h before age check."""
    utc_now = datetime.now(UTC).replace(tzinfo=None)
    ist_now = monitor.ist_naive()

    # A snapshot taken 5 min ago in UTC — should appear ~5 min old after IST conversion
    snap_utc = utc_now - timedelta(minutes=5)
    snap_ist = snap_utc + timedelta(hours=5, minutes=30)
    age_correct = (ist_now - snap_ist).total_seconds()

    # Without conversion (the old bug): IST_now - UTC_snap would be 5.5h + 5min off
    age_bugged = (ist_now - snap_utc).total_seconds()

    assert 0 < age_correct < 10 * 60, f"Correct age should be ~5 min, got {age_correct:.0f}s"
    assert age_bugged > 5 * 3600, f"Bugged age should be >5h, got {age_bugged:.0f}s"


# ── Entry window ───────────────────────────────────────────────────────────────

def test_entry_window_boundaries():
    """Entry is only allowed 09:30–14:00 IST."""
    assert monitor.ENTRY_START == dtime(9, 30)
    assert monitor.ENTRY_CUTOFF == dtime(14, 0)


# ── record_entry column schema ────────────────────────────────────────────────

def test_record_entry_column_count_matches_values():
    """column_names and values list in record_entry must have the same length."""
    ch = MagicMock()
    snap = {
        "expiry": "2026-05-07", "strike": 24000.0,
        "ce_ltp": 100.0, "pe_ltp": 100.0, "straddle": 200.0,
        "ce_iv": 15.0, "pe_iv": 15.0,
    }
    monitor.record_entry(ch, "NIFTY", snap, lot_size=75,
                         scorecard_conf=70.0, dry_run=False)
    call_args = ch.insert.call_args
    values_row = call_args[0][1][0]
    col_names  = call_args[1]["column_names"]
    assert len(values_row) == len(col_names), (
        f"record_entry mismatch: {len(values_row)} values vs {len(col_names)} columns"
    )


def test_record_entry_includes_entry_features():
    """record_entry must write entry_features to open_positions."""
    ch = MagicMock()
    snap = {
        "expiry": "2026-05-07", "strike": 24000.0,
        "ce_ltp": 100.0, "pe_ltp": 100.0, "straddle": 200.0,
        "ce_iv": 15.0, "pe_iv": 15.0,
    }
    monitor.record_entry(ch, "NIFTY", snap, lot_size=75,
                         scorecard_conf=70.0, dry_run=False)
    col_names = ch.insert.call_args[1]["column_names"]
    assert "entry_features" in col_names


# ── record_exit column schema ─────────────────────────────────────────────────

def _make_pos(lots=1, with_wings=False):
    pos = {
        "trade_id": "test-id", "symbol": "NIFTY", "expiry": "2026-05-07",
        "entry_time": datetime.now(UTC).replace(tzinfo=None), "strike": 24000.0,
        "entry_ce_ltp": 100.0, "entry_pe_ltp": 100.0, "entry_premium": 200.0,
        "lot_size": 75, "target_pts": 26.67, "stop_pts": 13.33,
        "target_inr": 2000.0 * lots, "stoploss_inr": 1000.0 * lots,
        "scorecard_conf": 70.0,
        "trailing_active": 0, "peak_pnl_inr": 0.0, "trail_stop_inr": 0.0,
        "entry_features": '{"test": 1}',
        "lots": lots,
        # iron fly columns (0 = naked straddle by default)
        "wing_ce_strike": 0.0, "wing_pe_strike": 0.0,
        "wing_ce_ltp": 0.0, "wing_pe_ltp": 0.0, "net_premium": 0.0,
    }
    if with_wings:
        pos.update({
            "wing_ce_strike": 24200.0, "wing_pe_strike": 23800.0,
            "wing_ce_ltp": 20.0, "wing_pe_ltp": 20.0,
            "net_premium": 160.0,   # straddle(200) - wings(40)
        })
    return pos


def test_record_exit_open_positions_includes_entry_features():
    """record_exit close update to open_positions must include entry_features."""
    ch = MagicMock()
    monitor.record_exit(ch, _make_pos(), current_straddle=180.0,
                        exit_reason="stop", dry_run=False)
    # Second insert call is the open_positions close update
    close_call = ch.insert.call_args_list[1]
    col_names = close_call[1]["column_names"]
    assert "entry_features" in col_names


def test_record_exit_trade_outcomes_preserves_entry_features():
    """record_exit must write entry_features to trade_outcomes."""
    ch = MagicMock()
    pos = _make_pos()
    pos["entry_features"] = '{"scorecard_conf": 70}'
    monitor.record_exit(ch, pos, current_straddle=210.0,
                        exit_reason="stop", dry_run=False)
    first_call = ch.insert.call_args_list[0]
    col_names  = first_call[1]["column_names"]
    values_row = first_call[0][1][0]
    assert "entry_features" in col_names
    idx = col_names.index("entry_features")
    assert values_row[idx] == '{"scorecard_conf": 70}'


# ── Iron fly column schema ─────────────────────────────────────────────────────

def test_record_entry_includes_wing_columns():
    """record_entry must write wing columns to open_positions."""
    ch = MagicMock()
    # get_wing_ltps is called inside record_entry; mock it to return known values
    with patch.object(monitor, "get_wing_ltps", return_value=(20.0, 18.0)):
        snap = {
            "expiry": "2026-05-07", "strike": 24000.0,
            "ce_ltp": 100.0, "pe_ltp": 100.0, "straddle": 200.0,
            "ce_iv": 15.0, "pe_iv": 15.0,
        }
        monitor.record_entry(ch, "NIFTY", snap, lot_size=75,
                             scorecard_conf=70.0, dry_run=False)
    col_names  = ch.insert.call_args[1]["column_names"]
    values_row = ch.insert.call_args[0][1][0]
    for col in ("wing_ce_strike", "wing_pe_strike", "wing_ce_ltp", "wing_pe_ltp", "net_premium"):
        assert col in col_names, f"missing column: {col}"
    np_idx = col_names.index("net_premium")
    assert values_row[np_idx] == pytest.approx(162.0)   # 200 - 20 - 18


def test_record_exit_iron_fly_pnl_uses_net_premium():
    """Iron fly P&L is entry net_premium minus current net position value."""
    ch = MagicMock()
    pos = _make_pos(with_wings=True)   # net_premium=160
    # Current: straddle=170, wings=30 → net=140; P&L = 160-140 = +20pts × 75 × 1 = ₹1500
    monitor.record_exit(ch, pos, current_straddle=140.0, exit_reason="target", dry_run=False)
    first_call = ch.insert.call_args_list[0]
    col_names  = first_call[1]["column_names"]
    values_row = first_call[0][1][0]
    pnl_pts_idx = col_names.index("pnl_pts")
    pnl_inr_idx = col_names.index("pnl_inr")
    assert values_row[pnl_pts_idx] == pytest.approx(20.0)
    assert values_row[pnl_inr_idx] == pytest.approx(1500.0)


def test_record_exit_naked_pnl_uses_entry_premium():
    """Naked straddle P&L uses entry_premium (legacy, no wings)."""
    ch = MagicMock()
    pos = _make_pos()   # wing_ce_strike=0 → naked
    monitor.record_exit(ch, pos, current_straddle=160.0, exit_reason="eod", dry_run=False)
    first_call = ch.insert.call_args_list[0]
    col_names  = first_call[1]["column_names"]
    values_row = first_call[0][1][0]
    pnl_pts_idx = col_names.index("pnl_pts")
    assert values_row[pnl_pts_idx] == pytest.approx(40.0)   # 200 - 160


def test_record_exit_wing_columns_in_trade_outcomes():
    """record_exit must store wing columns in trade_outcomes."""
    ch = MagicMock()
    pos = _make_pos(with_wings=True)
    monitor.record_exit(ch, pos, current_straddle=140.0, exit_reason="eod", dry_run=False)
    first_call = ch.insert.call_args_list[0]
    col_names  = first_call[1]["column_names"]
    for col in ("wing_ce_strike", "wing_pe_strike", "net_premium"):
        assert col in col_names, f"missing in trade_outcomes: {col}"


def test_migration_068_exists():
    """Migration 068 adding iron fly wing columns must exist."""
    import glob
    files = glob.glob(os.path.join(
        os.path.dirname(__file__), "..", "clickhouse", "migrations", "068_*.sql"
    ))
    assert files, "Migration 068 (iron fly wings) not found"


# ── Graduation stage gate ─────────────────────────────────────────────────────

def test_get_graduation_stage_returns_stage_from_db():
    """get_graduation_stage returns the stage stored in analysis.strategy_graduation."""
    ch = MagicMock()
    ch.query.return_value.result_rows = [(2,)]
    stage = monitor.get_graduation_stage(ch, "iron_fly_0dte")
    assert stage == 2

def test_get_graduation_stage_defaults_to_1_on_empty():
    """When no row exists for strategy, stage defaults to 1 (BACKTEST — no trading)."""
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    stage = monitor.get_graduation_stage(ch, "iron_fly_0dte")
    assert stage == 1

def test_get_graduation_stage_defaults_to_1_on_error():
    """DB error → default to Stage 1 (safe: no trading)."""
    ch = MagicMock()
    ch.query.side_effect = Exception("connection refused")
    stage = monitor.get_graduation_stage(ch, "iron_fly_0dte")
    assert stage == 1


# ── MIN_CONFIDENCE raised to 60 ───────────────────────────────────────────────

def test_min_confidence_at_least_60():
    """MIN_CONFIDENCE=50 is a coin-flip — must be ≥ 60 to be meaningful."""
    assert monitor.MIN_CONFIDENCE >= 60, (
        f"MIN_CONFIDENCE={monitor.MIN_CONFIDENCE} is too low. "
        "A score of 50-59 is essentially random for this model."
    )


# ── critical_features_all_zero gate ──────────────────────────────────────────

def test_critical_features_all_zero_true_when_iv_missing():
    """Returns True when iv_rank, atm_ce_iv, and vix are all zero (CRIT-003 symptom)."""
    features = {"iv_rank": 0.0, "atm_ce_iv": 0.0, "vix": 0.0, "pcr_oi": 0.8}
    assert monitor.critical_features_all_zero(features) is True


def test_critical_features_all_zero_false_when_vix_present():
    """Returns False when at least one critical feature is non-zero."""
    features = {"iv_rank": 0.0, "atm_ce_iv": 0.0, "vix": 14.5, "pcr_oi": 0.8}
    assert monitor.critical_features_all_zero(features) is False


def test_critical_features_all_zero_false_on_empty_dict():
    """Empty dict (score not available) must NOT block — returns False so logic falls through."""
    assert monitor.critical_features_all_zero({}) is False


def test_get_score_features_json_returns_dict_on_hit():
    """Returns parsed dict from features_json column."""
    import json
    ch = MagicMock()
    ch.query.return_value.result_rows = [('{"iv_rank": 24.5, "vix": 18.0}',)]
    result = monitor.get_score_features_json(ch, "NIFTY")
    assert result == {"iv_rank": 24.5, "vix": 18.0}


def test_get_score_features_json_returns_empty_on_miss():
    """Returns {} when no score exists (within 7 days)."""
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    result = monitor.get_score_features_json(ch, "NIFTY")
    assert result == {}


def test_get_score_features_json_returns_empty_on_error():
    """Returns {} on DB exception — must not crash."""
    ch = MagicMock()
    ch.query.side_effect = Exception("timeout")
    result = monitor.get_score_features_json(ch, "NIFTY")
    assert result == {}


# ── MIDCPNIFTY config completeness (HIGH-007) ────────────────────────────────

def test_midcpnifty_in_symbols():
    """MIDCPNIFTY must be in SYMBOLS list (HIGH-007)."""
    assert "MIDCPNIFTY" in monitor.SYMBOLS


def test_midcpnifty_config_completeness():
    """Every config dict must have an entry for every symbol in SYMBOLS."""
    config_dicts = {
        "DEFAULT_LOT_SIZES": monitor.DEFAULT_LOT_SIZES,
        "MIN_PREMIUM": monitor.MIN_PREMIUM,
        "WING_PTS": monitor.WING_PTS,
    }
    for name, d in config_dicts.items():
        for sym in monitor.SYMBOLS:
            assert sym in d, f"{name} missing key '{sym}'"


def test_all_symbols_have_four_entries():
    """Exactly 4 symbols are configured (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY)."""
    assert set(monitor.SYMBOLS) == {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
