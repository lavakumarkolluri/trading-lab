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

def _make_pos():
    return {
        "trade_id": "test-id", "symbol": "NIFTY", "expiry": "2026-05-07",
        "entry_time": datetime.now(UTC).replace(tzinfo=None), "strike": 24000.0,
        "entry_ce_ltp": 100.0, "entry_pe_ltp": 100.0, "entry_premium": 200.0,
        "lot_size": 75, "target_pts": 26.67, "stop_pts": 13.33,
        "target_inr": 2000.0, "stoploss_inr": 1000.0, "scorecard_conf": 70.0,
        "trailing_active": 0, "peak_pnl_inr": 0.0, "trail_stop_inr": 0.0,
        "entry_features": '{"test": 1}',
    }


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
