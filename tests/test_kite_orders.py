"""
Unit tests for kite_orders.py and Kite integration in intraday_monitor.py.
All tests run offline — kiteconnect is mocked throughout.
Covers: KiteOrderManager, instrument lookup, margin check, order placement,
        record_entry/record_exit column alignment with new order ID fields,
        migration 066 content.
"""
import sys
import os
from datetime import date
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))


# ── KiteOrderManager ───────────────────────────────────────────────────────────

def _make_manager(kite=None):
    from kite_orders import KiteOrderManager
    mgr = KiteOrderManager(kite or MagicMock())
    return mgr


def _make_instruments():
    return [
        {"name": "NIFTY", "expiry": date(2026, 5, 15), "strike": 22000.0,
         "instrument_type": "CE", "tradingsymbol": "NIFTY26MAY22000CE",
         "instrument_token": 12345, "lot_size": 75},
        {"name": "NIFTY", "expiry": date(2026, 5, 15), "strike": 22000.0,
         "instrument_type": "PE", "tradingsymbol": "NIFTY26MAY22000PE",
         "instrument_token": 12346, "lot_size": 75},
        {"name": "BANKNIFTY", "expiry": date(2026, 5, 14), "strike": 50000.0,
         "instrument_type": "CE", "tradingsymbol": "BANKNIFTY26MAY50000CE",
         "instrument_token": 22345, "lot_size": 35},
    ]


def test_load_instruments_builds_cache():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = _make_instruments()
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    assert mgr.get_instrument("NIFTY", date(2026, 5, 15), 22000.0, "CE") is not None
    assert mgr.get_instrument("NIFTY", date(2026, 5, 15), 22000.0, "PE") is not None


def test_load_instruments_filters_non_target_symbols():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = [
        {"name": "RELIANCE", "expiry": date(2026, 5, 15), "strike": 1000.0,
         "instrument_type": "CE", "tradingsymbol": "RELIANCE26MAY1000CE",
         "instrument_token": 99999, "lot_size": 250},
    ]
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    assert mgr.get_instrument("RELIANCE", date(2026, 5, 15), 1000.0, "CE") is None


def test_load_instruments_noop_when_kite_none():
    from kite_orders import KiteOrderManager
    mgr = KiteOrderManager(None)
    mgr.load_instruments()   # must not raise
    assert mgr.get_instrument("NIFTY", date(2026, 5, 15), 22000.0, "CE") is None


def test_get_instrument_returns_none_for_missing():
    mgr = _make_manager()
    assert mgr.get_instrument("NIFTY", date(2026, 5, 15), 99999.0, "CE") is None


# ── check_margin ───────────────────────────────────────────────────────────────

def test_check_margin_returns_sum_of_total():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = _make_instruments()
    kite.order_margins.return_value = [{"total": 80000}, {"total": 75000}]
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    margin = mgr.check_margin("NIFTY", date(2026, 5, 15), 22000.0, lot_size=75, lots=1)
    assert margin == pytest.approx(155000.0)


def test_check_margin_returns_zero_when_kite_none():
    from kite_orders import KiteOrderManager
    mgr = KiteOrderManager(None)
    assert mgr.check_margin("NIFTY", date(2026, 5, 15), 22000.0, 75, 1) == 0.0


def test_check_margin_returns_zero_when_instrument_missing():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = []
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    assert mgr.check_margin("NIFTY", date(2026, 5, 15), 22000.0, 75, 1) == 0.0


def test_check_margin_returns_zero_on_api_error():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = _make_instruments()
    kite.order_margins.side_effect = Exception("API error")
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    assert mgr.check_margin("NIFTY", date(2026, 5, 15), 22000.0, 75, 1) == 0.0


# ── place_straddle_entry ───────────────────────────────────────────────────────

def test_place_straddle_entry_places_two_sell_orders():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = _make_instruments()
    kite.place_order.side_effect = ["order_ce_001", "order_pe_001"]
    kite.VARIETY_REGULAR = "regular"
    kite.TRANSACTION_TYPE_SELL = "SELL"
    kite.PRODUCT_MIS = "MIS"
    kite.ORDER_TYPE_MARKET = "MARKET"
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    ce_id, pe_id = mgr.place_straddle_entry("NIFTY", date(2026, 5, 15), 22000.0, 75, 1)
    assert ce_id == "order_ce_001"
    assert pe_id == "order_pe_001"
    assert kite.place_order.call_count == 2
    # Both orders must be SELL
    for c in kite.place_order.call_args_list:
        assert c[1]["transaction_type"] == "SELL"


def test_place_straddle_entry_returns_empty_when_kite_none():
    from kite_orders import KiteOrderManager
    mgr = KiteOrderManager(None)
    ce_id, pe_id = mgr.place_straddle_entry("NIFTY", date(2026, 5, 15), 22000.0, 75, 1)
    assert ce_id == "" and pe_id == ""


# ── place_straddle_exit ────────────────────────────────────────────────────────

def test_place_straddle_exit_places_two_buy_orders():
    from kite_orders import KiteOrderManager
    kite = MagicMock()
    kite.instruments.return_value = _make_instruments()
    kite.place_order.side_effect = ["exit_ce_001", "exit_pe_001"]
    kite.VARIETY_REGULAR = "regular"
    kite.TRANSACTION_TYPE_BUY = "BUY"
    kite.PRODUCT_MIS = "MIS"
    kite.ORDER_TYPE_MARKET = "MARKET"
    mgr = KiteOrderManager(kite)
    mgr.load_instruments()
    ce_id, pe_id = mgr.place_straddle_exit("NIFTY", date(2026, 5, 15), 22000.0, 75, 1)
    assert ce_id == "exit_ce_001"
    assert pe_id == "exit_pe_001"
    for c in kite.place_order.call_args_list:
        assert c[1]["transaction_type"] == "BUY"


# ── build_kite_client ─────────────────────────────────────────────────────────

def test_build_kite_client_returns_none_when_env_missing():
    from kite_orders import build_kite_client
    with patch.dict(os.environ, {"KITE_API_KEY": "", "KITE_ACCESS_TOKEN": ""}):
        assert build_kite_client() is None


# ── intraday_monitor record_entry column alignment ────────────────────────────

import importlib
monitor = importlib.import_module("intraday_monitor")


def _make_snap():
    return {
        "expiry": date(2026, 5, 15), "strike": 22000.0,
        "ce_ltp": 150.0, "pe_ltp": 140.0, "straddle": 290.0,
        "ce_iv": 14.0, "pe_iv": 14.5,
        "timestamp": "2026-05-13 10:00:00", "atm_iv": 14.25,
    }


def test_record_entry_columns_match_values_with_kite_ids():
    ch = MagicMock()
    mgr = MagicMock()
    mgr.check_margin.return_value = 100000.0
    mgr.place_straddle_entry.return_value = ("ce_001", "pe_001")
    monitor.record_entry(ch, "NIFTY", _make_snap(), lot_size=75,
                         scorecard_conf=70.0, dry_run=False, kite_mgr=mgr)
    args = ch.insert.call_args
    values = args[0][1][0]
    cols   = args[1]["column_names"]
    assert len(values) == len(cols), (
        f"Column mismatch: {len(values)} values vs {len(cols)} columns"
    )
    assert "kite_ce_order_id" in cols
    assert "kite_pe_order_id" in cols


def test_record_entry_paper_mode_no_kite_calls():
    ch = MagicMock()
    monitor.record_entry(ch, "NIFTY", _make_snap(), lot_size=75,
                         scorecard_conf=70.0, dry_run=False, kite_mgr=None)
    cols = ch.insert.call_args[1]["column_names"]
    vals = ch.insert.call_args[0][1][0]
    assert "kite_ce_order_id" in cols
    idx = cols.index("kite_ce_order_id")
    assert vals[idx] == ""   # empty in paper mode


def _make_pos():
    return {
        "trade_id": "test-id", "symbol": "NIFTY",
        "expiry": date(2026, 5, 15), "strike": 22000.0,
        "entry_time": "2026-05-13 09:30:00",
        "entry_ce_ltp": 150.0, "entry_pe_ltp": 140.0, "entry_premium": 290.0,
        "lot_size": 75, "target_pts": 26.67, "stop_pts": 13.33,
        "target_inr": 2000.0, "stoploss_inr": 1000.0, "scorecard_conf": 70.0,
        "trailing_active": 0, "peak_pnl_inr": 0.0, "trail_stop_inr": 0.0,
        "entry_features": "{}",
    }


def test_record_exit_columns_match_values_with_kite_ids():
    ch = MagicMock()
    mgr = MagicMock()
    mgr.place_straddle_exit.return_value = ("exit_ce_001", "exit_pe_001")
    monitor.record_exit(ch, _make_pos(), current_straddle=250.0,
                        exit_reason="stop", dry_run=False, kite_mgr=mgr)
    # First insert = trade_outcomes
    first = ch.insert.call_args_list[0]
    values = first[0][1][0]
    cols   = first[1]["column_names"]
    assert len(values) == len(cols)
    assert "kite_ce_order_id" in cols
    assert "kite_pe_order_id" in cols


# ── Migration 066 ──────────────────────────────────────────────────────────────

def test_migration_066_exists_and_covers_both_tables():
    path = os.path.join(os.path.dirname(__file__), "..",
                        "clickhouse", "migrations", "066_add_kite_order_ids.sql")
    assert os.path.exists(path)
    sql = open(path).read()
    assert "open_positions"   in sql
    assert "trade_outcomes"   in sql
    assert "kite_ce_order_id" in sql
    assert "kite_pe_order_id" in sql
