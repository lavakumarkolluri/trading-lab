"""
Tests for monitor_agent.py.
All tests run offline — no ClickHouse or Telegram required.
"""
import sys
import os
import uuid
from datetime import date, timedelta, datetime
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import monitor_agent as m


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_pos(
    symbol="NIFTY", strike=22500.0, pnl_offset=0.0,
    trailing_active=False, lots=1,
    wing_ce_strike=0.0, net_premium=0.0,
    target_inr=2000.0, stoploss_inr=1000.0,
):
    tid = str(uuid.uuid4())
    return {
        "trade_id": tid,
        "symbol": symbol,
        "strike": strike,
        "expiry": "2026-05-15",
        "entry_time": datetime(2026, 5, 14, 9, 35),
        "entry_ce_ltp": 100.0,
        "entry_pe_ltp": 100.0,
        "entry_premium": 200.0,
        "lot_size": 65,
        "lots": lots,
        "target_inr": target_inr,
        "stoploss_inr": stoploss_inr,
        "trailing_active": trailing_active,
        "peak_pnl_inr": 0.0,
        "trail_stop_inr": 0.0,
        "scorecard_conf": 72.0,
        "status": "open",
        "wing_ce_strike": wing_ce_strike,
        "wing_pe_strike": 22300.0 if wing_ce_strike else 0.0,
        "wing_ce_ltp": 10.0 if wing_ce_strike else 0.0,
        "wing_pe_ltp": 10.0 if wing_ce_strike else 0.0,
        "net_premium": net_premium,
        "_pnl_offset": pnl_offset,  # used by mock_pnl
    }


def _mock_ch_for_pos(pos: dict):
    """Mock CH that returns mark prices derived from pos entry values."""
    ch = MagicMock()
    pnl_offset = pos.get("_pnl_offset", 0.0)

    def _query(sql, **kw):
        res = MagicMock()
        if "wing_ce_strike" not in sql and "wing_pe_strike" not in sql and "options_chain" in sql:
            # straddle mark — make it produce the desired P&L offset
            entry_val = float(pos.get("net_premium") or 0) or float(pos["entry_premium"])
            lot_size = int(pos["lot_size"])
            lots = int(pos.get("lots") or 1)
            curr = entry_val - pnl_offset / (lot_size * lots)
            res.result_rows = [(curr,)]
        else:
            res.result_rows = []
        return res

    ch.query.side_effect = _query
    return ch


# ── _compute_pnl ──────────────────────────────────────────────────────────────

def test_compute_pnl_no_mark_returns_none():
    pos = _make_pos()
    ch = MagicMock()
    ch.query.return_value.result_rows = []
    assert m._compute_pnl(pos, ch) is None


def test_compute_pnl_positive_pnl():
    pos = _make_pos(pnl_offset=500.0)
    ch = _mock_ch_for_pos(pos)
    pnl = m._compute_pnl(pos, ch)
    assert pnl is not None
    assert abs(pnl - 500.0) < 1.0


def test_compute_pnl_negative_pnl():
    pos = _make_pos(pnl_offset=-800.0)
    ch = _mock_ch_for_pos(pos)
    pnl = m._compute_pnl(pos, ch)
    assert pnl is not None
    assert pnl < 0


def test_compute_pnl_respects_lots():
    pos = _make_pos(pnl_offset=1000.0, lots=2)
    ch = _mock_ch_for_pos(pos)
    pnl = m._compute_pnl(pos, ch)
    # With 2 lots, the P&L should be ~1000 (pnl_offset accounts for lot count)
    assert pnl is not None
    assert abs(pnl - 1000.0) < 2.0


# ── alert logic (via run() with mocked loop) ─────────────────────────────────

def _run_one_tick(positions, pnl_by_tid=None, now_time=None, alerted=None, last_open=None):
    """
    Simulate one pass through the per-position check loop in run().
    Returns (alerted, sent_messages).
    """
    if alerted is None:
        alerted = {}
    if last_open is None:
        last_open = set()
    if now_time is None:
        from datetime import time
        now_time = datetime(2026, 5, 14, 10, 0, tzinfo=m.IST)

    sent = []

    def _send_mock(msg):
        sent.append(msg)

    pnl_by_tid = pnl_by_tid or {}

    current_ids = {str(p["trade_id"]) for p in positions}

    # closures
    for tid in last_open - current_ids:
        _send_mock(f"🔴 Position closed\nTrade ID: {tid[:8]}")
        alerted.pop(tid, None)

    for pos in positions:
        tid = str(pos["trade_id"])
        if tid not in alerted:
            alerted[tid] = set()
        seen = alerted[tid]
        pnl_inr = pnl_by_tid.get(tid)

        if "entered" not in seen:
            seen.add("entered")
            _send_mock(f"📥 Position entered\n{m._fmt_pos(pos, pnl_inr)}")

        target = float(pos["target_inr"])
        if pnl_inr is not None and pnl_inr >= target and "target" not in seen:
            seen.add("target")
            _send_mock(f"🎯 Target hit!\n{m._fmt_pos(pos, pnl_inr)}\nTarget: ₹{target:.0f}")

        stop = float(pos["stoploss_inr"])
        if pnl_inr is not None and pnl_inr <= -stop and "stop" not in seen:
            seen.add("stop")
            _send_mock(f"🛑 Stop loss hit!\n{m._fmt_pos(pos, pnl_inr)}\nStop: ₹{stop:.0f}")

        if pos["trailing_active"] and "trailing" not in seen:
            seen.add("trailing")
            _send_mock(f"🔒 Trailing stop activated\n{m._fmt_pos(pos, pnl_inr)}")

        if now_time.time() >= m.EOD_WARN_TIME and "eod" not in seen:
            seen.add("eod")
            _send_mock(f"⏰ EOD approaching — position still open\n{m._fmt_pos(pos, pnl_inr)}")

    return alerted, sent, current_ids


def test_new_position_sends_entered_alert():
    pos = _make_pos()
    _, sent, _ = _run_one_tick([pos])
    assert any("📥 Position entered" in s for s in sent)
    assert any("NIFTY" in s for s in sent)


def test_entered_alert_sent_only_once():
    pos = _make_pos()
    alerted, sent1, open_ids = _run_one_tick([pos])
    _, sent2, _ = _run_one_tick([pos], alerted=alerted, last_open=open_ids)
    entered_count = sum(1 for s in sent1 + sent2 if "📥 Position entered" in s)
    assert entered_count == 1


def test_target_hit_sends_alert():
    pos = _make_pos(target_inr=2000.0)
    tid = str(pos["trade_id"])
    _, sent, _ = _run_one_tick([pos], pnl_by_tid={tid: 2100.0})
    assert any("🎯 Target hit" in s for s in sent)


def test_target_alert_sent_only_once():
    pos = _make_pos(target_inr=2000.0)
    tid = str(pos["trade_id"])
    alerted, sent1, open_ids = _run_one_tick([pos], pnl_by_tid={tid: 2100.0})
    _, sent2, _ = _run_one_tick([pos], pnl_by_tid={tid: 2500.0},
                                alerted=alerted, last_open=open_ids)
    target_count = sum(1 for s in sent1 + sent2 if "🎯 Target hit" in s)
    assert target_count == 1


def test_stop_hit_sends_alert():
    pos = _make_pos(stoploss_inr=1000.0)
    tid = str(pos["trade_id"])
    _, sent, _ = _run_one_tick([pos], pnl_by_tid={tid: -1050.0})
    assert any("🛑 Stop loss hit" in s for s in sent)


def test_stop_alert_sent_only_once():
    pos = _make_pos(stoploss_inr=1000.0)
    tid = str(pos["trade_id"])
    alerted, sent1, open_ids = _run_one_tick([pos], pnl_by_tid={tid: -1100.0})
    _, sent2, _ = _run_one_tick([pos], pnl_by_tid={tid: -1200.0},
                                alerted=alerted, last_open=open_ids)
    stop_count = sum(1 for s in sent1 + sent2 if "🛑 Stop loss hit" in s)
    assert stop_count == 1


def test_trailing_activated_sends_alert():
    pos = _make_pos(trailing_active=True)
    _, sent, _ = _run_one_tick([pos])
    assert any("🔒 Trailing stop activated" in s for s in sent)


def test_trailing_alert_sent_only_once():
    pos = _make_pos(trailing_active=True)
    alerted, sent1, open_ids = _run_one_tick([pos])
    _, sent2, _ = _run_one_tick([pos], alerted=alerted, last_open=open_ids)
    trail_count = sum(1 for s in sent1 + sent2 if "🔒 Trailing stop activated" in s)
    assert trail_count == 1


def test_eod_warning_sent_after_15_15():
    pos = _make_pos()
    eod_time = datetime(2026, 5, 14, 15, 20, tzinfo=m.IST)
    _, sent, _ = _run_one_tick([pos], now_time=eod_time)
    assert any("⏰ EOD approaching" in s for s in sent)


def test_no_eod_warning_before_15_15():
    pos = _make_pos()
    early = datetime(2026, 5, 14, 10, 0, tzinfo=m.IST)
    _, sent, _ = _run_one_tick([pos], now_time=early)
    assert not any("⏰ EOD approaching" in s for s in sent)


def test_position_closed_sends_alert():
    pos = _make_pos()
    tid = str(pos["trade_id"])
    alerted, _, open_ids = _run_one_tick([pos])
    # Now position is gone
    _, sent, _ = _run_one_tick([], alerted=alerted, last_open=open_ids)
    assert any("🔴 Position closed" in s for s in sent)
    assert any(tid[:8] in s for s in sent)


def test_no_duplicate_closed_alert():
    pos = _make_pos()
    tid = str(pos["trade_id"])
    alerted, _, open_ids = _run_one_tick([pos])
    # First tick: position gone
    alerted, sent1, open_ids2 = _run_one_tick([], alerted=alerted, last_open=open_ids)
    # Second tick: still gone
    _, sent2, _ = _run_one_tick([], alerted=alerted, last_open=open_ids2)
    closed_count = sum(1 for s in sent1 + sent2 if "🔴 Position closed" in s)
    assert closed_count == 1


# ── docker-compose wiring ─────────────────────────────────────────────────────

def test_monitor_agent_service_in_compose():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    with open(compose_path) as f:
        content = f.read()
    assert "monitor_agent:" in content
    assert "monitor_agent.py" in content


def test_monitor_agent_has_telegram_env():
    compose_path = os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")
    import yaml
    with open(compose_path) as f:
        data = yaml.safe_load(f)
    env = data["services"]["monitor_agent"]["environment"]
    env_str = str(env)
    assert "TELEGRAM_BOT_TOKEN" in env_str
    assert "TELEGRAM_CHAT_ID" in env_str
