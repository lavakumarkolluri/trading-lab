"""
Tests for strategy_backtester.py P&L accounting.
All offline — no ClickHouse required.

Critical: if pnl_pts / net_credit / max_loss formulas are wrong, all
model training data has corrupted targets → wrong model → bad live trades.
"""
import sys
import os
from datetime import date
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
import strategy_backtester as bt


def _make_idx(prices: dict):
    """Build a price index matching get_ltp()'s lookup: idx[(snap_date, expiry, strike, otype)]."""
    return dict(prices)  # already the right shape: {(date, expiry, strike, otype): ltp}


# ── get_ltp helper ─────────────────────────────────────────────────────────────

def test_get_ltp_returns_price_from_index():
    entry = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    idx = _make_idx({(entry, expiry, 24000.0, "CE"): 120.0})
    assert bt.get_ltp(idx, entry, expiry, 24000.0, "CE") == 120.0


def test_get_ltp_returns_none_for_missing_strike():
    entry = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    idx = _make_idx({(entry, expiry, 24000.0, "CE"): 120.0})
    assert bt.get_ltp(idx, entry, expiry, 24100.0, "CE") is None


# ── compute_iron_condor P&L formula ───────────────────────────────────────────

_SCE_ENTRY = 80.0   # short CE entry ltp
_SPE_ENTRY = 70.0   # short PE entry ltp
_LCE_ENTRY = 20.0   # long  CE entry ltp (wing)
_LPE_ENTRY = 15.0   # long  PE entry ltp (wing)
_SCE_SETTLE = 0.5   # short CE settlement (OTM, near-zero)
_SPE_SETTLE = 0.5   # short PE settlement
_LCE_SETTLE = 0.05
_LPE_SETTLE = 0.05

_IC_NET_CREDIT = (_SCE_ENTRY + _SPE_ENTRY) - (_LCE_ENTRY + _LPE_ENTRY)  # 115


def _ic_prices(entry, expiry, atm=24000, step=100, short_n=1, wing_m=2):
    """Convenience: return price dict for an IC with known values."""
    sce_k = atm + short_n * step
    spe_k = atm - short_n * step
    lce_k = atm + (short_n + wing_m) * step
    lpe_k = atm - (short_n + wing_m) * step
    return {
        (entry, expiry, sce_k, "CE"): _SCE_ENTRY,
        (entry, expiry, spe_k, "PE"): _SPE_ENTRY,
        (entry, expiry, lce_k, "CE"): _LCE_ENTRY,
        (entry, expiry, lpe_k, "PE"): _LPE_ENTRY,
        # Settlement (both legs expire OTM — keep all premium)
        (expiry, expiry, sce_k, "CE"): _SCE_SETTLE,
        (expiry, expiry, spe_k, "PE"): _SPE_SETTLE,
        (expiry, expiry, lce_k, "CE"): _LCE_SETTLE,
        (expiry, expiry, lpe_k, "PE"): _LPE_SETTLE,
    }


def test_iron_condor_net_credit():
    """net_credit = (sell_CE + sell_PE) - (buy_CE + buy_PE)."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    idx = _make_idx(_ic_prices(entry, expiry))
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r is not None
    assert abs(r["net_credit"] - _IC_NET_CREDIT) < 0.01


def test_iron_condor_max_loss():
    """max_loss = wing_m * step - net_credit."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    wing_m, step = 2, 100
    idx = _make_idx(_ic_prices(entry, expiry, step=step, wing_m=wing_m))
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, step, short_n=1, wing_m=wing_m)
    expected_max_loss = wing_m * step - _IC_NET_CREDIT
    assert abs(r["max_loss"] - expected_max_loss) < 0.01


def test_iron_condor_pnl_winning_trade():
    """Both sides expire OTM → exit_cost ≈ 1.1 → pnl ≈ 115 - 1.1 = 113.9 > 0 → target=1."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    idx = _make_idx(_ic_prices(entry, expiry))
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r["pnl_pts"] > 0
    assert r["target"] == 1


def test_iron_condor_pnl_losing_trade():
    """CE side breached deeply → exit_cost > net_credit → pnl < 0 → target=0."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    prices = _ic_prices(entry, expiry)
    # Override CE settlement: short CE deep ITM, long CE also ITM
    prices[(expiry, expiry, 24100.0, "CE")] = 500.0   # short_ce settles deep ITM
    prices[(expiry, expiry, 24300.0, "CE")] = 300.0   # long_ce partial offset
    idx = _make_idx(prices)
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r is not None
    assert r["pnl_pts"] < 0
    assert r["target"] == 0


def test_iron_condor_pnl_capped_at_max_loss():
    """pnl_pts is capped at -max_loss even when exit_cost is enormous."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    prices = _ic_prices(entry, expiry)
    # Short CE settles at 1000, long CE at 0 → exit_cost = 1000 >> net_credit
    prices[(expiry, expiry, 24100.0, "CE")] = 1000.0
    prices[(expiry, expiry, 24300.0, "CE")] = 0.1
    idx = _make_idx(prices)
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r is not None
    assert r["pnl_pts"] >= -r["max_loss"] - 0.01, "P&L should be capped at max_loss"


def test_iron_condor_returns_none_when_debit_structure():
    """net_credit ≤ 0 (debit spread) → return None, don't record trade."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    # Wings cost more than short legs → debit
    prices = {
        (entry, expiry, 24100.0, "CE"): 10.0,   # short CE cheap
        (entry, expiry, 23900.0, "PE"): 10.0,   # short PE cheap
        (entry, expiry, 24300.0, "CE"): 50.0,   # wing CE expensive
        (entry, expiry, 23700.0, "PE"): 50.0,   # wing PE expensive
    }
    idx = _make_idx(prices)
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r is None


def test_iron_condor_returns_none_on_missing_price():
    """Missing price for any leg → return None (don't use incomplete data)."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    # Only 3 of 4 legs present
    prices = {
        (entry, expiry, 24100.0, "CE"): 80.0,
        (entry, expiry, 23900.0, "PE"): 70.0,
        (entry, expiry, 24300.0, "CE"): 20.0,
        # lpe missing
    }
    idx = _make_idx(prices)
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r is None


def test_iron_condor_target_is_int():
    """target column must be int (0 or 1), not bool — ClickHouse UInt8 requires this."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    idx = _make_idx(_ic_prices(entry, expiry))
    r = bt.compute_iron_condor(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert isinstance(r["target"], int)
    assert r["target"] in (0, 1)


# ── compute_bull_put P&L formula ──────────────────────────────────────────────

def test_bull_put_pnl_winning():
    """Bull put: sell PE at 80, buy PE at 30. Settle OTM → keep premium."""
    entry  = date(2025, 5, 1)
    expiry = date(2025, 5, 8)
    prices = {
        (entry, expiry, 23900.0, "PE"): 80.0,
        (entry, expiry, 23700.0, "PE"): 30.0,
        (expiry, expiry, 23900.0, "PE"): 0.5,
        (expiry, expiry, 23700.0, "PE"): 0.05,
    }
    idx = _make_idx(prices)
    r = bt.compute_bull_put(idx, entry, expiry, 24000, 100, short_n=1, wing_m=2)
    assert r is not None
    assert r["net_credit"] == pytest.approx(50.0)   # 80 - 30
    assert r["pnl_pts"] > 0
    assert r["target"] == 1


# ── compute_iron_fly P&L formula ──────────────────────────────────────────────

def _if_prices(entry, expiry, atm=24000, step=50, wing_m=4,
               sce=120.0, spe=110.0, lce=30.0, lpe=25.0,
               xsce=0.5, xspe=0.5, xlce=0.05, xlpe=0.05):
    """Return price dict for an iron fly with known values."""
    return {
        (entry, expiry, float(atm), "CE"): sce,
        (entry, expiry, float(atm), "PE"): spe,
        (entry, expiry, float(atm + wing_m * step), "CE"): lce,
        (entry, expiry, float(atm - wing_m * step), "PE"): lpe,
        (expiry, expiry, float(atm), "CE"): xsce,
        (expiry, expiry, float(atm), "PE"): xspe,
        (expiry, expiry, float(atm + wing_m * step), "CE"): xlce,
        (expiry, expiry, float(atm - wing_m * step), "PE"): xlpe,
    }


def test_iron_fly_net_credit():
    """net_credit = (sell_ATM_CE + sell_ATM_PE) - (buy_wing_CE + buy_wing_PE)."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    idx = _make_idx(_if_prices(entry, expiry, sce=120.0, spe=110.0, lce=30.0, lpe=25.0))
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is not None
    assert r["net_credit"] == pytest.approx(175.0)   # (120+110) - (30+25)
    assert r["strategy"] == "iron_fly"
    assert r["short_n"] == 0


def test_iron_fly_pnl_winning_trade():
    """Both ATM options settle near-zero → keep net_credit → pnl > 0, target=1."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    idx = _make_idx(_if_prices(entry, expiry))
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is not None
    assert r["pnl_pts"] > 0
    assert r["target"] == 1


def test_iron_fly_pnl_with_loss():
    """Market moves far: ATM CE settles deep ITM → exit_cost > net_credit → loss."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    # net_credit = (120+110)-(30+25) = 175; wing = 4*50 = 200; max_loss = 25
    # Settlement: ATM CE=350 (deep ITM), ATM PE≈0, wing CE=150, wing PE≈0
    # exit_cost = (350+0.5)-(150+0.05) = 200.45 → pnl = 175-200.45 = -25.45 → capped at -25
    prices = _if_prices(entry, expiry, xsce=350.0, xspe=0.5, xlce=150.0, xlpe=0.05)
    idx = _make_idx(prices)
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is not None
    assert r["pnl_pts"] < 0
    assert r["target"] == 0
    assert r["pnl_pts"] >= -r["max_loss"] - 0.01   # capped at max_loss


def test_iron_fly_returns_none_when_debit():
    """Wings cost more than short straddle → net_credit ≤ 0 → skip."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    prices = _if_prices(entry, expiry, sce=10.0, spe=10.0, lce=50.0, lpe=50.0)
    idx = _make_idx(prices)
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is None


def test_iron_fly_missing_settlement_treated_as_zero():
    """Missing settlement price = option expired OTM (value 0), not a skip."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    prices = _if_prices(entry, expiry)
    del prices[(expiry, expiry, 24200.0, "CE")]   # wing CE expired OTM, absent in DB
    idx = _make_idx(prices)
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is not None
    assert r["long_ce_settle"] == pytest.approx(0.0)  # treated as worthless


def test_iron_fly_returns_none_on_missing_entry_leg():
    """Missing entry-day price (not settlement) → return None."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    prices = _if_prices(entry, expiry)
    del prices[(entry, expiry, 24200.0, "CE")]   # missing entry price → can't size trade
    idx = _make_idx(prices)
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is None


def test_iron_fly_wing_m_from_symbol():
    """NIFTY uses wing_m=4 (4×50=200pt wings); result reflects that."""
    entry  = date(2025, 5, 6)
    expiry = date(2025, 5, 13)
    idx = _make_idx(_if_prices(entry, expiry))
    r = bt.compute_iron_fly(idx, entry, expiry, 24000, 50, "NIFTY")
    assert r is not None
    assert r["wing_m"] == 4
    assert r["long_ce_strike"] == pytest.approx(24200.0)
    assert r["long_pe_strike"] == pytest.approx(23800.0)
