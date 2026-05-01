"""
fo_utils.py
───────────
Shared F&O lot size lookup backed by market.fo_lot_sizes in ClickHouse.

Usage:
    ch    = get_ch_client()
    cache = build_lot_size_cache(ch)
    lot   = get_lot_size("NIFTY", date(2024, 6, 1), cache)  # → 75
    lot   = get_lot_size("NIFTY", date(2025, 1, 1), cache)  # → 25
"""

from datetime import date

# Fallback when ClickHouse has no data (e.g. first run before lot_size_pipeline)
# Derived from NSE bhavcopy NewBrdLotQty; covers dates before Apr 2024 (before bhavcopy history begins)
# NIFTY: 25 → 75 from Jan 2025 (SEBI ₹15L contract value mandate Nov 2024)
_FALLBACK: dict[str, list[tuple[date, int]]] = {
    "NIFTY":      [(date(2000, 1, 1), 75)],   # pre-Apr 2024 (bhavcopy covers Apr 2024+)
    "BANKNIFTY":  [(date(2000, 1, 1), 15)],
    "FINNIFTY":   [(date(2000, 1, 1), 40)],
    "MIDCPNIFTY": [(date(2000, 1, 1), 75)],
    "SENSEX":     [(date(2000, 1, 1), 10)],
    "BANKEX":     [(date(2000, 1, 1), 15)],
}


def build_lot_size_cache(ch) -> dict[str, list[tuple[date, int]]]:
    """
    Returns {symbol: [(effective_from, lot_size), ...]} sorted by effective_from asc.
    Falls back to _FALLBACK if the table is empty.
    """
    try:
        rows = ch.query(
            "SELECT symbol, effective_from, lot_size "
            "FROM market.fo_lot_sizes FINAL "
            "ORDER BY symbol, effective_from"
        ).result_rows
    except Exception:
        rows = []

    if not rows:
        return {sym: list(hist) for sym, hist in _FALLBACK.items()}

    cache: dict[str, list[tuple[date, int]]] = {}
    for sym, eff, ls in rows:
        cache.setdefault(sym, []).append((eff, int(ls)))
    return cache


def get_lot_size(symbol: str, trade_date: date, cache: dict[str, list[tuple[date, int]]],
                 default: int = 1) -> int:
    """Return the lot size effective on trade_date for symbol."""
    history = cache.get(symbol)
    if not history:
        # Try fallback
        history = _FALLBACK.get(symbol)
    if not history:
        return default
    lot = default
    for eff_from, ls in history:
        if eff_from <= trade_date:
            lot = ls
    return lot
