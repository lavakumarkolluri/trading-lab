#!/usr/bin/env python3
"""
kite_orders.py
──────────────
Zerodha Kite Connect order management for live straddle trading.

Wraps kiteconnect to:
  - Load NFO instruments list (instrument token lookup by symbol/expiry/strike/type)
  - Pre-flight margin check via Kite margin API
  - Place SELL orders on entry (short straddle)
  - Place BUY orders on exit (close straddle)

All methods are no-ops when kite=None (paper mode).

Credentials (set before intraday session):
  KITE_API_KEY      — from Kite Connect app
  KITE_ACCESS_TOKEN — obtained via kite_auth.py each morning (expires ~06:00 IST)
"""

import logging
import os

log = logging.getLogger(__name__)

TARGET_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY"}

MARGIN_UTILIZATION_CAP = 0.80  # fraction of available capital to commit
MAX_LOTS = 5                    # hard cap — never exceed regardless of capital


def build_kite_client():
    """
    Return a KiteConnect client with access_token set, or None if env vars missing.
    Import is deferred so the rest of the codebase doesn't require kiteconnect installed.
    """
    api_key      = os.getenv("KITE_API_KEY", "")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "")
    if not api_key or not access_token:
        return None
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        return kite
    except ImportError:
        log.error("kiteconnect package not installed — pip install kiteconnect")
        return None
    except Exception as e:
        log.error("Kite client init failed: %s", e)
        return None


class KiteOrderManager:
    """Manages live orders for one intraday session."""

    def __init__(self, kite):
        self._kite = kite
        # (symbol, expiry_date, strike_float, option_type) → {tradingsymbol, instrument_token, lot_size}
        self._instruments: dict = {}

    def load_instruments(self):
        """Download NFO instruments list and build lookup cache. Call once at session start."""
        if self._kite is None:
            return
        try:
            instruments = self._kite.instruments("NFO")
            count = 0
            for inst in instruments:
                if inst.get("name", "") not in TARGET_SYMBOLS:
                    continue
                if inst.get("instrument_type", "") not in ("CE", "PE"):
                    continue
                key = (
                    inst["name"],
                    inst["expiry"],          # datetime.date from kiteconnect
                    float(inst["strike"]),
                    inst["instrument_type"],
                )
                self._instruments[key] = {
                    "tradingsymbol":    inst["tradingsymbol"],
                    "instrument_token": inst["instrument_token"],
                    "lot_size":         int(inst["lot_size"]),
                }
                count += 1
            log.info("KiteOrderManager: loaded %d NFO instruments", count)
        except Exception as e:
            log.error("load_instruments failed: %s", e)

    def get_instrument(self, symbol: str, expiry, strike: float, option_type: str) -> dict | None:
        """Look up instrument metadata. Returns None if not found."""
        return self._instruments.get((symbol, expiry, strike, option_type))

    def check_margin(self, symbol: str, expiry, strike: float, lot_size: int, lots: int) -> float:
        """
        Call Kite margin API for a short straddle (SELL CE + SELL PE).
        Returns total margin required in ₹, or 0 if check fails.
        """
        if self._kite is None:
            return 0.0
        ce = self.get_instrument(symbol, expiry, strike, "CE")
        pe = self.get_instrument(symbol, expiry, strike, "PE")
        if not ce or not pe:
            log.warning("check_margin: instrument not found for %s %s %.0f", symbol, expiry, strike)
            return 0.0
        try:
            qty = lot_size * lots
            orders = [
                {
                    "exchange":          "NFO",
                    "tradingsymbol":     ce["tradingsymbol"],
                    "transaction_type":  "SELL",
                    "variety":           "regular",
                    "product":           "MIS",
                    "order_type":        "MARKET",
                    "quantity":          qty,
                    "price":             0,
                    "trigger_price":     0,
                },
                {
                    "exchange":          "NFO",
                    "tradingsymbol":     pe["tradingsymbol"],
                    "transaction_type":  "SELL",
                    "variety":           "regular",
                    "product":           "MIS",
                    "order_type":        "MARKET",
                    "quantity":          qty,
                    "price":             0,
                    "trigger_price":     0,
                },
            ]
            result = self._kite.order_margins(orders)
            total = sum(float(r.get("total", 0)) for r in result)
            log.info("Margin check %s %.0f × %d lots: ₹%.0f", symbol, strike, lots, total)
            return total
        except Exception as e:
            log.warning("check_margin failed: %s", e)
            return 0.0

    def place_straddle_entry(
        self, symbol: str, expiry, strike: float, lot_size: int, lots: int
    ) -> tuple[str, str]:
        """
        SELL CE + SELL PE at MARKET to open a short straddle.
        Returns (ce_order_id, pe_order_id). Both empty strings on failure.
        """
        if self._kite is None:
            return "", ""
        ce = self.get_instrument(symbol, expiry, strike, "CE")
        pe = self.get_instrument(symbol, expiry, strike, "PE")
        if not ce or not pe:
            log.error("place_straddle_entry: instrument not found %s %s %.0f", symbol, expiry, strike)
            return "", ""
        qty = lot_size * lots
        ce_order_id = pe_order_id = ""
        try:
            ce_order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange="NFO",
                tradingsymbol=ce["tradingsymbol"],
                transaction_type=self._kite.TRANSACTION_TYPE_SELL,
                quantity=qty,
                product=self._kite.PRODUCT_MIS,
                order_type=self._kite.ORDER_TYPE_MARKET,
            )
            log.info("ENTRY CE SELL order placed: %s  order_id=%s", ce["tradingsymbol"], ce_order_id)
        except Exception as e:
            log.error("place CE SELL failed: %s", e)

        try:
            pe_order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange="NFO",
                tradingsymbol=pe["tradingsymbol"],
                transaction_type=self._kite.TRANSACTION_TYPE_SELL,
                quantity=qty,
                product=self._kite.PRODUCT_MIS,
                order_type=self._kite.ORDER_TYPE_MARKET,
            )
            log.info("ENTRY PE SELL order placed: %s  order_id=%s", pe["tradingsymbol"], pe_order_id)
        except Exception as e:
            log.error("place PE SELL failed: %s", e)

        return str(ce_order_id), str(pe_order_id)

    def place_straddle_exit(
        self, symbol: str, expiry, strike: float, lot_size: int, lots: int
    ) -> tuple[str, str]:
        """
        BUY CE + BUY PE at MARKET to close a short straddle.
        Returns (ce_order_id, pe_order_id). Both empty strings on failure.
        """
        if self._kite is None:
            return "", ""
        ce = self.get_instrument(symbol, expiry, strike, "CE")
        pe = self.get_instrument(symbol, expiry, strike, "PE")
        if not ce or not pe:
            log.error("place_straddle_exit: instrument not found %s %s %.0f", symbol, expiry, strike)
            return "", ""
        qty = lot_size * lots
        ce_order_id = pe_order_id = ""
        try:
            ce_order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange="NFO",
                tradingsymbol=ce["tradingsymbol"],
                transaction_type=self._kite.TRANSACTION_TYPE_BUY,
                quantity=qty,
                product=self._kite.PRODUCT_MIS,
                order_type=self._kite.ORDER_TYPE_MARKET,
            )
            log.info("EXIT CE BUY order placed: %s  order_id=%s", ce["tradingsymbol"], ce_order_id)
        except Exception as e:
            log.error("place CE BUY failed: %s", e)

        try:
            pe_order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange="NFO",
                tradingsymbol=pe["tradingsymbol"],
                transaction_type=self._kite.TRANSACTION_TYPE_BUY,
                quantity=qty,
                product=self._kite.PRODUCT_MIS,
                order_type=self._kite.ORDER_TYPE_MARKET,
            )
            log.info("EXIT PE BUY order placed: %s  order_id=%s", pe["tradingsymbol"], pe_order_id)
        except Exception as e:
            log.error("place PE BUY failed: %s", e)

        return str(ce_order_id), str(pe_order_id)

    def get_available_capital(self) -> float:
        """Return available equity margin (net) in ₹, or 0 on failure."""
        if self._kite is None:
            return 0.0
        try:
            margins = self._kite.margins("equity")
            net = float(margins.get("net", 0))
            log.info("Available capital: ₹%.0f", net)
            return net
        except Exception as e:
            log.warning("get_available_capital failed: %s", e)
            return 0.0

    def compute_lots(self, symbol: str, expiry, strike: float, lot_size: int) -> int:
        """
        Size position using available capital.
        lots = floor(available_capital × MARGIN_UTILIZATION_CAP / margin_per_lot)
        Clamped to [1, MAX_LOTS].
        """
        available = self.get_available_capital()
        if available <= 0:
            return 1
        margin_1lot = self.check_margin(symbol, expiry, strike, lot_size, lots=1)
        if margin_1lot <= 0:
            return 1
        lots = int((available * MARGIN_UTILIZATION_CAP) // margin_1lot)
        lots = max(1, min(lots, MAX_LOTS))
        log.info("compute_lots %s: capital=₹%.0f margin_1lot=₹%.0f → %d lots",
                 symbol, available, margin_1lot, lots)
        return lots

    def get_order_status(self, order_id: str) -> str:
        """Return order status string (COMPLETE, REJECTED, OPEN, etc.) or 'UNKNOWN'."""
        if self._kite is None or not order_id:
            return "UNKNOWN"
        try:
            history = self._kite.order_history(order_id)
            return history[-1]["status"] if history else "UNKNOWN"
        except Exception as e:
            log.warning("get_order_status(%s) failed: %s", order_id, e)
            return "UNKNOWN"
