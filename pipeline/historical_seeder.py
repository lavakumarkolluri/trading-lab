"""
historical_seeder.py
────────────────────
Seeds analysis.historical_trade_features with (features, target) pairs
reconstructed from historical backtest data.

Reuses confidence_scorer.build_dataset() — no feature-join logic is duplicated.
This table feeds loss_attributor (win profile + historical losses) and
drift_detector (180-day baseline correlations).

Safety guarantee: this table NEVER flows back into confidence_scorer training.
The scorer reads spread_backtest directly; historical_trade_features is
read-only from the model's perspective.

Run once as backfill, then weekly (Sunday 08:30 UTC) to pick up new backtests.
"""
import json
import logging
import math
from datetime import date

from confidence_scorer import build_dataset, FEATURE_COLS

log = logging.getLogger(__name__)

SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

_COL_NAMES = [
    "symbol", "entry_date", "expiry", "strategy",
    "pnl_pts", "target", "features_json",
]


def _serialize_features(row) -> str:
    """Serialize FEATURE_COLS present in row to JSON. NaN values are omitted."""
    feat = {}
    for col in FEATURE_COLS:
        if col not in row.index:
            continue
        val = row[col]
        try:
            f = float(val)
            if not math.isnan(f):
                feat[col] = f
        except (TypeError, ValueError):
            pass
    return json.dumps(feat)


def run(ch, symbols: list[str] | None = None) -> None:
    """Seed historical_trade_features for all symbols. Idempotent via ReplacingMergeTree."""
    targets = symbols or SYMBOLS

    for sym in targets:
        df = build_dataset(ch, symbol=sym)
        if df.empty:
            log.warning("historical_seeder: build_dataset returned empty for %s — skipping", sym)
            continue

        rows = []
        for _, row in df.iterrows():
            feat_json = _serialize_features(row)
            parsed = json.loads(feat_json)
            if not parsed:
                log.debug("historical_seeder: row for %s on %s has no valid features — skipping",
                          sym, row.get("entry_date"))
                continue

            entry_date = row["entry_date"]
            expiry = row["expiry"]
            strategy = str(row.get("strategy_type", "iron_fly"))
            pnl_pts = float(row["pnl_pts"])
            target = int(row["target"])

            rows.append([sym, entry_date, expiry, strategy, pnl_pts, target, feat_json])

        if not rows:
            log.warning("historical_seeder: no valid rows for %s", sym)
            continue

        ch.insert(
            "analysis.historical_trade_features",
            rows,
            column_names=_COL_NAMES,
        )
        log.info("historical_seeder: inserted %d rows for %s", len(rows), sym)


if __name__ == "__main__":
    from ch_utils import ch_client
    from startup_validator import validate_service
    validate_service("historical_seeder")
    ch = ch_client()
    run(ch)
