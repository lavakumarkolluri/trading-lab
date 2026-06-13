"""
drift_detector.py
─────────────────
Computes rolling Pearson correlation between each entry feature and trade outcome (win=1/loss=0).
Detects drift by comparing current-window correlation to a 180-day baseline.

When 2+ features drift for the same symbol, emits a retrain trigger to
analysis.retrain_triggers so the scheduler can queue a confidence_scorer --compare run.

Run weekly (Sunday) after all EOD data is settled.
"""
import json
import logging
import math
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)

# Minimum trades in a window to compute a meaningful correlation
MIN_TRADES_FOR_DRIFT = 10
# Drift threshold: |corr_now - baseline| > this → drifted
DRIFT_THRESHOLD = 0.15
# Windows to compute (days)
WINDOWS = [30, 60, 90]
# Baseline window (days)
BASELINE_WINDOW = 180
# Min features that must drift to emit a retrain trigger
MIN_FEATURES_FOR_TRIGGER = 2


def pearson_correlation(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation between xs and ys. Returns 0 on insufficient data or zero variance."""
    n = len(xs)
    if n < 3 or len(ys) != n:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = math.sqrt(var_x * var_y)
    if denom < 1e-12:
        return 0.0
    return max(-1.0, min(1.0, cov / denom))


def is_drifted(baseline_corr: float, current_corr: float) -> bool:
    """Return True if the feature's predictive power has shifted significantly.
    Zero baseline means the feature was never predictive — skip the check.
    """
    if abs(baseline_corr) < 1e-6:
        return False
    return abs(current_corr - baseline_corr) > DRIFT_THRESHOLD


def extract_feature_win_pairs(
    rows: list[tuple],
) -> dict[str, list[tuple[float, int]]]:
    """Parse (entry_features_json, pnl_pts) rows into {feature: [(value, win_label), ...]}."""
    result: dict[str, list[tuple[float, int]]] = {}
    for (feat_json, pnl_pts) in rows:
        try:
            features = json.loads(feat_json)
        except (json.JSONDecodeError, TypeError):
            continue
        win = 1 if float(pnl_pts) >= 0 else 0
        for feat, val in features.items():
            if val is None:
                continue
            try:
                result.setdefault(feat, []).append((float(val), win))
            except (TypeError, ValueError):
                pass
    return result


def _should_trigger_retrain(drifted_features: list[str]) -> bool:
    return len(drifted_features) >= MIN_FEATURES_FOR_TRIGGER


def _fetch_live_pairs(ch, symbol: str, start: date, end: date) -> list[tuple]:
    """Fetch (entry_features_json, pnl_pts) from live paper trades only."""
    return ch.query(
        """
        SELECT entry_features, pnl_pts
        FROM trades.trade_outcomes FINAL
        WHERE symbol = {sym:String}
          AND toDate(exit_time) >= {start:Date}
          AND toDate(exit_time) <= {end:Date}
          AND entry_features != ''
        ORDER BY exit_time
        """,
        parameters={"sym": symbol, "start": start, "end": end},
    ).result_rows


def _fetch_historical_pairs(ch, symbol: str, start: date, end: date) -> list[tuple]:
    """Fetch (features_json, pnl_pts) from seeded historical backtest data."""
    return ch.query(
        """
        SELECT features_json, pnl_pts
        FROM analysis.historical_trade_features FINAL
        WHERE symbol = {sym:String}
          AND entry_date >= {start:Date}
          AND entry_date <= {end:Date}
          AND features_json != '{}'
        ORDER BY entry_date
        """,
        parameters={"sym": symbol, "start": start, "end": end},
    ).result_rows


def run(ch, symbol: str, run_date: date) -> None:
    """Compute feature drift for all windows for a symbol. Write results and any triggers.

    Baseline (180d): historical + live — establishes the reference correlation.
    Current windows (30/60/90d): live trades ONLY — avoids false drift signals
    from historical-data variance. If fewer than MIN_TRADES_FOR_DRIFT live trades
    exist for a window, that window is skipped rather than generating spurious drift.
    """

    # 1. Fetch baseline data: UNION historical + live (rich reference)
    baseline_start = run_date - timedelta(days=BASELINE_WINDOW)
    baseline_rows = (
        _fetch_historical_pairs(ch, symbol, baseline_start, run_date)
        + _fetch_live_pairs(ch, symbol, baseline_start, run_date)
    )
    baseline_pairs = extract_feature_win_pairs(baseline_rows)

    # 2. For each window, compute correlations using LIVE TRADES ONLY.
    # Historical data is not used here — if it were, a 30-day historical slice
    # would naturally differ from the 180-day baseline due to sampling noise
    # rather than genuine concept drift, generating false retrain triggers.
    perf_rows = []   # for analysis.feature_performance
    drifted_features: list[str] = []

    for window in WINDOWS:
        window_start = run_date - timedelta(days=window)
        window_rows = _fetch_live_pairs(ch, symbol, window_start, run_date)

        if not window_rows:
            log.debug("drift_detector: no live trades for %s in %dd window — skipping",
                      symbol, window)
            continue

        window_pairs = extract_feature_win_pairs(window_rows)

        for feat, pairs in window_pairs.items():
            if len(pairs) < MIN_TRADES_FOR_DRIFT:
                continue
            xs = [p[0] for p in pairs]
            ys = [float(p[1]) for p in pairs]
            corr = pearson_correlation(xs, ys)

            # Baseline correlation for this feature
            base_pairs = baseline_pairs.get(feat, [])
            if len(base_pairs) >= MIN_TRADES_FOR_DRIFT:
                bxs = [p[0] for p in base_pairs]
                bys = [float(p[1]) for p in base_pairs]
                baseline_corr = pearson_correlation(bxs, bys)
            else:
                baseline_corr = 0.0

            drifted = is_drifted(baseline_corr, corr)
            if drifted and window == 30 and feat not in drifted_features:
                drifted_features.append(feat)

            perf_rows.append([
                run_date, symbol, feat, window,
                len(pairs), corr, baseline_corr, int(drifted),
            ])

    # 3. Write feature_performance rows
    if perf_rows:
        ch.insert(
            "analysis.feature_performance",
            perf_rows,
            column_names=["computed_at", "symbol", "feature_name", "window_days",
                          "n_trades", "correlation", "baseline_corr", "drift_detected"],
        )
        log.info("drift_detector: wrote %d feature_performance rows for %s on %s",
                 len(perf_rows), symbol, run_date)

    # 4. Emit retrain trigger if enough features drifted
    if _should_trigger_retrain(drifted_features):
        reason = f"drift: {', '.join(drifted_features[:5])} ({len(drifted_features)} features)"
        ch.insert(
            "analysis.retrain_triggers",
            [[run_date, symbol, reason, ", ".join(drifted_features), 0]],
            column_names=["trigger_date", "symbol", "reason", "drifted_features", "acted_on"],
        )
        log.info("drift_detector: retrain trigger emitted for %s — %s", symbol, reason)


if __name__ == "__main__":
    import os
    from ch_utils import ch_client
    from startup_validator import validate_service
    validate_service("drift_detector")
    ch = ch_client()
    for sym in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"):
        run(ch, symbol=sym, run_date=date.today())
