"""
loss_attributor.py
──────────────────
For every closed losing trade, computes per-feature attribution scores:
how far each entry feature deviated from the winning-trade profile.

Positive attribution = feature value was above the winning mean.
Negative attribution = feature value was below the winning mean.
Magnitude = number of winning-trade standard deviations of deviation.

The sign's meaning depends on the feature:
  iv_rank high → good for short premium → high attribution on a loss = unusual high risk
  vix > 22 on entry → high attribution = likely caused stop

Run daily after trades settle (after 17:30 IST). Only attributes trades
from the current run_date that haven't been attributed yet.
"""
import json
import logging
import math
from datetime import date
from typing import Any

log = logging.getLogger(__name__)

# Minimum winning trades needed to build a reliable win profile
MIN_WIN_TRADES = 5


def compute_attribution(feature_val: float, win_mean: float, win_std: float) -> float:
    """Return (feature_val - win_mean) / win_std.
    Returns 0 if std is 0 (all winners had same value — deviation undefined).
    Caps at ±10 when std=0 but feature deviated (prevents inf).
    """
    if win_std < 1e-9:
        if abs(feature_val - win_mean) < 1e-9:
            return 0.0
        return max(-10.0, min(10.0, (feature_val - win_mean) * 10.0))
    return (feature_val - win_mean) / win_std


def build_win_profile(
    winning_features: list[dict[str, Any]],
) -> dict[str, tuple[float, float]]:
    """Return {feature_name: (mean, std)} computed from winning trade feature dicts.
    Skips None values. Returns {} if no winners.
    """
    if not winning_features:
        return {}
    accum: dict[str, list[float]] = {}
    for feat_dict in winning_features:
        for k, v in feat_dict.items():
            if v is not None:
                try:
                    accum.setdefault(k, []).append(float(v))
                except (TypeError, ValueError):
                    pass
    profile: dict[str, tuple[float, float]] = {}
    for feat, vals in accum.items():
        if not vals:
            continue
        mean = sum(vals) / len(vals)
        variance = sum((x - mean) ** 2 for x in vals) / len(vals)
        std = math.sqrt(variance)
        profile[feat] = (mean, std)
    return profile


def attribute_trade(
    trade_id: str,
    symbol: str,
    exit_date: date,
    exit_reason: str,
    pnl_pts: float,
    features: dict[str, Any],
    win_profile: dict[str, tuple[float, float]],
) -> list[dict[str, Any]]:
    """Return one attribution row per feature that exists in win_profile."""
    rows = []
    for feat_name, (win_mean, win_std) in win_profile.items():
        raw = features.get(feat_name)
        if raw is None:
            continue
        try:
            feat_val = float(raw)
        except (TypeError, ValueError):
            continue
        attr = compute_attribution(feat_val, win_mean, win_std)
        rows.append({
            "trade_id":      trade_id,
            "symbol":        symbol,
            "exit_date":     exit_date,
            "exit_reason":   exit_reason,
            "pnl_pts":       pnl_pts,
            "feature_name":  feat_name,
            "feature_value": feat_val,
            "win_mean":      win_mean,
            "attribution":   attr,
        })
    return rows


def run(ch, run_date: date) -> None:
    """Attribute all losing trades on run_date. Idempotent — safe to re-run."""

    # 1. Fetch losing trades for run_date
    losses = ch.query(
        """
        SELECT trade_id, symbol, toDate(exit_time) AS exit_date,
               exit_reason, pnl_pts, entry_features
        FROM trades.trade_outcomes FINAL
        WHERE toDate(exit_time) = {d:Date}
          AND pnl_pts < 0
        """,
        parameters={"d": run_date},
    ).result_rows
    if not losses:
        log.info("loss_attributor: no losses on %s — nothing to attribute", run_date)
        return

    # 2. Build win profile from all historical winning trades
    win_rows = ch.query(
        """
        SELECT entry_features
        FROM trades.trade_outcomes FINAL
        WHERE pnl_pts >= 0
          AND entry_features != ''
          AND entry_features != '{}'
        LIMIT 2000
        """,
        parameters={},
    ).result_rows
    if len(win_rows) < MIN_WIN_TRADES:
        log.warning(
            "loss_attributor: only %d winning trades — skipping (need >= %d)",
            len(win_rows), MIN_WIN_TRADES,
        )
        return

    winning_features = []
    for (feat_json,) in win_rows:
        try:
            winning_features.append(json.loads(feat_json))
        except (json.JSONDecodeError, TypeError):
            pass

    win_profile = build_win_profile(winning_features)
    if not win_profile:
        log.warning("loss_attributor: could not build win profile — skipping")
        return

    # 3. Attribute each losing trade
    all_rows = []
    for (trade_id, symbol, exit_date, exit_reason, pnl_pts, feat_json) in losses:
        try:
            features = json.loads(feat_json) if feat_json else {}
        except (json.JSONDecodeError, TypeError):
            features = {}
        if not features:
            log.debug("loss_attributor: trade %s has no entry_features — skipping", trade_id)
            continue
        rows = attribute_trade(
            trade_id=str(trade_id),
            symbol=symbol,
            exit_date=exit_date,
            exit_reason=exit_reason,
            pnl_pts=float(pnl_pts),
            features=features,
            win_profile=win_profile,
        )
        all_rows.extend(rows)

    if not all_rows:
        log.info("loss_attributor: no attributable rows produced for %s", run_date)
        return

    # 4. Insert to analysis.trade_attributions
    col_names = ["trade_id", "symbol", "exit_date", "exit_reason",
                 "pnl_pts", "feature_name", "feature_value", "win_mean", "attribution"]
    values = [[r[c] for c in col_names] for r in all_rows]
    ch.insert("analysis.trade_attributions", values, column_names=col_names)
    log.info("loss_attributor: attributed %d feature rows for %d losses on %s",
             len(all_rows), len(losses), run_date)


if __name__ == "__main__":
    from datetime import date as _date
    import os
    from ch_utils import ch_client
    from startup_validator import validate_service
    validate_service("loss_attributor")
    ch = ch_client()
    run(ch, run_date=_date.today())
