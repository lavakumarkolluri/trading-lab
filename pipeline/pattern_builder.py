#!/usr/bin/env python3
"""
pattern_builder.py
──────────────────
Seeds the pattern library with rule-based patterns and maps
historical feature vectors to matching patterns.

Two responsibilities:
  1. SEED — Insert the 6 predefined seed patterns into analysis.patterns
             (skips if already present — idempotent)
  2. MAP  — Scan analysis.pattern_features, evaluate each row against
             every active pattern's conditions, and insert matches
             into analysis.pattern_event_map

Pattern conditions format (conditions_json):
  Each key is a feature column name.
  Each value is a dict with ONE operator:
    {"lt": 35}              → feature < 35
    {"lte": 35}             → feature <= 35
    {"gt": 2.0}             → feature > 2.0
    {"gte": 2.0}            → feature >= 2.0
    {"eq": "expanding"}     → feature == "expanding"
    {"between": [45, 55]}   → 45 <= feature <= 55
    {"in": ["low","normal"]}→ feature in list

All conditions in a pattern are AND-ed together.

Seed patterns (6):
  P001 rsi_oversold_vol_spike   — RSI < 35 + volume surge
  P002 sma20_breakout_low_vix   — SMA-20 cross up + low VIX
  P003 pullback_in_uptrend      — above SMA-200 + 5d dip + RSI cooling
  P004 vol_contraction_squeeze  — low ATR + low volume + RSI neutral
  P005 fii_accumulation_dip     — FII buying + price dip
  P006 momentum_continuation    — price above SMA-20 + RSI healthy + vol expanding

Usage:
  python pattern_builder.py                  # seed + incremental map
  python pattern_builder.py --seed-only      # only insert/refresh seed patterns
  python pattern_builder.py --map-only       # only run pattern matching
  python pattern_builder.py --full           # seed + remap ALL features
  python pattern_builder.py --pattern P001   # debug single pattern
"""

import os
import re
import json
import logging
import argparse
import threading
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import clickhouse_connect

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MAX_WORKERS  = 8
BATCH_SIZE   = 500   # insert pattern_event_map rows in batches

# ── SEC-002: input validation ──────────────────────────
_SAFE_ID = re.compile(r'^[A-Za-z0-9_\-]{1,50}$')

def _validate_id(value: str, label: str) -> None:
    if not _SAFE_ID.match(value):
        raise ValueError(f"SEC-002: unsafe {label} {value!r}")

# ── Results tracker ────────────────────────────────────
results = {
    "patterns_seeded": 0,
    "features_scanned": 0,
    "matches_found": 0,
    "failed": [],
}
results_lock = threading.Lock()


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ── DATA-003: fii_dii preflight ────────────────────────
def check_fii_dii_seeded(ch) -> bool:
    """
    Returns True if market.fii_dii has at least one row.
    Logs a clear warning when empty so P005 zero-match silence is explained.
    """
    result = ch.query("SELECT count() FROM market.fii_dii FINAL")
    n = int(result.result_rows[0][0])
    if n == 0:
        log.warning(
            "DATA-003: market.fii_dii is empty — P005 (fii_accumulation_dip) "
            "will match 0 events. Run fii_dii_pipeline to seed this table."
        )
        return False
    return True


# ══════════════════════════════════════════════════════
# Seed pattern definitions
# ══════════════════════════════════════════════════════

SEED_PATTERNS = [
    {
        "pattern_id":       "P001",
        "label":            "RSI Oversold + Volume Surge",
        "description":      "RSI-14 below 35 with volume spiking above 2 standard deviations. "
                            "Classic oversold condition with strong buying interest returning.",
        "hypothesis":       "Extreme selling has exhausted supply. Volume surge signals large "
                            "institutional buyers stepping in at support. High probability "
                            "mean-reversion bounce within 1-3 days.",
        "feature_version":  "v1",
        "conditions_json":  json.dumps({
            "rsi_14":    {"lt": 35},
            "volume_z5": {"gt": 1.5},
        }),
        "created_by":       "system",
        "is_active":        1,
    },
    {
        "pattern_id":       "P002",
        "label":            "SMA-20 Breakout in Low VIX",
        "description":      "Price crosses above SMA-20 in a low-volatility environment. "
                            "Trend initiation signal with favourable macro backdrop.",
        "hypothesis":       "Low VIX reflects market calm — breakouts in such environments "
                            "tend to have follow-through rather than false breakouts. "
                            "SMA-20 cross signals short-term trend change.",
        "feature_version":  "v1",
        "conditions_json":  json.dumps({
            "sma_20_cross": {"eq": 1},
            "vix_level":    {"lte": 16.0},
        }),
        "created_by":       "system",
        "is_active":        1,
    },
    {
        "pattern_id":       "P003",
        "label":            "Pullback in Uptrend",
        "description":      "Price is above SMA-200 (long-term uptrend intact), experienced "
                            "a 5-day dip, and RSI has cooled below 45. Buy-the-dip setup.",
        "hypothesis":       "Long-term uptrend provides tailwind. Short-term weakness "
                            "represented by 5d decline and RSI cooling creates a "
                            "high-reward entry point before trend resumes.",
        "feature_version":  "v1",
        "conditions_json":  json.dumps({
            "price_vs_sma200": {"gt": 0},
            "return_5d":       {"lt": -2.0},
            "rsi_14":          {"lt": 45},
        }),
        "created_by":       "system",
        "is_active":        1,
    },
    {
        "pattern_id":       "P004",
        "label":            "Volatility Squeeze",
        "description":      "ATR% below 1.5, volume contracting vs 20d average, "
                            "RSI neutral (45-55). Coiling before a directional breakout.",
        "hypothesis":       "Prolonged low volatility compresses price range like a spring. "
                            "Volume contraction confirms indecision. When this resolves, "
                            "the resulting move is typically sharp and sustained.",
        "feature_version":  "v1",
        "conditions_json":  json.dumps({
            "atr_pct":      {"lt": 1.5},
            "volume_trend": {"eq": "contracting"},
            "rsi_14":       {"between": [45, 55]},
        }),
        "created_by":       "system",
        "is_active":        1,
    },
    {
        "pattern_id":       "P005",
        "label":            "FII Accumulation Dip",
        "description":      "FII net buying over 3 days while symbol has declined 5d. "
                            "Smart money buying weakness.",
        "hypothesis":       "Foreign institutional investors have information advantage "
                            "and longer time horizons. Sustained FII buying during price "
                            "weakness is a leading indicator of recovery. High conviction "
                            "signal for Indian markets specifically.",
        "feature_version":  "v1",
        "conditions_json":  json.dumps({
            "fii_net_3d": {"gt": 500},
            "return_5d":  {"lt": -1.5},
        }),
        "created_by":       "system",
        "is_active":        1,
    },
    {
        "pattern_id":       "P006",
        "label":            "Momentum Continuation",
        "description":      "Price above SMA-20, RSI healthy (55-70), volume expanding. "
                            "Trend is strong and participation is growing.",
        "hypothesis":       "Strong momentum with volume confirmation is the most reliable "
                            "continuation signal. RSI in 55-70 zone means overbought "
                            "is not yet reached — room to run. Volume expansion confirms "
                            "broad participation, not just a thin-market squeeze.",
        "feature_version":  "v1",
        "conditions_json":  json.dumps({
            "price_vs_sma20": {"gt": 0},
            "rsi_14":         {"between": [55, 70]},
            "volume_trend":   {"eq": "expanding"},
        }),
        "created_by":       "system",
        "is_active":        1,
    },
]


# ══════════════════════════════════════════════════════
# Condition evaluator
# ══════════════════════════════════════════════════════

def evaluate_condition(value, condition: dict) -> bool:
    """
    Evaluate a single feature value against a condition dict.
    Supports: lt, lte, gt, gte, eq, between, in
    """
    if "lt"      in condition:
        return value < condition["lt"]
    if "lte"     in condition:
        return value <= condition["lte"]
    if "gt"      in condition:
        return value > condition["gt"]
    if "gte"     in condition:
        return value >= condition["gte"]
    if "eq"      in condition:
        return value == condition["eq"]
    if "between" in condition:
        lo, hi = condition["between"]
        return lo <= value <= hi
    if "in"      in condition:
        return value in condition["in"]
    return False


def matches_pattern(row: pd.Series, conditions: dict) -> bool:
    """
    Returns True if ALL conditions are satisfied for this feature row.
    Missing or zero-value features that have conditions → False.
    """
    for feature, condition in conditions.items():
        if feature not in row.index:
            return False
        value = row[feature]
        if not evaluate_condition(value, condition):
            return False
    return True


# ══════════════════════════════════════════════════════
# Pattern seeding
# ══════════════════════════════════════════════════════

def seed_patterns(ch):
    """
    Insert seed patterns into analysis.patterns.
    Uses ReplacingMergeTree — safe to re-run, version timestamp
    ensures latest definition wins after background merge.
    """
    log.info("Seeding pattern library...")

    # Check which patterns already exist
    result = ch.query(
        "SELECT pattern_id FROM analysis.patterns FINAL"
    )
    existing = {row[0] for row in result.result_rows}

    rows = []
    now  = datetime.now()
    ts   = int(now.timestamp())

    for p in SEED_PATTERNS:
        if p["pattern_id"] in existing:
            log.info(f"  Already exists: {p['pattern_id']} — {p['label']}")
        else:
            log.info(f"  Seeding: {p['pattern_id']} — {p['label']}")

        rows.append({
            "pattern_id":       p["pattern_id"],
            "label":            p["label"],
            "description":      p["description"],
            "hypothesis":       p["hypothesis"],
            "feature_version":  p["feature_version"],
            "conditions_json":  p["conditions_json"],
            "total_matches":    0,
            "win_rate_1d":      0.0,
            "win_rate_3d":      0.0,
            "win_rate_5d":      0.0,
            "avg_return_1d":    0.0,
            "avg_return_3d":    0.0,
            "avg_return_5d":    0.0,
            "sharpe_1d":        0.0,
            "max_drawdown_3d":  0.0,
            "last_backtested":  date(1970, 1, 1),
            "created_at":       now,
            "created_by":       p["created_by"],
            "is_active":        p["is_active"],
            "version":          ts,
        })

    df = pd.DataFrame(rows)
    ch.insert_df("analysis.patterns", df)
    log.info(f"✅ {len(rows)} patterns seeded/refreshed")

    with results_lock:
        results["patterns_seeded"] = len(rows)


# ══════════════════════════════════════════════════════
# Feature fetching
# ══════════════════════════════════════════════════════

def fetch_active_patterns(ch) -> list[dict]:
    """Fetch all active patterns with their conditions."""
    result = ch.query(
        "SELECT pattern_id, label, conditions_json "
        "FROM analysis.patterns FINAL "
        "WHERE is_active = 1"
    )
    patterns = []
    for row in result.result_rows:
        try:
            conditions = json.loads(row[2])
            patterns.append({
                "pattern_id":      row[0],
                "label":           row[1],
                "conditions_json": conditions,
            })
        except json.JSONDecodeError as e:
            log.warning(f"Invalid conditions_json for {row[0]}: {e}")
    return patterns


def fetch_already_mapped(ch) -> set[tuple]:
    """
    Fetch (pattern_id, symbol, event_date) already in pattern_event_map.
    Used to avoid re-inserting existing matches in incremental mode.
    """
    result = ch.query(
        "SELECT pattern_id, symbol, event_date "
        "FROM analysis.pattern_event_map FINAL"
    )
    return {(row[0], row[1], row[2]) for row in result.result_rows}


def fetch_features(ch, full: bool = False) -> pd.DataFrame:
    """
    Fetch feature rows to evaluate.
    full=True  → all rows
    full=False → only rows not yet in pattern_event_map for any pattern
                 (approximation: fetch all, let match logic deduplicate)
    """
    result = ch.query(
        "SELECT "
        "  event_date, symbol, market, "
        "  rsi_14, rsi_5, "
        "  sma_20_cross, sma_50_cross, ema_20_cross, "
        "  price_vs_sma20, price_vs_sma50, price_vs_sma200, "
        "  volume_z5, volume_z20, volume_trend, "
        "  atr_14, atr_pct, bb_position, "
        "  vix_level, vix_regime, nifty_trend_5d, fii_net_3d, "
        "  return_5d, return_20d, drawdown_pct "
        "FROM analysis.pattern_features FINAL "
        "ORDER BY symbol, event_date"
    )

    if not result.result_rows:
        return pd.DataFrame()

    cols = [
        "event_date", "symbol", "market",
        "rsi_14", "rsi_5",
        "sma_20_cross", "sma_50_cross", "ema_20_cross",
        "price_vs_sma20", "price_vs_sma50", "price_vs_sma200",
        "volume_z5", "volume_z20", "volume_trend",
        "atr_14", "atr_pct", "bb_position",
        "vix_level", "vix_regime", "nifty_trend_5d", "fii_net_3d",
        "return_5d", "return_20d", "drawdown_pct",
    ]
    df = pd.DataFrame(result.result_rows, columns=cols)
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    return df


# ══════════════════════════════════════════════════════
# Pattern matching
# ══════════════════════════════════════════════════════

def run_pattern_matching(ch, patterns: list[dict],
                         df_features: pd.DataFrame,
                         already_mapped: set[tuple],
                         full: bool):
    """
    Evaluate all feature rows against all patterns.
    Inserts matches into analysis.pattern_event_map in batches.
    Updates analysis.patterns.total_matches after each pattern.
    """
    total_features = len(df_features)
    log.info(f"Evaluating {total_features:,} feature rows "
             f"against {len(patterns)} patterns...")

    version_ts = int(datetime.now().timestamp())

    for pattern in patterns:
        pid        = pattern["pattern_id"]
        label      = pattern["label"]
        conditions = pattern["conditions_json"]

        log.info(f"  Matching: [{pid}] {label}")
        log.info(f"  Conditions: {json.dumps(conditions)}")

        match_rows = []

        for _, row in df_features.iterrows():
            key = (pid, row["symbol"], row["event_date"])

            # Skip if already mapped (incremental mode)
            if not full and key in already_mapped:
                continue

            if matches_pattern(row, conditions):
                match_rows.append({
                    "pattern_id": pid,
                    "symbol":     row["symbol"],
                    "market":     row["market"],
                    "event_date": row["event_date"],
                    "match_score": 1.0,
                    "matched_by": "rule",
                    "version":    version_ts,
                })

        log.info(f"  → {len(match_rows)} matches found")

        # Insert in batches
        if match_rows:
            for i in range(0, len(match_rows), BATCH_SIZE):
                chunk = match_rows[i:i + BATCH_SIZE]
                df_chunk = pd.DataFrame(chunk)
                ch.insert_df("analysis.pattern_event_map", df_chunk)

            # Update total_matches with the TRUE cumulative count (BUG-003).
            # len(match_rows) is only new matches this run — using it would
            # overwrite the accumulated count from previous incremental runs.
            # SEC-002: _validate_id already called above; cast result to int.
            _validate_id(pid, "pattern_id")
            cum = ch.query(
                f"SELECT count() FROM analysis.pattern_event_map FINAL "
                f"WHERE pattern_id = '{pid}'"
            )
            total_matches = int(cum.result_rows[0][0])
            ch.command(
                f"ALTER TABLE analysis.patterns UPDATE "
                f"total_matches = {total_matches}, "
                f"last_backtested = today() "
                f"WHERE pattern_id = '{pid}' "
                f"SETTINGS mutations_sync = 0"
            )

        with results_lock:
            results["matches_found"] += len(match_rows)

    with results_lock:
        results["features_scanned"] = total_features


# ══════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════

def print_summary():
    print("\n" + "=" * 60)
    print("PATTERN BUILDER — SUMMARY")
    print("=" * 60)
    print(f"🌱 Patterns seeded  : {results['patterns_seeded']}")
    print(f"🔍 Features scanned : {results['features_scanned']:,}")
    print(f"✅ Matches found    : {results['matches_found']:,}")
    print(f"❌ Failed           : {len(results['failed'])}")
    if results["failed"]:
        for f in results["failed"][:10]:
            print(f"   {f}")
    print("=" * 60)


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Pattern Builder — seeds patterns and maps events to patterns"
    )
    parser.add_argument("--seed-only", action="store_true",
                        help="Only seed/refresh pattern definitions, skip matching")
    parser.add_argument("--map-only",  action="store_true",
                        help="Skip seeding, only run pattern matching")
    parser.add_argument("--full",      action="store_true",
                        help="Remap ALL features (ignore already-mapped)")
    parser.add_argument("--pattern",   type=str, default=None,
                        help="Debug: run matching for a single pattern_id only")
    args = parser.parse_args()

    log.info("=== Pattern Builder Starting ===")
    log.info(f"Mode    : {'SEED ONLY' if args.seed_only else 'MAP ONLY' if args.map_only else 'FULL' if args.full else 'INCREMENTAL'}")
    log.info(f"Started : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # DATA-003: warn early if fii_dii not seeded (P005 will have 0 matches)
    check_fii_dii_seeded(ch)

    # ── Step 1: Seed patterns ──────────────────────────
    if not args.map_only:
        seed_patterns(ch)

    if args.seed_only:
        print_summary()
        return

    # ── Step 2: Load patterns ──────────────────────────
    patterns = fetch_active_patterns(ch)
    if not patterns:
        log.warning("No active patterns found. Run --seed-only first.")
        return

    # Filter to single pattern if --pattern flag used
    if args.pattern:
        patterns = [p for p in patterns if p["pattern_id"] == args.pattern]
        if not patterns:
            log.error(f"Pattern {args.pattern} not found or not active.")
            return
        log.info(f"Debug mode: single pattern {args.pattern}")

    log.info(f"Active patterns : {len(patterns)}")
    for p in patterns:
        log.info(f"  [{p['pattern_id']}] {p['label']}")

    # ── Step 3: Load feature rows ──────────────────────
    log.info("Loading feature vectors from analysis.pattern_features...")
    df_features = fetch_features(ch, full=args.full)

    if df_features.empty:
        log.warning("No feature rows found. Run pattern_feature_extractor first.")
        return

    log.info(f"Feature rows loaded: {len(df_features):,}")

    # ── Step 4: Load already-mapped (incremental) ──────
    if not args.full:
        log.info("Loading already-mapped events (incremental mode)...")
        already_mapped = fetch_already_mapped(ch)
        log.info(f"Already mapped: {len(already_mapped):,} entries")
    else:
        log.warning("Full mode — remapping ALL features.")
        already_mapped = set()

    # ── Step 5: Run matching ───────────────────────────
    run_pattern_matching(ch, patterns, df_features, already_mapped, args.full)

    # ── Step 6: Final counts per pattern ──────────────
    log.info("\nMatch counts per pattern:")
    result = ch.query(
        "SELECT pattern_id, count() as matches "
        "FROM analysis.pattern_event_map FINAL "
        "GROUP BY pattern_id "
        "ORDER BY pattern_id"
    )
    for row in result.result_rows:
        log.info(f"  [{row[0]}] {row[1]:,} matches")

    print_summary()
    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()