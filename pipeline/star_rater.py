#!/usr/bin/env python3
"""
star_rater.py
─────────────
Assigns 1–5 star ratings to each active pattern based on backtest results.

Rating formula (score in [0, 1], then mapped to 1–5 stars):

  score = (win_rate_component    × 0.35)
        + (sample_adequacy       × 0.20)
        + (recency_score         × 0.20)
        + (risk_adj_return       × 0.25)

  stars = max(1, ceil(score / 0.20))   → maps [0,1] → 1–5

Components:

  win_rate_component:
    win_rate_1d / 100   (already in [0,1] range)
    win_rate_1d here means % of matches where next-day
    return was positive (not necessarily ≥ 2%).

  sample_adequacy:
    log10(n) / log10(1000), capped at 1.0
    n=10   → 0.33,  n=100  → 0.67,  n=1000 → 1.0

  recency_score:
    % of pattern matches that occurred in the last 90 days
    (recent market behaviour weighted more than stale history)

  risk_adj_return:
    Normalised Sharpe. Raw Sharpe clipped to [-3, 3],
    then scaled to [0, 1] via (sharpe + 3) / 6

Minimum thresholds before rating:
  • sample_size < 10  → stars = 1, needs_more_data = 1
  • sharpe_1d < -1    → stars = 1 (actively harmful pattern)

Stars interpretation:
  ★★★★★  Exceptional  — predict with full confidence
  ★★★★   Strong       — predict, monitor closely
  ★★★    Moderate     — predict with reduced position size
  ★★     Weak         — do not predict, watch for improvement
  ★      Poor/harmful — suppress completely

Writes to: analysis.pattern_ratings
  symbol = '*' for the all-symbol aggregate
  (per-symbol ratings are a future enhancement)

Usage:
  python star_rater.py              # rate all active patterns
  python star_rater.py --pattern P001  # rate single pattern (debug)
  python star_rater.py --verbose    # show detailed score breakdown
"""

import os
import math
import logging
import argparse
from datetime import datetime, date, timedelta

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

MIN_SAMPLE_SIZE = 10     # below this → 1 star, needs_more_data = 1
RECENCY_WINDOW  = 90     # days to look back for recency score

# Component weights (must sum to 1.0)
W_WIN_RATE    = 0.35
W_SAMPLE      = 0.20
W_RECENCY     = 0.20
W_RISK_ADJ    = 0.25


# ── ClickHouse client ──────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Data fetching
# ══════════════════════════════════════════════════════

def fetch_patterns(ch, pattern_id=None) -> pd.DataFrame:
    """Fetch active patterns with their backtest stats."""
    where = ""
    params = {}
    conditions = ["is_active = 1"]

    if pattern_id:
        conditions.append("pattern_id = {pid:String}")
        params["pid"] = pattern_id

    where = "WHERE " + " AND ".join(conditions)

    result = ch.query(
        f"SELECT pattern_id, label, total_matches, "
        f"       win_rate_1d, avg_return_1d, sharpe_1d, "
        f"       win_rate_3d, avg_return_3d, "
        f"       win_rate_5d, avg_return_5d "
        f"FROM analysis.patterns FINAL "
        f"{where}",
        parameters=params
    )

    if not result.result_rows:
        return pd.DataFrame()

    df = pd.DataFrame(result.result_rows, columns=[
        "pattern_id", "label", "total_matches",
        "win_rate_1d", "avg_return_1d", "sharpe_1d",
        "win_rate_3d", "avg_return_3d",
        "win_rate_5d", "avg_return_5d",
    ])
    return df


def fetch_recency_scores(ch) -> dict:
    """
    Compute recency score per pattern:
    % of matches with event_date in last RECENCY_WINDOW days.
    Returns {pattern_id: recency_score (0.0–1.0)}
    """
    cutoff = date.today() - timedelta(days=RECENCY_WINDOW)

    result = ch.query(
        "SELECT "
        "  pattern_id, "
        "  countIf(event_date >= {cutoff:Date}) as recent_matches, "
        "  count() as total_matches "
        "FROM analysis.pattern_event_map FINAL "
        "GROUP BY pattern_id",
        parameters={"cutoff": cutoff}
    )

    recency = {}
    for row in result.result_rows:
        pid, recent, total = row[0], row[1], row[2]
        recency[pid] = round(recent / total, 4) if total > 0 else 0.0

    return recency


def fetch_positive_win_rates(ch) -> dict:
    """
    Compute what % of backtest rows had positive return_1d
    (more lenient than hit_2pct — captures the directional edge).
    Returns {pattern_id: positive_rate (0.0–1.0)}
    """
    result = ch.query(
        "SELECT "
        "  pattern_id, "
        "  countIf(return_1d > 0) as positive, "
        "  count() as total "
        "FROM analysis.backtests FINAL "
        "GROUP BY pattern_id"
    )

    rates = {}
    for row in result.result_rows:
        pid, pos, total = row[0], row[1], row[2]
        rates[pid] = round(pos / total, 4) if total > 0 else 0.0

    return rates


# ══════════════════════════════════════════════════════
# Rating computation
# ══════════════════════════════════════════════════════

def compute_sample_adequacy(n: int) -> float:
    """
    log10(n) / log10(1000), capped at 1.0.
    n=10  → 0.33
    n=100 → 0.67
    n=1000→ 1.00
    """
    if n < 1:
        return 0.0
    return min(1.0, math.log10(max(1, n)) / math.log10(1000))


def compute_risk_adj(sharpe: float) -> float:
    """
    Normalise raw Sharpe to [0, 1].
    Clip to [-3, 3], then scale: (sharpe + 3) / 6
    sharpe = -3 → 0.0 (worst)
    sharpe =  0 → 0.5 (neutral)
    sharpe =  3 → 1.0 (best)
    """
    clipped = max(-3.0, min(3.0, sharpe))
    return round((clipped + 3.0) / 6.0, 4)


def compute_stars(score: float) -> int:
    """
    Map [0, 1] score to 1–5 stars.
    score < 0.20 → 1★
    score < 0.40 → 2★
    score < 0.60 → 3★
    score < 0.80 → 4★
    score >= 0.80→ 5★
    """
    return max(1, min(5, math.ceil(score / 0.20)))


def rate_pattern(row: pd.Series,
                 recency_scores: dict,
                 positive_rates: dict,
                 verbose: bool = False) -> dict:
    """
    Compute star rating for one pattern.
    Returns dict matching analysis.pattern_ratings schema.
    """
    pid     = row["pattern_id"]
    label   = row["label"]
    n       = int(row["total_matches"])
    sharpe  = float(row["sharpe_1d"])

    # ── Hard floors ────────────────────────────────────
    if n < MIN_SAMPLE_SIZE:
        reason = f"Insufficient samples (n={n} < {MIN_SAMPLE_SIZE})"
        if verbose:
            log.info(f"  [{pid}] {label}")
            log.info(f"  → ★ (needs more data: {reason})")
        return {
            "pattern_id":      pid,
            "symbol":          "*",
            "stars":           1,
            "win_rate":        0.0,
            "sample_size":     n,
            "recency_score":   0.0,
            "risk_adj_return": 0.0,
            "needs_more_data": 1,
            "rating_reason":   reason,
            "rated_at":        datetime.now(),
            "version":         int(datetime.now().timestamp()),
        }

    if sharpe < -1.0:
        reason = f"Actively harmful pattern (sharpe={sharpe:.2f} < -1.0)"
        if verbose:
            log.info(f"  [{pid}] {label}")
            log.info(f"  → ★ (suppressed: {reason})")
        return {
            "pattern_id":      pid,
            "symbol":          "*",
            "stars":           1,
            "win_rate":        round(positive_rates.get(pid, 0.0), 4),
            "sample_size":     n,
            "recency_score":   round(recency_scores.get(pid, 0.0), 4),
            "risk_adj_return": round(compute_risk_adj(sharpe), 4),
            "needs_more_data": 0,
            "rating_reason":   reason,
            "rated_at":        datetime.now(),
            "version":         int(datetime.now().timestamp()),
        }

    # ── Score components ───────────────────────────────
    positive_rate    = positive_rates.get(pid, 0.0)
    recency          = recency_scores.get(pid, 0.0)
    sample_adequacy  = compute_sample_adequacy(n)
    risk_adj         = compute_risk_adj(sharpe)

    win_rate_component = positive_rate   # already in [0,1]

    score = (
        win_rate_component * W_WIN_RATE +
        sample_adequacy    * W_SAMPLE   +
        recency            * W_RECENCY  +
        risk_adj           * W_RISK_ADJ
    )
    score = round(score, 4)
    stars = compute_stars(score)

    reason = (
        f"score={score:.3f} | "
        f"win_rate={positive_rate*100:.1f}% (w={W_WIN_RATE}) | "
        f"sample={sample_adequacy:.2f} (w={W_SAMPLE}) | "
        f"recency={recency:.2f} (w={W_RECENCY}) | "
        f"risk_adj={risk_adj:.2f} (w={W_RISK_ADJ})"
    )

    if verbose:
        stars_str = "★" * stars + "☆" * (5 - stars)
        log.info(f"  [{pid}] {label}")
        log.info(f"  n={n:,} | sharpe={sharpe:.2f} | "
                 f"positive_rate={positive_rate*100:.1f}% | "
                 f"recency={recency*100:.1f}%")
        log.info(f"  score={score:.3f} → {stars_str}")

    return {
        "pattern_id":      pid,
        "symbol":          "*",
        "stars":           stars,
        "win_rate":        round(positive_rate, 4),
        "sample_size":     n,
        "recency_score":   round(recency, 4),
        "risk_adj_return": round(risk_adj, 4),
        "needs_more_data": 0,
        "rating_reason":   reason,
        "rated_at":        datetime.now(),
        "version":         int(datetime.now().timestamp()),
    }


# ══════════════════════════════════════════════════════
# Insert
# ══════════════════════════════════════════════════════

RATING_COLS = [
    "pattern_id", "symbol", "stars",
    "win_rate", "sample_size",
    "recency_score", "risk_adj_return",
    "needs_more_data", "rating_reason",
    "rated_at", "version",
]


def insert_ratings(ch, ratings: list[dict]):
    if not ratings:
        return
    df = pd.DataFrame(ratings)[RATING_COLS]
    ch.insert_df("analysis.pattern_ratings", df)
    log.info(f"Inserted {len(df)} ratings into analysis.pattern_ratings")


# ══════════════════════════════════════════════════════
# Summary printer
# ══════════════════════════════════════════════════════

def print_summary(ratings: list[dict]):
    print("\n" + "=" * 65)
    print("STAR RATER — RESULTS")
    print("=" * 65)
    print(f"{'Pattern':<8} {'Label':<35} {'Stars':<8} {'n':>6} {'Win%':>6} {'Sharpe':>7}")
    print("-" * 65)

    for r in sorted(ratings, key=lambda x: x["stars"], reverse=True):
        stars_str = "★" * r["stars"] + "☆" * (5 - r["stars"])
        win_pct   = round(r["win_rate"] * 100, 1)
        risk_adj  = r["risk_adj_return"]
        # Convert risk_adj back to approximate Sharpe for display
        sharpe_approx = round(risk_adj * 6 - 3, 2)
        flag = " ⚠️ needs data" if r["needs_more_data"] else ""
        print(
            f"{r['pattern_id']:<8} "
            f"{r['rating_reason'][:35]:<35} "
            f"{stars_str:<8} "
            f"{r['sample_size']:>6,} "
            f"{win_pct:>5.1f}% "
            f"{sharpe_approx:>7.2f}"
            f"{flag}"
        )

    print("=" * 65)
    eligible = [r for r in ratings if r["stars"] >= 3 and not r["needs_more_data"]]
    print(f"\n✅ Patterns eligible for predictions (★★★+): {len(eligible)}")
    for r in eligible:
        print(f"   [{r['pattern_id']}] {'★' * r['stars']} — {r['rating_reason'][:50]}")
    print()


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Star Rater — assigns 1-5 star ratings to patterns"
    )
    parser.add_argument("--pattern", type=str, default=None,
                        help="Rate a single pattern_id (debug)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show detailed score breakdown per pattern")
    args = parser.parse_args()

    log.info("=== Star Rater Starting ===")
    log.info(f"Started : {datetime.now()}")

    ch = get_ch_client()
    log.info("ClickHouse connected ✅")

    # ── Fetch inputs ───────────────────────────────────
    log.info("Fetching active patterns...")
    df_patterns = fetch_patterns(ch, pattern_id=args.pattern)

    if df_patterns.empty:
        log.warning("No active patterns found. Run pattern_builder first.")
        return

    log.info(f"Patterns to rate: {len(df_patterns)}")

    log.info("Computing recency scores...")
    recency_scores = fetch_recency_scores(ch)

    log.info("Computing positive win rates from backtests...")
    positive_rates = fetch_positive_win_rates(ch)

    # ── Rate each pattern ──────────────────────────────
    ratings = []
    if args.verbose:
        log.info("\nDetailed breakdown:")

    for _, row in df_patterns.iterrows():
        rating = rate_pattern(row, recency_scores, positive_rates, args.verbose)
        ratings.append(rating)

    # ── Insert ratings ─────────────────────────────────
    insert_ratings(ch, ratings)

    # ── Print summary table ────────────────────────────
    print_summary(ratings)

    log.info(f"Finished : {datetime.now()}")


if __name__ == "__main__":
    main()
