#!/usr/bin/env python3
"""
gap_analyzer.py
───────────────
Closes the feedback loop by identifying systematic gaps between what the
pattern engine predicted and what actually happened.

Two passes per run:
  false_negative — events that occurred but had no active prediction
  false_positive — predictions that did not materialise (miss / expired)

Optionally calls the Claude API for root-cause diagnosis when
GAP_LLM_ENABLED=true (default: false — keeps daily pipeline zero-cost).

Results written to analysis.gap_analysis_log.

Usage:
  python gap_analyzer.py                    # last 30 days, stats only
  python gap_analyzer.py --days 90          # longer window
  python gap_analyzer.py --run-type fn      # false negatives only
  python gap_analyzer.py --run-type fp      # false positives only
  GAP_LLM_ENABLED=true python gap_analyzer.py   # include Claude analysis

Docker:
  docker compose run --rm gap_analyzer
"""

import os
import uuid
import logging
import argparse
from datetime import datetime, date, timedelta

import pandas as pd
import clickhouse_connect

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

LLM_ENABLED   = os.getenv("GAP_LLM_ENABLED", "false").lower() == "true"
LLM_MODEL     = os.getenv("GAP_LLM_MODEL", "claude-sonnet-4-6")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


# ══════════════════════════════════════════════════════
# Stats computation
# ══════════════════════════════════════════════════════

def compute_false_negatives(ch, from_date: date, to_date: date) -> dict:
    """
    Events that occurred (in detected_events) but had no prediction.
    Rate = missed / total events in window.
    """
    total_result = ch.query(
        "SELECT count() FROM analysis.detected_events FINAL "
        "WHERE event_date >= {d_from:Date} AND event_date <= {d_to:Date}",
        parameters={"d_from": from_date, "d_to": to_date}
    )
    total_events = int(total_result.result_rows[0][0])

    missed_result = ch.query(
        "SELECT count() FROM analysis.detected_events de FINAL "
        "WHERE de.event_date >= {d_from:Date} AND de.event_date <= {d_to:Date} "
        "  AND NOT EXISTS ("
        "    SELECT 1 FROM analysis.predictions p FINAL "
        "    WHERE p.symbol = de.symbol "
        "      AND p.prediction_date = de.event_date"
        "  )",
        parameters={"d_from": from_date, "d_to": to_date}
    )
    missed = int(missed_result.result_rows[0][0])
    rate   = round(missed / total_events, 4) if total_events > 0 else 0.0

    log.info(f"False negatives: {missed}/{total_events} events missed (rate={rate:.1%})")
    return {"missed_events": missed, "total_events": total_events,
            "false_negative_rate": rate}


def compute_false_positives(ch, from_date: date, to_date: date) -> dict:
    """
    Predictions that did not materialise (status = 'miss' or 'expired').
    Rate = wrong / total predictions in window.
    """
    total_result = ch.query(
        "SELECT count() FROM analysis.predictions FINAL "
        "WHERE prediction_date >= {d_from:Date} AND prediction_date <= {d_to:Date} "
        "  AND status != 'open'",
        parameters={"d_from": from_date, "d_to": to_date}
    )
    total_preds = int(total_result.result_rows[0][0])

    fp_result = ch.query(
        "SELECT count() FROM analysis.predictions FINAL "
        "WHERE prediction_date >= {d_from:Date} AND prediction_date <= {d_to:Date} "
        "  AND status IN ('miss', 'expired')",
        parameters={"d_from": from_date, "d_to": to_date}
    )
    wrong = int(fp_result.result_rows[0][0])
    rate  = round(wrong / total_preds, 4) if total_preds > 0 else 0.0

    log.info(f"False positives: {wrong}/{total_preds} predictions wrong (rate={rate:.1%})")
    return {"false_positives": wrong, "total_preds": total_preds,
            "false_positive_rate": rate}


def count_symbols_reviewed(ch, from_date: date, to_date: date) -> int:
    result = ch.query(
        "SELECT count(DISTINCT symbol) FROM analysis.detected_events FINAL "
        "WHERE event_date >= {d_from:Date} AND event_date <= {d_to:Date}",
        parameters={"d_from": from_date, "d_to": to_date}
    )
    return int(result.result_rows[0][0])


# ══════════════════════════════════════════════════════
# LLM analysis (opt-in)
# ══════════════════════════════════════════════════════

def run_llm_analysis(fn_stats: dict, fp_stats: dict,
                     from_date: date, to_date: date) -> dict:
    """
    Call Claude to diagnose systematic gaps. Returns dict with
    llm_summary, suggested_features, suggested_pattern_updates.
    """
    if not ANTHROPIC_KEY:
        log.warning("GAP_LLM_ENABLED=true but ANTHROPIC_API_KEY not set — skipping LLM")
        return {"llm_summary": "", "suggested_features": "",
                "suggested_pattern_updates": "", "model_used": "", "tokens_used": 0,
                "llm_cost_usd": 0.0}

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

        prompt = (
            f"You are analysing a trading pattern prediction system.\n"
            f"Window: {from_date} to {to_date}\n\n"
            f"False negatives (events with no prediction): "
            f"{fn_stats['missed_events']} / {fn_stats['total_events']} "
            f"({fn_stats['false_negative_rate']:.1%})\n"
            f"False positives (predictions that missed): "
            f"{fp_stats['false_positives']} / {fp_stats['total_preds']} "
            f"({fp_stats['false_positive_rate']:.1%})\n\n"
            f"In 3 concise sections:\n"
            f"1. DIAGNOSIS: What does this gap pattern suggest?\n"
            f"2. SUGGESTED_FEATURES: New feature columns to add to the extractor "
            f"(comma-separated names only)\n"
            f"3. SUGGESTED_PATTERN_UPDATES: Specific pattern condition changes "
            f"(pattern_id: change description)"
        )

        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )

        text        = response.content[0].text
        tokens_used = response.usage.input_tokens + response.usage.output_tokens
        # Rough cost estimate: $3/M input + $15/M output for Sonnet
        cost = (response.usage.input_tokens * 3 + response.usage.output_tokens * 15) / 1_000_000

        sections = {"llm_summary": "", "suggested_features": "",
                    "suggested_pattern_updates": ""}
        current = "llm_summary"
        for line in text.splitlines():
            if "SUGGESTED_FEATURES" in line:
                current = "suggested_features"
            elif "SUGGESTED_PATTERN_UPDATES" in line:
                current = "suggested_pattern_updates"
            else:
                sections[current] += line + "\n"

        return {
            **{k: v.strip() for k, v in sections.items()},
            "model_used":   LLM_MODEL,
            "tokens_used":  tokens_used,
            "llm_cost_usd": round(cost, 6),
        }

    except Exception as e:
        log.error(f"LLM analysis failed: {e}")
        return {"llm_summary": f"LLM error: {e}", "suggested_features": "",
                "suggested_pattern_updates": "", "model_used": LLM_MODEL,
                "tokens_used": 0, "llm_cost_usd": 0.0}


# ══════════════════════════════════════════════════════
# Insert
# ══════════════════════════════════════════════════════

def insert_log(ch, row: dict):
    ch.insert_df("analysis.gap_analysis_log", pd.DataFrame([row]))
    log.info("Gap analysis log written to analysis.gap_analysis_log")


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Gap Analyzer — measures false negative and false positive rates"
    )
    parser.add_argument("--days",     type=int, default=30,
                        help="Number of days to look back (default: 30)")
    parser.add_argument("--run-type", choices=["fn", "fp", "full"], default="full",
                        help="fn=false_negatives, fp=false_positives, full=both")
    args = parser.parse_args()

    today     = date.today()
    from_date = today - timedelta(days=args.days)
    run_type  = {"fn": "false_negative", "fp": "false_positive",
                 "full": "full"}[args.run_type]

    log.info("=== Gap Analyzer ===")
    log.info(f"Window    : {from_date} → {today}")
    log.info(f"Run type  : {run_type}")
    log.info(f"LLM       : {'enabled' if LLM_ENABLED else 'disabled'}")

    ch = get_ch_client()

    fn_stats = compute_false_negatives(ch, from_date, today)
    fp_stats = compute_false_positives(ch, from_date, today)
    symbols  = count_symbols_reviewed(ch, from_date, today)

    llm_result = (run_llm_analysis(fn_stats, fp_stats, from_date, today)
                  if LLM_ENABLED
                  else {"llm_summary": "", "suggested_features": "",
                        "suggested_pattern_updates": "", "model_used": "",
                        "tokens_used": 0, "llm_cost_usd": 0.0})

    row = {
        "id":                        str(uuid.uuid4()),
        "analysis_date":             today,
        "run_type":                  run_type,
        "date_range_start":          from_date,
        "date_range_end":            today,
        "symbols_reviewed":          symbols,
        "missed_events":             fn_stats["missed_events"],
        "false_positives":           fp_stats["false_positives"],
        "false_negative_rate":       fn_stats["false_negative_rate"],
        "false_positive_rate":       fp_stats["false_positive_rate"],
        "llm_summary":               llm_result["llm_summary"],
        "suggested_features":        llm_result["suggested_features"],
        "suggested_pattern_updates": llm_result["suggested_pattern_updates"],
        "action_taken":              "",
        "model_used":                llm_result["model_used"],
        "tokens_used":               llm_result["tokens_used"],
        "llm_cost_usd":              llm_result["llm_cost_usd"],
        "created_at":                datetime.now(),
    }

    insert_log(ch, row)

    print("\n" + "=" * 55)
    print(f"  False negative rate : {fn_stats['false_negative_rate']:.1%}  "
          f"({fn_stats['missed_events']}/{fn_stats['total_events']} events)")
    print(f"  False positive rate : {fp_stats['false_positive_rate']:.1%}  "
          f"({fp_stats['false_positives']}/{fp_stats['total_preds']} predictions)")
    print(f"  Symbols reviewed    : {symbols}")
    if llm_result["llm_summary"]:
        print(f"\n  LLM diagnosis: {llm_result['llm_summary'][:120]}...")
    print("=" * 55)


if __name__ == "__main__":
    main()
