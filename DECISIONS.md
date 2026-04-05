---

## Decision 020 — Event Detection Threshold at 2%
**Date:** 2026-04-04
**Decision:** Detect single-day price moves where |pct_change| >= 2.0% as "events".
**Rationale:** 2% captures meaningful moves while filtering noise. Applied to both UP and DOWN moves symmetrically using abs(pct_change).
**Trade-off:** May miss slow-burn trends. Threshold configurable via --threshold flag.
**Review:** Revisit after 4 weeks — may need separate thresholds by market cap or market type.

---

## Decision 021 — Feature Vector Versioned at v1
**Date:** 2026-04-04
**Decision:** All feature rows carry `feature_version = 'v1'`. Schema changes increment the version.
**Rationale:** Allows mixing old and new features without full recompute. Pattern conditions reference feature_version implicitly.
**Rule:** Any new feature column added to pattern_feature_extractor.py → bump to v2, backfill, update patterns.

---

## Decision 022 — LLM Gap Analysis Gated by ENV VAR
**Date:** 2026-04-04
**Decision:** `GAP_LLM_ENABLED=false` by default. Gap analyzer only calls Anthropic API when explicitly enabled.
**Rationale:** Keeps daily pipeline fast and zero-cost. LLM runs reserved for weekly manual review sessions.
**Usage:** `GAP_LLM_ENABLED=true docker compose run --rm gap_analyzer`

---

## Decision 023 — Minimum 3 Stars for Predictions
**Date:** 2026-04-04
**Decision:** `prediction_engine.py` only surfaces patterns with stars >= 3 and needs_more_data = 0.
**Rationale:** Ensures minimum evidence threshold before generating actionable signals.
**Hard floor:** Patterns with sharpe < -0.5 are suppressed to 1 star regardless of other scores.

---

## Decision 024 — Backtest Minimum 10 Samples
**Date:** 2026-04-04
**Decision:** Patterns with fewer than 10 historical matches are marked needs_more_data = 1 and excluded from predictions.
**Rationale:** Statistical significance requires minimum sample size. Below 10, win rates are noise.
**Example:** P005 (FII Accumulation Dip) has 0 matches until market.fii_dii is populated — correctly suppressed.

---

## Decision 025 — Next Trading Day as Target Date
**Date:** 2026-04-05
**Decision:** prediction_engine.py computes target_date as next weekday using a while loop:
  `while target_date.weekday() >= 5: target_date += timedelta(days=1)`
**Rationale:** Simple if/elif logic incorrectly targeted Sunday when prediction_date was Saturday.
  The while loop correctly handles all cases including Friday→Monday and holiday sequences.

---

## Decision 026 — ReplacingMergeTree version Column Never Updated via ALTER
**Date:** 2026-04-04
**Decision:** Never use ALTER TABLE UPDATE on `version` column in any ReplacingMergeTree table.
**Rationale:** ClickHouse raises CANNOT_UPDATE_COLUMN error. version is part of the dedup key.
  To refresh a row: re-insert with a new version = int(datetime.now().timestamp()).
**Affected tables:** All analysis.* tables, market.* tables.

---

## Decision 027 — Star Rating Hard Floor at Sharpe -0.5
**Date:** 2026-04-05
**Decision:** Patterns with sharpe_1d < -0.5 are suppressed to 1 star regardless of other score components.
**Rationale:** Initial threshold of -1.0 allowed P004 (Volatility Squeeze, sharpe=-0.78) to pass as 3 stars,
  which would have generated predictions from an actively harmful pattern.
**Impact:** P004 correctly suppressed. Threshold configurable in star_rater.py.