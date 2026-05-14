---

## Decision 001 — Iron Fly as Core Strategy
**Date:** 2026-05-13
**Decision:** Trade iron fly (short ATM straddle + long OTM wings) instead of naked straddle.
**Rationale:** Naked straddle has unlimited risk. Iron fly provides defined max loss = wing_width − net_premium. Required for paper-to-live transition and any risk-based sizing.
**Wing points:** NIFTY=200, BANKNIFTY=500, FINNIFTY=200.

---

## Decision 002 — Walk-Forward CV Must Split on entry_date Not expiry_dt
**Date:** 2026-05-13
**Decision:** Confidence scorer walk-forward splits must use `entry_date` (day the trade is entered) as the time boundary, not `expiry_dt` (expiry date of the contract).
**Rationale:** A single expiry date can have the ATM straddle entered days before expiry. Features extracted on those entry days are future data relative to any split point before the expiry. Using `expiry_dt` creates train/test contamination. ← **KNOWN BUG — not yet fixed** (see TODO.md CRIT-002).
**Rule:** No feature row can appear in train set if its `entry_date` >= the fold's test start date.

---

## Decision 003 — EOD Fallback for score_today()
**Date:** 2026-05-13
**Decision:** When `options_eod_summary` has no row matching today's date, fall back to the most recent available past date via `_nearest_eod()` helper.
**Rationale:** Today's bhavcopy is not available until after NSE publishes it (~17:00 IST). Scoring at 13:00 UTC (18:30 IST) would fail with zero IV if we require today's exact date. The most recent past EOD is accurate enough for next-day signal generation.

---

## Decision 004 — Per-Symbol XGBoost Models (Not Pooled)
**Date:** 2026-05-13
**Decision:** Train separate XGBoost models for each symbol (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY). The `--compare` run evaluates pooled vs per-symbol and picks the winner.
**Rationale:** Pooled model (AUC=0.577 OOS) performed worse than average per-symbol (AUC=0.589 OOS). Each index has distinct volatility regime and participant profile. MIDCPNIFTY AUC=0.733 shows strong per-symbol signal; pooling dilutes it.
**Override rule:** If a symbol has fewer than MIN_TRAIN=25 OOS rows, fall back to scorecard (rule-based).

---

## Decision 005 — Profit Threshold = 1% of SPAN Margin
**Date:** 2026-05-13
**Decision:** A backtest trade is classified WIN only when net PnL (after wing costs + transaction costs) ≥ 1% of the SPAN margin blocked for that trade.
**Rationale:** A 1pt straddle profit is not actually profitable after brokerage, STT, and exchange fees. The 1% SPAN threshold reflects a realistic minimum edge worth trading live.
**SPAN estimation:** Until exact SPAN data is integrated, estimate as `atm_premium × lot_size × 2 × 0.30` (30% of notional). ← **KNOWN BUG — exact SPAN not yet integrated** (see TODO.md HIGH-005).

---

## Decision 006 — No SENSEX (BSESENSEX) for Now
**Date:** 2026-05-13
**Decision:** Focus on 4 NSE indices: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY. SENSEX deferred.
**Rationale:** SENSEX F&O liquidity is thinner than NIFTY. NSE bhavcopy already covers all 4. Adding SENSEX requires BSE data source integration — separate effort.

---

## Decision 026 — ReplacingMergeTree version Column Never Updated via ALTER
**Date:** 2026-04-04
**Decision:** Never use ALTER TABLE UPDATE on `version` column in any ReplacingMergeTree table.
**Rationale:** ClickHouse raises CANNOT_UPDATE_COLUMN error. version is part of the dedup key.
  To refresh a row: re-insert with a new version = int(datetime.now().timestamp()).
**Affected tables:** All analysis.* tables, market.* tables.
