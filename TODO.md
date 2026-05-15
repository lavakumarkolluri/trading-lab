# Trading Lab — Prioritised Backlog
**Last updated:** 2026-05-15
**Process:** Every fix must have a failing test written first (TDD). Tests must run offline (mock DB). Push to `stage` — never `master` directly.
**Single source of truth** — progress.md removed; all status tracked here.

---

## Legend
| Severity | Meaning |
|----------|---------|
| 🔴 CRITICAL | Wrong predictions / data corruption / system broken |
| 🟠 HIGH | Serious model error / blocked pipeline / bad UX |
| 🟡 MEDIUM | Silent degradation / maintenance debt |
| 🟢 LOW | Code quality / minor inconsistency |

---

## 🔴 CRITICAL

### ~~CRIT-001~~ ✅ FIXED 2026-05-14 — Target variable ignores costs → model trains on wrong signal
**File:** `pipeline/confidence_scorer.py` → `build_dataset()`
**Fix:** `target = (net_pnl - txn_cost_pts) >= span_threshold_pts`. Net P&L after wing + txn + 1% SPAN.

---

### ~~CRIT-002~~ ✅ FIXED 2026-05-14 — Walk-forward CV splits on expiry_dt → lookahead bias
**File:** `pipeline/confidence_scorer.py` → `walk_forward_split()`
**Fix:** Split on `entry_date` not `expiry_dt`. No train-set row has `entry_date >= test_start`.

---

### ~~CRIT-003~~ ✅ FIXED 2026-05-15 — options_eod_summary NIFTY-only → wrong IV rank for all
**Files:** `pipeline/options_eod_summary_pipeline.py`, `pipeline/compute_historical_iv.py`
**Fix:** Migration 081 (ORDER BY now `(symbol, date, expiry)`). Backfill: NIFTY 1746, BANKNIFTY 1746, FINNIFTY 1243, MIDCPNIFTY 776 dates. `compute_historical_iv` rewritten for all 4 symbols. Dashboard eod_df query updated (stale NIFTY workaround removed).

---

### ~~CRIT-004~~ ✅ FIXED 2026-05-14 — INDEX_MAP wrong for FINNIFTY/MIDCPNIFTY
**File:** `pipeline/confidence_scorer.py` → `INDEX_MAP`
**Fix:** FINNIFTY → `^CNXFIN`, MIDCPNIFTY → `^NSMIDCP`.

---

### ~~CRIT-005~~ ✅ VERIFIED 2026-05-15 — data_freshness_check crashes on empty tables
**File:** `pipeline/data_freshness_check.py`
**Status:** Already guarded — `rows[0][0] if rows else None`. 21 tests pass.

---

### ~~CRIT-006~~ ✅ VERIFIED 2026-05-15 — Bhavcopy only 2 years → insufficient training data
**File:** `pipeline/option_chain_historical.py`
**Status:** `DEFAULT_FROM_DATE = date(2019, 1, 1)` already set. DB has 1746 trading days back to 2019-01-01 (8.9M rows).

---

## 🟠 HIGH

### ~~HIGH-001~~ ✅ VERIFIED 2026-05-15 — Feature transparency: silent zero fill
**File:** `dashboard/app.py`
**Status:** Already implemented — `missing_features()` function + ⚠️ badge expander in signal cards. `_warn_missing_features()` logs in confidence_scorer.

---

### ~~HIGH-002~~ ✅ VERIFIED 2026-05-15 — Dashboard 13 pages → overwhelming
**File:** `dashboard/app.py`
**Status:** Already consolidated to 5 pages: System Health, Model, Trade Log, Market Data, MF Advisor.

---

### HIGH-003 — SKIPPED (per user) — Iron fly P&L not used as training target
**File:** `pipeline/strategy_backtester.py`
**Note:** User decision to skip for now. Paper trading uses iron fly via `intraday_monitor`; backtest mismatch accepted.

---

### ~~HIGH-004~~ ✅ FIXED 2026-05-14 — MIN_TRAIN=25 too low for XGBoost
**Fix:** `MIN_TRAIN = 60`.

---

### HIGH-005 — No SPAN margin data → 1% threshold approximated
**Files:** `pipeline/confidence_scorer.py`, `pipeline/strategy_backtester.py`
**Issue:** SPAN approximated as `atm_premium × 2 × 0.30`. Could be 20-40% off actual SPAN.
**Fix (short):** Use `atm_iv × spot × sqrt(1/252) × lot_size × 3.0` (3-sigma VaR proxy).
**Fix (long):** Integrate actual NSE SPAN margin reports.

---

### HIGH-006 — intraday_monitor risk params not symbol-aware
**File:** `pipeline/intraday_monitor.py`
**Issue:** `TARGET_INR=2000, STOPLOSS_INR=1000` global. MIDCPNIFTY premium range differs from NIFTY.
**Fix:** Per-symbol dicts: `TARGET_INR = {"NIFTY": 2000, "BANKNIFTY": 3000, "FINNIFTY": 1500, "MIDCPNIFTY": 1000}`.
**TDD:** `test_target_stoploss_per_symbol()` — assert each symbol has distinct values.

---

### ~~HIGH-007~~ ✅ FIXED 2026-05-15 — MIDCPNIFTY missing from intraday_monitor
**Fix:** MIDCPNIFTY added to SYMBOLS, DEFAULT_LOT_SIZES (120), MIN_PREMIUM (30.0), WING_PTS (200.0).

---

## 🟡 MEDIUM

### MED-001 — Scheduler dependency gates fail-open
**File:** `pipeline/scheduler.py`
**Issue:** `job_confidence_scorer_daily()` continues even if `compute_oi_features` failed.
**Fix:** Return early if upstream dependency missing. Log clearly.

---

### MED-002 — No tests for strategy_backtester iron fly logic
**File:** `tests/test_strategy_backtester.py` (exists but limited)
**Fix:** Add iron fly P&L unit tests; assert `net_pnl = premium - wing_cost - txn_cost`.

---

### MED-003 — datetime.utcnow() deprecated across pipeline
**Files:** Multiple: `options_eod_summary_pipeline.py`, `pipeline_watchdog.py`, `scheduler.py`, `graduation_gate.py`
**Fix:** Replace all `datetime.utcnow()` → `datetime.now(timezone.utc)`.
**TDD:** Add `no_utcnow` grep assertion in `test_migrations.py` or new file.

---

### MED-004 — No model calibration curve in dashboard
**File:** `dashboard/app.py` → Model page
**Fix:** Add reliability diagram (confidence deciles vs actual win rate) to Model page. Query `analysis.confidence_backtest` for predicted vs actual.

---

### ~~MED-005~~ ✅ DONE 2026-05-15 — progress.md stale; duplicate tracking
**Fix:** Dropped progress.md. TODO.md is sole source of truth.

---

### MED-006 — GIT_SHA not injected into containers
**File:** `docker-compose.yml`
**Fix:** Add `GIT_SHA: ${GIT_SHA:-unknown}` to each service's environment block.

---

## 🟢 LOW

### LOW-001 — CI test timeout 10 min (fine for now)
422 tests run in ~4s offline. Revisit if integration tests are added.

### LOW-002 — Stale pipeline services in docker-compose
Services `breakout_backtest`, `gap_analyzer`, `pattern_feature_extractor` unused. Verify and remove or mark experimental.

### LOW-003 — No Telegram alert on UNRELIABLE SCORE
**Fix:** Call `_send_telegram()` when `_warn_missing_features()` detects zeros.

---

## Sprint 6 Completion Criteria (target 2026-05-20)

- [x] CRIT-001 ✅
- [x] CRIT-002 ✅
- [x] CRIT-003 ✅ (2026-05-15)
- [x] CRIT-004 ✅
- [x] CRIT-005 ✅ verified
- [x] CRIT-006 ✅ verified
- [x] HIGH-001 ✅ verified (feature badges already in dashboard)
- [x] HIGH-002 ✅ verified (5 pages already)
- [x] HIGH-004 ✅
- [x] HIGH-007 ✅ (2026-05-15)
- [x] 422 tests pass (2026-05-15)
- [ ] HIGH-005 (SPAN approximation improvement)
- [ ] HIGH-006 (symbol-aware risk params)
- [ ] MED-001 (scheduler fail-open)
- [ ] MED-003 (utcnow deprecation)
- [ ] MED-004 (calibration curve)
