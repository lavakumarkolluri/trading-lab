# Trading Lab — Prioritised Backlog
**Last updated:** 2026-05-15 (session 4)
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

### ~~HIGH-006~~ ✅ FIXED 2026-05-15 — intraday_monitor risk params now symbol-aware
**File:** `pipeline/intraday_monitor.py`
**Fix:** `TARGET_INR = {"NIFTY": 2000, "BANKNIFTY": 3000, "FINNIFTY": 1500, "MIDCPNIFTY": 1000}`, `STOPLOSS_INR = {"NIFTY": 1000, "BANKNIFTY": 1500, "FINNIFTY": 750, "MIDCPNIFTY": 500}`. All call sites updated to `.get(symbol, fallback)`. Tests: `test_target_stoploss_per_symbol`, `test_stoploss_half_of_target_per_symbol`.

---

### ~~HIGH-007~~ ✅ FIXED 2026-05-15 — MIDCPNIFTY missing from intraday_monitor
**Fix:** MIDCPNIFTY added to SYMBOLS, DEFAULT_LOT_SIZES (120), MIN_PREMIUM (30.0), WING_PTS (200.0).

---

## 🟡 MEDIUM

### ~~MED-001~~ ✅ VERIFIED 2026-05-15 — Scheduler dependency gates
**File:** `pipeline/scheduler.py`
**Status:** `_upstream_ok()` already blocks downstream when upstream fails; skips with "skipped" status and logs reason. Exception-path fail-open is intentional (avoids blocking when tracking DB is temporarily unavailable).

---

### ~~MED-002~~ ✅ FIXED 2026-05-15 — Extended backtester tests + N-DTE tests added
**File:** `tests/test_strategy_backtester.py`
**Fix:** Added N-DTE tests: `test_iron_fly_ndte_uses_exit_date_not_expiry`, `test_iron_fly_ndte_missing_exit_leg_returns_none`, `test_process_symbol_generates_ndte_rows`. 434 tests pass.

---

### ~~MED-003~~ ✅ FIXED 2026-05-15 — datetime.utcnow() deprecated
**Files:** `options_eod_summary_pipeline.py`, `compute_historical_iv.py`, `compute_oi_features.py`, `graduation_gate.py`, `confidence_scorer.py`, `strategy_backtester.py`, `scheduler.py`, `pipeline_watchdog.py`, `pipeline_utils.py`
**Fix:** Replaced all `datetime.utcnow()` with `datetime.now(timezone.utc)` (comparison contexts) or `int(time.time())` (version timestamp contexts). 424 tests pass, 0 deprecation warnings.

---

### ~~MED-004~~ ✅ VERIFIED 2026-05-15 — Calibration curve
**File:** `dashboard/app.py` → Model page → "Calibration" tab
**Status:** Already implemented — reliability diagram (confidence decile vs actual win rate) in `tab_calib` under the Model page walk-forward section.

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

---

## Daily Paper Trading Enablement (Sprint 7 — 2026-05-15)

### ~~DAILY-001~~ ✅ FIXED 2026-05-15 — MIDCPNIFTY missing from intraday scraper
**File:** `pipeline/option_chain_intraday.py` line 47
**Fix:** Added `"MIDCPNIFTY"` to `SYMBOLS`. Enables Monday data collection → Monday paper trades.

---

### ~~DAILY-002~~ ✅ FIXED 2026-05-15 — No net premium gate (wing cost ignored)
**File:** `pipeline/intraday_monitor.py` `record_entry()`
**Fix:** Added `MIN_NET_CREDIT` dict and gate: skip if `net_premium < MIN_NET_CREDIT[symbol]`. Protects against entering high-DTE trades where wings cost more than straddle.

---

### ~~DAILY-003~~ ✅ FIXED 2026-05-15 — Backtester only generates 0DTE trades
**File:** `pipeline/strategy_backtester.py` `process_symbol()`
**Fix:** Extended to N-DTE loop (MAX_DTE=5). All `compute_*` functions accept `exit_date` param. Migration 082 adds `entry_date` to `spread_backtest` ORDER BY.

---

### ~~DAILY-004~~ ✅ FIXED 2026-05-15 — DTE not a feature in confidence model
**File:** `pipeline/confidence_scorer.py`
**Fix:** `build_dataset()` iterates all (expiry, entry_date) pairs; adds `dte` column. `FEATURE_COLS` includes `"dte"`. `score_today()` populates `dte = (next_expiry − latest_snap).days`.

---

### ~~DAILY-005~~ ✅ FIXED 2026-05-15 — No cumulative P&L visibility
**File:** `dashboard/app.py` Trade Log page
**Fix:** Added 3 tabs: Cumulative P&L curve (per symbol), DTE win-rate (paper trades), N-DTE backtest win-rate pivot + bar chart from `analysis.spread_backtest`.

---

### DAILY-006 — Retrain confidence_scorer after N-DTE backtest runs
**Status:** Pending — backtester is running (N-DTE expansion). After completion:
```bash
docker compose run --rm pipeline python confidence_scorer.py --compare
```
Then rebuild intraday_monitor image and restart.

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
- [x] 434 tests pass, 0 deprecation warnings (2026-05-15, session 4)
- [x] HIGH-006 ✅ symbol-aware risk params
- [x] MED-001 ✅ verified — gates already in place
- [x] MED-003 ✅ utcnow swept across all active pipeline files
- [x] MED-004 ✅ verified — calibration tab already in Model page
- [ ] HIGH-005 (SPAN approximation improvement — long term)
