# Trading Lab — Prioritised Backlog
**Last updated:** 2026-05-15 (session 2)
**Process:** Every fix must have a failing test written first (TDD). Tests must run offline (mock DB). Push to `stage` — never `master` directly.

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
**Bug:** `target = (pnl_pts > 0)` labels any positive point gain as WIN, even when it is a loss after iron fly wing costs + transaction costs + 1% SPAN threshold.
**Impact:** Model learns a corrupt signal. High confidence score ≠ real-world profitability.
**Fix:**
1. Compute `net_pnl = straddle_pnl_pts - wing_cost_pts` (already in strategy_backtester output)
2. Compute `txn_cost_pts = txn_cost_inr / lot_size`
3. Compute `span_threshold_pts = (atm_premium * 2 * 0.30) * 0.01` (1% SPAN approximation)
4. `target = (net_pnl - txn_cost_pts) >= span_threshold_pts`
**TDD:** Write `test_target_variable_uses_net_pnl()` that asserts a 2pt straddle gain with 5pt wing cost is labeled LOSS.

---

### ~~CRIT-002~~ ✅ FIXED 2026-05-14 — Walk-forward CV splits on expiry_dt → train/test contamination (lookahead bias)
**File:** `pipeline/confidence_scorer.py` → `walk_forward_split()`
**Bug:** Folds split by `expiry_dt`. A trade entered on 2023-12-20 with expiry 2023-12-28 could be in "train" even though its features were extracted after the test fold's start date.
**Impact:** Model sees future data during training. OOS AUC is optimistic; live performance will be worse.
**Fix:** Use `entry_date` as the split boundary. All rows where `entry_date >= fold_start` go to test.
**TDD:** Write `test_walk_forward_no_future_leak()`: create synthetic rows where entry_date and expiry_dt differ; assert no train-set row has entry_date >= test_start.

---

### ~~CRIT-003~~ ✅ FIXED 2026-05-15 — options_eod_summary is NIFTY-only; used for all symbols → silent wrong IV rank
**File:** `pipeline/confidence_scorer.py` → `load_eod_summary()`
**File:** `pipeline/options_eod_summary_pipeline.py`
**File:** `pipeline/compute_historical_iv.py`
**Bug:** `market.options_eod_summary` only contained NIFTY rows. IV rank, IV percentile, PCR features were 0 for BANKNIFTY/FINNIFTY/MIDCPNIFTY.
**Fix applied 2026-05-15:**
- `options_eod_summary_pipeline.py`: Added groupby dedup fix for intraday duplicate strikes; backfilled BANKNIFTY (1746 dates), FINNIFTY (1243 dates), MIDCPNIFTY (40 dates) from 2019-01-01
- `compute_historical_iv.py`: Rewrote to support all 4 symbols using put-call parity spot estimation; per-symbol `run_symbol()` loops
- `confidence_scorer --score-only`: Re-run after IV backfill to get real IV-based scores
**TDD:** Write `test_eod_summary_per_symbol()`: mock DB returning only NIFTY rows; assert BANKNIFTY load returns empty/flagged result.

---

### ~~CRIT-004~~ ✅ FIXED 2026-05-14 — INDEX_MAP wrong for FINNIFTY and MIDCPNIFTY → wrong technical signals
**File:** `pipeline/confidence_scorer.py` → `INDEX_MAP`
**Bug:** `INDEX_MAP = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "^NSEI", "MIDCPNIFTY": "^NSEI"}`
FINNIFTY and MIDCPNIFTY both use `"^NSEI"` (NIFTY 50) for ATR, RSI, supertrend, CPR features.
**Impact:** FINNIFTY and MIDCPNIFTY technical signals are NIFTY signals — meaningless for those symbols.
**Fix:** Map to correct indices. FINNIFTY → `"^CNXFIN"` (Nifty Financial Services), MIDCPNIFTY → `"^NSMIDCP"` (Nifty MidCap 150). Verify ticker availability in yfinance before hardcoding.
**TDD:** Write `test_index_map_symbols()`: assert INDEX_MAP has 4 distinct values, none repeated.

---

### ~~CRIT-005~~ ✅ FIXED (already implemented) — data_freshness_check.py crashes on empty tables (IndexError)
**File:** `pipeline/data_freshness_check.py` → `_check_mf_nav()`, `_check_vix()`
**Fix verified 2026-05-15:** `_check_mf_nav` and `_check_vix` already guard `result_rows` with `rows[0][0] if rows else None`. Tests `test_check_mf_nav_empty_table_returns_empty` and `test_check_vix_empty_table_returns_empty` pass.

---

### CRIT-006 — Historical bhavcopy only 2 years → model trained on insufficient data
**File:** `pipeline/option_chain_historical.py`
**Bug:** `LOOKBACK_DAYS = 730` — downloads only 2 years. Confidence scorer has 374 NIFTY rows (covering ~2 years of weekly expiries). Model has not seen 2019/2020 COVID crash, 2022 rate-hike bear market.
**Impact:** Model cannot generalise across market regimes. 1000+ additional rows (2019-2023) would improve AUC.
**Fix:** Change default from `2019-01-01` (or `LOOKBACK_DAYS` computed from today). Add `--from 2019-01-01` to the docker compose default command. NSE bhavcopy old format covers back to ~2010.
**TDD:** Write `test_historical_date_range_from_2019()`: assert `parse_args(["--from", "2019-01-01"]).from_date <= date(2019, 1, 1)`.

---

## 🟠 HIGH

### HIGH-001 — Feature transparency: silent NaN/zero fill → model uses bad features without warning
**File:** `pipeline/confidence_scorer.py` → `build_row()`
**Bug:** Missing features (IV rank=0, VIX=0, ATR=0) are filled silently. The confidence score is computed without any flag that key inputs were missing.
**Impact:** Dashboard shows "confidence=60" with no indication that 3 of 8 features were imputed.
**Fix:** After building each feature row, compute `missing_features = [k for k, v in row.items() if v == 0 and k in CRITICAL_FEATURES]`. Store in `analysis.scorecard` as `missing_features_json`. Dashboard renders a warning badge if non-empty.
**TDD:** Write `test_missing_features_flagged()`: build a row with IV rank=0, VIX=0; assert `missing_features` list is non-empty.

---

### HIGH-002 — Dashboard: 13 pages overwhelming; no primary confidence view
**File:** `dashboard/app.py`
**Issue:** 13 nav pages (Overview, Today's Signals, Confidence Scores, Strategy Backtests, Trade Log, Market Pulse, Live Trading, Paper Trades, Breakout Backtest, Data Freshness, Fundamentals, Edge Analysis, Weekend Theta) with no clear entry point for a trader deciding whether to enter tomorrow.
**Fix:** Consolidate to ~6 focused pages:
  1. **Trade Signal** (was "Today's Signals" + "Confidence Scores") — primary page; shows go/no-go per symbol, confidence, feature health badges
  2. **Model Health** (was "Strategy Backtests" + "Edge Analysis") — walk-forward AUC chart, calibration curve, regime breakdown
  3. **Paper Trades** (unchanged) — live P&L, current positions
  4. **Market Data** (was "Market Pulse" + "Data Freshness") — data staleness, pipeline status
  5. **History** (was "Trade Log" + "Breakout Backtest") — completed trades, P&L distribution
  6. **Settings / Admin** — raw data explorer, fundamentals, weekend theta

---

### HIGH-003 — Iron fly P&L not used as training target in strategy_backtester
**File:** `pipeline/strategy_backtester.py`
**Bug:** `strategy_backtester.py` tests Iron Condor, Bull Put Spread, Bear Call Spread — NOT iron fly. The confidence_scorer trains on the output of strategy_backtester. Mismatch between what we backtest and what we trade.
**Fix:** Add iron fly backtest as primary strategy output. `pnl = straddle_premium_collected - wing_cost - txn_cost`. Only then feed this into confidence_scorer build_dataset.

---

### ~~HIGH-004~~ ✅ FIXED 2026-05-14 — MIN_TRAIN=25 too low for XGBoost
**File:** `pipeline/confidence_scorer.py`
**Bug:** `MIN_TRAIN = 25`. XGBoost with 25 training rows will overfit severely.
**Fix:** `MIN_TRAIN = 60`. With weekly expiries since 2019 → ~300 rows per symbol. 60 minimum gives meaningful first folds.

---

### HIGH-005 — No SPAN margin data → 1% threshold is approximated
**Files:** `pipeline/confidence_scorer.py`, `pipeline/strategy_backtester.py`
**Issue:** SPAN margin calculation requires lot sizes, IV, and NSE SPAN parameters. Currently approximated as `atm_premium × 2 × 0.30`. This could be 20-40% off actual SPAN.
**Fix (short term):** Use NSE's published margin calculator API or store estimated SPAN = `atm_iv × spot × sqrt(1/252) × lot_size × 3.0` (3-sigma intraday VaR proxy).
**Fix (long term):** Integrate actual SPAN data from NSE margin reports.

---

### HIGH-006 — intraday_monitor hardcodes risk params; not symbol-aware
**File:** `pipeline/intraday_monitor.py`
**Bug:** `TARGET_INR=2000, STOPLOSS_INR=1000` are global constants. A MIDCPNIFTY trade (different lot size, different premium range) uses the same ₹ amounts as NIFTY.
**Fix:** Make per-symbol: `TARGET_INR = {"NIFTY": 2000, "BANKNIFTY": 3000, "FINNIFTY": 1500, "MIDCPNIFTY": 1000}` based on typical premium × lot_size ranges.

---

### ~~HIGH-007~~ ✅ FIXED 2026-05-15 — MIDCPNIFTY missing from intraday_monitor
**File:** `pipeline/intraday_monitor.py`
**Fix applied:** Added MIDCPNIFTY to SYMBOLS, DEFAULT_LOT_SIZES (120), MIN_PREMIUM (30.0), WING_PTS (200.0).

---

## 🟡 MEDIUM

### MED-001 — Scheduler dependency gates fail-open
**File:** `pipeline/scheduler.py`
**Issue:** `job_confidence_scorer_daily()` runs even if `compute_oi_features` failed. The preflight `_ran_today()` check logs a warning but continues.
**Fix:** Return early if upstream dependency failed. Log clearly which dependency was missing.

---

### MED-002 — No tests for strategy_backtester, compute_oi_features (iron fly logic)
**Files:** `tests/` (missing files)
**Issue:** No test coverage for the iron fly P&L calculation, OI feature computation, or walk-forward CV temporal isolation.
**Fix:** Add `tests/test_strategy_backtester.py` with iron fly P&L unit tests and `test_walk_forward_no_future_leak`.

---

### MED-003 — datetime.utcnow() deprecated; use timezone-aware datetime throughout
**Files:** Multiple pipeline files
**Bug:** `datetime.utcnow()` returns a timezone-naive datetime. Python 3.12+ emits DeprecationWarning. Comparisons with timezone-aware values can silently compute wrong deltas.
**Fix:** Replace all `datetime.utcnow()` → `datetime.now(timezone.utc)`. Add `from datetime import timezone`.

---

### MED-004 — No model calibration visibility in dashboard
**File:** `dashboard/app.py`
**Issue:** Dashboard shows AUC but not calibration. A model with AUC=0.60 and perfect calibration is very different from AUC=0.60 with extreme over/underconfidence.
**Fix:** Add calibration curve (reliability diagram) to Model Health page: bucket predicted confidence into deciles, plot actual win rate vs predicted.

---

### MED-005 — progress.md and README.md stale
**Files:** `progress.md`, `README.md`
**Issue:** progress.md references Sprint 5 (2026-05-11). README likely outdates instruments, strategy.
**Fix:** Update progress.md to reflect current sprint (Sprint 6). Update README architecture section.

---

### MED-006 — GIT_SHA env var not injected into containers
**File:** `docker-compose.yml`
**Issue:** No `GIT_SHA` variable set, so pipeline logs can't self-identify which code version ran. Makes debugging regressions harder.
**Fix:** Add `GIT_SHA: ${GIT_SHA:-unknown}` to each service's environment block. Set in CI before `docker compose build`.

---

### MED-007 — No test for historical bhavcopy date range (2019 coverage)
**File:** `tests/` (missing)
**Issue:** option_chain_historical.py will be changed to fetch from 2019. No regression guard if the argument parsing or date logic breaks.
**Fix:** Add `tests/test_option_chain_historical.py` with date-range and URL-format selection tests.

---

## 🟢 LOW

### LOW-001 — CI test timeout 10 minutes may be tight as test suite grows
**File:** `.github/workflows/` (CI config)
**Note:** 141 tests run in ~2s offline. The 10-minute timeout is fine for now but will need revisiting if integration-style tests (with real Docker) are added.

### LOW-002 — Stale pipelins never cleaned up from docker-compose (breakout_backtest, gap_analyzer, pattern_*)
**File:** `docker-compose.yml`
**Note:** Several services (breakout_backtest, gap_analyzer, pattern_feature_extractor) appear unused in the active options-trading pipeline. Verify and remove or clearly mark as experimental.

### LOW-003 — No alert when confidence scorer produces UNRELIABLE SCORE
**File:** `pipeline/confidence_scorer.py`
**Issue:** UNRELIABLE SCORE is logged but no Telegram alert sent. Trader may not notice.
**Fix:** Call `_send_telegram()` when UNRELIABLE SCORE is detected.

---

## Completion Criteria (Sprint 6 — target 2026-05-20)

A "done" state means:
- [x] CRIT-001 ✅ fixed
- [x] CRIT-002 ✅ fixed
- [x] CRIT-003 ✅ fixed (2026-05-15) — all 4 symbols have IV rank/PCR
- [x] CRIT-004 ✅ fixed
- [x] CRIT-005 ✅ verified — empty table guard already in place, 21 tests pass
- [ ] CRIT-006 (bhavcopy 2019 — DONE, row count needs verify)
- [ ] HIGH-001 (feature transparency badges in dashboard)
- [ ] HIGH-002 (dashboard consolidated to 6 pages)
- [ ] HIGH-003 (iron fly backtest as primary output)
- [x] HIGH-004 ✅ fixed
- [x] HIGH-007 ✅ fixed (2026-05-15 — MIDCPNIFTY added to intraday_monitor)
- [ ] Dashboard: calibration curve visible on Model Health page
- [x] All tests pass (422 tests, 0 failures — 2026-05-15)
