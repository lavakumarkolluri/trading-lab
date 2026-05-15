# Trading Lab — Open Issues
**Last updated:** 2026-05-15
**Scope:** Active issues only. Resolved items moved to SUMMARY TABLE below.

---

## Legend
| Severity | Meaning |
|----------|---------|
| 🔴 CRITICAL | System broken / data corrupted / wrong model signal |
| 🟠 HIGH | Wrong results / blocked pipeline / serious risk |
| 🟡 MEDIUM | Silent degradation / maintenance debt |
| 🟢 LOW | Code quality / minor inconsistency |

---

## Sprint 6 — Active (2026-05-13 → 2026-05-20)

### CRIT-006 🔴 IN PROGRESS — Historical bhavcopy only 2 years; needs 2019
**File:** `pipeline/option_chain_historical.py`
**Fix:** Change `LOOKBACK_DAYS` default and `--from` argument default to `2019-01-01`. Backfill verified for NIFTY; other symbols may need a re-run. Row counts need verification.

### HIGH-001 🟠 OPEN — Feature transparency: silent NaN/zero fill
**File:** `pipeline/confidence_scorer.py` → `build_row()`
**Fix:** After building each feature row, compute `missing_features`. Store in `analysis.scorecard`. Dashboard renders warning badge if non-empty.

### HIGH-002 🟠 OPEN — Dashboard: 13 pages, no primary confidence view
**File:** `dashboard/app.py`
**Fix:** Consolidate to 6 focused pages: Trade Signal, Model Health, Paper Trades, Market Data, History, Settings/Admin.

### HIGH-003 🟠 OPEN — Iron fly P&L not used as training target in strategy_backtester
**File:** `pipeline/strategy_backtester.py`
**Fix:** Add iron fly backtest as primary strategy output. `pnl = straddle_premium_collected - wing_cost - txn_cost`. Feed into confidence_scorer.

---

## SUMMARY TABLE

| ID | Severity | Status | Short Description |
|----|----------|--------|-------------------|
| CRIT-001 | 🔴 | ✅ FIXED 2026-05-14 | Target variable uses net P&L after costs |
| CRIT-002 | 🔴 | ✅ FIXED 2026-05-14 | Walk-forward splits on entry_date (no lookahead) |
| CRIT-003 | 🔴 | ✅ FIXED 2026-05-15 | All 4 symbols have IV rank/PCR; migration 081 |
| CRIT-004 | 🔴 | ✅ FIXED 2026-05-14 | INDEX_MAP uses correct indices per symbol |
| CRIT-005 | 🔴 | ✅ FIXED (verified 2026-05-15) | data_freshness_check guards empty result_rows |
| CRIT-006 | 🔴 | 🔲 OPEN | Bhavcopy only 2 years; need from 2019 |
| HIGH-001 | 🟠 | 🔲 OPEN | Feature transparency — silent NaN/zero fill |
| HIGH-002 | 🟠 | 🔲 OPEN | Dashboard 13 pages → consolidate to 6 |
| HIGH-003 | 🟠 | 🔲 OPEN | Iron fly P&L not backtest target |
| HIGH-004 | 🟠 | ✅ FIXED 2026-05-14 | MIN_TRAIN raised to 60 |
| HIGH-005 | 🟠 | 🔲 OPEN | No actual SPAN margin data |
| HIGH-006 | 🟠 | 🔲 OPEN | Risk params not symbol-aware |
| HIGH-007 | 🟠 | ✅ FIXED 2026-05-15 | MIDCPNIFTY added to intraday_monitor |
| MED-001 | 🟡 | 🔲 OPEN | Scheduler dependency gates fail-open |
| MED-002 | 🟡 | 🔲 OPEN | No tests for strategy_backtester walk-forward CV |
| MED-003 | 🟡 | 🔲 OPEN | datetime.utcnow() deprecated across pipeline |
| MED-004 | 🟡 | 🔲 OPEN | No calibration curve in dashboard |
| MED-005 | 🟡 | ✅ FIXED 2026-05-15 | progress.md updated |
| MED-006 | 🟢 | 🔲 OPEN | GIT_SHA not injected in containers |
| LOW-001 | 🟢 | 🔲 OPEN | CI timeout 10 min (fine for now) |
| LOW-002 | 🟢 | 🔲 OPEN | Stale pipeline services in docker-compose |
| LOW-003 | 🟢 | 🔲 OPEN | No Telegram alert on UNRELIABLE SCORE |

---

## Sprint 5 — Resolved 2026-05-11

All items (SEC-008, BUG-007/008, DATA-007/008, OPS-008–014) fixed. See git log around 2026-05-11.

## Sprints 1–4 — Resolved

All items (BUG-001–006, SEC-001–007, PERF-001/003–004, DATA-001–006, OPS-001–007) resolved. See git log.
