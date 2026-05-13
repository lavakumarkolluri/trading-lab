# Trading Lab — Open Issues
**Last updated:** 2026-05-13
**Scope:** Active issues only. All Sprint 1–5 items resolved and removed (see git history).

---

## Legend
| Severity | Meaning |
|----------|---------|
| 🔴 CRITICAL | System broken / data corrupted / wrong model signal |
| 🟠 HIGH | Wrong results / blocked pipeline / serious risk |
| 🟡 MEDIUM | Silent degradation / maintenance debt |
| 🟢 LOW | Code quality / minor inconsistency |

---

## Sprint 6 — Active (2026-05-13 →)

Comprehensive audit completed 2026-05-13. Full prioritised backlog in `TODO.md`. Items below are actively in-flight or blocked.

### CRIT-005 🔴 ✅ FIXED — data_freshness_check.py crashes on empty tables (IndexError)
**File:** `pipeline/data_freshness_check.py`
**Fix:** Guard `result_rows[0]` with length check; treat empty result as `last_date = None`.

### CRIT-006 🔴 IN PROGRESS — Historical bhavcopy only 2 years; needs 2019
**File:** `pipeline/option_chain_historical.py`
**Fix:** Change `LOOKBACK_DAYS` default and `--from` argument default to `2019-01-01`.

---

## SUMMARY TABLE

| ID | Severity | Status | Short Description |
|----|----------|--------|-------------------|
| CRIT-001 | 🔴 | 🔲 OPEN | Target variable ignores iron fly costs → wrong model signal |
| CRIT-002 | 🔴 | 🔲 OPEN | Walk-forward splits on expiry_dt → lookahead bias |
| CRIT-003 | 🔴 | 🔲 OPEN | options_eod_summary NIFTY-only; used for all symbols |
| CRIT-004 | 🔴 | 🔲 OPEN | INDEX_MAP wrong for FINNIFTY/MIDCPNIFTY |
| CRIT-005 | 🔴 | ✅ FIXED | data_freshness_check IndexError on empty tables |
| CRIT-006 | 🔴 | 🔲 OPEN | Bhavcopy only 2 years; need from 2019 |
| HIGH-001 | 🟠 | 🔲 OPEN | Feature transparency — silent NaN/zero fill |
| HIGH-002 | 🟠 | 🔲 OPEN | Dashboard 13 pages → need consolidation to 6 |
| HIGH-003 | 🟠 | 🔲 OPEN | Iron fly P&L not used in strategy_backtester |
| HIGH-004 | 🟠 | 🔲 OPEN | MIN_TRAIN=25 too low for XGBoost |
| HIGH-005 | 🟠 | 🔲 OPEN | No actual SPAN margin data |
| HIGH-006 | 🟠 | 🔲 OPEN | Risk params not symbol-aware |
| HIGH-007 | 🟠 | 🔲 OPEN | MIDCPNIFTY missing from intraday_monitor |
| MED-001 | 🟡 | 🔲 OPEN | Scheduler dependency gates fail-open |
| MED-002 | 🟡 | 🔲 OPEN | No tests for strategy_backtester / walk-forward CV |
| MED-003 | 🟡 | 🔲 OPEN | datetime.utcnow() deprecated |
| MED-004 | 🟡 | 🔲 OPEN | No calibration curve in dashboard |
| MED-005 | 🟡 | ✅ FIXED | progress.md and README.md stale |
| MED-006 | 🟢 | 🔲 OPEN | GIT_SHA not injected in containers |
| LOW-001 | 🟢 | 🔲 OPEN | CI timeout 10 min |
| LOW-002 | 🟢 | 🔲 OPEN | Stale pipeline services in docker-compose |
| LOW-003 | 🟢 | 🔲 OPEN | No Telegram alert on UNRELIABLE SCORE |

---

## Sprint 5 — Resolved 2026-05-11

All items (SEC-008, BUG-007/008, DATA-007/008, OPS-008–014) fixed. See git log around 2026-05-11.

## Sprints 1–4 — Resolved

All items (BUG-001–006, SEC-001–007, PERF-001/003–004, DATA-001–006, OPS-001–007) resolved. See git log.
