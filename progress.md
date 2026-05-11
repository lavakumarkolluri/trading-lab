# Trading Lab — Open Issues
**Last updated:** 2026-05-11
**Scope:** Active issues only. All Sprint 1–4 items (BUG-001–006, SEC-001–007, PERF-001/003–004, DATA-001–006, OPS-001–007) resolved and removed. Sprint 5 items (SEC-008, BUG-007/008, DATA-007/008, OPS-008–014) fixed 2026-05-11.

---

## Legend
| Severity | Meaning |
|----------|---------|
| 🔴 CRITICAL | System broken / data corrupted / exploitable |
| 🟠 HIGH | Wrong results / blocked pipeline / serious risk |
| 🟡 MEDIUM | Silent degradation / maintenance debt |
| 🟢 LOW | Code quality / minor inconsistency |

---

## PERFORMANCE

### PERF-002 🟡 ✅ FIXED — SIP XIRR O(n × months) dominant runtime
**File:** `pipeline/mf_compute_returns.py`
**Fix:** Compute XIRR once per unique calendar month using pre-built nav_map + binary search; forward-fill within month. ~20× speedup (10yr dataset: <1s vs ~20s). Fixed 2026-05-11.

---

## SUMMARY TABLE (Sprint 5 — all resolved 2026-05-11)

| ID | Severity | Status | Short Description |
|----|----------|--------|-------------------|
| PERF-002 | 🟡 | 🔲 OPEN | SIP XIRR O(n × months) — dominant runtime |
| SEC-008 | 🔴 | ✅ FIXED | SQL injection in options_eod_summary_pipeline — parameterized + symbol validation |
| BUG-007 | 🟠 | ✅ FIXED | Trailing stop peak clamped to TARGET_INR — removed clamp, tracks actual peak |
| BUG-008 | 🟡 | ✅ FIXED | Confidence scorer skips symbol silently — writes partial_failure to pipeline_runs |
| DATA-007 | 🟠 | ✅ FIXED | Missing FINAL on confidence_scorer ohlcv_daily + mf_db mf_nav reads |
| DATA-008 | 🟡 | ✅ FIXED | Zero nifty_spot silently inserted by vix_pipeline — logs warning, visible now |
| OPS-008 | 🟠 | ✅ FIXED | No recovery when scheduler misses EOD window — startup re-trigger within 4h |
| OPS-009 | 🟠 | ✅ FIXED | vix_pipeline unscheduled — added to job_option_chain_eod() daily sequence |
| OPS-010 | 🟠 | ✅ FIXED | Disk grows unbounded — migration 055 TTL on options_chain, MinIO/log cleanup job |
| OPS-011 | 🟡 | ✅ FIXED | No env var validation — _assert_env() added to intraday_monitor + option_chain_intraday |
| OPS-012 | 🟡 | ✅ FIXED | No healthcheck for long-running containers — heartbeat file + docker-compose healthcheck |
| OPS-013 | 🟡 | ✅ FIXED | NSE session refresh keeps stale session — failure streak counter + Telegram alert |
| OPS-014 | 🟢 | ✅ FIXED | No SIGTERM handler — signal.SIGTERM registered in intraday_monitor + option_chain_intraday |
