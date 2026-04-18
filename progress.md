# Trading Lab — Issue Tracker & Audit Report
**Generated:** 2026-04-18
**Sprint 1 fixed:** 2026-04-18
**Sprint 2 fixed:** 2026-04-18
**Review scope:** Full codebase audit — bugs, security, performance, data integrity, maintenance

---

## Legend
| Severity | Meaning |
|----------|---------|
| 🔴 CRITICAL | System broken / data corrupted / exploitable |
| 🟠 HIGH | Wrong results / blocked pipeline / serious risk |
| 🟡 MEDIUM | Silent degradation / maintenance debt |
| 🟢 LOW | Code quality / minor inconsistency |

| Status | Meaning |
|--------|---------|
| ✅ FIXED | Applied to codebase and committed |
| 🔲 OPEN | Not yet fixed |

---

## BUGS

### BUG-001 🔴 ✅ FIXED meta_pipeline.py — Docker CLI not installed in pipeline image
**File:** `pipeline/Dockerfile`, `pipeline/meta_pipeline.py`
**Evidence:** `workspace/logs/meta_pipeline.log` and `meta_pipeline_weekly.log` both show:
```
✗ Step 1 FAILED — docker compose not found.
```
**Root cause:** `meta_pipeline.py` calls `docker compose run --rm <service>` via subprocess, but the `pipeline` Docker image is built from `python:3.12-slim` which has no Docker CLI. The `docker.sock` is mounted in `docker-compose.yml` but the CLI binary itself is absent.
**Fix applied (Sprint 1):** Dockerfile installs `docker-ce-cli`; COMPOSE_CMD uses `-f /trading-lab/docker-compose.yml --project-directory`; docker-compose.yml mounts `.:/trading-lab:ro` and sets `COMPOSE_FILE` env var.

---

### BUG-002 🟡 🔲 OPEN Migration 025 is an exact duplicate of migration 024
**File:** `clickhouse/migrations/025_recreate_market_mf_nav_enriched.sql`
**Root cause:** File is byte-for-byte identical to migration 024 — same comment header `-- Migration : 024`, same CREATE TABLE. The filename says "recreate" but `CREATE TABLE IF NOT EXISTS` makes it a no-op.
**Impact:** Whatever schema change was intended was never applied. The migration history is misleading.
**Fix (Sprint 3):** If no change was needed, rename to clearly document it was intentional. If a DROP+RECREATE was intended, it must be a new properly-written migration.

---

### BUG-003 🟠 ✅ FIXED pattern_builder.py — total_matches overwritten with per-run count, not cumulative
**File:** `pipeline/pattern_builder.py`, function `run_pattern_matching`
**Root cause:** In incremental mode, `len(match_rows)` is only the *new* matches found in this run. This overwrites the previously accumulated `total_matches` count in `analysis.patterns`.
**Impact:** `total_matches` in the patterns table is always wrong after the first incremental run. Star ratings and backtests use wrong sample sizes.
**Fix applied (Sprint 2):** Replaced `len(match_rows)` with a post-insert `SELECT count() FROM analysis.pattern_event_map FINAL WHERE pattern_id = '{pid}'` to get the true cumulative count.

---

### BUG-004 🟠 🔲 OPEN backtest_engine.py — update_pattern_stats uses ALTER TABLE UPDATE on ReplacingMergeTree
**File:** `pipeline/backtest_engine.py`, function `update_pattern_stats`
**Root cause:** Decision 026 explicitly forbids ALTER TABLE UPDATE on ReplacingMergeTree tables. While this doesn't touch `version` directly, ALTER UPDATE mutations can conflict with deduplication merges.
**Fix (Sprint 3):** Re-insert the full patterns row with `version = int(datetime.now().timestamp())` instead of using ALTER UPDATE.

---

### BUG-005 🟡 🔲 OPEN mf_compute_returns.py — XIRR Newton-Raphson domain failure
**File:** `pipeline/mf_compute_returns.py`, function `_xirr_newton`
**Root cause:** Global `warnings.filterwarnings("ignore", category=RuntimeWarning)` suppresses all RuntimeWarnings in the module, masking legitimate issues. The Newton-Raphson solver can iterate into invalid domains with fractional time periods.
**Fix (Sprint 4):** Remove global suppression; use `np.errstate` for the specific operation; add domain guard `rate > -1` before each Newton step.

---

### BUG-006 🟢 🔲 OPEN prediction_engine.py — VIX NaN guard is opaque
**File:** `pipeline/prediction_engine.py`
**Code:** `v == v` is a NaN check that also doesn't guard against `float('inf')`.
**Fix (Sprint 4):** Replace with `math.isfinite(v)`.

---

## SECURITY

### SEC-001 🔴 ✅ FIXED SQL Injection — backtest_engine.py
**File:** `pipeline/backtest_engine.py`, function `update_pattern_stats`
**Fix applied (Sprint 1):** `pattern_id` validated against `^[A-Za-z0-9_\-]{1,50}$`; all numeric stats cast to `int()`/`float()` before f-string interpolation.

---

### SEC-002 🔴 ✅ FIXED SQL Injection — pattern_builder.py
**File:** `pipeline/pattern_builder.py`, function `run_pattern_matching`
**Fix applied (Sprint 2):** `_validate_id(pid, "pattern_id")` called before ALTER UPDATE; `len(match_rows)` cast to `int()` before interpolation.

---

### SEC-003 🔴 ✅ FIXED SQL Injection — validation_engine.py
**File:** `pipeline/validation_engine.py`, function `update_prediction_status`
**Fix applied (Sprint 2):** `status` validated against `frozenset({"hit","miss","expired"})`; `pattern_id` and `symbol` validated against allowlist regex; `prediction_date` coerced via `str()`.

---

### SEC-004 🔴 ✅ FIXED SQL Injection — pattern_feature_extractor.py
**File:** `pipeline/pattern_feature_extractor.py`, function `mark_events_processed`
**Fix applied (Sprint 2):** `symbol` validated against bounded regex; each element in `dates` type-asserted as Python `date` object before interpolation.

---

### SEC-005 🟡 🔲 OPEN SQL Injection — cleanup_schemes.py (low risk, integers only)
**Fix (Sprint 4):** Parameterize or use typed integer list.

---

### SEC-006 🟡 🔲 OPEN ClickHouse allows broad 172.16.0.0/12 network
**Fix (Sprint 4):** Restrict to specific Docker network CIDR.

---

### SEC-007 🟡 🔲 OPEN participant_oi uses network_mode: host
**Fix (Sprint 4):** Move to standard Docker network.

---

## PERFORMANCE

### PERF-001 🟠 🔲 OPEN One ALTER UPDATE mutation per pattern per backtest run
**Fix (Sprint 3):** Re-insert full pattern rows instead of ALTER UPDATE.

### PERF-002 🟡 🔲 OPEN SIP XIRR O(n × months) dominant runtime
**Fix (Sprint 4):** Pre-compute monthly, forward-fill within month.

### PERF-003 🟡 🔲 OPEN cleanup_schemes mutations_sync=1 can timeout
**Fix (Sprint 4):** Switch to mutations_sync=0 with separate completion check.

### PERF-004 🟡 🔲 OPEN migrate.py single command() silently drops multi-statement files
**Fix (Sprint 3):** Split SQL on `;\n` and execute each statement separately.

---

## DATA INTEGRITY

### DATA-001 🟠 ✅ FIXED Entry price uses prediction_date close — forward-look bias
**File:** `pipeline/validation_engine.py`, function `compute_actual_returns`
**Fix applied (Sprint 2):** `entry_price` now uses `open` on `target_date`; `fetch_ohlcv_window` fetches the `open` column; `compute_actual_returns` takes `target_date` parameter.

---

### DATA-002 🟠 ✅ FIXED win_rate in star ratings ≠ win_rate_1d in patterns (threshold mismatch)
**File:** `pipeline/star_rater.py`, function `fetch_positive_win_rates`
**Fix applied (Sprint 2):** Renamed to `fetch_hit_rates`; query changed from `countIf(return_1d > 0)` to `countIf(hit_2pct_1d = 1)` to align with the same 2% threshold backtest_engine uses.

---

### DATA-003 🟠 ✅ FIXED P005 permanently zero matches until fii_dii seeded
**Fix applied (Sprint 2):** `check_fii_dii_seeded(ch)` preflight added to both `pattern_builder.py` and `pattern_feature_extractor.py`. Logs a clear warning at startup when `market.fii_dii` is empty. OPS-006 fix ensures fii_dii_pipeline now runs daily.

---

### DATA-004 🟡 🔲 OPEN fii_dii_pipeline has no validation of NSE API values
**Fix (Sprint 4):** Add bounds checking on parsed values.

### DATA-005 🟡 🔲 OPEN gap_analyzer.py missing — closed-loop feedback never runs
**Fix (Sprint 3):** Implement gap_analyzer.py.

### DATA-006 🟡 🔲 OPEN Partial migration executes as success (see PERF-004)
**Fix (Sprint 3):** Same fix as PERF-004.

---

## MAINTENANCE / OPERATIONAL

### OPS-001 🔴 🔲 OPEN No cron/scheduler — pipeline never auto-runs
**Note:** Original fix proposed a host cron job (`setup_cron.sh`). Rejected — policy is nothing runs on host.
**Fix (Sprint 3):** Implement a scheduler service in docker-compose.yml that runs meta_pipeline on a cron schedule inside Docker.

---

### OPS-002 🟠 🔲 OPEN gap_analyzer.py absent from Dockerfile and codebase
**Fix (Sprint 3):** Implement file; add COPY to Dockerfile.

### OPS-003 🟡 🔲 OPEN No failure alerting mechanism
**Fix (Sprint 4):** Add Telegram bot notification to meta_pipeline.py.

### OPS-004 🟡 🔲 OPEN Holiday cache never invalidated in long-running processes
**Fix (Sprint 4):** Add 24h TTL to `_cache_loaded` check.

### OPS-005 🟡 ✅ FIXED participant_oi_pipeline not in meta_pipeline steps
**Fix applied (Sprint 1):** Added as Step 3 (soft_fail=True) in DAILY_STEPS.

### OPS-006 🟡 ✅ FIXED fii_dii_pipeline not in meta_pipeline steps
**Fix applied (Sprint 1):** Added as Step 2 (soft_fail=True) in DAILY_STEPS.

### OPS-007 🟢 ✅ FIXED progress.md outdated since 2026-03-04
**Fix applied (Sprint 1/2):** This file.

---

## SUMMARY TABLE

| ID | Severity | Status | Short Description |
|----|----------|--------|-------------------|
| BUG-001 | 🔴 | ✅ FIXED | meta_pipeline fails — docker CLI missing in image |
| BUG-002 | 🟡 | 🔲 OPEN | Migration 025 is exact duplicate of 024 |
| BUG-003 | 🟠 | ✅ FIXED | pattern_builder overwrites total_matches with per-run count |
| BUG-004 | 🟠 | 🔲 OPEN | backtest_engine uses ALTER UPDATE on ReplacingMergeTree |
| BUG-005 | 🟡 | 🔲 OPEN | XIRR Newton-Raphson domain failure suppressed globally |
| BUG-006 | 🟢 | 🔲 OPEN | VIX NaN guard opaque and misses inf |
| SEC-001 | 🔴 | ✅ FIXED | SQL injection in backtest_engine update_pattern_stats |
| SEC-002 | 🔴 | ✅ FIXED | SQL injection in pattern_builder ALTER UPDATE |
| SEC-003 | 🔴 | ✅ FIXED | SQL injection in validation_engine update_prediction_status |
| SEC-004 | 🔴 | ✅ FIXED | SQL injection in pattern_feature_extractor mark_events_processed |
| SEC-005 | 🟡 | 🔲 OPEN | SQL injection in cleanup_schemes DELETE (integer, lower risk) |
| SEC-006 | 🟡 | 🔲 OPEN | ClickHouse allows broad 172.16.0.0/12 network range |
| SEC-007 | 🟡 | 🔲 OPEN | participant_oi uses network_mode: host |
| PERF-001 | 🟠 | 🔲 OPEN | One ALTER UPDATE mutation per pattern per backtest run |
| PERF-002 | 🟡 | 🔲 OPEN | SIP XIRR O(n × months) — dominant runtime bottleneck |
| PERF-003 | 🟡 | 🔲 OPEN | cleanup_schemes mutations_sync=1 can timeout on large tables |
| PERF-004 | 🟡 | 🔲 OPEN | migrate.py single command() silently drops multi-statement files |
| DATA-001 | 🟠 | ✅ FIXED | Entry price uses prediction_date close — forward-look bias |
| DATA-002 | 🟠 | ✅ FIXED | win_rate in star ratings ≠ win_rate_1d in patterns (threshold mismatch) |
| DATA-003 | 🟠 | ✅ FIXED | P005 permanently zero matches until fii_dii seeded |
| DATA-004 | 🟡 | 🔲 OPEN | fii_dii_pipeline has no validation of NSE API values |
| DATA-005 | 🟡 | 🔲 OPEN | gap_analyzer.py missing — closed-loop feedback never runs |
| DATA-006 | 🟡 | 🔲 OPEN | Partial migration executes as success (see PERF-004) |
| OPS-001 | 🔴 | 🔲 OPEN | No cron/scheduler — pipeline never auto-runs (host cron rejected) |
| OPS-002 | 🟠 | 🔲 OPEN | gap_analyzer.py absent from Dockerfile and codebase |
| OPS-003 | 🟡 | 🔲 OPEN | No failure alerting mechanism |
| OPS-004 | 🟡 | 🔲 OPEN | Holiday cache never invalidated in long-running processes |
| OPS-005 | 🟡 | ✅ FIXED | participant_oi_pipeline not in meta_pipeline steps |
| OPS-006 | 🟡 | ✅ FIXED | fii_dii_pipeline not in meta_pipeline steps |
| OPS-007 | 🟢 | ✅ FIXED | progress.md outdated since 2026-03-04 |

---

## Sprint Status

| Sprint | Focus | Status |
|--------|-------|--------|
| Sprint 1 | Pipeline runs + security baseline + orchestration | ✅ COMPLETE |
| Sprint 2 | Data correctness (BUG-003, DATA-001/002/003, SEC-002/003/004) | ✅ COMPLETE |
| Sprint 3 | Close the loop (gap_analyzer, BUG-004, migrate.py, OPS-001 Docker scheduler) | 🔲 PENDING |
| Sprint 4 | Quality & robustness (network, alerting, performance) | 🔲 PENDING |
