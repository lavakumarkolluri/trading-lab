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

### BUG-002 🟡 ✅ FIXED Migration 025 is an exact duplicate of migration 024
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

### BUG-004 🟠 ✅ FIXED backtest_engine.py — update_pattern_stats uses ALTER TABLE UPDATE on ReplacingMergeTree
**File:** `pipeline/backtest_engine.py`, function `update_pattern_stats`
**Root cause:** Decision 026 explicitly forbids ALTER TABLE UPDATE on ReplacingMergeTree tables. While this doesn't touch `version` directly, ALTER UPDATE mutations can conflict with deduplication merges.
**Fix (Sprint 3):** Re-insert the full patterns row with `version = int(datetime.now().timestamp())` instead of using ALTER UPDATE.

---

### BUG-005 🟡 ✅ FIXED mf_compute_returns.py — XIRR Newton-Raphson domain failure
**File:** `pipeline/mf_compute_returns.py`, function `_xirr_newton`
**Root cause:** Global `warnings.filterwarnings("ignore", category=RuntimeWarning)` suppresses all RuntimeWarnings in the module, masking legitimate issues. The Newton-Raphson solver can iterate into invalid domains with fractional time periods.
**Fix (Sprint 4):** Remove global suppression; use `np.errstate` for the specific operation; add domain guard `rate > -1` before each Newton step.

---

### BUG-006 🟢 ✅ FIXED prediction_engine.py — VIX NaN guard is opaque
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

### SEC-005 🟡 ✅ FIXED SQL Injection — cleanup_schemes.py (low risk, integers only)
**Fix applied (Sprint 3):** `safe_codes = [int(c) for c in to_delete]` before interpolation.

---

### SEC-006 🟡 ✅ FIXED ClickHouse allows broad 172.16.0.0/12 network
**Fix applied (Sprint 3):** Restricted to `172.18.0.0/16` (actual trading-lab compose network CIDR) in `clickhouse/config/default-user.xml`.

---

### SEC-007 🟡 ✅ FIXED participant_oi uses network_mode: host
**Fix applied (Sprint 3):** Removed `network_mode: host`; set `CH_HOST=clickhouse`, `MINIO_HOST=minio:9000`; added healthcheck `depends_on`.

---

## PERFORMANCE

### PERF-001 🟠 ✅ FIXED One ALTER UPDATE mutation per pattern per backtest run
**Fix applied (Sprint 3):** Re-insert full pattern row with `version=int(now.timestamp())` instead of ALTER UPDATE (Decision 026 compliance).

### PERF-002 🟡 🔲 OPEN SIP XIRR O(n × months) dominant runtime
**Fix (Sprint 4):** Pre-compute monthly, forward-fill within month.

### PERF-003 🟡 ✅ FIXED cleanup_schemes mutations_sync=1 can timeout
**Fix applied (Sprint 3):** All three DELETE statements now use `mutations_sync=0`.

### PERF-004 🟡 ✅ FIXED migrate.py single command() silently drops multi-statement files
**Fix applied (Sprint 3):** Split SQL on `";\n"` and execute each statement separately; log statement count.

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

### DATA-004 🟡 ✅ FIXED fii_dii_pipeline has no validation of NSE API values
**Fix applied (Sprint 3):** `_validate_flow(value, field, date_str)` clamps out-of-bounds values to 0 with a warning; `_MAX_FLOW_CR = 200_000.0`.

### DATA-005 🟡 ✅ FIXED gap_analyzer.py missing — closed-loop feedback never runs
**Fix applied (Sprint 3):** `pipeline/gap_analyzer.py` implemented; added as WEEKLY_STEPS Step 14 (soft_fail); `gap_analyzer` service in docker-compose.yml.

### DATA-006 🟡 ✅ FIXED Partial migration executes as success (see PERF-004)
**Fix applied (Sprint 3):** Same fix as PERF-004 — multi-statement split in migrate.py.

---

## MAINTENANCE / OPERATIONAL

### OPS-001 🔴 ✅ FIXED No cron/scheduler — pipeline never auto-runs
**Note:** Original host cron rejected. Fix is Docker-native.
**Fix applied (Sprint 3):** `pipeline/scheduler.py` uses `schedule` lib; `scheduler` service runs `restart: unless-stopped`; runs daily 16:30 IST Mon–Fri and weekly Sunday 06:00/06:30 IST.

---

### OPS-002 🟠 ✅ FIXED gap_analyzer.py absent from Dockerfile and codebase
**Fix applied (Sprint 3):** `pipeline/gap_analyzer.py` implemented; `COPY gap_analyzer.py .` added to Dockerfile; `gap_analyzer` service in docker-compose.yml.

### OPS-003 🟡 ✅ FIXED No failure alerting mechanism
**Fix applied (Sprint 3):** `meta_pipeline.py` calls `_send_telegram()` on any step failure when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` env vars are set.

### OPS-004 🟡 ✅ FIXED Holiday cache never invalidated in long-running processes
**Fix applied (Sprint 3):** `_cache_loaded_at` timestamp + `_CACHE_TTL_SECONDS=86400` force reload after 24h in `holidays_pipeline.py`.

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
| BUG-002 | 🟡 | ✅ FIXED | Migration 025 is exact duplicate of 024 |
| BUG-003 | 🟠 | ✅ FIXED | pattern_builder overwrites total_matches with per-run count |
| BUG-004 | 🟠 | ✅ FIXED | backtest_engine uses ALTER UPDATE on ReplacingMergeTree |
| BUG-005 | 🟡 | ✅ FIXED | XIRR Newton-Raphson domain failure suppressed globally |
| BUG-006 | 🟢 | ✅ FIXED | VIX NaN guard opaque and misses inf |
| SEC-001 | 🔴 | ✅ FIXED | SQL injection in backtest_engine update_pattern_stats |
| SEC-002 | 🔴 | ✅ FIXED | SQL injection in pattern_builder ALTER UPDATE |
| SEC-003 | 🔴 | ✅ FIXED | SQL injection in validation_engine update_prediction_status |
| SEC-004 | 🔴 | ✅ FIXED | SQL injection in pattern_feature_extractor mark_events_processed |
| SEC-005 | 🟡 | ✅ FIXED | SQL injection in cleanup_schemes DELETE (integer, lower risk) |
| SEC-006 | 🟡 | ✅ FIXED | ClickHouse allows broad 172.16.0.0/12 network range |
| SEC-007 | 🟡 | ✅ FIXED | participant_oi uses network_mode: host |
| PERF-001 | 🟠 | ✅ FIXED | One ALTER UPDATE mutation per pattern per backtest run |
| PERF-002 | 🟡 | 🔲 OPEN | SIP XIRR O(n × months) — dominant runtime bottleneck |
| PERF-003 | 🟡 | ✅ FIXED | cleanup_schemes mutations_sync=1 can timeout on large tables |
| PERF-004 | 🟡 | ✅ FIXED | migrate.py single command() silently drops multi-statement files |
| DATA-001 | 🟠 | ✅ FIXED | Entry price uses prediction_date close — forward-look bias |
| DATA-002 | 🟠 | ✅ FIXED | win_rate in star ratings ≠ win_rate_1d in patterns (threshold mismatch) |
| DATA-003 | 🟠 | ✅ FIXED | P005 permanently zero matches until fii_dii seeded |
| DATA-004 | 🟡 | ✅ FIXED | fii_dii_pipeline has no validation of NSE API values |
| DATA-005 | 🟡 | ✅ FIXED | gap_analyzer.py missing — closed-loop feedback never runs |
| DATA-006 | 🟡 | ✅ FIXED | Partial migration executes as success (see PERF-004) |
| OPS-001 | 🔴 | ✅ FIXED | No cron/scheduler — pipeline never auto-runs (host cron rejected) |
| OPS-002 | 🟠 | ✅ FIXED | gap_analyzer.py absent from Dockerfile and codebase |
| OPS-003 | 🟡 | ✅ FIXED | No failure alerting mechanism |
| OPS-004 | 🟡 | ✅ FIXED | Holiday cache never invalidated in long-running processes |
| OPS-005 | 🟡 | ✅ FIXED | participant_oi_pipeline not in meta_pipeline steps |
| OPS-006 | 🟡 | ✅ FIXED | fii_dii_pipeline not in meta_pipeline steps |
| OPS-007 | 🟢 | ✅ FIXED | progress.md outdated since 2026-03-04 |

---

## Sprint Status

| Sprint | Focus | Status |
|--------|-------|--------|
| Sprint 1 | Pipeline runs + security baseline + orchestration | ✅ COMPLETE |
| Sprint 2 | Data correctness (BUG-003, DATA-001/002/003, SEC-002/003/004) | ✅ COMPLETE |
| Sprint 3 | Close the loop (gap_analyzer, BUG-004, migrate.py, OPS-001 Docker scheduler) | ✅ COMPLETE |
| Sprint 4 | Quality & robustness (network, alerting, performance) | ✅ COMPLETE |

**Only remaining open issue: PERF-002** (SIP XIRR O(n × months) optimisation — deferred, not blocking).
