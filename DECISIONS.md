# Trading Lab — Architectural Decisions Log

> All key engineering decisions, design choices, and trade-offs made during the build.
> Updated: 2026-03-21

---

## Decision 001 — Containerised Local Dev Stack
**Date:** 2026-03-01
**Decision:** Run entire trading lab inside Docker Compose on local Ubuntu (Dell Latitude 5400).
**Components:** VS Code Server, MinIO, ClickHouse, Jupyter, Portainer, Pipeline containers.
**Rationale:** Full reproducibility, no cloud costs, all data stays local. Easy to migrate to cloud later by swapping env vars.
**Trade-off:** Tied to local machine uptime; no HA. Acceptable for paper-trading research phase.

---

## Decision 002 — ClickHouse as Single Source of Truth (SSOT)
**Date:** 2026-03-01
**Decision:** All processed data lives in ClickHouse. MinIO holds raw Parquet files as an immutable staging layer.
**Rationale:** ClickHouse columnar storage gives sub-second analytical queries on millions of OHLCV rows. MergeTree family handles time-series patterns natively.
**Trade-off:** Requires schema discipline (migrations). Not suitable for transactional workloads (use Postgres for those if needed later).

---

## Decision 003 — ReplacingMergeTree for Market Data Tables
**Date:** 2026-03-01
**Decision:** All market data tables (ohlcv_daily, options_chain, nifty_live, etc.) use `ReplacingMergeTree(version)` with a `version UInt64` column set to `toUnixTimestamp(now())` at insert time.
**Rationale:** Enables idempotent upserts — rerunning the pipeline never creates duplicate rows. Background merges deduplicate automatically.
**Trade-off:** Deduplication is eventual (not immediate). Use `FINAL` keyword in queries that need strict dedup: `SELECT ... FROM market.ohlcv_daily FINAL`.

---

## Decision 004 — Numbered Sequential Migration Files
**Date:** 2026-03-01
**Decision:** All schema changes are `NNN_description.sql` files in `clickhouse/migrations/`. Executed by `migrate.py` with MD5 checksum validation.
**Rationale:** Immutable migration history. Drift detection compares Git migrations vs live ClickHouse schema. No manual DDL on the running DB.
**Rule:** Never edit a migration file after it has run. Create a new numbered migration for any schema change.

---

## Decision 005 — Incremental Pipeline (max(date) per symbol)
**Date:** 2026-03-01
**Decision:** `main.py` pipeline checks `max(date)` per symbol in ClickHouse before downloading. Only downloads new rows.
**Rationale:** Avoids re-downloading years of history on every run. Reduces Yahoo Finance rate-limiting risk.
**Edge case handled:** Pre-1970 date overflow — ClickHouse Date type cannot store dates before 1970-01-01. Filter applied before insert.

---

## Decision 006 — MinIO Parquet Path Convention
**Date:** 2026-03-01
**Decision:** `{market}/daily/{YYYY-MM-DD}/{SYMBOL}.parquet`
**Rationale:** Partitioned by market and date for easy time-range queries and selective deletion. Symbol sanitised (= and & replaced with _) for S3-safe paths.

---

## Decision 007 — 5-Database Schema Separation
**Date:** 2026-03-01
**Decision:** Separate ClickHouse databases for each domain: `logs`, `staging`, `market`, `analysis`, `trades`, `learning`.
**Rationale:** Clear domain boundaries. Prevents cross-domain query accidents. Simplifies access control if multi-user later.

---

## Decision 008 — MF NAV Wide/Enriched Table (Separate Columns)
**Date:** 2026-03-21
**Decision:** Aggregations for Mutual Fund NAV are stored as **additional columns** in a single wide table `market.mf_nav_enriched` rather than separate aggregation tables.
**Aggregations included:**
  - Weekly rollup: `week_open`, `week_high`, `week_low`, `week_close`, `week_return_pct`
  - Monthly rollup: `month_open`, `month_high`, `month_low`, `month_close`, `month_return_pct`
  - SMA-20, SMA-50, SMA-200
  - EMA-20, EMA-50 (Wilder's method, `adjust=False`)
  - RSI-14 (Wilder's RSI, values 0–100)
  - ATR-14 proxy (14-day rolling mean of |ΔNAV|; MF has no intraday OHLC)
  - 52-week high / low (trailing 252 trading days)
  - YTD return % (resets each January 1)
  - All-time high (cumulative max from inception)
  - Drawdown % (% below ATH)
**Rationale:** Single JOIN for all analytics. No fan-out queries across multiple aggregation tables. Wide tables are idiomatic for ClickHouse's columnar storage.
**Trade-off:** **Trade-off:** Between a re-insert and the next background merge, duplicate rows are transiently visible. Always query with `SELECT ... FINAL` on `market.mf_nav_enriched` to guarantee correct deduplication at read time.

---

## Decision 009 — ATR Proxy for Mutual Funds
**Date:** 2026-03-21
**Decision:** True Average True Range (ATR) requires high/low/close per period. MF NAV data only has one price per day (closing NAV). ATR is proxied as `14-day rolling mean of |daily NAV change|`.
**Rationale:** Retains the semantic intent of ATR (average daily volatility) adapted to available data. Named `atr_14` to maintain consistency with OHLCV-based systems.
**Interpretation:** Higher `atr_14` = higher absolute daily NAV volatility in rupees. Use `atr_14 / nav * 100` in queries for percentage volatility.

---

## Decision 010 — Incremental Enrichment with Warm-up Window
**Date:** 2026-03-21
**Decision:** On incremental runs, `mf_aggregation.py` fetches from `(last_enriched_date - 210 days)` to ensure rolling windows (SMA-200 needs 200 rows) are correctly computed even when only a few new rows arrive.
**Rationale:** Without warm-up, SMA-200 for newly arriving rows would be NaN because the rolling window only sees the new rows.
**Trade-off:** Slightly more data read per run. Negligible at ClickHouse query speeds.

---

## Decision 011 — numpy Added as Pipeline Dependency
**Date:** 2026-03-21
**Decision:** Added `numpy` to `pipeline/Dockerfile` pip install.
**Rationale:** Required by `mf_aggregation.py` for `np.nan` and numeric operations in RSI / drawdown calculations. `pandas` depends on numpy but does not guarantee it is importable without explicit install in slim containers.

---

## Decision 012 — `mf_aggregation` as Separate On-Demand Docker Service
**Date:** 2026-03-21
**Decision:** Added `mf_aggregation` service to `docker-compose.yml` with `restart: "no"`. Reuses the `pipeline` image.
**Usage:**
  ```bash
  docker compose run --rm mf_aggregation            # incremental (default)
  docker compose run --rm mf_aggregation --full     # full recompute
  docker compose run --rm mf_aggregation --scheme 119598   # single scheme debug
  ```
**Rationale:** Keeps enrichment decoupled from raw ingestion (`mf_pipeline`). Can be triggered independently after each NAV load. Avoids tightly coupling compute to ingest.

---

## Decision 013 — Trading Strategy Constraints
**Date:** 2026-03-01
**Strategy:** Nifty 50 0DTE short strangle (intraday option selling).
**Hard rules:**
  - Exit all positions by 3:20 PM IST. No exceptions.
  - VIX sweet spot: 13–18. Avoid trades when VIX < 13 or > 25.
  - Paper trade for minimum 40 trades before deploying real capital.
  - Scale: 1 lot → 2–3 lots only if profitable after full analysis.

---

## Pending Decisions (Backlog)

| # | Decision Needed | Priority |
|---|-----------------|----------|
| D014 | Parallelism strategy for symbol downloads (ThreadPoolExecutor vs asyncio) | High |
| D015 | NSE options chain scraper — direct API vs screen scraping | High |
| D016 | Telegram bot framework (python-telegram-bot vs telethon) | Medium |
| D017 | Scheduler — cron inside Docker vs Airflow vs APScheduler | Medium |
| D018 | Expand aggregations to OHLCV market data (stocks/crypto) | Medium |
| D019 | MF NAV aggregation — add `return_1m`, `return_3m`, `return_1y` columns | Low |

---

*Maintained by: lavakumar | Repo: trading-lab (private)*
