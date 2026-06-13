# CLAUDE.md

## Domain Logic Rules

**Historical data only** — For any learning, training, backtesting, or drift-detection logic, ALWAYS use historical data windows. Never query live/paper-trade-only windows as the sole source. Verify the data source spans the intended historical range before implementing.

**Plan before core logic** — Before implementing seeders, learning loops, attribution, or drift detection: restate the intended goal and the data source in plain terms and confirm before writing code.

---

## Coding Guidelines

**Pipeline boilerplate rule** — Never inline ClickHouse/MinIO/logging setup in `pipeline/`. Always import from `ch_utils`, `logging_utils`, `pipeline_utils`. `scripts/lint_boilerplate.py` fails CI if you do.

**Simplicity First** — minimum code that solves the problem. No speculative features, no abstractions for single-use code, no error handling for impossible scenarios.

**Surgical Changes** — touch only what the request requires. Don't improve adjacent code. Remove only imports/variables your own changes orphaned.

**Think Before Coding** — state assumptions explicitly. If multiple interpretations exist, present them. If unclear, ask before implementing.

**Goal-Driven** — transform tasks into verifiable goals. For multi-step work, write a brief plan first.

---

## Branching & CI/CD

**All changes go to `stage` first. `master` is production — never commit directly.**

```
stage  →  CI tests pass  →  auto-merge to master  (daily 17:00 UTC Mon–Fri)
```

- Work on `stage` branch. Push features/fixes there.
- GitHub Actions runs `tests/` on every push to `stage` and on daily schedule.
- If all tests pass, the workflow auto-merges `stage` → `master` and tags `prod-YYYYMMDD-HHMM`.
- Every prod breakage must produce a failing test FIRST, then a fix. No exceptions.
- Tests must run offline (no live ClickHouse). Use `unittest.mock.MagicMock` for DB calls.
- Standard mock pattern: `ch = MagicMock(); ch.query.return_value.result_rows = [("NIFTY", 65)]`

**Test files (422 tests as of 2026-05-15):**
- `tests/test_docker_compose.py` — compose file structure, config mounts, credentials
- `tests/test_migrations.py` — SQL file numbering, syntax validity
- `tests/test_intraday_monitor.py` — trailing stop, timezone, column schema, MIDCPNIFTY config
- `tests/test_compute_oi_features.py` — DataFrame duplicate column regression
- `tests/test_dashboard.py` — expiry date format, nav completeness, SQL safety
- `tests/test_data_freshness.py` — staleness thresholds, auto-fix, empty table safety
- `tests/test_confidence_scorer.py` — duplicate-strike, EOD fallback, lot-size loading
- `tests/test_options_eod_summary.py` — symbol validation, parameterized queries, dedup fix

---

## Infrastructure Gotchas

**ClickHouse v26+ config mount naming**: The entrypoint creates `users.d/default-user.xml` as a directory on first boot. Any bind-mount using that exact filename fails with "Is a directory". Always mount user config as `trading-lab-user.xml` instead.

**COMPOSE_FILE in-container path**: Services that need to call `docker compose` (meta_pipeline, scheduler) run inside containers where the project is mounted at `/trading-lab`. COMPOSE_FILE must be `/trading-lab/docker-compose.yml`, NOT the host path `/home/lavakumar/trading-lab/docker-compose.yml`.

**Docker build cache per service**: Each service has its own cached image tag. Running `docker compose build pipeline` only rebuilds the `pipeline` image — it does NOT rebuild `compute_oi_features`, `intraday_monitor`, etc. even if they share the same Dockerfile. Always rebuild each affected service by name, or use `--no-cache`.

**ClickHouse log bloat**: System tables (text_log, trace_log, query_log) accumulate GiBs without TTL config. TTL is configured in `clickhouse/config/log-retention.xml` — 7 days for query/part logs, 3 days for trace/metric logs.

---

## Trading System Architecture

**Strategy**: Iron fly (0DTE) — sell ATM CE+PE (straddle) + buy OTM wings. Paper trading phase.

**Instruments** (4 NSE indices, lot sizes as of 2025-10-29 via `market.fo_lot_sizes FINAL`):
| Symbol      | Lot Size | Weekly Expiry | Wing Points |
|-------------|----------|---------------|-------------|
| NIFTY       | 65       | Tuesday       | 200 pts     |
| BANKNIFTY   | 30       | Wednesday     | 500 pts     |
| FINNIFTY    | 60       | Tuesday       | 200 pts     |
| MIDCPNIFTY  | 120      | Monday        | 200 pts     |

**Risk params**: Target ₹2000/trade, stop ₹1000/trade. Trailing activates at target with 75% floor.
Entry window 09:30–14:00 IST. EOD exit 15:20 IST. MIN_CONFIDENCE=60 gate before entering.

**Pipeline order** (daily, UTC):
1. `option_chain_intraday` — live options data during market hours (03:40)
2. `compute_oi_features` — OI/IV features after EOD (12:30)
3. `strategy_backtester` — historical iron fly P&L (12:30)
4. `confidence_scorer --compare` — full retrain + score (07:00 Sunday) **OR**
   `confidence_scorer --score-only` — score only using saved models (13:00 Mon–Fri)
5. `strategy_selector` — picks symbols to trade (13:30)
6. `intraday_monitor` — paper trades during market hours (03:50 next day)

**Score pipeline rule**: `--compare` retrains models weekly. `--score-only` must run every trading day so intraday_monitor reads fresh scores from `analysis.scorecard`; without it the monitor falls back to 50.0 for all symbols.

**Key tables**: `market.options_chain`, `market.open_positions`, `market.trade_outcomes`,
`analysis.scorecard`, `market.fo_lot_sizes`, `market.options_eod_summary`

---

## Common Debugging Patterns

**Duplicate DataFrame columns → ClickHouse crash**: When building column lists dynamically, use `list(dict.fromkeys(col_list))` to deduplicate before selecting. Selecting a column that appears twice returns a DataFrame instead of Series; ClickHouse driver then crashes with `AttributeError: dtype`.

**Duplicate strike index in options chain** (`extract_chain_features`): Multiple intraday snapshots on the same date create duplicate strikes after `set_index("strike")`. `.get(atm, 0)` returns a Series instead of a scalar → `float()` crashes. Fix: `.groupby(level=0).last()` after every `set_index("strike")` call.

**UTC→IST timezone in ClickHouse queries**: ClickHouse returns UTC-naive datetimes. When comparing against IST time, add `timedelta(hours=5, minutes=30)` before computing age/staleness. Without this, age calculations are 5.5h off.

**EOD fallback in score_today()**: `options_eod_summary` only has data through the last bhavcopy date. `latest_snap` = today means `extract_eod_features()` returns `{}` → zero IV/VIX → UNRELIABLE SCORE. Use `_nearest_eod()` helper to fall back to the most recent available past date.

**load_lot_sizes() non-deterministic**: `SELECT symbol, lot_size FROM ... FINAL` without `ORDER BY` gives random results when multiple effective dates exist. Always use `argMax(lot_size, effective_from) GROUP BY symbol`.

**query() parameterization**: The local `query()` helper in `dashboard/app.py` does NOT accept a `params=` kwarg. Use f-strings for trusted internal values (DB query results), or ClickHouse `{param:Type}` syntax for external input.

**confidence_scorer --compare**: After training, must call `score_today(ch, mc, sym)` for each symbol. Without this, intraday_monitor reads no scores from `analysis.scorecard` and defaults to 50.0 confidence.

**options_eod_summary schema requires (symbol, date, expiry) ORDER BY**: NIFTY and FINNIFTY both expire on Tuesday — same `(date, expiry)` pair. Without `symbol` in the ORDER BY, inserting FINNIFTY rows silently replaces NIFTY rows (ReplacingMergeTree collision). Fixed in migration 081. Always ensure `symbol` is in ORDER BY for any per-symbol time-series table.

**compute_historical_iv requires options_eod_summary rows first**: `run_symbol(ch, symbol, from_date)` reads dates from `options_eod_summary` then updates that table with IV. Run `options_eod_summary_pipeline` for all 4 symbols before running `compute_historical_iv`.

**ReplacingMergeTree re-insert**: Never use `ALTER TABLE UPDATE` on the `version` column — ClickHouse raises CANNOT_UPDATE_COLUMN. To refresh a row: re-insert with `version = int(datetime.now().timestamp())`.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
