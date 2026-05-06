# CLAUDE.md

## Coding Guidelines

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

**Test files:**
- `tests/test_docker_compose.py` — compose file structure, config mounts, credentials
- `tests/test_migrations.py` — SQL file numbering, syntax validity
- `tests/test_intraday_monitor.py` — trailing stop, timezone, column schema
- `tests/test_compute_oi_features.py` — DataFrame duplicate column regression

---

## Infrastructure Gotchas

**ClickHouse v26+ config mount naming**: The entrypoint creates `users.d/default-user.xml` as a directory on first boot. Any bind-mount using that exact filename fails with "Is a directory". Always mount user config as `trading-lab-user.xml` instead.

**COMPOSE_FILE in-container path**: Services that need to call `docker compose` (meta_pipeline, scheduler) run inside containers where the project is mounted at `/trading-lab`. COMPOSE_FILE must be `/trading-lab/docker-compose.yml`, NOT the host path `/home/lavakumar/trading-lab/docker-compose.yml`.

**Docker build cache per service**: Each service has its own cached image tag. Running `docker compose build pipeline` only rebuilds the `pipeline` image — it does NOT rebuild `compute_oi_features`, `intraday_monitor`, etc. even if they share the same Dockerfile. Always rebuild each affected service by name, or use `--no-cache`.

**ClickHouse log bloat**: System tables (text_log, trace_log, query_log) accumulate GiBs without TTL config. TTL is configured in `clickhouse/config/log-retention.xml` — 7 days for query/part logs, 3 days for trace/metric logs.

---

## Trading System Architecture

**Strategy**: 0DTE straddle paper trading — sell ATM CE+PE at open, manage intraday.

**Instruments**: NIFTY (Tuesday expiry, lot=65) + BANKNIFTY (monthly expiry, lot=30)

**Risk params**: Target ₹2000, stop ₹1000, trailing activates at target with 75% floor, entry window 09:30–14:00 IST, EOD exit 15:20 IST.

**Pipeline order** (daily, UTC):
1. `option_chain_intraday` — live options data during market hours
2. `compute_oi_features` — OI/IV features (05:30 after EOD)
3. `strategy_backtester` — historical PnL (05:30)
4. `confidence_scorer --compare` — trains model + scores today (07:00) ← **must call score_today() after training**
5. `strategy_selector` — picks which symbols to trade (08:00)
6. `intraday_monitor` — paper trades during market hours (09:15 start)

**Key tables**: `market.options_chain`, `market.open_positions`, `market.trade_outcomes`, `analysis.scorecard`

---

## Common Debugging Patterns

**Duplicate DataFrame columns → ClickHouse crash**: When building column lists dynamically, use `list(dict.fromkeys(col_list))` to deduplicate before selecting. Selecting a column that appears twice returns a DataFrame instead of Series; ClickHouse driver then crashes with `AttributeError: dtype`.

**UTC→IST timezone in ClickHouse queries**: ClickHouse returns UTC-naive datetimes. When comparing against IST time, add `timedelta(hours=5, minutes=30)` before computing age/staleness. Without this, age calculations are 5.5h off.

**query() parameterization**: The local `query()` helper in `dashboard/app.py` does NOT accept a `params=` kwarg. Use f-strings for trusted internal values (DB query results), or ClickHouse `{param:Type}` syntax for external input.

**confidence_scorer --compare**: After training, must call `score_today(ch, mc, sym)` for each symbol. Without this, intraday_monitor reads no scores from `analysis.scorecard` and defaults to 50.0 confidence.
