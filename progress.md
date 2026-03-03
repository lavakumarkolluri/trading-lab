# Trading Lab — Build Progress & Session Log

## Project Goal
Build a local containerized trading data engineering environment on Ubuntu to:
- Download market data (Indian stocks, US stocks, Crypto, Forex)
- Store raw data in MinIO (object storage)
- Load into ClickHouse for analytics
- Build agentic pipelines for Nifty 50 intraday option selling
- Paper trade, build confidence, then go live

---

## Environment

| Component | Detail |
|-----------|--------|
| OS | Ubuntu (Dell Latitude 5400) |
| Disk | 196 GB total, ~19 GB used |
| ClickHouse | v26.2.1.1139 |
| Docker | docker compose v2 |
| Git | Connected to GitHub (private repo: trading-lab) |
| VS Code | Native desktop + code-server in Docker |

---

## Folder Structure
```
trading-lab/
├── docker-compose.yml
├── .gitignore
├── PROGRESS.md                        ← this file
├── pipeline/
│   ├── Dockerfile
│   ├── main.py                        ← incremental OHLCV pipeline
│   └── symbols.py                     ← 724 symbol watchlist
├── clickhouse/
│   ├── migrate.py                     ← migration runner (4 modes)
│   ├── config/
│   │   └── default-user.xml           ← ClickHouse network config
│   └── migrations/
│       ├── 001_create_logs_database.sql
│       ├── 002_create_logs_agent_runs.sql
│       ├── 003_create_logs_agent_logs.sql
│       ├── 004_create_logs_data_quality.sql
│       ├── 005_create_staging_database.sql
│       ├── 006_create_staging_file_registry.sql
│       ├── 007_create_market_database.sql
│       ├── 008_create_market_nifty_live.sql
│       ├── 009_create_market_ohlcv_daily.sql
│       ├── 010_create_market_options_chain.sql
│       ├── 011_create_market_global_context.sql
│       ├── 012_create_market_events.sql
│       ├── 013_create_market_fii_dii.sql
│       ├── 014_create_analysis_database.sql
│       ├── 015_create_analysis_market_regime.sql
│       ├── 016_create_analysis_strike_recommendations.sql
│       ├── 017_create_trades_database.sql
│       ├── 018_create_trades_opportunities.sql
│       ├── 019_create_trades_daily_summary.sql
│       ├── 020_create_learning_database.sql
│       └── 021_create_learning_journal.sql
└── workspace/
    └── logs/
```

---

## Running Services (docker compose up -d)

| Service | URL | Credentials |
|---------|-----|-------------|
| VS Code (browser) | http://localhost:8080 | password: `yourpassword` |
| MinIO UI | http://localhost:9001 | admin / password123 |
| Jupyter | http://localhost:8888 | token: `yourtoken` |
| Portainer | https://localhost:9443 | set on first visit |
| ClickHouse HTTP | http://localhost:8123 | default / (no password) |

### On-demand services (no restart)
- `pipeline` — runs main.py on demand
- `migrate` — runs migrate.py on demand

---

## ClickHouse Databases & Tables

All 21 migrations ran successfully. Tables created:

| Database | Table | Engine | Purpose |
|----------|-------|--------|---------|
| logs | agent_runs | MergeTree | Every agent run recorded |
| logs | agent_logs | MergeTree | Every action logged |
| logs | data_quality | MergeTree | Data quality checks |
| staging | file_registry | MergeTree | MinIO file load tracking |
| market | nifty_live | ReplacingMergeTree | Nifty spot + VIX every 1 min |
| market | ohlcv_daily | ReplacingMergeTree | All symbols daily OHLCV ✅ data loaded |
| market | options_chain | ReplacingMergeTree | NSE options every 5 min |
| market | global_context | ReplacingMergeTree | SGX, crude, USD/INR daily |
| market | events | ReplacingMergeTree | RBI, expiry, budget calendar |
| market | fii_dii | ReplacingMergeTree | FII/DII institutional flow |
| analysis | market_regime | ReplacingMergeTree | VIX regime classification |
| analysis | strike_recommendations | ReplacingMergeTree | Optimal strikes for strategy |
| trades | opportunities | MergeTree | Paper + live trade log |
| trades | daily_summary | ReplacingMergeTree | Daily P&L summary |
| learning | journal | MergeTree | LinkedIn posts + learnings |

---

## Pipeline — Data Loaded

### Symbols Watchlist (symbols.py)

| Market | Count | Source | Status |
|--------|-------|--------|--------|
| Indian (NSE Nifty 500) | ~188 | yfinance (.NS) | ✅ ~125 loaded, incrementals active |
| BSE | ~178 | yfinance (.BO) | In progress |
| US (S&P 500 + ETFs) | ~248 | yfinance | ✅ Loaded |
| NASDAQ 100 | ~82 | yfinance | ✅ Loaded |
| Crypto (top 20) | 20 | yfinance | ✅ Loaded |
| Forex | 8 | yfinance | ✅ Loaded |
| **Total** | **~724** | | |

### Pipeline Behaviour (main.py)
- **Incremental** — checks `max(date)` per symbol in ClickHouse first
- **Skip** — if already up to date, skips entirely (no download)
- **New symbols** — downloads full history (max period)
- **Pre-1970 filter** — US stocks with 60+ years of data filtered to 1970-01-01
- **No deletes** — never deletes existing rows; only appends new rows
- **MinIO path** — `{market}/daily/{date}/{symbol}.parquet`

### Known Issues Fixed
| Issue | Fix Applied |
|-------|-------------|
| TATAMOTORS.NS — no data | Removed from watchlist |
| US stocks pre-1970 date overflow | Filter to `>= 1970-01-01` |
| symbols.py not in Docker image | Added `COPY symbols.py .` to Dockerfile |
| ClickHouse auth from containers | Mounted `default-user.xml`, allowed `::0` network |
| `version` attribute in compose | Removed obsolete `version: "3.8"` line |
| docker socket permission | `sudo chmod 666 /var/run/docker.sock` |

---

## migrate.py — Migration Runner

### Usage
```bash
docker compose run --rm migrate                                    # run new migrations
docker compose run --rm migrate python /clickhouse/migrate.py --status
docker compose run --rm migrate python /clickhouse/migrate.py --check-drift
```

### Modes
| Mode | What it does |
|------|-------------|
| `--run` | Runs only new/pending migrations |
| `--status` | Shows all migrations and their run status |
| `--check-drift` | Compares Git migrations vs actual ClickHouse schema |

### Rules
- Never modify an existing migration file after it has run
- Always create a new numbered migration for any schema change
- Checksums validate file integrity on every run
- All run history stored in `system_migrations.history`

---

## Git Workflow
```bash
# Daily workflow
cd ~/trading-lab
git add .
git commit -m "descriptive message"
git push origin master
```

GitHub repo: `https://github.com/lavakumarkolluri/trading-lab` (private)

Useful aliases added to `~/.bashrc`:
```bash
alias lab='cd ~/trading-lab'
```

---

## Architecture — Data Flow
```
External APIs (yfinance, NSE, Binance)
          │
          ▼
    Data Agents
          │
    ┌─────┴──────┐
    ▼            ▼
  Fast data    Slow data
  (1-min)      (EOD/weekly)
  Direct →     → MinIO staging
  ClickHouse   → Loader Agent
                 → ClickHouse
          │
          ▼
    ClickHouse (Single Source of Truth)
          │
          ▼
    Analysis Agents (read-only from ClickHouse)
          │
          ▼
    Signal Agent → Telegram Alert
          │
          ▼
    You: Paper / Live / Skip
          │
          ▼
    Monitor Agent + Journal Agent
          │
          ▼
    Learning Agent → LinkedIn posts
```

---

## Trading Strategy Plan

**Focus:** Nifty 50 intraday option selling (short strangle, 0DTE)

| Phase | Timeline | Goal |
|-------|----------|------|
| 1 | Month 1–2 | Paper trade only, log everything |
| 2 | Month 3 | 40+ paper trades analysed, refine rules |
| 3 | Month 3 | 1 lot real money, A+ setups only |
| 4 | Month 4–6 | Scale to 2–3 lots if profitable |
| 5 | Month 7–12 | Data-driven scaling |
| 6 | Year 2 | Expand to Bank Nifty if year 1 profitable |

**VIX Regime Rules:**

| VIX Level | Regime | Action |
|-----------|--------|--------|
| < 13 | Low vol | Avoid or use closer strikes |
| 13–18 | Sweet spot | Best for short strangle |
| 18–25 | High | Wider strikes, reduce size |
| > 25 | Extreme | Avoid or sit out |

**Hard Rule:** Exit every position by 3:20 PM IST. No exceptions.

---

## Next Steps (Backlog)

- [ ] Parallel symbol downloads (replace serial loop with ThreadPoolExecutor)
- [ ] NSE options chain agent (5-min scraper → MinIO → ClickHouse)
- [ ] Live market agent (Nifty spot + VIX every 1 min)
- [ ] FII/DII daily agent (NSE website scraper)
- [ ] Telegram bot setup for signal alerts
- [ ] Scheduler service (6:30 PM IST daily cron)
- [ ] Analysis agent — market regime classifier
- [ ] Analysis agent — strike recommendation engine
- [ ] Paper trade signal + outcome logging
- [ ] Learning agent — LinkedIn post generator
- [ ] Weekly review agent

---

## Key Commands Reference
```bash
# Start all containers
cd ~/trading-lab && docker compose up -d

# Stop all containers
docker compose down

# Run pipeline (incremental)
docker compose run --rm pipeline python main.py

# Run migrations
docker compose run --rm migrate

# Check migration status
docker compose run --rm migrate python /clickhouse/migrate.py --status

# Check schema drift
docker compose run --rm migrate python /clickhouse/migrate.py --check-drift

# ClickHouse query from terminal
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM market.ohlcv_daily"

# See all loaded symbols
docker exec -it clickhouse clickhouse-client --query \
  "SELECT market, count() as symbols, min(date), max(date) FROM market.ohlcv_daily GROUP BY market"

# Check container logs
docker compose logs clickhouse
docker compose logs pipeline
```

---

*Last updated: 2026-03-04*