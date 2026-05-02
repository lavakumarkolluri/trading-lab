# Trading Lab

Automated Indian equity/options data pipeline and 0DTE option-selling strategy system built on ClickHouse, MinIO, Docker Compose, and Streamlit.

---

## Services

| Service | Port | Purpose |
|---|---|---|
| `clickhouse` | 8123 (HTTP), 9009 (native) | Time-series data store |
| `minio` | 9000 (API), 9001 (console) | Raw data lake (bhavcopy, models) |
| `dashboard` | 8501 | Streamlit live dashboard |
| `vscode` | 8080 | Browser-based VS Code |
| `jupyter` | 8888 | Notebook analysis |
| `portainer` | 9443 | Docker UI |
| `scheduler` | — | Long-lived cron container |

All pipeline services (`meta_pipeline`, `option_chain_intraday`, etc.) are **ephemeral** — they run and exit via `docker compose run --rm`.

---

## Quick Start

```bash
cp .env.example .env          # fill in passwords
docker compose up -d clickhouse minio
docker compose run --rm migrate --run   # apply all migrations
docker compose up -d dashboard scheduler
```

---

## Daily Pipeline Sequence (meta_pipeline, Mon–Fri 16:30 IST)

| Step | Service | Description |
|---|---|---|
| 1 | `pipeline` | OHLCV data fetch |
| 2 | `fii_dii_pipeline` | FII/DII cash + futures flows |
| 3 | `participant_oi_pipeline` | NSE participant OI (FII/Client/Pro) |
| 4 | `vix_pipeline` | India VIX historical |
| 5 | `option_chain_pipeline` | Live NSE option chain (NIFTY/BANKNIFTY) |
| 6 | `event_detector` | Price pattern event detection |
| 7 | `pattern_feature_extractor` | Feature extraction for patterns |
| 8 | `validation_engine` | Validate past predictions |
| 9 | `pattern_builder --map-only` | Update pattern→outcome map |
| 10 | `backtest_engine` | Backtest engine run |
| 11 | `star_rater` | Rate patterns by historical accuracy |
| 12 | `prediction_engine` | Generate today's trade predictions |

## Option Chain EOD (Mon–Fri 18:00–19:30 IST, separate from meta_pipeline)

| Time (IST) | Service | Description |
|---|---|---|
| 18:00 | `option_chain_historical` | Download bhavcopy for all 4 indices |
| 18:30 | `options_eod_summary_pipeline` | PCR, max pain, IV from bhavcopy |
| 19:00 | `compute_historical_iv` | ATM IV + IV rank/percentile |
| 19:30 | `compute_oi_features` | OI walls, IV skew, FII futures |

## Weekly Jobs (Sunday UTC)

| Time (UTC) | Service |
|---|---|
| 00:30 | `meta_pipeline --weekly` (steps 13–16: full pattern remap, backtest, star rating) |
| 01:00 | `gap_analyzer` |
| 02:00 | `option_backtest` (buy + sell), `nifty_straddle_backtest` |
| 03:00 | `mf_pipeline` |
| 04:30 | `fundamental_pipeline` |
| 05:00 | `lot_size_pipeline` |
| 05:30 | `strategy_backtester` |
| 06:00 | `confidence_scorer` |
| 06:30 | `strategy_selector --backtest` |

## Daily Strategy Jobs (Mon–Thu)

| Time (UTC) | Service |
|---|---|
| 10:30 | `strategy_selector --recommend` |
| 14:00 | `strategy_selector --fill-outcomes` |

---

## 0DTE Option Selling — Build Roadmap

| Step | Status | Description |
|---|---|---|
| 1 | ✅ | Option chain historical data (NSE bhavcopy all indices) |
| 2 | ✅ | EOD summary: PCR, max pain, ATM IV, OI features |
| 3 | ✅ | XGBoost confidence scorer (walk-forward, per-symbol) |
| 4 | ✅ | Defined-risk strategy backtester (IC, bull put, bear call) |
| 5 | ✅ | Strategy selector: daily recommendation + compounding simulation |
| 6 | ⏳ | Telegram alerts (entry signal, exit reminder, daily P&L) |
| 7 | ⏳ | Zerodha MCP integration (live order placement) |
| 8 | ⏳ | Paper trading — 1 month |
| 9 | ⏳ | Live trading |

---

## Migrations

```bash
# Apply all pending migrations
docker compose run --rm migrate --run

# Check status
docker compose run --rm migrate --status
```

Migrations live in `clickhouse/migrations/`. They are numbered and idempotent (`CREATE TABLE IF NOT EXISTS`).

---

## Key Design Decisions

- **Expiry schedule**: Mon=MIDCPNIFTY, Tue=FINNIFTY, Wed=BANKNIFTY, Thu=NIFTY
- **Strike steps**: NIFTY=50, BANKNIFTY=100, FINNIFTY=50, MIDCPNIFTY=25
- **0DTE approximation**: entry = T-1 EOD prices, exit = expiry settlement (no intraday data)
- **Capital**: simulation uses ₹5L starting; pattern backtest dashboard shows ₹1L (separate contexts)
- **ReplacingMergeTree**: all analytics tables use versioned deduplication — always query with `FINAL`
- **MinIO buckets**: `trading-data` (raw bhavcopy), `ml-models` (XGBoost .ubj files)

---

## Security Notes

- ClickHouse HTTP port 8123 is LAN-exposed with password auth only — do not expose to internet
- Dashboard port 8501 has no auth — add `STREAMLIT_PASSWORD` env var or nginx basic auth before live trading
- Docker socket is bind-mounted (rw) on scheduler/meta_pipeline/portainer — required for container spawning; treat as privileged
