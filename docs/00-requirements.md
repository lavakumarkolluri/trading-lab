# Business Requirements — Trading Lab

**Version:** 1.0  
**Date:** 2026-06-13  
**Status:** Living document — update when strategy scope or graduation gates change.

---

## 1. Purpose

Trading Lab is a personal automated trading system for NSE index options. Its goal is to generate consistent, risk-controlled income by selling premium on weekly-expiry options (0DTE / near-expiry iron fly strategy) across four NSE indices.

The end state is a fully automated live trading system. The current phase is **paper trading**, accumulating the 30-trade history needed to pass the paper-gate before micro-live deployment.

---

## 2. Strategy in One Sentence

Sell an ATM straddle on the day before weekly expiry, buy OTM wings to cap risk (iron fly), hold to settlement — repeat each week across all four index expiries.

---

## 3. Instruments & Schedule

| Symbol | Lot Size | Expiry Day | Wing Width |
|---|---|---|---|
| MIDCPNIFTY | 120 | Monday | ±200 pts |
| NIFTY | 65 | Tuesday | ±200 pts |
| FINNIFTY | 60 | Tuesday | ±200 pts |
| BANKNIFTY | 30 | Wednesday | ±500 pts |

Up to four trades per week (one per expiry). Tuesday has two concurrent positions (NIFTY + FINNIFTY).

---

## 4. Risk Parameters

| Parameter | Value |
|---|---|
| Target P&L per trade | ₹2,000 (NIFTY / FINNIFTY) |
| Stop loss per trade | ₹1,000 |
| Trailing stop activation | At target; trails at 75% of peak profit |
| EOD forced exit | 15:20 IST on expiry day |
| Entry window | 09:30–14:00 IST |
| Minimum confidence gate | 60 / 100 (ML model score) |
| Starting paper capital | ₹5,00,000 |
| Max capital risk per trade | 2% of portfolio |
| Max lots per trade | 5 (hard cap) |

---

## 5. Current Capabilities

### 5.1 Data Pipeline

The system collects and processes the following daily:

- **Intraday options chain** — live NSE snapshots every 5 minutes during market hours (`option_chain_intraday`)
- **EOD bhavcopy** — full options settlement data for all four indices (`option_chain_historical`)
- **EOD features** — PCR, max pain, ATM IV, IV rank/percentile, OI walls (`options_eod_summary_pipeline`)
- **Historical IV** — ATM IV, IV rank, IV percentile computed from bhavcopy history (`compute_historical_iv`)
- **OI features** — OI concentration, IV skew, FII net futures position (`compute_oi_features`)
- **Index OHLCV** — daily price history for all four index underlyings
- **India VIX** — daily VIX readings for volatility regime
- **FII/DII flows** — cash and futures participation data
- **Participant OI** — FII, client, and pro net option positioning
- **Events calendar** — high-impact macroeconomic events (RBI, CPI, budget)
- **F&O lot sizes** — current contract sizes with effective-date history
- **Trading holidays** — NSE holiday calendar

### 5.2 Strategy Backtester

`strategy_backtester.py` simulates historical iron fly P&L for all four symbols using actual NSE settlement prices. It populates `analysis.spread_backtest` with per-expiry P&L that the confidence scorer uses as training labels.

Backtest covers: iron fly, iron condor, bull put spread, bear call spread — all parameterised by strike offsets so the selector can pick the best structure each week.

### 5.3 Confidence Scorer (ML Gate)

`confidence_scorer.py` trains per-symbol XGBoost / Logistic Regression / rule-based models using ~38 features drawn from:

- Options chain (straddle premium, OI concentration, PCR)
- EOD summary (IV rank, IV skew, max pain distance)
- Participant OI (FII/client net positioning)
- Technical regime (ATR percentile, RSI, HV ratio, Supertrend, ADX)
- VIX level and z-score vs 20-day mean
- IV term structure (contango/backwardation slope)
- Macro calendar (event in window, days-to-event)

Walk-forward out-of-sample validation selects the best model type weekly. Scores are written to `analysis.scorecard`. A confidence score ≥ 60 is required for entry.

### 5.4 Strategy Selector

`strategy_selector.py` runs daily to:

1. Pick the highest-confidence strategy+symbol combination for each upcoming expiry.
2. Apply a composite trade score (confidence 40%, VIX regime 20%, IV rank 20%, directional alignment 20%).
3. Gate on VIX range (11–28), minimum ML confidence (60), and minimum composite score (65).
4. Write trade recommendations to `analysis.trade_recommendations`.

### 5.5 Intraday Monitor (Paper Trading)

`intraday_monitor.py` runs every 5 minutes from 09:20–15:25 IST. It:

- Reads live option chain snapshots to compute current straddle P&L
- Evaluates entry conditions (confidence gate, premium floor, net credit floor)
- Manages position lifecycle: entry → trailing stop → EOD exit
- Places orders via Zerodha Kite API when in `--live` mode; logs paper trades otherwise
- Records all trade outcomes to `trades.trade_outcomes` / `market.paper_trades`

### 5.6 Self-Improvement Loop

Three components run after trades settle to improve model accuracy over time:

- **`loss_attributor.py`** — for each losing trade, computes per-feature attribution (how far each entry feature deviated from the winning-trade profile)
- **`drift_detector.py`** — weekly rolling Pearson correlation between each feature and trade outcome; flags when 2+ features drift > 0.15 from 180-day baseline and queues a model retrain
- **`historical_seeder.py`** — backfills `analysis.historical_trade_features` from backtester data to give the attributor and drift detector a long historical baseline

### 5.7 Graduation Gate

`graduation_gate.py` enforces a staged promotion path from paper to live:

| Stage | Label | Required |
|---|---|---|
| 1 | BACKTEST | ≥50 OOS trades, win rate ≥55%, Sharpe ≥0.10, ≥2 years history |
| 2 | PAPER | ≥30 paper trades, net PnL > 0, paper win rate within 10pp of backtest |
| 3 | MICRO_LIVE | ≥20 micro trades, net PnL > 0, micro win rate within 15pp of paper |
| 4 | FULL_LIVE | All prior gates passed |

Stage is recomputed daily and stored in `analysis.strategy_graduation`.

### 5.8 Dashboard

A Streamlit dashboard (`dashboard/app.py`) provides visibility into:

- Live paper trade P&L and open positions
- Historical backtest results and win-rate trends
- Confidence scores per symbol/strategy
- Data freshness health checks
- Graduation gate status (current stage and gate pass/fail breakdown)
- Pipeline run history and alert log

### 5.9 Infrastructure

- **ClickHouse** — primary time-series store (88 migrations; ReplacingMergeTree for all analytics tables)
- **MinIO** — raw bhavcopy files and trained ML model artifacts (XGBoost `.ubj`, Logistic Regression `.pkl`)
- **Docker Compose** — all services containerised; pipeline services are ephemeral (`--rm`)
- **Scheduler** — long-lived cron container orchestrates the daily/weekly pipeline sequence
- **CI/CD** — GitHub Actions runs 422 offline tests on every push to `stage`; auto-merges to `master` and tags `prod-YYYYMMDD-HHMM` when tests pass

---

## 6. Roadmap to Live

The system tracks six steps to production live trading:

| Step | Status | Description |
|---|---|---|
| 1–5 | ✅ Done | Historical data, EOD features, XGBoost scorer, backtester, strategy selector |
| 6 | ⏳ Pending | Telegram alerts (entry signal, exit reminder, daily P&L summary) |
| 7 | ⏳ Pending | Zerodha Kite Connect integration — code exists, not yet authorised for live |
| 8 | ⏳ In Progress | Paper trading — accumulating trade history for graduation gate (need ≥30 trades) |
| 9 | ⏳ Blocked on 8 | Live trading — requires paper gate + micro-live gate to pass |

Current blocker: paper trading started 2026-05-07 (after CH outage 1–6 May). The 30-trade paper gate requires approximately 7–8 weeks of normal weekly expiries.

---

## 7. Known Gaps & Risks

### 7.1 Data Gaps

- A ClickHouse outage from 2026-05-01 to 2026-05-06 erased five days of options chain data. Paper trades only began 2026-05-07; any confidence scores before that date used incomplete data.
- `options_eod_summary` is the sole IV/IV-rank source. If the bhavcopy pipeline fails overnight, the next day's scores use stale IV (UNRELIABLE SCORE logged and Telegram alert sent, but no automatic retry).

### 7.2 Execution Gaps

- **No Telegram alerts yet.** Entry signals and stop events are logged to ClickHouse only. Without alerts, live monitoring requires checking the dashboard manually.
- **Kite access token expires daily** (~06:00 IST). Automated refresh via `kite_auth.py` requires a one-time manual OAuth login each morning before the market opens — not yet automated.
- **NIFTY + FINNIFTY concurrent positions on Tuesday** are treated independently. No combined delta / margin netting logic exists yet.
- **Micro-live stage is not instrumented.** `compute_micro_metrics()` is a stub returning zeros; the system will never automatically advance past stage 2 (PAPER) to stage 3 (MICRO_LIVE) without implementing this.

### 7.3 Model Risks

- Training data for MIDCPNIFTY and FINNIFTY is thinner (~60–80 expiries vs ~200+ for NIFTY). Model confidence for these symbols is less reliable.
- Walk-forward uses 12-month training windows. If market regime shifts sharply (e.g., extended high-VIX period), the model may degrade before the weekly `--compare` retrain catches it. Drift detector mitigates but has a 2-feature threshold that may fire late.
- The scorecard (rule-based fallback) uses fixed IV-rank / VIX thresholds calibrated on historical data. It does not adapt to structural market changes.

### 7.4 Operational Risks

- Dashboard has **no authentication**. Exposing port 8501 to the internet before adding auth (Streamlit password or nginx basic auth) would leak portfolio state.
- Docker socket is bind-mounted read-write on `scheduler`, `meta_pipeline`, and `portainer`. A compromised container has host-level access.
- ClickHouse HTTP port 8123 is LAN-exposed with password auth only. Should not be exposed to the internet.
- Log table bloat (TTL configured at 7 days for query/part logs, 3 days for trace/metrics) — requires `log-retention.xml` to remain mounted on every ClickHouse restart.

### 7.5 Strategy Risks

- Iron fly is short gamma. Any gap open or high-volatility expiry day beyond the wing strikes causes the maximum defined loss. Lot sizing and the stop at ₹1,000 are the primary controls.
- Budget days, RBI policy days, and election results (all classified HIGH-impact in `market.events`) show elevated IV but historically remain in range. The scorer gives a +4 bonus for event weeks — this should be monitored as live trades accumulate.
- VIX filter (11–28) skips extreme low-vol and high-vol days. If VIX spends extended periods outside this range, trade frequency drops and the paper-gate timeline extends.

---

## 8. Out of Scope (MVP)

The following are explicitly deferred until after live trading is established:

- Multi-leg delta hedging during the day (Greeks tracked but hedging is not executed)
- Equity breakout and weekend theta decay strategies (backtested, graduation gate registered, but no paper trading)
- Mutual fund analytics (data pipeline and returns/risk computation exist but serve only research/reporting, not trading signals)
- Position sizing beyond 1-lot paper trades
- Portfolio-level Greeks and cross-symbol margin netting
