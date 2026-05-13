# Backtesting Specification — Iron Fly (0DTE)
**Version:** 1.0  **Date:** 2026-05-13

## 1. Goal

Produce a walk-forward backtest of an ATM iron fly on NSE weekly index options, covering Jan 2019 to present, with **zero lookahead bias**, that generates a daily prediction for the next expiry and evaluates whether net P&L ≥ 1% of SPAN margin blocked.

The confidence scorer must be trained using only data available *before* the prediction date. The model must not see the outcome of the trade it is predicting.

---

## 2. Universe

| Symbol      | Expiry Day | Lot Size | Wing Pts |
|-------------|------------|----------|----------|
| NIFTY       | Thursday   | 65       | 200      |
| BANKNIFTY   | Wednesday  | 30       | 500      |
| FINNIFTY    | Tuesday    | 60       | 200      |
| MIDCPNIFTY  | Monday     | per DB   | TBD      |

Historical data source: NSE F&O Bhavcopy from 2019-01-01.

---

## 3. Trade Structure

```
Entry:  Sell ATM CE + Sell ATM PE (straddle)
        Buy ATM+wing_pts CE (call wing)
        Buy ATM-wing_pts PE (put wing)

Net premium collected = straddle_premium - wing_ce_ltp - wing_pe_ltp

Max profit = net_premium_collected (if spot stays within wings at expiry)
Max loss   = wing_pts - net_premium  (per lot, per side)
```

### Entry Rules
- Entry time: 09:30–09:45 IST (opening auction settles, first stable price)
- ATM strike: nearest strike to spot price at entry
- Only enter if `confidence_score >= MIN_CONFIDENCE` (currently 50)
- Only enter on the expiry date of the traded contract (0DTE)

### Exit Rules (priority order)
1. **Target hit**: cumulative P&L ≥ TARGET_INR → exit all legs
2. **Trailing stop**: once target hit, trail at 75% of peak P&L. If P&L drops below 75% floor, exit.
3. **Hard stop**: cumulative P&L ≤ -STOPLOSS_INR → exit all legs
4. **EOD exit**: 15:20 IST — close all open positions regardless

---

## 4. P&L Calculation

```python
gross_pnl_pts = entry_net_premium - exit_net_premium
               # (positive when premium decays)

txn_cost_inr = (
    20 * n_legs * 1.18 +         # brokerage × GST, 4 legs
    spot_at_entry * lot_size * 0.0005  # STT 0.05% on sell side
    + spot_at_entry * lot_size * 0.0005  # exchange 0.05%
)

net_pnl_inr = (gross_pnl_pts * lot_size) - txn_cost_inr
```

### WIN Threshold
```
span_estimate = atm_premium_at_entry * lot_size * 2 * 0.30
win = (net_pnl_inr >= 0.01 * span_estimate)
```

`0.30` is a conservative SPAN fraction (30% margin of notional). Replace with actual NSE SPAN when available.

---

## 5. Walk-Forward Cross-Validation (Strict Temporal Isolation)

```
TRAIN_MONTHS = 18   # rolling training window
TEST_MONTHS  = 1    # walk-forward step
MIN_TRAIN    = 60   # minimum trades in training fold
```

### Split Rule (NO LOOKAHEAD)
```
For each fold:
  test_start  = fold_start_date        (a Monday at start of month)
  train_end   = test_start - 1 day

  train_rows = all rows where entry_date <  test_start
  test_rows  = all rows where entry_date >= test_start
               AND entry_date <  test_start + TEST_MONTHS months
```

**Critical**: The split boundary is `entry_date` (when the trade was actually entered), NOT `expiry_dt`. A trade entered on 2023-06-15 with expiry 2023-06-22 must appear in the test fold if `test_start = 2023-06-01`.

### Feature Extraction (per training row)
All features are computed using data available strictly before `entry_date`:
- OI/IV from previous day's EOD bhavcopy
- VIX from previous close
- RSI/ATR from OHLCV up to previous close
- Participant OI (FII/client) from previous day's EOD report

Data from the trade's own entry day is allowed **only** for pre-market snapshots (09:15–09:30 IST): ATM premium, spot level, IV at open.

### Feature Set (v1)

| Feature | Source | Allowed? |
|---------|--------|----------|
| straddle_pct | ATM CE+PE LTP at 09:30 / spot | ✅ Entry day pre-market |
| ce_pe_ratio | ATM CE/PE LTP ratio | ✅ Entry day pre-market |
| pcr_oi | OI Put/Call ratio | ✅ Previous EOD |
| iv_rank | ATM IV vs 52-week range | ✅ Previous EOD |
| iv_percentile | ATM IV percentile | ✅ Previous EOD |
| atm_ce_iv / atm_pe_iv | IV at ATM strike | ✅ Previous EOD |
| iv_skew | PE IV − CE IV | ✅ Previous EOD |
| iv_hv5_ratio | IV / 5-day HV | ✅ Previous EOD |
| vix | India VIX close | ✅ Previous close |
| atr_percentile | 14-day ATR percentile | ✅ Previous close |
| rsi14 | 14-day RSI | ✅ Previous close |
| supertrend_dir | Supertrend direction | ✅ Previous close |
| hv_ratio | Realised vol / IV | ✅ Previous close |
| cpr_width_pct | Central Pivot Range width | ✅ Previous close |
| fii_call_net, fii_put_net | FII options net OI | ✅ Previous EOD |
| client_call_net, client_put_net | Client options net OI | ✅ Previous EOD |
| day_of_week, week_of_month | Calendar | ✅ Always known |
| event_in_window | RBI/FOMC in next 5 days | ✅ Known in advance |
| days_to_event | Days to nearest event | ✅ Known in advance |

**❌ NOT ALLOWED (future data):**
- Intraday price path (max/min during the trade)
- Exit price (known only after trade closes)
- Any EOD data from the entry date itself

---

## 6. Target Variable

```python
win = (net_pnl_inr >= 0.01 * span_estimate)
```

This replaces the current `target = (pnl_pts > 0)` which ignores costs.

---

## 7. Model Selection (--compare run)

The `--compare` flag runs all model variants and picks the best by mean OOS AUC:

| Model | Config |
|-------|--------|
| XGBoost per-symbol | n_estimators=200, max_depth=3, lr=0.05, subsample=0.8 |
| Logistic Regression per-symbol | C=1.0, class_weight=balanced |
| Rule-based Scorecard | Threshold scoring on 6 key features |
| XGBoost pooled (all symbols) | Same hyperparams + symbol one-hot |
| LR pooled | Same config + symbol one-hot |

Winner: highest mean OOS AUC across folds. Minimum AUC threshold = 0.55 (below this, fall back to scorecard).

---

## 8. Prediction Output (daily, before market open)

```
symbol         NIFTY
next_expiry    2026-05-15
confidence     72.4          # predicted win probability (calibrated)
expected_pnl   +₹1,840       # confidence × avg_win - (1-conf) × avg_loss
span_estimate  ₹14,300
threshold_pnl  ₹143          # 1% of SPAN
go_nogo        GO            # GO if confidence >= MIN_CONFIDENCE
missing_feats  []            # list of features that were zero/imputed
model_version  xgb_v2        # which model file was used
data_asof      2026-05-12    # latest EOD data used for features
```

The dashboard renders this with a **feature health badge**: green if `missing_feats` is empty, amber if ≤2 features missing, red if >2.

---

## 9. Validation Gates (before using prediction for live trading)

1. **Calibration check**: predicted confidence 60–70% must correlate with 60–70% actual wins in backtest. If Brier score > 0.30 → flag uncalibrated.
2. **Regime coverage**: training data must include ≥1 crash regime (VIX > 30), ≥1 rally regime, ≥1 sideways regime. Checked by bucketing training rows by VIX tertile.
3. **Minimum OOS AUC**: 0.55. Below → display "SIGNAL WEAK" on dashboard, do not trade.
4. **Data freshness**: `data_asof` must be ≤ 2 trading days old. Stale → display "DATA STALE" warning.
5. **Feature completeness**: if `missing_feats` has > 2 entries → display "FEATURES INCOMPLETE", do not trade.

---

## 10. Data Sources

| Data | Source | Frequency |
|------|--------|-----------|
| Options OHLC + OI | NSE F&O Bhavcopy (`option_chain_historical.py`) | Daily |
| Intraday chain (entry price) | jugaad-data NSELive (`option_chain_intraday.py`) | 3-min |
| VIX | `vix_pipeline.py` | Daily |
| Participant OI (FII/client) | `participant_oi_pipeline.py` | Daily |
| Index OHLCV (RSI, ATR, Supertrend) | yfinance via `compute_oi_features.py` | Daily |
| Events calendar | `events_pipeline.py` | Manual / quarterly update |
