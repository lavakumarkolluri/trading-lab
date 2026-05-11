"""
Trading Lab — Live Dashboard
Reads from ClickHouse. Auto-refreshes every 60s.
"""

import os
import time
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import clickhouse_connect

# ── Config ────────────────────────────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

STARTING_CAPITAL = 100_000   # ₹1L
DAILY_STOP_PCT   = 0.02      # 2%
CAPITAL_FLOOR    = 5_000     # ₹5,000

st.set_page_config(
    page_title="Trading Lab",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB ────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    """Default cache: 5 min. Use query_weekly() for data that only changes on Sundays."""
    try:
        return get_ch().query_df(sql)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def query_weekly(sql: str) -> pd.DataFrame:
    """5-min cache (same as query — renamed kept for call-site clarity)."""
    try:
        return get_ch().query_df(sql)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_inr(val: float) -> str:
    if abs(val) >= 1_00_000:
        return f"₹{val/1_00_000:.2f}L"
    if abs(val) >= 1_000:
        return f"₹{val/1_000:.1f}K"
    return f"₹{val:.0f}"


def color(val: float) -> str:
    return "normal" if val >= 0 else "inverse"


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("📈 Trading Lab")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
page = st.sidebar.radio("Navigate", [
    "Overview",
    "Today's Signals",
    "Confidence Scores",
    "Strategy Backtests",
    "Trade Log",
    "Market Pulse",
    "Live Trading",
    "Paper Trades",
    "Breakout Backtest",
    "Data Freshness",
    "Fundamentals",
])
st.sidebar.markdown("---")
st.sidebar.caption(f"Auto-refresh: 60s | Capital: {fmt_inr(STARTING_CAPITAL)}")
st.sidebar.caption(f"Daily stop: {DAILY_STOP_PCT*100:.0f}% | Floor: {fmt_inr(CAPITAL_FLOOR)}")

if st.sidebar.button("Refresh Now"):
    st.cache_data.clear()
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "Overview":
    st.title("Overview")
    st.warning(
        "⚠️ **Existing backtests are preliminary — uncapped strategies.**  "
        "The nifty_straddle and option_backtest scripts use naked straddles with no spread "
        "protection, so worst-case losses are unlimited and can appear large (e.g. ₹81k). "
        "The new Iron Condor / spread strategy (Step 4) will have defined-risk structure "
        "where max loss per trade is capped by wing width. Capital floor (₹5k) is a "
        "live-trading halt rule — it does not cap individual trade losses in historical backtests.",
        icon="⚠️"
    )

    trades = query("""
        SELECT strategy, entry_month, exit_month, net_ret, is_open
        FROM analysis.strategy_trades FINAL
        ORDER BY entry_month
    """)

    runs = query("""
        SELECT strategy, period, n_trades, win_rate, avg_net_ret,
               sharpe, start_date, end_date
        FROM analysis.strategy_runs FINAL
        WHERE period = 'full'
        ORDER BY strategy
    """)

    if trades.empty:
        st.info("No backtest trades in DB yet. Run a backtest first.")
    else:
        # ── KPI row ──────────────────────────────────────────────────────────
        trades["pnl_inr"] = trades["net_ret"] * STARTING_CAPITAL
        total_pnl  = trades["pnl_inr"].sum()
        n_trades   = len(trades)
        win_rate   = (trades["net_ret"] > 0).mean() * 100
        avg_trade  = trades["pnl_inr"].mean()
        best_trade = trades["pnl_inr"].max()
        worst      = trades["pnl_inr"].min()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total P&L", fmt_inr(total_pnl), delta=f"{total_pnl/STARTING_CAPITAL*100:.1f}%")
        c2.metric("Trades", n_trades)
        c3.metric("Win Rate", f"{win_rate:.1f}%")
        c4.metric("Best Trade", fmt_inr(best_trade))
        c5.metric("Worst Trade", fmt_inr(worst))

        # ── Cumulative P&L ───────────────────────────────────────────────────
        st.subheader("Cumulative P&L")
        trades["entry_month"] = pd.to_datetime(trades["entry_month"])
        trades_sorted = trades.sort_values("entry_month")
        trades_sorted["cumulative"] = trades_sorted["pnl_inr"].cumsum() + STARTING_CAPITAL

        fig = go.Figure()
        for strat, grp in trades_sorted.groupby("strategy"):
            grp = grp.sort_values("entry_month")
            grp["cum"] = grp["pnl_inr"].cumsum() + STARTING_CAPITAL
            fig.add_trace(go.Scatter(
                x=grp["entry_month"], y=grp["cum"],
                mode="lines", name=strat, hovertemplate="₹%{y:,.0f}<extra></extra>"
            ))
        fig.add_hline(y=STARTING_CAPITAL, line_dash="dash", line_color="gray",
                      annotation_text="Starting ₹1L")
        fig.add_hline(y=CAPITAL_FLOOR, line_dash="dot", line_color="red",
                      annotation_text="Floor")
        fig.update_layout(height=350, yaxis_tickformat="₹,.0f",
                          legend=dict(orientation="h", yanchor="bottom", y=1))
        st.plotly_chart(fig, use_container_width=True)

        # ── Strategy summary table ───────────────────────────────────────────
        st.subheader("Strategy Summary")
        if not runs.empty:
            runs["Win Rate"] = (runs["win_rate"] * 100).round(1).astype(str) + "%"
            runs["Avg P&L"]  = (runs["avg_net_ret"] * STARTING_CAPITAL).round(0).apply(fmt_inr)
            runs["Sharpe"]   = runs["sharpe"].round(2)
            runs["Period"]   = runs["start_date"].astype(str) + " → " + runs["end_date"].astype(str)
            st.dataframe(
                runs[["strategy", "n_trades", "Win Rate", "Avg P&L", "Sharpe", "Period"]],
                use_container_width=True, hide_index=True
            )

    # ── Pipeline Health grid ─────────────────────────────────────────────────
    st.subheader("Pipeline Health — Last 7 Days")

    health_df = query("""
        SELECT
            service,
            run_date,
            status,
            rows_written,
            error_msg,
            git_sha,
            dateDiff('second', started_at, finished_at) AS duration_s
        FROM system_meta.pipeline_runs FINAL
        WHERE run_date >= today() - 7
        ORDER BY run_date DESC, service
    """)

    if health_df.empty:
        st.info("No pipeline run records yet. Runs are logged to system_meta.pipeline_runs.")
    else:
        health_df["run_date"] = pd.to_datetime(health_df["run_date"]).dt.date

        # Build pivot: rows = service, columns = date
        services = sorted(health_df["service"].unique())
        dates    = sorted(health_df["run_date"].unique(), reverse=True)[:7]

        STATUS_COLOUR = {
            "success": "🟢",
            "failed":  "🔴",
            "running": "🟡",
            "skipped": "⬜",
        }

        grid_rows = []
        detail_rows = []
        for svc in services:
            row = {"Service": svc}
            for d in dates:
                cell = health_df[(health_df["service"] == svc) & (health_df["run_date"] == d)]
                if cell.empty:
                    row[str(d)] = "⚫"
                else:
                    # pick last status for the day
                    rec = cell.sort_values("run_date").iloc[-1]
                    icon = STATUS_COLOUR.get(rec["status"], "❓")
                    row[str(d)] = icon
                    if rec["status"] == "failed":
                        detail_rows.append({
                            "service":    svc,
                            "date":       str(d),
                            "error_msg":  rec["error_msg"] or "—",
                            "duration_s": int(rec["duration_s"]),
                            "git_sha":    rec["git_sha"],
                            "rows":       int(rec["rows_written"]),
                        })
            grid_rows.append(row)

        grid = pd.DataFrame(grid_rows).set_index("Service")
        st.dataframe(grid, use_container_width=True)
        st.caption("🟢 success  🔴 failed  🟡 running  ⬜ skipped  ⚫ no record")

        # ── Failure drill-down (task 12) ─────────────────────────────────────
        if detail_rows:
            st.subheader("Recent Failures")
            fail_df = pd.DataFrame(detail_rows)
            fail_df.columns = ["Service", "Date", "Error", "Duration (s)", "Git SHA", "Rows Written"]
            st.dataframe(fail_df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE — TODAY'S SIGNALS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Today's Signals":
    import json as _json
    from datetime import date as _date

    WEEKDAY_SYMBOL = {0: "MIDCPNIFTY", 1: "FINNIFTY", 2: "BANKNIFTY", 3: "NIFTY"}
    PCR_BULLISH = 1.2; PCR_BEARISH = 0.8
    VIX_MIN = 11.0; VIX_MAX = 28.0; VIX_SWEET_LO = 14.0; VIX_SWEET_HI = 22.0
    IV_SKEW_THRESH = 2.0; TRADE_SCORE_THRESHOLD = 65.0

    def _trade_score(conf, vix, iv_rank, pcr, iv_skew):
        s_conf = min(conf, 100) / 100 * 40
        if vix is None:
            s_vix = 10.0
        elif VIX_SWEET_LO <= vix <= VIX_SWEET_HI:
            s_vix = 20.0
        elif VIX_MIN <= vix <= VIX_MAX:
            s_vix = 10.0
        else:
            s_vix = 0.0
        s_ivr = min(iv_rank, 100) / 100 * 20
        pcr_bull = pcr > PCR_BULLISH; pcr_bear = pcr < PCR_BEARISH
        skew_bull = iv_skew > IV_SKEW_THRESH; skew_bear = iv_skew < -IV_SKEW_THRESH
        if (pcr_bull and skew_bull) or (pcr_bear and skew_bear):
            s_align = 20.0
        elif pcr_bull or pcr_bear:
            s_align = 10.0
        else:
            s_align = 0.0
        return s_conf + s_vix + s_ivr + s_align, s_conf, s_vix, s_ivr, s_align

    today = _date.today()
    weekday = today.weekday()
    symbol = WEEKDAY_SYMBOL.get(weekday)

    st.title("Today's Signals")
    if symbol is None:
        st.info(f"No expiry today (weekday={weekday}). Markets open Mon–Thu for 0DTE signals.")
        st.stop()

    st.caption(f"Expiry symbol for today ({today.strftime('%A')}): **{symbol}**")

    # ── Fetch all signals ─────────────────────────────────────────────────────
    conf_df = query(f"""
        SELECT confidence, next_expiry, features_json
        FROM analysis.confidence_scores FINAL
        WHERE score_date >= today() - 7 AND symbol = '{symbol}'
        ORDER BY score_date DESC, version DESC LIMIT 1
    """)

    vix_df = query("""
        SELECT vix, nifty_spot, toDate(timestamp) AS snap_date
        FROM market.nifty_live FINAL
        ORDER BY timestamp DESC LIMIT 1
    """)

    if conf_df.empty:
        st.warning(f"No confidence score for {symbol} yet. Run `confidence_scorer` first.")
        st.stop()

    conf_row   = conf_df.iloc[0]
    confidence = float(conf_row["confidence"])
    expiry     = str(conf_row["next_expiry"])[:10]  # strip time part — CH Date column needs 'YYYY-MM-DD'
    feats      = _json.loads(conf_row["features_json"]) if conf_row["features_json"] else {}
    pcr        = float(feats.get("pcr_oi", 1.0))
    iv_skew_f  = float(feats.get("iv_skew", 0.0))

    vix_val   = float(vix_df["vix"].iloc[0])     if not vix_df.empty else None
    nifty_spt = float(vix_df["nifty_spot"].iloc[0]) if not vix_df.empty else None
    snap_date = vix_df["snap_date"].iloc[0]      if not vix_df.empty else None

    eod_df = query(f"""
        SELECT iv_rank, max_pain_strike,
               ifNull(ce_wall_strike, 0) AS ce_wall,
               ifNull(pe_wall_strike, 0) AS pe_wall,
               ifNull(iv_skew, 0) AS iv_skew_eod,
               pcr AS pcr_eod
        FROM market.options_eod_summary FINAL
        WHERE expiry = '{expiry}'
        ORDER BY date DESC LIMIT 1
    """)

    if not eod_df.empty:
        eod = eod_df.iloc[0]
        iv_rank   = float(eod["iv_rank"]   or 50.0)
        max_pain  = float(eod["max_pain_strike"] or 0.0)
        ce_wall   = float(eod["ce_wall"]   or 0.0)
        pe_wall   = float(eod["pe_wall"]   or 0.0)
        iv_skew_eod = float(eod["iv_skew_eod"] or 0.0)
        pcr_eod   = float(eod["pcr_eod"]   or pcr)
    else:
        iv_rank = 50.0; max_pain = 0.0; ce_wall = 0.0; pe_wall = 0.0
        iv_skew_eod = iv_skew_f; pcr_eod = pcr

    # Use best available iv_skew
    iv_skew = iv_skew_eod if iv_skew_eod != 0.0 else iv_skew_f
    pcr_use = pcr_eod if pcr_eod != pcr else pcr

    score, s_conf, s_vix, s_ivr, s_align = _trade_score(confidence, vix_val, iv_rank, pcr_use, iv_skew)

    # ── GO / NO-GO banner ─────────────────────────────────────────────────────
    go_signal = score >= TRADE_SCORE_THRESHOLD
    vix_ok = vix_val is None or (VIX_MIN <= vix_val <= VIX_MAX)

    if go_signal and vix_ok:
        st.success(f"## ✅ GO — Trade {symbol} | Score: **{score:.1f} / 100**")
    elif not vix_ok:
        st.error(f"## 🚫 NO-GO — VIX {vix_val:.1f} outside safe range [{VIX_MIN}–{VIX_MAX}]")
    else:
        st.error(f"## ❌ NO-GO — Score {score:.1f} < {TRADE_SCORE_THRESHOLD} threshold")

    # ── Score gauge ───────────────────────────────────────────────────────────
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": "Trade Score / 100"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#2ecc71" if go_signal else "#e74c3c"},
            "steps": [
                {"range": [0, 50],   "color": "#fadbd8"},
                {"range": [50, 65],  "color": "#fdebd0"},
                {"range": [65, 100], "color": "#d5f5e3"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 3},
                "thickness": 0.75,
                "value": TRADE_SCORE_THRESHOLD,
            },
        },
    ))
    fig_gauge.update_layout(height=260, margin=dict(t=40, b=0, l=20, r=20))

    col_gauge, col_break = st.columns([1, 2])
    with col_gauge:
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_break:
        st.markdown("**Score Breakdown**")
        breakdown_data = [
            {"Component": "Confidence",    "Score": f"{s_conf:.1f}/40",
             "Value": f"{confidence:.1f}/100",
             "Status": "🟢" if s_conf >= 28 else "🟡" if s_conf >= 20 else "🔴"},
            {"Component": "VIX Zone",      "Score": f"{s_vix:.1f}/20",
             "Value": f"{vix_val:.1f}" if vix_val else "n/a",
             "Status": "🟢" if s_vix >= 20 else "🟡" if s_vix >= 10 else "🔴"},
            {"Component": "IV Rank",       "Score": f"{s_ivr:.1f}/20",
             "Value": f"{iv_rank:.0f}%",
             "Status": "🟢" if s_ivr >= 15 else "🟡" if s_ivr >= 8 else "🔴"},
            {"Component": "PCR+Skew Align","Score": f"{s_align:.1f}/20",
             "Value": f"PCR={pcr_use:.2f} · skew={iv_skew:+.2f}",
             "Status": "🟢" if s_align >= 20 else "🟡" if s_align >= 10 else "🔴"},
        ]
        st.dataframe(pd.DataFrame(breakdown_data), use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Market snapshot ───────────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Market Snapshot**")
        pcr_label = "Bullish 🟢" if pcr_use > PCR_BULLISH else "Bearish 🔴" if pcr_use < PCR_BEARISH else "Neutral ⚪"
        skew_label = "Fear (PE prem) 🔴" if iv_skew > IV_SKEW_THRESH else "Greed (CE prem) 🟢" if iv_skew < -IV_SKEW_THRESH else "Balanced ⚪"
        vix_label = ("🟢 Sweet spot" if vix_val and VIX_SWEET_LO <= vix_val <= VIX_SWEET_HI
                     else "🟡 Borderline" if vix_val and VIX_MIN <= vix_val <= VIX_MAX
                     else "🔴 Outside gate" if vix_val else "—")
        iv_rank_mult = 0.5 + iv_rank / 100
        snap_rows = [
            {"Signal": "Nifty Spot",  "Value": f"{nifty_spt:,.0f}" if nifty_spt else "—", "Context": ""},
            {"Signal": "VIX",         "Value": f"{vix_val:.2f}" if vix_val else "—",       "Context": vix_label},
            {"Signal": "PCR (OI)",    "Value": f"{pcr_use:.3f}",                            "Context": pcr_label},
            {"Signal": "IV Skew",     "Value": f"{iv_skew:+.2f}%",                          "Context": skew_label},
            {"Signal": "IV Rank",     "Value": f"{iv_rank:.0f}%",                           "Context": f"Lot mult = {iv_rank_mult:.2f}×"},
            {"Signal": "Max Pain",    "Value": f"{max_pain:,.0f}" if max_pain else "—",     "Context": "Gravitational strike"},
        ]
        st.dataframe(pd.DataFrame(snap_rows), use_container_width=True, hide_index=True)

    with col2:
        st.markdown("**OI Wall Guard**")
        # Show where short strikes would land vs walls
        lot_size_df = query(f"SELECT lot_size FROM market.fo_lot_sizes WHERE symbol='{symbol}' ORDER BY effective_from DESC LIMIT 1")
        params_df   = query_weekly(f"""
            SELECT strategy, short_n, wing_m, avg_max_loss
            FROM analysis.spread_optimal FINAL
            WHERE symbol='{symbol}' AND n_trades >= 10
            ORDER BY sharpe_pct DESC LIMIT 3
        """)
        if not params_df.empty:
            # Pick strategy
            pcr_bull = pcr_use > PCR_BULLISH; pcr_bear = pcr_use < PCR_BEARISH
            skew_bear_b = iv_skew < -IV_SKEW_THRESH; skew_bull_b = iv_skew > IV_SKEW_THRESH
            if pcr_bull and not skew_bear_b:   chosen_strat = "bull_put"
            elif pcr_bear and not skew_bull_b: chosen_strat = "bear_call"
            else:                              chosen_strat = "iron_condor"

            best = params_df[params_df["strategy"] == chosen_strat]
            if best.empty:
                best = params_df.iloc[[0]]
            p = best.iloc[0]
            sn = int(p["short_n"]); wm = int(p["wing_m"])

            # Get chain snapshot for step size
            chain_df = query(f"""
                SELECT strike FROM market.options_chain
                WHERE symbol='{symbol}' AND expiry='{expiry}'
                  AND option_type='CE' AND ltp > 0.05
                ORDER BY toDate(timestamp) DESC, strike LIMIT 30
            """)
            if not chain_df.empty and len(chain_df) >= 2:
                strikes = sorted(chain_df["strike"].unique())
                diffs = [strikes[i+1]-strikes[i] for i in range(len(strikes)-1) if strikes[i+1]>strikes[i]]
                step = float(max(set(diffs), key=diffs.count)) if diffs else 50.0
            else:
                step = {"NIFTY":50,"BANKNIFTY":100,"FINNIFTY":50,"MIDCPNIFTY":25}.get(symbol, 50)

            spot = nifty_spt or 0
            short_ce = spot + sn * step
            short_pe = spot - sn * step

            wall_rows = []
            if chosen_strat in ("iron_condor", "bear_call") and ce_wall > 0:
                ce_ok = short_ce >= ce_wall
                wall_rows.append({
                    "Leg": f"Short CE (~{short_ce:,.0f})",
                    "OI Wall": f"CE wall {ce_wall:,.0f}",
                    "Pass": "✅ Beyond wall" if ce_ok else "⛔ Inside wall",
                })
            if chosen_strat in ("iron_condor", "bull_put") and pe_wall > 0:
                pe_ok = short_pe <= pe_wall
                wall_rows.append({
                    "Leg": f"Short PE (~{short_pe:,.0f})",
                    "OI Wall": f"PE wall {pe_wall:,.0f}",
                    "Pass": "✅ Beyond wall" if pe_ok else "⛔ Inside wall",
                })
            if wall_rows:
                st.markdown(f"Strategy: **{chosen_strat.replace('_',' ').title()}** · sn={sn} · wm={wm}")
                st.dataframe(pd.DataFrame(wall_rows), use_container_width=True, hide_index=True)
            else:
                st.info("OI wall data not available yet (run compute_oi_features).")
        else:
            st.info("No optimal params yet for this symbol.")

    st.markdown("---")
    st.caption(f"Data as of: snap_date={snap_date} · expiry={expiry} · "
               f"confidence from score_date {conf_df['next_expiry'].iloc[0] if not conf_df.empty else '—'}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — CONFIDENCE SCORES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Confidence Scores":
    st.title("Confidence Scores — XGBoost 0DTE Straddle")
    st.caption(
        "Per-symbol XGBoost models trained on 24–28 months of weekly expiry data. "
        "Score = probability (0–100) that selling ATM straddle on next expiry will be profitable. "
        "Threshold ≥55 to trade. Walk-forward OOS AUC: NIFTY≈0.55, FINNIFTY≈0.62, "
        "BANKNIFTY≈0.47, MIDCPNIFTY≈0.44. Models are retrained weekly (Sunday)."
    )

    # ── Today's scores ───────────────────────────────────────────────────────
    scores = query("""
        SELECT symbol, next_expiry, round(confidence, 1) AS confidence,
               round(expected_pnl_pct, 1) AS expected_pnl_pct
        FROM analysis.confidence_scores FINAL
        WHERE score_date = today()
        ORDER BY confidence DESC
    """)

    if scores.empty:
        st.info("No confidence scores for today yet. Run: docker compose run --rm confidence_scorer")
    else:
        st.subheader("Today's Signal")
        cols = st.columns(len(scores))
        for i, (_, row) in enumerate(scores.iterrows()):
            conf = float(row["confidence"])
            color_str = "🟢" if conf >= 70 else "🟡" if conf >= 55 else "🔴"
            action = "TRADE" if conf >= 65 else "SKIP"
            cols[i].metric(
                label=f"{color_str} {row['symbol']}",
                value=f"{conf:.0f}/100",
                delta=f"{action} — Expiry {row['next_expiry']}",
            )

        st.dataframe(
            scores.rename(columns={
                "symbol": "Symbol", "next_expiry": "Next Expiry",
                "confidence": "Confidence /100", "expected_pnl_pct": "Expected P&L %"
            }),
            use_container_width=True, hide_index=True,
        )

    # ── Walk-forward backtest ─────────────────────────────────────────────────
    st.subheader("Walk-Forward Backtest Results")
    st.info(
        "⚠️ **These are raw naked ATM straddle results — used only to train the ML model, not actual trades.**  \n"
        "The model learns which days are profitable for option selling. "
        "Large % losses (e.g. -200%) occur when the market moves beyond the collected premium — "
        "this is why we **never take straddles live**. Actual trades use hedged spreads (IC / bull_put / bear_call) "
        "with defined max loss. See **Live Trading → Compounding Simulation** for real strategy P&L."
    )
    bt = query_weekly("""
        SELECT symbol, expiry, entry_date, atm_strike, entry_premium,
               pnl_pts, pnl_pct, target, round(confidence * 100, 1) AS confidence
        FROM analysis.confidence_backtest FINAL
        ORDER BY symbol, expiry
    """)

    if bt.empty:
        st.info("No backtest data yet.")
    else:
        symbol_filter = st.selectbox("Symbol", ["All"] + sorted(bt["symbol"].unique().tolist()))
        bt_view = bt if symbol_filter == "All" else bt[bt["symbol"] == symbol_filter]

        # Summary KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("OOS Predictions", len(bt_view))
        k2.metric("Win Rate (actual)",  f"{bt_view['target'].mean():.1%}")
        k3.metric("Avg Confidence",  f"{bt_view['confidence'].mean():.1f}/100")
        k4.metric("Avg P&L (pts)",   f"{bt_view['pnl_pts'].mean():.1f}")

        # Confidence vs outcome scatter
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Confidence vs Actual P&L**")
            fig = go.Figure()
            for outcome, label, marker_color in [(1, "Win", "#2ecc71"), (0, "Loss", "#e74c3c")]:
                grp = bt_view[bt_view["target"] == outcome]
                fig.add_trace(go.Scatter(
                    x=grp["confidence"], y=grp["pnl_pts"],
                    mode="markers", name=label,
                    marker=dict(color=marker_color, size=7, opacity=0.7),
                    hovertemplate="%{customdata[0]} | conf=%{x} | pnl=%{y:.0f}pts<extra></extra>",
                    customdata=grp[["symbol"]].values,
                ))
            fig.add_vline(x=65, line_dash="dash", line_color="gray",
                          annotation_text="Threshold 65")
            fig.update_layout(height=320, xaxis_title="Confidence Score",
                              yaxis_title="P&L (points)")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Win Rate by Confidence Band**")
            bt_view = bt_view.copy()
            bt_view["band"] = pd.cut(
                bt_view["confidence"],
                bins=[0, 50, 60, 70, 80, 100],
                labels=["<50", "50–60", "60–70", "70–80", ">80"]
            )
            band_stats = bt_view.groupby("band", observed=True).agg(
                n=("target", "count"),
                win_rate=("target", "mean"),
                avg_pnl=("pnl_pts", "mean")
            ).reset_index()
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=band_stats["band"].astype(str),
                y=(band_stats["win_rate"] * 100).round(1),
                text=(band_stats["win_rate"] * 100).round(1).astype(str) + "%",
                textposition="outside",
                marker_color=["#e74c3c", "#e67e22", "#f1c40f", "#2ecc71", "#27ae60"],
            ))
            fig2.add_hline(y=50, line_dash="dash", line_color="gray")
            fig2.update_layout(height=320, yaxis_title="Win Rate %",
                               xaxis_title="Confidence Band", yaxis_range=[0, 110])
            st.plotly_chart(fig2, use_container_width=True)

        # P&L over time
        st.markdown("**Cumulative P&L by Confidence Threshold (≥65 vs all)**")
        bt_sorted = bt_view.sort_values("expiry")
        bt_sorted["expiry"] = pd.to_datetime(bt_sorted["expiry"])
        fig3 = go.Figure()
        for min_conf, label, clr in [(0, "All trades", "#aaaaaa"), (65, "Conf ≥ 65", "#3498db")]:
            grp = bt_sorted[bt_sorted["confidence"] >= min_conf].copy()
            grp["cum_pnl"] = grp["pnl_pts"].cumsum()
            fig3.add_trace(go.Scatter(
                x=grp["expiry"], y=grp["cum_pnl"],
                mode="lines", name=label, line_color=clr,
            ))
        fig3.update_layout(height=280, yaxis_title="Cumulative P&L (pts)")
        st.plotly_chart(fig3, use_container_width=True)

        # Raw table
        with st.expander("Full backtest table"):
            st.dataframe(
                bt_view[["symbol", "expiry", "entry_date", "atm_strike",
                          "entry_premium", "pnl_pts", "pnl_pct", "target", "confidence"]]
                .sort_values("expiry", ascending=False),
                use_container_width=True, hide_index=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — STRATEGY BACKTESTS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Strategy Backtests":
    st.title("Strategy Backtests")

    runs = query("""
        SELECT strategy, period, n_trades, win_rate, avg_net_ret,
               median_net_ret, best_trade, worst_trade, sharpe,
               avg_hold_months, start_date, end_date
        FROM analysis.strategy_runs FINAL
        ORDER BY strategy, period
    """)

    if runs.empty:
        st.info("No strategy runs found.")
    else:
        strategies = runs["strategy"].unique().tolist()
        selected   = st.selectbox("Strategy", strategies)
        sub        = runs[runs["strategy"] == selected]

        # ── Train / Test / Full tabs ─────────────────────────────────────────
        tabs = st.tabs([p.upper() for p in sub["period"].tolist()])
        for tab, (_, row) in zip(tabs, sub.iterrows()):
            with tab:
                c1, c2, c3, c4 = tab.columns(4)
                c1.metric("Trades",    row["n_trades"])
                c2.metric("Win Rate",  f"{row['win_rate']*100:.1f}%")
                c3.metric("Sharpe",    f"{row['sharpe']:.2f}")
                c4.metric("Avg P&L",   fmt_inr(row["avg_net_ret"] * STARTING_CAPITAL))

                c5, c6, c7 = tab.columns(3)
                c5.metric("Best",   fmt_inr(row["best_trade"]  * STARTING_CAPITAL))
                c6.metric("Worst",  fmt_inr(row["worst_trade"] * STARTING_CAPITAL))
                c7.metric("Period", f"{row['start_date']} → {row['end_date']}")

        # ── Trade distribution ───────────────────────────────────────────────
        trades = query(f"""
            SELECT entry_month, net_ret, symbol
            FROM analysis.strategy_trades FINAL
            WHERE strategy = '{selected}'
            ORDER BY entry_month
        """)

        if not trades.empty:
            trades["entry_month"] = pd.to_datetime(trades["entry_month"])
            trades["pnl_inr"]     = trades["net_ret"] * STARTING_CAPITAL
            trades["color"]       = trades["pnl_inr"].apply(lambda x: "profit" if x >= 0 else "loss")

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Monthly P&L")
                fig = go.Figure(go.Bar(
                    x=trades["entry_month"], y=trades["pnl_inr"],
                    marker_color=trades["pnl_inr"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c"),
                    hovertemplate="₹%{y:,.0f}<extra></extra>"
                ))
                fig.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("P&L Distribution")
                fig = px.histogram(trades, x="pnl_inr", nbins=30, color="color",
                                   color_discrete_map={"profit": "#2ecc71", "loss": "#e74c3c"})
                fig.update_layout(height=300, showlegend=False, bargap=0.1)
                st.plotly_chart(fig, use_container_width=True)

            # ── Drawdown ─────────────────────────────────────────────────────
            st.subheader("Drawdown")
            trades_s  = trades.sort_values("entry_month")
            cum       = trades_s["pnl_inr"].cumsum() + STARTING_CAPITAL
            peak      = cum.cummax()
            drawdown  = (cum - peak) / peak * 100

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=trades_s["entry_month"], y=drawdown,
                fill="tozeroy", line_color="#e74c3c",
                hovertemplate="%{y:.1f}%<extra></extra>"
            ))
            fig.add_hline(y=-DAILY_STOP_PCT * 100, line_dash="dash",
                          line_color="orange", annotation_text="Daily stop 2%")
            fig.update_layout(height=250, yaxis_title="Drawdown %")
            st.plotly_chart(fig, use_container_width=True)


# ── Spread Strategy Backtests ─────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Defined-Risk Strategy Backtests (IC / Spreads)")
    st.caption(
        "Entry: previous-day EOD prices. Exit: expiry settlement. "
        "All hedge leg costs deducted from net_credit before any P&L calculation."
    )

    opt = query_weekly("""
        SELECT symbol, strategy, short_n, wing_m, n_trades,
               round(win_rate*100,1) AS win_rate,
               round(avg_pnl_pts,1) AS avg_pnl_pts,
               round(avg_pnl_pct,1) AS avg_pnl_pct,
               round(sharpe_pct,2)  AS sharpe,
               round(premium_to_risk,1) AS premium_pct
        FROM analysis.spread_optimal FINAL
        ORDER BY symbol, strategy, sharpe DESC
    """)

    spread_trades = query_weekly("""
        SELECT symbol, strategy, short_n, wing_m, expiry, entry_date,
               atm_strike, net_credit, max_loss, pnl_pts, pnl_pct, target
        FROM analysis.spread_backtest FINAL
        WHERE strategy != 'straddle'
        ORDER BY symbol, expiry
    """)

    if opt.empty:
        st.info("No spread backtest data. Run: docker compose run --rm strategy_backtester")
    else:
        sym_sel = st.selectbox("Symbol", sorted(opt["symbol"].unique()), key="spr_sym")
        opt_sym = opt[opt["symbol"] == sym_sel]

        # Win rate heatmap per strategy × short_n
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Win Rate % by Strategy & Short Strike Distance**")
            pivot = opt_sym[opt_sym.strategy != "straddle"].pivot_table(
                index="strategy", columns="short_n", values="win_rate", aggfunc="max"
            )
            fig = go.Figure(go.Heatmap(
                z=pivot.values,
                x=[f"sn={c}" for c in pivot.columns],
                y=pivot.index,
                colorscale="RdYlGn",
                text=[[f"{v:.0f}%" for v in row] for row in pivot.values],
                texttemplate="%{text}",
                zmin=40, zmax=100,
            ))
            fig.update_layout(height=280)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Sharpe Ratio by Strategy & Wing Width**")
            pivot2 = opt_sym[opt_sym.strategy != "straddle"].pivot_table(
                index="strategy", columns="wing_m", values="sharpe", aggfunc="max"
            )
            fig2 = go.Figure(go.Heatmap(
                z=pivot2.values,
                x=[f"wm={c}" for c in pivot2.columns],
                y=pivot2.index,
                colorscale="RdYlGn",
                text=[[f"{v:.2f}" for v in row] for row in pivot2.values],
                texttemplate="%{text}",
            ))
            fig2.update_layout(height=280)
            st.plotly_chart(fig2, use_container_width=True)

        # Straddle vs IC P&L comparison over time
        if not spread_trades.empty:
            st.markdown(f"**Cumulative P&L: All Strategies for {sym_sel}**")
            st_sym = spread_trades[spread_trades["symbol"] == sym_sel].copy()
            st_sym["expiry"] = pd.to_datetime(st_sym["expiry"])

            # Best IC params per symbol
            best_ic = opt_sym[opt_sym.strategy == "iron_condor"].nlargest(1, "sharpe")
            best_bp = opt_sym[opt_sym.strategy == "bull_put"].nlargest(1, "sharpe")
            best_bc = opt_sym[opt_sym.strategy == "bear_call"].nlargest(1, "sharpe")

            fig3 = go.Figure()
            for (strat, sn, wm), label, clr in [
                (("iron_condor", int(best_ic["short_n"].iloc[0]), int(best_ic["wing_m"].iloc[0])),
                 "Best IC", "#3498db"),
                (("bull_put", int(best_bp["short_n"].iloc[0]), int(best_bp["wing_m"].iloc[0])),
                 "Best Bull Put", "#2ecc71"),
                (("bear_call", int(best_bc["short_n"].iloc[0]), int(best_bc["wing_m"].iloc[0])),
                 "Best Bear Call", "#f39c12"),
            ]:
                grp = st_sym[
                    (st_sym.strategy == strat) &
                    (st_sym.short_n == sn) &
                    (st_sym.wing_m == wm)
                ].sort_values("expiry")
                if not grp.empty:
                    fig3.add_trace(go.Scatter(
                        x=grp["expiry"], y=grp["pnl_pts"].cumsum(),
                        mode="lines", name=f"{label} (sn={sn}, wm={wm})", line_color=clr,
                    ))

            fig3.update_layout(height=320, yaxis_title="Cumulative P&L (pts)",
                               legend=dict(orientation="h", yanchor="bottom", y=1))
            st.plotly_chart(fig3, use_container_width=True)

        # PCR vs strategy win rate breakdown
        st.markdown("**Win Rate by PCR Band & Strategy** (validates directional bias thresholds)")
        pcr_perf = query_weekly(f"""
            SELECT
                multiIf(s.pcr < 0.8, '<0.8 (Bearish)',
                         s.pcr < 1.0, '0.8–1.0',
                         s.pcr < 1.2, '1.0–1.2',
                         '>=1.2 (Bullish)') AS pcr_band,
                sb.strategy,
                round(avg(sb.target) * 100, 1) AS win_rate,
                count()                         AS n_trades
            FROM analysis.spread_backtest AS sb FINAL
            JOIN market.options_eod_summary AS s FINAL ON s.date = sb.entry_date
            WHERE sb.strategy IN ('iron_condor', 'bull_put', 'bear_call')
              AND sb.symbol = '{sym_sel}'
            GROUP BY pcr_band, sb.strategy
            ORDER BY pcr_band, sb.strategy
        """)
        if not pcr_perf.empty:
            fig_pcr = px.bar(
                pcr_perf, x="pcr_band", y="win_rate", color="strategy",
                barmode="group", text="win_rate",
                labels={"pcr_band": "PCR at Entry", "win_rate": "Win Rate %", "strategy": "Strategy"},
                color_discrete_map={
                    "iron_condor": "#3498db", "bull_put": "#2ecc71", "bear_call": "#f39c12"
                },
            )
            fig_pcr.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="50%")
            fig_pcr.update_traces(textposition="outside")
            fig_pcr.update_layout(height=320, yaxis_range=[0, 110],
                                   legend=dict(orientation="h"))
            st.plotly_chart(fig_pcr, use_container_width=True)
            st.caption(
                "If bull_put wins more in PCR ≥1.2 bands and bear_call in PCR <0.8, "
                "the directional bias in strategy_selector.py is validated. "
                "If IC consistently outperforms across all bands, use IC only."
            )
        else:
            st.info("Not enough data to show PCR breakdown for this symbol yet.")

        # Optimal params table
        st.markdown("**Top 10 Combinations (by Sharpe)**")
        top = opt_sym[opt_sym.strategy != "straddle"].nlargest(10, "sharpe")[
            ["strategy", "short_n", "wing_m", "n_trades", "win_rate",
             "avg_pnl_pts", "avg_pnl_pct", "sharpe", "premium_pct"]
        ].rename(columns={
            "strategy": "Strategy", "short_n": "Short N", "wing_m": "Wing M",
            "n_trades": "Trades", "win_rate": "Win %", "avg_pnl_pts": "Avg P&L pts",
            "avg_pnl_pct": "Avg P&L %", "sharpe": "Sharpe", "premium_pct": "Prem/Risk %"
        })
        st.dataframe(top, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — TRADE LOG
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Trade Log":
    st.title("Trade Log")

    tab_opt, tab_pat = st.tabs(["Option Strategies", "Pattern Backtest"])

    # ── Tab 1: Option strategy trades (spread_backtest) ───────────────────────
    with tab_opt:
        opt_trades = query_weekly("""
            SELECT
                sb.symbol, sb.expiry, sb.entry_date, sb.strategy,
                sb.atm_strike, sb.short_ce_strike, sb.short_pe_strike,
                sb.long_ce_strike, sb.long_pe_strike,
                sb.net_credit, sb.max_loss, sb.pnl_pts, sb.pnl_pct, sb.target,
                round(s.pcr, 2)      AS pcr_at_entry,
                round(s.iv_skew, 2)  AS iv_skew_at_entry
            FROM analysis.spread_backtest AS sb FINAL
            LEFT JOIN market.options_eod_summary AS s FINAL ON s.date = sb.entry_date
            ORDER BY sb.expiry DESC
            LIMIT 500
        """)
        if opt_trades.empty:
            st.info("No option strategy trades found. Run strategy_backtester first.")
        else:
            opt_trades["P&L (pts)"] = opt_trades["pnl_pts"].round(1)
            opt_trades["P&L %"]     = opt_trades["pnl_pct"].round(1)
            opt_trades["Result"]    = opt_trades["target"].apply(lambda x: "Win" if x else "Loss")
            opt_trades["Max Loss"]  = opt_trades["max_loss"].round(1)
            opt_trades["Credit"]    = opt_trades["net_credit"].round(1)

            STRATEGY_LABEL = {
                "bull_put":    "Bull Put Spread",
                "bear_call":   "Bear Call Spread",
                "iron_condor": "Iron Condor",
            }

            def why_chosen(r):
                pcr = r.get("pcr_at_entry", None)
                skew = r.get("iv_skew_at_entry", None)
                parts = []
                if r.strategy == "bull_put":
                    parts.append("Bullish bias")
                    if pcr and pcr >= 1.2:
                        parts.append(f"PCR {pcr:.2f} ≥ 1.2 (put-heavy)")
                elif r.strategy == "bear_call":
                    parts.append("Bearish bias")
                    if pcr and pcr <= 0.8:
                        parts.append(f"PCR {pcr:.2f} ≤ 0.8 (call-heavy)")
                else:
                    parts.append("Neutral / range-bound")
                    if pcr:
                        parts.append(f"PCR {pcr:.2f}")
                if skew and abs(skew) > 0.01:
                    parts.append(f"IV skew {skew:+.2f}")
                return " · ".join(parts)

            def leg_strikes(r):
                if r.strategy == "bull_put":
                    return f"{int(r.short_pe_strike)}PE → {int(r.long_pe_strike)}PE hedge"
                if r.strategy == "bear_call":
                    return f"{int(r.short_ce_strike)}CE → {int(r.long_ce_strike)}CE hedge"
                return (f"{int(r.short_pe_strike)}PE→{int(r.long_pe_strike)}PE  "
                        f"{int(r.short_ce_strike)}CE→{int(r.long_ce_strike)}CE")

            opt_trades["Strategy"]   = opt_trades["strategy"].map(STRATEGY_LABEL)
            opt_trades["ATM"]        = opt_trades["atm_strike"].astype(int)
            opt_trades["Legs"]       = opt_trades.apply(leg_strikes, axis=1)
            opt_trades["Why Chosen"] = opt_trades.apply(why_chosen, axis=1)

            c1, c2, c3 = st.columns(3)
            sym_f   = c1.selectbox("Symbol",   ["All"] + sorted(opt_trades["symbol"].unique().tolist()), key="ol_sym")
            strat_f = c2.selectbox("Strategy", ["All"] + sorted(opt_trades["strategy"].unique().tolist()), key="ol_str")
            res_f   = c3.selectbox("Result",   ["All", "Win", "Loss"], key="ol_res")

            f = opt_trades.copy()
            if sym_f   != "All": f = f[f["symbol"]   == sym_f]
            if strat_f != "All": f = f[f["strategy"] == strat_f]
            if res_f   != "All": f = f[f["Result"]   == res_f]

            wins  = (f["target"] == 1).sum()
            total = len(f)
            st.caption(
                f"{total} trades | Win rate: {wins/total*100:.1f}% | "
                f"Avg credit: {f['net_credit'].mean():.1f} pts | Avg P&L: {f['pnl_pts'].mean():.1f} pts"
            )

            st.dataframe(
                f[["symbol", "expiry", "entry_date", "Strategy", "Why Chosen",
                   "ATM", "Legs", "Credit", "Max Loss", "P&L (pts)", "P&L %", "Result"]].reset_index(drop=True),
                use_container_width=True, hide_index=True,
            )

    # ── Tab 2: Pattern backtest trades (strategy_trades) ─────────────────────
    with tab_pat:
        st.caption("Equity pattern-based strategy backtest. These are directional stock trades, not option spreads.")
        trades = query("""
            SELECT strategy, symbol, entry_month, exit_month,
                   hold_months, entry_px, exit_px, raw_ret, net_ret, is_open
            FROM analysis.strategy_trades FINAL
            ORDER BY entry_month DESC
            LIMIT 500
        """)
        if trades.empty:
            st.info("No pattern trades found.")
        else:
            trades["P&L (₹)"]  = (trades["net_ret"] * STARTING_CAPITAL).round(0)
            trades["Return %"] = (trades["net_ret"] * 100).round(2)
            trades["Status"]   = trades["is_open"].apply(lambda x: "Open" if x else "Closed")

            col1, col2 = st.columns(2)
            strategies = ["All"] + trades["strategy"].unique().tolist()
            sel_strat  = col1.selectbox("Strategy", strategies, key="pt_strat")
            sel_status = col2.selectbox("Status", ["All", "Open", "Closed"], key="pt_status")

            filtered = trades.copy()
            if sel_strat  != "All": filtered = filtered[filtered["strategy"] == sel_strat]
            if sel_status != "All": filtered = filtered[filtered["Status"]   == sel_status]

            st.dataframe(
                filtered[["strategy", "symbol", "entry_month", "exit_month",
                           "Return %", "P&L (₹)", "Status"]].reset_index(drop=True),
                use_container_width=True, hide_index=True,
            )
            wins  = (filtered["net_ret"] > 0).sum()
            total = len(filtered)
            st.caption(f"{total} trades | Win rate: {wins/total*100:.1f}% | "
                       f"Total P&L: {fmt_inr(filtered['P&L (₹)'].sum())}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — MARKET PULSE
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Market Pulse":
    st.title("Market Pulse")

    vix = query("""
        SELECT toDate(timestamp) as date,
               max(vix)         as vix,
               max(nifty_spot)  as nifty_spot
        FROM market.nifty_live FINAL
        WHERE timestamp >= today() - 365
        GROUP BY date ORDER BY date
    """)

    pcr = query("""
        SELECT date, pcr, max_pain_strike AS max_pain
        FROM market.options_eod_summary FINAL
        WHERE date >= today() - 90
        ORDER BY date
    """)

    if not vix.empty:
        vix["date"] = pd.to_datetime(vix["date"])

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("India VIX")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=vix["date"], y=vix["vix"],
                                     line_color="#f39c12", name="VIX"))
            fig.add_hrect(y0=0,  y1=13, fillcolor="green",  opacity=0.05, line_width=0)
            fig.add_hrect(y0=13, y1=18, fillcolor="yellow", opacity=0.05, line_width=0)
            fig.add_hrect(y0=18, y1=25, fillcolor="orange", opacity=0.05, line_width=0)
            fig.add_hrect(y0=25, y1=80, fillcolor="red",    opacity=0.05, line_width=0)
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            latest_vix = vix["vix"].iloc[-1] if not vix.empty else 0
            regime = ("Low" if latest_vix < 13 else "Normal" if latest_vix < 18
                      else "Elevated" if latest_vix < 25 else "Extreme")
            st.metric("Current VIX", f"{latest_vix:.2f}", delta=regime)

        with col2:
            st.subheader("Nifty 50")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=vix["date"], y=vix["nifty_spot"],
                                     line_color="#3498db", name="Nifty"))
            fig.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            latest_nifty = vix["nifty_spot"].iloc[-1] if not vix.empty else 0
            prev_nifty   = vix["nifty_spot"].iloc[-2] if len(vix) > 1 else latest_nifty
            change_pct   = (latest_nifty - prev_nifty) / prev_nifty * 100 if prev_nifty else 0
            st.metric("Current Nifty", f"{latest_nifty:,.0f}", delta=f"{change_pct:+.2f}%")

    if not pcr.empty:
        st.subheader("Nifty Put-Call Ratio (last 90 days)")
        pcr["date"] = pd.to_datetime(pcr["date"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=pcr["date"], y=pcr["pcr"],
                                 line_color="#9b59b6", name="PCR"))
        fig.add_hline(y=1.3, line_dash="dash", line_color="green",
                      annotation_text="Bullish >1.3")
        fig.add_hline(y=0.7, line_dash="dash", line_color="red",
                      annotation_text="Bearish <0.7")
        fig.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — LIVE TRADING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Live Trading":
    st.title("Live Trading — Strategy Selector")

    # ── Today's recommendation ────────────────────────────────────────────────
    st.subheader("Today's Trade Recommendation")
    rec_df = query("""
        SELECT rec_date, symbol, expiry, strategy, short_n, wing_m,
               atm_strike, short_ce_strike, short_pe_strike,
               long_ce_strike, long_pe_strike,
               net_credit, max_loss, lots, capital_at_risk,
               confidence, pcr_bias, iv_skew, outcome
        FROM analysis.trade_recommendations FINAL
        ORDER BY rec_date DESC
        LIMIT 1
    """)
    if rec_df.empty:
        st.info("No recommendation yet for today. Run `strategy_selector --recommend`.")
    else:
        r = rec_df.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Symbol",     r["symbol"])
        c2.metric("Strategy",   r["strategy"].replace("_", " ").title())
        c3.metric("Confidence", f"{r['confidence']:.1f}/100")
        c4.metric("PCR Bias",   f"{r['pcr_bias']:.3f}")
        c5.metric("IV Skew",    f"{r['iv_skew']:+.2f}%")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Net Credit",      f"{r['net_credit']:.2f} pts")
        c2.metric("Max Loss",        f"{r['max_loss']:.2f} pts")
        c3.metric("Lots",            int(r["lots"]))
        c4.metric("Capital at Risk", fmt_inr(float(r["capital_at_risk"])))

        with st.expander("Leg Strikes & Signals"):
            col_l, col_r = st.columns(2)
            col_l.json({
                "ATM":      int(r["atm_strike"]),
                "Short CE": int(r["short_ce_strike"]), "Short PE": int(r["short_pe_strike"]),
                "Long CE":  int(r["long_ce_strike"]),  "Long PE":  int(r["long_pe_strike"]),
            })
            col_r.json({
                "Confidence": round(float(r["confidence"]), 1),
                "PCR Bias":   round(float(r["pcr_bias"]), 3),
                "IV Skew":    round(float(r["iv_skew"]), 2),
                "Outcome":    r["outcome"],
            })
        st.caption("➡️ Full signal breakdown available in **Today's Signals** page")

    # ── Past recommendations ──────────────────────────────────────────────────
    st.subheader("Recommendation History")
    hist_df = query("""
        SELECT rec_date, symbol, strategy, short_n, wing_m,
               net_credit, max_loss, lots,
               confidence, outcome, pnl_pts, pnl_amount
        FROM analysis.trade_recommendations FINAL
        ORDER BY rec_date DESC
        LIMIT 30
    """)
    if not hist_df.empty:
        st.dataframe(hist_df, use_container_width=True, hide_index=True)

    # ── Compounding simulation ────────────────────────────────────────────────
    st.subheader("Compounding Simulation (OOS Backtest)")
    sim_df = query("""
        SELECT sim_date, symbol, strategy, confidence, lots,
               pnl_pts, pnl_amount, capital_before, capital_after, skipped
        FROM analysis.strategy_simulation FINAL
        ORDER BY sim_date
    """)
    if sim_df.empty:
        st.info("No simulation data yet. Run `strategy_selector --backtest`.")
    else:
        sim_df["sim_date"] = pd.to_datetime(sim_df["sim_date"])
        sim_start_capital = float(sim_df["capital_before"].iloc[0])

        # Capital curve
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=sim_df["sim_date"], y=sim_df["capital_after"],
            mode="lines", name="Capital (traded)",
            line=dict(color="royalblue", width=2),
        ))
        fig.add_trace(go.Scatter(
            x=sim_df["sim_date"],
            y=[sim_start_capital] * len(sim_df),
            mode="lines", name="Starting capital",
            line=dict(color="gray", width=1, dash="dash"),
        ))
        fig.update_layout(
            height=340, title="Capital Curve (Compounding)",
            yaxis_title="Capital (₹)", xaxis_title="Date",
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Summary stats
        traded = sim_df[sim_df["skipped"] == 0]
        if not traded.empty:
            final_cap = float(sim_df["capital_after"].iloc[-1])
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Final Capital",   fmt_inr(final_cap))
            c2.metric("Total Return",    f"{(final_cap/sim_start_capital - 1)*100:.1f}%")
            c3.metric("Trades Taken",    len(traded))
            c4.metric("Trades Skipped",  int(sim_df["skipped"].sum()))

        st.dataframe(
            sim_df[["sim_date", "symbol", "strategy", "confidence",
                     "lots", "pnl_pts", "pnl_amount", "capital_after", "skipped"]].tail(30),
            use_container_width=True, hide_index=True,
        )

    # ── Risk parameters ───────────────────────────────────────────────────────
    st.subheader("Risk Parameters (Strategy Selector)")
    c1, c2, c3 = st.columns(3)
    c1.metric("Starting Capital",  fmt_inr(500_000))
    c2.metric("Max Risk / Trade",  "2% of capital")
    c3.metric("Capital Floor",     fmt_inr(50_000))

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 8 — PAPER TRADES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Paper Trades":
    st.title("Paper Trades — Intraday Straddle Monitor")
    st.caption("Auto paper trading: NIFTY + BANKNIFTY straddle, ₹2000 target (trailing), ₹1000 stop")

    # ── Open positions ────────────────────────────────────────────────────────
    st.subheader("Open Positions")
    open_df = query("""
        SELECT symbol, expiry, entry_time, strike,
               entry_ce_ltp, entry_pe_ltp, entry_premium,
               lot_size, target_inr, stoploss_inr,
               trailing_active, peak_pnl_inr, trail_stop_inr,
               scorecard_conf
        FROM trades.open_positions FINAL
        WHERE status = 'open'
        ORDER BY entry_time DESC
    """)
    if open_df.empty:
        st.info("No open positions right now.")
    else:
        for _, r in open_df.iterrows():
            sym    = r["symbol"]
            strike = float(r["strike"])
            expiry = str(r["expiry"])[:10]  # strip time part

            # Live mark-to-market from intraday chain (best-effort, values are internal/trusted)
            mark_df = query(f"""
                SELECT sumIf(ltp, option_type='CE') + sumIf(ltp, option_type='PE') AS curr
                FROM market.options_chain
                WHERE symbol   = '{sym}'
                  AND strike   = {strike}
                  AND expiry   = '{expiry}'
                  AND toDate(timestamp) = today()
                  AND timestamp = (
                      SELECT max(timestamp) FROM market.options_chain
                      WHERE symbol = '{sym}' AND toDate(timestamp) = today()
                  )
                GROUP BY symbol
                HAVING curr > 0
            """)

            curr_straddle  = float(mark_df["curr"].iloc[0]) if not mark_df.empty else None
            entry_premium  = float(r["entry_premium"])
            lot_size       = int(r["lot_size"])
            if curr_straddle is not None:
                unreal_pts = entry_premium - curr_straddle
                unreal_inr = unreal_pts * lot_size
            else:
                unreal_pts = unreal_inr = None

            with st.container(border=True):
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                c1.metric("Symbol",   sym)
                c2.metric("Strike",   f"{strike:.0f}")
                c3.metric("Premium",  f"{entry_premium:.1f} pts")
                c4.metric("Conf",     f"{r['scorecard_conf']:.0f}/100")
                c5.metric("Trailing", "Yes" if r["trailing_active"] else "No")
                if unreal_inr is not None:
                    c6.metric("Unrealized P&L", fmt_inr(unreal_inr),
                              delta=f"{unreal_pts:+.1f} pts")
                else:
                    c6.metric("Unrealized P&L", "—")

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Expiry",       str(expiry))
                c2.metric("Entry CE",     f"{r['entry_ce_ltp']:.1f}")
                c3.metric("Entry PE",     f"{r['entry_pe_ltp']:.1f}")
                c4.metric("Peak P&L",     fmt_inr(float(r["peak_pnl_inr"])))
                c5.metric("Trail Floor",  fmt_inr(float(r["trail_stop_inr"])))

    # ── Today's completed trades ───────────────────────────────────────────────
    st.subheader("Today's Completed Trades")
    today_df = query("""
        SELECT symbol, strike, expiry,
               entry_time, exit_time, exit_reason,
               entry_premium, exit_premium,
               pnl_pts, pnl_inr, lot_size, scorecard_conf
        FROM trades.trade_outcomes FINAL
        WHERE toDate(entry_time) = today()
        ORDER BY exit_time DESC
    """)
    if today_df.empty:
        st.info("No completed trades today.")
    else:
        total_inr = float(today_df["pnl_inr"].sum())
        wins  = int((today_df["pnl_inr"] > 0).sum())
        stops = int((today_df["exit_reason"] == "stop").sum())
        trails = int((today_df["exit_reason"] == "trail").sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Today P&L",  fmt_inr(total_inr), delta_color=color(total_inr))
        c2.metric("Trades",     len(today_df))
        c3.metric("Wins",       wins)
        c4.metric("Stops",      stops)
        c5.metric("Trail Exits", trails)

        display = today_df[[
            "symbol", "strike", "entry_time", "exit_time", "exit_reason",
            "entry_premium", "exit_premium", "pnl_pts", "pnl_inr", "scorecard_conf"
        ]].copy()
        display["entry_time"] = pd.to_datetime(display["entry_time"]).dt.strftime("%H:%M")
        display["exit_time"]  = pd.to_datetime(display["exit_time"]).dt.strftime("%H:%M")
        display["pnl_inr"]    = display["pnl_inr"].apply(lambda v: f"₹{v:.0f}")
        st.dataframe(display, use_container_width=True, hide_index=True)

    # ── Historical outcomes ────────────────────────────────────────────────────
    st.subheader("Historical Outcomes")
    hist_df = query("""
        SELECT toDate(entry_time) AS trade_date,
               symbol, strike, exit_reason,
               entry_premium, exit_premium,
               pnl_pts, pnl_inr, scorecard_conf
        FROM trades.trade_outcomes FINAL
        ORDER BY entry_time DESC
        LIMIT 200
    """)
    if hist_df.empty:
        st.info("No historical trade data yet.")
    else:
        hist_df["trade_date"] = pd.to_datetime(hist_df["trade_date"])

        # Cumulative P&L curve
        cum = hist_df.sort_values("trade_date").copy()
        cum["cum_pnl"] = cum["pnl_inr"].cumsum()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=cum["trade_date"], y=cum["cum_pnl"],
            mode="lines+markers", name="Cumulative P&L",
            line=dict(color="royalblue", width=2),
            fill="tozeroy",
            fillcolor="rgba(65,105,225,0.1)",
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(
            height=300, title="Cumulative P&L (₹)",
            yaxis_title="₹", xaxis_title="Date",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Summary by exit reason
        c1, c2 = st.columns(2)
        with c1:
            by_reason = hist_df.groupby("exit_reason").agg(
                trades=("pnl_inr", "count"),
                total_inr=("pnl_inr", "sum"),
                avg_inr=("pnl_inr", "mean"),
                win_rate=("pnl_inr", lambda x: (x > 0).mean() * 100),
            ).reset_index()
            st.markdown("**By Exit Reason**")
            st.dataframe(by_reason, use_container_width=True, hide_index=True)

        with c2:
            by_sym = hist_df.groupby("symbol").agg(
                trades=("pnl_inr", "count"),
                total_inr=("pnl_inr", "sum"),
                avg_inr=("pnl_inr", "mean"),
                win_rate=("pnl_inr", lambda x: (x > 0).mean() * 100),
            ).reset_index()
            st.markdown("**By Symbol**")
            st.dataframe(by_sym, use_container_width=True, hide_index=True)

        # Full trade log
        with st.expander("Full Trade Log"):
            st.dataframe(
                hist_df[["trade_date","symbol","strike","exit_reason",
                          "entry_premium","exit_premium","pnl_pts","pnl_inr","scorecard_conf"]],
                use_container_width=True, hide_index=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
elif page == "Breakout Backtest":
    st.title("Monthly Breakout — Hypothesis Backtest")
    st.caption(
        "Entry: monthly close > max(high) of prior 3 months · "
        "Exit: 2× target OR daily close < prior week's low · "
        "Universe: Indian market (178 stocks, 1996–present)"
    )

    trades = query("""
        SELECT symbol, entry_date, exit_date, entry_price, exit_price,
               exit_reason, holding_days, return_pct, xirr_annualized
        FROM analysis.monthly_breakout_trades FINAL
        ORDER BY entry_date
    """)

    if trades.empty:
        st.warning("No backtest results yet. Run: `docker compose run --rm monthly_breakout_backtest`")
    else:
        trades["entry_date"] = pd.to_datetime(trades["entry_date"])
        trades["exit_date"]  = pd.to_datetime(trades["exit_date"])
        trades["xirr_pct"]   = trades["xirr_annualized"] * 100

        n          = len(trades)
        n_target   = (trades["exit_reason"] == "target").sum()
        n_stop     = (trades["exit_reason"] == "stop").sum()
        avg_ret    = trades["return_pct"].mean()
        med_ret    = trades["return_pct"].median()
        avg_hold   = trades["holding_days"].mean()
        med_xirr   = trades["xirr_pct"].median()

        # Monthly Sharpe
        trades["exit_month"] = trades["exit_date"].dt.to_period("M")
        monthly_ret = trades.groupby("exit_month")["return_pct"].mean()
        all_months  = pd.period_range(monthly_ret.index.min(), monthly_ret.index.max(), freq="M")
        monthly_ret = monthly_ret.reindex(all_months, fill_value=0.0)
        sharpe = (monthly_ret.mean() / monthly_ret.std() * 12 ** 0.5) if monthly_ret.std() > 0 else 0

        # ── KPI row ───────────────────────────────────────────────────────────
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Total trades", f"{n:,}")
        c2.metric("Win rate (2×)", f"{n_target/n*100:.1f}%")
        c3.metric("Avg return", f"{avg_ret:.2f}%", delta=f"med {med_ret:.2f}%")
        c4.metric("Avg hold", f"{avg_hold:.0f}d")
        c5.metric("Median XIRR", f"{med_xirr:.1f}%")
        c6.metric("Sharpe", f"{sharpe:.2f}")

        verdict = "POSITIVE" if avg_ret > 0 and sharpe > 0.5 else ("MARGINAL" if avg_ret > 0 else "NEGATIVE")
        color   = {"POSITIVE": "success", "MARGINAL": "warning", "NEGATIVE": "error"}[verdict]
        getattr(st, color)(
            f"Verdict: **{verdict}** — "
            f"{n_target} trades hit 2× target ({n_target/n*100:.1f}%), "
            f"{n_stop} stopped out ({n_stop/n*100:.1f}%). "
            f"Median return {med_ret:.2f}%, Sharpe {sharpe:.2f}."
        )

        st.markdown("---")

        # ── Return distribution ───────────────────────────────────────────────
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("Return distribution")
            fig_dist = go.Figure()
            for reason, color_val in [("target", "#4caf50"), ("stop", "#f44336")]:
                sub = trades[trades["exit_reason"] == reason]["return_pct"]
                fig_dist.add_trace(go.Histogram(
                    x=sub, name=reason.capitalize(),
                    marker_color=color_val, opacity=0.7,
                    xbins=dict(size=2),
                ))
            fig_dist.update_layout(
                barmode="overlay",
                xaxis_title="Return (%)", yaxis_title="Trades",
                legend=dict(x=0.7, y=0.95),
                height=350, margin=dict(t=20, b=40),
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white",
            )
            st.plotly_chart(fig_dist, use_container_width=True)

        with col_b:
            st.subheader("Holding period distribution")
            fig_hold = go.Figure()
            for reason, color_val in [("target", "#4caf50"), ("stop", "#f44336")]:
                sub = trades[trades["exit_reason"] == reason]["holding_days"]
                fig_hold.add_trace(go.Histogram(
                    x=sub, name=reason.capitalize(),
                    marker_color=color_val, opacity=0.7,
                    xbins=dict(size=10),
                ))
            fig_hold.update_layout(
                barmode="overlay",
                xaxis_title="Holding days", yaxis_title="Trades",
                legend=dict(x=0.7, y=0.95),
                height=350, margin=dict(t=20, b=40),
                plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                font_color="white",
            )
            st.plotly_chart(fig_hold, use_container_width=True)

        # ── Cumulative equity curve ───────────────────────────────────────────
        st.subheader("Cumulative return (equal-weight, ₹1 per trade)")
        trades_sorted = trades.sort_values("exit_date")
        trades_sorted["cum_return"] = (1 + trades_sorted["return_pct"] / 100).cumprod()

        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=trades_sorted["exit_date"],
            y=trades_sorted["cum_return"],
            mode="lines",
            line=dict(color="#2196f3", width=1.5),
            name="Equity curve",
        ))
        fig_eq.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="Breakeven")
        fig_eq.update_layout(
            xaxis_title="Exit date", yaxis_title="Portfolio value (₹1 start)",
            height=350, margin=dict(t=20, b=40),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="white",
        )
        st.plotly_chart(fig_eq, use_container_width=True)

        # ── Annualised XIRR by year ───────────────────────────────────────────
        st.subheader("Median XIRR by entry year")
        trades["entry_year"] = trades["entry_date"].dt.year
        yearly = trades.groupby("entry_year").agg(
            trades=("return_pct", "count"),
            median_xirr=("xirr_pct", "median"),
            win_rate=("exit_reason", lambda x: (x == "target").mean() * 100),
        ).reset_index()

        fig_yr = go.Figure()
        fig_yr.add_trace(go.Bar(
            x=yearly["entry_year"],
            y=yearly["median_xirr"],
            marker_color=yearly["median_xirr"].apply(lambda v: "#4caf50" if v > 0 else "#f44336"),
            name="Median XIRR %",
            text=yearly["trades"].apply(lambda v: f"{v}t"),
            textposition="outside",
        ))
        fig_yr.update_layout(
            xaxis_title="Entry year", yaxis_title="Median XIRR (%)",
            height=350, margin=dict(t=30, b=40),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="white",
        )
        st.plotly_chart(fig_yr, use_container_width=True)

        # ── Top winning and losing stocks ─────────────────────────────────────
        col_w, col_l = st.columns(2)
        sym_stats = trades.groupby("symbol").agg(
            trades=("return_pct", "count"),
            avg_ret=("return_pct", "mean"),
            win_rate=("exit_reason", lambda x: (x == "target").mean() * 100),
        ).reset_index().sort_values("avg_ret", ascending=False)

        with col_w:
            st.subheader("Top 10 symbols by avg return")
            st.dataframe(sym_stats.head(10)[["symbol", "trades", "avg_ret", "win_rate"]]
                         .rename(columns={"avg_ret": "avg_ret_%", "win_rate": "win_%"})
                         .round(2),
                         use_container_width=True, hide_index=True)
        with col_l:
            st.subheader("Bottom 10 symbols")
            st.dataframe(sym_stats.tail(10)[["symbol", "trades", "avg_ret", "win_rate"]]
                         .rename(columns={"avg_ret": "avg_ret_%", "win_rate": "win_%"})
                         .round(2),
                         use_container_width=True, hide_index=True)

        with st.expander("All trades"):
            st.dataframe(
                trades[["symbol", "entry_date", "exit_date", "holding_days",
                         "entry_price", "exit_price", "return_pct", "xirr_pct", "exit_reason"]]
                .sort_values("entry_date", ascending=False)
                .round(2),
                use_container_width=True, hide_index=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
elif page == "Data Freshness":
    from datetime import date as _date, timedelta as _td
    st.title("Data Freshness")
    st.caption("Staleness thresholds: OHLCV 3d · Options chain 4d · VIX 4d · MF NAV 5d · Per-symbol lag 10d vs market max")

    today = _date.today()

    # ── Source-level freshness ────────────────────────────────────────────────
    st.subheader("Source freshness")

    ohlcv_df = query(
        "SELECT market, max(date) as last_date FROM market.ohlcv_daily FINAL GROUP BY market ORDER BY market"
    )
    mf_df = query("SELECT max(date) as last_date FROM market.mf_nav FINAL")
    vix_df = query("SELECT max(toDate(timestamp)) as last_date FROM market.nifty_live FINAL")
    chain_df = query(
        "SELECT symbol, max(toDate(timestamp)) as last_date "
        "FROM market.options_chain FINAL WHERE toHour(timestamp) = 15 GROUP BY symbol ORDER BY symbol"
    )

    THRESHOLDS = {
        "ohlcv":   3,
        "mf_nav":  5,
        "vix":     4,
        "options": 4,
    }

    rows = []

    for _, r in ohlcv_df.iterrows():
        last = r["last_date"]
        if hasattr(last, "date"):
            last = last.date()
        stale = (today - last).days
        rows.append({
            "Source": f"ohlcv_daily [{r['market']}]",
            "Last date": str(last),
            "Days stale": stale,
            "Threshold": THRESHOLDS["ohlcv"],
            "Status": "OK" if stale <= THRESHOLDS["ohlcv"] else "STALE",
        })

    if not mf_df.empty:
        last = mf_df.iloc[0]["last_date"]
        if hasattr(last, "date"):
            last = last.date()
        stale = (today - last).days
        rows.append({
            "Source": "mf_nav",
            "Last date": str(last),
            "Days stale": stale,
            "Threshold": THRESHOLDS["mf_nav"],
            "Status": "OK" if stale <= THRESHOLDS["mf_nav"] else "STALE",
        })

    if not vix_df.empty:
        last = vix_df.iloc[0]["last_date"]
        if hasattr(last, "date"):
            last = last.date()
        stale = (today - last).days
        rows.append({
            "Source": "nifty_live (VIX)",
            "Last date": str(last),
            "Days stale": stale,
            "Threshold": THRESHOLDS["vix"],
            "Status": "OK" if stale <= THRESHOLDS["vix"] else "STALE",
        })

    for _, r in chain_df.iterrows():
        last = r["last_date"]
        if hasattr(last, "date"):
            last = last.date()
        stale = (today - last).days
        rows.append({
            "Source": f"options_chain [{r['symbol']}]",
            "Last date": str(last),
            "Days stale": stale,
            "Threshold": THRESHOLDS["options"],
            "Status": "OK" if stale <= THRESHOLDS["options"] else "STALE",
        })

    src_df = pd.DataFrame(rows)

    def _color_status(val):
        return "background-color: #1a3a1a; color: #4caf50" if val == "OK" else "background-color: #3a1a1a; color: #f44336"

    def _color_stale(val):
        if val <= 1:
            return "color: #4caf50"
        if val <= 4:
            return "color: #ff9800"
        return "color: #f44336"

    styled = (
        src_df.style
        .applymap(_color_status, subset=["Status"])
        .applymap(_color_stale, subset=["Days stale"])
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    n_stale = (src_df["Status"] == "STALE").sum()
    if n_stale == 0:
        st.success("All sources are fresh")
    else:
        st.error(f"{n_stale} source(s) are stale — auto-fix runs daily at 19:00 IST")

    # ── Per-symbol lag table ──────────────────────────────────────────────────
    st.subheader("Symbols lagging >10 days behind market max")
    sym_df = query("""
        SELECT market, symbol, last_date, market_max,
               toInt32(dateDiff('day', last_date, market_max)) as days_behind
        FROM (
          SELECT market, symbol, max(date) as last_date
          FROM market.ohlcv_daily FINAL GROUP BY market, symbol
        ) t
        JOIN (
          SELECT market, max(date) as market_max
          FROM market.ohlcv_daily FINAL GROUP BY market
        ) m USING (market)
        WHERE dateDiff('day', last_date, market_max) > 10
        ORDER BY market, days_behind DESC
        LIMIT 200
    """)
    if sym_df.empty:
        st.success("No symbols lagging behind their market")
    else:
        st.warning(f"{len(sym_df)} symbol(s) lagging — may need manual investigation")
        st.dataframe(sym_df, use_container_width=True, hide_index=True)

    # ── Pipeline run history ──────────────────────────────────────────────────
    st.subheader("Recent pipeline runs (last 7 days)")
    runs_df = query("""
        SELECT service, run_date, status, duration_s, error_msg
        FROM system_meta.pipeline_runs FINAL
        WHERE run_date >= today() - 7
        ORDER BY run_date DESC, service
    """)
    if runs_df.empty:
        st.info("No pipeline run records found")
    else:
        def _run_color(val):
            if val == "success":
                return "background-color: #1a3a1a; color: #4caf50"
            if val == "partial_failure":
                return "background-color: #3a2a0a; color: #ff9800"
            return "background-color: #3a1a1a; color: #f44336"
        st.dataframe(
            runs_df.style.applymap(_run_color, subset=["status"]),
            use_container_width=True, hide_index=True,
        )


elif page == "Fundamentals":
    st.title("Fundamentals")
    st.caption("Quality screen: PE > 0, ROE > 5%, positive margins = quality stock. Data from market.fundamental_snapshot.")

    fund_df = query(
        "SELECT symbol, pe_ratio, pb_ratio, roe, roa, profit_margins, "
        "       gross_margins, operating_margins, free_cashflow, total_debt, "
        "       market_cap, eps_ttm, date "
        "FROM market.fundamental_snapshot FINAL "
        "ORDER BY market_cap DESC"
    )

    if fund_df.empty:
        st.warning("No fundamental data found. Run fundamental_pipeline first.")
    else:
        # ── Quality score ─────────────────────────────────────────────────────
        fund_df["quality"] = (
            (fund_df["pe_ratio"] > 0).astype(int) +
            (fund_df["roe"] > 0.05).astype(int) +
            (fund_df["profit_margins"] > 0).astype(int) +
            (fund_df["free_cashflow"] > 0).astype(int)
        )

        # ── KPIs ─────────────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        quality_pass = (fund_df["quality"] >= 3).sum()
        c1.metric("Total stocks", len(fund_df))
        c2.metric("Quality ≥3/4", int(quality_pass))
        c3.metric("Profitable (PE>0)", int((fund_df["pe_ratio"] > 0).sum()))
        c4.metric("Positive FCF", int((fund_df["free_cashflow"] > 0).sum()))

        st.markdown("---")

        # ── Filters ───────────────────────────────────────────────────────────
        col_f1, col_f2 = st.columns(2)
        min_quality = col_f1.slider("Min quality score (0–4)", 0, 4, 0)
        search = col_f2.text_input("Search symbol", "")

        display = fund_df[fund_df["quality"] >= min_quality].copy()
        if search:
            display = display[display["symbol"].str.contains(search.upper(), na=False)]

        # ── Format for display ────────────────────────────────────────────────
        def _pct(v):
            return f"{v*100:.1f}%" if v and abs(v) < 100 else ("—" if not v else f"{v:.1f}x")

        display_cols = display[["symbol", "pe_ratio", "pb_ratio", "roe", "profit_margins",
                                 "free_cashflow", "market_cap", "quality", "date"]].copy()
        display_cols.columns = ["Symbol", "PE", "PB", "ROE", "Net Margin",
                                  "FCF (₹)", "Mkt Cap (₹)", "Quality", "As of"]
        display_cols["ROE"] = display_cols["ROE"].apply(lambda v: f"{v*100:.1f}%")
        display_cols["Net Margin"] = display_cols["Net Margin"].apply(lambda v: f"{v*100:.1f}%")
        display_cols["FCF (₹)"] = display_cols["FCF (₹)"].apply(
            lambda v: f"{'▲' if v > 0 else '▼'} {abs(v)/1e7:.0f}Cr" if v else "—"
        )
        display_cols["Mkt Cap (₹)"] = display_cols["Mkt Cap (₹)"].apply(
            lambda v: f"{v/1e9:.0f}B" if v else "—"
        )
        display_cols["PE"] = display_cols["PE"].apply(
            lambda v: f"{v:.1f}" if v > 0 else "—"
        )

        def _color_quality(val):
            if val == 4: return "background-color: #1a7a3a; color: white"
            if val == 3: return "background-color: #4a9a5a; color: white"
            if val == 2: return "background-color: #7a7a2a; color: white"
            return "background-color: #7a2a2a; color: white"

        st.dataframe(
            display_cols.style.applymap(_color_quality, subset=["Quality"]),
            use_container_width=True, hide_index=True,
        )

        st.markdown("---")
        # ── Quality distribution chart ────────────────────────────────────────
        import plotly.express as px
        q_counts = fund_df["quality"].value_counts().sort_index().reset_index()
        q_counts.columns = ["Quality Score", "Count"]
        q_counts["Label"] = q_counts["Quality Score"].map(
            {0: "0 – Avoid", 1: "1 – Weak", 2: "2 – Fair", 3: "3 – Good", 4: "4 – Quality"}
        )
        fig = px.bar(q_counts, x="Label", y="Count",
                     color="Quality Score",
                     color_continuous_scale=["#7a2a2a", "#7a4a2a", "#7a7a2a", "#4a9a5a", "#1a7a3a"],
                     title="Fundamental Quality Distribution")
        st.plotly_chart(fig, use_container_width=True)


# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(60)
st.rerun()
