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


@st.cache_data(ttl=3600)
def query_weekly(sql: str) -> pd.DataFrame:
    """1-hour cache for spread_backtest, spread_optimal, confidence_backtest — weekly writes only."""
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
page = st.sidebar.radio("Navigate", [
    "Overview",
    "Confidence Scores",
    "Strategy Backtests",
    "Trade Log",
    "Market Pulse",
    "Live Trading",
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


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — CONFIDENCE SCORES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Confidence Scores":
    st.title("Confidence Scores — XGBoost 0DTE Straddle")
    st.caption(
        "Per-symbol XGBoost models trained on 24–28 months of weekly expiry data. "
        "Score = probability that selling ATM straddle on next expiry will be profitable."
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

            straddle_data = query_weekly(f"""
                SELECT expiry, pnl_pts
                FROM analysis.spread_backtest FINAL
                WHERE symbol = '{sym_sel}' AND strategy = 'straddle'
                ORDER BY expiry
            """)

            fig3 = go.Figure()
            for label, df_src, clr in [
                ("Straddle (no hedge)", straddle_data, "#e74c3c"),
            ]:
                if not df_src.empty:
                    df_src["expiry"] = pd.to_datetime(df_src["expiry"])
                    df_src = df_src.sort_values("expiry")
                    fig3.add_trace(go.Scatter(
                        x=df_src["expiry"], y=df_src["pnl_pts"].cumsum(),
                        mode="lines", name=label, line_color=clr, line_dash="dash",
                    ))

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

    trades = query("""
        SELECT strategy, symbol, entry_month, exit_month,
               hold_months, entry_px, exit_px, raw_ret, net_ret, is_open
        FROM analysis.strategy_trades FINAL
        ORDER BY entry_month DESC
        LIMIT 500
    """)

    if trades.empty:
        st.info("No trades found.")
    else:
        trades["P&L (₹)"]   = (trades["net_ret"] * STARTING_CAPITAL).round(0)
        trades["Return %"]  = (trades["net_ret"] * 100).round(2)
        trades["Status"]    = trades["is_open"].apply(lambda x: "Open" if x else "Closed")

        # ── Filters ──────────────────────────────────────────────────────────
        col1, col2 = st.columns(2)
        strategies = ["All"] + trades["strategy"].unique().tolist()
        sel_strat  = col1.selectbox("Strategy", strategies)
        sel_status = col2.selectbox("Status", ["All", "Open", "Closed"])

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
        st.caption(f"{total} trades shown | Win rate: {wins/total*100:.1f}% | "
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
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Symbol",     r["symbol"])
        c2.metric("Strategy",   r["strategy"].replace("_", " ").title())
        c3.metric("Confidence", f"{r['confidence']:.1f}/100")
        c4.metric("Outcome",    r["outcome"].upper())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Net Credit",      f"{r['net_credit']:.2f} pts")
        c2.metric("Max Loss",        f"{r['max_loss']:.2f} pts")
        c3.metric("Lots",            int(r["lots"]))
        c4.metric("Capital at Risk", fmt_inr(float(r["capital_at_risk"])))

        with st.expander("Leg Strikes"):
            st.json({
                "ATM":      r["atm_strike"],
                "Short CE": r["short_ce_strike"], "Short PE": r["short_pe_strike"],
                "Long CE":  r["long_ce_strike"],  "Long PE":  r["long_pe_strike"],
            })

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

# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(60)
st.rerun()
