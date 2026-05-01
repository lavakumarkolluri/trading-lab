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


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
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
# PAGE 2 — STRATEGY BACKTESTS
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


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — TRADE LOG
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
        SELECT date, symbol, pcr, max_pain
        FROM market.options_eod_summary FINAL
        WHERE date >= today() - 90
        ORDER BY date, symbol
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
        st.subheader("Put-Call Ratio (last 90 days)")
        pcr["date"] = pd.to_datetime(pcr["date"])
        fig = px.line(pcr, x="date", y="pcr", color="symbol",
                      labels={"pcr": "PCR", "date": ""})
        fig.add_hline(y=1.3, line_dash="dash", line_color="green",
                      annotation_text="Bullish threshold")
        fig.add_hline(y=0.7, line_dash="dash", line_color="red",
                      annotation_text="Bearish threshold")
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — LIVE TRADING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Live Trading":
    st.title("Live Trading")
    st.info("Live trading not yet active. This page will show open positions, "
            "today's P&L, and real-time alerts once the Zerodha integration is live "
            "(Step 7 of the build sequence).")

    st.subheader("Risk Parameters")
    c1, c2, c3 = st.columns(3)
    c1.metric("Starting Capital",  fmt_inr(STARTING_CAPITAL))
    c2.metric("Daily Stop (2%)",   fmt_inr(STARTING_CAPITAL * DAILY_STOP_PCT))
    c3.metric("Capital Floor",     fmt_inr(CAPITAL_FLOOR))

    st.subheader("Expiry Calendar")
    cal = pd.DataFrame({
        "Day":    ["Monday",    "Tuesday", "Wednesday", "Thursday", "Friday"],
        "Index":  ["MidcapNifty + Bankex", "FinNifty", "BankNifty", "Nifty 50", "Sensex"],
        "Status": ["Pending"] * 5,
    })
    st.dataframe(cal, use_container_width=True, hide_index=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(60)
st.rerun()
