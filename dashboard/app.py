"""
Trading Lab — Dashboard
4-page design: System Health | Model | Trade Log | Market Data
"""

import json
import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import clickhouse_connect

# ── Config ────────────────────────────────────────────────────────────────────
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

MIN_CONFIDENCE = 50.0

# Features that must be non-zero for a reliable model score
CRITICAL_FEATURES = ["vix", "iv_rank", "pcr_oi", "straddle_pct", "atr_percentile", "rsi14"]

SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

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


@st.cache_data(ttl=3600)
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


def traffic_light(ok: bool, warn: bool = False) -> str:
    """Return 🟢/🟡/🔴 based on status."""
    if ok:
        return "🟢"
    if warn:
        return "🟡"
    return "🔴"


def missing_features(features_json: str) -> list[str]:
    """Return list of CRITICAL_FEATURES that are zero or missing from the JSON blob."""
    if not features_json:
        return CRITICAL_FEATURES[:]
    try:
        feats = json.loads(features_json)
    except Exception:
        return CRITICAL_FEATURES[:]
    return [f for f in CRITICAL_FEATURES if not feats.get(f, 0)]


def span_estimate_pts(features_json: str, spot: float, lot_size: int) -> float:
    """Rough SPAN estimate = ATM straddle premium × lot_size × 2 × 0.30."""
    try:
        feats = json.loads(features_json) if features_json else {}
        straddle_pct = float(feats.get("straddle_pct", 0))
        if straddle_pct > 0 and spot > 0:
            atm_premium = straddle_pct * spot / 100
            return atm_premium * lot_size * 2 * 0.30
    except Exception:
        pass
    return 0.0


LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 60, "MIDCPNIFTY": 120}

IST = timezone(timedelta(hours=5, minutes=30))


def _age_str(dt) -> str:
    """Human-readable age of a UTC datetime: 'just now', '5m ago', '3h ago', '2d ago'."""
    if dt is None:
        return "—"
    try:
        if pd.isna(dt):
            return "—"
    except Exception:
        pass
    try:
        if not isinstance(dt, datetime):
            dt = pd.Timestamp(dt).to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return "just now"
        if secs < 3600:
            return f"{secs // 60}m ago"
        if secs < 86400:
            return f"{secs // 3600}h ago"
        return f"{secs // 86400}d ago"
    except Exception:
        return "—"


def _to_ist_str(dt) -> str:
    """Format a UTC datetime as IST HH:MM string."""
    if dt is None:
        return "—"
    try:
        if pd.isna(dt):
            return "—"
    except Exception:
        pass
    try:
        if not isinstance(dt, datetime):
            dt = pd.Timestamp(dt).to_pydatetime()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).strftime("%H:%M IST")
    except Exception:
        return "—"

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("📈 Trading Lab")
if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

page = st.sidebar.radio("", [
    "System Health",
    "Model",
    "Trade Log",
    "Market Data",
])


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — SYSTEM HEALTH
# ══════════════════════════════════════════════════════════════════════════════
if page == "System Health":

    # Auto-refresh every 60 min (browser-level reload)
    st.markdown('<meta http-equiv="refresh" content="3600">', unsafe_allow_html=True)

    # Track when the current session first loaded this page
    if "health_loaded_at" not in st.session_state:
        st.session_state.health_loaded_at = datetime.now(timezone.utc)
    loaded_at_utc = st.session_state.health_loaded_at
    loaded_age    = _age_str(loaded_at_utc)

    now_ist = datetime.now(IST)
    today   = now_ist.date()

    # ── Page header bar ───────────────────────────────────────────────────────
    hdr_left, hdr_right = st.columns([7, 3])
    with hdr_left:
        st.title("🟢 System Health")
        st.caption(
            f"{now_ist.strftime('%A, %d %B %Y · %H:%M IST')}  ·  "
            f"Page loaded {loaded_age}  ·  data cache refreshes every 60 min"
        )
    with hdr_right:
        st.markdown("")  # vertical padding
        if st.button("🔄 Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.session_state.health_loaded_at = datetime.now(timezone.utc)
            st.rerun()

    st.divider()

    # ── Current branch (latest CI result) ─────────────────────────────────────
    st.subheader("Branch Status")

    ci_df = query("""
        SELECT branch, commit_sha, commit_msg, commit_author,
               tests_passed, tests_failed, tests_total, duration_s,
               failed_tests, status, run_at
        FROM system_meta.ci_results FINAL
        ORDER BY run_at DESC
        LIMIT 1
    """)

    if ci_df.empty:
        st.info("No CI results yet — scheduler records tests after each auto-deploy.")
    else:
        ci      = ci_df.iloc[0]
        n_pass  = int(ci["tests_passed"])
        n_fail  = int(ci["tests_failed"])
        n_total = int(ci["tests_total"])
        all_ok  = n_fail == 0 and n_pass > 0
        ci_icon = "🟢" if all_ok else "🔴"
        run_at_dt = pd.Timestamp(ci["run_at"]).to_pydatetime().replace(tzinfo=timezone.utc)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Branch",  str(ci["branch"]))
        c2.metric("Commit",  str(ci["commit_sha"])[:10])
        c3.metric("Message", str(ci["commit_msg"])[:35] + ("…" if len(str(ci["commit_msg"])) > 35 else ""))
        c4.metric(f"{ci_icon} Tests", f"{n_pass}/{n_total}",
                  delta=f"{n_fail} failed" if n_fail else "all pass",
                  delta_color="inverse" if n_fail else "normal")
        c5.metric("Last Run", _age_str(run_at_dt),
                  delta=f"{float(ci['duration_s']):.0f}s")

        if n_fail > 0:
            with st.expander(f"🔴 {n_fail} failing tests — expand for names"):
                st.code(str(ci["failed_tests"]))

    st.divider()

    # ── Last 7 deployments (stage → prod propagation history) ─────────────────
    st.subheader("Last 7 Deployments  (stage → prod)")
    st.caption(
        "Every auto-deploy that detects a code change runs the test suite and records here. "
        "Stage auto-merges to prod when all tests pass."
    )

    deploy_df = query("""
        SELECT branch, commit_sha, commit_msg, commit_author,
               tests_passed, tests_failed, tests_total, duration_s, status, run_at
        FROM system_meta.deploy_log
        ORDER BY run_at DESC
        LIMIT 7
    """)

    if deploy_df.empty:
        st.info(
            "No deployment history yet — records appear here after the scheduler "
            "runs its next auto-deploy cycle.",
            icon="ℹ️",
        )
    else:
        deploy_rows = []
        for _, r in deploy_df.iterrows():
            n_p = int(r["tests_passed"])
            n_f = int(r["tests_failed"])
            n_t = int(r["tests_total"])
            ok  = n_f == 0 and n_p > 0
            run_ts = pd.Timestamp(r["run_at"]).to_pydatetime().replace(tzinfo=timezone.utc)
            deploy_rows.append({
                "Date / Time (IST)": run_ts.astimezone(IST).strftime("%d %b %Y  %H:%M"),
                "Branch":            str(r["branch"]),
                "Commit":            str(r["commit_sha"])[:10],
                "Message":           str(r["commit_msg"])[:60],
                "Author":            str(r["commit_author"])[:20],
                "Tests":             f"{n_p}/{n_t}",
                "Status":            "🟢 pass" if ok else "🔴 FAIL",
                "Dur (s)":           f"{float(r['duration_s']):.0f}",
            })
        st.dataframe(pd.DataFrame(deploy_rows), use_container_width=True, hide_index=True)

    st.divider()

    # ── Pipeline Jobs ─────────────────────────────────────────────────────────
    st.subheader("Pipeline Jobs — Last 7 Days")

    runs_df = query("""
        SELECT service, run_date, status, rows_written,
               dateDiff('second', started_at, finished_at) AS duration_s,
               error_msg
        FROM system_meta.pipeline_runs FINAL
        WHERE run_date >= today() - 7
        ORDER BY run_date DESC, service
    """)

    # Only services that actually write to pipeline_runs (others tracked via data freshness)
    KEY_JOBS = ["strategy_backtester", "confidence_scorer"]
    STATUS_ICON = {"success": "🟢", "failed": "🔴", "running": "🟡", "skipped": "⬜"}

    if runs_df.empty:
        st.info("No pipeline run records yet — jobs populate this after first run.")
    else:
        runs_df["run_date"] = pd.to_datetime(runs_df["run_date"]).dt.date
        dates = sorted(runs_df["run_date"].unique(), reverse=True)[:7]

        grid_rows = []
        for svc in KEY_JOBS:
            svc_rows = runs_df[runs_df["service"] == svc]
            if svc_rows.empty:
                last_run, last_status, recent_err = "—", "⚫", ""
            else:
                latest     = svc_rows.sort_values("run_date", ascending=False).iloc[0]
                last_run   = str(latest["run_date"])
                last_status = STATUS_ICON.get(str(latest["status"]), "❓")
                recent_err  = str(latest["error_msg"] or "")[:80]

            row = {"Job": svc, "Last Run": last_run, "Status": last_status}
            for d in dates[:5]:
                cell = svc_rows[svc_rows["run_date"] == d]
                row[str(d)] = ("⚫" if cell.empty else
                               STATUS_ICON.get(str(cell.sort_values("run_date").iloc[-1]["status"]), "❓"))
            if recent_err:
                row["Last Error"] = recent_err
            grid_rows.append(row)

        st.dataframe(pd.DataFrame(grid_rows), use_container_width=True, hide_index=True)
        st.caption("🟢 success  🔴 failed  🟡 running  ⬜ skipped  ⚫ no record")

        fails = runs_df[runs_df["status"] == "failed"].sort_values("run_date", ascending=False).head(5)
        if not fails.empty:
            with st.expander(f"🔴 {len(fails)} recent failures"):
                st.dataframe(
                    fails[["service", "run_date", "error_msg", "duration_s"]],
                    use_container_width=True, hide_index=True,
                )

    st.divider()

    # ── Data Freshness ────────────────────────────────────────────────────────
    st.subheader("Data Freshness")
    st.caption("Latest record in each table. Age is relative to now (IST). 🟢 < 1d · 🟡 < 4d · 🔴 older.")

    def _freshness_row(label, table, date_sql, ts_sql=None, ok_days=1, warn_days=4):
        """
        Build one freshness row. date_sql must return a single scalar date/datetime.
        ts_sql is optional — returns the most recent timestamp for the 'At (IST)' column.
        """
        _no_data = {"Source": label, "Table": table, "Latest Date": "—",
                    "At (IST)": "—", "Age": "—", "Status": "⚫ no data"}
        df = query(date_sql)
        if df.empty:
            return _no_data
        raw = df.iloc[0, 0]
        # ClickHouse NULL → pandas NaT or None; both mean no data
        if raw is None or (hasattr(raw, '_value') and raw is pd.NaT) or pd.isna(raw):
            return _no_data
        if isinstance(raw, (datetime, pd.Timestamp)):
            ts = pd.Timestamp(raw)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            dt_utc = ts.to_pydatetime()
            ld     = dt_utc.astimezone(IST).date()
        else:
            dt_utc = None
            try:
                ld = raw.date() if hasattr(raw, "date") else date.fromisoformat(str(raw)[:10])
            except Exception:
                return {"Source": label, "Table": table, "Latest Date": str(raw),
                        "At (IST)": "—", "Age": "—", "Status": "❓"}

        # timestamp column
        at_ist = "—"
        if ts_sql:
            ts_df = query(ts_sql)
            if not ts_df.empty:
                ts_raw = ts_df.iloc[0, 0]
                if ts_raw is not None and not pd.isna(ts_raw):
                    at_ist = _to_ist_str(ts_raw)
        elif dt_utc:
            at_ist = _to_ist_str(dt_utc)

        age_days = (today - ld).days
        age_secs = age_days * 86400
        # Re-compute precise age if we have a UTC datetime
        if dt_utc:
            age_secs = int((datetime.now(timezone.utc) - dt_utc).total_seconds())
            age_days = age_secs // 86400

        age_label = _age_str(dt_utc) if dt_utc else (f"{age_days}d ago" if age_days else "today")
        icon  = traffic_light(age_days <= ok_days, age_days <= warn_days)
        return {
            "Source":      label,
            "Table":       table,
            "Latest Date": str(ld),
            "At (IST)":    at_ist,
            "Age":         age_label,
            "Status":      icon,
        }

    freshness_rows = []

    # Options chain — one row per symbol
    chain_sym_df = query("""
        SELECT symbol,
               nullIf(max(timestamp), toDateTime(0))       AS last_ts,
               nullIf(max(toDate(timestamp)), toDate(0))   AS last_date
        FROM market.options_chain FINAL
        GROUP BY symbol ORDER BY symbol
    """)
    if chain_sym_df.empty:
        for sym in SYMBOLS:
            freshness_rows.append({
                "Source": f"Options Chain · {sym}", "Table": "market.options_chain",
                "Latest Date": "—", "At (IST)": "—", "Age": "—", "Status": "⚫ no data",
            })
    else:
        for _, r in chain_sym_df.iterrows():
            ts_raw = r["last_ts"]
            dt_utc = None
            if ts_raw is not None and not pd.isna(ts_raw):
                ts = pd.Timestamp(ts_raw)
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                dt_utc = ts.to_pydatetime()
            ld_raw = r["last_date"]
            if ld_raw is None or (not isinstance(ld_raw, date) and pd.isna(ld_raw)):
                ld, age_days = None, 99
            else:
                ld = ld_raw.date() if hasattr(ld_raw, "date") else ld_raw
                age_days = (today - ld).days if ld else 99
            freshness_rows.append({
                "Source":      f"Options Chain · {r['symbol']}",
                "Table":       "market.options_chain",
                "Latest Date": str(ld),
                "At (IST)":    _to_ist_str(dt_utc),
                "Age":         _age_str(dt_utc),
                "Status":      traffic_light(age_days <= 1, age_days <= 4),
            })

    # India VIX / Nifty spot
    freshness_rows.append(_freshness_row(
        "India VIX / Nifty Spot", "market.nifty_live",
        "SELECT nullIf(max(timestamp), toDateTime(0)) FROM market.nifty_live FINAL",
        ok_days=1, warn_days=3,
    ))

    # OI features — options_eod_summary is NIFTY-only (no symbol column)
    freshness_rows.append(_freshness_row(
        "OI Features (EOD) · NIFTY", "market.options_eod_summary",
        "SELECT max(date) FROM market.options_eod_summary FINAL",
        ok_days=1, warn_days=4,
    ))

    # OHLCV by market
    ohlcv_df = query("""
        SELECT market, max(date) AS last_date
        FROM market.ohlcv_daily FINAL
        WHERE market IN ('nse_index', 'indian', 'bse')
        GROUP BY market ORDER BY market
    """)
    for _, r in (ohlcv_df.iterrows() if not ohlcv_df.empty else iter([])):
        ld_raw = r["last_date"]
        ld = ld_raw.date() if hasattr(ld_raw, "date") else (
            date.fromisoformat(str(ld_raw)[:10]) if ld_raw else None)
        age_days = (today - ld).days if ld else 99
        freshness_rows.append({
            "Source":      f"OHLCV · {r['market']}",
            "Table":       "market.ohlcv_daily",
            "Latest Date": str(ld) if ld else "—",
            "At (IST)":    "—",
            "Age":         f"{age_days}d ago" if ld else "—",
            "Status":      traffic_light(age_days <= 1, age_days <= 3),
        })

    # Participant OI
    freshness_rows.append(_freshness_row(
        "Participant OI", "market.participant_oi",
        "SELECT max(date) FROM market.participant_oi FINAL",
        ok_days=1, warn_days=3,
    ))

    # Confidence scores
    freshness_rows.append(_freshness_row(
        "Confidence Scores", "analysis.confidence_scores",
        "SELECT max(score_date) FROM analysis.confidence_scores FINAL",
        ok_days=1, warn_days=2,
    ))

    # Open positions — nullIf guards against epoch (1970) returned from empty table
    freshness_rows.append(_freshness_row(
        "Open Positions", "trades.open_positions",
        "SELECT nullIf(max(entry_time), toDateTime(0)) FROM trades.open_positions FINAL",
        ok_days=4, warn_days=7,
    ))

    # Trade outcomes
    freshness_rows.append(_freshness_row(
        "Trade Outcomes", "trades.trade_outcomes",
        "SELECT nullIf(max(exit_time), toDateTime(0)) FROM trades.trade_outcomes FINAL",
        ok_days=4, warn_days=14,
    ))

    st.dataframe(pd.DataFrame(freshness_rows), use_container_width=True, hide_index=True)

    st.divider()

    # ── Confidence Scores ─────────────────────────────────────────────────────
    st.subheader("Confidence Scores")
    scores_df = query("""
        SELECT symbol, score_date, confidence
        FROM analysis.confidence_scores FINAL
        WHERE score_date >= today() - 7
        ORDER BY symbol, score_date DESC
    """)

    if scores_df.empty:
        st.error("No confidence scores in the last 7 days. Run: confidence_scorer --score-only")
    else:
        scores_df["score_date"] = pd.to_datetime(scores_df["score_date"]).dt.date
        latest_scores = scores_df.groupby("symbol").first().reset_index()

        cols = st.columns(4)
        for i, sym in enumerate(SYMBOLS):
            row = latest_scores[latest_scores["symbol"] == sym]
            with cols[i]:
                if row.empty:
                    st.metric(sym, "—", delta="NO SCORE")
                    st.write("🔴 Never scored")
                else:
                    r = row.iloc[0]
                    conf     = float(r["confidence"])
                    age_days = (today - r["score_date"]).days
                    go       = conf >= MIN_CONFIDENCE
                    icon     = traffic_light(age_days == 0, age_days <= 1)
                    st.metric(
                        label=f"{icon} {sym}",
                        value=f"{conf:.0f}/100",
                        delta=f"{'✅ GO' if go else '❌ NO-GO'} · {age_days}d ago",
                        delta_color="normal" if go else "inverse",
                    )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — MODEL
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Model":
    st.title("🧠 Model")
    st.caption(
        "Per-symbol XGBoost trained on weekly expiry iron fly data. "
        "Retrained weekly (Sunday). Daily re-score at 18:30 IST. "
        f"Gate: confidence ≥ {MIN_CONFIDENCE:.0f} to trade."
    )

    today = date.today()

    # ── Today's signal cards — all 4 symbols ─────────────────────────────────
    st.subheader("Today's Signal")

    scores_df = query("""
        SELECT symbol, score_date, next_expiry,
               confidence, expected_pnl_pct, features_json
        FROM analysis.confidence_scores FINAL
        WHERE score_date >= today() - 3
        ORDER BY symbol, score_date DESC, version DESC
    """)

    vix_df = query("""
        SELECT vix, nifty_spot, toDate(timestamp) AS snap_date
        FROM market.nifty_live FINAL
        ORDER BY timestamp DESC LIMIT 1
    """)
    vix_val = float(vix_df["vix"].iloc[0]) if not vix_df.empty else None
    spot_val = float(vix_df["nifty_spot"].iloc[0]) if not vix_df.empty else None

    eod_df = query("""
        SELECT symbol, date, iv_rank, pcr
        FROM market.options_eod_summary FINAL
        WHERE date >= today() - 5
        ORDER BY symbol, date DESC
    """)

    cols = st.columns(4)
    for i, sym in enumerate(SYMBOLS):
        with cols[i]:
            sym_scores = (scores_df[scores_df["symbol"] == sym]
                          if not scores_df.empty else pd.DataFrame())
            sym_eod = (eod_df[eod_df["symbol"] == sym]
                       if not eod_df.empty else pd.DataFrame())

            if sym_scores.empty:
                st.error(f"**{sym}**\nNo score")
                continue

            row = sym_scores.iloc[0]
            conf = float(row["confidence"])
            expiry = str(row["next_expiry"])[:10]
            exp_pnl_pct = float(row["expected_pnl_pct"])
            feats_json = row["features_json"] if row["features_json"] else ""
            miss = missing_features(feats_json)

            go = conf >= MIN_CONFIDENCE
            card_color = "🟢" if go else "🔴"

            # Graduation model: prominent headline, detail below
            st.markdown(f"### {card_color} {sym}")

            # Confidence gauge (simple colored metric)
            conf_level = ("HIGH" if conf >= 70 else "MED" if conf >= MIN_CONFIDENCE else "LOW")
            st.metric(
                label="Confidence",
                value=f"{conf:.0f}/100",
                delta=f"{'✅ GO' if go else '❌ NO-GO'} — {conf_level}",
                delta_color="normal" if go else "inverse",
            )

            # Expected P&L
            lot_size = LOT_SIZES.get(sym, 65)
            span_pts = span_estimate_pts(feats_json, spot_val or 24000, lot_size)
            threshold_inr = span_pts * 0.01 if span_pts > 0 else None
            exp_pnl_inr = exp_pnl_pct / 100 * (spot_val or 24000) * lot_size if spot_val else None
            if exp_pnl_inr is not None and threshold_inr:
                beat_threshold = exp_pnl_inr >= threshold_inr
                st.metric(
                    label="Expected P&L",
                    value=fmt_inr(exp_pnl_inr),
                    delta=f"Threshold: {fmt_inr(threshold_inr)} {'✅' if beat_threshold else '⚠️'}",
                    delta_color="normal" if beat_threshold else "off",
                )
            else:
                st.metric("Expected P&L", f"{exp_pnl_pct:+.1f}%")

            # Key context
            iv_rank_val = None
            pcr_val = None
            if not sym_eod.empty:
                eod_row = sym_eod.iloc[0]
                iv_rank_val = float(eod_row.get("iv_rank", 0) or 0)
                pcr_val = float(eod_row.get("pcr", 0) or 0)

            context_parts = []
            if vix_val:
                context_parts.append(f"VIX {vix_val:.1f}")
            if pcr_val:
                context_parts.append(f"PCR {pcr_val:.2f}")
            if iv_rank_val:
                context_parts.append(f"IVR {iv_rank_val:.0f}%")
            if context_parts:
                st.caption(" · ".join(context_parts))

            # Feature health badges — graduation: green summary, expand for detail
            if miss:
                badge_txt = f"⚠️ {len(miss)} feature{'s' if len(miss) > 1 else ''} missing"
                with st.expander(badge_txt, expanded=False):
                    for f in miss:
                        st.write(f"🔴 `{f}` = 0 (score may be unreliable)")
                    remaining = [f for f in CRITICAL_FEATURES if f not in miss]
                    for f in remaining:
                        st.write(f"🟢 `{f}`")
            else:
                st.success("✅ All features present", icon=None)

            st.caption(f"Expiry: {expiry}")

    st.divider()

    # ── Model quality: walk-forward AUC ──────────────────────────────────────
    st.subheader("Walk-Forward Backtest")
    st.caption("Out-of-sample predictions only. Each dot = one expiry where the model had no look-ahead.")

    bt_df = query("""
        SELECT symbol, expiry, entry_date, pnl_pts, target,
               round(confidence * 100, 1) AS confidence
        FROM analysis.confidence_backtest FINAL
        ORDER BY symbol, expiry
    """)

    if bt_df.empty:
        st.info("No walk-forward backtest data yet. Run: confidence_scorer --compare")
    else:
        sym_filter = st.selectbox("Symbol", ["All"] + sorted(bt_df["symbol"].unique().tolist()), key="model_sym")
        bt_view = bt_df if sym_filter == "All" else bt_df[bt_df["symbol"] == sym_filter]
        bt_view = bt_view.copy()

        # KPI row
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("OOS Trades", len(bt_view))
        k2.metric("Actual Win Rate", f"{bt_view['target'].mean():.1%}")
        k3.metric("Avg Confidence", f"{bt_view['confidence'].mean():.1f}/100")
        k4.metric("Avg P&L (pts)", f"{bt_view['pnl_pts'].mean():.1f}")

        tab_scatter, tab_band, tab_curve, tab_calib = st.tabs([
            "Confidence vs P&L", "Win Rate by Band", "Cumulative P&L", "Calibration"
        ])

        with tab_scatter:
            # Graduation: color by win/loss, hover for detail
            bt_view["outcome"] = bt_view["target"].map({1: "Win", 0: "Loss"})
            fig = px.scatter(
                bt_view, x="confidence", y="pnl_pts",
                color="outcome",
                color_discrete_map={"Win": "#2ecc71", "Loss": "#e74c3c"},
                opacity=0.7,
                labels={"confidence": "Confidence Score", "pnl_pts": "P&L (pts)"},
                hover_data=["symbol", "expiry"],
            )
            fig.add_vline(x=MIN_CONFIDENCE, line_dash="dash", line_color="gray",
                          annotation_text=f"Gate {MIN_CONFIDENCE:.0f}")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        with tab_band:
            bt_view["band"] = pd.cut(
                bt_view["confidence"],
                bins=[0, 50, 60, 70, 80, 100],
                labels=["<50", "50–60", "60–70", "70–80", ">80"]
            )
            band_stats = (bt_view.groupby("band", observed=True)
                          .agg(n=("target", "count"), win_rate=("target", "mean"))
                          .reset_index())
            band_stats["win_pct"] = (band_stats["win_rate"] * 100).round(1)

            # Graduation: bar color maps win rate to green/yellow/red
            colors = []
            for wr in band_stats["win_rate"]:
                if wr >= 0.65:
                    colors.append("#2ecc71")
                elif wr >= 0.50:
                    colors.append("#f39c12")
                else:
                    colors.append("#e74c3c")

            fig2 = go.Figure(go.Bar(
                x=band_stats["band"].astype(str),
                y=band_stats["win_pct"],
                text=band_stats["win_pct"].astype(str) + "% (n=" + band_stats["n"].astype(str) + ")",
                textposition="outside",
                marker_color=colors,
            ))
            fig2.add_hline(y=50, line_dash="dash", line_color="gray",
                           annotation_text="Random (50%)")
            fig2.update_layout(height=350, yaxis_title="Win Rate %",
                               xaxis_title="Confidence Band", yaxis_range=[0, 115])
            st.plotly_chart(fig2, use_container_width=True)

        with tab_curve:
            bt_sorted = bt_view.sort_values("expiry").copy()
            bt_sorted["expiry"] = pd.to_datetime(bt_sorted["expiry"])
            fig3 = go.Figure()
            for min_c, label, clr in [
                (0, "All trades", "#aaaaaa"),
                (MIN_CONFIDENCE, f"Conf ≥ {MIN_CONFIDENCE:.0f}", "#3498db"),
            ]:
                grp = bt_sorted[bt_sorted["confidence"] >= min_c].copy()
                grp["cum"] = grp["pnl_pts"].cumsum()
                fig3.add_trace(go.Scatter(
                    x=grp["expiry"], y=grp["cum"],
                    mode="lines", name=label, line_color=clr,
                ))
            fig3.add_hline(y=0, line_dash="dash", line_color="gray")
            fig3.update_layout(height=350, yaxis_title="Cumulative P&L (pts)")
            st.plotly_chart(fig3, use_container_width=True)

        with tab_calib:
            # Calibration curve: predicted confidence decile vs actual win rate
            st.caption("A well-calibrated model's line follows the diagonal. Deviation = over/under-confidence.")
            bt_view["bucket"] = (bt_view["confidence"] // 10 * 10).clip(0, 90)
            calib = (bt_view.groupby("bucket")
                     .agg(n=("target", "count"), actual_wr=("target", "mean"))
                     .reset_index())
            calib["predicted_mid"] = calib["bucket"] + 5

            fig4 = go.Figure()
            # Perfect calibration reference
            fig4.add_trace(go.Scatter(
                x=[0, 100], y=[0, 100],
                mode="lines", name="Perfect calibration",
                line=dict(color="gray", dash="dash"), opacity=0.5,
            ))
            # Actual calibration (size = sample count)
            fig4.add_trace(go.Scatter(
                x=calib["predicted_mid"],
                y=(calib["actual_wr"] * 100).round(1),
                mode="markers+lines",
                name="Model",
                marker=dict(
                    size=calib["n"].clip(5, 40),
                    color="#3498db",
                    line=dict(color="white", width=1),
                ),
                text=calib["n"].astype(str) + " trades",
                hovertemplate="Predicted: %{x}%<br>Actual: %{y}%<br>%{text}<extra></extra>",
            ))
            fig4.update_layout(
                height=350,
                xaxis_title="Predicted Confidence (%)",
                yaxis_title="Actual Win Rate (%)",
                xaxis_range=[0, 100], yaxis_range=[0, 100],
            )
            st.plotly_chart(fig4, use_container_width=True)
            st.caption("Bubble size = number of trades in that bucket.")

    st.divider()

    # ── Strategy Graduation Ladder ────────────────────────────────────────────
    st.subheader("Strategy Graduation Ladder")
    st.caption(
        "Each strategy must pass strictly enforced gate thresholds before advancing. "
        "Stage 1 = backtest only (no trading). Stage 2 = paper. "
        "Stage 3 = micro-live (10% lot). Stage 4 = full live."
    )

    grad_df = query("""
        SELECT strategy_id, strategy_name, stage, stage_label, stage_since,
               bt_oos_trades, bt_win_rate, bt_sharpe, bt_years,
               bt_gate_trades, bt_gate_win_rate, bt_gate_sharpe, bt_gate_years,
               paper_trades, paper_win_rate, paper_net_pnl, paper_vs_bt_delta,
               paper_gate_trades, paper_gate_win_rate, paper_gate_pnl,
               micro_trades, micro_win_rate, micro_net_pnl,
               micro_gate_trades, micro_gate_win_rate, micro_gate_pnl
        FROM analysis.strategy_graduation FINAL
        ORDER BY stage DESC, strategy_id
    """)

    STAGE_LABELS = {1: "BACKTEST", 2: "PAPER", 3: "MICRO_LIVE", 4: "FULL_LIVE"}
    STAGE_COLOR  = {1: "🔴", 2: "🟡", 3: "🟠", 4: "🟢"}
    ALL_STAGES   = [
        ("iron_fly_0dte",   "Iron Fly 0DTE"),
        ("weekend_theta",   "Weekend Theta Decay"),
        ("equity_breakout", "Monthly Equity Breakout"),
    ]

    if grad_df.empty:
        st.info(
            "No graduation data yet. Run: "
            "`docker compose run --rm graduation_gate`",
            icon="ℹ️"
        )
        for sid, sname in ALL_STAGES:
            st.markdown(f"**{sid}** — ⬜ Stage 1 (BACKTEST) — no data")
    else:
        for sid, sname in ALL_STAGES:
            row = grad_df[grad_df["strategy_id"] == sid]
            if row.empty:
                col_hdr, col_bar = st.columns([3, 7])
                with col_hdr:
                    st.markdown(f"**{sname}**")
                with col_bar:
                    st.markdown("⬜ No data — run `graduation_gate`")
                continue

            r = row.iloc[0]
            stage      = int(r["stage"])
            lbl        = r["stage_label"]
            icon       = STAGE_COLOR.get(stage, "⬜")
            since      = str(r["stage_since"])[:10]

            # Summary row: stage badge + 4-step progress
            col_hdr, col_prog = st.columns([3, 7])
            with col_hdr:
                st.markdown(f"**{sname}**")
                st.caption(f"{icon} Stage {stage}: {lbl} (since {since})")
            with col_prog:
                prog_parts = []
                for s in range(1, 5):
                    slbl = STAGE_LABELS[s]
                    if s < stage:
                        prog_parts.append(f"✅ {slbl}")
                    elif s == stage:
                        prog_parts.append(f"**→ {slbl}**")
                    else:
                        prog_parts.append(f"⬜ {slbl}")
                st.markdown(" · ".join(prog_parts))

            with st.expander(f"Gate details — {sname}", expanded=False):
                gc1, gc2, gc3 = st.columns(3)

                with gc1:
                    st.markdown("**Stage 1 → 2 (Backtest gates)**")
                    g = lambda v: "🟢" if v else "🔴"
                    st.write(f"{g(r['bt_gate_trades'])} Trades ≥ 50: {int(r['bt_oos_trades'])}")
                    st.write(f"{g(r['bt_gate_win_rate'])} Win rate ≥ 55%: {float(r['bt_win_rate'])*100:.1f}%")
                    st.write(f"{g(r['bt_gate_sharpe'])} Sharpe ≥ 0.50: {float(r['bt_sharpe']):.2f}")
                    st.write(f"{g(r['bt_gate_years'])} Data ≥ 2 yrs: {float(r['bt_years']):.1f} yrs")

                with gc2:
                    st.markdown("**Stage 2 → 3 (Paper gates)**")
                    pt = int(r["paper_trades"])
                    if pt == 0:
                        st.write("⬜ No paper trades yet")
                    else:
                        g = lambda v: "🟢" if v else "🔴"
                        delta = float(r["paper_vs_bt_delta"]) * 100
                        st.write(f"{g(r['paper_gate_trades'])} Trades ≥ 30: {pt}")
                        st.write(f"{g(r['paper_gate_win_rate'])} Drift > -10%: {delta:+.1f}%")
                        st.write(f"{g(r['paper_gate_pnl'])} Net P&L > 0: ₹{float(r['paper_net_pnl']):,.0f}")

                with gc3:
                    st.markdown("**Stage 3 → 4 (Micro-live gates)**")
                    mt = int(r["micro_trades"])
                    if mt == 0:
                        st.write("⬜ No micro-live trades yet")
                    else:
                        g = lambda v: "🟢" if v else "🔴"
                        st.write(f"{g(r['micro_gate_trades'])} Trades ≥ 20: {mt}")
                        st.write(f"{g(r['micro_gate_win_rate'])} Win rate: {float(r['micro_win_rate'])*100:.1f}%")
                        st.write(f"{g(r['micro_gate_pnl'])} Net P&L > 0: ₹{float(r['micro_net_pnl']):,.0f}")

    st.divider()

    # ── Strategy Backtests — tabbed by type ───────────────────────────────────
    st.subheader("Strategy Backtests")

    tab_fly, tab_spread, tab_breakout, tab_pattern, tab_edge = st.tabs([
        "Iron Fly", "IC / Spreads", "Breakout", "Pattern Equity", "Edge Analysis"
    ])

    with tab_fly:
        st.info(
            "Iron fly backtest (short ATM straddle + long OTM wings) — "
            "this is the live trading strategy. Data sourced from confidence_backtest "
            "when strategy_backtester is updated to produce iron fly P&L.",
            icon="ℹ️"
        )
        # For now show the walk-forward data which approximates iron fly performance
        if not bt_df.empty:
            sym_sel_fly = st.selectbox("Symbol", sorted(bt_df["symbol"].unique()), key="fly_sym")
            fly_sym = bt_df[bt_df["symbol"] == sym_sel_fly].copy()
            fly_sym["expiry"] = pd.to_datetime(fly_sym["expiry"])
            fly_sym = fly_sym.sort_values("expiry")

            c1, c2, c3 = st.columns(3)
            c1.metric("Trades", len(fly_sym))
            c2.metric("Win Rate", f"{fly_sym['target'].mean():.1%}")
            c3.metric("Avg P&L", f"{fly_sym['pnl_pts'].mean():.1f} pts")

            fig_fly = go.Figure()
            fly_sym["cum"] = fly_sym["pnl_pts"].cumsum()
            fig_fly.add_trace(go.Scatter(
                x=fly_sym["expiry"], y=fly_sym["cum"],
                mode="lines", fill="tozeroy",
                line_color="#3498db",
                fillcolor="rgba(52,152,219,0.1)",
            ))
            fig_fly.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_fly.update_layout(height=300, yaxis_title="Cumulative P&L (pts)")
            st.plotly_chart(fig_fly, use_container_width=True)

    with tab_spread:
        st.caption("Iron Condor / Bull Put / Bear Call spread backtests.")
        opt_df = query("""
            SELECT symbol, strategy, short_n, wing_m, n_trades,
                   round(win_rate*100,1) AS win_rate,
                   round(avg_pnl_pts,1) AS avg_pnl_pts,
                   round(sharpe_pct,2)  AS sharpe,
                   round(premium_to_risk,1) AS premium_pct
            FROM analysis.spread_optimal FINAL
            ORDER BY symbol, strategy, sharpe DESC
        """)
        if opt_df.empty:
            st.info("No spread backtest data. Run: docker compose run --rm strategy_backtester")
        else:
            sym_sel_sp = st.selectbox("Symbol", sorted(opt_df["symbol"].unique()), key="spr_sym")
            opt_sym = opt_df[opt_df["symbol"] == sym_sel_sp]

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Win Rate by Strategy & Short Distance**")
                pivot = opt_sym[opt_sym.strategy != "straddle"].pivot_table(
                    index="strategy", columns="short_n", values="win_rate", aggfunc="max"
                )
                if not pivot.empty:
                    fig_h = go.Figure(go.Heatmap(
                        z=pivot.values,
                        x=[f"sn={c}" for c in pivot.columns],
                        y=pivot.index,
                        colorscale="RdYlGn",
                        text=[[f"{v:.0f}%" for v in row] for row in pivot.values],
                        texttemplate="%{text}",
                        zmin=40, zmax=100,
                    ))
                    fig_h.update_layout(height=280)
                    st.plotly_chart(fig_h, use_container_width=True)

            with col2:
                st.markdown("**Top 10 Combinations (by Sharpe)**")
                top = opt_sym[opt_sym.strategy != "straddle"].nlargest(10, "sharpe")[
                    ["strategy", "short_n", "wing_m", "n_trades", "win_rate",
                     "avg_pnl_pts", "sharpe"]
                ]
                st.dataframe(top, use_container_width=True, hide_index=True)

    with tab_breakout:
        st.caption("Monthly breakout hypothesis: entry on monthly high breakout, exit at 2× or stop.")
        bo_df = query("""
            SELECT symbol, entry_date, exit_date, exit_reason,
                   return_pct, xirr_annualized, holding_days
            FROM analysis.monthly_breakout_trades FINAL
            ORDER BY entry_date
        """)
        if bo_df.empty:
            st.info("No breakout backtest data yet.")
        else:
            bo_df["entry_date"] = pd.to_datetime(bo_df["entry_date"])
            n = len(bo_df)
            n_target = (bo_df["exit_reason"] == "target").sum()

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Trades", n)
            c2.metric("Hit Target (2×)", f"{n_target/n*100:.1f}%")
            c3.metric("Avg Return", f"{bo_df['return_pct'].mean():.2f}%")
            c4.metric("Median XIRR", f"{bo_df['xirr_annualized'].median()*100:.1f}%")

            fig_bo = px.histogram(
                bo_df, x="return_pct", color="exit_reason",
                color_discrete_map={"target": "#2ecc71", "stop": "#e74c3c"},
                nbins=30, barmode="overlay", opacity=0.7,
                labels={"return_pct": "Return %"},
            )
            fig_bo.update_layout(height=300)
            st.plotly_chart(fig_bo, use_container_width=True)

    with tab_pattern:
        st.caption("Pattern-based equity backtests (directional stock trades, multi-month hold).")
        runs_df2 = query("""
            SELECT strategy, period, n_trades, win_rate, avg_net_ret, sharpe, start_date, end_date
            FROM analysis.strategy_runs FINAL WHERE period = 'full' ORDER BY strategy
        """)
        trades_df2 = query("""
            SELECT strategy, entry_month, net_ret
            FROM analysis.strategy_trades FINAL ORDER BY entry_month
        """)
        if runs_df2.empty:
            st.info("No pattern strategy runs found.")
        else:
            st.dataframe(runs_df2, use_container_width=True, hide_index=True)
            if not trades_df2.empty:
                trades_df2["entry_month"] = pd.to_datetime(trades_df2["entry_month"])
                trades_df2["pnl_inr"] = trades_df2["net_ret"] * 100_000
                trades_df2_s = trades_df2.sort_values("entry_month")
                trades_df2_s["cum"] = trades_df2_s["pnl_inr"].cumsum()
                fig_pt = go.Figure(go.Scatter(
                    x=trades_df2_s["entry_month"], y=trades_df2_s["cum"],
                    mode="lines", line_color="#9b59b6",
                ))
                fig_pt.add_hline(y=0, line_dash="dash", line_color="gray")
                fig_pt.update_layout(height=280, yaxis_title="Cumulative P&L (₹1L base)")
                st.plotly_chart(fig_pt, use_container_width=True)

    with tab_edge:
        st.caption(
            "VRP (Volatility Risk Premium) edge analysis per symbol — "
            "implied vs realised move, 0DTE and 1DTE straddle win rates. "
            "Run: `docker compose run --rm edge_analysis`"
        )
        edge_df = query("""
            SELECT symbol, expiry_date, day_of_week,
                   win_0dte, win_1dte,
                   profit_0dte_pts, profit_1dte_pts,
                   vrp, implied_move_pct, realized_move_pct,
                   iv_rank, atm_iv, pcr
            FROM analysis.edge_analysis FINAL
            ORDER BY symbol, expiry_date
        """)
        if edge_df.empty:
            st.info("No edge analysis data. Run: `docker compose run --rm edge_analysis`")
        else:
            sym_sel_e = st.selectbox("Symbol", ["All"] + sorted(edge_df["symbol"].unique().tolist()), key="edge_sym")
            ev = edge_df if sym_sel_e == "All" else edge_df[edge_df["symbol"] == sym_sel_e]
            ev = ev.copy()
            ev["expiry_date"] = pd.to_datetime(ev["expiry_date"])

            # Summary KPIs
            e1, e2, e3, e4 = st.columns(4)
            e1.metric("Expiries", len(ev))
            e2.metric("0DTE Win Rate", f"{ev['win_0dte'].mean():.1%}")
            e3.metric("1DTE Win Rate", f"{ev['win_1dte'].mean():.1%}")
            e4.metric("Avg VRP", f"{ev['vrp'].mean():.2f}")

            # Win rate by symbol (summary table)
            if sym_sel_e == "All":
                summary = (ev.groupby("symbol").agg(
                    n=("win_0dte", "count"),
                    wr_0dte=("win_0dte", "mean"),
                    wr_1dte=("win_1dte", "mean"),
                    avg_vrp=("vrp", "mean"),
                    avg_profit_0dte=("profit_0dte_pts", "mean"),
                ).reset_index())
                summary["wr_0dte"] = (summary["wr_0dte"] * 100).round(1)
                summary["wr_1dte"] = (summary["wr_1dte"] * 100).round(1)
                summary.columns = ["Symbol", "N", "0DTE Win%", "1DTE Win%", "Avg VRP", "Avg 0DTE P&L (pts)"]
                st.dataframe(summary, use_container_width=True, hide_index=True)

            # VRP vs win scatter
            col_e1, col_e2 = st.columns(2)
            with col_e1:
                st.markdown("**VRP vs 0DTE Profit**")
                fig_e1 = px.scatter(
                    ev, x="vrp", y="profit_0dte_pts",
                    color="symbol" if sym_sel_e == "All" else "day_of_week",
                    opacity=0.6, height=300,
                    labels={"vrp": "VRP (impl - realised)", "profit_0dte_pts": "0DTE P&L (pts)"},
                )
                fig_e1.add_hline(y=0, line_dash="dash", line_color="gray")
                fig_e1.add_vline(x=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig_e1, use_container_width=True)

            with col_e2:
                st.markdown("**IV Rank vs 0DTE Win Rate (by quartile)**")
                ev["iv_rank_q"] = pd.qcut(ev["iv_rank"], q=4,
                                           labels=["Q1 (Low)", "Q2", "Q3", "Q4 (High)"],
                                           duplicates="drop")
                ivq = (ev.groupby("iv_rank_q", observed=True)
                         .agg(wr=("win_0dte", "mean"), n=("win_0dte", "count"))
                         .reset_index())
                fig_e2 = go.Figure(go.Bar(
                    x=ivq["iv_rank_q"].astype(str),
                    y=(ivq["wr"] * 100).round(1),
                    text=ivq["n"].astype(str) + " trades",
                    textposition="outside",
                    marker_color="#3498db",
                ))
                fig_e2.add_hline(y=50, line_dash="dash", line_color="gray",
                                  annotation_text="50%")
                fig_e2.update_layout(height=300, yaxis_title="Win Rate %", yaxis_range=[0, 110])
                st.plotly_chart(fig_e2, use_container_width=True)

            # Win rate by day of week
            dow = (ev.groupby("day_of_week").agg(
                wr_0dte=("win_0dte", "mean"), n=("win_0dte", "count")
            ).reset_index())
            dow["wr_0dte_pct"] = (dow["wr_0dte"] * 100).round(1)
            fig_e3 = go.Figure(go.Bar(
                x=dow["day_of_week"],
                y=dow["wr_0dte_pct"],
                text=dow["n"].astype(str) + " trades",
                textposition="outside",
                marker_color="#2ecc71",
            ))
            fig_e3.add_hline(y=50, line_dash="dash", line_color="gray",
                              annotation_text="50% baseline")
            fig_e3.update_layout(height=280, title="0DTE Win Rate by Day of Week",
                                  yaxis_title="Win Rate %", yaxis_range=[0, 110])
            st.plotly_chart(fig_e3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — TRADE LOG
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Trade Log":
    st.title("💼 Trade Log")
    st.caption("Paper trades — iron fly 0DTE. Will expand to live trades.")

    _BROKERAGE    = 20.0
    _STT_PCT      = 0.0005
    _EXCHANGE_PCT = 0.0005
    _GST          = 0.18

    def est_txn_cost(entry_premium: float, wing_cost: float,
                     lot_size: int, lots: int, is_iron_fly: bool) -> float:
        n_orders = 8 if is_iron_fly else 4
        brok  = n_orders * _BROKERAGE * (1 + _GST)
        stt   = entry_premium * _STT_PCT * lot_size * lots * 2
        total_prem = (entry_premium + wing_cost) * lot_size * lots
        exch  = total_prem * _EXCHANGE_PCT * 2
        return round(brok + stt + exch)

    _IST = pd.Timedelta(hours=5, minutes=30)

    # ── Open Positions ────────────────────────────────────────────────────────
    st.subheader("Open Positions")
    open_df = query("""
        SELECT symbol, expiry, entry_time, strike,
               entry_ce_ltp, entry_pe_ltp, entry_premium,
               lot_size, lots, target_inr, stoploss_inr,
               trailing_active, peak_pnl_inr, trail_stop_inr,
               scorecard_conf,
               wing_ce_strike, wing_pe_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.open_positions FINAL
        ORDER BY entry_time DESC
    """)

    if open_df.empty:
        st.info("No open positions right now.")
    else:
        rows = []
        for _, r in open_df.iterrows():
            sym    = r["symbol"]
            strike = float(r["strike"])
            expiry = str(r["expiry"])[:10]
            wce    = float(r.get("wing_ce_strike", 0) or 0)
            wpe    = float(r.get("wing_pe_strike", 0) or 0)
            is_fly = wce > 0

            mark_df = query(f"""
                SELECT sumIf(ltp, option_type='CE') + sumIf(ltp, option_type='PE') AS curr
                FROM market.options_chain
                WHERE symbol='{sym}' AND strike={strike} AND expiry='{expiry}'
                  AND toDate(timestamp)=today()
                  AND timestamp=(SELECT max(timestamp) FROM market.options_chain
                                 WHERE symbol='{sym}' AND toDate(timestamp)=today())
                GROUP BY symbol HAVING curr > 0
            """)
            curr_straddle = float(mark_df["curr"].iloc[0]) if not mark_df.empty else None
            curr_net = curr_straddle
            if is_fly and curr_straddle is not None:
                wing_df = query(f"""
                    SELECT
                        sumIf(ltp, option_type='CE' AND strike={wce}) AS wce_ltp,
                        sumIf(ltp, option_type='PE' AND strike={wpe}) AS wpe_ltp
                    FROM market.options_chain
                    WHERE symbol='{sym}' AND expiry='{expiry}'
                      AND toDate(timestamp)=today()
                      AND timestamp=(SELECT max(timestamp) FROM market.options_chain
                                     WHERE symbol='{sym}' AND toDate(timestamp)=today())
                """)
                if not wing_df.empty:
                    curr_net = curr_straddle - float(wing_df["wce_ltp"].iloc[0]) - float(wing_df["wpe_ltp"].iloc[0])

            entry_value = (float(r["net_premium"])
                           if is_fly and float(r.get("net_premium", 0) or 0) > 0
                           else float(r["entry_premium"]))
            lot_size  = int(r["lot_size"])
            lots      = int(r.get("lots", 1))
            unreal_pts = (entry_value - curr_net) if curr_net is not None else None
            unreal_inr = (unreal_pts * lot_size * lots) if curr_net is not None else None
            wing_cost  = float(r.get("wing_ce_ltp", 0) or 0) + float(r.get("wing_pe_ltp", 0) or 0)
            txn_cost   = est_txn_cost(float(r["entry_premium"]), wing_cost, lot_size, lots, is_fly)
            net_inr    = (unreal_inr - txn_cost) if unreal_inr is not None else None

            rows.append({
                "Symbol":     sym,
                "ATM":        f"{int(strike)}",
                "Wing CE":    f"{int(wce)}" if is_fly else "—",
                "Wing PE":    f"{int(wpe)}" if is_fly else "—",
                "Expiry":     expiry,
                "Entry":      (pd.to_datetime(r["entry_time"]) + _IST).strftime("%d-%b %I:%M %p"),
                "Net Prem":   f"{entry_value:.1f}",
                "Lots":       lots,
                "Conf":       f"{r['scorecard_conf']:.0f}",
                "Target":     fmt_inr(float(r["target_inr"])),
                "Stop":       fmt_inr(float(r["stoploss_inr"])),
                "Gross P&L":  (f"{'+' if unreal_inr>=0 else ''}₹{unreal_inr:.0f} ({unreal_pts:+.1f}pts)"
                               if unreal_inr is not None else "—"),
                "TxCost":     f"-₹{txn_cost:.0f}",
                "Net P&L":    (f"{'+' if net_inr>=0 else ''}₹{net_inr:.0f}"
                               if net_inr is not None else "—"),
                "Trailing":   "🔒" if r["trailing_active"] else "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Today's Completed Trades ──────────────────────────────────────────────
    st.subheader("Today's Trades")
    today_df = query("""
        SELECT symbol, strike, expiry,
               entry_time, exit_time, exit_reason,
               entry_premium, exit_premium,
               pnl_pts, pnl_inr, lot_size, lots, scorecard_conf,
               wing_ce_strike, wing_pe_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.trade_outcomes FINAL
        WHERE toDate(entry_time) = today()
        ORDER BY exit_time DESC
    """)

    if today_df.empty:
        st.info("No completed trades today.")
    else:
        def _row_txn_cost(row):
            is_fly = float(row.get("wing_ce_strike", 0) or 0) > 0
            wc = float(row.get("wing_ce_ltp", 0) or 0) + float(row.get("wing_pe_ltp", 0) or 0)
            return est_txn_cost(float(row["entry_premium"]), wc,
                                int(row["lot_size"]), int(row.get("lots", 1)), is_fly)

        today_df["txn_cost"] = today_df.apply(_row_txn_cost, axis=1)
        today_df["net_pnl"]  = today_df["pnl_inr"] - today_df["txn_cost"]

        total_gross = float(today_df["pnl_inr"].sum())
        total_cost  = float(today_df["txn_cost"].sum())
        total_net   = float(today_df["net_pnl"].sum())
        wins = int((today_df["net_pnl"] > 0).sum())

        # Graduation: headline KPIs prominent
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Net P&L", fmt_inr(total_net),
                  delta=f"{'▲' if total_net>=0 else '▼'} Gross {fmt_inr(total_gross)}",
                  delta_color="normal" if total_net >= 0 else "inverse")
        c2.metric("TxCost", f"-₹{total_cost:.0f}")
        c3.metric("Trades", len(today_df))
        c4.metric("Wins (net)", wins)
        c5.metric("Stops", int((today_df["exit_reason"] == "stop").sum()))

        rows = []
        for _, r in today_df.iterrows():
            is_fly = float(r.get("wing_ce_strike", 0) or 0) > 0
            wce = int(r.get("wing_ce_strike", 0) or 0)
            wpe = int(r.get("wing_pe_strike", 0) or 0)
            net = float(r["pnl_inr"]) - float(r["txn_cost"])
            rows.append({
                "Symbol":   r["symbol"],
                "ATM":      int(r["strike"]),
                "Wing CE":  wce if is_fly else "—",
                "Wing PE":  wpe if is_fly else "—",
                "Entry":    (pd.to_datetime(r["entry_time"])  + _IST).strftime("%d-%b %I:%M %p"),
                "Exit":     (pd.to_datetime(r["exit_time"])   + _IST).strftime("%I:%M %p"),
                "Reason":   r["exit_reason"],
                "Status":   "✅ Win" if net > 0 else "❌ Loss",
                "Net Entry": f"{float(r.get('net_premium', 0) or r['entry_premium']):.1f}",
                "Exit Val": f"{float(r['exit_premium']):.1f}",
                "P&L pts":  f"{float(r['pnl_pts']):+.1f}",
                "Net P&L":  f"{'+' if net>=0 else ''}₹{net:.0f}",
                "Conf":     f"{float(r['scorecard_conf']):.0f}",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.divider()

    # ── Historical Outcomes ───────────────────────────────────────────────────
    st.subheader("Historical Outcomes")
    hist_df = query("""
        SELECT toDate(entry_time) AS trade_date, symbol, strike,
               exit_reason, entry_premium, exit_premium,
               pnl_pts, pnl_inr, scorecard_conf
        FROM trades.trade_outcomes FINAL
        ORDER BY entry_time DESC LIMIT 500
    """)

    if hist_df.empty:
        st.info("No historical trade data yet.")
    else:
        hist_df["trade_date"] = pd.to_datetime(hist_df["trade_date"])
        cum = hist_df.sort_values("trade_date").copy()
        cum["cum_pnl"] = cum["pnl_inr"].cumsum()

        # Graduation: primary chart first, stats second, raw table behind expander
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Scatter(
            x=cum["trade_date"], y=cum["cum_pnl"],
            mode="lines+markers", line_color="#3498db",
            fill="tozeroy", fillcolor="rgba(52,152,219,0.1)",
        ))
        fig_hist.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_hist.update_layout(height=300, yaxis_title="Cumulative P&L (₹)")
        st.plotly_chart(fig_hist, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            by_reason = hist_df.groupby("exit_reason").agg(
                trades=("pnl_inr", "count"),
                total_inr=("pnl_inr", "sum"),
                win_rate=("pnl_inr", lambda x: (x > 0).mean() * 100),
            ).reset_index()
            st.markdown("**By Exit Reason**")
            st.dataframe(by_reason, use_container_width=True, hide_index=True)

        with col2:
            by_sym = hist_df.groupby("symbol").agg(
                trades=("pnl_inr", "count"),
                total_inr=("pnl_inr", "sum"),
                win_rate=("pnl_inr", lambda x: (x > 0).mean() * 100),
            ).reset_index()
            st.markdown("**By Symbol**")
            st.dataframe(by_sym, use_container_width=True, hide_index=True)

        with st.expander("Full Trade Log (last 500)"):
            st.dataframe(
                hist_df[["trade_date", "symbol", "strike", "exit_reason",
                          "entry_premium", "exit_premium", "pnl_pts", "pnl_inr", "scorecard_conf"]],
                use_container_width=True, hide_index=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — MARKET DATA
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Market Data":
    st.title("🔍 Market Data")

    # ── India VIX ─────────────────────────────────────────────────────────────
    st.subheader("India VIX — 1 Year")
    vix_df = query("""
        SELECT toDate(timestamp) as date,
               max(vix) as vix,
               max(nifty_spot) as nifty_spot
        FROM market.nifty_live FINAL
        WHERE timestamp >= today() - 365
        GROUP BY date ORDER BY date
    """)

    if not vix_df.empty:
        vix_df["date"] = pd.to_datetime(vix_df["date"])
        latest_vix = float(vix_df["vix"].iloc[-1])
        regime = ("Low (<13)" if latest_vix < 13
                  else "Normal (13–18)" if latest_vix < 18
                  else "Elevated (18–25)" if latest_vix < 25
                  else "Extreme (>25)")
        # Graduation: traffic-light color for current VIX
        vix_icon = traffic_light(latest_vix < 18, latest_vix < 25)

        col1, col2 = st.columns([2, 1])
        with col1:
            fig_vix = go.Figure()
            fig_vix.add_trace(go.Scatter(
                x=vix_df["date"], y=vix_df["vix"],
                line_color="#f39c12", name="VIX",
            ))
            fig_vix.add_hrect(y0=0,  y1=13, fillcolor="green",  opacity=0.05, line_width=0)
            fig_vix.add_hrect(y0=13, y1=18, fillcolor="yellow", opacity=0.05, line_width=0)
            fig_vix.add_hrect(y0=18, y1=25, fillcolor="orange", opacity=0.05, line_width=0)
            fig_vix.add_hrect(y0=25, y1=80, fillcolor="red",    opacity=0.05, line_width=0)
            fig_vix.update_layout(height=280, showlegend=False)
            st.plotly_chart(fig_vix, use_container_width=True)
        with col2:
            st.metric(f"{vix_icon} Current VIX", f"{latest_vix:.2f}", delta=regime)
            latest_nifty = float(vix_df["nifty_spot"].iloc[-1])
            prev_nifty = float(vix_df["nifty_spot"].iloc[-2]) if len(vix_df) > 1 else latest_nifty
            chg = (latest_nifty - prev_nifty) / prev_nifty * 100 if prev_nifty else 0
            chg_icon = traffic_light(abs(chg) < 1, abs(chg) < 2)
            st.metric(f"{chg_icon} Nifty Spot", f"{latest_nifty:,.0f}", delta=f"{chg:+.2f}%",
                      delta_color="normal" if chg >= 0 else "inverse")

    st.divider()

    # ── Put-Call Ratio ────────────────────────────────────────────────────────
    st.subheader("PCR & Max Pain (last 90 days)")
    pcr_df = query("""
        SELECT date, pcr, max_pain_strike AS max_pain
        FROM market.options_eod_summary FINAL
        WHERE date >= today() - 90
        ORDER BY date
    """)
    if not pcr_df.empty:
        pcr_df["date"] = pd.to_datetime(pcr_df["date"])
        fig_pcr = go.Figure()
        fig_pcr.add_trace(go.Scatter(x=pcr_df["date"], y=pcr_df["pcr"],
                                     line_color="#9b59b6", name="PCR"))
        fig_pcr.add_hline(y=1.3, line_dash="dash", line_color="#2ecc71",
                          annotation_text="Bullish >1.3")
        fig_pcr.add_hline(y=0.7, line_dash="dash", line_color="#e74c3c",
                          annotation_text="Bearish <0.7")
        fig_pcr.update_layout(height=260, showlegend=False)
        st.plotly_chart(fig_pcr, use_container_width=True)
    else:
        st.info("PCR data not available. Run compute_oi_features.")

    st.divider()

    # ── OI Walls ──────────────────────────────────────────────────────────────
    st.subheader("OI Walls — Current Expiry")
    sym_sel_oi = st.selectbox("Symbol", SYMBOLS, key="mkt_sym")
    wall_df = query(f"""
        SELECT date, expiry, iv_rank, max_pain_strike,
               ce_wall_strike, pe_wall_strike, pcr, iv_skew
        FROM market.options_eod_summary FINAL
        WHERE symbol = '{sym_sel_oi}'
        ORDER BY date DESC LIMIT 1
    """)
    if not wall_df.empty:
        wr = wall_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("IV Rank", f"{wr['iv_rank']:.0f}%")
        c2.metric("Max Pain", f"{int(wr['max_pain_strike']):,}" if wr['max_pain_strike'] else "—")
        c3.metric("CE Wall", f"{int(wr['ce_wall_strike']):,}" if wr.get('ce_wall_strike') else "—")
        c4.metric("PE Wall", f"{int(wr['pe_wall_strike']):,}" if wr.get('pe_wall_strike') else "—")
        st.caption(f"As of {wr['date']} · Expiry {str(wr['expiry'])[:10]} · PCR {float(wr['pcr']):.2f}")
    else:
        st.info(f"No OI wall data for {sym_sel_oi}. Run compute_oi_features.")

    st.divider()

    # ── Fundamentals (compact) ────────────────────────────────────────────────
    with st.expander("📦 Fundamentals & MF NAV"):
        mf_df = query("""
            SELECT date, scheme_name, nav
            FROM market.mf_nav FINAL
            ORDER BY date DESC LIMIT 10
        """)
        if not mf_df.empty:
            st.dataframe(mf_df, use_container_width=True, hide_index=True)
        else:
            st.info("No MF NAV data.")

        fund_df = query("""
            SELECT symbol, period, revenue, net_income, eps, roe
            FROM market.fundamental_quarterly FINAL
            ORDER BY period DESC LIMIT 20
        """)
        if not fund_df.empty:
            st.dataframe(fund_df, use_container_width=True, hide_index=True)
