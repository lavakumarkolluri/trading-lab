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


@st.cache_data(ttl=30)
def query_live(sql: str) -> pd.DataFrame:
    """30-second cache for live trade data (positions, today's P&L, mark prices)."""
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
st.sidebar.caption(f"🟢 Live · {datetime.now(IST).strftime('%H:%M IST')}")
if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

page = st.sidebar.radio("", [
    "System Health",
    "Model",
    "Trade Log",
    "Market Data",
    "MF Advisor",
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
    st.subheader("🧠 Confidence Scores")
    scores_df = query("""
        SELECT symbol, score_date, confidence, expected_pnl_pct, features_json
        FROM analysis.confidence_scores FINAL
        WHERE score_date >= today() - 7
        ORDER BY symbol, score_date DESC
    """)

    if scores_df.empty:
        st.error("No confidence scores in the last 7 days. Run: confidence_scorer --score-only")
    else:
        scores_df["score_date"] = pd.to_datetime(scores_df["score_date"]).dt.date
        latest_scores = scores_df.groupby("symbol").first().reset_index()

        score_rows = []
        for sym in SYMBOLS:
            row = latest_scores[latest_scores["symbol"] == sym]
            if row.empty:
                score_rows.append({
                    "🎯 Symbol": sym, "📊 Confidence": "—", "🚦 Gate": "🔴 NO-GO",
                    "📈 Exp P&L": "—", "📅 Score Date": "—",
                    "🔬 IV Rank": "—", "🌡️ VIX": "—", "📉 PCR": "—",
                    "⚠️ Missing": "no score",
                })
            else:
                r = row.iloc[0]
                conf     = float(r["confidence"])
                age_days = (today - r["score_date"]).days
                age_flag = "" if age_days == 0 else f" 🔴{age_days}d" if age_days > 1 else f" 🟡{age_days}d"
                try:
                    feats = json.loads(str(r.get("features_json", "") or "{}"))
                except Exception:
                    feats = {}
                miss = missing_features(str(r.get("features_json", "") or ""))
                score_rows.append({
                    "🎯 Symbol":   sym,
                    "📊 Confidence": f"{conf:.0f}/100",
                    "🚦 Gate":     "🟢 GO" if conf >= MIN_CONFIDENCE else "🔴 NO-GO",
                    "📈 Exp P&L":  f"{float(r.get('expected_pnl_pct', 0)):+.1f}%",
                    "📅 Score Date": str(r["score_date"]) + age_flag,
                    "🔬 IV Rank":  f"{feats.get('iv_rank', 0):.0f}" if feats.get("iv_rank") else "—",
                    "🌡️ VIX":      f"{feats.get('vix', 0):.1f}" if feats.get("vix") else "—",
                    "📉 PCR":      f"{feats.get('pcr_oi', 0):.2f}" if feats.get("pcr_oi") else "—",
                    "⚠️ Missing":  ", ".join(miss) if miss else "—",
                })
        st.dataframe(pd.DataFrame(score_rows), use_container_width=True, hide_index=True)

    st.divider()

    # ── Resources & Cleanup ───────────────────────────────────────────────────
    st.subheader("🗄️ Resources & Cleanup")

    def _fmt_bytes(n: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(n) < 1024:
                return f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} PB"

    _res_tabs = st.tabs(["Host", "Docker", "Cleanup Log", "Suggestions"])

    # ── Host tab ─────────────────────────────────────────────────────────────
    with _res_tabs[0]:
        import shutil as _shutil

        # Memory — /proc/meminfo is the host kernel's view even inside a container
        try:
            _mem = {}
            with open("/proc/meminfo") as _mf:
                for _ml in _mf:
                    _mp = _ml.split()
                    if len(_mp) >= 2:
                        _mem[_mp[0].rstrip(":")] = int(_mp[1]) * 1024
            _mem_total = _mem.get("MemTotal", 0)
            _mem_avail = _mem.get("MemAvailable", 0)
            _mem_used  = _mem_total - _mem_avail
            _mem_pct   = _mem_used / _mem_total * 100 if _mem_total else 0
            _hc1, _hc2, _hc3 = st.columns(3)
            _hc1.metric("RAM Total",     _fmt_bytes(_mem_total))
            _hc2.metric("RAM Used",      _fmt_bytes(_mem_used),
                        f"{_mem_pct:.0f}%",
                        delta_color="inverse" if _mem_pct > 85 else "normal")
            _hc3.metric("RAM Available", _fmt_bytes(_mem_avail))
        except Exception as _e:
            st.caption(f"Memory read error: {_e}")

        st.markdown("**Filesystem usage**")
        try:
            import subprocess as _sp
            _df_out = _sp.run(
                ["df", "-B1", "--output=target,size,used,avail,pcent"],
                capture_output=True, text=True, timeout=5
            ).stdout.splitlines()
            _disk_rows = []
            for _dl in _df_out[1:]:
                _dp = _dl.split()
                if len(_dp) >= 5:
                    try:
                        _disk_rows.append({
                            "Mount": _dp[0], "Total": _fmt_bytes(int(_dp[1])),
                            "Used": _fmt_bytes(int(_dp[2])), "Free": _fmt_bytes(int(_dp[3])),
                            "Used%": _dp[4],
                        })
                    except ValueError:
                        pass
            if _disk_rows:
                _dk_df = pd.DataFrame(_disk_rows)
                st.dataframe(_dk_df, use_container_width=True, hide_index=True)
        except Exception as _e:
            st.caption(f"Disk read error: {_e}")

        _cpu_count = os.cpu_count() or "?"
        try:
            with open("/proc/loadavg") as _lf:
                _load = _lf.read().split()[:3]
                _load_str = "  /  ".join(_load)
        except Exception:
            _load_str = "—"
        st.caption(f"CPU cores: {_cpu_count}   |   Load avg (1/5/15 min): {_load_str}")

    # ── Docker tab ────────────────────────────────────────────────────────────
    with _res_tabs[1]:
        try:
            import docker as _docker_sdk
            _dc = _docker_sdk.from_env()

            # Images
            _all_imgs   = _dc.images.list()
            _dang_imgs  = _dc.images.list(filters={"dangling": True})
            _img_bytes  = sum(i.attrs.get("Size", 0) for i in _all_imgs)
            _dang_bytes = sum(i.attrs.get("Size", 0) for i in _dang_imgs)

            # Containers
            _all_cts   = _dc.containers.list(all=True)
            _run_cts   = [c for c in _all_cts if c.status == "running"]
            _stop_cts  = [c for c in _all_cts if c.status in ("exited", "created", "dead")]
            # Orphaned = stopped for more than 7 days (no FinishedAt or very old)
            import datetime as _dt_mod
            _now_utc = _dt_mod.datetime.now(_dt_mod.timezone.utc)
            _orphans = []
            for _ct in _stop_cts:
                _fa = (_ct.attrs.get("State") or {}).get("FinishedAt", "")
                if _fa and _fa != "0001-01-01T00:00:00Z":
                    try:
                        _fin = _dt_mod.datetime.fromisoformat(_fa.replace("Z", "+00:00"))
                        if (_now_utc - _fin).days >= 7:
                            _orphans.append(_ct)
                    except Exception:
                        pass

            # Volumes
            _all_vols  = _dc.volumes.list()
            _dang_vols = _dc.volumes.list(filters={"dangling": True})

            # Build cache via system df
            try:
                _sysdf = _dc.df()
                _cache_bytes = sum(
                    (c.get("Size", 0) or 0)
                    for c in (_sysdf.get("BuildCache") or [])
                )
            except Exception:
                _cache_bytes = 0

            # Display
            _d1, _d2, _d3, _d4 = st.columns(4)
            _d1.metric("Images",          f"{len(_all_imgs)}",
                       f"{_fmt_bytes(_img_bytes)} total")
            _d2.metric("Containers",      f"{len(_all_cts)}",
                       f"{len(_run_cts)} running  /  {len(_stop_cts)} stopped")
            _d3.metric("Volumes",         f"{len(_all_vols)}",
                       f"{len(_dang_vols)} dangling")
            _d4.metric("Build Cache",     _fmt_bytes(_cache_bytes))

            # Dangling images table
            if _dang_imgs:
                st.markdown(f"**Dangling images** ({len(_dang_imgs)}, {_fmt_bytes(_dang_bytes)}) — safe to prune")
                _di_rows = []
                for _im in _dang_imgs:
                    _di_rows.append({
                        "Image ID": _im.short_id,
                        "Size": _fmt_bytes(_im.attrs.get("Size", 0)),
                        "Created": _im.attrs.get("Created", "")[:10],
                    })
                st.dataframe(pd.DataFrame(_di_rows), use_container_width=True, hide_index=True)

            # All containers status table
            st.markdown("**All containers**")
            _ct_rows = []
            for _ct in sorted(_all_cts, key=lambda c: c.name):
                _fa = (_ct.attrs.get("State") or {}).get("FinishedAt", "")
                _fin_str = _fa[:19].replace("T", " ") if _fa and _fa != "0001-01-01T00:00:00Z" else "—"
                _sz = (_ct.attrs.get("SizeRootFs") or 0)
                _ct_rows.append({
                    "Container": _ct.name,
                    "Status": _ct.status,
                    "Image": (_ct.image.tags[0] if _ct.image and _ct.image.tags else _ct.image.short_id if _ct.image else "—"),
                    "Stopped at": _fin_str,
                    "Orphan": "⚠️ yes" if _ct in _orphans else "",
                })
            st.dataframe(pd.DataFrame(_ct_rows), use_container_width=True, hide_index=True)

            # Dangling volumes
            if _dang_vols:
                st.markdown(f"**Dangling volumes** ({len(_dang_vols)}) — not mounted by any container")
                _dv_rows = [{"Volume": v.name, "Driver": v.attrs.get("Driver", "")} for v in _dang_vols]
                st.dataframe(pd.DataFrame(_dv_rows), use_container_width=True, hide_index=True)

        except Exception as _e:
            st.warning(f"Docker SDK unavailable: {_e}")
            st.caption("Ensure /var/run/docker.sock is mounted and the `docker` package is installed.")

    # ── Cleanup Log tab ───────────────────────────────────────────────────────
    with _res_tabs[2]:
        _cl_df = query("""
            SELECT task_name, status, items_freed, bytes_freed, detail,
                   ran_at
            FROM system_meta.cleanup_log
            ORDER BY ran_at DESC
            LIMIT 50
        """)
        if _cl_df.empty:
            st.info("No cleanup runs recorded yet. Cleanup runs every Sunday 09:00 UTC.")
        else:
            _cl_df["bytes_freed"] = _cl_df["bytes_freed"].apply(
                lambda x: _fmt_bytes(int(x)) if x and int(x) > 0 else "—"
            )
            _cl_df["ran_at"] = pd.to_datetime(_cl_df["ran_at"])
            _cl_df["ran_at"] = _cl_df["ran_at"].apply(
                lambda d: d.strftime("%Y-%m-%d %H:%M") if pd.notna(d) else "—"
            )
            st.dataframe(
                _cl_df.rename(columns={
                    "task_name": "Task", "status": "Status",
                    "items_freed": "Items Freed", "bytes_freed": "Bytes Freed",
                    "detail": "Detail", "ran_at": "Ran At",
                }),
                use_container_width=True, hide_index=True,
            )
            # Show docker system df from latest docker_assessment detail
            _last_docker = _cl_df[_cl_df["Task"] == "docker_assessment"].head(1)
            if not _last_docker.empty:
                try:
                    _d = json.loads(_last_docker.iloc[0]["Detail"])
                    if _d.get("system_df"):
                        with st.expander("docker system df (from last assessment)"):
                            st.code(_d["system_df"])
                except Exception:
                    pass

    # ── Suggestions tab ───────────────────────────────────────────────────────
    with _res_tabs[3]:
        st.markdown("**Automated cleanup suggestions** (based on current Docker state)")
        try:
            import docker as _docker_sdk2
            _dc2 = _docker_sdk2.from_env()

            _sugg = []

            _dang2      = _dc2.images.list(filters={"dangling": True})
            _stop2      = [c for c in _dc2.containers.list(all=True)
                           if c.status in ("exited", "created", "dead")]
            _dang_vol2  = _dc2.volumes.list(filters={"dangling": True})

            _dang_sz2  = sum(i.attrs.get("Size", 0) for i in _dang2)
            if _dang2:
                _sugg.append({
                    "Priority": "🟡 Medium", "Issue": f"{len(_dang2)} dangling image(s) ({_fmt_bytes(_dang_sz2)})",
                    "Command": "docker image prune -f",
                    "Impact": f"Frees ~{_fmt_bytes(_dang_sz2)}",
                })

            if _stop2:
                _sugg.append({
                    "Priority": "🟢 Low", "Issue": f"{len(_stop2)} stopped container(s)",
                    "Command": "docker container prune -f",
                    "Impact": "Frees stopped container layers",
                })

            if _dang_vol2:
                _sugg.append({
                    "Priority": "🟡 Medium", "Issue": f"{len(_dang_vol2)} dangling volume(s)",
                    "Command": "docker volume prune -f",
                    "Impact": "Frees anonymous/unused volume data",
                })

            # Build cache
            try:
                _sysdf2 = _dc2.df()
                _cache2 = sum(
                    (c.get("Size", 0) or 0) for c in (_sysdf2.get("BuildCache") or [])
                )
                if _cache2 > 500 * 1024 * 1024:  # > 500 MB
                    _sugg.append({
                        "Priority": "🟡 Medium", "Issue": f"Build cache {_fmt_bytes(_cache2)}",
                        "Command": "docker builder prune -f",
                        "Impact": f"Frees ~{_fmt_bytes(_cache2)}",
                    })
            except Exception:
                pass

            # Disk pressure check
            try:
                _root_du = _shutil.disk_usage("/")
                _disk_pct = _root_du.used / _root_du.total * 100
                if _disk_pct > 85:
                    _sugg.insert(0, {
                        "Priority": "🔴 High", "Issue": f"Root filesystem {_disk_pct:.0f}% full",
                        "Command": "docker system prune -f --volumes",
                        "Impact": "Full prune: removes stopped containers, dangling images, unused volumes",
                    })
                elif _disk_pct > 70:
                    _sugg.append({
                        "Priority": "🟡 Medium", "Issue": f"Root filesystem {_disk_pct:.0f}% full (approaching limit)",
                        "Command": "docker system prune -f",
                        "Impact": "Removes stopped containers + dangling images",
                    })
            except Exception:
                pass

            # Orphaned containers (project containers not in docker-compose)
            _compose_names = {
                "clickhouse", "dashboard", "scheduler", "minio", "portainer",
                "pipeline", "meta_pipeline", "compute_oi_features",
                "intraday_monitor", "option_chain_intraday",
            }
            _all2 = _dc2.containers.list(all=True)
            _orphan_cts = [c for c in _all2
                           if c.status in ("exited", "created", "dead")
                           and not any(n in c.name for n in _compose_names)]
            if _orphan_cts:
                _sugg.append({
                    "Priority": "🟢 Low",
                    "Issue": f"{len(_orphan_cts)} orphan container(s) (not part of compose project)",
                    "Command": "docker container prune -f",
                    "Impact": "Removes containers with no running compose service",
                })

            if not _sugg:
                st.success("✅ No cleanup needed — Docker is in good shape.")
            else:
                st.dataframe(pd.DataFrame(_sugg), use_container_width=True, hide_index=True)
                st.caption(
                    "Run commands on the **host** (or inside the scheduler container which has "
                    "Docker socket access). Commands with `-f` skip the confirmation prompt."
                )

        except Exception as _e:
            st.warning(f"Docker SDK unavailable: {_e}")

        # ClickHouse system table advice
        st.markdown("**ClickHouse system tables**")
        _ch_sys = query("""
            SELECT table,
                   formatReadableSize(sum(bytes_on_disk)) AS size_on_disk,
                   sum(rows) AS rows
            FROM system.parts
            WHERE database = 'system'
            GROUP BY table
            ORDER BY sum(bytes_on_disk) DESC
        """)
        if not _ch_sys.empty:
            st.dataframe(_ch_sys, use_container_width=True, hide_index=True)
            _ch_sys["_bytes"] = query("""
                SELECT table, sum(bytes_on_disk) AS b
                FROM system.parts WHERE database='system'
                GROUP BY table ORDER BY b DESC
            """).set_index("table").get("b", pd.Series(dtype=float))
            _total_sys_mb = query("""
                SELECT sum(bytes_on_disk)/1e6 AS mb FROM system.parts WHERE database='system'
            """)
            if not _total_sys_mb.empty:
                _smb = float(_total_sys_mb.iloc[0]["mb"])
                if _smb > 500:
                    st.warning(
                        f"ClickHouse system tables use {_smb:.0f} MB. "
                        "Logs older than TTL will be purged automatically. "
                        "Check `clickhouse/config/log-retention.xml` if this keeps growing."
                    )
                else:
                    st.caption(f"System tables total: {_smb:.0f} MB — within normal range.")

    st.divider()

    # ── Alert Log ─────────────────────────────────────────────────────────────
    st.subheader("🔔 Alert Log")
    st.caption("Last 50 Telegram alerts sent by any agent.")
    alert_df = query("""
        SELECT alert_time, source, level, message
        FROM system_meta.alert_log FINAL
        ORDER BY alert_time DESC
        LIMIT 50
    """)

    if alert_df.empty:
        st.info("No alerts logged yet — agents write here after each Telegram send.")
    else:
        _level_icon = {"INFO": "🔵", "WARN": "🟡", "CRIT": "🔴"}
        alert_rows = []
        for _, r in alert_df.iterrows():
            ts_utc = pd.Timestamp(r["alert_time"])
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.tz_localize("UTC")
            ts_ist = ts_utc.astimezone("Asia/Kolkata").strftime("%d-%b %H:%M IST")
            lvl = str(r["level"])
            alert_rows.append({
                "🕐 Time":     ts_ist,
                "🤖 Source":   str(r["source"]),
                "🚦 Level":    _level_icon.get(lvl, "⚪") + " " + lvl,
                "💬 Message":  str(r["message"])[:120],
            })
        st.dataframe(pd.DataFrame(alert_rows), use_container_width=True, hide_index=True)


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

    # options_eod_summary is NIFTY-only (CRIT-003) — inject 'NIFTY' so downstream
    # symbol-filter still works; other symbols return empty sym_eod correctly.
    eod_df = query("""
        SELECT 'NIFTY' AS symbol, date, iv_rank, pcr
        FROM market.options_eod_summary FINAL
        WHERE date >= today() - 5
        ORDER BY date DESC
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

            signal_ok  = conf >= MIN_CONFIDENCE
            card_color = "🟢" if signal_ok else "🔴"

            # Graduation model: prominent headline, detail below
            st.markdown(f"### {card_color} {sym}")

            # Confidence gauge (simple colored metric)
            conf_level = ("HIGH" if conf >= 70 else "MED" if conf >= MIN_CONFIDENCE else "LOW")
            st.metric(
                label="Confidence",
                value=f"{conf:.0f}/100",
                delta=f"{'✅ GO' if signal_ok else '❌ NO-GO'} — {conf_level}",
                delta_color="normal" if signal_ok else "inverse",
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
    import io as _io
    st.title("💼 Trade Log")
    st.caption("Paper trades — iron fly 0DTE.")

    _BROKERAGE    = 20.0
    _STT_PCT      = 0.0005
    _EXCHANGE_PCT = 0.0005
    _GST          = 0.18

    def est_txn_cost_itemized(entry_premium: float, wing_cost: float,
                              lot_size: int, lots: int, is_iron_fly: bool) -> dict:
        n_orders = 8 if is_iron_fly else 4
        brok  = round(n_orders * _BROKERAGE * (1 + _GST), 2)
        stt   = round(entry_premium * _STT_PCT * lot_size * lots * 2, 2)
        total_prem = (entry_premium + wing_cost) * lot_size * lots
        exch  = round(total_prem * _EXCHANGE_PCT * 2, 2)
        return {"brok": brok, "stt": stt, "exch": exch, "total": round(brok + stt + exch, 2)}

    def est_txn_cost(entry_premium: float, wing_cost: float,
                     lot_size: int, lots: int, is_iron_fly: bool) -> float:
        return est_txn_cost_itemized(entry_premium, wing_cost, lot_size, lots, is_iron_fly)["total"]

    _IST = pd.Timedelta(hours=5, minutes=30)

    def _excel_bytes(df: pd.DataFrame) -> bytes:
        buf = _io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        return buf.getvalue()

    def _exit_emoji(reason: str) -> str:
        return {"target": "🎯", "stop": "🛑", "eod": "🕐", "trail": "🔒"}.get(str(reason), "📤")

    # ── D1: Summary Banner ────────────────────────────────────────────────────
    st.subheader("📊 Summary")
    all_trades_df = query("""
        SELECT entry_time, exit_time, exit_reason,
               entry_premium, exit_premium,
               pnl_inr, lot_size, lots,
               wing_ce_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.trade_outcomes FINAL
        ORDER BY exit_time DESC
    """)

    if not all_trades_df.empty:
        def _add_costs(df):
            df = df.copy()
            def _cost_row(r):
                is_fly = float(r.get("wing_ce_strike", 0) or 0) > 0
                wc = float(r.get("wing_ce_ltp", 0) or 0) + float(r.get("wing_pe_ltp", 0) or 0)
                return est_txn_cost(float(r["entry_premium"]), wc,
                                    int(r["lot_size"]), int(r.get("lots", 1)), is_fly)
            df["txn_cost"] = df.apply(_cost_row, axis=1)
            df["net_pnl"]  = df["pnl_inr"] - df["txn_cost"]
            return df

        all_trades_df["exit_time"] = pd.to_datetime(all_trades_df["exit_time"])
        all_trades_df["entry_time"] = pd.to_datetime(all_trades_df["entry_time"])
        atdf = _add_costs(all_trades_df)
        now_ist_ts = pd.Timestamp.now(tz="Asia/Kolkata")

        def _period_stats(mask, label):
            sub = atdf[mask]
            if sub.empty:
                return {"Period": label, "💰 Net P&L": "—", "🎯 Win Rate": "—",
                        "💸 Costs": "—", "📊 Trades": 0}
            wins = int((sub["net_pnl"] > 0).sum())
            n    = len(sub)
            return {
                "Period":      label,
                "💰 Net P&L":  fmt_inr(float(sub["net_pnl"].sum())),
                "🎯 Win Rate": f"{wins}/{n} ({wins/n*100:.0f}%)" if n else "—",
                "💸 Costs":    fmt_inr(float(sub["txn_cost"].sum())),
                "📊 Trades":   n,
            }

        today_mask  = atdf["exit_time"].dt.date == now_ist_ts.date()
        week_start  = now_ist_ts.date() - pd.Timedelta(days=now_ist_ts.weekday())
        week_mask   = atdf["exit_time"].dt.date >= week_start
        month_mask  = ((atdf["exit_time"].dt.year == now_ist_ts.year) &
                       (atdf["exit_time"].dt.month == now_ist_ts.month))

        summary_rows = [
            _period_stats(today_mask, "📅 Today"),
            _period_stats(week_mask,  "📆 This Week"),
            _period_stats(month_mask, "🗓️ This Month"),
            _period_stats(pd.Series([True]*len(atdf), index=atdf.index), "🏦 All-Time"),
        ]
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No completed trades yet.")

    st.divider()

    # ── D2: Open Positions ────────────────────────────────────────────────────
    st.subheader("📂 Open Positions")
    if st.button("🔄 Refresh positions", key="refresh_pos"):
        query_live.clear()
    open_df = query_live("""
        SELECT symbol, expiry, entry_time, strike,
               entry_ce_ltp, entry_pe_ltp, entry_premium,
               lot_size, lots, target_inr, stoploss_inr,
               trailing_active, peak_pnl_inr, trail_stop_inr,
               scorecard_conf,
               wing_ce_strike, wing_pe_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.open_positions FINAL
        WHERE status = 'open'
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

            mark_df = query_live(f"""
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
                wing_df = query_live(f"""
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
            wing_cost = float(r.get("wing_ce_ltp", 0) or 0) + float(r.get("wing_pe_ltp", 0) or 0)
            hedge_cost_inr = wing_cost * lot_size * lots
            wing_dist = abs(wce - strike) if is_fly else 0
            max_loss_inr = (wing_dist - entry_value) * lot_size * lots if is_fly else None
            margin_est   = max_loss_inr * 1.10 if max_loss_inr is not None else entry_value * lot_size * 2 * 0.30

            unreal_pts = (entry_value - curr_net) if curr_net is not None else None
            unreal_inr = (unreal_pts * lot_size * lots) if curr_net is not None else None
            txn_cost   = est_txn_cost(float(r["entry_premium"]), wing_cost, lot_size, lots, is_fly)
            net_inr    = (unreal_inr - txn_cost) if unreal_inr is not None else None

            entry_dt = pd.to_datetime(r["entry_time"]) + _IST
            dur_min  = int((datetime.now() - entry_dt.to_pydatetime()).total_seconds() // 60)
            dur_str  = f"{dur_min // 60}h {dur_min % 60}m" if dur_min >= 60 else f"{dur_min}m"

            target_pct = (unreal_inr / float(r["target_inr"]) * 100) if (unreal_inr and float(r["target_inr"])) else 0
            stop_pct   = (abs(unreal_inr) / float(r["stoploss_inr"]) * 100) if (unreal_inr and unreal_inr < 0 and float(r["stoploss_inr"])) else 0
            alert_tag  = ("🔒 Trailing" if r["trailing_active"]
                          else "🎯 Near target" if target_pct > 70
                          else "🛑 Near stop" if stop_pct > 70
                          else "📈 Normal")

            rows.append({
                "🎯 Symbol":   sym,
                "ATM Strike": int(strike),
                "Wing CE ↑":  int(wce) if is_fly else "—",
                "Wing PE ↓":  int(wpe) if is_fly else "—",
                "📅 Expiry":  expiry,
                "⏱️ Duration": dur_str,
                "Entry IST":  entry_dt.strftime("%d-%b %H:%M"),
                "Net Prem":   f"{entry_value:.1f}",
                "Lots":       lots,
                "📊 Conf":    int(float(r["scorecard_conf"])),
                "🎯 Target":  fmt_inr(float(r["target_inr"])),
                "🛑 Stop":    fmt_inr(float(r["stoploss_inr"])),
                "💼 Hedge ₹": f"₹{hedge_cost_inr:.0f}" if is_fly else "—",
                "📐 Max Loss":fmt_inr(max_loss_inr) if max_loss_inr is not None else "∞",
                "📊 Margin":  fmt_inr(margin_est),
                "Gross P&L":  (f"{'+' if unreal_inr>=0 else ''}₹{unreal_inr:.0f}"
                               if unreal_inr is not None else "—"),
                "💸 TxCost":  f"₹{txn_cost:.0f}",
                "💰 Net P&L": (f"{'+' if net_inr>=0 else ''}₹{net_inr:.0f}"
                               if net_inr is not None else "—"),
                "🔔 Alert":   alert_tag,
            })
        pos_df = pd.DataFrame(rows)
        st.dataframe(pos_df, use_container_width=True, hide_index=True)
        st.download_button("⬇️ Download Excel",
                           data=_excel_bytes(pos_df),
                           file_name=f"open_positions_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_open")

    st.divider()

    # ── D3: Today's Completed Trades ──────────────────────────────────────────
    st.subheader("📋 Today's Trades")
    today_df = query_live("""
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
        def _row_costs(row):
            is_fly = float(row.get("wing_ce_strike", 0) or 0) > 0
            wc = float(row.get("wing_ce_ltp", 0) or 0) + float(row.get("wing_pe_ltp", 0) or 0)
            return est_txn_cost_itemized(float(row["entry_premium"]), wc,
                                         int(row["lot_size"]), int(row.get("lots", 1)), is_fly)

        costs_list = today_df.apply(_row_costs, axis=1)
        today_df["brok"]     = [c["brok"]  for c in costs_list]
        today_df["stt"]      = [c["stt"]   for c in costs_list]
        today_df["exch"]     = [c["exch"]  for c in costs_list]
        today_df["txn_cost"] = [c["total"] for c in costs_list]
        today_df["net_pnl"]  = today_df["pnl_inr"] - today_df["txn_cost"]

        total_gross = float(today_df["pnl_inr"].sum())
        total_cost  = float(today_df["txn_cost"].sum())
        total_net   = float(today_df["net_pnl"].sum())
        wins        = int((today_df["net_pnl"] > 0).sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("💰 Net P&L", fmt_inr(total_net),
                  delta=f"Gross {fmt_inr(total_gross)}",
                  delta_color="normal" if total_net >= 0 else "inverse")
        c2.metric("💸 TxCost", f"-₹{total_cost:.0f}")
        c3.metric("📊 Trades", len(today_df))
        c4.metric("🎯 Wins", wins)
        c5.metric("🛑 Stops", int((today_df["exit_reason"] == "stop").sum()))

        rows = []
        for _, r in today_df.iterrows():
            is_fly = float(r.get("wing_ce_strike", 0) or 0) > 0
            wce = int(r.get("wing_ce_strike", 0) or 0)
            wpe = int(r.get("wing_pe_strike", 0) or 0)
            wc_inr = (float(r.get("wing_ce_ltp", 0) or 0) + float(r.get("wing_pe_ltp", 0) or 0)) * int(r["lot_size"]) * int(r.get("lots", 1))
            net = float(r["net_pnl"])
            entry_dt = pd.to_datetime(r["entry_time"]) + _IST
            exit_dt  = pd.to_datetime(r["exit_time"])  + _IST
            dur_min  = int((exit_dt - entry_dt).total_seconds() // 60)
            dur_str  = f"{dur_min // 60}h {dur_min % 60}m" if dur_min >= 60 else f"{dur_min}m"
            rows.append({
                "🎯 Symbol":  r["symbol"],
                "ATM":        int(r["strike"]),
                "Wing CE":    wce if is_fly else "—",
                "Wing PE":    wpe if is_fly else "—",
                "📅 Entry":   entry_dt.strftime("%d-%b %H:%M"),
                "Exit":       exit_dt.strftime("%H:%M"),
                "⏱️ Dur":     dur_str,
                "Exit":       _exit_emoji(r["exit_reason"]) + " " + str(r["exit_reason"]),
                "Net Prem":   f"{float(r.get('net_premium', 0) or r['entry_premium']):.1f}",
                "Exit Val":   f"{float(r['exit_premium']):.1f}",
                "P&L pts":   f"{float(r['pnl_pts']):+.1f}",
                "Gross ₹":   f"{'+' if float(r['pnl_inr'])>=0 else ''}₹{float(r['pnl_inr']):.0f}",
                "💼 Hedge ₹": f"₹{wc_inr:.0f}" if is_fly else "—",
                "🏷️ Brok":    f"₹{r['brok']:.0f}",
                "🏷️ STT":     f"₹{r['stt']:.0f}",
                "🏷️ Exch":    f"₹{r['exch']:.0f}",
                "💸 TxCost":  f"₹{r['txn_cost']:.0f}",
                "💰 Net ₹":   f"{'+' if net>=0 else ''}₹{net:.0f}",
                "📊 Conf":    int(float(r["scorecard_conf"])),
                "Result":     "🟢 Win" if net > 0 else "🔴 Loss",
            })
        today_tbl = pd.DataFrame(rows)
        st.dataframe(today_tbl, use_container_width=True, hide_index=True)
        st.download_button("⬇️ Download Excel",
                           data=_excel_bytes(today_tbl),
                           file_name=f"today_trades_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_today")

    st.divider()

    # ── D4: 7-Day Rolling Audit Log ───────────────────────────────────────────
    st.subheader("📅 7-Day Audit Log")
    week_df = query("""
        SELECT symbol, strike, expiry,
               entry_time, exit_time, exit_reason,
               entry_premium, exit_premium,
               pnl_pts, pnl_inr, lot_size, lots, scorecard_conf,
               wing_ce_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.trade_outcomes FINAL
        WHERE exit_time >= now() - INTERVAL 7 DAY
        ORDER BY exit_time DESC
    """)

    if week_df.empty:
        st.info("No trades in the last 7 days.")
    else:
        def _week_costs(row):
            is_fly = float(row.get("wing_ce_strike", 0) or 0) > 0
            wc = float(row.get("wing_ce_ltp", 0) or 0) + float(row.get("wing_pe_ltp", 0) or 0)
            return est_txn_cost_itemized(float(row["entry_premium"]), wc,
                                         int(row["lot_size"]), int(row.get("lots", 1)), is_fly)

        wk_costs = week_df.apply(_week_costs, axis=1)
        week_df["txn_cost"] = [c["total"] for c in wk_costs]
        week_df["net_pnl"]  = week_df["pnl_inr"] - week_df["txn_cost"]

        audit_rows = []
        for _, r in week_df.iterrows():
            net = float(r["net_pnl"])
            exit_dt = pd.to_datetime(r["exit_time"]) + _IST
            entry_dt = pd.to_datetime(r["entry_time"]) + _IST
            audit_rows.append({
                "📅 Date":    exit_dt.strftime("%d-%b"),
                "🕐 Exit":    exit_dt.strftime("%H:%M"),
                "🎯 Symbol":  r["symbol"],
                "ATM":        int(r["strike"]),
                "Exit":       _exit_emoji(r["exit_reason"]) + " " + str(r["exit_reason"]),
                "Gross ₹":   f"{'+' if float(r['pnl_inr'])>=0 else ''}₹{float(r['pnl_inr']):.0f}",
                "💸 Costs":   f"₹{r['txn_cost']:.0f}",
                "💰 Net ₹":   f"{'+' if net>=0 else ''}₹{net:.0f}",
                "📊 Conf":    int(float(r["scorecard_conf"])),
                "Result":     "🟢 Win" if net > 0 else "🔴 Loss",
            })
        audit_tbl = pd.DataFrame(audit_rows)
        st.dataframe(audit_tbl, use_container_width=True, hide_index=True)

        # Daily subtotals
        week_df["exit_date"] = (pd.to_datetime(week_df["exit_time"]) + _IST).dt.date
        daily = (week_df.groupby("exit_date")
                 .agg(trades=("net_pnl", "count"),
                      wins=("net_pnl", lambda x: (x > 0).sum()),
                      net_pnl=("net_pnl", "sum"),
                      costs=("txn_cost", "sum"))
                 .reset_index()
                 .sort_values("exit_date", ascending=False))
        daily_rows = [{
            "📅 Date":    str(r["exit_date"]),
            "📊 Trades":  int(r["trades"]),
            "🎯 Wins":    int(r["wins"]),
            "💰 Net P&L": fmt_inr(float(r["net_pnl"])),
            "💸 Costs":   fmt_inr(float(r["costs"])),
        } for _, r in daily.iterrows()]
        st.markdown("**Daily Subtotals**")
        st.dataframe(pd.DataFrame(daily_rows), use_container_width=True, hide_index=True)

        st.download_button("⬇️ Download 7-Day Excel",
                           data=_excel_bytes(audit_tbl),
                           file_name=f"trade_log_7d_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_7d")

    st.divider()

    # ── D5: Cost Breakdown Summary ────────────────────────────────────────────
    st.subheader("💸 Cost Breakdown")
    if not all_trades_df.empty:
        def _all_costs(row):
            is_fly = float(row.get("wing_ce_strike", 0) or 0) > 0
            wc = float(row.get("wing_ce_ltp", 0) or 0) + float(row.get("wing_pe_ltp", 0) or 0)
            return est_txn_cost_itemized(float(row["entry_premium"]), wc,
                                         int(row["lot_size"]), int(row.get("lots", 1)), is_fly)
        ac_list = all_trades_df.apply(_all_costs, axis=1)
        all_trades_df["brok"]     = [c["brok"]  for c in ac_list]
        all_trades_df["stt"]      = [c["stt"]   for c in ac_list]
        all_trades_df["exch"]     = [c["exch"]  for c in ac_list]
        all_trades_df["txn_cost"] = [c["total"] for c in ac_list]
        all_trades_df["net_pnl"]  = all_trades_df["pnl_inr"] - all_trades_df["txn_cost"]

        n  = len(all_trades_df)
        gross_total = float(all_trades_df["pnl_inr"].sum())
        brok_total  = float(all_trades_df["brok"].sum())
        stt_total   = float(all_trades_df["stt"].sum())
        exch_total  = float(all_trades_df["exch"].sum())
        cost_total  = float(all_trades_df["txn_cost"].sum())
        net_total   = float(all_trades_df["net_pnl"].sum())
        pct = lambda v: f"{v/gross_total*100:.1f}%" if gross_total else "—"

        cost_rows = [
            {"💸 Cost Item": "🏷️ Brokerage", "🏦 All-Time": fmt_inr(brok_total),
             "📊 Avg/Trade": fmt_inr(brok_total/n) if n else "—", "% of Gross": pct(brok_total)},
            {"💸 Cost Item": "🏷️ STT",        "🏦 All-Time": fmt_inr(stt_total),
             "📊 Avg/Trade": fmt_inr(stt_total/n)  if n else "—", "% of Gross": pct(stt_total)},
            {"💸 Cost Item": "🏷️ Exchange",    "🏦 All-Time": fmt_inr(exch_total),
             "📊 Avg/Trade": fmt_inr(exch_total/n) if n else "—", "% of Gross": pct(exch_total)},
            {"💸 Cost Item": "💸 Total Costs", "🏦 All-Time": fmt_inr(cost_total),
             "📊 Avg/Trade": fmt_inr(cost_total/n) if n else "—", "% of Gross": pct(cost_total)},
            {"💸 Cost Item": "💰 Gross P&L",   "🏦 All-Time": fmt_inr(gross_total),
             "📊 Avg/Trade": fmt_inr(gross_total/n) if n else "—", "% of Gross": "100%"},
            {"💸 Cost Item": "💚 Net P&L",     "🏦 All-Time": fmt_inr(net_total),
             "📊 Avg/Trade": fmt_inr(net_total/n) if n else "—", "% of Gross": pct(net_total)},
        ]
        st.dataframe(pd.DataFrame(cost_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No trade data for cost breakdown.")

    st.divider()

    # ── D6: Historical Audit ──────────────────────────────────────────────────
    st.subheader("📜 Historical Audit")
    hist_df = query("""
        SELECT toDate(entry_time)  AS trade_date,
               entry_time, exit_time, symbol, strike,
               exit_reason, entry_premium, exit_premium,
               pnl_pts, pnl_inr, lot_size, lots, scorecard_conf,
               wing_ce_strike, wing_ce_ltp, wing_pe_ltp, net_premium
        FROM trades.trade_outcomes FINAL
        ORDER BY entry_time DESC LIMIT 500
    """)

    if hist_df.empty:
        st.info("No historical trade data yet.")
    else:
        # Cumulative P&L chart
        hist_df["trade_date"] = pd.to_datetime(hist_df["trade_date"])
        cum = hist_df.sort_values("trade_date").copy()
        cum["cum_pnl"] = cum["pnl_inr"].cumsum()
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

        # Filter controls
        st.markdown("**Filter Trade Log**")
        fc1, fc2 = st.columns(2)
        with fc1:
            sym_filter = st.multiselect("Symbol", options=sorted(hist_df["symbol"].unique()),
                                        default=[], key="hist_sym")
        with fc2:
            reason_filter = st.multiselect("Exit Reason", options=sorted(hist_df["exit_reason"].unique()),
                                           default=[], key="hist_reason")
        filtered = hist_df.copy()
        if sym_filter:
            filtered = filtered[filtered["symbol"].isin(sym_filter)]
        if reason_filter:
            filtered = filtered[filtered["exit_reason"].isin(reason_filter)]

        def _hist_cost(row):
            is_fly = float(row.get("wing_ce_strike", 0) or 0) > 0
            wc = float(row.get("wing_ce_ltp", 0) or 0) + float(row.get("wing_pe_ltp", 0) or 0)
            return est_txn_cost(float(row["entry_premium"]), wc,
                                int(row["lot_size"]), int(row.get("lots", 1)), is_fly)
        filtered["txn_cost"] = filtered.apply(_hist_cost, axis=1)
        filtered["net_pnl"]  = filtered["pnl_inr"] - filtered["txn_cost"]
        filtered["exit_dt_ist"] = (pd.to_datetime(filtered["exit_time"]) + _IST).dt.strftime("%d-%b %H:%M")

        hist_tbl = filtered[[
            "trade_date", "exit_dt_ist", "symbol", "strike", "exit_reason",
            "entry_premium", "exit_premium", "pnl_pts", "pnl_inr",
            "txn_cost", "net_pnl", "scorecard_conf",
        ]].rename(columns={
            "trade_date":   "📅 Date",
            "exit_dt_ist":  "🕐 Exit IST",
            "symbol":       "🎯 Symbol",
            "strike":       "ATM",
            "exit_reason":  "Exit",
            "entry_premium":"Entry Prem",
            "exit_premium": "Exit Prem",
            "pnl_pts":      "P&L pts",
            "pnl_inr":      "Gross ₹",
            "txn_cost":     "💸 Costs",
            "net_pnl":      "💰 Net ₹",
            "scorecard_conf":"📊 Conf",
        })
        st.dataframe(hist_tbl, use_container_width=True, hide_index=True)
        st.download_button("⬇️ Download Audit Excel",
                           data=_excel_bytes(hist_tbl),
                           file_name=f"trade_audit_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_hist")


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
    # options_eod_summary is NIFTY-only (CRIT-003) — no symbol column
    st.subheader("OI Walls — Current Expiry (NIFTY)")
    st.caption("⚠️ CRIT-003: options_eod_summary is NIFTY-only. BANKNIFTY/FINNIFTY/MIDCPNIFTY data not available here.")
    wall_df = query("""
        SELECT date, expiry, iv_rank, max_pain_strike,
               ce_wall_strike, pe_wall_strike, pcr, iv_skew
        FROM market.options_eod_summary FINAL
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
        st.info("No OI wall data. Run compute_oi_features.")

    st.divider()

    # ── MF NAV Explorer ───────────────────────────────────────────────────────
    st.subheader("Mutual Fund NAV Explorer")

    # Load full scheme list once (cached 1 hr)
    schemes_df = query("""
        SELECT scheme_code, scheme_name, fund_house, scheme_type, scheme_category
        FROM market.mf_schemes
        WHERE is_active = 1
        ORDER BY scheme_name
    """)

    if schemes_df.empty:
        st.info("No MF scheme metadata. Ensure mf_schemes table is populated.")
    else:
        schemes_df["scheme_code"] = schemes_df["scheme_code"].astype(int)

        # ── Derive plan-type tags from name ──────────────────────────────────
        sn = schemes_df["scheme_name"].str.upper()
        schemes_df["_is_growth"]  = sn.str.contains("GROWTH",  na=False)
        schemes_df["_is_idcw"]    = sn.str.contains(r"IDCW|DIVIDEND", na=False, regex=True)
        schemes_df["_is_direct"]  = sn.str.contains("DIRECT",  na=False)
        schemes_df["_is_regular"] = ~schemes_df["_is_direct"]  # anything not Direct = Regular

        # ── Filter row 1: Type / Category ────────────────────────────────────
        fcol1, fcol2 = st.columns([2, 4])
        with fcol1:
            scheme_type_sel = st.selectbox(
                "Scheme Type",
                ["All"] + sorted(schemes_df["scheme_type"].dropna().unique().tolist()),
                index=["All", "Equity", "Debt", "Index/ETF", "Hybrid",
                       "FoF", "Commodity", "Other"].index("Equity")
                       if "Equity" in schemes_df["scheme_type"].values else 0,
                key="mf_type",
            )
        type_mask = (
            schemes_df["scheme_type"] == scheme_type_sel
            if scheme_type_sel != "All"
            else pd.Series(True, index=schemes_df.index)
        )
        avail_cats = (
            ["All"] + sorted(schemes_df.loc[type_mask, "scheme_category"]
                             .dropna().unique().tolist())
        )
        with fcol2:
            cat_sel = st.multiselect(
                "Category",
                options=avail_cats[1:],          # exclude "All" — empty = all
                default=[],
                placeholder="All categories (leave empty for all)",
                key="mf_cat",
            )

        # ── Filter row 2: Plan / Direct ───────────────────────────────────────
        fcol3, fcol4 = st.columns(2)
        with fcol3:
            plan_sel = st.radio(
                "Plan Type",
                ["Growth", "IDCW / Dividend", "Both"],
                horizontal=True,
                key="mf_plan",
            )
        with fcol4:
            dist_sel = st.radio(
                "Distribution Channel",
                ["Direct", "Regular", "Both"],
                horizontal=True,
                key="mf_dist",
            )

        # ── Apply filters ─────────────────────────────────────────────────────
        mask = type_mask.copy()
        if cat_sel:
            mask &= schemes_df["scheme_category"].isin(cat_sel)
        if plan_sel == "Growth":
            mask &= schemes_df["_is_growth"]
        elif plan_sel == "IDCW / Dividend":
            mask &= schemes_df["_is_idcw"]
        if dist_sel == "Direct":
            mask &= schemes_df["_is_direct"]
        elif dist_sel == "Regular":
            mask &= schemes_df["_is_regular"]

        filtered = schemes_df[mask].copy()
        st.caption(f"{len(filtered):,} funds match filters")

        if filtered.empty:
            st.warning("No funds match the selected filters.")
        else:
            # ── Fund picker ───────────────────────────────────────────────────
            fund_options = dict(zip(
                filtered["scheme_name"],
                filtered["scheme_code"],
            ))
            selected_names = st.multiselect(
                "Select funds to chart (max 15)",
                options=list(fund_options.keys()),
                default=[],
                max_selections=15,
                placeholder="Pick one or more funds…",
                key="mf_funds",
            )
            selected_codes = [fund_options[n] for n in selected_names]

            # ── Date range ────────────────────────────────────────────────────
            dc1, dc2, dc3 = st.columns([2, 2, 2])
            with dc1:
                from_date = st.date_input(
                    "From date",
                    value=date(2015, 1, 1),
                    min_value=date(2006, 4, 1),
                    max_value=date.today(),
                    key="mf_from",
                )
            with dc2:
                index_to_100 = st.checkbox(
                    "Index all to 100 (compare growth)",
                    value=True,
                    key="mf_idx",
                )
            with dc3:
                show_perf = st.checkbox(
                    "Show performance table",
                    value=True,
                    key="mf_perf",
                )

            if not selected_codes:
                st.info("Select at least one fund above to see the NAV chart.")
            else:
                codes_sql = ", ".join(str(c) for c in selected_codes)
                # Join mf_nav with mf_schemes for the name
                nav_df = query(f"""
                    SELECT n.date, n.scheme_code, n.nav, s.scheme_name
                    FROM market.mf_nav n FINAL
                    JOIN (
                        SELECT scheme_code, scheme_name
                        FROM market.mf_schemes
                        WHERE scheme_code IN ({codes_sql})
                    ) s ON n.scheme_code = s.scheme_code
                    WHERE n.scheme_code IN ({codes_sql})
                      AND n.date >= '{from_date}'
                    ORDER BY n.date
                """)

                if nav_df.empty:
                    st.warning("No NAV data found for selected funds from the chosen date.")
                else:
                    nav_df["date"] = pd.to_datetime(nav_df["date"])

                    # Shorten display names: strip "- Direct Plan - Growth" suffixes
                    def _short(name):
                        for cut in [" - Direct Plan", " Direct Plan",
                                    " - Regular Plan", " Regular Plan",
                                    " - Growth", "-Growth", " Growth",
                                    " - IDCW", "-IDCW", " IDCW",
                                    " Option", " Fund"]:
                            name = name.replace(cut, "")
                        return name.strip(" -")

                    nav_df["label"] = nav_df["scheme_name"].apply(_short)

                    # ── Chart ─────────────────────────────────────────────────
                    if index_to_100:
                        # Divide each fund's NAV by its first value × 100
                        first_navs = (nav_df.sort_values("date")
                                      .groupby("scheme_code")["nav"].first())
                        nav_df = nav_df.join(
                            first_navs.rename("first_nav"), on="scheme_code"
                        )
                        nav_df["plot_val"] = nav_df["nav"] / nav_df["first_nav"] * 100
                        y_label = "Indexed NAV (base=100)"
                    else:
                        nav_df["plot_val"] = nav_df["nav"]
                        y_label = "NAV (₹)"

                    fig_mf = px.line(
                        nav_df, x="date", y="plot_val",
                        color="label",
                        labels={"date": "", "plot_val": y_label, "label": "Fund"},
                        height=480,
                    )
                    fig_mf.update_traces(line_width=1.8)
                    fig_mf.update_layout(
                        legend=dict(orientation="h", yanchor="top",
                                    y=-0.15, xanchor="left", x=0),
                        hovermode="x unified",
                        margin=dict(b=120),
                    )
                    if index_to_100:
                        fig_mf.add_hline(y=100, line_dash="dot",
                                         line_color="gray", opacity=0.5)
                    st.plotly_chart(fig_mf, use_container_width=True)

                    # ── Performance table ──────────────────────────────────────
                    if show_perf:
                        perf_df = query(f"""
                            SELECT e.scheme_code, s.scheme_name,
                                   round(e.return_1m,  1) AS ret_1m,
                                   round(e.return_3m,  1) AS ret_3m,
                                   round(e.return_6m,  1) AS ret_6m,
                                   round(e.return_1y,  1) AS ret_1y,
                                   round(e.return_3y,  1) AS ret_3y,
                                   round(e.return_5y,  1) AS ret_5y,
                                   round(e.cagr_3y,    1) AS cagr_3y,
                                   round(e.cagr_5y,    1) AS cagr_5y,
                                   round(e.sharpe_1y,  2) AS sharpe_1y,
                                   round(e.max_drawdown_pct, 1) AS max_dd_pct,
                                   round(e.volatility_1y, 1) AS vol_1y
                            FROM market.mf_nav_enriched e FINAL
                            JOIN (
                                SELECT scheme_code, scheme_name
                                FROM market.mf_schemes
                                WHERE scheme_code IN ({codes_sql})
                            ) s ON e.scheme_code = s.scheme_code
                            WHERE e.scheme_code IN ({codes_sql})
                            ORDER BY e.date DESC
                            LIMIT 1 BY e.scheme_code
                        """)

                        if not perf_df.empty:
                            perf_df["scheme_name"] = perf_df["scheme_name"].apply(_short)
                            perf_df = perf_df.rename(columns={
                                "scheme_name": "Fund",
                                "ret_1m": "1M%", "ret_3m": "3M%",
                                "ret_6m": "6M%", "ret_1y": "1Y%",
                                "ret_3y": "3Y%", "ret_5y": "5Y%",
                                "cagr_3y": "CAGR 3Y", "cagr_5y": "CAGR 5Y",
                                "sharpe_1y": "Sharpe", "max_dd_pct": "MaxDD%",
                                "vol_1y": "Vol%",
                            }).drop(columns=["scheme_code"])
                            st.dataframe(
                                perf_df, use_container_width=True, hide_index=True
                            )

    st.divider()

    # ── Fundamentals (compact) ────────────────────────────────────────────────
    with st.expander("📦 Fundamentals"):
        fund_df = query("""
            SELECT symbol, period, revenue, net_income, eps,
                   free_cashflow, ebitda
            FROM market.fundamental_quarterly FINAL
            ORDER BY period DESC LIMIT 20
        """)
        if not fund_df.empty:
            st.dataframe(fund_df, use_container_width=True, hide_index=True)
        else:
            st.info("No fundamentals data.")

# ══════════════════════════════════════════════════════════════════════════════
# MF ADVISOR PAGE
# ══════════════════════════════════════════════════════════════════════════════
elif page == "MF Advisor":
    try:
        from cryptography.fernet import Fernet
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.primitives import hashes
        import base64
        import hashlib
    except ImportError:
        st.error("cryptography package missing — rebuild the dashboard image.")
        st.stop()

    # ── Encryption helpers ────────────────────────────────────────────────────
    def _pid(pp: str) -> str:
        return hashlib.sha256(pp.encode()).hexdigest()

    def _fernet(pp: str, salt_hex: str) -> Fernet:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32,
            salt=bytes.fromhex(salt_hex), iterations=480_000,
        )
        return Fernet(base64.urlsafe_b64encode(kdf.derive(pp.encode())))

    def _save_profile(pp: str, profile: dict) -> None:
        salt = os.urandom(16).hex()
        cipher = _fernet(pp, salt).encrypt(json.dumps(profile).encode()).decode()
        get_ch().insert(
            "system_meta.investor_profiles",
            [[_pid(pp), salt, cipher]],
            column_names=["profile_id", "profile_salt", "profile_data"],
        )

    def _load_profile(pp: str) -> dict | None:
        pid = _pid(pp)
        try:
            df = get_ch().query_df(
                "SELECT profile_salt, profile_data "
                "FROM system_meta.investor_profiles FINAL "
                f"WHERE profile_id = '{pid}' LIMIT 1"
            )
        except Exception:
            return None
        if df.empty:
            return None
        try:
            return json.loads(
                _fernet(pp, df.iloc[0]["profile_salt"]).decrypt(
                    df.iloc[0]["profile_data"].encode()
                )
            )
        except Exception:
            return None

    # ── Page header ───────────────────────────────────────────────────────────
    st.header("MF Advisor")

    # ── Passphrase ────────────────────────────────────────────────────────────
    with st.expander("🔐 Load / Save Profile",
                     expanded="mf_profile" not in st.session_state):
        pp_input = st.text_input("Passphrase", type="password", key="mf_pp")
        pp_c1, pp_c2 = st.columns(2)
        if pp_c1.button("Load Profile"):
            if pp_input:
                _loaded = _load_profile(pp_input)
                if _loaded:
                    st.session_state.mf_profile = _loaded
                    st.success("Profile loaded.")
                    st.rerun()
                else:
                    st.error("No profile found or wrong passphrase.")
            else:
                st.warning("Enter a passphrase first.")
        if pp_c2.button("Save Profile"):
            if pp_input:
                if "mf_profile" in st.session_state:
                    _save_profile(pp_input, st.session_state.mf_profile)
                    st.success("Profile saved.")
                else:
                    st.warning("Fill the profile form first.")
            else:
                st.warning("Enter a passphrase first.")

    # ── Questionnaire ─────────────────────────────────────────────────────────
    st.subheader("Investor Profile")
    prof = st.session_state.get("mf_profile", {})

    with st.form("mf_profile_form"):
        st.markdown("**Personal Context**")
        pc1, pc2, pc3 = st.columns(3)
        p_name       = pc1.text_input("Name / alias (optional)", value=prof.get("name", ""))
        p_age        = pc2.number_input("Age", min_value=18, max_value=80,
                                        value=int(prof.get("age", 35)))
        p_dependents = pc3.number_input("Number of dependents (spouse, kids, parents)",
                                        min_value=0, max_value=15,
                                        value=int(prof.get("dependents", 0)))

        st.markdown("**Monthly Cash Flows (₹)** — helps us suggest a realistic SIP")
        cf1, cf2, cf3 = st.columns(3)
        p_income = cf1.number_input("Take-home income",
                                    min_value=0, value=int(prof.get("monthly_income", 0)),
                                    step=10_000, help="Monthly in-hand salary / business income")
        p_nec    = cf2.number_input("Monthly necessities",
                                    min_value=0, value=int(prof.get("monthly_necessities", 0)),
                                    step=5_000,
                                    help="Non-negotiable fixed costs: rent, groceries, utilities, insurance premiums")
        p_fam    = cf3.number_input("Family commitments",
                                    min_value=0, value=int(prof.get("monthly_family", 0)),
                                    step=5_000,
                                    help="School/college fees, elderly parent support, home loan EMI, other EMIs")
        _avail   = max(0, p_income - p_nec - p_fam)
        st.caption(
            f"Surplus after necessities + family commitments: **₹{_avail:,.0f}/month** "
            f"(thumb rule: invest 50–70% of surplus)"
        )

        st.markdown("**Investment Profile**")
        ip1, ip2 = st.columns(2)
        _HOR_OPTS  = ["< 1 year", "1–3 years", "3–7 years", "7+ years"]
        _RISK_OPTS = ["Conservative", "Moderate", "Aggressive", "Very Aggressive"]
        p_horizon = ip1.radio(
            "Investment horizon", _HOR_OPTS,
            index=_HOR_OPTS.index(prof["horizon_label"])
                  if prof.get("horizon_label") in _HOR_OPTS else 3,
        )
        p_risk = ip2.radio(
            "Risk tolerance", _RISK_OPTS,
            index=_RISK_OPTS.index(prof["risk"])
                  if prof.get("risk") in _RISK_OPTS else 2,
        )

        _GOAL_OPTS = [
            "Wealth Creation", "Retirement",
            "Children's Education / Marriage", "Home Purchase",
            "Tax Saving (ELSS)", "Regular Income (IDCW)",
        ]
        p_goals = st.multiselect(
            "Financial goals (select all that apply)", _GOAL_OPTS,
            default=[g for g in prof.get("goals", ["Wealth Creation"]) if g in _GOAL_OPTS],
        )

        p_emergency = st.checkbox(
            "I already have 6+ months of expenses saved as an emergency fund",
            value=bool(prof.get("emergency_ok", False)),
        )
        if not p_emergency:
            st.info(
                "Tip: park 3–6 months of (necessities + family commitments) in a Liquid fund "
                "before starting aggressive SIPs. It protects you from redeeming equity at the wrong time."
            )

        st.markdown("**SIP & Tax**")
        st1, st2, st3 = st.columns(3)
        _sip_default = int(prof.get("sip_inr", max(500, int(_avail * 0.6))))
        p_sip  = st1.number_input("Monthly SIP (₹)", min_value=500,
                                   value=_sip_default, step=500)
        _TAX   = [0, 10, 20, 30]
        _td    = int(prof.get("tax_bracket", 30))
        p_tax  = st2.selectbox("Income tax bracket (%)", _TAX,
                               index=_TAX.index(_td) if _td in _TAX else 3)
        _PLAN  = ["Direct", "Regular", "Both"]
        p_plan = st3.radio(
            "Fund plan preference", _PLAN,
            index=_PLAN.index(prof["plan_type"]) if prof.get("plan_type") in _PLAN else 0,
            help="Direct plans have lower expense ratios — recommended if you don't use a distributor.",
        )

        st.markdown("**Existing Holdings — categories to avoid duplicating**")
        _CAT_OPTS = [
            "Large Cap", "Mid Cap", "Small Cap", "Flexi Cap", "Multi Cap",
            "Large & Mid Cap", "ELSS", "Balanced Advantage",
            "International / Global", "Liquid", "Ultra Short Duration",
            "Short Duration", "Thematic",
        ]
        p_exclude = st.multiselect(
            "Already heavily invested in", _CAT_OPTS,
            default=[c for c in prof.get("exclude_categories", []) if c in _CAT_OPTS],
        )

        p_submitted = st.form_submit_button(
            "Update Profile & Get Recommendations", use_container_width=True
        )

    if p_submitted:
        _HOR_MAP = {"< 1 year": 0.5, "1–3 years": 2.0, "3–7 years": 5.0, "7+ years": 10.0}
        st.session_state.mf_profile = {
            "name": p_name, "age": int(p_age), "dependents": int(p_dependents),
            "monthly_income": int(p_income), "monthly_necessities": int(p_nec),
            "monthly_family": int(p_fam), "emergency_ok": bool(p_emergency),
            "horizon_label": p_horizon, "horizon_years": _HOR_MAP[p_horizon],
            "risk": p_risk, "goals": p_goals,
            "sip_inr": int(p_sip), "tax_bracket": int(p_tax),
            "plan_type": p_plan, "exclude_categories": p_exclude,
            "saved_at": date.today().isoformat(),
        }
        st.rerun()

    if "mf_profile" not in st.session_state:
        st.info("Complete the profile form above to get fund recommendations.")
        st.stop()

    prof = st.session_state.mf_profile

    if not prof.get("emergency_ok", True):
        st.warning(
            "⚠️  Build your emergency fund first (Liquid / Ultra Short Duration funds). "
            "Rushing into equity SIPs without a buffer often leads to panic withdrawals."
        )

    # ── Category mapping + SIP allocation ─────────────────────────────────────
    horizon_yrs  = float(prof.get("horizon_years", 10.0))
    risk         = prof.get("risk", "Aggressive")
    goals        = prof.get("goals", [])
    tax_goal     = "Tax Saving (ELSS)" in goals
    income_goal  = "Regular Income (IDCW)" in goals
    exclude_cats = set(prof.get("exclude_categories", []))

    def _hb(y: float) -> str:
        return "short" if y < 3 else ("mid" if y < 7 else "long")

    _rk = (risk, "any") if risk == "Conservative" else (risk, _hb(horizon_yrs))

    _BASE_W: dict[tuple, dict] = {
        ("Conservative",    "any"):  {"Liquid": 0.20, "Ultra Short Duration": 0.20,
                                      "Short Duration": 0.30, "Balanced Advantage": 0.30},
        ("Moderate",        "short"): {"Balanced Advantage": 0.40, "Large Cap": 0.30,
                                       "Short Duration": 0.30},
        ("Moderate",        "mid"):   {"Large Cap": 0.35, "Flexi Cap": 0.30,
                                       "Large & Mid Cap": 0.25, "ELSS": 0.10},
        ("Moderate",        "long"):  {"Large Cap": 0.30, "Flexi Cap": 0.25,
                                       "Large & Mid Cap": 0.20, "Mid Cap": 0.15, "ELSS": 0.10},
        ("Aggressive",      "mid"):   {"Flexi Cap": 0.35, "Multi Cap": 0.25,
                                       "Large & Mid Cap": 0.25, "ELSS": 0.10,
                                       "International / Global": 0.05},
        ("Aggressive",      "long"):  {"Large Cap": 0.25, "Flexi Cap": 0.30,
                                       "Small Cap": 0.20, "Multi Cap": 0.10,
                                       "ELSS": 0.10, "International / Global": 0.05},
        ("Very Aggressive", "long"):  {"Small Cap": 0.30, "Mid Cap": 0.25,
                                       "Flexi Cap": 0.30, "Thematic": 0.15},
    }

    _weights = dict(_BASE_W.get(_rk, _BASE_W.get((risk, "long"), {"Flexi Cap": 1.0})))

    for _ec in list(_weights.keys()):
        if _ec in exclude_cats:
            del _weights[_ec]

    _ELSS_CAP = 12_500
    if not tax_goal and "ELSS" in _weights:
        _ew = _weights.pop("ELSS")
        for _fb in ["Flexi Cap", "Large Cap", "Multi Cap", "Balanced Advantage"]:
            if _fb in _weights:
                _weights[_fb] += _ew
                break
    elif tax_goal and "ELSS" not in _weights and "ELSS" not in exclude_cats:
        _weights["ELSS"] = 0.10

    _tw = sum(_weights.values()) or 1.0
    _weights = {k: v / _tw for k, v in _weights.items()}

    sip = int(prof.get("sip_inr", 5_000))
    alloc: dict[str, float] = {}
    _overflow = 0.0
    for _cat, _w in _weights.items():
        _amt = sip * _w
        if _cat == "ELSS" and tax_goal and _amt > _ELSS_CAP:
            _overflow = _amt - _ELSS_CAP
            _amt = _ELSS_CAP
        alloc[_cat] = _amt
    if _overflow > 0:
        for _fb in ["Flexi Cap", "Large Cap", "Multi Cap", "Balanced Advantage"]:
            if _fb in alloc:
                alloc[_fb] += _overflow
                break

    if not alloc:
        st.error("All recommended categories are excluded. Remove some from the exclusion list.")
        st.stop()

    alloc_df = pd.DataFrame([
        {"Category": k, "Amount (₹)": round(v), "Pct": round(v / sip * 100, 1)}
        for k, v in alloc.items()
    ])

    # ── SIP allocation display ────────────────────────────────────────────────
    st.subheader("Recommended SIP Allocation")
    _pc, _tc = st.columns([1, 1])
    with _pc:
        _fp = px.pie(alloc_df, names="Category", values="Amount (₹)",
                     title=f"Monthly SIP: ₹{sip:,.0f}")
        _fp.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(_fp, use_container_width=True)
    with _tc:
        _da = alloc_df.copy()
        _da["Amount (₹)"] = _da["Amount (₹)"].apply(lambda x: f"₹{x:,.0f}")
        _da["Pct"]        = _da["Pct"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(_da, use_container_width=True, hide_index=True)
        st.caption(f"Total: ₹{sum(alloc.values()):,.0f} / month")
        if income_goal:
            st.caption("Income goal → prefer IDCW plans (change plan type filter above).")
        if tax_goal and "ELSS" in alloc:
            st.caption(f"ELSS capped at ₹{_ELSS_CAP:,}/month = ₹{_ELSS_CAP*12:,}/year (full 80C deduction at {prof.get('tax_bracket',30)}%).")

    # ── Fund scoring ──────────────────────────────────────────────────────────
    st.subheader("Fund Recommendations")

    _score_cats = list(alloc.keys())
    _cats_sql   = ", ".join(f"'{c}'" for c in _score_cats)
    _plan_type  = prof.get("plan_type", "Direct")
    if _plan_type == "Direct":
        _plan_filter = "lower(s.scheme_name) LIKE '%direct%'"
    elif _plan_type == "Regular":
        _plan_filter = "lower(s.scheme_name) NOT LIKE '%direct%'"
    else:
        _plan_filter = "1=1"

    _enr = query(f"""
        SELECT e.scheme_code, s.scheme_name, s.scheme_category, s.fund_house,
               e.cagr_5y, e.cagr_3y, e.consistency_score, e.sip_return_5y,
               e.sortino_1y, e.sharpe_1y, e.max_drawdown_pct, e.volatility_1y,
               e.beta_vs_nifty, e.return_1y, e.return_3y, e.return_5y
        FROM market.mf_nav_enriched e FINAL
        JOIN market.mf_schemes s ON e.scheme_code = s.scheme_code
        WHERE s.scheme_category IN ({_cats_sql})
          AND s.is_active = 1
          AND {_plan_filter}
          AND e.return_5y > 0
          AND e.cagr_5y IS NOT NULL
        ORDER BY e.date DESC
        LIMIT 1 BY e.scheme_code
    """)

    if _enr.empty:
        st.warning("No fund data found for selected categories. Ensure mf_nav_enriched is populated.")
        st.stop()

    # ── Scoring helpers ───────────────────────────────────────────────────────
    _W11 = {
        "cagr_5y":           +0.15,
        "consistency_score": +0.15,
        "sip_return_5y":     +0.15,
        "sortino_1y":        +0.12,
        "sharpe_1y":         +0.12,
        "upside_capture":    +0.10,
        "alpha":             +0.05,
        "downside_capture":  -0.12,
        "max_drawdown_pct":  -0.09,
        "volatility_1y":     -0.06,
        "beta_vs_nifty":     -0.04,
    }
    _W9 = {k: v for k, v in _W11.items()
           if k not in ("upside_capture", "downside_capture", "alpha")}

    def _norm_score(sub: pd.DataFrame, weights: dict) -> pd.Series:
        sub = sub.copy()
        _pk = [k for k, v in weights.items() if v > 0 and k in sub.columns]
        _nk = [k for k, v in weights.items() if v < 0 and k in sub.columns]
        for k in _pk + _nk:
            mn, mx = sub[k].min(), sub[k].max()
            sub[k] = (sub[k] - mn) / (mx - mn) if mx > mn else 0.5
        _s = sum(sub[k] * weights[k] for k in _pk) if _pk else pd.Series(0.0, index=sub.index)
        _s -= sum(sub[k] * abs(weights[k]) for k in _nk)
        mn_s, mx_s = _s.min(), _s.max()
        return (_s - mn_s) / (mx_s - mn_s) * 100 if mx_s > mn_s else _s * 0 + 50.0

    # Ensure all metric columns exist and are numeric.
    # upside_capture / downside_capture / alpha come from later joins — skip here.
    _enr = _enr.copy()
    _LATE_COLS = {"upside_capture", "downside_capture", "alpha"}
    for _col in list(_W11.keys()):
        if _col in _LATE_COLS:
            continue
        if _col not in _enr.columns:
            _enr[_col] = 0.0
        else:
            _enr[_col] = pd.to_numeric(_enr[_col], errors="coerce").fillna(0.0)

    # First pass: 9-metric score → shortlist top-30 per category for capture query
    _enr["_s9"] = 0.0
    for _cat in _score_cats:
        _m = _enr["scheme_category"] == _cat
        if _m.sum() > 0:
            _enr.loc[_m, "_s9"] = _norm_score(_enr.loc[_m], _W9).values

    _top30_codes = (
        _enr.sort_values("_s9", ascending=False)
            .groupby("scheme_category")
            .head(30)["scheme_code"]
            .astype(str).tolist()
    )
    _top30_sql = ", ".join(f"'{c}'" for c in _top30_codes)

    # Upside / downside capture for shortlisted funds
    _cap_df = query(f"""
        WITH
        nifty_m AS (
            SELECT toStartOfMonth(date) AS m,
                   (argMax(close, date) - argMin(close, date))
                    / nullIf(argMin(close, date), 0) * 100 AS nr
            FROM market.ohlcv_daily
            WHERE symbol = '^NSEI' AND date >= today() - INTERVAL 5 YEAR
            GROUP BY m
        ),
        fund_m AS (
            SELECT scheme_code, toStartOfMonth(date) AS m,
                   (argMax(nav, date) - argMin(nav, date))
                    / nullIf(argMin(nav, date), 0) * 100 AS fr
            FROM market.mf_nav
            WHERE scheme_code IN ({_top30_sql})
              AND date >= today() - INTERVAL 5 YEAR
            GROUP BY scheme_code, m
        ),
        jnd AS (
            SELECT f.scheme_code, f.fr, n.nr
            FROM fund_m f JOIN nifty_m n ON f.m = n.m
            WHERE n.nr != 0
        )
        SELECT scheme_code,
               avgIf(fr / nr * 100, nr > 0) AS upside_capture,
               avgIf(fr / nr * 100, nr < 0) AS downside_capture
        FROM jnd GROUP BY scheme_code
    """) if _top30_sql else pd.DataFrame()

    # Nifty 5Y CAGR for alpha
    _nc_df = query("""
        SELECT argMax(close, date) / nullIf(argMin(close, date), 0) AS ratio,
               dateDiff('year', min(date), max(date)) AS yrs
        FROM market.ohlcv_daily
        WHERE symbol = '^NSEI' AND date >= today() - INTERVAL 5 YEAR
    """)
    if not _nc_df.empty and float(_nc_df.iloc[0]["yrs"]) > 0:
        _nifty5y = (float(_nc_df.iloc[0]["ratio"]) ** (1.0 / float(_nc_df.iloc[0]["yrs"])) - 1) * 100
    else:
        _nifty5y = 12.0

    if not _cap_df.empty:
        _enr = _enr.merge(_cap_df, on="scheme_code", how="left")
        for _cc in ("upside_capture", "downside_capture"):
            _enr[_cc] = pd.to_numeric(_enr[_cc], errors="coerce").fillna(0.0)
    else:
        _enr["upside_capture"]   = 0.0
        _enr["downside_capture"] = 0.0

    _enr["alpha"] = (_enr["cagr_5y"] - _enr["beta_vs_nifty"] * _nifty5y).fillna(0.0)

    # Second pass: full 11-metric score
    _enr["Quality Score"] = 0.0
    for _cat in _score_cats:
        _m = _enr["scheme_category"] == _cat
        if _m.sum() > 0:
            _enr.loc[_m, "Quality Score"] = _norm_score(_enr.loc[_m], _W11).values

    _enr["Fund"] = _enr["scheme_name"].str[:46]

    _DCOLS = ["Fund", "Quality Score", "return_1y", "cagr_5y", "sharpe_1y",
              "sortino_1y", "beta_vs_nifty", "upside_capture", "downside_capture",
              "alpha", "max_drawdown_pct", "volatility_1y"]
    _DREN  = {
        "return_1y": "1Y%", "cagr_5y": "5Y CAGR",
        "sharpe_1y": "Sharpe", "sortino_1y": "Sortino",
        "beta_vs_nifty": "Beta", "upside_capture": "Up Cap",
        "downside_capture": "Dn Cap", "alpha": "Alpha",
        "max_drawdown_pct": "MaxDD%", "volatility_1y": "Vol%",
    }

    # ── Per-category tabs (top 5) ─────────────────────────────────────────────
    st.markdown("**Top 5 per Category**")
    _tabs = st.tabs(_score_cats)
    for _tab, _cat in zip(_tabs, _score_cats):
        with _tab:
            _sub = (_enr[_enr["scheme_category"] == _cat]
                    .sort_values("Quality Score", ascending=False)
                    .head(5))
            if _sub.empty:
                st.info(f"No data for {_cat}")
                continue
            _d = _sub[[c for c in _DCOLS if c in _sub.columns]].rename(columns=_DREN)
            _nc = [c for c in _d.columns if c != "Fund"]
            st.dataframe(
                _d.style.format({c: "{:.1f}" for c in _nc}, na_rep="—"),
                use_container_width=True, hide_index=True,
            )

    # ── Combined top 10 ───────────────────────────────────────────────────────
    st.markdown("**Combined Top 10 (all categories)**")
    _top10 = _enr.sort_values("Quality Score", ascending=False).head(10)
    _d10   = _top10[["Fund", "scheme_category"] + [c for c in _DCOLS[1:] if c in _top10.columns]].rename(
        columns={"scheme_category": "Category", **_DREN}
    )
    _nc10  = [c for c in _d10.columns if c not in ("Fund", "Category")]
    st.dataframe(
        _d10.style.format({c: "{:.1f}" for c in _nc10}, na_rep="—"),
        use_container_width=True, hide_index=True,
    )

    with st.expander("How is the Quality Score calculated?"):
        # Build a human-readable narrative from the actual weights dict
        _pos_items = sorted(
            [(k, v) for k, v in _W11.items() if v > 0], key=lambda x: -x[1]
        )
        _neg_items = sorted(
            [(k, abs(v)) for k, v in _W11.items() if v < 0], key=lambda x: -x[1]
        )
        _total_pos = sum(v for _, v in _pos_items)
        _total_neg = sum(v for _, v in _neg_items)
        _metric_labels = {
            "cagr_5y": "5-year CAGR", "consistency_score": "consistency score",
            "sip_return_5y": "5-year SIP return", "sortino_1y": "Sortino ratio (1Y)",
            "sharpe_1y": "Sharpe ratio (1Y)", "upside_capture": "upside capture ratio",
            "alpha": "alpha vs Nifty", "downside_capture": "downside capture ratio",
            "max_drawdown_pct": "max drawdown", "volatility_1y": "1-year volatility",
            "beta_vs_nifty": "beta vs Nifty",
        }
        _pos_desc = ", ".join(
            f"**{_metric_labels.get(k, k)}** ({v:.0%})" for k, v in _pos_items
        )
        _neg_desc = ", ".join(
            f"**{_metric_labels.get(k, k)}** ({v:.0%})" for k, v in _neg_items
        )
        st.markdown(
            f"Each fund is given a **Quality Score from 0 to 100**, computed separately "
            f"within its category so funds are always compared to peers — not the entire universe.\n\n"
            f"**{_total_pos:.0%} of the score rewards higher values:** {_pos_desc}.\n\n"
            f"**{_total_neg:.0%} of the score penalises higher values** (lower is safer/better): "
            f"{_neg_desc}.\n\n"
            f"Every metric is first normalised 0–1 within the category (min-max), then multiplied "
            f"by its weight and summed. The final score is scaled so the best fund in a category "
            f"scores 100 and the worst scores 0.\n\n"
            f"*Upside capture* = how much of the market's rally the fund captured; "
            f"*downside capture* = how much of the market's fall the fund absorbed (lower = better). "
            f"*Alpha* = fund's 5Y CAGR minus (beta × Nifty 5Y CAGR) — skill beyond market exposure. "
            f"Both are computed from 5 years of monthly returns against Nifty 50."
        )
        st.dataframe(
            pd.DataFrame([
                {"Metric": _metric_labels.get(k, k),
                 "Direction": "↑ higher is better" if v > 0 else "↓ lower is better",
                 "Weight": f"{abs(v):.0%}"}
                for k, v in _W11.items()
            ]),
            use_container_width=True, hide_index=True,
        )

    # ── NAV trend (indexed to 100, last 3 years) ──────────────────────────────
    st.markdown("**NAV Trend — Top 10 (indexed to 100)**")
    _t10_sql = ", ".join(f"'{c}'" for c in _top10["scheme_code"].astype(str))
    if _t10_sql:
        _nav_df = query(f"""
            SELECT n.scheme_code, n.date, n.nav, s.scheme_name
            FROM market.mf_nav n
            JOIN market.mf_schemes s ON n.scheme_code = s.scheme_code
            WHERE n.scheme_code IN ({_t10_sql})
              AND n.date >= today() - INTERVAL 3 YEAR
            ORDER BY n.scheme_code, n.date
        """)
        if not _nav_df.empty:
            _nav_df["date"] = pd.to_datetime(_nav_df["date"])
            _nav_df = _nav_df.sort_values(["scheme_code", "date"])

            def _idx100(g):
                g = g.copy()
                _f = g["nav"].iloc[0]
                g["indexed"] = g["nav"] / _f * 100 if _f > 0 else g["nav"]
                return g

            _nav_df = _nav_df.groupby("scheme_code", group_keys=False).apply(_idx100)
            _nav_df["label"] = _nav_df["scheme_name"].str[:32]
            _fn = px.line(_nav_df, x="date", y="indexed", color="label",
                          labels={"indexed": "NAV (indexed 100)", "date": "", "label": "Fund"},
                          height=420)
            _fn.update_layout(legend=dict(orientation="h", y=-0.32, font_size=9))
            st.plotly_chart(_fn, use_container_width=True)

    # ── Portfolio Review ───────────────────────────────────────────────────────
    st.divider()
    st.subheader("Portfolio Review")

    _sch_df = query("""
        SELECT scheme_code, scheme_name, scheme_category
        FROM market.mf_schemes WHERE is_active = 1
        ORDER BY scheme_name
    """)

    if _sch_df.empty:
        st.info("No scheme data available for fund lookup.")
        st.stop()

    _sch_map = dict(zip(_sch_df["scheme_name"], _sch_df["scheme_code"].astype(str)))

    st.markdown("Enter your current mutual fund holdings:")
    _hd = prof.get("holdings", [])
    _htmpl = (
        pd.DataFrame(_hd)[["Fund", "Units", "Avg NAV"]]
        if isinstance(_hd, list) and _hd and isinstance(_hd[0], dict) and "Fund" in _hd[0]
        else pd.DataFrame([{"Fund": "", "Units": 0.0, "Avg NAV": 0.0}])
    )

    _hedit = st.data_editor(
        _htmpl,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "Fund": st.column_config.SelectboxColumn(
                "Fund Name", options=list(_sch_map.keys()), required=False,
            ),
            "Units": st.column_config.NumberColumn("Units held", min_value=0.0, format="%.3f"),
            "Avg NAV": st.column_config.NumberColumn("Avg purchase NAV (₹)", min_value=0.0, format="%.2f"),
        },
    )

    if st.checkbox("Save holdings with profile on next 'Save Profile'"):
        st.session_state.mf_profile["holdings"] = _hedit.to_dict("records")

    if st.button("Analyse Portfolio", use_container_width=True):
        _valid = _hedit[
            _hedit["Fund"].notna() & (_hedit["Fund"] != "") &
            (_hedit["Units"] > 0) & (_hedit["Avg NAV"] > 0)
        ].copy()

        if _valid.empty:
            st.warning("Add at least one holding with units and avg NAV.")
        else:
            _valid["scheme_code"] = _valid["Fund"].map(_sch_map)
            _hcodes = _valid["scheme_code"].dropna().astype(str).tolist()
            _hsql   = ", ".join(f"'{c}'" for c in _hcodes)

            _lat = query(f"""
                SELECT n.scheme_code,
                       argMax(n.nav, n.date) AS current_nav,
                       any(s.scheme_category) AS category,
                       any(e.return_1y)  AS ret1y,
                       any(e.return_3y)  AS ret3y,
                       any(e.return_5y)  AS ret5y
                FROM market.mf_nav n
                JOIN market.mf_schemes s ON n.scheme_code = s.scheme_code
                LEFT JOIN (
                    SELECT scheme_code, return_1y, return_3y, return_5y
                    FROM market.mf_nav_enriched FINAL
                    LIMIT 1 BY scheme_code
                ) e ON n.scheme_code = e.scheme_code
                WHERE n.scheme_code IN ({_hsql})
                GROUP BY n.scheme_code
            """)

            if _lat.empty:
                st.warning("Could not fetch current NAV for selected funds.")
            else:
                _valid["scheme_code"] = _valid["scheme_code"].astype(str)
                _lat["scheme_code"]   = _lat["scheme_code"].astype(str)
                _mg = _valid.merge(_lat, on="scheme_code", how="left")
                _mg["current_nav"]    = pd.to_numeric(_mg["current_nav"], errors="coerce")
                _mg["current_value"]  = _mg["Units"] * _mg["current_nav"]
                _mg["invested_value"] = _mg["Units"] * _mg["Avg NAV"]
                _mg["return_pct"]     = ((_mg["current_nav"] / _mg["Avg NAV"] - 1) * 100).round(2)

                _bench = query("""
                    SELECT
                        (argMax(close, date) -
                         argMaxIf(close, date, date <= today() - INTERVAL 1 YEAR)) /
                         nullIf(argMaxIf(close, date, date <= today() - INTERVAL 1 YEAR), 0)
                         * 100 AS b1y,
                        (argMax(close, date) -
                         argMaxIf(close, date, date <= today() - INTERVAL 3 YEAR)) /
                         nullIf(argMaxIf(close, date, date <= today() - INTERVAL 3 YEAR), 0)
                         * 100 AS b3y
                    FROM market.ohlcv_daily WHERE symbol = '^NSEI'
                """)
                _b1 = float(_bench.iloc[0]["b1y"]) if not _bench.empty else 0.0
                _b3 = float(_bench.iloc[0]["b3y"]) if not _bench.empty else 0.0

                _mg["alpha_1y"] = (pd.to_numeric(_mg["ret1y"], errors="coerce") - _b1).round(2)
                _mg["alpha_3y"] = (pd.to_numeric(_mg["ret3y"], errors="coerce") - _b3).round(2)

                _ti = _mg["invested_value"].sum()
                _tc = _mg["current_value"].sum()
                _tg = _tc - _ti

                _m1, _m2, _m3 = st.columns(3)
                _m1.metric("Total Invested",  f"₹{_ti:,.0f}")
                _m2.metric("Current Value",   f"₹{_tc:,.0f}")
                _m3.metric("Gain / Loss",     f"₹{_tg:,.0f}",
                           f"{_tg / _ti * 100:.1f}%" if _ti > 0 else "—")

                _sh = _mg[["Fund", "category", "Units", "Avg NAV",
                            "current_nav", "current_value", "return_pct",
                            "ret1y", "alpha_1y", "ret3y", "alpha_3y"]].rename(columns={
                    "category": "Category", "current_nav": "Curr NAV",
                    "current_value": "Value (₹)", "return_pct": "Return%",
                    "ret1y": "1Y%", "alpha_1y": "Alpha 1Y",
                    "ret3y": "3Y%", "alpha_3y": "Alpha 3Y",
                })
                st.dataframe(
                    _sh.style.format({
                        "Value (₹)": "₹{:,.0f}", "Return%": "{:.2f}",
                        "1Y%": "{:.1f}", "Alpha 1Y": "{:.1f}",
                        "3Y%": "{:.1f}", "Alpha 3Y": "{:.1f}",
                    }, na_rep="—"),
                    use_container_width=True, hide_index=True,
                )

                # Rebalancing vs target allocation
                st.markdown("**Rebalancing vs Target**")
                _cg = (_mg.groupby("category")["current_value"]
                          .sum().reset_index()
                          .rename(columns={"category": "Category",
                                           "current_value": "Current (₹)"}))
                _cg["Current%"] = (_cg["Current (₹)"] / _tc * 100 if _tc > 0 else 0)
                _tp = {k: v / sip * 100 for k, v in alloc.items()}
                _cg["Target%"] = _cg["Category"].map(_tp).fillna(0)
                _cg["Delta%"]  = _cg["Current%"] - _cg["Target%"]

                for _tc2, _tw2 in _tp.items():
                    if _tc2 not in _cg["Category"].values:
                        _cg = pd.concat([_cg, pd.DataFrame([{
                            "Category": _tc2, "Current (₹)": 0,
                            "Current%": 0.0, "Target%": _tw2, "Delta%": -_tw2,
                        }])], ignore_index=True)

                _cg["Action"] = _cg["Delta%"].apply(
                    lambda d: "🔴 REDUCE" if d > 5 else ("🟡 ADD" if d < -5 else "🟢 OK")
                )
                _rd = _cg[["Category", "Target%", "Current%", "Delta%", "Action"]].copy()
                _rd["Target%"]  = _rd["Target%"].apply(lambda x: f"{x:.1f}%")
                _rd["Current%"] = _rd["Current%"].apply(lambda x: f"{x:.1f}%")
                _rd["Delta%"]   = _rd["Delta%"].apply(lambda x: f"{x:+.1f}%")
                st.dataframe(_rd, use_container_width=True, hide_index=True)

                _p1, _p2 = st.columns(2)
                with _p1:
                    _fcp = px.pie(_cg, names="Category", values="Current (₹)",
                                  title="Current Allocation")
                    st.plotly_chart(_fcp, use_container_width=True)
                with _p2:
                    _ftp = px.pie(alloc_df, names="Category", values="Amount (₹)",
                                  title="Target Allocation")
                    st.plotly_chart(_ftp, use_container_width=True)
