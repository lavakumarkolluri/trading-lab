#!/usr/bin/env python3
"""
scheduler.py
────────────
Docker-native cron scheduler for the trading pipeline (OPS-001).
Runs inside a long-lived container — no host cron required.

Schedule (all times UTC, IST = UTC+5:30):
  Daily    — Mon–Fri 03:40 UTC (09:10 IST)  → option_chain_intraday (self-exits 15:35 IST)
  Daily    — Mon–Fri 11:00 UTC (16:30 IST)  → meta_pipeline (steps 1-12)
  Daily    — Mon–Fri 12:30 UTC (18:00 IST)  → option_chain_historical (bhavcopy pickup)
  Daily    — Mon–Fri 13:00 UTC (18:30 IST)  → options_eod_summary_pipeline (PCR/max pain)
  Daily    — Mon–Fri 13:30 UTC (19:00 IST)  → compute_historical_iv (ATM IV + rank)
  Weekly   — Sun     00:30 UTC (06:00 IST)  → meta_pipeline --weekly (steps 13-16)
  Weekly   — Sun     01:00 UTC (06:30 IST)  → gap_analyzer
  Weekly   — Sun     02:00 UTC (07:30 IST)  → option_backtest (full 2yr refresh)
  Weekly   — Sun     03:00 UTC (08:30 IST)  → mf_pipeline (NAV refresh)
  Weekly   — Sun     06:30 UTC (12:00 IST)  → strategy_selector --backtest
  Daily    — Mon–Thu 10:30 UTC (16:00 IST)  → strategy_selector --recommend
  Monthly  — 1st     04:00 UTC (09:30 IST)  → holidays_pipeline (seed next year)

Docker:
  docker compose up -d scheduler
"""

import os
import logging
import subprocess
import time
from datetime import datetime, timedelta

import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

import sys

_COMPOSE_FILE = os.getenv("COMPOSE_FILE", "docker-compose.yml")
_PROJECT_DIR  = os.path.dirname(_COMPOSE_FILE) if "/" in _COMPOSE_FILE else "."
_GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
_GIT_DIR      = _PROJECT_DIR  # /trading-lab inside container

COMPOSE_CMD = [
    "docker", "compose",
    "-f", _COMPOSE_FILE,
    "--project-directory", _PROJECT_DIR,
    "run", "--rm",
]
_COMPOSE_BASE = [
    "docker", "compose",
    "-f", _COMPOSE_FILE,
    "--project-directory", _PROJECT_DIR,
]

try:
    from pipeline_utils import GIT_SHA
    from ch_utils import ch_client as _ch_client
    _TRACKING_OK = True
except ImportError:
    GIT_SHA = "unknown"
    _TRACKING_OK = False
    log.warning("pipeline_utils/ch_utils not importable — pipeline_runs tracking disabled")


def _record_run(service: str, started_at: datetime, status: str,
                finished_at: datetime | None = None, error_msg: str = ""):
    if not _TRACKING_OK:
        return
    if finished_at is None:
        finished_at = datetime.utcnow()
    try:
        ch = _ch_client()
        ch.command(
            """
            INSERT INTO system_meta.pipeline_runs
                (service, started_at, finished_at, status, git_sha, error_msg)
            VALUES (
                {service:String}, {started_at:DateTime}, {finished_at:DateTime},
                {status:String}, {git_sha:String}, {error_msg:String}
            )
            """,
            parameters={
                "service":     service,
                "started_at":  started_at,
                "finished_at": finished_at,
                "status":      status,
                "git_sha":     GIT_SHA,
                "error_msg":   error_msg,
            },
        )
    except Exception as e:
        log.warning("pipeline_runs write failed for %s: %s", service, e)


def _git(args: list, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a git command inside the project directory."""
    return subprocess.run(
        ["git", "-C", _GIT_DIR] + args,
        capture_output=True, text=True, timeout=timeout,
    )


def _compose(args: list, timeout: int = 300):
    """Run a docker compose command (build / up -d / etc.)."""
    subprocess.run(_COMPOSE_BASE + args, check=False, timeout=timeout)


def job_auto_deploy():
    """
    Pull latest master from GitHub, detect changed files, rebuild affected
    images, and restart services — all without any manual intervention.

    Rebuild rules:
      pipeline/**               → rebuild pipeline image → self-exit (restart policy
                                   brings new scheduler up; one-shot services pick up
                                   new image on next scheduled run automatically)
      dashboard/Dockerfile      → rebuild + restart dashboard container
      dashboard/requirements.txt→ rebuild + restart dashboard container
      dashboard/app.py          → no-op (volume-mounted, Streamlit hot-reloads)
      docker-compose.yml        → docker compose up -d (apply config changes)
      anything else             → log only

    Runs every 15 minutes. No-op if already on latest commit.
    """
    try:
        # ── Current HEAD ─────────────────────────────────────────────────────
        r = _git(["rev-parse", "HEAD"])
        if r.returncode != 0:
            log.warning("AUTO-DEPLOY: git rev-parse HEAD failed — is %s a git repo?", _GIT_DIR)
            return
        old_sha = r.stdout.strip()

        # ── Fetch latest master ───────────────────────────────────────────────
        if _GITHUB_TOKEN:
            remote = (
                f"https://{_GITHUB_TOKEN}@github.com/"
                f"lavakumarkolluri/trading-lab.git"
            )
        else:
            remote = "origin"

        fetch = _git(["fetch", remote, "master:refs/remotes/origin/master"], timeout=45)
        if fetch.returncode != 0:
            log.warning("AUTO-DEPLOY: git fetch failed: %s", fetch.stderr.strip()[:200])
            return

        new_sha = _git(["rev-parse", "origin/master"]).stdout.strip()
        if old_sha == new_sha:
            return  # already up to date

        log.info("AUTO-DEPLOY: new commit detected %s → %s", old_sha[:8], new_sha[:8])

        # ── Identify changed files ────────────────────────────────────────────
        diff = _git(["diff", "--name-only", old_sha, new_sha])
        changed = [f.strip() for f in diff.stdout.strip().splitlines() if f.strip()]
        log.info("AUTO-DEPLOY: %d file(s) changed: %s",
                 len(changed), ", ".join(changed[:15]))

        # ── Fast-forward merge ────────────────────────────────────────────────
        merge = _git(["merge", "--ff-only", "origin/master"])
        if merge.returncode != 0:
            log.warning("AUTO-DEPLOY: merge failed (diverged history?): %s",
                        merge.stderr.strip()[:200])
            return

        log.info("AUTO-DEPLOY: merged to %s", new_sha[:8])

        # ── Rebuild dashboard if its infra files changed ──────────────────────
        dashboard_infra = {"dashboard/Dockerfile", "dashboard/requirements.txt"}
        if any(f in dashboard_infra for f in changed):
            log.info("AUTO-DEPLOY: rebuilding dashboard image...")
            _compose(["build", "dashboard"])
            _compose(["up", "-d", "dashboard"])
            log.info("AUTO-DEPLOY: dashboard rebuilt and restarted")

        # ── Apply compose config changes ──────────────────────────────────────
        if "docker-compose.yml" in changed:
            log.info("AUTO-DEPLOY: docker-compose.yml changed — applying with up -d...")
            _compose(["up", "-d", "--remove-orphans"])

        # ── Rebuild pipeline image and self-restart ───────────────────────────
        pipeline_changed = any(f.startswith("pipeline/") for f in changed)
        if pipeline_changed:
            log.info("AUTO-DEPLOY: pipeline code changed — rebuilding image...")
            _compose(["build", "scheduler"])   # scheduler = pipeline image
            log.info("AUTO-DEPLOY: rebuild done — exiting for self-restart "
                     "(restart: unless-stopped will bring up new image)")
            sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        log.error("AUTO-DEPLOY: unexpected error: %s", e, exc_info=True)


# ── Dependency map ─────────────────────────────────────────────────────────
# Maps each service to the upstream services that must have succeeded today
# before it is allowed to run. Missing or failed upstream → skip with 'skipped'.
UPSTREAM_DEPS: dict[str, list[str]] = {
    "options_eod_summary_pipeline": ["option_chain_historical"],
    "compute_historical_iv":        ["options_eod_summary_pipeline"],
    "compute_oi_features":          ["compute_historical_iv"],
    "confidence_scorer":            ["compute_oi_features", "strategy_backtester"],
    "strategy_selector":            ["confidence_scorer"],
}


def _upstream_ok(service: str, today_date: str) -> tuple[bool, str]:
    """Return (ok, reason). ok=True means all upstreams succeeded today."""
    deps = UPSTREAM_DEPS.get(service, [])
    if not deps or not _TRACKING_OK:
        return True, ""
    try:
        ch = _ch_client()
        for dep in deps:
            result = ch.query(
                """
                SELECT status FROM system_meta.pipeline_runs FINAL
                WHERE service  = {service:String}
                  AND run_date = {run_date:Date}
                ORDER BY version DESC LIMIT 1
                """,
                parameters={"service": dep, "run_date": today_date},
            )
            if not result.result_rows:
                return False, f"upstream '{dep}' has no run record for {today_date}"
            status = result.result_rows[0][0]
            if status != "success":
                return False, f"upstream '{dep}' status={status} on {today_date}"
    except Exception as e:
        log.warning("Dependency gate check failed: %s", e)
        return True, ""   # fail-open: don't block if we can't query
    return True, ""


def _run(service: str, *args: str):
    cmd = COMPOSE_CMD + [service] + list(args)
    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    ok, reason = _upstream_ok(service, today_str)
    if not ok:
        log.warning("SKIPPING %s — %s", service, reason)
        started = datetime.utcnow()
        _record_run(service, started, "skipped", error_msg=reason)
        return

    log.info("Running: %s", " ".join(cmd))
    started = datetime.utcnow()
    try:
        result = subprocess.run(cmd, capture_output=False, text=True, check=False)
    except Exception as e:
        _record_run(service, started, "failed", error_msg=str(e))
        log.error("Command exception for %s: %s", service, e)
        return
    if result.returncode != 0:
        log.error("Command failed (exit %d): %s", result.returncode, " ".join(cmd))
        _record_run(service, started, "failed", error_msg=f"exit {result.returncode}")
    else:
        log.info("Done: %s", service)
        _record_run(service, started, "success")


def job_daily():
    log.info("=== Daily pipeline triggered ===")
    _run("meta_pipeline")


def job_weekly():
    log.info("=== Weekly refresh triggered ===")
    _run("meta_pipeline", "--weekly")


def job_gap_analyzer():
    log.info("=== Gap analyzer triggered ===")
    _run("gap_analyzer")


def job_fii_dii_pipeline():
    log.info("=== FII/DII pipeline triggered ===")
    _run("fii_dii_pipeline")


def job_participant_oi_pipeline():
    log.info("=== Participant OI pipeline triggered ===")
    _run("participant_oi_pipeline")


def job_option_backtest():
    log.info("=== Option backtest weekly refresh triggered ===")
    _run("option_backtest")                        # buy strategy
    _run("option_backtest", "--strategy", "sell")  # sell strategy
    _run("nifty_straddle_backtest")               # weekly straddle backtest refresh


def job_mf_pipeline():
    log.info("=== MF pipeline weekly NAV refresh triggered ===")
    _run("mf_pipeline")


def job_fundamental_pipeline():
    log.info("=== Fundamental data pipeline weekly refresh triggered ===")
    _run("fundamental_pipeline")


def job_lot_size_pipeline():
    log.info("=== F&O lot size pipeline weekly refresh triggered ===")
    _run("lot_size_pipeline")


def job_strategy_backtester():
    log.info("=== Strategy backtester weekly refresh triggered ===")
    _run("strategy_backtester")


def job_confidence_scorer():
    log.info("=== Confidence scorer weekly retrain triggered ===")
    _run("confidence_scorer", "--compare")


def job_strategy_selector_recommend():
    log.info("=== Strategy selector daily recommendation triggered ===")
    _run("strategy_selector", "--recommend")


def job_strategy_selector_backtest():
    log.info("=== Strategy selector compounding simulation triggered ===")
    _run("strategy_selector", "--backtest")


def job_strategy_selector_fill_outcomes():
    log.info("=== Strategy selector outcome fill-back triggered ===")
    _run("strategy_selector", "--fill-outcomes")


def job_events_pipeline():
    """Weekly refresh of FOMC/RBI/budget event calendar."""
    log.info("=== Events pipeline weekly refresh triggered ===")
    _run("events_pipeline")


def job_holidays():
    """Run only on the 1st of each month."""
    if datetime.utcnow().day != 1:
        return
    log.info("=== Holidays pipeline monthly refresh triggered ===")
    _run("holidays_pipeline")


def job_vix_pipeline():
    log.info("=== VIX pipeline triggered ===")
    _run("vix_pipeline")


def job_cleanup():
    """Weekly disk cleanup: MinIO intraday snapshots >30 days, workspace logs >7 days."""
    log.info("=== Weekly cleanup triggered ===")
    try:
        _cleanup_minio_intraday()
    except Exception as e:
        log.error("MinIO cleanup failed: %s", e)
    try:
        _cleanup_logs()
    except Exception as e:
        log.error("Log cleanup failed: %s", e)


def _cleanup_minio_intraday():
    """Delete MinIO option_chain/intraday/ objects older than 30 days."""
    minio_host = os.getenv("MINIO_HOST", "minio:9000")
    minio_user = os.getenv("MINIO_USER", "admin")
    minio_pass = os.getenv("MINIO_PASSWORD", "")
    if not minio_pass:
        log.warning("MINIO_PASSWORD not set — skipping MinIO cleanup")
        return

    try:
        from minio import Minio
        mc = Minio(minio_host, access_key=minio_user,
                   secret_key=minio_pass, secure=False)
        cutoff = datetime.utcnow() - timedelta(days=30)
        bucket = "trading-data"
        if not mc.bucket_exists(bucket):
            return
        objects = mc.list_objects(bucket, prefix="option_chain/intraday/", recursive=True)
        deleted = 0
        for obj in objects:
            if obj.last_modified and obj.last_modified.replace(tzinfo=None) < cutoff:
                mc.remove_object(bucket, obj.object_name)
                deleted += 1
        log.info("MinIO cleanup: deleted %d intraday objects older than 30 days", deleted)
    except ImportError:
        log.warning("minio package not available — skipping MinIO cleanup")


def _cleanup_logs():
    """Truncate workspace log files older than 7 days to keep disk usage bounded."""
    import glob as _glob
    log_dir = os.getenv("LOG_DIR", "/trading-lab/workspace/logs")
    cutoff = datetime.utcnow() - timedelta(days=7)
    for path in _glob.glob(f"{log_dir}/*.log"):
        try:
            mtime = datetime.utcfromtimestamp(os.path.getmtime(path))
            size_mb = os.path.getsize(path) / (1024 * 1024)
            if mtime < cutoff or size_mb > 50:
                open(path, "w").close()  # truncate
                log.info("Truncated log: %s (was %.1f MB)", path, size_mb)
        except Exception as e:
            log.warning("Could not truncate %s: %s", path, e)


def job_option_chain_intraday():
    log.info("=== Option chain intraday scraper triggered ===")
    _run("option_chain_intraday")


def job_intraday_monitor():
    log.info("=== Intraday straddle monitor triggered ===")
    _run("intraday_monitor")


def job_option_chain_eod():
    """
    Daily bhavcopy chain: download → PCR/max pain → IV → OI walls/skew → VIX.
    Runs sequentially after NSE publishes bhavcopy (~17:30-18:00 IST).
    Each step is idempotent — safe to re-run if a step fails.
    """
    log.info("=== Option chain EOD pipeline triggered ===")
    _run("option_chain_historical")         # download new bhavcopy day
    _run("options_eod_summary_pipeline")    # compute PCR + max pain
    _run("compute_historical_iv")           # compute ATM IV + iv_rank
    _run("compute_oi_features")             # compute OI walls + IV skew + FII futures
    _run("vix_pipeline")                    # VIX + market regime (required by confidence_scorer)


def job_data_freshness_check():
    """Daily data quality watchdog — runs after EOD pipelines complete."""
    log.info("=== Data freshness check triggered ===")
    _run("data_freshness_check")


def _startup_recovery():
    """
    On scheduler restart, check if today's critical EOD jobs were missed.
    Only re-triggers if we're within a 4-hour catch-up window of the expected
    run time and today is a weekday.
    """
    now_utc = datetime.utcnow()
    if now_utc.weekday() >= 5:  # weekend — no recovery
        return
    if not _TRACKING_OK:
        log.warning("Startup recovery skipped — tracking unavailable")
        return

    today_str = now_utc.strftime("%Y-%m-%d")
    # EOD chain runs at 12:30 UTC; recovery window is 12:30–16:30 UTC (4h)
    eod_window_start = now_utc.replace(hour=12, minute=30, second=0, microsecond=0)
    eod_window_end   = eod_window_start + timedelta(hours=4)

    if not (eod_window_start <= now_utc <= eod_window_end):
        return

    try:
        ch = _ch_client()
        result = ch.query(
            """
            SELECT status FROM system_meta.pipeline_runs FINAL
            WHERE service  = {service:String}
              AND run_date = {run_date:Date}
            ORDER BY version DESC LIMIT 1
            """,
            parameters={"service": "option_chain_historical", "run_date": today_str},
        )
        if result.result_rows:
            return  # already ran today — no recovery needed

        elapsed_min = (now_utc - eod_window_start).total_seconds() / 60
        log.warning(
            "STARTUP RECOVERY: option_chain_historical has no run record for %s "
            "(scheduler was likely down; restarted %d min after window). "
            "Re-triggering EOD chain now.",
            today_str, int(elapsed_min),
        )
        job_option_chain_eod()
    except Exception as e:
        log.warning("Startup recovery check failed: %s", e)


def _recompute_check():
    """Print today's run status for all tracked services and exit."""
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"\nRecompute check — {today_str}")
    print(f"{'Service':<40} {'Status':<10} {'Upstream OK'}")
    print("-" * 70)
    all_services = list(UPSTREAM_DEPS.keys()) + [
        s for s in [
            "option_chain_historical", "meta_pipeline",
            "fii_dii_pipeline", "participant_oi_pipeline",
            "vix_pipeline", "strategy_backtester",
        ] if s not in UPSTREAM_DEPS
    ]
    if not _TRACKING_OK:
        print("  (tracking unavailable — ch_utils not importable)")
        return
    try:
        ch = _ch_client()
        for svc in all_services:
            result = ch.query(
                """
                SELECT status FROM system_meta.pipeline_runs FINAL
                WHERE service  = {service:String}
                  AND run_date = {run_date:Date}
                ORDER BY version DESC LIMIT 1
                """,
                parameters={"service": svc, "run_date": today_str},
            )
            status = result.result_rows[0][0] if result.result_rows else "no record"
            up_ok, reason = _upstream_ok(svc, today_str)
            upstream_col = "ok" if up_ok else f"BLOCKED ({reason})"
            print(f"  {svc:<38} {status:<10} {upstream_col}")
    except Exception as e:
        print(f"  Error: {e}")
    print()


def main():
    import sys as _sys
    if "--recompute-check" in _sys.argv:
        _recompute_check()
        return

    log.info("Scheduler started — all times UTC (git_sha=%s)", GIT_SHA)
    _startup_recovery()
    log.info("  Events pipeline     : Sun     00:00 UTC (05:30 IST) --weekly")
    log.info("  Intraday OC scraper : Mon–Fri 03:40 UTC (09:10 IST)")
    log.info("  Intraday monitor    : Mon–Fri 03:50 UTC (09:20 IST) paper straddle")
    log.info("  Daily   pipeline    : Mon–Fri 11:00 UTC (16:30 IST)")
    log.info("  Option chain EOD    : Mon–Fri 12:30 UTC (18:00 IST) → historical+PCR+IV+VIX")
    log.info("  Weekly  refresh     : Sun     00:30 UTC (06:00 IST)")
    log.info("  Gap     analyzer    : Sun     01:00 UTC (06:30 IST)")
    log.info("  Option  backtest    : Sun     02:00 UTC (07:30 IST)")
    log.info("  MF      pipeline    : Sun     03:00 UTC (08:30 IST)")
    log.info("  Fundamentals        : Sun     04:30 UTC (10:00 IST)")
    log.info("  Lot sizes           : Sun     05:00 UTC (10:30 IST)")
    log.info("  Confidence scorer   : Sun     07:00 UTC (12:30 IST) --compare (90 min after backtester)")
    log.info("  Strategy selector   : Sun     08:00 UTC (13:30 IST) --backtest")
    log.info("  Data freshness check: Mon-Fri 13:30 UTC (19:00 IST) auto-fix stale sources")
    log.info("  Outcome fill-back   : Mon-Fri 14:00 UTC (19:30 IST) --fill-outcomes (marks yesterday's recommendations)")
    log.info("  Strategy recommend  : Mon-Thu 10:30 UTC (16:00 IST) --recommend")
    log.info("  FII/DII + ParticOI  : Mon-Fri 10:45 UTC (16:15 IST) pre-feed for meta_pipeline")
    log.info("  Holidays pipeline   : 1st of month 04:00 UTC (09:30 IST)")
    log.info("  Auto-deploy         : every 15 min — git pull + rebuild on change")

    # Intraday option chain: start at 09:10 IST (03:40 UTC), self-exits at 15:35 IST
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("03:40").do(job_option_chain_intraday)

    # Intraday straddle monitor: 09:20 IST (03:50 UTC), self-exits at 15:25 IST
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("03:50").do(job_intraday_monitor)

    # FII/DII + Participant OI: run before meta_pipeline so data is ready
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("10:45").do(job_fii_dii_pipeline)
        getattr(schedule.every(), day).at("10:45").do(job_participant_oi_pipeline)

    # Daily meta pipeline: 16:30 IST (11:00 UTC)
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("11:00").do(job_daily)

    # Option chain EOD: bhavcopy + PCR/max pain + IV at 18:00 IST (12:30 UTC)
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("12:30").do(job_option_chain_eod)

    schedule.every().sunday.at("00:00").do(job_events_pipeline)
    schedule.every().sunday.at("00:30").do(job_weekly)
    schedule.every().sunday.at("01:00").do(job_gap_analyzer)
    schedule.every().sunday.at("02:00").do(job_option_backtest)
    schedule.every().sunday.at("03:00").do(job_mf_pipeline)
    schedule.every().sunday.at("04:30").do(job_fundamental_pipeline)
    schedule.every().sunday.at("05:00").do(job_lot_size_pipeline)
    schedule.every().sunday.at("05:30").do(job_strategy_backtester)
    schedule.every().sunday.at("07:00").do(job_confidence_scorer)   # 90 min after backtester
    schedule.every().sunday.at("08:00").do(job_strategy_selector_backtest)
    schedule.every().sunday.at("09:00").do(job_cleanup)  # 09:00 UTC = 14:30 IST

    # Daily recommendation: 30 min before market open (10:30 UTC = 16:00 IST)
    for day in ("monday", "tuesday", "wednesday", "thursday"):
        getattr(schedule.every(), day).at("10:30").do(job_strategy_selector_recommend)

    # Data freshness watchdog: after all EOD pipelines (13:30 UTC = 19:00 IST)
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("13:30").do(job_data_freshness_check)

    # Outcome fill-back: after EOD pipeline (14:00 UTC = 19:30 IST)
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("14:00").do(job_strategy_selector_fill_outcomes)

    # Monthly: schedule runs daily at 04:00, guard inside job checks day==1
    schedule.every().day.at("04:00").do(job_holidays)

    # Auto-deploy: pull master every 15 min, rebuild+restart if code changed
    schedule.every(15).minutes.do(job_auto_deploy)
    job_auto_deploy()  # check immediately on startup too

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
