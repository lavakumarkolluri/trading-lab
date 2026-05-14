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
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone

import schedule

from logging_utils import get_logger
log = get_logger(__name__)

import sys

_IST = timezone(timedelta(hours=5, minutes=30))

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

from ch_utils import ch_client as _ch_client, GIT_SHA
_TRACKING_OK = True


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
    env = os.environ.copy()
    env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    return subprocess.run(
        ["git", "-C", _GIT_DIR, "-c", f"safe.directory={_GIT_DIR}"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def _compose(args: list, timeout: int = 300):
    """Run a docker compose command (build / up -d / etc.)."""
    subprocess.run(_COMPOSE_BASE + args, check=False, timeout=timeout)


_DEPLOY_SHA_FILE = "/tmp/auto_deploy_last_sha"


def _get_last_deployed_sha() -> str:
    """Read the last SHA we deployed from origin/master; empty string if unknown."""
    try:
        with open(_DEPLOY_SHA_FILE) as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _save_deployed_sha(sha: str) -> None:
    with open(_DEPLOY_SHA_FILE, "w") as f:
        f.write(sha)


def job_auto_deploy():
    """
    Poll origin/master every 15 min. On new commits, detect changed files,
    rebuild affected images, and self-restart — without touching the local branch.

    Rebuild rules:
      pipeline/**               → rebuild pipeline image → self-exit (restart policy
                                   brings new scheduler up; one-shot services pick up
                                   new image on next scheduled run automatically)
      dashboard/Dockerfile      → rebuild + restart dashboard container
      dashboard/requirements.txt→ rebuild + restart dashboard container
      dashboard/app.py          → no-op (volume-mounted, Streamlit hot-reloads)
      docker-compose.yml        → docker compose up -d (apply config changes)
      anything else             → log only

    Never merges or checks out — safe to run while local branch is on stage.
    """
    try:
        # ── Fetch latest master (no working-tree changes) ─────────────────────
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
        if not new_sha:
            log.warning("AUTO-DEPLOY: could not resolve origin/master SHA")
            return

        old_sha = _get_last_deployed_sha()
        if not old_sha:
            # First run — record current master, nothing to deploy yet
            _save_deployed_sha(new_sha)
            log.info("AUTO-DEPLOY: initialised — tracking origin/master at %s", new_sha[:8])
            return

        if old_sha == new_sha:
            return  # already up to date

        log.info("AUTO-DEPLOY: new commit detected %s → %s", old_sha[:8], new_sha[:8])

        # ── Identify changed files ────────────────────────────────────────────
        diff = _git(["diff", "--name-only", old_sha, new_sha])
        changed = [f.strip() for f in diff.stdout.strip().splitlines() if f.strip()]
        log.info("AUTO-DEPLOY: %d file(s) changed: %s",
                 len(changed), ", ".join(changed[:15]))

        _save_deployed_sha(new_sha)

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

        # ── Run tests on any code change ─────────────────────────────────────
        code_changed = any(
            f.startswith("pipeline/") or f.startswith("tests/")
            or f.startswith("dashboard/") or f.startswith("clickhouse/")
            for f in changed
        )
        if code_changed:
            log.info("AUTO-DEPLOY: code changed — running test suite...")
            _run_tests_and_record()

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


def job_confidence_scorer_daily():
    log.info("=== Confidence scorer daily score-only triggered ===")
    _run("confidence_scorer", "--score-only")


def job_strategy_selector_recommend():
    log.info("=== Strategy selector daily recommendation triggered ===")
    _run("strategy_selector", "--recommend")


def job_strategy_selector_backtest():
    log.info("=== Strategy selector compounding simulation triggered ===")
    _run("strategy_selector", "--backtest")


def job_strategy_selector_fill_outcomes():
    log.info("=== Strategy selector outcome fill-back triggered ===")
    _run("strategy_selector", "--fill-outcomes")


def _run_tests_and_record():
    """
    Run offline test suite and write results to system_meta.ci_results.
    Called after every successful auto-deploy that changes code.
    Tests run against the mounted source tree at /trading-lab/tests/.
    """
    if not _TRACKING_OK:
        log.warning("TEST-RUN: tracking unavailable, skipping CI record")
        return
    import re as _re
    test_dir = "/trading-lab/tests"
    t0 = datetime.utcnow()
    try:
        result = subprocess.run(
            ["python", "-m", "pytest", test_dir, "--tb=line", "-q", "--no-header"],
            capture_output=True, text=True, timeout=180,
        )
        output = result.stdout + result.stderr
        # Parse "X passed, Y failed" from pytest summary line
        m_pass = _re.search(r"(\d+) passed", output)
        m_fail = _re.search(r"(\d+) failed", output)
        n_pass = int(m_pass.group(1)) if m_pass else 0
        n_fail = int(m_fail.group(1)) if m_fail else 0
        # Collect failed test names from output
        failed_lines = [l for l in output.splitlines() if "FAILED" in l]
        failed_names = "\n".join(failed_lines[:30])
        status = "pass" if n_fail == 0 and n_pass > 0 else "fail"
        if result.returncode not in (0, 1):
            status = "error"
    except Exception as e:
        n_pass, n_fail, failed_names, status = 0, 0, str(e)[:200], "error"
    duration = (datetime.utcnow() - t0).total_seconds()

    # Collect git info from the mounted repo
    try:
        branch = _git(["rev-parse", "--abbrev-ref", "HEAD"],
                      ).stdout.strip() or "unknown"
        sha    = _git(["rev-parse", "HEAD"]).stdout.strip()[:12] or "unknown"
        msg    = _git(["log", "-1", "--format=%s"]).stdout.strip()[:120] or ""
        author = _git(["log", "-1", "--format=%an"]).stdout.strip()[:60] or ""
        cts    = _git(["log", "-1", "--format=%ci"]).stdout.strip()[:19] or "1970-01-01 00:00:00"
    except Exception:
        branch, sha, msg, author, cts = "unknown", "unknown", "", "", "1970-01-01 00:00:00"

    try:
        ch = _ch_client()
        run_at = datetime.utcnow()
        row = [branch, sha, msg, author, cts,
               n_pass, n_fail, n_pass + n_fail,
               duration, failed_names, status, run_at]
        col_names = [
            "branch", "commit_sha", "commit_msg", "commit_author", "commit_ts",
            "tests_passed", "tests_failed", "tests_total", "duration_s",
            "failed_tests", "status", "run_at",
        ]
        # ci_results: latest per branch (ReplacingMergeTree keyed by branch)
        ch.insert(
            "system_meta.ci_results",
            [row + [int(t0.timestamp())]],
            column_names=col_names + ["version"],
        )
        # deploy_log: append-only history — dashboard reads last 7 for propagation table
        ch.insert("system_meta.deploy_log", [row], column_names=col_names)
        log.info(
            "TEST-RUN: branch=%s sha=%s %s/%s tests %s (%.1fs)",
            branch, sha, n_pass, n_pass + n_fail, status.upper(), duration,
        )
    except Exception as e:
        log.error("TEST-RUN: failed to write ci_results/deploy_log: %s", e)


def job_graduation_gate():
    """Daily strategy graduation check — updates analysis.strategy_graduation."""
    log.info("=== Graduation gate check triggered ===")
    _run("graduation_gate")


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


def _log_cleanup(task: str, status: str, items: int = 0, freed: int = 0, detail: str = ""):
    """Write one row to system_meta.cleanup_log."""
    if not _TRACKING_OK:
        return
    try:
        import json as _json
        ch = _ch_client()
        ch.command(
            "INSERT INTO system_meta.cleanup_log "
            "(task_name, status, items_freed, bytes_freed, detail) "
            "VALUES ({t:String},{s:String},{i:Int64},{f:Int64},{d:String})",
            parameters={"t": task, "s": status, "i": items, "f": freed, "d": detail},
        )
    except Exception as e:
        log.warning("cleanup_log write failed for %s: %s", task, e)


def job_cleanup():
    """Weekly maintenance: MinIO snapshots, workspace logs, Docker assessment, CH system tables."""
    log.info("=== Weekly cleanup triggered ===")
    try:
        _cleanup_minio_intraday()
    except Exception as e:
        log.error("MinIO cleanup failed: %s", e)
        _log_cleanup("minio_intraday", "error", detail=str(e))
    try:
        _cleanup_logs()
    except Exception as e:
        log.error("Log cleanup failed: %s", e)
        _log_cleanup("workspace_logs", "error", detail=str(e))
    try:
        _assess_docker_resources()
    except Exception as e:
        log.error("Docker assessment failed: %s", e)
        _log_cleanup("docker_assessment", "error", detail=str(e))
    try:
        _assess_clickhouse_system_tables()
    except Exception as e:
        log.error("ClickHouse system table assessment failed: %s", e)
        _log_cleanup("clickhouse_system_tables", "error", detail=str(e))


def _cleanup_minio_intraday():
    """Delete MinIO option_chain/intraday/ objects older than 30 days."""
    minio_host = os.getenv("MINIO_HOST", "minio:9000")
    minio_user = os.getenv("MINIO_USER", "admin")
    minio_pass = os.getenv("MINIO_PASSWORD", "")
    if not minio_pass:
        log.warning("MINIO_PASSWORD not set — skipping MinIO cleanup")
        _log_cleanup("minio_intraday", "skipped", detail="MINIO_PASSWORD not set")
        return

    try:
        from minio import Minio
        mc = Minio(minio_host, access_key=minio_user,
                   secret_key=minio_pass, secure=False)
        cutoff = datetime.utcnow() - timedelta(days=30)
        bucket = "trading-data"
        if not mc.bucket_exists(bucket):
            _log_cleanup("minio_intraday", "skipped", detail="bucket not found")
            return
        objects = mc.list_objects(bucket, prefix="option_chain/intraday/", recursive=True)
        deleted = 0
        bytes_freed = 0
        for obj in objects:
            if obj.last_modified and obj.last_modified.replace(tzinfo=None) < cutoff:
                bytes_freed += obj.size or 0
                mc.remove_object(bucket, obj.object_name)
                deleted += 1
        log.info("MinIO cleanup: deleted %d intraday objects older than 30 days", deleted)
        _log_cleanup("minio_intraday", "ok", items=deleted, freed=bytes_freed,
                     detail=f"removed {deleted} objects >30d old")
    except ImportError:
        log.warning("minio package not available — skipping MinIO cleanup")
        _log_cleanup("minio_intraday", "skipped", detail="minio package not installed")


def _cleanup_logs():
    """Truncate workspace log files older than 7 days to keep disk usage bounded."""
    import glob as _glob
    log_dir = os.getenv("LOG_DIR", "/trading-lab/workspace/logs")
    cutoff = datetime.utcnow() - timedelta(days=7)
    truncated = 0
    freed = 0
    for path in _glob.glob(f"{log_dir}/*.log"):
        try:
            mtime = datetime.utcfromtimestamp(os.path.getmtime(path))
            size_b = os.path.getsize(path)
            size_mb = size_b / (1024 * 1024)
            if mtime < cutoff or size_mb > 50:
                open(path, "w").close()  # truncate
                log.info("Truncated log: %s (was %.1f MB)", path, size_mb)
                truncated += 1
                freed += size_b
        except Exception as e:
            log.warning("Could not truncate %s: %s", path, e)
    _log_cleanup("workspace_logs", "ok", items=truncated, freed=freed,
                 detail=f"truncated {truncated} log files")


def _assess_docker_resources():
    """Assess Docker disk usage (images, containers, volumes, build cache) and log summary."""
    import json as _json

    def _docker(*args, timeout=15):
        r = subprocess.run(["docker"] + list(args), capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()

    # Dangling images
    dangling_ids = [l for l in _docker("images", "-f", "dangling=true", "-q").splitlines() if l]
    n_dangling = len(dangling_ids)

    # All images
    all_ids = [l for l in _docker("images", "-q").splitlines() if l]
    n_images = len(all_ids)

    # Stopped containers (exited / created / dead)
    stopped_ids = [l for l in _docker(
        "ps", "-a", "-f", "status=exited", "-f", "status=created", "-f", "status=dead", "-q"
    ).splitlines() if l]
    n_stopped = len(stopped_ids)

    # All containers
    all_ct = [l for l in _docker("ps", "-a", "-q").splitlines() if l]
    n_containers = len(all_ct)

    # Volumes (all, then dangling)
    all_vols   = [l for l in _docker("volume", "ls", "-q").splitlines() if l]
    dang_vols  = [l for l in _docker("volume", "ls", "-f", "dangling=true", "-q").splitlines() if l]
    n_vols     = len(all_vols)
    n_dang_vol = len(dang_vols)

    # docker system df text → parse sizes
    df_txt = _docker("system", "df")
    # Approximate reclaimable bytes from text: look for lines with sizes
    reclaimable_gb = 0.0
    for line in df_txt.splitlines():
        parts = line.split()
        if len(parts) >= 5:
            reclaim_field = parts[-1].strip("(%)")
            try:
                val_str = parts[-2] if "GB" in parts[-1] or "MB" in parts[-1] else parts[-1]
                if "GB" in val_str:
                    reclaimable_gb += float(val_str.replace("GB", ""))
                elif "MB" in val_str:
                    reclaimable_gb += float(val_str.replace("MB", "")) / 1024
            except ValueError:
                pass

    detail = _json.dumps({
        "images_total": n_images,
        "images_dangling": n_dangling,
        "containers_total": n_containers,
        "containers_stopped": n_stopped,
        "volumes_total": n_vols,
        "volumes_dangling": n_dang_vol,
        "reclaimable_gb_approx": round(reclaimable_gb, 2),
        "system_df": df_txt[:1500],
    })
    reclaimable_bytes = int(reclaimable_gb * 1024**3)
    log.info("Docker assessment: %d images (%d dangling), %d containers (%d stopped), "
             "%d volumes (%d dangling)", n_images, n_dangling, n_containers, n_stopped,
             n_vols, n_dang_vol)
    _log_cleanup("docker_assessment", "ok",
                 items=n_dangling + n_stopped + n_dang_vol,
                 freed=reclaimable_bytes, detail=detail)


def _assess_clickhouse_system_tables():
    """Check ClickHouse system table sizes and log the result."""
    import json as _json
    if not _TRACKING_OK:
        return
    try:
        ch = _ch_client()
        rows = ch.query(
            "SELECT table, sum(bytes_on_disk) AS bytes "
            "FROM system.parts WHERE database='system' GROUP BY table ORDER BY bytes DESC"
        ).result_rows
        tbl_sizes = {r[0]: r[1] for r in rows}
        total_bytes = sum(tbl_sizes.values())
        detail = _json.dumps({
            "tables": {k: f"{v/1e6:.1f}MB" for k, v in tbl_sizes.items()},
            "total_mb": round(total_bytes / 1e6, 1),
        })
        log.info("ClickHouse system tables: %.1f MB total", total_bytes / 1e6)
        _log_cleanup("clickhouse_system_tables", "ok",
                     freed=0, detail=detail)
    except Exception as e:
        _log_cleanup("clickhouse_system_tables", "error", detail=str(e))


def _run_background(service: str, *args: str):
    """Start a long-running intraday service in a daemon thread.

    _run() calls subprocess.run() (blocking). Intraday services run for 6+ hours,
    which would freeze the scheduler loop if called directly. Running in a daemon
    thread keeps schedule.run_pending() firing every 30 s.
    """
    t = threading.Thread(target=_run, args=(service,) + args, daemon=True, name=service)
    t.start()
    log.info("Started %s in background thread %s", service, t.name)


def job_option_chain_intraday():
    log.info("=== Option chain intraday scraper triggered ===")
    _run_background("option_chain_intraday")


def job_intraday_monitor():
    log.info("=== Intraday straddle monitor triggered ===")
    _run_background("intraday_monitor")


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
    log.info("  Confidence scorer   : Mon–Fri 13:00 UTC (18:30 IST) --score-only (daily refresh)")
    log.info("  Graduation gate     : Mon–Fri 13:05 UTC (18:35 IST) strategy stage check")
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

    # Daily confidence scoring (score-only): after EOD pipeline at 13:00 UTC (18:30 IST)
    # Uses existing model; ensures intraday_monitor has fresh scores each morning
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("13:00").do(job_confidence_scorer_daily)

    # Graduation gate: after daily scorer at 13:05 UTC (18:35 IST)
    # Updates analysis.strategy_graduation so dashboard shows current stage
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        getattr(schedule.every(), day).at("13:05").do(job_graduation_gate)

    schedule.every().sunday.at("00:00").do(job_events_pipeline)
    schedule.every().sunday.at("00:30").do(job_weekly)
    schedule.every().sunday.at("01:00").do(job_gap_analyzer)
    schedule.every().sunday.at("02:00").do(job_option_backtest)
    schedule.every().sunday.at("03:00").do(job_mf_pipeline)
    schedule.every().sunday.at("04:30").do(job_fundamental_pipeline)
    schedule.every().sunday.at("05:00").do(job_lot_size_pipeline)
    schedule.every().sunday.at("05:30").do(job_strategy_backtester)
    schedule.every().sunday.at("07:00").do(job_confidence_scorer)   # 90 min after backtester
    schedule.every().sunday.at("07:30").do(job_graduation_gate)     # after weekly retrain
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
