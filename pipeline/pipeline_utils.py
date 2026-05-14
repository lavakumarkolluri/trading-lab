"""
pipeline_utils.py
─────────────────
Lightweight run-tracking context manager for system_meta.pipeline_runs.

Usage:
    from pipeline_utils import PipelineRun

    with PipelineRun("options_eod_summary") as run:
        rows = do_work()
        run.rows_written = rows
"""

import os
from datetime import datetime

GIT_SHA = "unknown"
try:
    with open("/app/version.txt") as f:
        GIT_SHA = f.read().strip() or "unknown"
except FileNotFoundError:
    GIT_SHA = os.getenv("GIT_SHA", "unknown")


class PipelineRun:
    def __init__(self, service: str, ch=None):
        self.service = service
        self.ch = ch
        self.run_id = None
        self.rows_written = 0
        self._started_at = None

    def _get_ch(self):
        if self.ch:
            return self.ch
        from ch_utils import ch_client
        return ch_client()

    def __enter__(self):
        self._started_at = datetime.utcnow()
        ch = self._get_ch()
        self.run_id = ch.query(
            """
            INSERT INTO system_meta.pipeline_runs
                (service, started_at, status, git_sha)
            VALUES ({service:String}, {started_at:DateTime}, 'running', {git_sha:String})
            """,
            parameters={
                "service":    self.service,
                "started_at": self._started_at,
                "git_sha":    GIT_SHA,
            }
        )
        # Re-query to get the generated run_id
        result = ch.query(
            """
            SELECT run_id FROM system_meta.pipeline_runs FINAL
            WHERE service = {service:String}
              AND started_at = {started_at:DateTime}
            ORDER BY version DESC LIMIT 1
            """,
            parameters={
                "service":    self.service,
                "started_at": self._started_at,
            }
        )
        if result.result_rows:
            self.run_id = str(result.result_rows[0][0])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "failed" if exc_type else "success"
        error_msg = str(exc_val) if exc_val else ""
        finished_at = datetime.utcnow()
        try:
            ch = self._get_ch()
            ch.command(
                """
                INSERT INTO system_meta.pipeline_runs
                    (service, started_at, finished_at, status, rows_written, git_sha, error_msg)
                VALUES (
                    {service:String},
                    {started_at:DateTime},
                    {finished_at:DateTime},
                    {status:String},
                    {rows_written:UInt64},
                    {git_sha:String},
                    {error_msg:String}
                )
                """,
                parameters={
                    "service":     self.service,
                    "started_at":  self._started_at,
                    "finished_at": finished_at,
                    "status":      status,
                    "rows_written": self.rows_written,
                    "git_sha":     GIT_SHA,
                    "error_msg":   error_msg,
                },
            )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("pipeline_utils: failed to write run record: %s", e)
        return False  # never suppress exceptions


def record_run(ch, service: str, status: str, started_at, error_msg: str = ""):
    """Lightweight one-shot run record for scripts that don't use PipelineRun context."""
    from datetime import datetime
    try:
        ch.command(
            """INSERT INTO system_meta.pipeline_runs
               (service, started_at, finished_at, status, git_sha, error_msg)
               VALUES ({svc:String},{start:DateTime},{end:DateTime},{st:String},{sha:String},{err:String})""",
            parameters={"svc": service, "start": started_at,
                        "end": datetime.utcnow(), "st": status,
                        "sha": GIT_SHA, "err": error_msg},
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("pipeline_runs write failed: %s", e)
