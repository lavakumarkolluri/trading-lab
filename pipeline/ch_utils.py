"""
ch_utils.py
───────────
Shared client factories for ClickHouse and MinIO.
Import instead of copy-pasting connection boilerplate in every pipeline.

Usage:
    from ch_utils import ch_client, minio_client, GIT_SHA
    ch  = ch_client()
    mc  = minio_client()
"""

import os
import logging
from datetime import datetime, timezone
import clickhouse_connect
from minio import Minio

CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

MINIO_HOST     = os.getenv("MINIO_HOST", "minio:9000")
MINIO_USER     = os.getenv("MINIO_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "")

GIT_SHA = os.getenv("GIT_SHA", "unknown")


def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )


def minio_client() -> Minio:
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=False)


def write_alert_log(ch, source: str, level: str, message: str) -> None:
    """Write an alert entry to system_meta.alert_log. Silent on failure."""
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        ch.insert(
            "system_meta.alert_log",
            [[now, source, level, message[:500]]],
            column_names=["alert_time", "source", "level", "message"],
        )
    except Exception as e:
        logging.getLogger(__name__).warning("alert_log write failed: %s", e)
