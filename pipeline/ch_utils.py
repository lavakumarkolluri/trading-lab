"""
ch_utils.py
───────────
Shared ClickHouse client factory.  Import instead of copy-pasting in every pipeline.

Usage:
    from ch_utils import ch_client
    ch = ch_client()
"""

import os
import clickhouse_connect

CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")


def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )
