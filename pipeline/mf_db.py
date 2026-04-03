#!/usr/bin/env python3
"""
mf_db.py
────────
All ClickHouse query helpers for MF aggregation pipeline.
Each function is stateless — pass a client, get data back.
"""

import os
import logging
from datetime import timedelta

import pandas as pd
import clickhouse_connect

log = logging.getLogger(__name__)

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")

INSERT_CHUNK_SIZE = 50_000


def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS
    )


def fetch_all_scheme_codes(ch) -> list[int]:
    """All scheme_codes present in market.mf_nav."""
    result = ch.query(
        "SELECT DISTINCT scheme_code FROM market.mf_nav ORDER BY scheme_code"
    )
    return [row[0] for row in result.result_rows]


def fetch_last_enriched_date(ch, scheme_code: int):
    """max(date) from mf_nav_enriched for one scheme, or None."""
    result = ch.query(
        "SELECT max(date) FROM market.mf_nav_enriched "
        "WHERE scheme_code = {code:UInt32}",
        parameters={"code": scheme_code}
    )
    return result.result_rows[0][0]


def fetch_all_last_enriched_dates(ch) -> dict:
    """
    max(date) for ALL schemes in ONE query — eliminates N round-trips.
    Returns {scheme_code: last_date}
    """
    result = ch.query(
        "SELECT scheme_code, max(date) "
        "FROM market.mf_nav_enriched "
        "GROUP BY scheme_code"
    )
    return {row[0]: row[1] for row in result.result_rows}


def fetch_nav_for_scheme(ch, scheme_code: int,
                         from_date=None,
                         warmup_days: int = 3650) -> pd.DataFrame:
    """
    Fetch NAV rows for one scheme from market.mf_nav.

    warmup_days=3650 (10y) ensures return_10y can be computed
    even for incremental runs that only insert a few new rows.
    The caller trims before inserting — warmup rows are used only
    for rolling window computation.
    """
    if from_date is not None:
        warmup_start = from_date - timedelta(days=warmup_days)
        result = ch.query(
            "SELECT date, nav FROM market.mf_nav "
            "WHERE scheme_code = {code:UInt32} AND date >= {d:Date} "
            "ORDER BY date",
            parameters={"code": scheme_code, "d": warmup_start}
        )
    else:
        result = ch.query(
            "SELECT date, nav FROM market.mf_nav "
            "WHERE scheme_code = {code:UInt32} "
            "ORDER BY date",
            parameters={"code": scheme_code}
        )

    if not result.result_rows:
        return pd.DataFrame(columns=["date", "nav"])

    df = pd.DataFrame(result.result_rows, columns=["date", "nav"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["nav"]  = df["nav"].astype(float)
    return df


def fetch_nifty_nav(ch) -> pd.DataFrame:
    """
    Fetch NIFTYBEES.NS daily close from market.ohlcv_daily.
    Used as benchmark for beta computation.
    Returns DataFrame with columns [date, nifty_close].
    """
    result = ch.query(
        "SELECT date, close FROM market.ohlcv_daily FINAL "
        "WHERE symbol = 'NIFTYBEES.NS' AND market = 'indian' "
        "ORDER BY date"
    )
    if not result.result_rows:
        return pd.DataFrame(columns=["date", "nifty_close"])

    df = pd.DataFrame(result.result_rows, columns=["date", "nifty_close"])
    df["date"]         = pd.to_datetime(df["date"]).dt.date
    df["nifty_close"]  = df["nifty_close"].astype(float)
    return df


def insert_enriched(ch, df: pd.DataFrame):
    """
    Insert enriched DataFrame into market.mf_nav_enriched in chunks.
    No DELETE before insert — ReplacingMergeTree(version) deduplicates.
    Always use SELECT ... FINAL on reads for strict dedup.
    """
    for start in range(0, len(df), INSERT_CHUNK_SIZE):
        chunk = df.iloc[start:start + INSERT_CHUNK_SIZE]
        ch.insert_df("market.mf_nav_enriched", chunk)
