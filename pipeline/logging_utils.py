"""
logging_utils.py
────────────────
IST-stamped logger factory.  Replaces the copy-pasted _ISTFormatter
boilerplate in every pipeline script.

Usage:
    from logging_utils import get_logger
    log = get_logger(__name__)
"""

import logging
from datetime import datetime, timedelta, timezone

_IST = timezone(timedelta(hours=5, minutes=30))


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, tz=_IST).strftime("%Y-%m-%d %H:%M:%S IST")


def get_logger(name: str, fmt: str = "%(asctime)s [%(levelname)s] %(message)s") -> logging.Logger:
    h = logging.StreamHandler()
    h.setFormatter(ISTFormatter(fmt))
    logging.basicConfig(level=logging.INFO, handlers=[h])
    return logging.getLogger(name)
