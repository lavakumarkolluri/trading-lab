#!/usr/bin/env python3
"""
One-shot migration: replace inline CH/MinIO/logging boilerplate in pipeline
scripts with imports from ch_utils and logging_utils.

Run from repo root:
    python scripts/migrate_to_shared_utils.py [--dry-run]
"""

import re
import sys
import argparse
from pathlib import Path

PIPELINE_DIR = Path("pipeline")

ALREADY_DONE = {
    "ch_utils.py", "logging_utils.py", "pipeline_utils.py",
    "scheduler.py", "intraday_monitor.py", "confidence_scorer.py",
    "compute_oi_features.py", "strategy_backtester.py", "mf_pipeline.py",
    "walk_forward_validation.py", "momentum_factor_test.py", "breakout_backtest.py",
}

# ── Regexes ───────────────────────────────────────────────────────────────────

# 1. CH config block (4 lines)
_CH_VARS = re.compile(
    r'CH_HOST\s*=\s*os\.getenv\([^\n]+\)\s*\n'
    r'\s*CH_PORT\s*=\s*int\([^\n]+\)\s*\n'
    r'\s*CH_USER\s*=\s*os\.getenv\([^\n]+\)\s*\n'
    r'\s*CH_PASS\s*=\s*os\.getenv\([^\n]+\)\s*\n',
    re.MULTILINE,
)

# 2. Inline get_ch / get_ch_client function (just wraps clickhouse_connect)
_GET_CH_FUNC = re.compile(
    r'\n?def (get_ch(?:_client)?)\(\):\s*\n'
    r'(?:    [^\n]+\n)*?'        # any number of indented lines
    r'    return clickhouse_connect\.get_client\([^\n]*\n'
    r'(?:        [^\n]+\n)*',    # continuation lines
    re.MULTILINE,
)

# 3. logging.basicConfig (single-line or multi-line)
_LOGGING_BASIC = re.compile(
    r'logging\.basicConfig\([^)]*\)\s*\n',
    re.DOTALL,
)

# 4. log = logging.getLogger(...)
_LOG_GETLOGGER = re.compile(r'log\s*=\s*logging\.getLogger\([^\n]+\)\s*\n')

# 5. import logging (standalone)
_IMPORT_LOGGING = re.compile(r'^import logging\s*\n', re.MULTILINE)

# 6. import clickhouse_connect
_IMPORT_CH = re.compile(r'^import clickhouse_connect\s*\n', re.MULTILINE)

# 7. MinIO config vars (any ordering, up to 4 lines)
_MINIO_VARS = re.compile(
    r'(?:(?:MINIO_HOST|MINIO_USER|MINIO_PASS(?:WORD)?|MINIO_BUCKET)\s*=\s*[^\n]+\n)+',
    re.MULTILINE,
)

# 8. from minio import Minio (when Minio only used for client creation)
_IMPORT_MINIO = re.compile(r'^from minio import Minio\s*\n', re.MULTILINE)

# 9. Inline Minio() construction inside a function
_GET_MC_FUNC = re.compile(
    r'\n?def (get_(?:mc|minio|minio_client))\(\)[^:]*:\s*\n'
    r'(?:    [^\n]+\n)*?'
    r'    return Minio\([^\n]*\n'
    r'(?:        [^\n]*\n)*',
    re.MULTILINE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _has_minio(src: str) -> bool:
    return "Minio(" in src or "minio_client" in src or "MINIO_HOST" in src

def _already_imports_ch(src: str) -> bool:
    return "from ch_utils import" in src

def _already_imports_log(src: str) -> bool:
    return "from logging_utils import" in src

def _get_ch_func_name(src: str) -> str | None:
    m = _GET_CH_FUNC.search(src)
    return m.group(1) if m else None

def _get_mc_func_name(src: str) -> str | None:
    m = _GET_MC_FUNC.search(src)
    return m.group(1) if m else None


def migrate(src: str, fname: str) -> str:
    changed = False

    # ── Step 1: remove CH config vars ────────────────────────────────────────
    if not _already_imports_ch(src):
        new = _CH_VARS.sub("", src)
        if new != src:
            src = new
            changed = True

    # ── Step 2: replace inline get_ch* function with alias import ────────────
    if not _already_imports_ch(src):
        ch_func = _get_ch_func_name(src)
        if ch_func:
            src = _GET_CH_FUNC.sub("", src)
            # Build the import line with the same alias used in the file
            alias = f" as {ch_func}" if ch_func != "ch_client" else ""
            ch_import = f"from ch_utils import ch_client{alias}"
            changed = True
        else:
            ch_import = "from ch_utils import ch_client"

        # Determine what to import from ch_utils
        needs_minio = _has_minio(src) and not "from ch_utils import" in src
        mc_func = _get_mc_func_name(src)
        if needs_minio and mc_func:
            mc_alias = f" as {mc_func}" if mc_func != "minio_client" else ""
            ch_import = ch_import.replace(
                "\n", ""
            ) + f", minio_client{mc_alias}"

        src = _IMPORT_CH.sub("", src)  # remove bare import clickhouse_connect
        src = src.replace("import clickhouse_connect\n", "")

        # Insert the ch_utils import after the last stdlib import block
        src = _insert_import(src, ch_import)
        changed = True

    # ── Step 3: remove inline MinIO client function ───────────────────────────
    new = _GET_MC_FUNC.sub("", src)
    if new != src:
        src = new
        changed = True

    # ── Step 4: remove MinIO config vars ─────────────────────────────────────
    # Only remove MINIO_BUCKET if it's the standard "trading-data" value
    # Keep custom MINIO_BUCKET assignments
    def _strip_minio_vars(m):
        block = m.group(0)
        # Keep MINIO_BUCKET if it's not the default "trading-data"
        lines = []
        for line in block.splitlines(keepends=True):
            if "MINIO_BUCKET" in line and '"trading-data"' not in line:
                lines.append(line)
            elif not any(v in line for v in ("MINIO_HOST", "MINIO_USER", "MINIO_PASS", "MINIO_BUCKET")):
                lines.append(line)
        return "".join(lines)

    new = _MINIO_VARS.sub(_strip_minio_vars, src)
    if new != src:
        src = new
        changed = True

    # ── Step 5: remove bare Minio import ─────────────────────────────────────
    new = _IMPORT_MINIO.sub("", src)
    if new != src:
        src = new
        changed = True

    # ── Step 6: replace logging.basicConfig + log = getLogger ────────────────
    if not _already_imports_log(src):
        new = _LOGGING_BASIC.sub("", src)
        if new != src:
            src = new
            changed = True
        new = _LOG_GETLOGGER.sub("", src)
        if new != src:
            src = new
            changed = True
        # Remove bare import logging if nothing else uses it
        if "logging." not in src or src.count("logging.") <= src.count("import logging"):
            new = _IMPORT_LOGGING.sub("", src)
            if new != src:
                src = new
                changed = True

        log_import = "from logging_utils import get_logger\nlog = get_logger(__name__)"
        src = _insert_import(src, log_import)
        changed = True

    return src


def _insert_import(src: str, import_line: str) -> str:
    """Insert import_line after the last 'from X import' or 'import X' line."""
    lines = src.splitlines(keepends=True)
    last_import_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("import ") or stripped.startswith("from "):
            last_import_idx = i
    if last_import_idx == -1:
        return import_line + "\n" + src
    # Avoid duplicates
    if import_line.split("\n")[0] in src:
        return src
    lines.insert(last_import_idx + 1, import_line + "\n")
    return "".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    targets = sorted(
        p for p in PIPELINE_DIR.glob("*.py")
        if p.name not in ALREADY_DONE
    )

    n_changed = 0
    for path in targets:
        original = path.read_text()
        migrated = migrate(original, path.name)
        if migrated != original:
            n_changed += 1
            if args.dry_run:
                print(f"[dry-run] would migrate: {path.name}")
            else:
                path.write_text(migrated)
                print(f"migrated: {path.name}")

    print(f"\n{'[dry-run] ' if args.dry_run else ''}{n_changed}/{len(targets)} files changed")


if __name__ == "__main__":
    main()
