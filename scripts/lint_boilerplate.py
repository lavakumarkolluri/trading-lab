#!/usr/bin/env python3
"""
lint_boilerplate.py
───────────────────
CI guard: fail if any pipeline script defines its own ClickHouse/MinIO
connection or IST logging formatter instead of importing from shared utils.

Exit 0 = clean. Exit 1 = violations found.

Usage:
    python scripts/lint_boilerplate.py
    python scripts/lint_boilerplate.py --warn-only   # print but don't fail CI
"""

import ast
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PIPELINE_DIR = ROOT / "pipeline"

# Patterns that signal inline boilerplate (checked via AST where possible,
# string-search otherwise for speed).
_INLINE_CH = [
    "clickhouse_connect.get_client(",   # direct CH client construction
]
_INLINE_MINIO = [
    "Minio(",                           # direct Minio() construction
]
_INLINE_LOGGER = [
    "class _ISTFormatter",              # copy-pasted IST formatter
    "logging.basicConfig(",             # raw basicConfig instead of get_logger
]

# Files that are the shared utilities themselves — skip them.
_EXEMPT = {"ch_utils.py", "logging_utils.py", "pipeline_utils.py"}

# Files that are known research-only (not enforced).
_RESEARCH = {"walk_forward_validation.py", "momentum_factor_test.py", "breakout_backtest.py"}


def check_file(path: Path) -> list[tuple[int, str]]:
    """Return list of (line_number, violation_message) for a script."""
    violations = []
    lines = path.read_text().splitlines()
    # Track whether the file imports from shared utils
    imports_ch_utils    = any("from ch_utils" in l or "import ch_utils" in l for l in lines)
    imports_log_utils   = any("from logging_utils" in l or "import logging_utils" in l for l in lines)

    for i, line in enumerate(lines, start=1):
        stripped = line.strip()
        for pattern in _INLINE_CH:
            if pattern in stripped and not imports_ch_utils:
                violations.append((i, f"inline CH client — use `from ch_utils import ch_client`"))
        for pattern in _INLINE_MINIO:
            if pattern in stripped and not imports_ch_utils:
                violations.append((i, f"inline Minio() — use `from ch_utils import minio_client`"))
        for pattern in _INLINE_LOGGER:
            if pattern in stripped and not imports_log_utils:
                violations.append((i, f"inline logging boilerplate — use `from logging_utils import get_logger`"))

    return violations


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--warn-only", action="store_true",
                        help="Print violations but exit 0 (non-blocking CI)")
    args = parser.parse_args()

    all_violations: dict[str, list] = {}
    for path in sorted(PIPELINE_DIR.glob("*.py")):
        if path.name in _EXEMPT or path.name in _RESEARCH:
            continue
        v = check_file(path)
        if v:
            all_violations[path.name] = v

    if not all_violations:
        print("lint_boilerplate: OK — no inline connection/logging boilerplate found")
        sys.exit(0)

    print(f"lint_boilerplate: {sum(len(v) for v in all_violations.values())} violation(s) in "
          f"{len(all_violations)} file(s):\n")
    for fname, violations in all_violations.items():
        for lineno, msg in violations:
            print(f"  {fname}:{lineno}  {msg}")

    sys.exit(0 if args.warn_only else 1)


if __name__ == "__main__":
    main()
