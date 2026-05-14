#!/usr/bin/env python3
"""
check_env_consistency.py
─────────────────────────
Cross-checks environment variables used in pipeline scripts against those
declared in docker-compose.yml service environment blocks.

Reports:
  - Vars used in code but absent from ALL compose services (likely missing)
  - Vars declared in compose but never read by any script (likely stale)

Always exits 0 (warning-only — some vars are optional or set at runtime).

Usage:
    python scripts/check_env_consistency.py
"""

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("check_env_consistency: pyyaml not installed — skipping (pip install pyyaml)")
    sys.exit(0)

ROOT         = Path(__file__).resolve().parent.parent
PIPELINE_DIR = ROOT / "pipeline"
COMPOSE_FILE = ROOT / "docker-compose.yml"

# Vars provided by the runtime environment (not in compose) — don't flag these.
_RUNTIME_ONLY = {
    "VIRTUAL_ENV", "PATH", "HOME", "USER", "SHELL", "PWD",
    "PYTHONPATH", "PYTHONDONTWRITEBYTECODE",
    "EDITOR", "TERM", "LANG", "LC_ALL",
}

# Patterns to extract os.getenv / os.environ usage from Python source.
_GETENV_RE  = re.compile(r'os\.getenv\(\s*["\']([A-Z_][A-Z0-9_]*)["\']')
_ENVIRON_RE = re.compile(r'os\.environ(?:\.get)?\(\s*["\']([A-Z_][A-Z0-9_]*)["\']')
_ENVIRON_KV = re.compile(r'os\.environ\[\s*["\']([A-Z_][A-Z0-9_]*)["\']')


def collect_code_vars() -> set[str]:
    used = set()
    for path in PIPELINE_DIR.glob("*.py"):
        src = path.read_text()
        used |= set(_GETENV_RE.findall(src))
        used |= set(_ENVIRON_RE.findall(src))
        used |= set(_ENVIRON_KV.findall(src))
    return used - _RUNTIME_ONLY


def collect_compose_vars() -> set[str]:
    declared = set()
    data = yaml.safe_load(COMPOSE_FILE.read_text())
    for svc in data.get("services", {}).values():
        env = svc.get("environment", {})
        if isinstance(env, list):
            for item in env:
                key = item.split("=")[0].split(":")[0].strip()
                declared.add(key)
        elif isinstance(env, dict):
            declared |= set(env.keys())
    return declared


def main():
    code_vars    = collect_code_vars()
    compose_vars = collect_compose_vars()

    missing_in_compose = code_vars - compose_vars - _RUNTIME_ONLY
    stale_in_compose   = compose_vars - code_vars - _RUNTIME_ONLY

    ok = True
    if missing_in_compose:
        ok = False
        print("ENV vars used in code but absent from docker-compose.yml:")
        for v in sorted(missing_in_compose):
            print(f"  MISSING: {v}")

    if stale_in_compose:
        print("\nENV vars in docker-compose.yml but never read by any pipeline script:")
        for v in sorted(stale_in_compose):
            print(f"  UNUSED:  {v}")

    if ok and not stale_in_compose:
        print("check_env_consistency: OK — code and compose env vars are consistent")

    sys.exit(0)  # always warning-only


if __name__ == "__main__":
    main()
