
#!/usr/bin/env python3
"""
ClickHouse Migration Runner
Usage:
  python migrate.py --run
  python migrate.py --status
  python migrate.py --check-drift
"""

import os
import sys
import glob
import hashlib
import argparse
import json
from datetime import datetime
import clickhouse_connect

# ── Config ─────────────────────────────────────────────
CH_HOST        = os.getenv("CH_HOST", "localhost")
CH_PORT        = int(os.getenv("CH_PORT", "8123"))
MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")

# ── Colours ────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"

def ok(msg):   print(f"{GREEN}✅ {msg}{RESET}")
def err(msg):  print(f"{RED}❌ {msg}{RESET}")
def warn(msg): print(f"{YELLOW}⚠️  {msg}{RESET}")
def info(msg): print(f"{BLUE}ℹ️  {msg}{RESET}")


# ── Connection ─────────────────────────────────────────
def get_client():
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=os.getenv("CH_USER", "default"),
            password=os.getenv("CH_PASSWORD", "")
        )
        client.command("SELECT 1")
        return client
    except Exception as e:
        err(f"Cannot connect to ClickHouse: {e}")
        sys.exit(1)

# ── Checksum ───────────────────────────────────────────
def get_checksum(filepath):
    with open(filepath, "r") as f:
        return hashlib.md5(f.read().encode()).hexdigest()


# ── Setup Tracking Tables ──────────────────────────────
def setup_tracking(client):
    client.command("""
        CREATE DATABASE IF NOT EXISTS system_migrations
    """)
    client.command("""
        CREATE TABLE IF NOT EXISTS system_migrations.history
        (
            migration_id    String,
            filename        String,
            executed_at     DateTime DEFAULT now(),
            checksum        String,
            status          String,
            duration_ms     UInt32 DEFAULT 0,
            error_message   String DEFAULT ''
        )
        ENGINE = MergeTree()
        ORDER BY executed_at
    """)
    client.command("""
        CREATE TABLE IF NOT EXISTS system_migrations.drift_log
        (
            id              UUID DEFAULT generateUUIDv4(),
            checked_at      DateTime DEFAULT now(),
            drift_detected  UInt8,
            drift_count     UInt32,
            drift_details   String,
            resolved        UInt8 DEFAULT 0
        )
        ENGINE = MergeTree()
        ORDER BY checked_at
    """)


# ── Get Migration Files ────────────────────────────────
def get_migration_files():
    pattern = os.path.join(MIGRATIONS_DIR, "*.sql")
    files   = sorted(glob.glob(pattern))
    if not files:
        warn(f"No migration files found in {MIGRATIONS_DIR}")
    return files


# ── Get Already Run Migrations ────────────────────────
def get_run_migrations(client):
    result = client.query("""
        SELECT migration_id, checksum, status, executed_at
        FROM system_migrations.history
        ORDER BY executed_at
    """)
    run = {}
    for row in result.result_rows:
        run[row[0]] = {
            "checksum":    row[1],
            "status":      row[2],
            "executed_at": row[3]
        }
    return run


# ── Record Migration ───────────────────────────────────
def record_migration(client, migration_id, filename,
                     checksum, status, duration_ms, error=""):
    safe_error = error[:500].replace("'", "''")
    client.command(f"""
        INSERT INTO system_migrations.history
        (migration_id, filename, checksum, status, duration_ms, error_message)
        VALUES (
            '{migration_id}',
            '{filename}',
            '{checksum}',
            '{status}',
            {duration_ms},
            '{safe_error}'
        )
    """)


# ══════════════════════════════════════════════════════
# MODE 1 — RUN
# ══════════════════════════════════════════════════════
def run_migrations(client):
    print(f"\n{BLUE}{'═'*55}")
    print("  MIGRATION RUNNER")
    print(f"{'═'*55}{RESET}\n")

    files       = get_migration_files()
    already_run = get_run_migrations(client)
    pending     = []
    errors      = []

    # ── Validate existing migrations ──────────────────
    for filepath in files:
        filename     = os.path.basename(filepath)
        migration_id = filename.replace(".sql", "")
        checksum     = get_checksum(filepath)

        if migration_id in already_run:
            stored = already_run[migration_id]
            if stored["checksum"] != checksum:
                err(f"MODIFIED: {filename}")
                err(f"  File was changed after running!")
                err(f"  Create a new migration instead.")
                errors.append(filename)
            else:
                ok(f"Already run: {filename}")
        else:
            pending.append((migration_id, filename, filepath, checksum))

    if errors:
        err(f"\n{len(errors)} modified migration(s) detected.")
        err("Fix these before running new migrations.")
        sys.exit(1)

    if not pending:
        ok("\nAll migrations already run. Database is up to date.")
        return

    print(f"\n{YELLOW}Pending: {len(pending)} migration(s){RESET}\n")

    # ── Run pending migrations ─────────────────────────
    success = 0
    for migration_id, filename, filepath, checksum in pending:
        info(f"Running: {filename}")
        start = datetime.now()

        try:
            with open(filepath, "r") as f:
                sql = f.read().strip()

            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for statement in statements:
                client.command(statement)

            duration = int((datetime.now() - start).total_seconds() * 1000)
            record_migration(client, migration_id, filename,
                             checksum, "success", duration)
            ok(f"  Done in {duration}ms")
            success += 1

        except Exception as e:
            duration = int((datetime.now() - start).total_seconds() * 1000)
            record_migration(client, migration_id, filename,
                             checksum, "failed", duration, str(e))
            err(f"  FAILED: {e}")
            err(f"  Stopping. Fix this migration before continuing.")
            sys.exit(1)

    print(f"\n{GREEN}{'═'*55}")
    print(f"  Complete: {success} migration(s) run successfully")
    print(f"{'═'*55}{RESET}\n")


# ══════════════════════════════════════════════════════
# MODE 2 — STATUS
# ══════════════════════════════════════════════════════
def show_status(client):
    print(f"\n{BLUE}{'═'*55}")
    print("  MIGRATION STATUS")
    print(f"{'═'*55}{RESET}\n")

    files       = get_migration_files()
    already_run = get_run_migrations(client)

    for filepath in files:
        filename     = os.path.basename(filepath)
        migration_id = filename.replace(".sql", "")
        checksum     = get_checksum(filepath)

        if migration_id in already_run:
            stored = already_run[migration_id]
            if stored["checksum"] != checksum:
                warn(f"MODIFIED  {filename}  ← checksum mismatch!")
            else:
                ok(f"RUN       {filename}  ({stored['executed_at']})")
        else:
            info(f"PENDING   {filename}")

    total   = len(files)
    run     = len(already_run)
    pending = total - run
    print(f"\nTotal: {total}  |  Run: {run}  |  Pending: {pending}\n")


# ══════════════════════════════════════════════════════
# MODE 3 — DRIFT DETECTION
# ══════════════════════════════════════════════════════
def check_drift(client):
    print(f"\n{BLUE}{'═'*55}")
    print("  DRIFT DETECTION")
    print(f"{'═'*55}{RESET}\n")

    # Actual tables in ClickHouse
    result = client.query("""
        SELECT database, name
        FROM system.tables
        WHERE database NOT IN (
            'system', 'information_schema',
            'INFORMATION_SCHEMA', 'system_migrations'
        )
        ORDER BY database, name
    """)

    actual_tables = set()
    for row in result.result_rows:
        actual_tables.add(f"{row[0]}.{row[1]}")

    # Expected tables from migration files
    expected_tables = set()
    files = get_migration_files()
    for filepath in files:
        with open(filepath, "r") as f:
            content = f.read().upper()
        if "CREATE TABLE" in content:
            for line in content.split("\n"):
                if "CREATE TABLE" in line and "IF NOT EXISTS" in line:
                    parts = line.replace(
                        "CREATE TABLE IF NOT EXISTS", ""
                    ).strip().split()
                    if parts:
                        table = parts[0].strip().lower()
                        if "." in table:
                            expected_tables.add(table)

    # Compare
    extra   = actual_tables - expected_tables
    missing = expected_tables - actual_tables
    drifts  = []

    if extra:
        for table in sorted(extra):
            warn(f"Extra in DB (not in migrations):    {table}")
            drifts.append(f"extra:{table}")

    if missing:
        for table in sorted(missing):
            warn(f"Missing in DB (in migrations only): {table}")
            drifts.append(f"missing:{table}")

    # Record drift check
    drift_count    = len(drifts)
    drift_detected = 1 if drifts else 0
    details        = json.dumps(drifts).replace("'", "''")

    client.command(f"""
        INSERT INTO system_migrations.drift_log
        (drift_detected, drift_count, drift_details)
        VALUES ({drift_detected}, {drift_count}, '{details}')
    """)

    if not drifts:
        ok("No drift detected. Database matches migrations perfectly.")
    else:
        err(f"\n{drift_count} drift(s) detected.")
        warn("Review and resolve before next migration run.")

    print()


# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="ClickHouse Migration Runner"
    )
    parser.add_argument(
        "--run",         action="store_true",
        help="Run pending migrations"
    )
    parser.add_argument(
        "--status",      action="store_true",
        help="Show migration status"
    )
    parser.add_argument(
        "--check-drift", action="store_true",
        help="Detect schema drift"
    )

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(0)

    info(f"Connecting to ClickHouse at {CH_HOST}:{CH_PORT}")
    client = get_client()
    ok("Connected\n")

    setup_tracking(client)

    if args.run:
        run_migrations(client)
    elif args.status:
        show_status(client)
    elif args.check_drift:
        check_drift(client)


if __name__ == "__main__":
    main()