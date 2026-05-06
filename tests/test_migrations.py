"""
Validates ClickHouse migration files.
Catches: missing files, non-monotone numbering, duplicate numbers, unparseable SQL.
"""
import os
import re
import glob
import sqlparse
import pytest

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "clickhouse", "migrations")


@pytest.fixture(scope="module")
def migration_files():
    pattern = os.path.join(MIGRATIONS_DIR, "*.sql")
    files = sorted(glob.glob(pattern))
    assert files, "No migration files found"
    return files


def test_migration_files_exist(migration_files):
    assert len(migration_files) >= 50, "Expected at least 50 migration files"


def test_migration_numbering_is_monotone(migration_files):
    """Numbers must be strictly increasing with no gaps."""
    numbers = []
    for f in migration_files:
        m = re.match(r"(\d+)_", os.path.basename(f))
        assert m, f"Migration filename doesn't start with number: {f}"
        numbers.append(int(m.group(1)))
    for i in range(len(numbers) - 1):
        assert numbers[i+1] == numbers[i] + 1, (
            f"Gap in migration numbering: {numbers[i]} → {numbers[i+1]}"
        )


def test_no_duplicate_migration_numbers(migration_files):
    numbers = [re.match(r"(\d+)", os.path.basename(f)).group(1) for f in migration_files]
    assert len(numbers) == len(set(numbers)), "Duplicate migration numbers found"


def test_migrations_are_valid_sql(migration_files):
    """Each migration must parse as valid SQL (no truncated/malformed files)."""
    for f in migration_files:
        with open(f) as fh:
            content = fh.read().strip()
        assert content, f"Empty migration file: {f}"
        parsed = sqlparse.parse(content)
        assert parsed, f"SQL parse returned nothing for: {f}"
