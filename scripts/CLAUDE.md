# Scripts — Reference

## `lint_boilerplate.py`

Run by CI on every push to `stage`. Scans `pipeline/*.py` for inline ClickHouse/MinIO/logging boilerplate and fails CI (exit 1) if found.

```bash
python scripts/lint_boilerplate.py             # CI mode — exits 1 on violations
python scripts/lint_boilerplate.py --warn-only # print violations but exit 0
```

**To add a new forbidden pattern:** append to `_INLINE_CH`, `_INLINE_MINIO`, or `_INLINE_LOGGER` in the script. Each entry is a substring matched against stripped lines. Include a clear error message string in the violations list.

Exempt from linting: `ch_utils.py`, `logging_utils.py`, `pipeline_utils.py`.
Research files (not enforced): `walk_forward_validation.py`, `momentum_factor_test.py`, `breakout_backtest.py`.

---

## `check_env_consistency.py`

Cross-validates environment variables referenced in Python files against `docker-compose.yml` declarations. Exits 0 with warnings (does not block CI, but reported in workflow output).

```bash
python scripts/check_env_consistency.py
```

---

## `migrate_to_shared_utils.py`

**ONE-SHOT — do not re-run.** This script was used once to refactor inline ClickHouse/MinIO/logging boilerplate across the pipeline into the shared utility imports. The codebase is already migrated. Kept for reference only.

---

## `new-migration.sh`

Scaffolds the next numbered ClickHouse migration file. **Always use this instead of creating files manually** — it assigns the correct sequential number and opens `$EDITOR`.

```bash
bash scripts/new-migration.sh add_expiry_column
# Creates: clickhouse/migrations/089_add_expiry_column.sql
# Opens in $EDITOR (falls back to nano)
```

After editing: follow the migration conventions in `clickhouse/CLAUDE.md`.
