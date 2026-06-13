# ClickHouse — Migrations & Config

## Migration File Format

Files live in `migrations/` and must follow this exact naming and header:

```
NNN_snake_case_description.sql
```

```sql
-- Migration : NNN
-- Description: One-line description
-- Created: YYYY-MM-DD

CREATE TABLE IF NOT EXISTS ...
```

- Number is **zero-padded 3-digit**, sequential with **no gaps**. Use `bash scripts/new-migration.sh <description>` to scaffold — never create files manually (risks numbering gaps).
- The header space before `:` is intentional — match it exactly or `migrate.py` may not parse correctly.

---

## Multi-Statement Rule

ClickHouse executes one DDL statement at a time. `migrate.py` splits on `;\n` (semicolon + newline). This means:

- **Each statement must end with `;` on its own line**, followed by a blank line before the next statement.
- **Never put `;` inside a comment** on a line that would be split — this caused a parse error in migration 083. Keep comment lines free of semicolons.

```sql
-- Good
CREATE TABLE IF NOT EXISTS foo (...) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS bar (...) ENGINE = MergeTree() ORDER BY id;

-- Bad — comment with ; causes split
-- Insert rows; update version field
CREATE TABLE foo ...
```

---

## Checksum Validation — Never Amend a Committed Migration

`migrate.py` MD5-checksums every file. If a committed migration is modified, the runner **halts with an error** on next run — it will not apply any further migrations until the checksum mismatch is resolved.

**Rule: never edit a committed migration file.** Create a new numbered fix migration instead (e.g., `083_fix_...sql`).

---

## Idempotency Patterns

- `CREATE TABLE IF NOT EXISTS` — always use this
- `CREATE DATABASE IF NOT EXISTS` — always use this
- `DROP TABLE IF EXISTS` — safe for rollback helpers
- `ALTER TABLE` — **not rollbackable**. If an ALTER may fail, wrap the change in a new migration that can be skipped on re-run, or guard with `IF EXISTS` clauses.

---

## `migrate.py` Modes

Run from inside the `migrate` container or via `docker compose run --rm migrate`:

```bash
python migrate.py --run           # apply all pending migrations
python migrate.py --status        # list applied / pending migrations
python migrate.py --check-drift   # compare actual CH schema vs migration CREATE statements
```

Drift detection (`--check-drift`) compares the live ClickHouse table list against what the migrations would have created. Any table created by manual DDL (running SQL directly in CH) will appear as drift. Always go through migrations — never run DDL directly on the CH instance.

---

## Config Files

- `config/trading-lab-user.xml` — bind-mounted as `trading-lab-user.xml` (NOT `default-user.xml` — see root CLAUDE.md ClickHouse v26 gotcha)
- `config/log-retention.xml` — TTL: 7 days for query/text/part logs, 3 days for trace/metric logs

---

## Migration Workflow Checklist

1. `bash scripts/new-migration.sh <description>` — creates next numbered file
2. Write SQL with `CREATE ... IF NOT EXISTS`; end each statement with `;\n`
3. No `;` inside comment lines
4. Test locally: `docker compose run --rm migrate python migrate.py --run`
5. Verify: `python migrate.py --status` shows it as applied
6. Commit — never amend after committing
