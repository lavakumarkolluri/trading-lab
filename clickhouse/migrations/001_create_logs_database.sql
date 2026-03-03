-- ─────────────────────────────────────────────────────
-- Migration : 001
-- Description: Create logs database
-- This database holds all agent run logs,
-- debug logs and data quality checks
-- Created : 2026-03-01
-- ─────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS logs
```

---

## What This Does
```
CREATE DATABASE  → creates a new database
IF NOT EXISTS    → safe to run multiple times
                   if already exists, skips silently
logs             → name of the database
                   will hold all our logging tables