# Tests — Conventions & Patterns

## All Tests Are Offline

No live ClickHouse, no live MinIO. Every test must work without any running services. Use `unittest.mock.MagicMock` for all DB clients.

```python
from unittest.mock import MagicMock

ch = MagicMock()
ch.query.return_value.result_rows = [("NIFTY", 65), ("BANKNIFTY", 30)]
```

If a test accidentally connects to ClickHouse, it will fail in CI (no CH server available). If it silently passes in CI, it means the mock isn't being hit — verify the patch path.

---

## Import Path — `from module import ...` Not `from pipeline.module import ...`

`conftest.py` inserts `pipeline/` into `sys.path`. Import pipeline modules directly:

```python
from confidence_scorer import build_dataset   # correct
from pipeline.confidence_scorer import ...    # wrong — will fail
```

---

## Test-First Rule

Every prod breakage must have a **failing test written first**, then the fix. Tests serve as regression guards. A fix without a test will regress silently.

---

## Standard Mock Patterns

```python
# ClickHouse query returning rows
ch.query.return_value.result_rows = [("NIFTY", 65)]

# ClickHouse query_df returning a DataFrame
import pandas as pd
ch.query_df.return_value = pd.DataFrame({"symbol": ["NIFTY"], "lot_size": [65]})

# ClickHouse command (DDL/DML, no return value needed)
ch.command.return_value = None

# MinIO client
mc = MagicMock()
mc.get_object.return_value.__enter__ = lambda s: s
mc.get_object.return_value.__exit__ = MagicMock(return_value=False)
mc.get_object.return_value.read.return_value = b"..."
```

---

## File Naming

Test files must match the pipeline module exactly: `test_<module_name>.py`. This is how test discovery maps failures back to modules.

---

## Timeouts

Add `@pytest.mark.timeout(30)` to any test that has a loop, retry, or sleep. CI enforces a 30-second per-test limit.

---

## Running Tests Locally

```bash
python -m pytest tests/ -q --tb=short --timeout=30        # full suite
python -m pytest tests/test_confidence_scorer.py -v       # single file
python -m pytest tests/ -k "lot_size" -v                  # by keyword
```

Current suite: **422 tests** as of 2026-05-15. All must pass offline before pushing to `stage`.
