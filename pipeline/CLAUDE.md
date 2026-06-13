# Pipeline — Conventions & Shared Utilities

## Shared Utilities — Always Import, Never Inline

The CI linter (`scripts/lint_boilerplate.py`) **fails on any of these patterns** in pipeline scripts:

| Forbidden pattern | Use instead |
|---|---|
| `clickhouse_connect.get_client(...)` | `from ch_utils import ch_client` |
| `Minio(...)` (without importing ch_utils) | `from ch_utils import minio_client` |
| `class _ISTFormatter` | `from logging_utils import get_logger` |
| `logging.basicConfig(...)` (without importing logging_utils) | `from logging_utils import get_logger` |

Exempt from linting: `ch_utils.py`, `logging_utils.py`, `pipeline_utils.py` (they are the shared utils).
Research files (not enforced): `walk_forward_validation.py`, `momentum_factor_test.py`, `breakout_backtest.py`.

---

## Shared Utility Reference

### `ch_utils.py`
```python
from ch_utils import ch_client, minio_client, write_alert_log, GIT_SHA

ch = ch_client()       # ClickHouse client
mc = minio_client()    # MinIO client
write_alert_log(ch, source="my_module", level="ERROR", message="...")
```

### `logging_utils.py`
```python
from logging_utils import get_logger
log = get_logger(__name__)   # IST-stamped logger
```

### `pipeline_utils.py`
```python
from pipeline_utils import PipelineRun

with PipelineRun("service_name", ch=ch) as run:
    rows = do_work()
    run.rows_written = rows   # recorded in system_meta.pipeline_runs
```

### `fo_utils.py`
```python
from fo_utils import get_lot_size
lot = get_lot_size(ch, "NIFTY")   # with fallback cache
```

### `symbols.py`
Symbol/market constants — import from here, do not hardcode.

---

## Standard `main()` Structure

All 67 pipeline modules follow this pattern:

```python
def main():
    args = parse_args()                            # 1. argparse
    ch = ch_client()                               # 2. ClickHouse client
    with PipelineRun("service_name", ch=ch) as run:  # 3. run tracking
        data = load(ch)                            # 4. load/validate
        result = compute(data)                     # 5. core logic
        run.rows_written = write(ch, result)       # 6. write + track
```

---

## ClickHouse Query Conventions

- **Always `FINAL`** on ReplacingMergeTree reads: `SELECT ... FROM table FINAL`
- **Incremental filter**: `toDate(timestamp) >= '{from_date}'` for pipelines that process new rows only
- **Snapshot dedup**: `argMax(ltp, timestamp) AS last_ltp GROUP BY strike` to keep latest intraday value
- **ReplacingMergeTree updates**: re-insert with `version = int(datetime.now().timestamp())`. Never use `ALTER TABLE UPDATE` on the `version` column — ClickHouse raises `CANNOT_UPDATE_COLUMN`.
- **`ORDER BY` must include `symbol`** for any per-symbol time-series table (NIFTY and FINNIFTY share the same Tuesday expiry — without `symbol` in ORDER BY, one silently overwrites the other).

---

## Feature Column Alignment

`confidence_scorer.FEATURE_COLS` and the backtest feature list must stay in sync. Adding a feature to one without updating the other causes silent score degradation (model trained on one set, scored on another).

The 6 critical features (zero = UNRELIABLE score): `vix, iv_rank, pcr_oi, straddle_pct, atr_percentile, rsi14`.

---

## Timezone

IST = UTC + 5:30. ClickHouse returns UTC-naive datetimes. Always add `timedelta(hours=5, minutes=30)` before any comparison against IST time. Using `date.today()` for EOD date comparisons is correct; `datetime.now()` causes 5.5h drift.

---

## Common Pitfalls

- **Duplicate DataFrame columns → ClickHouse crash**: deduplicate column lists with `list(dict.fromkeys(col_list))` before selecting. A duplicate column returns a DataFrame instead of a Series; the ClickHouse driver crashes with `AttributeError: dtype`.
- **Duplicate strike index**: after `df.set_index("strike")`, call `.groupby(level=0).last()` to keep latest. Multiple snapshots on the same date create duplicate strike keys; `.get(atm, 0)` then returns a Series, not a scalar, and `float()` crashes.
- **load_lot_sizes() without ORDER BY**: always use `argMax(lot_size, effective_from) GROUP BY symbol` — plain `SELECT ... FINAL` gives random results when multiple effective dates exist.
