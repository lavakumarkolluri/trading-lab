# Dashboard — app.py Conventions

## `query()` Helper — No `params=` Kwarg

```python
@st.cache_data(ttl=180)
def query(sql: str) -> pd.DataFrame: ...
```

This helper takes **only a SQL string** — it does NOT accept a `params=` keyword argument. Passing `params=` silently ignores it (no parameterization happens), creating a SQL injection risk.

- **Trusted internal values** (e.g., symbol from a DB result): f-string is fine.
- **Any user-controlled input** (selectbox choice, date picker): use ClickHouse `{param:Type}` syntax embedded in the SQL string, or use `get_ch().query_df(sql, parameters={...})` directly.

`query_live(sql)` works the same way but has a 30-second cache TTL (used for live positions and mark prices).

---

## Page Routing

Pages are driven by a Streamlit sidebar radio widget. Adding a page requires updating **both**:
1. The radio options list (the display names are exact string keys)
2. The conditional render block that checks the selected page

Do not assume the page name from the sidebar matches a function name — they are string-matched.

---

## Reuse These Helpers — Do Not Inline

| Helper | Purpose |
|---|---|
| `fmt_inr(val)` | Format INR values with ₹ symbol and comma separators |
| `traffic_light(val, thresholds)` | Coloured emoji indicator (🟢🟡🔴) |
| `_age_str(ts)` | Human-readable age from a UTC timestamp |
| `_to_ist_str(ts)` | Format UTC timestamp as IST string |
| `missing_features(features_json)` | List which critical features are zero/missing |

---

## Caching

- `@st.cache_data(ttl=180)` — 3-minute cache on all standard DB queries
- `@st.cache_resource` — session-scoped ClickHouse client (`get_ch()`)
- Manual refresh: `st.cache_data.clear()` — used by the refresh button

---

## Expiry Date Format

Expiry dates must be formatted as `%d-%b-%Y` (e.g., `10-Jun-2026`) throughout the dashboard — in display, in SQL `WHERE` clauses, and in string comparisons. Using `%Y-%m-%d` or any other format causes mismatches with the stored values.

---

## SQL Safety Rule

All user-controlled inputs — symbol selectors (`st.selectbox`), date pickers (`st.date_input`), any widget value passed into SQL — **must use ClickHouse parameterized syntax**, not f-string interpolation:

```python
# Safe
sql = "SELECT * FROM trades WHERE symbol = {sym:String}"
get_ch().query_df(sql, parameters={"sym": selected_symbol})

# Unsafe — do not do this with widget values
sql = f"SELECT * FROM trades WHERE symbol = '{selected_symbol}'"
```

---

## Connection

The dashboard uses `clickhouse_connect.get_client(...)` directly (not `ch_utils`) because it runs in its own container with its own Streamlit stack. This is intentional — do not replace it with a `ch_utils` import.
