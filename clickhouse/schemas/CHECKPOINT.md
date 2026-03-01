# ClickHouse Checkpoint

## Date: 2026-03-01

## Version: 26.2.1.1139

## Status: Clean installation

## Existing Databases (system only):
- system
- INFORMATION_SCHEMA
- information_schema
- default (empty)

## Custom Tables: NONE

## Plan — Databases to create:
- [ ] logs      (agent logs, run history, data quality)
- [ ] staging   (file registry)
- [ ] market    (raw market data)
- [ ] analysis  (processed results)
- [ ] trades    (opportunities, paper trades)
- [ ] learning  (linkedin posts, journal)

## Table Creation Order:
1. logs.agent_runs
2. logs.agent_logs
3. logs.data_quality
4. learning.journal
5. staging.file_registry
6. market.nifty_live
7. market.ohlcv_daily
8. market.options_chain
9. market.global_context
10. market.events
11. market.fii_dii
12. analysis.market_regime
13. analysis.strike_recommendations
14. trades.opportunities
15. trades.daily_summary
