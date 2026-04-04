-- Migration : 029
-- Description: Detected market events (single-day moves >= threshold %)
-- Created    : 2026-04-04
--
-- Populated by: event_detector.py (daily, after OHLCV pipeline)
-- Key design:
--   • pct_change = (close - prev_close) / prev_close * 100
--   • direction  = 'UP' | 'DOWN'
--   • event_type = 'gap_up' | 'gap_down' | 'intraday_surge' | 'intraday_crash'
--   • processed  = 0 until pattern_feature_extractor has run on this row
--   • Always query with FINAL to avoid duplicate reads during background merge

CREATE TABLE IF NOT EXISTS analysis.detected_events
(
    date            Date,
    symbol          String,
    market          String,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          UInt64,
    prev_close      Float64,
    pct_change      Float64,        -- (close - prev_close) / prev_close * 100
    direction       String,         -- 'UP' | 'DOWN'
    event_type      String,         -- 'gap_up' | 'gap_down' | 'intraday_surge' | 'intraday_crash'
    threshold_pct   Float64,        -- configured threshold that triggered detection (e.g. 2.0)
    vix_on_day      Float64 DEFAULT 0,
    market_regime   String DEFAULT '',
    processed       UInt8 DEFAULT 0,    -- 1 after pattern_feature_extractor runs
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, date)
