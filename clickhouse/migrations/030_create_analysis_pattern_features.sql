-- Migration : 030
-- Description: Feature vectors computed the day BEFORE each detected event
-- Created    : 2026-04-04
--
-- Populated by: pattern_feature_extractor.py
-- Key design:
--   • All features are computed on (event_date - 1) to avoid lookahead bias
--   • feature_version allows rolling schema upgrades without full recompute
--   • sma_*_cross: +1 = closed above that MA today after being below yesterday
--                  -1 = crossed below, 0 = no cross
--   • bb_position: 0.0 = at lower Bollinger Band, 1.0 = at upper band
--   • volume_trend: 'expanding' | 'contracting' | 'flat'
--   • fii_net_3d pulled from market.fii_dii if available, else 0

CREATE TABLE IF NOT EXISTS analysis.pattern_features
(
    event_date      Date,
    symbol          String,
    market          String,

    -- Momentum / trend features (day before event)
    rsi_14          Float64 DEFAULT 0,
    rsi_5           Float64 DEFAULT 0,
    sma_20_cross    Int8 DEFAULT 0,         -- +1 crossed above, -1 crossed below, 0 no cross
    sma_50_cross    Int8 DEFAULT 0,
    ema_20_cross    Int8 DEFAULT 0,
    price_vs_sma20  Float64 DEFAULT 0,      -- % above/below SMA-20
    price_vs_sma50  Float64 DEFAULT 0,
    price_vs_sma200 Float64 DEFAULT 0,

    -- Volume features
    volume_z5       Float64 DEFAULT 0,      -- z-score vs 5-day rolling avg
    volume_z20      Float64 DEFAULT 0,      -- z-score vs 20-day rolling avg
    volume_trend    String DEFAULT '',       -- 'expanding' | 'contracting' | 'flat'

    -- Volatility features
    atr_14          Float64 DEFAULT 0,
    atr_pct         Float64 DEFAULT 0,      -- atr_14 / close * 100
    bb_position     Float64 DEFAULT 0,      -- 0 = lower band, 1 = upper band

    -- Market context features
    vix_level       Float64 DEFAULT 0,
    vix_regime      String DEFAULT '',      -- 'low' | 'normal' | 'elevated' | 'extreme'
    nifty_trend_5d  Float64 DEFAULT 0,      -- Nifty 5-day return %
    fii_net_3d      Float64 DEFAULT 0,      -- FII net buy/sell 3-day rolling sum (crores)

    -- Symbol-level return context
    return_5d       Float64 DEFAULT 0,      -- symbol 5-day return % before event
    return_20d      Float64 DEFAULT 0,
    drawdown_pct    Float64 DEFAULT 0,      -- % below 52-week high

    -- Housekeeping
    feature_version String DEFAULT 'v1',
    computed_at     DateTime DEFAULT now(),
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, event_date)
