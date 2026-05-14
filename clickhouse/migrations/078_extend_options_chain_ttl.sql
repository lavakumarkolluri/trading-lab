-- Extend options_chain TTL from 90 days to 10 years.
-- 90-day TTL was fine for intraday use but the strategy backtester and
-- confidence_scorer need 7 years of historical chain features for model training.
-- At ~50 MiB compressed for 7 years of data, disk impact is negligible.
ALTER TABLE market.options_chain MODIFY TTL timestamp + toIntervalDay(3650);
