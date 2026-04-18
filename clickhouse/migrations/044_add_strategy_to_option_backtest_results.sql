-- Migration : 044
-- Table     : analysis.option_backtest_results
-- Purpose   : Add strategy column (buy/sell) so both strategies can coexist.
--             Recreate table with (signal_date, bias, strategy) ORDER BY.

CREATE TABLE IF NOT EXISTS analysis.option_backtest_results_new
(
    signal_date     Date,
    expiry          Date,
    nifty_spot      Float64,
    vix             Float64,
    vix_rank        Float64,
    pcr             Float64,
    bias            String,
    strategy        String DEFAULT 'buy',   -- 'buy' | 'sell'
    atm_strike      Float64,
    entry_type      String,
    entry_price     Float64,
    exit_date       Date,
    exit_price      Float64,
    nifty_return_3d Float64,
    pnl             Float64,
    pnl_pct         Float64,
    hit             UInt8,
    version         UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (signal_date, bias, strategy);

INSERT INTO analysis.option_backtest_results_new
    SELECT signal_date, expiry, nifty_spot, vix, vix_rank, pcr, bias,
           'buy' AS strategy,
           atm_strike, entry_type, entry_price, exit_date, exit_price,
           nifty_return_3d, pnl, pnl_pct, hit, version
    FROM analysis.option_backtest_results;

DROP TABLE analysis.option_backtest_results;

RENAME TABLE analysis.option_backtest_results_new TO analysis.option_backtest_results;
