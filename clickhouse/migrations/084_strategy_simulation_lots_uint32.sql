-- Migration 084: widen lots column from UInt8 to UInt32
-- UInt8 max is 255; compounding simulation grows capital ~30x (₹5L → ₹16M+),
-- making risk-sized lots routinely exceed 255.
ALTER TABLE analysis.strategy_simulation
    MODIFY COLUMN lots UInt32;
