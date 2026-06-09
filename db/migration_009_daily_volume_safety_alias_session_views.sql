-- IBKR Data System - Migration 009: Make default volume+safety daily views
-- session-normalized.
--
-- Migration 007/008 introduced explicit *_session_* views.  This migration
-- also moves the shorter volume+safety daily view names onto the same
-- exchange-calendar-normalized source to avoid accidental use of raw IBKR
-- roll-hour daily fragments in backtests.

DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_ratio_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_back_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_raw;

CREATE VIEW continuous_futures_daily_volume_safety_raw AS
SELECT
    time,
    date_str,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    bar_count,
    source_con_id,
    contract_month,
    local_symbol,
    trading_class,
    exchange,
    currency,
    multiplier,
    roll_time,
    roll_rule,
    segment_start,
    segment_end,
    is_roll_date
FROM continuous_futures_daily_volume_safety_session_raw;

CREATE VIEW continuous_futures_daily_volume_safety_back_adjusted AS
SELECT
    time,
    date_str,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    bar_count,
    source_con_id,
    contract_month,
    local_symbol,
    trading_class,
    exchange,
    currency,
    multiplier,
    roll_time,
    roll_rule,
    segment_start,
    segment_end,
    is_roll_date,
    adjustment_value,
    adjustment_method
FROM continuous_futures_daily_volume_safety_session_back_adjusted;

CREATE VIEW continuous_futures_daily_volume_safety_ratio_adjusted AS
SELECT
    time,
    date_str,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    bar_count,
    source_con_id,
    contract_month,
    local_symbol,
    trading_class,
    exchange,
    currency,
    multiplier,
    roll_time,
    roll_rule,
    segment_start,
    segment_end,
    is_roll_date,
    adjustment_ratio,
    adjustment_method
FROM continuous_futures_daily_volume_safety_session_ratio_adjusted;
