-- IBKR Data System - Migration 006: Volume+safety roll calendar views
--
-- This adds a second, auditable continuous-futures rule set without replacing
-- the existing futures_roll_events based views.

CREATE TABLE IF NOT EXISTS futures_roll_events_volume_safety (
    symbol                TEXT NOT NULL,
    from_con_id           BIGINT NOT NULL,
    to_con_id             BIGINT NOT NULL,
    from_contract_month   TEXT,
    to_contract_month     TEXT,
    from_local_symbol     TEXT,
    to_local_symbol       TEXT,
    roll_time             TIMESTAMPTZ NOT NULL,
    roll_rule             TEXT NOT NULL,
    price_gap             NUMERIC(16,6) NOT NULL,
    ratio                 NUMERIC(18,10) NOT NULL,
    old_price             NUMERIC(16,6),
    new_price             NUMERIC(16,6),
    old_volume            BIGINT,
    new_volume            BIGINT,
    old_bar_count         BIGINT,
    new_bar_count         BIGINT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, from_con_id, to_con_id, roll_time)
);

CREATE INDEX IF NOT EXISTS idx_futures_roll_events_volume_safety_symbol_time
    ON futures_roll_events_volume_safety (symbol, roll_time);

DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_ratio_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_back_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_raw;
DROP VIEW IF EXISTS continuous_futures_volume_safety_ratio_adjusted;
DROP VIEW IF EXISTS continuous_futures_volume_safety_back_adjusted;
DROP VIEW IF EXISTS continuous_futures_volume_safety_raw;

CREATE VIEW continuous_futures_volume_safety_raw AS
WITH ordered_rolls AS (
    SELECT
        symbol,
        from_con_id,
        to_con_id,
        roll_time,
        roll_rule,
        row_number() OVER (
            PARTITION BY symbol ORDER BY roll_time
        ) AS rn,
        lead(roll_time) OVER (
            PARTITION BY symbol ORDER BY roll_time
        ) AS next_roll_time
    FROM futures_roll_events_volume_safety
),
segments AS (
    SELECT
        symbol,
        from_con_id AS con_id,
        NULL::timestamptz AS segment_start,
        roll_time AS segment_end,
        NULL::timestamptz AS roll_time,
        NULL::text AS roll_rule
    FROM ordered_rolls
    WHERE rn = 1

    UNION ALL

    SELECT
        symbol,
        to_con_id AS con_id,
        roll_time AS segment_start,
        next_roll_time AS segment_end,
        roll_time,
        roll_rule
    FROM ordered_rolls
)
SELECT
    b.time,
    b.symbol,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume,
    b.bar_count,
    b.con_id AS source_con_id,
    b.contract_month,
    b.local_symbol,
    b.trading_class,
    b.exchange,
    b.currency,
    b.multiplier,
    s.roll_time,
    s.roll_rule,
    s.segment_start,
    s.segment_end,
    (s.roll_time IS NOT NULL AND b.time::date = s.roll_time::date)
        AS is_roll_date,
    row_number() OVER (
        PARTITION BY b.symbol, s.con_id, s.segment_start
        ORDER BY b.time
    ) - 1 AS bars_since_segment_start
FROM segments s
JOIN futures_minute_bars b
  ON b.symbol = s.symbol
 AND b.con_id = s.con_id
 AND (s.segment_start IS NULL OR b.time >= s.segment_start)
 AND (s.segment_end IS NULL OR b.time < s.segment_end);

CREATE VIEW continuous_futures_volume_safety_back_adjusted AS
SELECT
    r.time,
    r.symbol,
    r.open + adj.cumulative_gap AS open,
    r.high + adj.cumulative_gap AS high,
    r.low + adj.cumulative_gap AS low,
    r.close + adj.cumulative_gap AS close,
    r.volume,
    r.bar_count,
    r.source_con_id,
    r.contract_month,
    r.local_symbol,
    r.trading_class,
    r.exchange,
    r.currency,
    r.multiplier,
    r.roll_time,
    r.roll_rule,
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    r.bars_since_segment_start,
    adj.cumulative_gap AS adjustment_value,
    'back_adjusted_difference_volume_safety'::text AS adjustment_method
FROM continuous_futures_volume_safety_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap
    FROM futures_roll_events_volume_safety e
    WHERE e.symbol = r.symbol
      AND e.roll_time > r.time
) adj;

CREATE VIEW continuous_futures_volume_safety_ratio_adjusted AS
SELECT
    r.time,
    r.symbol,
    r.open * adj.cumulative_ratio AS open,
    r.high * adj.cumulative_ratio AS high,
    r.low * adj.cumulative_ratio AS low,
    r.close * adj.cumulative_ratio AS close,
    r.volume,
    r.bar_count,
    r.source_con_id,
    r.contract_month,
    r.local_symbol,
    r.trading_class,
    r.exchange,
    r.currency,
    r.multiplier,
    r.roll_time,
    r.roll_rule,
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    r.bars_since_segment_start,
    adj.cumulative_ratio AS adjustment_ratio,
    'ratio_adjusted_volume_safety'::text AS adjustment_method
FROM continuous_futures_volume_safety_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(
        EXP(SUM(LN(e.ratio::double precision)))::numeric,
        1::numeric
    ) AS cumulative_ratio
    FROM futures_roll_events_volume_safety e
    WHERE e.symbol = r.symbol
      AND e.roll_time > r.time
) adj;

CREATE VIEW continuous_futures_daily_volume_safety_raw AS
WITH ordered_rolls AS (
    SELECT
        symbol,
        from_con_id,
        to_con_id,
        roll_time,
        roll_rule,
        row_number() OVER (
            PARTITION BY symbol ORDER BY roll_time
        ) AS rn,
        lead(roll_time) OVER (
            PARTITION BY symbol ORDER BY roll_time
        ) AS next_roll_time
    FROM futures_roll_events_volume_safety
),
segments AS (
    SELECT
        symbol,
        from_con_id AS con_id,
        NULL::date AS segment_start,
        roll_time::date AS segment_end,
        NULL::timestamptz AS roll_time,
        NULL::text AS roll_rule
    FROM ordered_rolls
    WHERE rn = 1

    UNION ALL

    SELECT
        symbol,
        to_con_id AS con_id,
        roll_time::date AS segment_start,
        next_roll_time::date AS segment_end,
        roll_time,
        roll_rule
    FROM ordered_rolls
)
SELECT
    b.time,
    b.date_str,
    b.symbol,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume,
    b.bar_count,
    b.con_id AS source_con_id,
    b.contract_month,
    b.local_symbol,
    b.trading_class,
    b.exchange,
    b.currency,
    b.multiplier,
    s.roll_time,
    s.roll_rule,
    s.segment_start,
    s.segment_end,
    (s.roll_time IS NOT NULL AND b.time::date = s.roll_time::date)
        AS is_roll_date
FROM segments s
JOIN futures_daily_bars b
  ON b.symbol = s.symbol
 AND b.con_id = s.con_id
 AND (s.segment_start IS NULL OR b.time::date >= s.segment_start)
 AND (s.segment_end IS NULL OR b.time::date < s.segment_end);

CREATE VIEW continuous_futures_daily_volume_safety_back_adjusted AS
SELECT
    r.time,
    r.date_str,
    r.symbol,
    r.open + adj.cumulative_gap AS open,
    r.high + adj.cumulative_gap AS high,
    r.low + adj.cumulative_gap AS low,
    r.close + adj.cumulative_gap AS close,
    r.volume,
    r.bar_count,
    r.source_con_id,
    r.contract_month,
    r.local_symbol,
    r.trading_class,
    r.exchange,
    r.currency,
    r.multiplier,
    r.roll_time,
    r.roll_rule,
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    adj.cumulative_gap AS adjustment_value,
    'back_adjusted_difference_volume_safety'::text AS adjustment_method
FROM continuous_futures_daily_volume_safety_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap
    FROM futures_roll_events_volume_safety e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.time::date
) adj;

CREATE VIEW continuous_futures_daily_volume_safety_ratio_adjusted AS
SELECT
    r.time,
    r.date_str,
    r.symbol,
    r.open * adj.cumulative_ratio AS open,
    r.high * adj.cumulative_ratio AS high,
    r.low * adj.cumulative_ratio AS low,
    r.close * adj.cumulative_ratio AS close,
    r.volume,
    r.bar_count,
    r.source_con_id,
    r.contract_month,
    r.local_symbol,
    r.trading_class,
    r.exchange,
    r.currency,
    r.multiplier,
    r.roll_time,
    r.roll_rule,
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    adj.cumulative_ratio AS adjustment_ratio,
    'ratio_adjusted_volume_safety'::text AS adjustment_method
FROM continuous_futures_daily_volume_safety_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(
        EXP(SUM(LN(e.ratio::double precision)))::numeric,
        1::numeric
    ) AS cumulative_ratio
    FROM futures_roll_events_volume_safety e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.time::date
) adj;
