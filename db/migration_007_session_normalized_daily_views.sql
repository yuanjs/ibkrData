-- IBKR Data System - Migration 007: Session-normalized futures daily bars
--
-- Raw IBKR futures daily bars can be split by exchange roll hour.  For
-- example, SPI can have Saturday bars that belong to the following Monday
-- session, and holiday fragments around Easter belong to the next open
-- session.  This migration keeps futures_daily_bars immutable and creates
-- normalized daily views for backtests.

CREATE TABLE IF NOT EXISTS futures_daily_session_date_overrides (
    symbol       TEXT NOT NULL,
    source_date  DATE NOT NULL,
    target_date  DATE NOT NULL,
    reason       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, source_date)
);

-- SNFE/SPI Easter closures in the current backtest range.  Rows only affect
-- dates that exist in futures_daily_bars; missing raw dates are harmless.
INSERT INTO futures_daily_session_date_overrides
    (symbol, source_date, target_date, reason)
VALUES
    ('SPI', DATE '2024-03-29', DATE '2024-04-02', 'easter_holiday'),
    ('SPI', DATE '2024-03-30', DATE '2024-04-02', 'easter_holiday'),
    ('SPI', DATE '2024-03-31', DATE '2024-04-02', 'easter_holiday'),
    ('SPI', DATE '2024-04-01', DATE '2024-04-02', 'easter_holiday'),
    ('SPI', DATE '2025-04-18', DATE '2025-04-22', 'easter_holiday'),
    ('SPI', DATE '2025-04-19', DATE '2025-04-22', 'easter_holiday'),
    ('SPI', DATE '2025-04-20', DATE '2025-04-22', 'easter_holiday'),
    ('SPI', DATE '2025-04-21', DATE '2025-04-22', 'easter_holiday'),
    ('SPI', DATE '2026-04-03', DATE '2026-04-07', 'easter_holiday'),
    ('SPI', DATE '2026-04-04', DATE '2026-04-07', 'easter_holiday'),
    ('SPI', DATE '2026-04-05', DATE '2026-04-07', 'easter_holiday'),
    ('SPI', DATE '2026-04-06', DATE '2026-04-07', 'easter_holiday')
ON CONFLICT (symbol, source_date) DO UPDATE SET
    target_date = EXCLUDED.target_date,
    reason = EXCLUDED.reason;

DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_session_ratio_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_session_back_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_volume_safety_session_raw;
DROP VIEW IF EXISTS continuous_futures_daily_session_ratio_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_session_back_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_session_raw;
DROP VIEW IF EXISTS futures_daily_bars_session_normalized;

CREATE VIEW futures_daily_bars_session_normalized AS
WITH mapped AS (
    SELECT
        b.*,
        COALESCE(
            o.target_date,
            CASE
                WHEN EXTRACT(ISODOW FROM b.time::date) IN (6, 7)
                THEN next_session.next_date
                ELSE b.time::date
            END,
            b.time::date
        ) AS session_date
    FROM futures_daily_bars b
    LEFT JOIN futures_daily_session_date_overrides o
      ON o.symbol = b.symbol
     AND o.source_date = b.time::date
    LEFT JOIN LATERAL (
        SELECT nb.time::date AS next_date
        FROM futures_daily_bars nb
        WHERE nb.symbol = b.symbol
          AND nb.con_id = b.con_id
          AND nb.time::date > b.time::date
          AND EXTRACT(ISODOW FROM nb.time::date) BETWEEN 1 AND 5
        ORDER BY nb.time
        LIMIT 1
    ) next_session ON TRUE
)
SELECT
    m.session_date::timestamp AT TIME ZONE 'UTC' AS time,
    to_char(m.session_date, 'YYYYMMDD') AS date_str,
    m.symbol,
    m.con_id,
    m.local_symbol,
    m.trading_class,
    m.contract_month,
    m.last_trade_date,
    m.exchange,
    m.currency,
    m.multiplier,
    (array_agg(m.open ORDER BY m.time))[1] AS open,
    MAX(m.high) AS high,
    MIN(m.low) AS low,
    (array_agg(m.close ORDER BY m.time DESC))[1] AS close,
    SUM(COALESCE(m.volume, 0)) AS volume,
    SUM(COALESCE(m.bar_count, 0)) AS bar_count,
    m.session_date
FROM mapped m
GROUP BY
    m.symbol, m.con_id, m.local_symbol, m.trading_class, m.contract_month,
    m.last_trade_date, m.exchange, m.currency, m.multiplier, m.session_date;

CREATE VIEW continuous_futures_daily_session_raw AS
WITH ordered_rolls AS (
    SELECT
        symbol,
        from_con_id,
        to_con_id,
        roll_time,
        price_gap,
        ratio,
        row_number() OVER (
            PARTITION BY symbol ORDER BY roll_time
        ) AS rn,
        lead(roll_time) OVER (
            PARTITION BY symbol ORDER BY roll_time
        ) AS next_roll_time
    FROM futures_roll_events
),
segments AS (
    SELECT
        symbol,
        from_con_id AS con_id,
        NULL::date AS segment_start,
        roll_time::date AS segment_end,
        NULL::timestamptz AS roll_time
    FROM ordered_rolls
    WHERE rn = 1

    UNION ALL

    SELECT
        symbol,
        to_con_id AS con_id,
        roll_time::date AS segment_start,
        next_roll_time::date AS segment_end,
        roll_time
    FROM ordered_rolls
)
SELECT
    b.time,
    b.date_str,
    b.session_date,
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
    s.segment_start,
    s.segment_end,
    (s.roll_time IS NOT NULL AND b.session_date = s.roll_time::date)
        AS is_roll_date
FROM segments s
JOIN futures_daily_bars_session_normalized b
  ON b.symbol = s.symbol
 AND b.con_id = s.con_id
 AND (s.segment_start IS NULL OR b.session_date >= s.segment_start)
 AND (s.segment_end IS NULL OR b.session_date < s.segment_end);

CREATE VIEW continuous_futures_daily_session_back_adjusted AS
SELECT
    r.time,
    r.date_str,
    r.session_date,
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
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    adj.cumulative_gap AS adjustment_value,
    'back_adjusted_difference_session_normalized'::text AS adjustment_method
FROM continuous_futures_daily_session_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap
    FROM futures_roll_events e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.session_date
) adj;

CREATE VIEW continuous_futures_daily_session_ratio_adjusted AS
SELECT
    r.time,
    r.date_str,
    r.session_date,
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
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    adj.cumulative_ratio AS adjustment_ratio,
    'ratio_adjusted_session_normalized'::text AS adjustment_method
FROM continuous_futures_daily_session_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(
        EXP(SUM(LN(e.ratio::double precision)))::numeric,
        1::numeric
    ) AS cumulative_ratio
    FROM futures_roll_events e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.session_date
) adj;

CREATE VIEW continuous_futures_daily_volume_safety_session_raw AS
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
    b.session_date,
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
    (s.roll_time IS NOT NULL AND b.session_date = s.roll_time::date)
        AS is_roll_date
FROM segments s
JOIN futures_daily_bars_session_normalized b
  ON b.symbol = s.symbol
 AND b.con_id = s.con_id
 AND (s.segment_start IS NULL OR b.session_date >= s.segment_start)
 AND (s.segment_end IS NULL OR b.session_date < s.segment_end);

CREATE VIEW continuous_futures_daily_volume_safety_session_back_adjusted AS
SELECT
    r.time,
    r.date_str,
    r.session_date,
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
    'back_adjusted_difference_volume_safety_session_normalized'::text
        AS adjustment_method
FROM continuous_futures_daily_volume_safety_session_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap
    FROM futures_roll_events_volume_safety e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.session_date
) adj;

CREATE VIEW continuous_futures_daily_volume_safety_session_ratio_adjusted AS
SELECT
    r.time,
    r.date_str,
    r.session_date,
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
    'ratio_adjusted_volume_safety_session_normalized'::text
        AS adjustment_method
FROM continuous_futures_daily_volume_safety_session_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(
        EXP(SUM(LN(e.ratio::double precision)))::numeric,
        1::numeric
    ) AS cumulative_ratio
    FROM futures_roll_events_volume_safety e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.session_date
) adj;
