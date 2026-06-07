-- IBKR Data System - Migration 005: Raw and continuous futures daily bars
--
-- Raw daily bars are stored per individual futures contract.  Continuous
-- daily views are derived from futures_roll_events and do not modify raw data.

CREATE TABLE IF NOT EXISTS futures_daily_bars (
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    date_str          TEXT NOT NULL,
    time              TIMESTAMPTZ,
    local_symbol      TEXT,
    trading_class     TEXT,
    contract_month    TEXT,
    last_trade_date   DATE,
    exchange          TEXT,
    currency          TEXT,
    multiplier        TEXT,
    open              NUMERIC(16,6),
    high              NUMERIC(16,6),
    low               NUMERIC(16,6),
    close             NUMERIC(16,6),
    volume            BIGINT,
    bar_count         INTEGER,
    source            TEXT NOT NULL DEFAULT 'IBKR',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, con_id, date_str)
);

CREATE INDEX IF NOT EXISTS idx_futures_daily_bars_symbol_time
    ON futures_daily_bars (symbol, time DESC);

CREATE INDEX IF NOT EXISTS idx_futures_daily_bars_contract
    ON futures_daily_bars (symbol, contract_month, con_id, date_str);

DROP VIEW IF EXISTS continuous_futures_daily_ratio_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_back_adjusted;
DROP VIEW IF EXISTS continuous_futures_daily_raw;

CREATE VIEW continuous_futures_daily_raw AS
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
    (s.roll_time IS NOT NULL AND b.time::date = s.roll_time::date)
        AS is_roll_date
FROM segments s
JOIN futures_daily_bars b
  ON b.symbol = s.symbol
 AND b.con_id = s.con_id
 AND (s.segment_start IS NULL OR b.time::date >= s.segment_start)
 AND (s.segment_end IS NULL OR b.time::date < s.segment_end);

CREATE VIEW continuous_futures_daily_back_adjusted AS
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
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    adj.cumulative_gap AS adjustment_value,
    'back_adjusted_difference'::text AS adjustment_method
FROM continuous_futures_daily_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap
    FROM futures_roll_events e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.time::date
) adj;

CREATE VIEW continuous_futures_daily_ratio_adjusted AS
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
    r.segment_start,
    r.segment_end,
    r.is_roll_date,
    adj.cumulative_ratio AS adjustment_ratio,
    'ratio_adjusted'::text AS adjustment_method
FROM continuous_futures_daily_raw r
CROSS JOIN LATERAL (
    SELECT COALESCE(
        EXP(SUM(LN(e.ratio::double precision)))::numeric,
        1::numeric
    ) AS cumulative_ratio
    FROM futures_roll_events e
    WHERE e.symbol = r.symbol
      AND e.roll_time::date > r.time::date
) adj;
