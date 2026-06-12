-- IBKR Data System - Migration 010: As-of futures roll events and views
--
-- These objects support walk-forward backtests.  They keep raw futures bars
-- immutable and only use roll events that would have been known as of the
-- requested date.

CREATE TABLE IF NOT EXISTS futures_roll_events_asof (
    id                      BIGSERIAL PRIMARY KEY,
    symbol                  TEXT NOT NULL,
    from_con_id             BIGINT NOT NULL,
    to_con_id               BIGINT NOT NULL,
    from_contract_month     TEXT,
    to_contract_month       TEXT,
    from_local_symbol       TEXT,
    to_local_symbol         TEXT,
    effective_roll_time     TIMESTAMPTZ NOT NULL,
    known_at                TIMESTAMPTZ NOT NULL,
    decision_session_date   DATE NOT NULL,
    price_session_date      DATE NOT NULL,
    roll_rule               TEXT NOT NULL,
    price_gap               NUMERIC(16,6) NOT NULL,
    ratio                   NUMERIC(18,10) NOT NULL,
    old_price               NUMERIC(16,6),
    new_price               NUMERIC(16,6),
    old_volume              BIGINT,
    new_volume              BIGINT,
    old_bar_count           BIGINT,
    new_bar_count           BIGINT,
    source                  TEXT NOT NULL DEFAULT 'volume_safety_asof',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, from_con_id, to_con_id, effective_roll_time)
);

CREATE INDEX IF NOT EXISTS idx_futures_roll_events_asof_symbol_known
    ON futures_roll_events_asof (symbol, known_at);

CREATE INDEX IF NOT EXISTS idx_futures_roll_events_asof_symbol_effective
    ON futures_roll_events_asof (symbol, effective_roll_time);

DROP FUNCTION IF EXISTS continuous_futures_daily_asof(TEXT, DATE, DATE, TEXT);

CREATE FUNCTION continuous_futures_daily_asof(
    p_symbol TEXT,
    p_start_date DATE,
    p_as_of_date DATE,
    p_adjustment TEXT DEFAULT 'back_adjusted'
)
RETURNS TABLE (
    "time" TIMESTAMPTZ,
    date_str TEXT,
    session_date DATE,
    symbol TEXT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    bar_count BIGINT,
    source_con_id BIGINT,
    contract_month TEXT,
    local_symbol TEXT,
    trading_class TEXT,
    exchange TEXT,
    currency TEXT,
    multiplier TEXT,
    roll_event_id BIGINT,
    roll_time TIMESTAMPTZ,
    known_at TIMESTAMPTZ,
    decision_session_date DATE,
    price_session_date DATE,
    segment_start DATE,
    segment_end DATE,
    is_roll_date BOOLEAN,
    adjustment_value NUMERIC,
    adjustment_ratio NUMERIC,
    adjustment_method TEXT
)
LANGUAGE sql
STABLE
AS $$
WITH known_rolls AS (
    SELECT
        e.*,
        e.effective_roll_time::date AS effective_date,
        row_number() OVER (
            PARTITION BY e.symbol ORDER BY e.effective_roll_time
        ) AS rn,
        lead(e.effective_roll_time::date) OVER (
            PARTITION BY e.symbol ORDER BY e.effective_roll_time
        ) AS next_effective_date
    FROM futures_roll_events_asof e
    WHERE e.symbol = p_symbol
      AND e.known_at::date <= p_as_of_date
      AND e.effective_roll_time::date <= p_as_of_date
),
first_roll_contract AS (
    SELECT
        e.symbol,
        e.from_con_id AS con_id
    FROM futures_roll_events_asof e
    WHERE e.symbol = p_symbol
    ORDER BY e.effective_roll_time ASC, e.known_at ASC, e.id ASC
    LIMIT 1
),
first_roll_contract_metadata AS (
    SELECT
        md.symbol,
        md.con_id,
        md.contract_month,
        md.local_symbol,
        md.trading_class,
        md.exchange,
        md.currency,
        md.multiplier,
        md.session_date AS latest_session_date,
        1 AS priority
    FROM first_roll_contract f
    JOIN LATERAL (
        SELECT
            b.symbol,
            b.con_id,
            b.contract_month,
            b.local_symbol,
            b.trading_class,
            b.exchange,
            b.currency,
            b.multiplier,
            b.session_date
        FROM futures_daily_bars_session_normalized b
        WHERE b.symbol = p_symbol
          AND b.con_id = f.con_id
        ORDER BY b.session_date DESC
        LIMIT 1
    ) md ON TRUE
),
raw_asof_contract AS (
    SELECT
        b.symbol,
        b.con_id,
        b.contract_month,
        b.local_symbol,
        b.trading_class,
        b.exchange,
        b.currency,
        b.multiplier,
        b.session_date AS latest_session_date,
        2 AS priority
    FROM futures_daily_bars_session_normalized b
    WHERE b.symbol = p_symbol
      AND b.session_date <= p_as_of_date
    ORDER BY
        b.session_date DESC,
        COALESCE(b.volume, 0) DESC,
        b.contract_month ASC NULLS LAST,
        b.con_id ASC
    LIMIT 1
),
first_contract AS (
    SELECT
        c.symbol,
        c.con_id,
        c.contract_month,
        c.local_symbol,
        c.trading_class,
        c.exchange,
        c.currency,
        c.multiplier,
        c.latest_session_date
    FROM (
        SELECT * FROM first_roll_contract_metadata
        UNION ALL
        SELECT * FROM raw_asof_contract
    ) c
    ORDER BY c.priority, c.latest_session_date DESC
    LIMIT 1
),
latest_known_roll AS (
    SELECT *
    FROM known_rolls
    ORDER BY effective_roll_time DESC, known_at DESC, id DESC
    LIMIT 1
),
roll_tail_contract AS (
    SELECT
        k.symbol,
        k.to_con_id AS con_id,
        md.last_trade_date
    FROM latest_known_roll k
    JOIN LATERAL (
        SELECT
            d.last_trade_date
        FROM futures_daily_bars_session_normalized d
        WHERE d.symbol = p_symbol
          AND d.con_id = k.to_con_id
        ORDER BY d.time DESC
        LIMIT 1
    ) md ON TRUE
),
tail_fallback AS (
    SELECT
        f.symbol,
        f.con_id,
        (rtc.last_trade_date + 1) AS segment_start
    FROM first_contract f
    JOIN roll_tail_contract rtc
      ON rtc.symbol = f.symbol
    WHERE f.con_id <> rtc.con_id
      AND rtc.last_trade_date IS NOT NULL
      AND rtc.last_trade_date < p_as_of_date
),
segments AS (
    SELECT
        k.symbol,
        k.from_con_id AS con_id,
        NULL::date AS segment_start,
        k.effective_date AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time,
        NULL::timestamptz AS known_at,
        NULL::date AS decision_session_date,
        NULL::date AS price_session_date
    FROM known_rolls k
    WHERE k.rn = 1

    UNION ALL

    SELECT
        k.symbol,
        k.to_con_id AS con_id,
        k.effective_date AS segment_start,
        k.next_effective_date AS segment_end,
        k.id AS roll_event_id,
        k.effective_roll_time AS roll_time,
        k.known_at,
        k.decision_session_date,
        k.price_session_date
    FROM known_rolls k

    UNION ALL

    SELECT
        f.symbol,
        f.con_id,
        NULL::date AS segment_start,
        NULL::date AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time,
        NULL::timestamptz AS known_at,
        NULL::date AS decision_session_date,
        NULL::date AS price_session_date
    FROM first_contract f
    WHERE NOT EXISTS (SELECT 1 FROM known_rolls)

    UNION ALL

    SELECT
        t.symbol,
        t.con_id,
        t.segment_start,
        NULL::date AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time,
        NULL::timestamptz AS known_at,
        NULL::date AS decision_session_date,
        NULL::date AS price_session_date
    FROM tail_fallback t
),
raw AS (
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
        s.roll_event_id,
        s.roll_time,
        s.known_at,
        s.decision_session_date,
        s.price_session_date,
        s.segment_start,
        s.segment_end,
        (s.roll_time IS NOT NULL AND b.session_date = s.roll_time::date)
            AS is_roll_date
    FROM segments s
    JOIN futures_daily_bars_session_normalized b
      ON b.symbol = s.symbol
     AND b.con_id = s.con_id
     AND (s.segment_start IS NULL OR b.session_date >= s.segment_start)
     AND (s.segment_end IS NULL OR b.session_date < s.segment_end)
    WHERE b.session_date >= p_start_date
      AND b.session_date <= p_as_of_date
),
adjusted AS (
    SELECT
        r.*,
        adj.cumulative_gap,
        adj.cumulative_ratio
    FROM raw r
    CROSS JOIN LATERAL (
        SELECT
            COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap,
            COALESCE(
                EXP(SUM(LN(e.ratio::double precision)))::numeric,
                1::numeric
            ) AS cumulative_ratio
        FROM known_rolls e
        WHERE e.effective_roll_time::date > r.session_date
    ) adj
)
SELECT
    a.time,
    a.date_str,
    a.session_date,
    a.symbol,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.open * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.open
        ELSE a.open + a.cumulative_gap
    END AS open,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.high * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.high
        ELSE a.high + a.cumulative_gap
    END AS high,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.low * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.low
        ELSE a.low + a.cumulative_gap
    END AS low,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.close * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.close
        ELSE a.close + a.cumulative_gap
    END AS close,
    a.volume::bigint AS volume,
    a.bar_count::bigint AS bar_count,
    a.source_con_id,
    a.contract_month,
    a.local_symbol,
    a.trading_class,
    a.exchange,
    a.currency,
    a.multiplier,
    a.roll_event_id,
    a.roll_time,
    a.known_at,
    a.decision_session_date,
    a.price_session_date,
    a.segment_start,
    a.segment_end,
    a.is_roll_date,
    CASE
        WHEN p_adjustment = 'raw' THEN 0::numeric
        ELSE a.cumulative_gap
    END AS adjustment_value,
    CASE
        WHEN p_adjustment = 'raw' THEN 1::numeric
        ELSE a.cumulative_ratio
    END AS adjustment_ratio,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN 'ratio_adjusted_asof'
        WHEN p_adjustment = 'raw' THEN 'raw_asof'
        ELSE 'back_adjusted_difference_asof'
    END AS adjustment_method
FROM adjusted a
ORDER BY a.session_date;
$$;

DROP FUNCTION IF EXISTS continuous_futures_minute_asof_raw(TEXT, TIMESTAMPTZ, TIMESTAMPTZ);

CREATE FUNCTION continuous_futures_minute_asof_raw(
    p_symbol TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE (
    "time" TIMESTAMPTZ,
    symbol TEXT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    bar_count INTEGER,
    source_con_id BIGINT,
    contract_month TEXT,
    local_symbol TEXT,
    trading_class TEXT,
    exchange TEXT,
    currency TEXT,
    multiplier TEXT,
    roll_event_id BIGINT,
    roll_time TIMESTAMPTZ,
    segment_start TIMESTAMPTZ,
    segment_end TIMESTAMPTZ
)
LANGUAGE sql
STABLE
AS $$
WITH ordered_rolls AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.symbol ORDER BY e.effective_roll_time
        ) AS rn,
        lead(e.effective_roll_time) OVER (
            PARTITION BY e.symbol ORDER BY e.effective_roll_time
        ) AS next_effective_time
    FROM futures_roll_events_asof e
    WHERE e.symbol = p_symbol
),
first_contract AS (
    SELECT
        b.symbol,
        b.con_id,
        b.contract_month,
        b.local_symbol,
        b.trading_class,
        b.exchange,
        b.currency,
        b.multiplier,
        max(b.time) AS latest_time
    FROM futures_minute_bars b
    WHERE b.symbol = p_symbol
      AND b.time < p_end_time
    GROUP BY
        b.symbol, b.con_id, b.contract_month, b.local_symbol,
        b.trading_class, b.exchange, b.currency, b.multiplier
    ORDER BY latest_time DESC, b.contract_month DESC, b.con_id DESC
    LIMIT 1
),
segments AS (
    SELECT
        r.symbol,
        r.from_con_id AS con_id,
        NULL::timestamptz AS segment_start,
        r.effective_roll_time AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time
    FROM ordered_rolls r
    WHERE r.rn = 1

    UNION ALL

    SELECT
        r.symbol,
        r.to_con_id AS con_id,
        r.effective_roll_time AS segment_start,
        r.next_effective_time AS segment_end,
        r.id AS roll_event_id,
        r.effective_roll_time AS roll_time
    FROM ordered_rolls r

    UNION ALL

    SELECT
        f.symbol,
        f.con_id,
        NULL::timestamptz AS segment_start,
        NULL::timestamptz AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time
    FROM first_contract f
    WHERE NOT EXISTS (SELECT 1 FROM ordered_rolls)
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
    s.roll_event_id,
    s.roll_time,
    s.segment_start,
    s.segment_end
FROM segments s
JOIN futures_minute_bars b
  ON b.symbol = s.symbol
 AND b.con_id = s.con_id
 AND (s.segment_start IS NULL OR b.time >= s.segment_start)
 AND (s.segment_end IS NULL OR b.time < s.segment_end)
WHERE b.time >= p_start_time
  AND b.time < p_end_time
ORDER BY b.time;
$$;

DROP FUNCTION IF EXISTS continuous_futures_minute_asof_adjusted(
    TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TEXT
);

CREATE FUNCTION continuous_futures_minute_asof_adjusted(
    p_symbol TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_as_of_time TIMESTAMPTZ,
    p_adjustment TEXT DEFAULT 'back_adjusted'
)
RETURNS TABLE (
    "time" TIMESTAMPTZ,
    symbol TEXT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    bar_count INTEGER,
    source_con_id BIGINT,
    contract_month TEXT,
    local_symbol TEXT,
    trading_class TEXT,
    exchange TEXT,
    currency TEXT,
    multiplier TEXT,
    roll_event_id BIGINT,
    roll_time TIMESTAMPTZ,
    known_at TIMESTAMPTZ,
    segment_start TIMESTAMPTZ,
    segment_end TIMESTAMPTZ,
    is_roll_time BOOLEAN,
    adjustment_value NUMERIC,
    adjustment_ratio NUMERIC,
    adjustment_method TEXT
)
LANGUAGE sql
STABLE
AS $$
WITH known_rolls AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.symbol ORDER BY e.effective_roll_time
        ) AS rn,
        lead(e.effective_roll_time) OVER (
            PARTITION BY e.symbol ORDER BY e.effective_roll_time
        ) AS next_effective_time
    FROM futures_roll_events_asof e
    WHERE e.symbol = p_symbol
      AND e.known_at <= p_as_of_time
      AND e.effective_roll_time <= p_as_of_time
),
first_contract AS (
    SELECT
        b.symbol,
        b.con_id,
        b.contract_month,
        b.local_symbol,
        b.trading_class,
        b.exchange,
        b.currency,
        b.multiplier,
        max(b.time) AS latest_time
    FROM futures_minute_bars b
    WHERE b.symbol = p_symbol
      AND b.time <= p_as_of_time
    GROUP BY
        b.symbol, b.con_id, b.contract_month, b.local_symbol,
        b.trading_class, b.exchange, b.currency, b.multiplier
    ORDER BY latest_time DESC, b.contract_month DESC, b.con_id DESC
    LIMIT 1
),
segments AS (
    SELECT
        r.symbol,
        r.from_con_id AS con_id,
        NULL::timestamptz AS segment_start,
        r.effective_roll_time AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time,
        NULL::timestamptz AS known_at
    FROM known_rolls r
    WHERE r.rn = 1

    UNION ALL

    SELECT
        r.symbol,
        r.to_con_id AS con_id,
        r.effective_roll_time AS segment_start,
        r.next_effective_time AS segment_end,
        r.id AS roll_event_id,
        r.effective_roll_time AS roll_time,
        r.known_at
    FROM known_rolls r

    UNION ALL

    SELECT
        f.symbol,
        f.con_id,
        NULL::timestamptz AS segment_start,
        NULL::timestamptz AS segment_end,
        NULL::bigint AS roll_event_id,
        NULL::timestamptz AS roll_time,
        NULL::timestamptz AS known_at
    FROM first_contract f
    WHERE NOT EXISTS (SELECT 1 FROM known_rolls)
),
raw AS (
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
        s.roll_event_id,
        s.roll_time,
        s.known_at,
        s.segment_start,
        s.segment_end,
        (s.roll_time IS NOT NULL AND b.time = s.roll_time) AS is_roll_time
    FROM segments s
    JOIN futures_minute_bars b
      ON b.symbol = s.symbol
     AND b.con_id = s.con_id
     AND (s.segment_start IS NULL OR b.time >= s.segment_start)
     AND (s.segment_end IS NULL OR b.time < s.segment_end)
    WHERE b.time >= p_start_time
      AND b.time < p_end_time
      AND b.time <= p_as_of_time
),
adjusted AS (
    SELECT
        r.*,
        adj.cumulative_gap,
        adj.cumulative_ratio
    FROM raw r
    CROSS JOIN LATERAL (
        SELECT
            COALESCE(SUM(e.price_gap), 0)::numeric AS cumulative_gap,
            COALESCE(
                EXP(SUM(LN(e.ratio::double precision)))::numeric,
                1::numeric
            ) AS cumulative_ratio
        FROM known_rolls e
        WHERE e.effective_roll_time > r.time
    ) adj
)
SELECT
    a.time,
    a.symbol,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.open * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.open
        ELSE a.open + a.cumulative_gap
    END AS open,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.high * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.high
        ELSE a.high + a.cumulative_gap
    END AS high,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.low * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.low
        ELSE a.low + a.cumulative_gap
    END AS low,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN a.close * a.cumulative_ratio
        WHEN p_adjustment = 'raw' THEN a.close
        ELSE a.close + a.cumulative_gap
    END AS close,
    a.volume,
    a.bar_count,
    a.source_con_id,
    a.contract_month,
    a.local_symbol,
    a.trading_class,
    a.exchange,
    a.currency,
    a.multiplier,
    a.roll_event_id,
    a.roll_time,
    a.known_at,
    a.segment_start,
    a.segment_end,
    a.is_roll_time,
    CASE
        WHEN p_adjustment = 'raw' THEN 0::numeric
        ELSE a.cumulative_gap
    END AS adjustment_value,
    CASE
        WHEN p_adjustment = 'raw' THEN 1::numeric
        ELSE a.cumulative_ratio
    END AS adjustment_ratio,
    CASE
        WHEN p_adjustment = 'ratio_adjusted' THEN 'ratio_adjusted_asof'
        WHEN p_adjustment = 'raw' THEN 'raw_asof'
        ELSE 'back_adjusted_difference_asof'
    END AS adjustment_method
FROM adjusted a
ORDER BY a.time;
$$;
