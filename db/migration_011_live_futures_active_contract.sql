-- IBKR Data System - Migration 011: Live futures active contract support
--
-- Adds the DB primitives needed by live futures roll handling.  Raw ticks are
-- stored by real contract identity, and active contract lookup is derived from
-- as-of roll events so live receivers, APIs, and trading code can agree on the
-- same contract.

CREATE TABLE IF NOT EXISTS futures_ticks (
    time              TIMESTAMPTZ NOT NULL,
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    local_symbol      TEXT,
    trading_class     TEXT,
    contract_month    TEXT,
    last_trade_date   DATE,
    exchange          TEXT,
    currency          TEXT,
    multiplier        TEXT,
    bid               NUMERIC(16,6),
    ask               NUMERIC(16,6),
    last              NUMERIC(16,6),
    volume            BIGINT,
    open              NUMERIC(16,6),
    high              NUMERIC(16,6),
    low               NUMERIC(16,6),
    close             NUMERIC(16,6),
    source            TEXT NOT NULL DEFAULT 'IBKR',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'create_hypertable'
          AND n.nspname = 'public'
    ) THEN
        PERFORM create_hypertable('futures_ticks', 'time', if_not_exists => TRUE);
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Skipping futures_ticks hypertable creation: %', SQLERRM;
END $$;

CREATE INDEX IF NOT EXISTS idx_futures_ticks_symbol_contract_time
    ON futures_ticks (symbol, con_id, time DESC);

DROP FUNCTION IF EXISTS active_futures_contract_asof(TEXT, TIMESTAMPTZ);

CREATE FUNCTION active_futures_contract_asof(
    p_symbol TEXT,
    p_as_of TIMESTAMPTZ
)
RETURNS TABLE (
    symbol TEXT,
    con_id BIGINT,
    contract_month TEXT,
    local_symbol TEXT,
    trading_class TEXT,
    exchange TEXT,
    currency TEXT,
    multiplier TEXT,
    last_trade_date DATE,
    effective_from TIMESTAMPTZ,
    roll_event_id BIGINT
)
LANGUAGE sql
STABLE
AS $$
WITH latest_effective_roll AS (
    SELECT
        e.symbol,
        e.to_con_id AS con_id,
        e.to_contract_month AS event_contract_month,
        e.to_local_symbol AS event_local_symbol,
        e.effective_roll_time AS effective_from,
        e.id AS roll_event_id,
        1 AS priority
    FROM futures_roll_events_asof e
    WHERE e.symbol = p_symbol
      AND e.known_at <= p_as_of
      AND e.effective_roll_time <= p_as_of
    ORDER BY e.effective_roll_time DESC, e.known_at DESC, e.id DESC
    LIMIT 1
),
first_roll_contract AS (
    SELECT
        e.symbol,
        e.from_con_id AS con_id,
        e.from_contract_month AS event_contract_month,
        e.from_local_symbol AS event_local_symbol,
        NULL::timestamptz AS effective_from,
        NULL::bigint AS roll_event_id,
        2 AS priority
    FROM futures_roll_events_asof e
    WHERE e.symbol = p_symbol
    ORDER BY e.effective_roll_time ASC, e.known_at ASC, e.id ASC
    LIMIT 1
),
first_raw_contract AS (
    SELECT
        b.symbol,
        b.con_id,
        b.contract_month AS event_contract_month,
        b.local_symbol AS event_local_symbol,
        MIN(b.observed_at) AS effective_from,
        NULL::bigint AS roll_event_id,
        3 AS priority
    FROM (
        SELECT
            d.symbol,
            d.con_id,
            d.contract_month,
            d.local_symbol,
            d.time AS observed_at
        FROM futures_daily_bars d
        WHERE d.symbol = p_symbol

        UNION ALL

        SELECT
            m.symbol,
            m.con_id,
            m.contract_month,
            m.local_symbol,
            m.time AS observed_at
        FROM futures_minute_bars m
        WHERE m.symbol = p_symbol
    ) b
    GROUP BY b.symbol, b.con_id, b.contract_month, b.local_symbol
    ORDER BY MAX(b.observed_at) DESC, b.contract_month DESC NULLS LAST, b.con_id DESC
    LIMIT 1
),
chosen AS (
    SELECT *
    FROM (
        SELECT * FROM latest_effective_roll
        UNION ALL
        SELECT * FROM first_roll_contract
        UNION ALL
        SELECT * FROM first_raw_contract
    ) c
    ORDER BY c.priority
    LIMIT 1
),
raw_fallback AS (
    SELECT
        b.symbol,
        b.con_id,
        b.contract_month,
        b.local_symbol,
        b.trading_class,
        b.exchange,
        b.currency,
        b.multiplier,
        b.last_trade_date,
        MIN(b.observed_at) AS effective_from
    FROM (
        SELECT
            d.symbol,
            d.con_id,
            d.contract_month,
            d.local_symbol,
            d.trading_class,
            d.exchange,
            d.currency,
            d.multiplier,
            d.last_trade_date,
            d.time AS observed_at
        FROM futures_daily_bars d
        WHERE d.symbol = p_symbol

        UNION ALL

        SELECT
            m.symbol,
            m.con_id,
            m.contract_month,
            m.local_symbol,
            m.trading_class,
            m.exchange,
            m.currency,
            m.multiplier,
            m.last_trade_date,
            m.time AS observed_at
        FROM futures_minute_bars m
        WHERE m.symbol = p_symbol
    ) b
    GROUP BY
        b.symbol, b.con_id, b.contract_month, b.local_symbol,
        b.trading_class, b.exchange, b.currency, b.multiplier, b.last_trade_date
    ORDER BY MAX(b.observed_at) DESC, b.contract_month DESC NULLS LAST, b.con_id DESC
    LIMIT 1
),
metadata AS (
    SELECT
        md.symbol,
        md.con_id,
        md.contract_month,
        md.local_symbol,
        md.trading_class,
        md.exchange,
        md.currency,
        md.multiplier,
        md.last_trade_date
    FROM chosen c
    JOIN LATERAL (
        SELECT
            d.symbol,
            d.con_id,
            d.contract_month,
            d.local_symbol,
            d.trading_class,
            d.exchange,
            d.currency,
            d.multiplier,
            d.last_trade_date,
            1 AS source_priority,
            d.time AS observed_at
        FROM futures_daily_bars d
        WHERE d.symbol = c.symbol
          AND d.con_id = c.con_id

        UNION ALL

        SELECT
            m.symbol,
            m.con_id,
            m.contract_month,
            m.local_symbol,
            m.trading_class,
            m.exchange,
            m.currency,
            m.multiplier,
            m.last_trade_date,
            2 AS source_priority,
            m.time AS observed_at
        FROM futures_minute_bars m
        WHERE m.symbol = c.symbol
          AND m.con_id = c.con_id
    ) md ON TRUE
    ORDER BY md.source_priority, md.observed_at DESC NULLS LAST
    LIMIT 1
),
selected AS (
    SELECT
        c.symbol,
        c.con_id,
        COALESCE(md.contract_month, c.event_contract_month) AS contract_month,
        COALESCE(md.local_symbol, c.event_local_symbol) AS local_symbol,
        md.trading_class,
        md.exchange,
        md.currency,
        md.multiplier,
        md.last_trade_date,
        c.effective_from,
        c.roll_event_id
    FROM chosen c
    LEFT JOIN metadata md
      ON md.symbol = c.symbol
     AND md.con_id = c.con_id
),
resolved AS (
    SELECT *, 1 AS priority
    FROM selected
    UNION ALL
    SELECT
        r.symbol,
        r.con_id,
        r.contract_month,
        r.local_symbol,
        r.trading_class,
        r.exchange,
        r.currency,
        r.multiplier,
        r.last_trade_date,
        r.effective_from,
        NULL::bigint AS roll_event_id,
        0 AS priority
    FROM raw_fallback r
    CROSS JOIN selected s
    WHERE s.last_trade_date IS NOT NULL
      AND s.last_trade_date < p_as_of::date
      AND (r.last_trade_date IS NULL OR r.last_trade_date > s.last_trade_date)
)
SELECT
    symbol,
    con_id,
    contract_month,
    local_symbol,
    trading_class,
    exchange,
    currency,
    multiplier,
    last_trade_date,
    effective_from,
    roll_event_id
FROM resolved
ORDER BY priority
LIMIT 1;
$$;
