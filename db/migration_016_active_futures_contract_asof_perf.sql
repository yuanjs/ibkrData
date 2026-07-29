-- IBKR Data System - Migration 016: active_futures_contract_asof performance
--
-- The function used to resolve the current contract by grouping every raw bar
-- for the symbol (futures_daily_bars UNION ALL futures_minute_bars) and taking
-- the group with the newest observation. That scanned the symbol's entire
-- minute history twice per call -- ~906k rows for MES, ~1.87M for MYM -- to
-- return a single row, costing ~1.0s. It is called on every daily-chart
-- request (via _append_live_partial_daily) and by /futures/{symbol}/active-contract.
--
-- The grouping is replaced by two index-backed seeks:
--   1. newest bar per source via (symbol, time DESC)
--   2. that contract's first observation via the (symbol, con_id, time) PKs
-- This relies on (symbol, con_id) determining the contract metadata, which
-- holds in both raw tables. Results are unchanged; verified equal to the old
-- definition across every symbol at every roll boundary plus sparse sampling.

CREATE OR REPLACE FUNCTION public.active_futures_contract_asof(p_symbol text, p_as_of timestamp with time zone)
 RETURNS TABLE(symbol text, con_id bigint, contract_month text, local_symbol text, trading_class text, exchange text, currency text, multiplier text, last_trade_date date, effective_from timestamp with time zone, roll_event_id bigint)
 LANGUAGE sql
 STABLE
AS $function$
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
-- The most recently observed contract for this symbol. Equivalent to grouping
-- every raw bar by contract and taking the group with the greatest observation,
-- but seeks the newest bar via (symbol, time DESC) instead of scanning the
-- symbol's whole minute history. Safe because (symbol, con_id) determines the
-- contract metadata in both raw tables.
latest_raw_obs AS (
    SELECT b.con_id, b.contract_month, b.local_symbol, b.trading_class,
           b.exchange, b.currency, b.multiplier, b.last_trade_date
    FROM (
        SELECT d.con_id, d.contract_month, d.local_symbol, d.trading_class,
               d.exchange, d.currency, d.multiplier, d.last_trade_date,
               d.time AS observed_at
        FROM futures_daily_bars d
        WHERE d.symbol = p_symbol
          AND d.time = (SELECT max(x.time) FROM futures_daily_bars x WHERE x.symbol = p_symbol)

        UNION ALL

        SELECT m.con_id, m.contract_month, m.local_symbol, m.trading_class,
               m.exchange, m.currency, m.multiplier, m.last_trade_date,
               m.time AS observed_at
        FROM futures_minute_bars m
        WHERE m.symbol = p_symbol
          AND m.time = (SELECT max(x.time) FROM futures_minute_bars x WHERE x.symbol = p_symbol)
    ) b
    ORDER BY b.observed_at DESC, b.contract_month DESC NULLS LAST, b.con_id DESC
    LIMIT 1
),
-- First observation of that contract; both lookups ride the (symbol, con_id, time)
-- primary keys. LEAST ignores NULLs, matching MIN() over the UNION ALL.
latest_raw_contract AS (
    SELECT
        p_symbol AS symbol,
        r.con_id,
        r.contract_month,
        r.local_symbol,
        r.trading_class,
        r.exchange,
        r.currency,
        r.multiplier,
        r.last_trade_date,
        LEAST(
            (SELECT min(d.time) FROM futures_daily_bars d
              WHERE d.symbol = p_symbol AND d.con_id = r.con_id),
            (SELECT min(m.time) FROM futures_minute_bars m
              WHERE m.symbol = p_symbol AND m.con_id = r.con_id)
        ) AS effective_from
    FROM latest_raw_obs r
),
first_raw_contract AS (
    SELECT
        c.symbol,
        c.con_id,
        c.contract_month AS event_contract_month,
        c.local_symbol AS event_local_symbol,
        c.effective_from,
        NULL::bigint AS roll_event_id,
        3 AS priority
    FROM latest_raw_contract c
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
        c.symbol,
        c.con_id,
        c.contract_month,
        c.local_symbol,
        c.trading_class,
        c.exchange,
        c.currency,
        c.multiplier,
        c.last_trade_date,
        c.effective_from
    FROM latest_raw_contract c
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
        SELECT * FROM (
            (SELECT
                d.symbol, d.con_id, d.contract_month, d.local_symbol, d.trading_class,
                d.exchange, d.currency, d.multiplier, d.last_trade_date,
                1 AS source_priority, d.time AS observed_at
             FROM futures_daily_bars d
             WHERE d.symbol = c.symbol AND d.con_id = c.con_id
             ORDER BY d.time DESC NULLS LAST
             LIMIT 1)

            UNION ALL

            (SELECT
                m.symbol, m.con_id, m.contract_month, m.local_symbol, m.trading_class,
                m.exchange, m.currency, m.multiplier, m.last_trade_date,
                2 AS source_priority, m.time AS observed_at
             FROM futures_minute_bars m
             WHERE m.symbol = c.symbol AND m.con_id = c.con_id
             ORDER BY m.time DESC NULLS LAST
             LIMIT 1)
        ) u
        ORDER BY u.source_priority, u.observed_at DESC NULLS LAST
        LIMIT 1
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
$function$;
