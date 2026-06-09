-- IBKR Data System - Migration 008: Exchange-calendar daily normalization
--
-- Upgrades futures_daily_bars_session_normalized from weekend/override logic
-- to exchange-calendar driven session mapping.  Raw futures_daily_bars remains
-- immutable.

CREATE TABLE IF NOT EXISTS exchange_trading_days (
    exchange_code  TEXT NOT NULL,
    trading_date   DATE NOT NULL,
    is_open        BOOLEAN NOT NULL,
    reason         TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange_code, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_exchange_trading_days_open
    ON exchange_trading_days (exchange_code, trading_date)
    WHERE is_open;

CREATE TABLE IF NOT EXISTS futures_daily_symbol_calendars (
    symbol         TEXT PRIMARY KEY,
    exchange_code  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE VIEW futures_daily_bars_session_normalized AS
WITH mapped AS (
    SELECT
        b.*,
        COALESCE(
            o.target_date,
            CASE
                WHEN sc.exchange_code IS NOT NULL
                 AND COALESCE(td.is_open, FALSE) IS FALSE
                THEN next_open.trading_date
                WHEN sc.exchange_code IS NOT NULL
                THEN b.time::date
                WHEN EXTRACT(ISODOW FROM b.time::date) IN (6, 7)
                THEN next_raw_weekday.next_date
                ELSE b.time::date
            END,
            b.time::date
        ) AS session_date
    FROM futures_daily_bars b
    LEFT JOIN futures_daily_session_date_overrides o
      ON o.symbol = b.symbol
     AND o.source_date = b.time::date
    LEFT JOIN futures_daily_symbol_calendars sc
      ON sc.symbol = b.symbol
    LEFT JOIN exchange_trading_days td
      ON td.exchange_code = sc.exchange_code
     AND td.trading_date = b.time::date
    LEFT JOIN LATERAL (
        SELECT etd.trading_date
        FROM exchange_trading_days etd
        WHERE etd.exchange_code = sc.exchange_code
          AND etd.trading_date > b.time::date
          AND etd.is_open
        ORDER BY etd.trading_date
        LIMIT 1
    ) next_open ON TRUE
    LEFT JOIN LATERAL (
        SELECT nb.time::date AS next_date
        FROM futures_daily_bars nb
        WHERE nb.symbol = b.symbol
          AND nb.con_id = b.con_id
          AND nb.time::date > b.time::date
          AND EXTRACT(ISODOW FROM nb.time::date) BETWEEN 1 AND 5
        ORDER BY nb.time
        LIMIT 1
    ) next_raw_weekday ON TRUE
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
