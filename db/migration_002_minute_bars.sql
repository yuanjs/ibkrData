-- IBKR Data System - Migration 002: Minute-bar history table for backfiller
--
-- Creates a TimescaleDB hypertable for storing 1-minute OHLCV bars obtained
-- from IBKR's reqHistoricalData endpoint.
--
-- The PRIMARY KEY on (symbol, time) enables ON CONFLICT DO NOTHING upsert
-- from the backfiller, ensuring idempotent re-runs.

CREATE TABLE IF NOT EXISTS minute_bars (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    bar_count   INTEGER,
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable('minute_bars', 'time', if_not_exists => TRUE);
