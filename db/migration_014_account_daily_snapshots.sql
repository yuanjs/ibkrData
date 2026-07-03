-- IBKR Data System - Migration 014: Account daily snapshots
--
-- Pre-aggregates account equity history at day/account granularity so the
-- account history API does not scan and sort raw account_snapshots on every
-- request.

CREATE INDEX IF NOT EXISTS idx_account_snapshots_account_time
    ON account_snapshots (account_id, time DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS account_daily_snapshots
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    account_id,
    last(net_liquidation, time) AS net_liquidation,
    last(daily_pnl, time) AS daily_pnl
FROM account_snapshots
GROUP BY bucket, account_id
WITH NO DATA;

ALTER MATERIALIZED VIEW account_daily_snapshots
    SET (timescaledb.materialized_only = false);

CREATE INDEX IF NOT EXISTS idx_account_daily_snapshots_account_bucket
    ON account_daily_snapshots (account_id, bucket DESC);

DO $$
BEGIN
    PERFORM add_continuous_aggregate_policy(
        'account_daily_snapshots',
        start_offset => INTERVAL '180 days',
        end_offset => INTERVAL '1 minute',
        schedule_interval => INTERVAL '5 minutes'
    );
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END $$;

CALL refresh_continuous_aggregate('account_daily_snapshots', NULL, NULL);
