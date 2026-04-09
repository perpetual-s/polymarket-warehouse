-- Market holders raw table.
-- One row per (captured_at, market, token, wallet).

CREATE SCHEMA IF NOT EXISTS bonus;

CREATE TABLE IF NOT EXISTS bonus.raw_market_holders_snapshots (
    captured_at TIMESTAMPTZ NOT NULL,
    run_id UUID NOT NULL,
    market_condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    proxy_wallet TEXT NOT NULL,
    amount NUMERIC(38, 8),
    outcome_index SMALLINT,
    name TEXT,
    pseudonym TEXT,
    bio TEXT,
    profile_image TEXT,
    display_username_public BOOLEAN,
    verified BOOLEAN,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, market_condition_id, token_id, proxy_wallet)
);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_holders_wallet_time
    ON bonus.raw_market_holders_snapshots (proxy_wallet, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_holders_market_time
    ON bonus.raw_market_holders_snapshots (market_condition_id, captured_at DESC);

SELECT create_hypertable(
    'bonus.raw_market_holders_snapshots',
    'captured_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
