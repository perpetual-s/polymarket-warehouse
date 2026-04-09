-- Wallet-first raw foundation.
-- Source of truth is the raw_wallet_* layer (append-only, full JSON payloads).

CREATE SCHEMA IF NOT EXISTS bonus;

-- --- wallets (normalized dimension) ---
CREATE TABLE IF NOT EXISTS bonus.wallets (
    address TEXT PRIMARY KEY,
    user_name TEXT,
    name TEXT,
    pseudonym TEXT,
    bio TEXT,
    x_username TEXT,
    profile_image TEXT,
    verified_badge BOOLEAN NOT NULL DEFAULT FALSE,
    display_username_public BOOLEAN,
    created_at_polymarket TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_profile_refresh_at TIMESTAMPTZ,
    last_positions_refresh_at TIMESTAMPTZ,
    last_activity_refresh_at TIMESTAMPTZ,
    discovered_via TEXT NOT NULL
        CHECK (discovered_via IN ('leaderboard','holders','manual','activity','follow','pma')),
    is_tracked BOOLEAN NOT NULL DEFAULT FALSE,
    priority SMALLINT NOT NULL DEFAULT 0,
    tracked_since TIMESTAMPTZ,
    latest_leaderboard_rank INTEGER,
    latest_leaderboard_pnl NUMERIC(24, 6),
    latest_leaderboard_vol NUMERIC(24, 6),
    latest_portfolio_value_usd NUMERIC(24, 6),
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_bonus_wallets_tracked
    ON bonus.wallets (is_tracked, priority DESC, latest_leaderboard_pnl DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_bonus_wallets_discovered_via
    ON bonus.wallets (discovered_via, first_seen_at DESC);

-- --- raw_wallet_leaderboard_snapshots ---
CREATE TABLE IF NOT EXISTS bonus.raw_wallet_leaderboard_snapshots (
    captured_at TIMESTAMPTZ NOT NULL,
    run_id UUID NOT NULL,
    rank INTEGER NOT NULL,
    proxy_wallet TEXT NOT NULL,
    user_name TEXT,
    x_username TEXT,
    verified_badge BOOLEAN,
    vol NUMERIC(24, 6),
    pnl NUMERIC(24, 6),
    profile_image TEXT,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, rank, proxy_wallet)
);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_leaderboard_wallet_time
    ON bonus.raw_wallet_leaderboard_snapshots (proxy_wallet, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_leaderboard_run
    ON bonus.raw_wallet_leaderboard_snapshots (run_id);

SELECT create_hypertable(
    'bonus.raw_wallet_leaderboard_snapshots',
    'captured_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- --- raw_wallet_profiles ---
CREATE TABLE IF NOT EXISTS bonus.raw_wallet_profiles (
    captured_at TIMESTAMPTZ NOT NULL,
    run_id UUID NOT NULL,
    proxy_wallet TEXT NOT NULL,
    name TEXT,
    bio TEXT,
    profile_image TEXT,
    verified_badge BOOLEAN,
    display_username_public BOOLEAN,
    created_at_polymarket TIMESTAMPTZ,
    users JSONB,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, proxy_wallet)
);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_profiles_wallet_time
    ON bonus.raw_wallet_profiles (proxy_wallet, captured_at DESC);

SELECT create_hypertable(
    'bonus.raw_wallet_profiles',
    'captured_at',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- --- raw_wallet_positions_snapshots ---
CREATE TABLE IF NOT EXISTS bonus.raw_wallet_positions_snapshots (
    captured_at TIMESTAMPTZ NOT NULL,
    run_id UUID NOT NULL,
    proxy_wallet TEXT NOT NULL,
    asset TEXT NOT NULL,
    condition_id TEXT,
    size NUMERIC(38, 8),
    avg_price NUMERIC(18, 8),
    current_value NUMERIC(24, 6),
    initial_value NUMERIC(24, 6),
    cur_price NUMERIC(18, 8),
    cash_pnl NUMERIC(24, 6),
    percent_pnl NUMERIC(18, 6),
    realized_pnl NUMERIC(24, 6),
    percent_realized_pnl NUMERIC(18, 6),
    title TEXT,
    slug TEXT,
    outcome TEXT,
    outcome_index SMALLINT,
    opposite_outcome TEXT,
    end_date TIMESTAMPTZ,
    redeemable BOOLEAN,
    mergeable BOOLEAN,
    negative_risk BOOLEAN,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, proxy_wallet, asset)
);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_positions_wallet_time
    ON bonus.raw_wallet_positions_snapshots (proxy_wallet, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_positions_condition
    ON bonus.raw_wallet_positions_snapshots (condition_id, captured_at DESC);

SELECT create_hypertable(
    'bonus.raw_wallet_positions_snapshots',
    'captured_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- --- raw_wallet_activity_events ---
CREATE TABLE IF NOT EXISTS bonus.raw_wallet_activity_events (
    event_ts TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    run_id UUID NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    proxy_wallet TEXT NOT NULL,
    activity_type TEXT NOT NULL,
    transaction_hash TEXT,
    condition_id TEXT,
    asset TEXT,
    title TEXT,
    slug TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index SMALLINT,
    side TEXT,
    price NUMERIC(18, 8),
    size NUMERIC(38, 8),
    usdc_size NUMERIC(24, 6),
    payload JSONB NOT NULL,
    PRIMARY KEY (event_ts, event_id)
);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_activity_wallet_time
    ON bonus.raw_wallet_activity_events (proxy_wallet, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_activity_condition_time
    ON bonus.raw_wallet_activity_events (condition_id, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_activity_type_time
    ON bonus.raw_wallet_activity_events (activity_type, event_ts DESC);

SELECT create_hypertable(
    'bonus.raw_wallet_activity_events',
    'event_ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- --- collector_runs ---
CREATE TABLE IF NOT EXISTS bonus.collector_runs (
    run_id UUID PRIMARY KEY,
    ingestor TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL
        CHECK (status IN ('running','ok','error','partial')),
    rows_fetched INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_skipped INTEGER NOT NULL DEFAULT 0,
    wallet_count INTEGER,
    error_message TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_bonus_collector_runs_ingestor_time
    ON bonus.collector_runs (ingestor, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_collector_runs_status_time
    ON bonus.collector_runs (status, started_at DESC);

-- --- collector_health ---
CREATE TABLE IF NOT EXISTS bonus.collector_health (
    ts TIMESTAMPTZ NOT NULL,
    ingestor TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_per_minute NUMERIC(18, 4),
    lag_seconds NUMERIC(18, 4),
    error_count INTEGER NOT NULL DEFAULT 0,
    run_id UUID,
    PRIMARY KEY (ts, ingestor)
);

CREATE INDEX IF NOT EXISTS idx_bonus_collector_health_ingestor_time
    ON bonus.collector_health (ingestor, ts DESC);

SELECT create_hypertable(
    'bonus.collector_health',
    'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
