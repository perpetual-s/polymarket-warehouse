-- PolymarketAnalytics trader intelligence.

CREATE SCHEMA IF NOT EXISTS bonus;

CREATE TABLE IF NOT EXISTS bonus.raw_pma_trader_snapshots (
    captured_at TIMESTAMPTZ NOT NULL,
    run_id UUID NOT NULL,
    rank INTEGER NOT NULL,
    trader_address TEXT NOT NULL,
    trader_name TEXT,
    tag TEXT NOT NULL DEFAULT 'Overall',
    overall_gain NUMERIC(24, 6),
    win_amount NUMERIC(24, 6),
    loss_amount NUMERIC(24, 6),
    win_rate NUMERIC(10, 8),
    win_count INTEGER,
    total_positions INTEGER,
    active_positions INTEGER,
    total_current_value NUMERIC(24, 6),
    event_count INTEGER,
    trader_tags TEXT,
    payload JSONB NOT NULL,
    PRIMARY KEY (captured_at, rank, trader_address)
);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_pma_trader_address_time
    ON bonus.raw_pma_trader_snapshots (trader_address, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_pma_trader_rank
    ON bonus.raw_pma_trader_snapshots (rank, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_raw_pma_pnl
    ON bonus.raw_pma_trader_snapshots (overall_gain DESC NULLS LAST);

SELECT create_hypertable(
    'bonus.raw_pma_trader_snapshots',
    'captured_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);
