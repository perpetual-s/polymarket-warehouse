CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable(
    'bonus.market_ticks_1m',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'bonus.orderbook_topn_1m',
    'ts',
    chunk_time_interval => INTERVAL '2 days',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'bonus.account_trades',
    'executed_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'bonus.account_positions_1h',
    'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_bonus_ticks_market_ts
    ON bonus.market_ticks_1m (market_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_ticks_token_ts
    ON bonus.market_ticks_1m (token_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_orderbook_market_ts
    ON bonus.orderbook_topn_1m (market_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_trades_account_time
    ON bonus.account_trades (account_id, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_trades_market_time
    ON bonus.account_trades (market_id, executed_at DESC);
