CREATE SCHEMA IF NOT EXISTS bonus;

CREATE TABLE IF NOT EXISTS bonus.markets (
    market_id TEXT PRIMARY KEY,
    pelion_market_uuid UUID UNIQUE,
    source_condition_id TEXT NOT NULL UNIQUE,
    source_market_id TEXT,
    slug TEXT NOT NULL UNIQUE,
    question TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'uncategorized',
    outcome_a TEXT NOT NULL,
    outcome_b TEXT NOT NULL,
    token_a TEXT NOT NULL UNIQUE,
    token_b TEXT NOT NULL UNIQUE,
    outcome_price_a_live NUMERIC(10, 6),
    outcome_price_b_live NUMERIC(10, 6),
    group_item_title TEXT,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    volume_total_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
    volume_24h_live_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
    liquidity_live_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
    best_bid_live NUMERIC(10, 6),
    best_ask_live NUMERIC(10, 6),
    spread_live NUMERIC(10, 6),
    last_trade_price_live NUMERIC(10, 6),
    competitive NUMERIC(10, 8),
    order_min_size NUMERIC(12, 2),
    order_price_min_tick_size NUMERIC(10, 6),
    accepting_orders BOOLEAN NOT NULL DEFAULT TRUE,
    is_restricted BOOLEAN NOT NULL DEFAULT FALSE,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    closed BOOLEAN NOT NULL DEFAULT FALSE,
    is_tracked BOOLEAN NOT NULL DEFAULT FALSE,
    tracked_rank INTEGER,
    seed_captured_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bonus_markets_category ON bonus.markets (category);
CREATE INDEX IF NOT EXISTS idx_bonus_markets_tracked ON bonus.markets (is_tracked, tracked_rank);

CREATE TABLE IF NOT EXISTS bonus.accounts (
    account_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    risk_style TEXT NOT NULL,
    home_region TEXT NOT NULL DEFAULT 'US',
    base_currency TEXT NOT NULL DEFAULT 'USDC',
    starting_cash_usd NUMERIC(18, 2) NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bonus.market_ticks_1m (
    ts TIMESTAMPTZ NOT NULL,
    market_id TEXT NOT NULL REFERENCES bonus.markets (market_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    last_trade_price NUMERIC(10, 6) NOT NULL,
    midpoint_price NUMERIC(10, 6),
    best_bid NUMERIC(10, 6),
    best_ask NUMERIC(10, 6),
    spread NUMERIC(10, 6),
    liquidity_usd NUMERIC(18, 2),
    volume_usd_1m NUMERIC(18, 2) NOT NULL DEFAULT 0,
    trade_count_1m INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'generated',
    PRIMARY KEY (market_id, token_id, ts)
);

CREATE TABLE IF NOT EXISTS bonus.orderbook_topn_1m (
    ts TIMESTAMPTZ NOT NULL,
    market_id TEXT NOT NULL REFERENCES bonus.markets (market_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
    level_no SMALLINT NOT NULL CHECK (level_no BETWEEN 1 AND 5),
    price NUMERIC(10, 6) NOT NULL,
    size_shares NUMERIC(18, 4) NOT NULL,
    notional_usd NUMERIC(18, 2) NOT NULL,
    source TEXT NOT NULL DEFAULT 'generated',
    PRIMARY KEY (market_id, token_id, ts, side, level_no)
);

CREATE TABLE IF NOT EXISTS bonus.account_trades (
    executed_at TIMESTAMPTZ NOT NULL,
    trade_id TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES bonus.accounts (account_id) ON DELETE CASCADE,
    market_id TEXT NOT NULL REFERENCES bonus.markets (market_id) ON DELETE CASCADE,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price NUMERIC(10, 6) NOT NULL,
    size_shares NUMERIC(18, 4) NOT NULL,
    notional_usd NUMERIC(18, 2) NOT NULL,
    fee_usd NUMERIC(18, 4) NOT NULL DEFAULT 0,
    slippage_bps INTEGER NOT NULL DEFAULT 0,
    realized_pnl_usd NUMERIC(18, 2) NOT NULL DEFAULT 0,
    maker_taker TEXT NOT NULL DEFAULT 'TAKER',
    source TEXT NOT NULL DEFAULT 'generated',
    PRIMARY KEY (executed_at, trade_id)
);

CREATE TABLE IF NOT EXISTS bonus.account_positions_1h (
    ts TIMESTAMPTZ NOT NULL,
    account_id TEXT NOT NULL REFERENCES bonus.accounts (account_id) ON DELETE CASCADE,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    position_size NUMERIC(18, 4) NOT NULL DEFAULT 0,
    avg_entry_price NUMERIC(10, 6),
    mark_price NUMERIC(10, 6),
    cost_basis_usd NUMERIC(18, 2),
    market_value_usd NUMERIC(18, 2),
    realized_pnl_to_date_usd NUMERIC(18, 2),
    unrealized_pnl_usd NUMERIC(18, 2),
    cash_balance_usd NUMERIC(18, 2),
    equity_usd NUMERIC(18, 2),
    concentration_pct NUMERIC(10, 4),
    source TEXT NOT NULL DEFAULT 'generated',
    PRIMARY KEY (ts, account_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_bonus_position_rows_account_ts
    ON bonus.account_positions_1h (account_id, ts DESC);

CREATE TABLE IF NOT EXISTS bonus.dataset_runs (
    run_id TEXT PRIMARY KEY,
    days INTEGER NOT NULL,
    market_count INTEGER NOT NULL,
    tracked_market_count INTEGER NOT NULL,
    account_count INTEGER NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    notes TEXT
);
