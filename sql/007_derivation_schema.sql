-- Derivation layer — analytics-ready normalized tables from raw ingestion.

CREATE SCHEMA IF NOT EXISTS bonus;

-- --- wallet_trade_events ---
CREATE TABLE IF NOT EXISTS bonus.wallet_trade_events (
    event_ts TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    proxy_wallet TEXT NOT NULL,
    condition_id TEXT,
    asset TEXT,
    title TEXT,
    outcome TEXT,
    outcome_index SMALLINT,
    side TEXT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    size NUMERIC(38, 8) NOT NULL,
    notional_usd NUMERIC(24, 6),
    transaction_hash TEXT,
    slug TEXT,
    event_slug TEXT,
    derived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_ts, event_id)
);

CREATE INDEX IF NOT EXISTS idx_bonus_wte_wallet_ts
    ON bonus.wallet_trade_events (proxy_wallet, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_wte_condition_ts
    ON bonus.wallet_trade_events (condition_id, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_bonus_wte_side_ts
    ON bonus.wallet_trade_events (side, event_ts DESC);

SELECT create_hypertable(
    'bonus.wallet_trade_events',
    'event_ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- --- wallet_performance_daily ---
CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.wallet_performance_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', event_ts) AS day,
    proxy_wallet,
    COUNT(*) AS trade_count,
    COUNT(*) FILTER (WHERE side = 'BUY') AS buy_count,
    COUNT(*) FILTER (WHERE side = 'SELL') AS sell_count,
    SUM(notional_usd) AS volume_usd,
    SUM(notional_usd) FILTER (WHERE side = 'BUY') AS buy_volume_usd,
    SUM(notional_usd) FILTER (WHERE side = 'SELL') AS sell_volume_usd,
    AVG(price)::NUMERIC(18, 8) AS avg_price,
    COUNT(DISTINCT condition_id) AS distinct_markets,
    MIN(event_ts) AS first_trade_ts,
    MAX(event_ts) AS last_trade_ts
FROM bonus.wallet_trade_events
GROUP BY 1, 2
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_bonus_wpd_wallet_day
    ON bonus.wallet_performance_daily (proxy_wallet, day DESC);

-- --- v_wallet_summary ---
CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.v_wallet_summary AS
WITH trade_stats AS (
    SELECT
        proxy_wallet,
        COUNT(*) AS total_trades,
        COUNT(*) FILTER (WHERE side = 'BUY') AS total_buys,
        COUNT(*) FILTER (WHERE side = 'SELL') AS total_sells,
        SUM(notional_usd) AS total_volume_usd,
        COUNT(DISTINCT condition_id) AS markets_traded,
        MIN(event_ts) AS first_trade,
        MAX(event_ts) AS last_trade,
        COUNT(DISTINCT DATE_TRUNC('day', event_ts)) AS active_days
    FROM bonus.wallet_trade_events
    GROUP BY proxy_wallet
),
position_stats AS (
    SELECT DISTINCT ON (proxy_wallet)
        proxy_wallet,
        COUNT(*) OVER (PARTITION BY proxy_wallet) AS open_positions,
        SUM(current_value) OVER (PARTITION BY proxy_wallet) AS portfolio_value_usd
    FROM bonus.raw_wallet_positions_snapshots
    WHERE captured_at = (
        SELECT MAX(captured_at) FROM bonus.raw_wallet_positions_snapshots
    )
)
SELECT
    w.address,
    COALESCE(NULLIF(w.name, ''), NULLIF(w.user_name, ''), LEFT(w.address, 12) || '...') AS display_name,
    w.verified_badge,
    w.discovered_via,
    w.is_tracked,
    w.latest_leaderboard_rank,
    w.latest_leaderboard_pnl,
    w.latest_leaderboard_vol,
    w.created_at_polymarket,
    COALESCE(ts.total_trades, 0) AS total_trades,
    COALESCE(ts.total_buys, 0) AS total_buys,
    COALESCE(ts.total_sells, 0) AS total_sells,
    COALESCE(ts.total_volume_usd, 0) AS total_volume_usd,
    COALESCE(ts.markets_traded, 0) AS markets_traded,
    ts.first_trade,
    ts.last_trade,
    COALESCE(ts.active_days, 0) AS active_days,
    COALESCE(ps.open_positions, 0) AS open_positions,
    COALESCE(ps.portfolio_value_usd, 0) AS portfolio_value_usd,
    CASE
        WHEN ts.total_trades > 0 AND ts.active_days > 0
        THEN ROUND(ts.total_trades::NUMERIC / ts.active_days, 2)
        ELSE 0
    END AS avg_trades_per_day,
    CASE
        WHEN ts.total_volume_usd > 0 AND ts.total_trades > 0
        THEN ROUND(ts.total_volume_usd / ts.total_trades, 2)
        ELSE 0
    END AS avg_trade_size_usd
FROM bonus.wallets w
LEFT JOIN trade_stats ts ON ts.proxy_wallet = w.address
LEFT JOIN position_stats ps ON ps.proxy_wallet = w.address
WHERE w.is_tracked
   OR ts.total_trades > 0
   OR w.latest_leaderboard_pnl IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_bonus_v_wallet_summary_address
    ON bonus.v_wallet_summary (address);

-- --- v_market_flow ---
CREATE OR REPLACE VIEW bonus.v_market_flow AS
WITH trade_flow AS (
    SELECT
        t.condition_id,
        t.title,
        t.outcome,
        DATE_TRUNC('hour', t.event_ts) AS hour,
        COUNT(*) FILTER (WHERE t.side = 'BUY') AS buy_count,
        COUNT(*) FILTER (WHERE t.side = 'SELL') AS sell_count,
        COALESCE(SUM(t.notional_usd) FILTER (WHERE t.side = 'BUY'), 0) AS buy_volume_usd,
        COALESCE(SUM(t.notional_usd) FILTER (WHERE t.side = 'SELL'), 0) AS sell_volume_usd,
        COUNT(DISTINCT t.proxy_wallet) AS active_wallets,
        COUNT(DISTINCT t.proxy_wallet) FILTER (WHERE t.side = 'BUY') AS buyers,
        COUNT(DISTINCT t.proxy_wallet) FILTER (WHERE t.side = 'SELL') AS sellers
    FROM bonus.wallet_trade_events t
    WHERE t.condition_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT
    tf.*,
    (tf.buy_volume_usd - tf.sell_volume_usd) AS net_flow_usd,
    CASE
        WHEN (tf.buy_volume_usd + tf.sell_volume_usd) > 0
        THEN ROUND(
            ((tf.buy_volume_usd - tf.sell_volume_usd) / (tf.buy_volume_usd + tf.sell_volume_usd)) * 100,
            2
        )
        ELSE 0
    END AS flow_imbalance_pct
FROM trade_flow tf;

-- --- v_whale_concentration ---
CREATE OR REPLACE VIEW bonus.v_whale_concentration AS
WITH latest_holders AS (
    SELECT *
    FROM bonus.raw_market_holders_snapshots
    WHERE captured_at = (SELECT MAX(captured_at) FROM bonus.raw_market_holders_snapshots)
),
market_totals AS (
    SELECT
        market_condition_id,
        token_id,
        SUM(amount) AS total_amount,
        COUNT(*) AS holder_count
    FROM latest_holders
    WHERE amount > 0
    GROUP BY 1, 2
),
top_holders AS (
    SELECT
        h.market_condition_id,
        h.token_id,
        h.proxy_wallet,
        COALESCE(NULLIF(h.name, ''), LEFT(h.proxy_wallet, 10) || '...') AS display_name,
        h.amount,
        RANK() OVER (PARTITION BY h.market_condition_id, h.token_id ORDER BY h.amount DESC) AS holder_rank
    FROM latest_holders h
    WHERE h.amount > 0
)
SELECT
    m.question AS market,
    m.category,
    th.token_id,
    CASE th.token_id
        WHEN m.token_a THEN m.outcome_a
        WHEN m.token_b THEN m.outcome_b
        ELSE 'Unknown'
    END AS outcome,
    mt.holder_count,
    mt.total_amount::NUMERIC(24, 2) AS total_amount,
    th.display_name AS top_holder,
    th.amount::NUMERIC(24, 2) AS top_holder_amount,
    CASE
        WHEN mt.total_amount > 0
        THEN ROUND((th.amount / mt.total_amount) * 100, 2)
        ELSE 0
    END AS top_holder_pct,
    (
        SELECT ROUND((SUM(sub.amount) / mt.total_amount) * 100, 2)
        FROM top_holders sub
        WHERE sub.market_condition_id = th.market_condition_id
          AND sub.token_id = th.token_id
          AND sub.holder_rank <= 5
    ) AS top5_concentration_pct
FROM top_holders th
JOIN bonus.markets m ON m.market_id = th.market_condition_id
JOIN market_totals mt ON mt.market_condition_id = th.market_condition_id AND mt.token_id = th.token_id
WHERE th.holder_rank = 1
ORDER BY mt.total_amount DESC;
