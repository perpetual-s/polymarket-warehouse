CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.market_ohlc_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '5 minutes', ts) AS bucket,
    market_id,
    token_id,
    outcome,
    first(last_trade_price, ts) AS open_price,
    MAX(last_trade_price) AS high_price,
    MIN(last_trade_price) AS low_price,
    last(last_trade_price, ts) AS close_price,
    AVG(midpoint_price) AS avg_midpoint_price,
    AVG(best_bid) AS avg_best_bid,
    AVG(best_ask) AS avg_best_ask,
    AVG(spread) AS avg_spread,
    SUM(volume_usd_1m) AS volume_usd,
    SUM(trade_count_1m) AS trade_count
FROM bonus.market_ticks_1m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_bonus_market_ohlc_5m_market_bucket
    ON bonus.market_ohlc_5m (market_id, token_id, bucket DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.market_spreads_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '5 minutes', ts) AS bucket,
    market_id,
    token_id,
    outcome,
    AVG(spread) AS avg_spread,
    MAX(spread) AS max_spread,
    AVG(midpoint_price) AS avg_midpoint_price,
    AVG(trade_count_1m)::NUMERIC(18, 4) AS avg_trade_count_1m
FROM bonus.market_ticks_1m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_bonus_market_spreads_5m_market_bucket
    ON bonus.market_spreads_5m (market_id, token_id, bucket DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.market_liquidity_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '5 minutes', ts) AS bucket,
    market_id,
    token_id,
    outcome,
    AVG(liquidity_usd) AS avg_liquidity_usd,
    AVG(best_bid) AS avg_best_bid,
    AVG(best_ask) AS avg_best_ask,
    AVG(spread) AS avg_spread,
    SUM(volume_usd_1m) AS volume_usd
FROM bonus.market_ticks_1m
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_bonus_market_liquidity_5m_market_bucket
    ON bonus.market_liquidity_5m (market_id, token_id, bucket DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.account_pnl_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', executed_at) AS bucket,
    account_id,
    SUM(realized_pnl_usd) AS realized_pnl_usd,
    SUM(notional_usd) AS traded_notional_usd,
    SUM(fee_usd) AS fees_usd,
    AVG(slippage_bps)::NUMERIC(18, 4) AS avg_slippage_bps,
    COUNT(*) AS trade_count,
    COUNT(*) FILTER (WHERE realized_pnl_usd > 0) AS winning_trade_count
FROM bonus.account_trades
GROUP BY 1, 2
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_bonus_account_pnl_daily_account_bucket
    ON bonus.account_pnl_daily (account_id, bucket DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS bonus.account_risk_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', ts) AS bucket,
    account_id,
    last(equity_usd, ts) AS closing_equity_usd,
    last(cash_balance_usd, ts) AS closing_cash_usd,
    last(realized_pnl_to_date_usd, ts) AS realized_pnl_to_date_usd,
    last(unrealized_pnl_usd, ts) AS closing_unrealized_pnl_usd,
    AVG(market_value_usd) AS avg_market_value_usd,
    MAX(concentration_pct) AS max_concentration_pct
FROM bonus.account_positions_1h
WHERE token_id = '__portfolio__'
GROUP BY 1, 2
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_bonus_account_risk_daily_account_bucket
    ON bonus.account_risk_daily (account_id, bucket DESC);
