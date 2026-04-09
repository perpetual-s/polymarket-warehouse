CREATE OR REPLACE VIEW bonus.v_market_overview AS
WITH latest_ticks AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        market_id,
        outcome,
        ts,
        last_trade_price,
        midpoint_price,
        best_bid,
        best_ask,
        spread,
        liquidity_usd
    FROM bonus.market_ticks_1m
    ORDER BY token_id, ts DESC
),
first_24h AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        close_price
    FROM bonus.market_ohlc_5m
    WHERE bucket >= NOW() - INTERVAL '24 hours'
    ORDER BY token_id, bucket ASC
),
last_24h AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        close_price
    FROM bonus.market_ohlc_5m
    WHERE bucket >= NOW() - INTERVAL '24 hours'
    ORDER BY token_id, bucket DESC
),
volume_24h AS (
    SELECT
        market_id,
        SUM(volume_usd) AS generated_volume_24h_usd
    FROM bonus.market_ohlc_5m
    WHERE bucket >= NOW() - INTERVAL '24 hours'
    GROUP BY market_id
)
SELECT
    m.market_id,
    m.question,
    m.category,
    m.slug,
    m.group_item_title,
    m.end_date,
    m.is_tracked,
    m.tracked_rank,
    m.outcome_a,
    m.outcome_b,
    COALESCE(la.last_trade_price, m.outcome_price_a_live) AS price_a,
    COALESCE(lb.last_trade_price, m.outcome_price_b_live) AS price_b,
    COALESCE(la.spread, m.spread_live) AS spread_a,
    COALESCE(lb.spread, m.spread_live) AS spread_b,
    COALESCE(la.liquidity_usd, m.liquidity_live_usd) AS liquidity_a_usd,
    COALESCE(lb.liquidity_usd, m.liquidity_live_usd) AS liquidity_b_usd,
    COALESCE(v.generated_volume_24h_usd, 0) AS generated_volume_24h_usd,
    CASE
        WHEN COALESCE(l24a.close_price, f24a.close_price) IS NULL THEN NULL
        WHEN f24a.close_price = 0 THEN NULL
        ELSE ROUND(((l24a.close_price - f24a.close_price) / f24a.close_price) * 100, 4)
    END AS price_change_24h_pct,
    CASE
        WHEN COALESCE(la.last_trade_price, m.outcome_price_a_live, 0) >= COALESCE(lb.last_trade_price, m.outcome_price_b_live, 0)
            THEN m.outcome_a
        ELSE m.outcome_b
    END AS leading_outcome,
    GREATEST(
        COALESCE(la.last_trade_price, m.outcome_price_a_live, 0),
        COALESCE(lb.last_trade_price, m.outcome_price_b_live, 0)
    ) AS leading_price
FROM bonus.markets m
LEFT JOIN latest_ticks la ON la.token_id = m.token_a
LEFT JOIN latest_ticks lb ON lb.token_id = m.token_b
LEFT JOIN first_24h f24a ON f24a.token_id = m.token_a
LEFT JOIN last_24h l24a ON l24a.token_id = m.token_a
LEFT JOIN volume_24h v ON v.market_id = m.market_id;

CREATE OR REPLACE VIEW bonus.v_market_detail_latest AS
WITH latest_ticks AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        market_id,
        outcome,
        ts,
        last_trade_price,
        midpoint_price,
        best_bid,
        best_ask,
        spread,
        liquidity_usd
    FROM bonus.market_ticks_1m
    ORDER BY token_id, ts DESC
),
latest_depth AS (
    SELECT DISTINCT ON (market_id, token_id, side, level_no)
        market_id,
        token_id,
        outcome,
        side,
        level_no,
        ts,
        price,
        size_shares,
        notional_usd
    FROM bonus.orderbook_topn_1m
    ORDER BY market_id, token_id, side, level_no, ts DESC
),
depth_summary AS (
    SELECT
        market_id,
        token_id,
        outcome,
        SUM(size_shares) FILTER (WHERE side = 'bid') AS bid_depth_top5_shares,
        SUM(size_shares) FILTER (WHERE side = 'ask') AS ask_depth_top5_shares,
        SUM(notional_usd) FILTER (WHERE side = 'bid') AS bid_depth_top5_usd,
        SUM(notional_usd) FILTER (WHERE side = 'ask') AS ask_depth_top5_usd
    FROM latest_depth
    GROUP BY 1, 2, 3
)
SELECT
    t.market_id,
    m.question,
    m.category,
    t.token_id,
    t.outcome,
    t.ts AS last_tick_at,
    t.last_trade_price,
    t.midpoint_price,
    t.best_bid,
    t.best_ask,
    t.spread,
    t.liquidity_usd,
    d.bid_depth_top5_shares,
    d.ask_depth_top5_shares,
    d.bid_depth_top5_usd,
    d.ask_depth_top5_usd
FROM latest_ticks t
JOIN bonus.markets m USING (market_id)
LEFT JOIN depth_summary d
    ON d.market_id = t.market_id
   AND d.token_id = t.token_id
   AND d.outcome = t.outcome;

CREATE OR REPLACE VIEW bonus.v_trade_activity AS
SELECT
    t.executed_at,
    DATE_TRUNC('day', t.executed_at) AS trade_day,
    DATE_TRUNC('hour', t.executed_at) AS trade_hour,
    t.trade_id,
    t.account_id,
    a.display_name,
    a.risk_style,
    t.market_id,
    m.question,
    m.category,
    t.token_id,
    t.outcome,
    t.side,
    t.price,
    t.size_shares,
    t.notional_usd,
    t.fee_usd,
    t.slippage_bps,
    t.realized_pnl_usd,
    t.maker_taker,
    t.source
FROM bonus.account_trades t
JOIN bonus.accounts a USING (account_id)
JOIN bonus.markets m USING (market_id);

CREATE OR REPLACE VIEW bonus.v_pnl_risk AS
WITH combined AS (
    SELECT
        COALESCE(p.bucket, r.bucket) AS day,
        COALESCE(p.account_id, r.account_id) AS account_id,
        p.realized_pnl_usd,
        p.traded_notional_usd,
        p.fees_usd,
        p.avg_slippage_bps,
        p.trade_count,
        p.winning_trade_count,
        r.closing_equity_usd,
        r.closing_cash_usd,
        r.realized_pnl_to_date_usd,
        r.closing_unrealized_pnl_usd,
        r.avg_market_value_usd,
        r.max_concentration_pct
    FROM bonus.account_pnl_daily p
    FULL OUTER JOIN bonus.account_risk_daily r
        ON p.bucket = r.bucket
       AND p.account_id = r.account_id
),
with_peaks AS (
    SELECT
        c.*,
        MAX(closing_equity_usd) OVER (
            PARTITION BY account_id
            ORDER BY day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_peak_equity
    FROM combined c
)
SELECT
    w.day,
    w.account_id,
    a.display_name,
    a.risk_style,
    COALESCE(w.realized_pnl_usd, 0) AS realized_pnl_usd,
    COALESCE(w.traded_notional_usd, 0) AS traded_notional_usd,
    COALESCE(w.fees_usd, 0) AS fees_usd,
    w.avg_slippage_bps,
    COALESCE(w.trade_count, 0) AS trade_count,
    COALESCE(w.winning_trade_count, 0) AS winning_trade_count,
    CASE
        WHEN COALESCE(w.trade_count, 0) = 0 THEN NULL
        ELSE ROUND((w.winning_trade_count::NUMERIC / w.trade_count::NUMERIC) * 100, 4)
    END AS win_rate_pct,
    w.closing_equity_usd,
    w.closing_cash_usd,
    w.realized_pnl_to_date_usd,
    w.closing_unrealized_pnl_usd,
    w.avg_market_value_usd,
    w.max_concentration_pct,
    CASE
        WHEN COALESCE(w.running_peak_equity, 0) = 0 THEN NULL
        ELSE ROUND(((w.closing_equity_usd - w.running_peak_equity) / w.running_peak_equity) * 100, 4)
    END AS drawdown_pct
FROM with_peaks w
JOIN bonus.accounts a USING (account_id);
