#!/usr/bin/env python3
"""Derive wallet_trade_events from raw_wallet_activity_events."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

THIS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = THIS_DIR.parents[1]
sys.path.insert(0, str(PROJECT_DIR))

from etl.common import get_bonus_db_url  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("derive_trades")


DERIVE_SQL = """
INSERT INTO bonus.wallet_trade_events (
    event_ts, event_id, proxy_wallet,
    condition_id, asset, title, outcome, outcome_index,
    side, price, size, notional_usd,
    transaction_hash, slug, event_slug,
    derived_at
)
SELECT
    r.event_ts,
    r.event_id,
    r.proxy_wallet,
    r.condition_id,
    r.asset,
    r.title,
    r.outcome,
    r.outcome_index,
    r.side,
    r.price,
    r.size,
    COALESCE(r.usdc_size, r.price * r.size) AS notional_usd,
    r.transaction_hash,
    r.slug,
    r.event_slug,
    NOW()
FROM bonus.raw_wallet_activity_events r
WHERE r.activity_type = 'TRADE'
  AND r.side IS NOT NULL
  AND r.price IS NOT NULL
  AND r.size IS NOT NULL
ON CONFLICT (event_ts, event_id) DO NOTHING
"""


async def derive_wallet_trades(conn: asyncpg.Connection) -> dict:
    """Run the derivation query and return stats."""
    before = await conn.fetchval("SELECT COUNT(*) FROM bonus.wallet_trade_events")
    result = await conn.execute(DERIVE_SQL)
    after = await conn.fetchval("SELECT COUNT(*) FROM bonus.wallet_trade_events")
    newly_derived = after - before

    logger.info(
        "derive_wallet_trades: before=%d after=%d new=%d (%s)",
        before, after, newly_derived, result,
    )
    return {
        "before": before,
        "after": after,
        "newly_derived": newly_derived,
    }


async def main() -> None:
    dsn = get_bonus_db_url()
    conn = await asyncpg.connect(dsn)
    try:
        stats = await derive_wallet_trades(conn)
        print(f"wallet_trade_events: {stats['before']} → {stats['after']} (+{stats['newly_derived']})")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
