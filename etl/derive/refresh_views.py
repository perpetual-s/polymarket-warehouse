#!/usr/bin/env python3
"""Refresh all continuous aggregates and materialized views."""

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
logger = logging.getLogger("refresh_views")


CONTINUOUS_AGGREGATES = [
    "bonus.wallet_performance_daily",
    # market aggregates
    "bonus.market_ohlc_5m",
    "bonus.market_spreads_5m",
    "bonus.market_liquidity_5m",
    "bonus.account_pnl_daily",
    "bonus.account_risk_daily",
]

MATERIALIZED_VIEWS = [
    "bonus.v_wallet_summary",
]


async def refresh_all(conn: asyncpg.Connection) -> None:
    """Refresh continuous aggregates and materialized views."""
    for cagg in CONTINUOUS_AGGREGATES:
        try:
            logger.info("refreshing continuous aggregate: %s", cagg)
            await conn.execute(
                f"CALL refresh_continuous_aggregate('{cagg}', NULL, NULL)"
            )
        except Exception as exc:
            logger.warning("failed to refresh %s: %s", cagg, exc)

    for mv in MATERIALIZED_VIEWS:
        try:
            logger.info("refreshing materialized view: %s", mv)
            await conn.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
        except Exception as exc:
            # First refresh can't be concurrent
            try:
                await conn.execute(f"REFRESH MATERIALIZED VIEW {mv}")
                logger.info("refreshed %s (non-concurrent)", mv)
            except Exception as exc2:
                logger.warning("failed to refresh %s: %s", mv, exc2)

    # ANALYZE for query planner
    for table in [
        "bonus.wallet_trade_events",
        "bonus.wallet_performance_daily",
    ]:
        try:
            await conn.execute(f"ANALYZE {table}")
        except Exception:
            pass


async def main() -> None:
    dsn = get_bonus_db_url()
    conn = await asyncpg.connect(dsn)
    try:
        await refresh_all(conn)
        logger.info("all views refreshed")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
