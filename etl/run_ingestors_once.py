#!/usr/bin/env python3
"""Pipeline orchestrator — runs all ingestors once end-to-end."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

THIS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = THIS_DIR.parent
REPO_DIR = PROJECT_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))
sys.path.insert(0, str(REPO_DIR))

from etl.common import get_bonus_db_url  # noqa: E402
from etl.derive.derive_wallet_trades import derive_wallet_trades  # noqa: E402
from etl.derive.refresh_views import refresh_all  # noqa: E402
from etl.ingestors.activity_ingestor import ingest_activity  # noqa: E402
from etl.ingestors.holders_ingestor import ingest_holders  # noqa: E402
from etl.ingestors.leaderboard_ingestor import ingest_leaderboard  # noqa: E402
from etl.ingestors.pma_traders_ingestor import ingest_pma_traders  # noqa: E402
from etl.ingestors.positions_ingestor import ingest_positions  # noqa: E402
from etl.ingestors.profile_ingestor import ingest_profiles  # noqa: E402
from etl.sources.polymarket_public import PolymarketPublicClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")


async def promote_top_wallets(
    conn: asyncpg.Connection, tracked_count: int
) -> list[str]:
    """Mark exactly top N wallets by leaderboard pnl as is_tracked."""
    await conn.execute(
        """
        UPDATE bonus.wallets
           SET is_tracked = FALSE,
               priority = 0
         WHERE priority = 100
        """
    )
    await conn.execute(
        """
        UPDATE bonus.wallets
           SET is_tracked = TRUE,
               tracked_since = COALESCE(tracked_since, NOW()),
               priority = 100
         WHERE address IN (
            SELECT address
              FROM bonus.wallets
             WHERE latest_leaderboard_pnl IS NOT NULL
             ORDER BY latest_leaderboard_pnl DESC NULLS LAST
             LIMIT $1
         )
        """,
        tracked_count,
    )
    rows = await conn.fetch(
        """
        SELECT address
          FROM bonus.wallets
         WHERE is_tracked = TRUE
         ORDER BY latest_leaderboard_pnl DESC NULLS LAST
         LIMIT $1
        """,
        tracked_count,
    )
    return [row["address"] for row in rows]


async def run_pipeline(
    tracked_count: int,
    pma_max: int,
    holders_all_markets: bool,
    activity_max_pages: int,
) -> None:
    dsn = get_bonus_db_url()
    logger.info("connecting to %s", dsn)
    conn = await asyncpg.connect(dsn)
    try:
        async with PolymarketPublicClient() as client:
            logger.info("pipeline starting")

            # Step 1: leaderboard → seed wallets dimension
            leaderboard_result = await ingest_leaderboard(conn, client)
            logger.info("leaderboard result: %s", leaderboard_result)

            # Step 2: holders → discover wallets from markets
            if holders_all_markets:
                market_query = "SELECT market_id FROM bonus.markets WHERE active ORDER BY volume_total_usd DESC"
            else:
                market_query = "SELECT market_id FROM bonus.markets WHERE is_tracked ORDER BY tracked_rank NULLS LAST"
            tracked_market_ids = [
                row["market_id"] for row in await conn.fetch(market_query)
            ]
            if tracked_market_ids:
                holders_result = await ingest_holders(conn, client, tracked_market_ids)
                logger.info("holders result: %s", holders_result)
            else:
                logger.warning("no tracked markets in bonus.markets — skipping holders")

            # Step 2b: PMA trader intelligence
            pma_result = await ingest_pma_traders(
                conn, client, max_traders=pma_max, page_size=1000
            )
            logger.info("PMA traders result: %s", pma_result)

            # Step 3: mark top N as tracked
            tracked_addresses = await promote_top_wallets(conn, tracked_count)
            logger.info(
                "promoted %d wallets to is_tracked (requested %d)",
                len(tracked_addresses),
                tracked_count,
            )
            if not tracked_addresses:
                logger.error("no tracked wallets — aborting downstream ingestors")
                return

            # Step 3: profile per tracked wallet
            profile_result = await ingest_profiles(conn, client, tracked_addresses)
            logger.info("profile result: %s", profile_result)

            # Step 4: positions per tracked wallet
            positions_result = await ingest_positions(conn, client, tracked_addresses)
            logger.info("positions result: %s", positions_result)

            # Step 5: activity per tracked wallet (deeper pagination)
            activity_result = await ingest_activity(
                conn, client, tracked_addresses,
                per_wallet_max_pages=activity_max_pages,
            )
            logger.info("activity result: %s", activity_result)

            logger.info("ingestion complete, running derivation")

            # Step 7: derive wallet_trade_events from raw activity
            trade_stats = await derive_wallet_trades(conn)
            logger.info("derive_wallet_trades: %s", trade_stats)

            # Step 8: refresh continuous aggregates + materialized views
            await refresh_all(conn)
            logger.info("views refreshed")

            logger.info("pipeline complete")

            # Final summary query
            summary = await conn.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM bonus.wallets) AS wallets_total,
                    (SELECT COUNT(*) FROM bonus.wallets WHERE is_tracked) AS wallets_tracked,
                    (SELECT COUNT(*) FROM bonus.raw_wallet_leaderboard_snapshots) AS raw_leaderboard,
                    (SELECT COUNT(*) FROM bonus.raw_market_holders_snapshots) AS raw_holders,
                    (SELECT COUNT(*) FROM bonus.raw_wallet_profiles) AS raw_profiles,
                    (SELECT COUNT(*) FROM bonus.raw_wallet_positions_snapshots) AS raw_positions,
                    (SELECT COUNT(*) FROM bonus.raw_wallet_activity_events) AS raw_activity,
                    (SELECT COUNT(*) FROM bonus.raw_pma_trader_snapshots) AS raw_pma_traders,
                    (SELECT COUNT(*) FROM bonus.wallet_trade_events) AS derived_trades,
                    (SELECT COUNT(*) FROM bonus.wallet_performance_daily) AS perf_daily_rows,
                    (SELECT COUNT(*) FROM bonus.collector_runs) AS collector_runs,
                    (SELECT COUNT(*) FROM bonus.collector_health) AS collector_health
                """
            )
            print("\n" + "=" * 60)
            print("Pipeline summary")
            print("=" * 60)
            for key, value in summary.items():
                print(f"  {key:<25} {value}")
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tracked-count",
        type=int,
        default=20,
        help="How many top wallets to track (default: 20)",
    )
    parser.add_argument(
        "--pma-max",
        type=int,
        default=100000,
        help="Max PMA traders to fetch (default: 100000, use 2600000 for all)",
    )
    parser.add_argument(
        "--holders-all-markets",
        action="store_true",
        help="Scan ALL markets for holders, not just tracked",
    )
    parser.add_argument(
        "--activity-max-pages",
        type=int,
        default=4,
        help="Max activity pages per wallet (default: 4, use 10+ for deeper history)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_pipeline(
        tracked_count=args.tracked_count,
        pma_max=args.pma_max,
        holders_all_markets=args.holders_all_markets,
        activity_max_pages=args.activity_max_pages,
    ))


if __name__ == "__main__":
    main()
