"""PolymarketAnalytics traders ingestor — bulk trader intelligence."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg

from etl.ingestors._base import (
    collector_run,
    lower_address,
    to_bool,
    to_decimal,
    to_int,
)
import aiohttp

from etl.sources.polymarket_public import PolymarketPublicClient

logger = logging.getLogger(__name__)

PMA_TRADERS_URL = "https://polymarketanalytics.com/api/traders-tag-performance"
PMA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Referer": "https://polymarketanalytics.com/traders",
    "Accept": "*/*",
}
PMA_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def _fetch_pma_page(
    session: aiohttp.ClientSession,
    *,
    tag: str = "Overall",
    limit: int = 1000,
    offset: int = 0,
    sort_column: str = "rank",
    sort_direction: str = "ASC",
) -> dict[str, Any]:
    """Fetch one page from PMA traders API with browser-like headers."""
    params = {
        "tag": tag,
        "sortColumn": sort_column,
        "sortDirection": sort_direction,
        "limit": limit,
        "offset": offset,
        "minTotalPositions": 1,
        "maxTotalPositions": 999999,
        "minPnL": -999999999,
        "maxPnL": 999999999,
        "minActivePositions": 0,
        "maxActivePositions": 999999,
        "minWinAmount": 0,
        "maxWinAmount": 999999999,
        "minLossAmount": -999999999,
        "maxLossAmount": 0,
        "minWinRate": 0,
        "maxWinRate": 100,
        "minCurrentValue": 0,
        "maxCurrentValue": 999999999999,
    }
    for attempt in range(3):
        try:
            async with session.get(PMA_TRADERS_URL, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status in (429, 500, 502, 503):
                    await asyncio.sleep(2 ** attempt)
                    continue
                text = (await resp.text())[:200]
                logger.warning("PMA %d at offset=%d: %s", resp.status, offset, text)
                return {"data": [], "totalCount": 0}
        except aiohttp.ClientError as exc:
            if attempt == 2:
                logger.warning("PMA client error at offset=%d: %s", offset, exc)
                return {"data": [], "totalCount": 0}
            await asyncio.sleep(2 ** attempt)
    return {"data": [], "totalCount": 0}


async def ingest_pma_traders(
    conn: asyncpg.Connection,
    client: PolymarketPublicClient,
    *,
    max_traders: int = 100000,
    page_size: int = 1000,
) -> dict[str, Any]:
    """Paginate PMA traders and bulk insert."""
    max_pages = (max_traders + page_size - 1) // page_size

    async with collector_run(conn, "pma_traders") as (run_id, stats):
        captured_at = datetime.now(timezone.utc)
        all_wallets: set[str] = set()
        total_count: Optional[int] = None

        async with aiohttp.ClientSession(
            headers=PMA_HEADERS, timeout=PMA_TIMEOUT
        ) as session:
         for page_idx in range(max_pages):
            offset = page_idx * page_size
            try:
                response = await _fetch_pma_page(
                    session, limit=page_size, offset=offset
                )
            except Exception as exc:
                logger.warning("PMA page %d failed: %s", page_idx, exc)
                stats.rows_skipped += page_size
                continue

            rows = response.get("data", [])
            if total_count is None:
                total_count = response.get("totalCount", 0)
                stats.metadata["total_available"] = total_count

            if not rows:
                break

            stats.rows_fetched += len(rows)
            batch: list[tuple] = []
            wallet_batch: list[dict[str, Any]] = []

            for trader in rows:
                address = lower_address(trader.get("trader"))
                if not address:
                    stats.rows_skipped += 1
                    continue

                rank = to_int(trader.get("rank"))
                if rank is None:
                    stats.rows_skipped += 1
                    continue

                batch.append((
                    captured_at,
                    run_id,
                    rank,
                    address,
                    trader.get("trader_name"),
                    trader.get("tag", "Overall"),
                    to_decimal(trader.get("overall_gain")),
                    to_decimal(trader.get("win_amount")),
                    to_decimal(trader.get("loss_amount")),
                    to_decimal(trader.get("win_rate")),
                    to_int(trader.get("win_count")),
                    to_int(trader.get("total_positions")),
                    to_int(trader.get("active_positions")),
                    to_decimal(trader.get("total_current_value")),
                    to_int(trader.get("event_ct")),
                    trader.get("trader_tags"),
                    json.dumps(trader),
                ))

                if address not in all_wallets:
                    all_wallets.add(address)
                    wallet_batch.append({
                        "address": address,
                        "name": trader.get("trader_name"),
                        "pnl": to_decimal(trader.get("overall_gain")),
                        "vol": to_decimal(trader.get("win_amount")),
                        "rank": rank,
                    })

            if batch:
                async with conn.transaction():
                    await conn.executemany(
                        """
                        INSERT INTO bonus.raw_pma_trader_snapshots (
                            captured_at, run_id, rank, trader_address, trader_name,
                            tag, overall_gain, win_amount, loss_amount, win_rate,
                            win_count, total_positions, active_positions,
                            total_current_value, event_count, trader_tags, payload
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17::jsonb
                        )
                        ON CONFLICT (captured_at, rank, trader_address) DO NOTHING
                        """,
                        batch,
                    )
                    stats.rows_inserted += len(batch)

                    for wallet in wallet_batch:
                        await conn.execute(
                            """
                            INSERT INTO bonus.wallets (
                                address, name, discovered_via,
                                first_seen_at, last_seen_at,
                                latest_leaderboard_rank,
                                latest_leaderboard_pnl,
                                latest_leaderboard_vol
                            )
                            VALUES ($1, $2, 'pma', NOW(), NOW(), $3, $4, $5)
                            ON CONFLICT (address) DO UPDATE SET
                                name = COALESCE(NULLIF(EXCLUDED.name, ''), bonus.wallets.name),
                                last_seen_at = NOW(),
                                latest_leaderboard_rank = LEAST(EXCLUDED.latest_leaderboard_rank, bonus.wallets.latest_leaderboard_rank),
                                latest_leaderboard_pnl = GREATEST(EXCLUDED.latest_leaderboard_pnl, bonus.wallets.latest_leaderboard_pnl),
                                latest_leaderboard_vol = GREATEST(EXCLUDED.latest_leaderboard_vol, bonus.wallets.latest_leaderboard_vol)
                            """,
                            wallet["address"],
                            wallet["name"],
                            wallet["rank"],
                            wallet["pnl"],
                            wallet["vol"],
                        )

            if (page_idx + 1) % 10 == 0:
                logger.info(
                    "PMA progress: page %d/%d, fetched=%d, inserted=%d",
                    page_idx + 1, max_pages, stats.rows_fetched, stats.rows_inserted,
                )

            # Pace requests: ~5/s to avoid Vercel rate limits
            await asyncio.sleep(0.2)

        stats.metadata["distinct_wallets"] = len(all_wallets)
        stats.metadata["pages_fetched"] = (stats.rows_fetched + page_size - 1) // page_size

        return {
            "run_id": str(run_id),
            "rows_fetched": stats.rows_fetched,
            "rows_inserted": stats.rows_inserted,
            "rows_skipped": stats.rows_skipped,
            "distinct_wallets": len(all_wallets),
            "total_available": total_count,
        }
