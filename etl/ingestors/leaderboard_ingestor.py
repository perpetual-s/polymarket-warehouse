"""Leaderboard ingestor — seeds wallets dimension from the public leaderboard."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from etl.ingestors._base import (
    collector_run,
    lower_address,
    to_decimal,
    to_int,
    to_bool,
    upsert_wallet_from_leaderboard,
)
from etl.sources.polymarket_public import PolymarketPublicClient

logger = logging.getLogger(__name__)


async def ingest_leaderboard(
    conn: asyncpg.Connection,
    client: PolymarketPublicClient,
) -> dict[str, Any]:
    """Fetch leaderboard, write raw snapshots + upsert wallets."""
    async with collector_run(conn, "leaderboard") as (run_id, stats):
        captured_at = datetime.now(timezone.utc)
        leaders = await client.get_leaderboard()
        stats.rows_fetched = len(leaders)

        if not leaders:
            logger.warning("leaderboard returned no rows")
            return {"run_id": str(run_id), "rows_fetched": 0, "rows_inserted": 0}

        raw_rows = []
        wallet_payloads: list[dict[str, Any]] = []

        for leader in leaders:
            address = lower_address(leader.get("proxyWallet"))
            if not address:
                stats.rows_skipped += 1
                continue
            rank = to_int(leader.get("rank"))
            if rank is None:
                stats.rows_skipped += 1
                continue

            raw_rows.append(
                (
                    captured_at,
                    run_id,
                    rank,
                    address,
                    leader.get("userName"),
                    leader.get("xUsername"),
                    to_bool(leader.get("verifiedBadge")),
                    to_decimal(leader.get("vol")),
                    to_decimal(leader.get("pnl")),
                    leader.get("profileImage"),
                    json.dumps(leader),
                )
            )
            wallet_payloads.append(
                {
                    "address": address,
                    "user_name": leader.get("userName"),
                    "x_username": leader.get("xUsername"),
                    "profile_image": leader.get("profileImage"),
                    "verified_badge": to_bool(leader.get("verifiedBadge")),
                    "rank": rank,
                    "pnl": to_decimal(leader.get("pnl")),
                    "vol": to_decimal(leader.get("vol")),
                }
            )

        async with conn.transaction():
            if raw_rows:
                await conn.executemany(
                    """
                    INSERT INTO bonus.raw_wallet_leaderboard_snapshots (
                        captured_at, run_id, rank, proxy_wallet,
                        user_name, x_username, verified_badge,
                        vol, pnl, profile_image, payload
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                    ON CONFLICT (captured_at, rank, proxy_wallet) DO NOTHING
                    """,
                    raw_rows,
                )
            for payload in wallet_payloads:
                await upsert_wallet_from_leaderboard(conn, **payload)

        stats.rows_inserted = len(raw_rows)
        stats.metadata["distinct_wallets"] = len({p["address"] for p in wallet_payloads})

        return {
            "run_id": str(run_id),
            "rows_fetched": stats.rows_fetched,
            "rows_inserted": stats.rows_inserted,
            "rows_skipped": stats.rows_skipped,
            "distinct_wallets": stats.metadata["distinct_wallets"],
        }
