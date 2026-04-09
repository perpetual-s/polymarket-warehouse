"""Holders ingestor — discovers wallets from tracked market holder lists."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from etl.ingestors._base import (
    collector_run,
    lower_address,
    to_bool,
    to_decimal,
    to_int,
)
from etl.sources.polymarket_public import PolymarketPublicClient, PolymarketPublicError

logger = logging.getLogger(__name__)


async def ingest_holders(
    conn: asyncpg.Connection,
    client: PolymarketPublicClient,
    market_condition_ids: list[str],
    *,
    per_market_limit: int = 500,
    min_balance: int = 1,
) -> dict[str, Any]:
    """Fetch holders for each market, flatten, write raw rows + upsert wallets."""
    async with collector_run(
        conn, "holders", wallet_count=None
    ) as (run_id, stats):
        captured_at = datetime.now(timezone.utc)
        all_wallets_seen: set[str] = set()
        markets_with_holders = 0

        for condition_id in market_condition_ids:
            try:
                token_groups = await client.get_holders(
                    condition_id,
                    limit=per_market_limit,
                    min_balance=min_balance,
                )
            except (PolymarketPublicError, ValueError) as exc:
                logger.warning("holders fetch failed for %s: %s", condition_id[:16], exc)
                stats.rows_skipped += 1
                continue

            if not token_groups:
                continue

            markets_with_holders += 1
            batch: list[tuple] = []
            wallet_upserts: list[dict[str, Any]] = []

            for group in token_groups:
                token_id = str(group.get("token", ""))
                holders_list = group.get("holders", [])
                if not holders_list:
                    continue

                stats.rows_fetched += len(holders_list)

                for holder in holders_list:
                    address = lower_address(holder.get("proxyWallet"))
                    if not address:
                        stats.rows_skipped += 1
                        continue

                    batch.append(
                        (
                            captured_at,
                            run_id,
                            condition_id,
                            token_id,
                            address,
                            to_decimal(holder.get("amount")),
                            to_int(holder.get("outcomeIndex")),
                            holder.get("name"),
                            holder.get("pseudonym"),
                            holder.get("bio"),
                            holder.get("profileImage"),
                            to_bool(holder.get("displayUsernamePublic")),
                            to_bool(holder.get("verified")),
                            json.dumps(holder),
                        )
                    )

                    if address not in all_wallets_seen:
                        all_wallets_seen.add(address)
                        wallet_upserts.append(
                            {
                                "address": address,
                                "name": holder.get("name"),
                                "pseudonym": holder.get("pseudonym"),
                                "bio": holder.get("bio"),
                                "profile_image": holder.get("profileImage"),
                                "verified_badge": to_bool(holder.get("verified")),
                                "display_username_public": to_bool(
                                    holder.get("displayUsernamePublic")
                                ),
                            }
                        )

            if not batch:
                continue

            async with conn.transaction():
                await conn.executemany(
                    """
                    INSERT INTO bonus.raw_market_holders_snapshots (
                        captured_at, run_id, market_condition_id, token_id,
                        proxy_wallet, amount, outcome_index,
                        name, pseudonym, bio, profile_image,
                        display_username_public, verified, payload
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7,
                        $8, $9, $10, $11, $12, $13, $14::jsonb
                    )
                    ON CONFLICT (captured_at, market_condition_id, token_id, proxy_wallet)
                    DO NOTHING
                    """,
                    batch,
                )
                stats.rows_inserted += len(batch)

                for wallet in wallet_upserts:
                    await conn.execute(
                        """
                        INSERT INTO bonus.wallets (
                            address, name, pseudonym, bio,
                            profile_image, verified_badge, display_username_public,
                            discovered_via, first_seen_at, last_seen_at
                        )
                        VALUES ($1, $2, $3, $4, $5, COALESCE($6, FALSE), $7,
                                'holders', NOW(), NOW())
                        ON CONFLICT (address) DO UPDATE SET
                            name = COALESCE(NULLIF(EXCLUDED.name, ''), bonus.wallets.name),
                            pseudonym = COALESCE(NULLIF(EXCLUDED.pseudonym, ''), bonus.wallets.pseudonym),
                            bio = COALESCE(NULLIF(EXCLUDED.bio, ''), bonus.wallets.bio),
                            profile_image = COALESCE(NULLIF(EXCLUDED.profile_image, ''), bonus.wallets.profile_image),
                            verified_badge = COALESCE(EXCLUDED.verified_badge, bonus.wallets.verified_badge),
                            display_username_public = COALESCE(EXCLUDED.display_username_public, bonus.wallets.display_username_public),
                            last_seen_at = NOW()
                        """,
                        wallet["address"],
                        wallet["name"],
                        wallet["pseudonym"],
                        wallet["bio"],
                        wallet["profile_image"],
                        wallet["verified_badge"],
                        wallet["display_username_public"],
                    )

        stats.metadata["markets_queried"] = len(market_condition_ids)
        stats.metadata["markets_with_holders"] = markets_with_holders
        stats.metadata["distinct_wallets_seen"] = len(all_wallets_seen)

        return {
            "run_id": str(run_id),
            "rows_fetched": stats.rows_fetched,
            "rows_inserted": stats.rows_inserted,
            "rows_skipped": stats.rows_skipped,
            "markets_queried": len(market_condition_ids),
            "markets_with_holders": markets_with_holders,
            "distinct_wallets_seen": len(all_wallets_seen),
        }
