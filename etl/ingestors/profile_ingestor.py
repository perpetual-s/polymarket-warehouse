"""Profile ingestor — fetches public profile per tracked wallet."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from etl.ingestors._base import (
    collector_run,
    enrich_wallet_from_profile,
    lower_address,
    parse_iso_ts,
    to_bool,
)
from etl.sources.polymarket_public import PolymarketPublicClient, PolymarketPublicError

logger = logging.getLogger(__name__)


async def ingest_profiles(
    conn: asyncpg.Connection,
    client: PolymarketPublicClient,
    wallet_addresses: list[str],
) -> dict[str, Any]:
    """Fetch public-profile for each supplied address."""
    async with collector_run(
        conn, "profile", wallet_count=len(wallet_addresses)
    ) as (run_id, stats):
        captured_at_base = datetime.now(timezone.utc)

        for address in wallet_addresses:
            normalized = lower_address(address)
            if not normalized:
                stats.rows_skipped += 1
                continue

            try:
                profile = await client.get_public_profile(normalized)
            except PolymarketPublicError as exc:
                logger.warning("profile fetch failed for %s: %s", normalized, exc)
                stats.rows_skipped += 1
                continue

            if not profile:
                stats.rows_skipped += 1
                continue

            stats.rows_fetched += 1
            captured_at = datetime.now(timezone.utc)
            created_at_polymarket = parse_iso_ts(profile.get("createdAt"))
            verified = to_bool(profile.get("verifiedBadge"))
            display_public = to_bool(profile.get("displayUsernamePublic"))

            async with conn.transaction():
                inserted = await conn.execute(
                    """
                    INSERT INTO bonus.raw_wallet_profiles (
                        captured_at, run_id, proxy_wallet,
                        name, bio, profile_image, verified_badge,
                        display_username_public, created_at_polymarket,
                        users, payload
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb)
                    ON CONFLICT (captured_at, proxy_wallet) DO NOTHING
                    """,
                    captured_at,
                    run_id,
                    normalized,
                    profile.get("name"),
                    profile.get("bio"),
                    profile.get("profileImage"),
                    verified,
                    display_public,
                    created_at_polymarket,
                    json.dumps(profile.get("users", [])),
                    json.dumps(profile),
                )
                # asyncpg returns a status string like 'INSERT 0 1'; count=1 means inserted
                if inserted.endswith(" 1"):
                    stats.rows_inserted += 1
                else:
                    stats.rows_skipped += 1

                await enrich_wallet_from_profile(
                    conn,
                    address=normalized,
                    name=profile.get("name"),
                    bio=profile.get("bio"),
                    profile_image=profile.get("profileImage"),
                    verified_badge=verified,
                    display_username_public=display_public,
                    created_at_polymarket=created_at_polymarket,
                )

        stats.metadata["captured_at_base"] = captured_at_base.isoformat()
        return {
            "run_id": str(run_id),
            "rows_fetched": stats.rows_fetched,
            "rows_inserted": stats.rows_inserted,
            "rows_skipped": stats.rows_skipped,
        }
