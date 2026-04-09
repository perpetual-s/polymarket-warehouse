"""Activity ingestor — paginates /activity per tracked wallet."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg

from etl.ingestors._base import (
    collector_run,
    lower_address,
    parse_unix_ts,
    to_decimal,
    to_int,
    touch_wallet_activity,
)
from etl.sources.polymarket_public import PolymarketPublicClient, PolymarketPublicError

logger = logging.getLogger(__name__)


def make_event_id(
    *,
    transaction_hash: Optional[str],
    asset: Optional[str],
    side: Optional[str],
    activity_type: str,
    outcome_index: Optional[int],
    timestamp: int,
) -> str:
    """Deterministic event_id: unique per (tx, asset, outcome_index, side)."""
    parts = [
        transaction_hash or "notx",
        asset or "noasset",
        str(outcome_index if outcome_index is not None else ""),
        (side or activity_type).upper(),
        str(timestamp),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:40]


async def ingest_activity(
    conn: asyncpg.Connection,
    client: PolymarketPublicClient,
    wallet_addresses: list[str],
    *,
    per_wallet_page_size: int = 500,
    per_wallet_max_pages: int = 4,
) -> dict[str, Any]:
    """Paginate activity for each wallet, dedupe by event_id."""
    async with collector_run(
        conn, "activity", wallet_count=len(wallet_addresses)
    ) as (run_id, stats):
        wallets_with_events = 0
        captured_at = datetime.now(timezone.utc)

        for address in wallet_addresses:
            normalized = lower_address(address)
            if not normalized:
                stats.rows_skipped += 1
                continue

            try:
                events = await client.get_activity_all(
                    normalized,
                    page_size=per_wallet_page_size,
                    max_pages=per_wallet_max_pages,
                )
            except (PolymarketPublicError, ValueError) as exc:
                logger.warning("activity fetch failed for %s: %s", normalized, exc)
                stats.rows_skipped += 1
                continue

            if not events:
                await touch_wallet_activity(conn, normalized)
                continue

            wallets_with_events += 1
            stats.rows_fetched += len(events)

            batch: list[tuple] = []
            for event in events:
                ts_raw = event.get("timestamp")
                event_ts = parse_unix_ts(ts_raw)
                if not event_ts:
                    stats.rows_skipped += 1
                    continue
                # Polymarket data-api returns camelCase
                activity_type = str(event.get("type") or "UNKNOWN").upper()
                side = event.get("side") or None  # empty string → None
                asset = event.get("asset")
                tx_hash = event.get("transactionHash") or event.get("transaction_hash")
                outcome_index = to_int(
                    event.get("outcomeIndex") or event.get("outcome_index")
                )
                event_id = make_event_id(
                    transaction_hash=tx_hash,
                    asset=asset,
                    side=side,
                    activity_type=activity_type,
                    outcome_index=outcome_index,
                    timestamp=int(ts_raw) if ts_raw is not None else 0,
                )
                batch.append(
                    (
                        event_ts,
                        event_id,
                        run_id,
                        captured_at,
                        normalized,
                        activity_type,
                        tx_hash,
                        event.get("conditionId") or event.get("condition_id"),
                        str(asset) if asset is not None else None,
                        event.get("title"),
                        event.get("slug"),
                        event.get("eventSlug") or event.get("event_slug"),
                        event.get("outcome"),
                        outcome_index,
                        side,
                        to_decimal(event.get("price")),
                        to_decimal(event.get("size")),
                        to_decimal(event.get("usdcSize") or event.get("usdc_size")),
                        json.dumps(event),
                    )
                )

            if not batch:
                await touch_wallet_activity(conn, normalized)
                continue

            async with conn.transaction():
                # count new inserts vs duplicates
                rows_before = await conn.fetchval(
                    "SELECT COUNT(*) FROM bonus.raw_wallet_activity_events WHERE proxy_wallet = $1",
                    normalized,
                )
                await conn.executemany(
                    """
                    INSERT INTO bonus.raw_wallet_activity_events (
                        event_ts, event_id, run_id, captured_at, proxy_wallet,
                        activity_type, transaction_hash, condition_id, asset,
                        title, slug, event_slug, outcome, outcome_index,
                        side, price, size, usdc_size, payload
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14, $15, $16, $17, $18, $19::jsonb
                    )
                    ON CONFLICT (event_ts, event_id) DO NOTHING
                    """,
                    batch,
                )
                rows_after = await conn.fetchval(
                    "SELECT COUNT(*) FROM bonus.raw_wallet_activity_events WHERE proxy_wallet = $1",
                    normalized,
                )
                newly_inserted = max(rows_after - rows_before, 0)
                stats.rows_inserted += newly_inserted
                stats.rows_skipped += max(len(batch) - newly_inserted, 0)
                await touch_wallet_activity(conn, normalized)

        stats.metadata["wallets_with_events"] = wallets_with_events
        return {
            "run_id": str(run_id),
            "rows_fetched": stats.rows_fetched,
            "rows_inserted": stats.rows_inserted,
            "rows_skipped": stats.rows_skipped,
            "wallets_with_events": wallets_with_events,
        }
