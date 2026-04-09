"""Positions ingestor — snapshots current positions per tracked wallet."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from etl.ingestors._base import (
    collector_run,
    lower_address,
    parse_iso_ts,
    to_bool,
    to_decimal,
    to_int,
    touch_wallet_positions,
)
from etl.sources.polymarket_public import PolymarketPublicClient, PolymarketPublicError

logger = logging.getLogger(__name__)


async def ingest_positions(
    conn: asyncpg.Connection,
    client: PolymarketPublicClient,
    wallet_addresses: list[str],
    *,
    per_wallet_limit: int = 500,
) -> dict[str, Any]:
    """Fetch positions per wallet, insert raw snapshot rows."""
    async with collector_run(
        conn, "positions", wallet_count=len(wallet_addresses)
    ) as (run_id, stats):
        captured_at = datetime.now(timezone.utc)
        wallets_with_positions = 0

        for address in wallet_addresses:
            normalized = lower_address(address)
            if not normalized:
                stats.rows_skipped += 1
                continue

            try:
                positions = await client.get_positions(
                    normalized, limit=per_wallet_limit
                )
            except (PolymarketPublicError, ValueError) as exc:
                logger.warning("positions fetch failed for %s: %s", normalized, exc)
                stats.rows_skipped += 1
                continue

            if not positions:
                await touch_wallet_positions(conn, normalized)
                continue

            wallets_with_positions += 1
            stats.rows_fetched += len(positions)
            batch: list[tuple] = []

            for pos in positions:
                # Polymarket data-api returns camelCase
                asset = pos.get("asset") or pos.get("tokenId") or pos.get("token_id")
                if not asset:
                    stats.rows_skipped += 1
                    continue
                batch.append(
                    (
                        captured_at,
                        run_id,
                        normalized,
                        str(asset),
                        pos.get("conditionId") or pos.get("condition_id"),
                        to_decimal(pos.get("size")),
                        to_decimal(pos.get("avgPrice") or pos.get("avg_price")),
                        to_decimal(pos.get("currentValue") or pos.get("current_value")),
                        to_decimal(pos.get("initialValue") or pos.get("initial_value")),
                        to_decimal(pos.get("curPrice") or pos.get("cur_price")),
                        to_decimal(pos.get("cashPnl") or pos.get("cash_pnl")),
                        to_decimal(pos.get("percentPnl") or pos.get("percent_pnl")),
                        to_decimal(pos.get("realizedPnl") or pos.get("realized_pnl")),
                        to_decimal(
                            pos.get("percentRealizedPnl") or pos.get("percent_realized_pnl")
                        ),
                        pos.get("title"),
                        pos.get("slug"),
                        pos.get("outcome"),
                        to_int(pos.get("outcomeIndex") or pos.get("outcome_index")),
                        pos.get("oppositeOutcome") or pos.get("opposite_outcome"),
                        parse_iso_ts(pos.get("endDate") or pos.get("end_date")),
                        to_bool(pos.get("redeemable")),
                        to_bool(pos.get("mergeable")),
                        to_bool(pos.get("negativeRisk") or pos.get("negative_risk")),
                        json.dumps(pos),
                    )
                )

            if not batch:
                await touch_wallet_positions(conn, normalized)
                continue

            async with conn.transaction():
                await conn.executemany(
                    """
                    INSERT INTO bonus.raw_wallet_positions_snapshots (
                        captured_at, run_id, proxy_wallet, asset,
                        condition_id, size, avg_price, current_value, initial_value,
                        cur_price, cash_pnl, percent_pnl, realized_pnl, percent_realized_pnl,
                        title, slug, outcome, outcome_index, opposite_outcome,
                        end_date, redeemable, mergeable, negative_risk, payload
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                        $20, $21, $22, $23, $24::jsonb
                    )
                    ON CONFLICT (captured_at, proxy_wallet, asset) DO NOTHING
                    """,
                    batch,
                )
                # Reflect latest portfolio total into wallets dimension
                total_value = sum(
                    (row[7] or 0) for row in batch  # current_value at index 7
                )
                await conn.execute(
                    """
                    UPDATE bonus.wallets
                       SET last_positions_refresh_at = NOW(),
                           last_seen_at = NOW(),
                           latest_portfolio_value_usd = $2
                     WHERE address = $1
                    """,
                    normalized,
                    total_value,
                )

            stats.rows_inserted += len(batch)

        stats.metadata["wallets_with_positions"] = wallets_with_positions
        return {
            "run_id": str(run_id),
            "rows_fetched": stats.rows_fetched,
            "rows_inserted": stats.rows_inserted,
            "rows_skipped": stats.rows_skipped,
            "wallets_with_positions": wallets_with_positions,
        }
