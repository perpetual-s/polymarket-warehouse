"""Shared helpers for wallet ingestors."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, AsyncIterator, Optional
from uuid import UUID, uuid4

import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    """Mutable counters updated by the ingestor during a run."""

    rows_fetched: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@asynccontextmanager
async def collector_run(
    conn: asyncpg.Connection,
    ingestor: str,
    *,
    wallet_count: Optional[int] = None,
) -> AsyncIterator[tuple[UUID, RunStats]]:
    """Context manager that books a collector_runs row and closes it on exit.

    Also writes one collector_health sample at close time for Grafana.
    """
    run_id = uuid4()
    started_at = datetime.now(timezone.utc)
    stats = RunStats()

    await conn.execute(
        """
        INSERT INTO bonus.collector_runs (
            run_id, ingestor, started_at, status, wallet_count
        ) VALUES ($1, $2, $3, 'running', $4)
        """,
        run_id,
        ingestor,
        started_at,
        wallet_count,
    )

    try:
        yield run_id, stats
    except Exception as exc:  # noqa: BLE001
        finished_at = datetime.now(timezone.utc)
        error_msg = f"{type(exc).__name__}: {exc}"[:1000]
        await conn.execute(
            """
            UPDATE bonus.collector_runs
               SET finished_at = $2,
                   status = 'error',
                   rows_fetched = $3,
                   rows_inserted = $4,
                   rows_skipped = $5,
                   error_message = $6,
                   metadata = $7::jsonb
             WHERE run_id = $1
            """,
            run_id,
            finished_at,
            stats.rows_fetched,
            stats.rows_inserted,
            stats.rows_skipped,
            error_msg,
            json.dumps(stats.metadata),
        )
        await conn.execute(
            """
            INSERT INTO bonus.collector_health (
                ts, ingestor, status, rows_per_minute, lag_seconds, error_count, run_id
            ) VALUES ($1, $2, 'error', 0, 0, 1, $3)
            ON CONFLICT (ts, ingestor) DO NOTHING
            """,
            finished_at,
            ingestor,
            run_id,
        )
        logger.error("ingestor %s failed: %s", ingestor, error_msg)
        raise
    else:
        finished_at = datetime.now(timezone.utc)
        elapsed = max((finished_at - started_at).total_seconds(), 0.001)
        rpm = (stats.rows_inserted / elapsed) * 60.0
        # 'ok' on success. Dedupe skips (ON CONFLICT) are healthy and expected
        # — they should not flip the status. Use stats.metadata['parse_errors']
        # explicitly if an ingestor wants to signal partial failure.
        status = "partial" if stats.metadata.get("parse_errors", 0) > 0 else "ok"
        await conn.execute(
            """
            UPDATE bonus.collector_runs
               SET finished_at = $2,
                   status = $3,
                   rows_fetched = $4,
                   rows_inserted = $5,
                   rows_skipped = $6,
                   metadata = $7::jsonb
             WHERE run_id = $1
            """,
            run_id,
            finished_at,
            status,
            stats.rows_fetched,
            stats.rows_inserted,
            stats.rows_skipped,
            json.dumps(stats.metadata),
        )
        await conn.execute(
            """
            INSERT INTO bonus.collector_health (
                ts, ingestor, status, rows_per_minute, lag_seconds, error_count, run_id
            ) VALUES ($1, $2, $3, $4, $5, 0, $6)
            ON CONFLICT (ts, ingestor) DO NOTHING
            """,
            finished_at,
            ingestor,
            status,
            Decimal(f"{rpm:.4f}"),
            Decimal(f"{elapsed:.4f}"),
            run_id,
        )
        logger.info(
            "ingestor %s %s: fetched=%d inserted=%d skipped=%d elapsed=%.2fs",
            ingestor,
            status,
            stats.rows_fetched,
            stats.rows_inserted,
            stats.rows_skipped,
            elapsed,
        )


# --- value coercion helpers ---


def to_decimal(value: Any) -> Optional[Decimal]:
    """Coerce value to Decimal, returning None on failure."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def to_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "t")
    return None


def parse_unix_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        ts = int(value)
    except (ValueError, TypeError):
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def parse_iso_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        # try bare date
        try:
            return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def lower_address(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value.startswith("0x") or len(value) < 42:
        return None
    return value.lower()


# --- wallets upsert helper ---


async def upsert_wallet_from_leaderboard(
    conn: asyncpg.Connection,
    *,
    address: str,
    user_name: Optional[str],
    x_username: Optional[str],
    profile_image: Optional[str],
    verified_badge: Optional[bool],
    rank: Optional[int],
    pnl: Optional[Decimal],
    vol: Optional[Decimal],
) -> None:
    await conn.execute(
        """
        INSERT INTO bonus.wallets (
            address,
            user_name,
            x_username,
            profile_image,
            verified_badge,
            discovered_via,
            first_seen_at,
            last_seen_at,
            latest_leaderboard_rank,
            latest_leaderboard_pnl,
            latest_leaderboard_vol
        )
        VALUES ($1, $2, $3, $4, COALESCE($5, FALSE), 'leaderboard', NOW(), NOW(), $6, $7, $8)
        ON CONFLICT (address) DO UPDATE SET
            user_name = COALESCE(EXCLUDED.user_name, bonus.wallets.user_name),
            x_username = COALESCE(EXCLUDED.x_username, bonus.wallets.x_username),
            profile_image = COALESCE(EXCLUDED.profile_image, bonus.wallets.profile_image),
            verified_badge = COALESCE(EXCLUDED.verified_badge, bonus.wallets.verified_badge),
            last_seen_at = NOW(),
            latest_leaderboard_rank = EXCLUDED.latest_leaderboard_rank,
            latest_leaderboard_pnl = EXCLUDED.latest_leaderboard_pnl,
            latest_leaderboard_vol = EXCLUDED.latest_leaderboard_vol
        """,
        address,
        user_name,
        x_username,
        profile_image,
        verified_badge,
        rank,
        pnl,
        vol,
    )


async def enrich_wallet_from_profile(
    conn: asyncpg.Connection,
    *,
    address: str,
    name: Optional[str],
    bio: Optional[str],
    profile_image: Optional[str],
    verified_badge: Optional[bool],
    display_username_public: Optional[bool],
    created_at_polymarket: Optional[datetime],
) -> None:
    await conn.execute(
        """
        UPDATE bonus.wallets
           SET name = COALESCE($2, name),
               bio = COALESCE($3, bio),
               profile_image = COALESCE($4, profile_image),
               verified_badge = COALESCE($5, verified_badge),
               display_username_public = COALESCE($6, display_username_public),
               created_at_polymarket = COALESCE($7, created_at_polymarket),
               last_profile_refresh_at = NOW(),
               last_seen_at = NOW()
         WHERE address = $1
        """,
        address,
        name,
        bio,
        profile_image,
        verified_badge,
        display_username_public,
        created_at_polymarket,
    )


async def touch_wallet_positions(conn: asyncpg.Connection, address: str) -> None:
    await conn.execute(
        """
        UPDATE bonus.wallets
           SET last_positions_refresh_at = NOW(),
               last_seen_at = NOW()
         WHERE address = $1
        """,
        address,
    )


async def touch_wallet_activity(conn: asyncpg.Connection, address: str) -> None:
    await conn.execute(
        """
        UPDATE bonus.wallets
           SET last_activity_refresh_at = NOW(),
               last_seen_at = NOW()
         WHERE address = $1
        """,
        address,
    )
