#!/usr/bin/env python3
"""Load generated Sparta DB bonus data into TimescaleDB."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from pathlib import Path
import sys

import asyncpg

THIS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = THIS_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from etl.common import (
    PATHS,
    ROOT,
    get_bonus_db_url,
    load_sql,
    read_json,
    SQL_DIR,
)
from etl.generate_history import (  # noqa: E402
    ORDERBOOK_COLUMNS,
    POSITION_COLUMNS,
    TICK_COLUMNS,
    TRADE_COLUMNS,
)


SQL_FILES = [
    SQL_DIR / "001_schema.sql",
    SQL_DIR / "002_timescale.sql",
    SQL_DIR / "003_continuous_aggregates.sql",
    SQL_DIR / "004_views.sql",
]


def parse_optional_timestamp(value: str | None) -> datetime | None:
    """Convert ISO-8601 strings from JSON artifacts into datetimes."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def wait_for_db(dsn: str, timeout_seconds: int) -> None:
    """Wait for Docker TimescaleDB to accept connections."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        try:
            conn = await asyncpg.connect(dsn)
        except Exception:
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(2)
            continue
        await conn.close()
        return


async def apply_sql_files(conn: asyncpg.Connection) -> None:
    """Apply schema and view SQL files."""
    for path in SQL_FILES:
        print(f"Applying {path.name}")
        await conn.execute(load_sql(path))


async def load_dimensions(conn: asyncpg.Connection) -> None:
    """Insert dimension tables from JSON artifacts."""
    markets_payload = read_json(PATHS.markets)
    accounts_payload = read_json(PATHS.accounts)
    summary = read_json(PATHS.generation_summary)

    await conn.execute(
        """
        TRUNCATE TABLE
            bonus.account_positions_1h,
            bonus.account_trades,
            bonus.orderbook_topn_1m,
            bonus.market_ticks_1m,
            bonus.accounts,
            bonus.markets,
            bonus.dataset_runs
        """
    )

    await conn.executemany(
        """
        INSERT INTO bonus.markets (
            market_id,
            pelion_market_uuid,
            source_condition_id,
            source_market_id,
            slug,
            question,
            category,
            outcome_a,
            outcome_b,
            token_a,
            token_b,
            outcome_price_a_live,
            outcome_price_b_live,
            group_item_title,
            start_date,
            end_date,
            volume_total_usd,
            volume_24h_live_usd,
            liquidity_live_usd,
            best_bid_live,
            best_ask_live,
            spread_live,
            last_trade_price_live,
            competitive,
            order_min_size,
            order_price_min_tick_size,
            accepting_orders,
            is_restricted,
            is_archived,
            active,
            closed,
            is_tracked,
            tracked_rank,
            seed_captured_at
        )
        VALUES (
            $1, $2::uuid, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15::timestamptz, $16::timestamptz,
            $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
            $27, $28, $29, $30, $31, $32, $33, NOW()
        )
        """,
        [
            (
                market["market_id"],
                market["pelion_market_uuid"],
                market["source_condition_id"],
                market["source_market_id"],
                market["slug"],
                market["question"],
                market["category"],
                market["outcome_a"],
                market["outcome_b"],
                market["token_a"],
                market["token_b"],
                market["outcome_price_a_live"],
                market["outcome_price_b_live"],
                market["group_item_title"],
                parse_optional_timestamp(market["start_date"]),
                parse_optional_timestamp(market["end_date"]),
                market["volume_total_usd"],
                market["volume_24h_live_usd"],
                market["liquidity_live_usd"],
                market["best_bid_live"],
                market["best_ask_live"],
                market["spread_live"],
                market["last_trade_price_live"],
                market["competitive"],
                market["order_min_size"],
                market["order_price_min_tick_size"],
                market["accepting_orders"],
                market["is_restricted"],
                market["is_archived"],
                market["active"],
                market["closed"],
                market["is_tracked"],
                market["tracked_rank"],
            )
            for market in markets_payload["markets"]
        ],
    )

    await conn.executemany(
        """
        INSERT INTO bonus.accounts (
            account_id,
            display_name,
            risk_style,
            home_region,
            base_currency,
            starting_cash_usd,
            notes
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        [
            (
                account["account_id"],
                account["display_name"],
                account["risk_style"],
                account["home_region"],
                account["base_currency"],
                account["starting_cash_usd"],
                account["notes"],
            )
            for account in accounts_payload
        ],
    )

    await conn.execute(
        """
        INSERT INTO bonus.dataset_runs (
            run_id,
            days,
            market_count,
            tracked_market_count,
            account_count,
            generated_at,
            notes
        )
        VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7)
        """,
        f"run-{summary['seed']}-{summary['days']}",
        summary["days"],
        summary["market_count"],
        summary["tracked_market_count"],
        summary["account_count"],
        parse_optional_timestamp(summary["generated_at"]),
        "Loaded by load_dataset.py",
    )


def ensure_csv(path: Path) -> Path:
    """Decompress .csv.gz if the .csv doesn't exist."""
    if path.exists():
        return path
    gz_path = Path(str(path) + ".gz")
    if gz_path.exists():
        import gzip, shutil
        print(f"Decompressing {gz_path.name} -> {path.name}")
        with gzip.open(gz_path, "rb") as f_in, path.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        return path
    raise FileNotFoundError(f"Neither {path} nor {gz_path} found")


async def copy_csv(
    conn: asyncpg.Connection,
    *,
    table: str,
    columns: list[str],
    path: Path,
) -> None:
    """COPY CSV file into TimescaleDB. Auto-decompresses .gz if needed."""
    path = ensure_csv(path)
    print(f"Loading {path.name} -> {table}")
    with path.open("rb") as handle:
        await conn.copy_to_table(
            table,
            schema_name="bonus",
            source=handle,
            columns=columns,
            format="csv",
            delimiter=",",
            header=True,
            null="",
        )


async def refresh_aggregates(conn: asyncpg.Connection) -> None:
    """Refresh continuous aggregates after load."""
    for aggregate in (
        "bonus.market_ohlc_5m",
        "bonus.market_spreads_5m",
        "bonus.market_liquidity_5m",
        "bonus.account_pnl_daily",
        "bonus.account_risk_daily",
    ):
        print(f"Refreshing {aggregate}")
        await conn.execute(f"CALL refresh_continuous_aggregate('{aggregate}', NULL, NULL)")


async def load_dataset(timeout_seconds: int) -> None:
    """Main loader."""
    dsn = get_bonus_db_url()
    await wait_for_db(dsn, timeout_seconds=timeout_seconds)
    conn = await asyncpg.connect(dsn)
    try:
        await apply_sql_files(conn)
        await load_dimensions(conn)
        await copy_csv(conn, table="market_ticks_1m", columns=TICK_COLUMNS, path=PATHS.market_ticks_csv)
        await copy_csv(conn, table="orderbook_topn_1m", columns=ORDERBOOK_COLUMNS, path=PATHS.orderbook_csv)
        await copy_csv(conn, table="account_trades", columns=TRADE_COLUMNS, path=PATHS.account_trades_csv)
        await copy_csv(conn, table="account_positions_1h", columns=POSITION_COLUMNS, path=PATHS.account_positions_csv)
        await refresh_aggregates(conn)
        await conn.execute("ANALYZE bonus.market_ticks_1m")
        await conn.execute("ANALYZE bonus.orderbook_topn_1m")
        await conn.execute("ANALYZE bonus.account_trades")
        await conn.execute("ANALYZE bonus.account_positions_1h")
    finally:
        await conn.close()

    print("Dataset load complete")
    print(f"SQL root: {SQL_DIR}")
    print(f"Project root: {ROOT}")


def parse_args() -> argparse.Namespace:
    """CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-seconds", type=int, default=90, help="Wait time for Docker TimescaleDB")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    asyncio.run(load_dataset(args.timeout_seconds))


if __name__ == "__main__":
    main()
