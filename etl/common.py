"""Shared helpers for Sparta DB bonus ETL."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SEED_DIR = DATA_DIR / "seed"
GENERATED_DIR = DATA_DIR / "generated"
LOAD_DIR = DATA_DIR / "load"
SQL_DIR = ROOT / "sql"

DEFAULT_PELION_DB_URL = "postgresql://localhost:9000/pelion"
DEFAULT_BONUS_DB_URL = "postgresql://bonus:bonus@localhost:5433/sparta_bonus"


@dataclass(frozen=True)
class PathSet:
    """Common ETL paths."""

    markets: Path = SEED_DIR / "markets.json"
    public_state: Path = SEED_DIR / "public_state.json"
    accounts: Path = GENERATED_DIR / "accounts.json"
    generation_summary: Path = GENERATED_DIR / "generation_summary.json"
    market_ticks_csv: Path = LOAD_DIR / "market_ticks_1m.csv"
    orderbook_csv: Path = LOAD_DIR / "orderbook_topn_1m.csv"
    account_trades_csv: Path = LOAD_DIR / "account_trades.csv"
    account_positions_csv: Path = LOAD_DIR / "account_positions_1h.csv"


PATHS = PathSet()


def ensure_directories() -> None:
    """Create required directories."""
    for path in (SEED_DIR, GENERATED_DIR, LOAD_DIR):
        path.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    """Current UTC timestamp."""
    return datetime.now(timezone.utc)


def isoformat(value: datetime | None) -> str | None:
    """Serialize datetimes to ISO-8601."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def json_dumps(data: Any) -> str:
    """Stable JSON formatter."""
    return json.dumps(data, indent=2, sort_keys=False, ensure_ascii=True)


def write_json(path: Path, data: Any) -> None:
    """Write JSON file."""
    ensure_directories()
    path.write_text(json_dumps(data) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    """Read JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def parse_json_field(value: Any) -> Any:
    """Parse JSONB-like values from asyncpg rows."""
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def normalize_dsn(dsn: str) -> str:
    """Strip unsupported query args for asyncpg."""
    if "?" in dsn:
        return dsn.split("?", 1)[0]
    return dsn


def get_pelion_db_url() -> str:
    """Pelion PostgreSQL DSN."""
    raw = os.getenv("PELION_DATABASE_URL") or os.getenv("DATABASE_URL") or DEFAULT_PELION_DB_URL
    return normalize_dsn(raw)


def get_bonus_db_url() -> str:
    """TimescaleDB DSN for bonus dataset."""
    raw = os.getenv("SPARTA_BONUS_DB_URL") or DEFAULT_BONUS_DB_URL
    return normalize_dsn(raw)


def chunked(items: Sequence[Any], size: int) -> Iterator[list[Any]]:
    """Yield fixed-size chunks."""
    for index in range(0, len(items), size):
        yield list(items[index:index + size])


def request_json(url: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> Any:
    """HTTP GET JSON helper."""
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def request_text(url: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> str:
    """HTTP GET text helper."""
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.text


def run_async(coro: Any) -> Any:
    """Run async helper from script entrypoints."""
    return asyncio.run(coro)


def load_sql(path: Path) -> str:
    """Load SQL file."""
    return path.read_text(encoding="utf-8")


def market_score(volume_24h: float, liquidity: float, volume_total: float, competitive: float | None) -> float:
    """Ranking score for tracked-market selection."""
    quality = 0.0 if competitive is None else competitive * 1_000.0
    return (volume_24h * 0.55) + (liquidity * 0.30) + (volume_total * 0.10) + quality


def outcome_pair_price(price_a: float | None, price_b: float | None) -> tuple[float, float]:
    """Normalize binary market live prices."""
    if price_a is None and price_b is None:
        return 0.5, 0.5
    if price_a is None:
        return max(0.01, 1.0 - float(price_b)), float(price_b)
    if price_b is None:
        return float(price_a), max(0.01, 1.0 - float(price_a))
    return float(price_a), float(price_b)
