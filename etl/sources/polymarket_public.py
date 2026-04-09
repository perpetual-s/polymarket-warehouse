"""Async client for Polymarket public endpoints (raw JSON, no Pydantic)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)


DATA_API_URL = "https://data-api.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"


class PolymarketPublicError(RuntimeError):
    """Raised when a public endpoint returns a non-retryable error."""


class PolymarketPublicClient:
    """Minimal async client for public Polymarket endpoints.

    Usage:
        async with PolymarketPublicClient() as client:
            leaders = await client.get_leaderboard()
            positions = await client.get_positions(user="0x...")
    """

    def __init__(
        self,
        *,
        timeout_seconds: int = 20,
        max_concurrent: int = 5,
        min_request_interval: float = 0.063,
        max_retries: int = 3,
    ) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._min_interval = min_request_interval
        self._max_retries = max_retries
        self._last_request_monotonic = 0.0
        self._pace_lock = asyncio.Lock()

    async def __aenter__(self) -> "PolymarketPublicClient":
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _throttle(self) -> None:
        async with self._pace_lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            elapsed = now - self._last_request_monotonic
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request_monotonic = loop.time()

    async def _get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("PolymarketPublicClient must be used as async context manager")

        await self._throttle()

        last_error: Optional[str] = None
        for attempt in range(self._max_retries):
            async with self._semaphore:
                try:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        text = (await resp.text())[:300]
                        if resp.status in (429, 500, 502, 503, 504):
                            last_error = f"HTTP {resp.status}: {text}"
                            wait = 2 ** attempt
                            logger.warning(
                                "transient %s on %s (attempt %d/%d) -> sleep %ds",
                                resp.status, url, attempt + 1, self._max_retries, wait
                            )
                            await asyncio.sleep(wait)
                            continue
                        raise PolymarketPublicError(
                            f"GET {url} failed with {resp.status}: {text}"
                        )
                except aiohttp.ClientError as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt == self._max_retries - 1:
                        raise PolymarketPublicError(
                            f"GET {url} exhausted retries: {last_error}"
                        ) from exc
                    await asyncio.sleep(2 ** attempt)

        raise PolymarketPublicError(f"GET {url} exhausted retries: {last_error}")

    # --- leaderboard ---

    async def get_leaderboard(self) -> list[dict[str, Any]]:
        """Fetch top traders from /v1/leaderboard.

        The API does not accept window/limit parameters — it returns a fixed
        snapshot (25 rows observed). Called as-is; no params.
        """
        data = await self._get(f"{DATA_API_URL}/v1/leaderboard")
        if isinstance(data, list):
            return data
        logger.warning("Unexpected leaderboard response shape: %s", type(data).__name__)
        return []

    # --- public profile ---

    async def get_public_profile(self, address: str) -> Optional[dict[str, Any]]:
        """Fetch /public-profile?address=... from Gamma."""
        if not address:
            return None
        data = await self._get(
            f"{GAMMA_API_URL}/public-profile",
            params={"address": address.lower()},
        )
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data:
            return data[0] if isinstance(data[0], dict) else None
        return None

    # --- positions ---

    async def get_positions(
        self,
        user: str,
        *,
        size_threshold: float = 1.0,
        limit: int = 500,
        offset: int = 0,
        sort_by: str = "CURRENT",
        sort_direction: str = "DESC",
    ) -> list[dict[str, Any]]:
        """Fetch /positions?user=... Returns raw list of dicts."""
        if not user or not user.startswith("0x"):
            raise ValueError(f"Invalid user address: {user}")
        params: dict[str, Any] = {
            "user": user.lower(),
            "sizeThreshold": size_threshold,
            "limit": min(limit, 500),
            "offset": min(offset, 10000),
            "sortBy": sort_by,
            "sortDirection": sort_direction,
        }
        data = await self._get(f"{DATA_API_URL}/positions", params=params)
        if isinstance(data, list):
            return data
        logger.warning("Unexpected positions response shape: %s", type(data).__name__)
        return []

    # --- activity ---

    async def get_activity(
        self,
        user: str,
        *,
        limit: int = 500,
        offset: int = 0,
        activity_type: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Fetch /activity?user=... Returns raw list of dicts (no Pydantic)."""
        if not user or not user.startswith("0x"):
            raise ValueError(f"Invalid user address: {user}")
        params: dict[str, Any] = {
            "user": user.lower(),
            "limit": min(limit, 500),
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        if activity_type:
            params["type"] = activity_type
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end
        data = await self._get(f"{DATA_API_URL}/activity", params=params)
        if isinstance(data, list):
            return data
        logger.warning("Unexpected activity response shape: %s", type(data).__name__)
        return []

    # --- holders ---

    async def get_holders(
        self,
        market: str,
        *,
        limit: int = 500,
        min_balance: int = 1,
    ) -> list[dict[str, Any]]:
        """Fetch /holders?market=... Returns nested [{token, holders: [...]}]."""
        if not market:
            raise ValueError("Market conditionId is required")
        params: dict[str, Any] = {
            "market": market,
            "limit": min(limit, 500),
            "minBalance": min_balance,
        }
        data = await self._get(f"{DATA_API_URL}/holders", params=params)
        if isinstance(data, list):
            return data
        logger.warning("Unexpected holders response shape: %s", type(data).__name__)
        return []

    # --- activity (paginated) ---

    async def get_activity_all(
        self,
        user: str,
        *,
        page_size: int = 500,
        max_pages: int = 10,
    ) -> list[dict[str, Any]]:
        """Paginate activity until empty or max_pages reached."""
        all_rows: list[dict[str, Any]] = []
        for page_index in range(max_pages):
            offset = page_index * page_size
            batch = await self.get_activity(user, limit=page_size, offset=offset)
            if not batch:
                break
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
        return all_rows
