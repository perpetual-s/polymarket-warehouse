#!/usr/bin/env python3
"""Extract a balanced set of Pelion markets for the bonus dataset."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

import asyncpg

THIS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = THIS_DIR.parent
REPO_DIR = PROJECT_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))
sys.path.insert(0, str(REPO_DIR))

from etl.common import (  # type: ignore  # noqa: E402
    PATHS,
    ensure_directories,
    get_pelion_db_url,
    isoformat,
    market_score,
    outcome_pair_price,
    parse_json_field,
    request_json,
    run_async,
    utc_now,
    write_json,
)


MARKET_QUERY = """
SELECT
    id::text AS pelion_market_uuid,
    condition_id,
    market_id AS source_market_id,
    slug,
    question,
    COALESCE(NULLIF(category, ''), 'uncategorized') AS category,
    outcomes::text AS outcomes_json,
    outcome_prices::text AS outcome_prices_json,
    tokens::text AS tokens_json,
    group_item_title,
    start_date,
    end_date,
    COALESCE(volume, 0) AS volume_total_usd,
    COALESCE(volume_24h, 0) AS volume_24h_live_usd,
    COALESCE(liquidity, 0) AS liquidity_live_usd,
    best_bid,
    best_ask,
    spread,
    last_trade_price,
    competitive,
    order_min_size,
    order_price_min_tick_size,
    accepting_orders,
    is_restricted,
    is_archived,
    active,
    closed
FROM markets
WHERE active = TRUE
  AND closed = FALSE
  AND COALESCE(is_archived, FALSE) = FALSE
  AND jsonb_typeof(outcomes) = 'array'
  AND jsonb_typeof(tokens) = 'array'
  AND jsonb_array_length(outcomes) = 2
  AND jsonb_array_length(tokens) = 2
  AND slug IS NOT NULL
  AND question IS NOT NULL
ORDER BY COALESCE(volume_24h, 0) DESC, COALESCE(liquidity, 0) DESC, COALESCE(volume, 0) DESC
LIMIT $1
"""


def build_market(row: asyncpg.Record) -> dict[str, Any] | None:
    """Normalize a Pelion market row into bonus-market metadata."""
    outcomes = parse_json_field(row["outcomes_json"]) or []
    tokens = parse_json_field(row["tokens_json"]) or []
    outcome_prices = parse_json_field(row["outcome_prices_json"]) or []
    if len(outcomes) != 2 or len(tokens) != 2:
        return None

    price_a_raw = float(outcome_prices[0]) if len(outcome_prices) > 0 else None
    price_b_raw = float(outcome_prices[1]) if len(outcome_prices) > 1 else None
    price_a, price_b = outcome_pair_price(price_a_raw, price_b_raw)

    volume_24h = float(row["volume_24h_live_usd"] or 0)
    liquidity = float(row["liquidity_live_usd"] or 0)
    volume_total = float(row["volume_total_usd"] or 0)
    competitive = float(row["competitive"]) if row["competitive"] is not None else None

    return {
        "market_id": row["condition_id"],
        "pelion_market_uuid": row["pelion_market_uuid"],
        "source_condition_id": row["condition_id"],
        "source_market_id": row["source_market_id"],
        "slug": row["slug"],
        "question": row["question"],
        "category": derive_category(row["question"], row["slug"], row["category"]),
        "outcome_a": outcomes[0],
        "outcome_b": outcomes[1],
        "token_a": tokens[0],
        "token_b": tokens[1],
        "outcome_price_a_live": price_a,
        "outcome_price_b_live": price_b,
        "group_item_title": row["group_item_title"],
        "start_date": isoformat(row["start_date"]),
        "end_date": isoformat(row["end_date"]),
        "volume_total_usd": volume_total,
        "volume_24h_live_usd": volume_24h,
        "liquidity_live_usd": liquidity,
        "best_bid_live": float(row["best_bid"]) if row["best_bid"] is not None else None,
        "best_ask_live": float(row["best_ask"]) if row["best_ask"] is not None else None,
        "spread_live": float(row["spread"]) if row["spread"] is not None else None,
        "last_trade_price_live": float(row["last_trade_price"]) if row["last_trade_price"] is not None else None,
        "competitive": competitive,
        "order_min_size": float(row["order_min_size"]) if row["order_min_size"] is not None else None,
        "order_price_min_tick_size": float(row["order_price_min_tick_size"]) if row["order_price_min_tick_size"] is not None else None,
        "accepting_orders": bool(row["accepting_orders"]),
        "is_restricted": bool(row["is_restricted"]),
        "is_archived": bool(row["is_archived"]),
        "active": bool(row["active"]),
        "closed": bool(row["closed"]),
        "source_catalog": "pelion",
        "selection_score": round(market_score(volume_24h, liquidity, volume_total, competitive), 4),
    }


def derive_category(question: str, slug: str | None, category: str | None) -> str:
    """Backfill category when Pelion cache does not have one."""
    if category and category.strip():
        return category.strip()

    haystack = f"{question} {slug or ''}".lower()
    keyword_map = {
        "Crypto": ["bitcoin", "ethereum", "solana", "crypto", "btc", "eth", "doge", "xrp"],
        "Sports": ["nba", "nfl", "mlb", "nhl", "fifa", "world cup", "super bowl", "champions league", "final four", "playoffs"],
        "Politics": ["trump", "biden", "senate", "house", "election", "fed", "rates", "president", "governor", "campaign"],
        "World": ["ukraine", "russia", "china", "israel", "gaza", "taiwan", "iran", "ceasefire"],
        "Entertainment": ["oscar", "grammy", "emmy", "movie", "album", "gta", "netflix", "drake", "rihanna"],
        "Business": ["tesla", "apple", "earnings", "ipo", "s&p", "nasdaq", "recession", "inflation", "economy", "tariff"],
        "Pop Culture": ["celebrity", "album", "release", "trailer", "box office", "tiktok", "streaming"],
        "News": ["breaking", "headline", "verdict", "trial", "storm", "earthquake", "wildfire"],
    }

    for derived, keywords in keyword_map.items():
        if any(keyword in haystack for keyword in keywords):
            return derived

    return "General"


def build_live_market(market: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a live Gamma market into bonus-market metadata."""
    tokens = market.get("clobTokenIds") or market.get("tokens") or []
    outcomes = market.get("outcomes") or []
    outcome_prices = market.get("outcomePrices") or market.get("outcome_prices") or []
    if isinstance(tokens, str):
        tokens = parse_json_field(tokens) or []
    if isinstance(outcomes, str):
        outcomes = parse_json_field(outcomes) or []
    if isinstance(outcome_prices, str):
        outcome_prices = parse_json_field(outcome_prices) or []
    if len(tokens) != 2 or len(outcomes) != 2:
        return None

    price_a, price_b = outcome_pair_price(
        float(outcome_prices[0]) if len(outcome_prices) > 0 else None,
        float(outcome_prices[1]) if len(outcome_prices) > 1 else None,
    )
    volume_24h = float(market.get("volume24hr") or market.get("volume_24h") or 0)
    liquidity = float(market.get("liquidity") or market.get("liquidityNum") or 0)
    volume_total = float(market.get("volume") or market.get("volumeNum") or 0)
    competitive = float(market["competitive"]) if market.get("competitive") is not None else None

    return {
        "market_id": market["conditionId"],
        "pelion_market_uuid": None,
        "source_condition_id": market["conditionId"],
        "source_market_id": market["id"],
        "slug": market["slug"],
        "question": market["question"],
        "category": derive_category(market["question"], market.get("slug"), market.get("category")),
        "outcome_a": outcomes[0],
        "outcome_b": outcomes[1],
        "token_a": tokens[0],
        "token_b": tokens[1],
        "outcome_price_a_live": price_a,
        "outcome_price_b_live": price_b,
        "group_item_title": market.get("groupItemTitle"),
        "start_date": isoformat(datetime.fromisoformat(market["startDate"].replace("Z", "+00:00"))) if market.get("startDate") else None,
        "end_date": isoformat(datetime.fromisoformat(market["endDate"].replace("Z", "+00:00"))) if market.get("endDate") else None,
        "volume_total_usd": volume_total,
        "volume_24h_live_usd": volume_24h,
        "liquidity_live_usd": liquidity,
        "best_bid_live": float(market["bestBid"]) if market.get("bestBid") is not None else None,
        "best_ask_live": float(market["bestAsk"]) if market.get("bestAsk") is not None else None,
        "spread_live": float(market["spread"]) if market.get("spread") is not None else None,
        "last_trade_price_live": float(market["lastTradePrice"]) if market.get("lastTradePrice") is not None else None,
        "competitive": competitive,
        "order_min_size": float(market["orderMinSize"]) if market.get("orderMinSize") is not None else None,
        "order_price_min_tick_size": float(market["orderPriceMinTickSize"]) if market.get("orderPriceMinTickSize") is not None else None,
        "accepting_orders": bool(market.get("acceptingOrders", True)),
        "is_restricted": bool(market.get("restricted", False)),
        "is_archived": bool(market.get("archived", False)),
        "active": bool(market.get("active", True)),
        "closed": bool(market.get("closed", False)),
        "source_catalog": "gamma",
        "selection_score": round(market_score(volume_24h, liquidity, volume_total, competitive), 4),
    }


async def fetch_live_markets() -> list[dict[str, Any]]:
    """Fetch current live markets from public Gamma API."""
    live_markets: list[dict[str, Any]] = []
    offset = 0
    while offset < 1000:
        batch = request_json(
            "https://gamma-api.polymarket.com/markets",
            params={"active": "true", "closed": "false", "limit": 200, "offset": offset},
        )
        if not isinstance(batch, list) or not batch:
            break
        live_markets.extend(batch)
        if len(batch) < 200:
            break
        offset += 200

    return [market for item in live_markets if (market := build_live_market(item)) is not None]


def merge_market_catalogs(pelion_markets: list[dict[str, Any]], live_markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prefer live Gamma data while preserving Pelion UUIDs when available."""
    merged: dict[str, dict[str, Any]] = {market["market_id"]: market for market in pelion_markets}
    for live in live_markets:
        existing = merged.get(live["market_id"])
        if existing is None:
            merged[live["market_id"]] = live
            continue

        live["pelion_market_uuid"] = existing.get("pelion_market_uuid")
        merged[live["market_id"]] = {**existing, **live}
    return list(merged.values())


def is_fresh_market(market: dict[str, Any]) -> bool:
    """Treat far-past end dates as stale cache entries."""
    end_date = market.get("end_date")
    if not end_date:
        return True
    parsed = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    return parsed >= datetime.now(timezone.utc) - timedelta(days=7)


def select_balanced_markets(candidates: list[dict[str, Any]], market_limit: int) -> list[dict[str, Any]]:
    """Round-robin across categories so the dataset is not mono-topic."""
    by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for market in sorted(candidates, key=lambda item: item["selection_score"], reverse=True):
        by_category[market["category"]].append(market)

    categories = sorted(
        by_category.keys(),
        key=lambda category: (
            len(by_category[category]),
            max(item["selection_score"] for item in by_category[category]),
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    while len(selected) < market_limit:
        progress = False
        for category in categories:
            queue = by_category[category]
            while queue and queue[0]["market_id"] in seen_ids:
                queue.pop(0)
            if not queue:
                continue

            market = queue.pop(0)
            selected.append(market)
            seen_ids.add(market["market_id"])
            progress = True
            if len(selected) >= market_limit:
                break

        if not progress:
            break

    return selected


async def extract_market_seed(
    market_limit: int,
    tracked_markets: int,
    candidate_limit: int,
    output_path: Path,
) -> None:
    """Extract and persist bonus-market metadata."""
    ensure_directories()
    conn = await asyncpg.connect(get_pelion_db_url())
    try:
        rows = await conn.fetch(MARKET_QUERY, candidate_limit)
    finally:
        await conn.close()

    pelion_candidates = [market for row in rows if (market := build_market(row)) is not None]
    live_candidates = await fetch_live_markets()
    candidates = merge_market_catalogs(pelion_candidates, live_candidates)
    candidates = [market for market in candidates if is_fresh_market(market)]
    selected = select_balanced_markets(candidates, market_limit)
    tracked = sorted(selected, key=lambda item: item["selection_score"], reverse=True)[:tracked_markets]
    tracked_ids = {item["market_id"] for item in tracked}
    tracked_rank = {item["market_id"]: index + 1 for index, item in enumerate(tracked)}

    for market in selected:
        market["is_tracked"] = market["market_id"] in tracked_ids
        market["tracked_rank"] = tracked_rank.get(market["market_id"])

    category_counts: dict[str, int] = defaultdict(int)
    for market in selected:
        category_counts[market["category"]] += 1

    payload = {
        "generated_at": isoformat(utc_now()),
        "market_limit": market_limit,
        "tracked_markets": tracked_markets,
        "candidate_limit": candidate_limit,
        "category_counts": dict(sorted(category_counts.items(), key=lambda item: (-item[1], item[0]))),
        "markets": selected,
    }
    write_json(output_path, payload)

    print(f"Wrote {len(selected)} markets to {output_path}")
    print(f"Tracked markets: {len(tracked_ids)}")
    print(f"Categories: {len(category_counts)}")


def parse_args() -> argparse.Namespace:
    """CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-limit", type=int, default=200, help="Final market count")
    parser.add_argument("--tracked-markets", type=int, default=24, help="Markets with full generated history")
    parser.add_argument("--candidate-limit", type=int, default=1200, help="Raw Pelion markets to score")
    parser.add_argument("--output", type=Path, default=PATHS.markets, help="Seed output path")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    run_async(
        extract_market_seed(
            market_limit=args.market_limit,
            tracked_markets=args.tracked_markets,
            candidate_limit=args.candidate_limit,
            output_path=args.output,
        )
    )


if __name__ == "__main__":
    main()
