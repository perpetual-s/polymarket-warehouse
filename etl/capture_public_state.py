#!/usr/bin/env python3
"""Capture current public Polymarket state for tracked bonus markets."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

THIS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = THIS_DIR.parent
REPO_DIR = PROJECT_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))
sys.path.insert(0, str(REPO_DIR))

import requests

from etl.common import (  # type: ignore  # noqa: E402
    PATHS,
    ensure_directories,
    isoformat,
    read_json,
    request_json,
    run_async,
    utc_now,
    write_json,
)


def summarize_public_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Build quick stats from public trade sample."""
    sizes = [float(item.get("size", 0) or 0) for item in trades]
    prices = [float(item.get("price", 0) or 0) for item in trades]
    notionals = [size * price for size, price in zip(sizes, prices, strict=False)]
    if not notionals:
        return {"count": 0}

    return {
        "count": len(trades),
        "avg_trade_notional_usd": round(sum(notionals) / len(notionals), 4),
        "max_trade_notional_usd": round(max(notionals), 4),
        "avg_price": round(sum(prices) / len(prices), 6),
        "distinct_titles": len({item.get("title") for item in trades if item.get("title")}),
    }


async def capture_public_state(markets_path: Path, output_path: Path, public_trade_limit: int) -> None:
    """Capture live public state for tracked markets."""
    ensure_directories()
    seed_payload = read_json(markets_path)
    markets = seed_payload["markets"]
    tracked_markets = [market for market in markets if market.get("is_tracked")]
    tracked_tokens = [token for market in tracked_markets for token in (market["token_a"], market["token_b"])]

    orderbooks: dict[str, dict[str, Any]] = {}
    for token_id in tracked_tokens:
        try:
            payload = request_json("https://clob.polymarket.com/book", params={"token_id": token_id}, timeout=15)
        except requests.RequestException:
            continue

        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        best_bid = float(bids[-1]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        midpoint = None
        spread = None
        if best_bid is not None and best_ask is not None:
            midpoint = round((best_bid + best_ask) / 2.0, 6)
            spread = round(best_ask - best_bid, 6)

        orderbooks[token_id] = {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "midpoint": midpoint,
            "bids": [
                {"price": float(level["price"]), "size": float(level["size"])}
                for level in bids[-5:]
            ][::-1],
            "asks": [
                {"price": float(level["price"]), "size": float(level["size"])}
                for level in asks[:5]
            ],
            "timestamp": payload.get("timestamp"),
            "last_trade_price": float(payload["last_trade_price"]) if payload.get("last_trade_price") is not None else None,
            "tick_size": float(payload["tick_size"]) if payload.get("tick_size") is not None else None,
        }

    smoke_test: dict[str, Any] = {"captured_at": isoformat(utc_now())}
    try:
        smoke_test["geoblock"] = request_json("https://polymarket.com/api/geoblock")
    except requests.RequestException as exc:
        smoke_test["geoblock_error"] = str(exc)

    try:
        gamma_probe = request_json(
            "https://gamma-api.polymarket.com/markets",
            params={"active": "true", "closed": "false", "limit": 1},
        )
        smoke_test["gamma_probe_count"] = len(gamma_probe) if isinstance(gamma_probe, list) else 0
    except requests.RequestException as exc:
        smoke_test["gamma_probe_error"] = str(exc)

    public_trades: list[dict[str, Any]] = []
    try:
        response = request_json(
            "https://data-api.polymarket.com/trades",
            params={"limit": public_trade_limit},
        )
        if isinstance(response, list):
            public_trades = response
            smoke_test["public_trade_probe_count"] = len(public_trades)
    except requests.RequestException as exc:
        smoke_test["public_trade_probe_error"] = str(exc)

    payload = {
        "captured_at": isoformat(utc_now()),
        "tracked_market_count": len(tracked_markets),
        "tracked_token_count": len(tracked_tokens),
        "smoke_test": smoke_test,
        "orderbooks": orderbooks,
        "public_trade_sample": public_trades,
        "public_trade_stats": summarize_public_trades(public_trades),
    }
    write_json(output_path, payload)

    print(f"Wrote public state to {output_path}")
    print(f"Tracked token orderbooks captured: {len(orderbooks)}")
    print(f"Public trade sample size: {len(public_trades)}")


def parse_args() -> argparse.Namespace:
    """CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", type=Path, default=PATHS.markets, help="Seed markets JSON")
    parser.add_argument("--output", type=Path, default=PATHS.public_state, help="Public-state output JSON")
    parser.add_argument("--public-trade-limit", type=int, default=500, help="Public trade sample size")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    run_async(capture_public_state(args.markets, args.output, args.public_trade_limit))


if __name__ == "__main__":
    main()
