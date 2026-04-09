#!/usr/bin/env python3
"""Generate realistic Polymarket-like history for Grafana dashboards."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import sys
from typing import Any

THIS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = THIS_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from etl.common import (
    PATHS,
    ensure_directories,
    isoformat,
    read_json,
    write_json,
)


TICK_COLUMNS = [
    "ts",
    "market_id",
    "token_id",
    "outcome",
    "last_trade_price",
    "midpoint_price",
    "best_bid",
    "best_ask",
    "spread",
    "liquidity_usd",
    "volume_usd_1m",
    "trade_count_1m",
    "source",
]

ORDERBOOK_COLUMNS = [
    "ts",
    "market_id",
    "token_id",
    "outcome",
    "side",
    "level_no",
    "price",
    "size_shares",
    "notional_usd",
    "source",
]

TRADE_COLUMNS = [
    "executed_at",
    "trade_id",
    "account_id",
    "market_id",
    "token_id",
    "outcome",
    "side",
    "price",
    "size_shares",
    "notional_usd",
    "fee_usd",
    "slippage_bps",
    "realized_pnl_usd",
    "maker_taker",
    "source",
]

POSITION_COLUMNS = [
    "ts",
    "account_id",
    "market_id",
    "token_id",
    "outcome",
    "position_size",
    "avg_entry_price",
    "mark_price",
    "cost_basis_usd",
    "market_value_usd",
    "realized_pnl_to_date_usd",
    "unrealized_pnl_usd",
    "cash_balance_usd",
    "equity_usd",
    "concentration_pct",
    "source",
]


ACCOUNT_PROFILES = [
    {
        "account_id": "acct_macro_comet",
        "display_name": "Macro Comet",
        "risk_style": "swing",
        "home_region": "US",
        "starting_cash_usd": 40000.0,
        "notes": "Prefers politics and macro event markets",
        "preferred_categories": ["Politics", "World", "News"],
        "entry_rate": 0.42,
        "max_open_positions": 8,
        "min_trade_usd": 450.0,
        "max_trade_usd": 2600.0,
        "hold_hours": (18, 120),
        "momentum_bias": 0.65,
        "exit_stop": -0.085,
        "exit_take": 0.12,
        "partial_exit_rate": 0.35,
        "execution_penalty_bps": 9,
        "max_position_pct": 0.12,
    },
    {
        "account_id": "acct_chart_falcon",
        "display_name": "Chart Falcon",
        "risk_style": "momentum",
        "home_region": "US",
        "starting_cash_usd": 32000.0,
        "notes": "Chases fast movers with looser risk control",
        "preferred_categories": ["Crypto", "Business", "Politics"],
        "entry_rate": 0.58,
        "max_open_positions": 7,
        "min_trade_usd": 300.0,
        "max_trade_usd": 2200.0,
        "hold_hours": (6, 48),
        "momentum_bias": 0.82,
        "exit_stop": -0.11,
        "exit_take": 0.16,
        "partial_exit_rate": 0.25,
        "execution_penalty_bps": 14,
        "max_position_pct": 0.11,
    },
    {
        "account_id": "acct_event_harbor",
        "display_name": "Event Harbor",
        "risk_style": "carry",
        "home_region": "US",
        "starting_cash_usd": 55000.0,
        "notes": "Spreads risk over many small event positions",
        "preferred_categories": ["Sports", "Entertainment", "Pop Culture"],
        "entry_rate": 0.34,
        "max_open_positions": 12,
        "min_trade_usd": 200.0,
        "max_trade_usd": 1200.0,
        "hold_hours": (24, 168),
        "momentum_bias": 0.52,
        "exit_stop": -0.07,
        "exit_take": 0.10,
        "partial_exit_rate": 0.42,
        "execution_penalty_bps": 6,
        "max_position_pct": 0.07,
    },
    {
        "account_id": "acct_noise_vessel",
        "display_name": "Noise Vessel",
        "risk_style": "overtrader",
        "home_region": "US",
        "starting_cash_usd": 18000.0,
        "notes": "Too active and pays for it in slippage",
        "preferred_categories": ["Sports", "Crypto", "Politics"],
        "entry_rate": 0.78,
        "max_open_positions": 6,
        "min_trade_usd": 150.0,
        "max_trade_usd": 900.0,
        "hold_hours": (2, 24),
        "momentum_bias": 0.55,
        "exit_stop": -0.14,
        "exit_take": 0.08,
        "partial_exit_rate": 0.18,
        "execution_penalty_bps": 24,
        "max_position_pct": 0.10,
    },
    {
        "account_id": "acct_whale_lite",
        "display_name": "Whale Lite",
        "risk_style": "large-ticket",
        "home_region": "US",
        "starting_cash_usd": 95000.0,
        "notes": "Concentrated positions in liquid markets",
        "preferred_categories": ["Politics", "Business", "Crypto"],
        "entry_rate": 0.29,
        "max_open_positions": 5,
        "min_trade_usd": 1200.0,
        "max_trade_usd": 7000.0,
        "hold_hours": (12, 96),
        "momentum_bias": 0.68,
        "exit_stop": -0.09,
        "exit_take": 0.14,
        "partial_exit_rate": 0.33,
        "execution_penalty_bps": 8,
        "max_position_pct": 0.18,
    },
    {
        "account_id": "acct_underdog",
        "display_name": "Underdog",
        "risk_style": "contrarian",
        "home_region": "US",
        "starting_cash_usd": 21000.0,
        "notes": "Leans into compressed underdog odds",
        "preferred_categories": ["Sports", "Politics", "World"],
        "entry_rate": 0.46,
        "max_open_positions": 9,
        "min_trade_usd": 220.0,
        "max_trade_usd": 1400.0,
        "hold_hours": (10, 80),
        "momentum_bias": 0.34,
        "exit_stop": -0.10,
        "exit_take": 0.15,
        "partial_exit_rate": 0.40,
        "execution_penalty_bps": 11,
        "max_position_pct": 0.09,
    },
    {
        "account_id": "acct_slate_arrow",
        "display_name": "Slate Arrow",
        "risk_style": "news-reactive",
        "home_region": "US",
        "starting_cash_usd": 27000.0,
        "notes": "Trades event clusters around news bursts",
        "preferred_categories": ["News", "World", "Politics"],
        "entry_rate": 0.53,
        "max_open_positions": 7,
        "min_trade_usd": 260.0,
        "max_trade_usd": 1800.0,
        "hold_hours": (3, 36),
        "momentum_bias": 0.72,
        "exit_stop": -0.12,
        "exit_take": 0.13,
        "partial_exit_rate": 0.28,
        "execution_penalty_bps": 12,
        "max_position_pct": 0.10,
    },
    {
        "account_id": "acct_delta_hollow",
        "display_name": "Delta Hollow",
        "risk_style": "slow-burn",
        "home_region": "US",
        "starting_cash_usd": 36000.0,
        "notes": "Steadier book with lower turnover",
        "preferred_categories": ["Business", "Crypto", "Politics"],
        "entry_rate": 0.24,
        "max_open_positions": 6,
        "min_trade_usd": 500.0,
        "max_trade_usd": 2500.0,
        "hold_hours": (36, 180),
        "momentum_bias": 0.59,
        "exit_stop": -0.06,
        "exit_take": 0.11,
        "partial_exit_rate": 0.37,
        "execution_penalty_bps": 5,
        "max_position_pct": 0.12,
    },
    {
        "account_id": "acct_kite_room",
        "display_name": "Kite Room",
        "risk_style": "speculative",
        "home_region": "US",
        "starting_cash_usd": 14000.0,
        "notes": "Small aggressive account with visible drawdowns",
        "preferred_categories": ["Entertainment", "Pop Culture", "Sports"],
        "entry_rate": 0.63,
        "max_open_positions": 5,
        "min_trade_usd": 120.0,
        "max_trade_usd": 700.0,
        "hold_hours": (4, 30),
        "momentum_bias": 0.78,
        "exit_stop": -0.16,
        "exit_take": 0.09,
        "partial_exit_rate": 0.20,
        "execution_penalty_bps": 26,
        "max_position_pct": 0.11,
    },
    {
        "account_id": "acct_bridge_nova",
        "display_name": "Bridge Nova",
        "risk_style": "balanced",
        "home_region": "US",
        "starting_cash_usd": 48000.0,
        "notes": "Generalist account that smooths the aggregate portfolio",
        "preferred_categories": ["Politics", "Crypto", "Sports", "Business"],
        "entry_rate": 0.38,
        "max_open_positions": 10,
        "min_trade_usd": 350.0,
        "max_trade_usd": 2000.0,
        "hold_hours": (8, 96),
        "momentum_bias": 0.57,
        "exit_stop": -0.08,
        "exit_take": 0.12,
        "partial_exit_rate": 0.36,
        "execution_penalty_bps": 7,
        "max_position_pct": 0.10,
    },
]


@dataclass
class HourPoint:
    """Hourly mark data for one market."""

    ts: datetime
    price_a: float
    price_b: float
    spread: float
    liquidity: float
    momentum_6h: float


def clamp(value: float, lower: float, upper: float) -> float:
    """Clamp numeric value."""
    return max(lower, min(upper, value))


def fmt(value: Any) -> Any:
    """CSV formatter."""
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}" if abs(value) < 1000 else f"{value:.2f}"
    return value


def activity_weight(ts: datetime) -> float:
    """Intraday/weekly volume profile."""
    hour = ts.hour
    weekday = ts.weekday()
    base = 0.18
    if 12 <= hour <= 23:
        base += 0.22
    elif 6 <= hour < 12:
        base += 0.08
    if weekday in (1, 2, 3):
        base += 0.06
    if weekday == 6:
        base -= 0.04
    return clamp(base, 0.08, 0.52)


def category_volatility(category: str) -> float:
    """Category-level volatility coefficient."""
    category = (category or "").lower()
    if "crypto" in category:
        return 0.010
    if "sports" in category:
        return 0.008
    if "politics" in category or "world" in category or "news" in category:
        return 0.007
    if "business" in category:
        return 0.006
    return 0.005


def urgency_factor(end_date: datetime | None, ts: datetime) -> float:
    """Volatility bump for near-expiry markets."""
    if end_date is None:
        return 1.0
    days_left = (end_date - ts).total_seconds() / 86400.0
    if days_left <= 2:
        return 2.4
    if days_left <= 7:
        return 1.9
    if days_left <= 21:
        return 1.4
    return 1.0


def build_orderbook_rows(
    ts: datetime,
    market: dict[str, Any],
    midpoint: float,
    spread: float,
    liquidity: float,
    tick_size: float,
) -> list[list[Any]]:
    """Create top-5 orderbook levels for both outcomes."""
    rows: list[list[Any]] = []
    for token_key, outcome_key, token_price in (
        ("token_a", "outcome_a", midpoint),
        ("token_b", "outcome_b", 1.0 - midpoint),
    ):
        token_id = market[token_key]
        outcome = market[outcome_key]
        best_bid = clamp(token_price - (spread / 2.0), 0.01, 0.99)
        best_ask = clamp(token_price + (spread / 2.0), 0.01, 0.99)

        for level_no in range(1, 6):
            bid_price = clamp(best_bid - ((level_no - 1) * tick_size), 0.01, 0.99)
            ask_price = clamp(best_ask + ((level_no - 1) * tick_size), 0.01, 0.99)
            depth_weight = max(0.18, 1.0 - ((level_no - 1) * 0.14))
            level_notional = max(35.0, liquidity * 0.012 * depth_weight)
            bid_size = level_notional / max(bid_price, 0.01)
            ask_size = level_notional / max(ask_price, 0.01)
            rows.append([
                ts.isoformat(),
                market["market_id"],
                token_id,
                outcome,
                "bid",
                level_no,
                fmt(bid_price),
                fmt(bid_size),
                fmt(level_notional),
                "generated",
            ])
            rows.append([
                ts.isoformat(),
                market["market_id"],
                token_id,
                outcome,
                "ask",
                level_no,
                fmt(ask_price),
                fmt(ask_size),
                fmt(level_notional),
                "generated",
            ])
    return rows


def choose_profiles(account_count: int) -> list[dict[str, Any]]:
    """Trim profile list to requested account count."""
    profiles = ACCOUNT_PROFILES[:account_count]
    if len(profiles) < account_count:
        raise ValueError(f"Requested {account_count} accounts but only {len(ACCOUNT_PROFILES)} profiles exist")
    return profiles


def choose_market(profile: dict[str, Any], tracked_markets: list[dict[str, Any]], rng: random.Random) -> dict[str, Any]:
    """Pick a market aligned with the account profile."""
    preferred = [
        market for market in tracked_markets
        if market["category"] in profile["preferred_categories"]
    ]
    pool = preferred or tracked_markets
    weighted = sorted(pool, key=lambda item: item["selection_score"], reverse=True)
    slice_size = min(len(weighted), max(4, profile["max_open_positions"] * 2))
    return rng.choice(weighted[:slice_size])


def execution_price(price: float, side: str, slippage_bps: int) -> float:
    """Apply slippage to an execution price."""
    shift = (slippage_bps / 10_000.0) * price
    if side == "BUY":
        return clamp(price + shift, 0.01, 0.99)
    return clamp(price - shift, 0.01, 0.99)


def compute_slippage_bps(liquidity: float, notional: float, base_penalty: int, rng: random.Random) -> int:
    """Estimate execution slippage from liquidity and account quality."""
    impact = 0.0 if liquidity <= 0 else (notional / liquidity) * 9_500
    noise = rng.randint(0, 8)
    return max(1, int(base_penalty + impact + noise))


def should_exit(
    profile: dict[str, Any],
    age_hours: int,
    pnl_pct: float,
    momentum: float,
    rng: random.Random,
) -> bool:
    """Exit rule for synthetic accounts."""
    min_hold, max_hold = profile["hold_hours"]
    if pnl_pct <= profile["exit_stop"]:
        return True
    if pnl_pct >= profile["exit_take"]:
        return True
    if age_hours >= max_hold:
        return True
    if age_hours < min_hold:
        return False
    chance = 0.03
    if pnl_pct > 0.02:
        chance += 0.08
    if momentum * profile["momentum_bias"] < -0.02:
        chance += 0.06
    return rng.random() < chance


def write_csv_header(path: Path, columns: list[str]) -> tuple[Any, csv.writer]:
    """Open a CSV writer with header row."""
    handle = path.open("w", encoding="utf-8", newline="")
    writer = csv.writer(handle)
    writer.writerow(columns)
    return handle, writer


def parse_end_date(raw: str | None) -> datetime | None:
    """Parse ISO timestamps."""
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def generate_market_history(
    markets: list[dict[str, Any]],
    public_state: dict[str, Any],
    days: int,
    detail_markets: int,
    rng: random.Random,
) -> tuple[dict[str, list[HourPoint]], dict[str, int]]:
    """Generate raw market ticks and orderbook depth."""
    ensure_directories()
    captured_at = public_state.get("captured_at")
    end_time = (
        datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
        if captured_at
        else datetime.now(timezone.utc)
    ).replace(second=0, microsecond=0)
    start_time = end_time - timedelta(days=days)
    total_minutes = days * 24 * 60

    tracked_markets = [market for market in markets if market.get("is_tracked")]
    detail_market_ids = {market["market_id"] for market in tracked_markets[:detail_markets]}
    orderbooks = public_state.get("orderbooks", {})

    market_hourly: dict[str, list[HourPoint]] = {}
    row_counts = {"market_ticks_1m": 0, "orderbook_topn_1m": 0}

    tick_handle, tick_writer = write_csv_header(PATHS.market_ticks_csv, TICK_COLUMNS)
    orderbook_handle, orderbook_writer = write_csv_header(PATHS.orderbook_csv, ORDERBOOK_COLUMNS)

    try:
        for market in markets:
            live_mid = orderbooks.get(market["token_a"], {}).get("midpoint")
            if live_mid is None:
                live_mid = market["outcome_price_a_live"]
            live_mid = clamp(float(live_mid or 0.5), 0.03, 0.97)
            end_date = parse_end_date(market.get("end_date"))
            tick_size = max(float(market.get("order_price_min_tick_size") or 0.01), 0.01)
            base_liquidity = max(float(market.get("liquidity_live_usd") or 0.0), 300.0)
            base_volume = max(float(market.get("volume_24h_live_usd") or 0.0), 150.0)
            volatility = category_volatility(market["category"])
            current_price = live_mid
            starting_price = clamp(live_mid + rng.gauss(0.0, 0.12), 0.04, 0.96)
            detail_market = market["market_id"] in detail_market_ids

            if not market.get("is_tracked"):
                spread = clamp(float(market.get("spread_live") or 0.02), 0.006, 0.08)
                liquidity = max(base_liquidity, 150.0)
                for token_id, outcome, price in (
                    (market["token_a"], market["outcome_a"], live_mid),
                    (market["token_b"], market["outcome_b"], 1.0 - live_mid),
                ):
                    best_bid = clamp(price - (spread / 2.0), 0.01, 0.99)
                    best_ask = clamp(price + (spread / 2.0), 0.01, 0.99)
                    tick_writer.writerow([
                        end_time.isoformat(),
                        market["market_id"],
                        token_id,
                        outcome,
                        fmt(price),
                        fmt(price),
                        fmt(best_bid),
                        fmt(best_ask),
                        fmt(spread),
                        fmt(liquidity),
                        fmt(base_volume / 96.0),
                        1,
                        "seed-live",
                    ])
                    row_counts["market_ticks_1m"] += 1
                continue

            hourly_points: list[HourPoint] = []
            price = starting_price
            last_observed_minute = -1
            last_price = price

            for minute_idx in range(total_minutes):
                ts = start_time + timedelta(minutes=minute_idx)
                frac = minute_idx / max(total_minutes - 1, 1)
                urgency = urgency_factor(end_date, ts)
                intraday = activity_weight(ts)
                target = starting_price + ((current_price - starting_price) * frac)
                shock = rng.gauss(0.0, volatility * 2.5) if rng.random() < 0.0025 * urgency else 0.0
                drift = (target - price) * (0.020 * urgency)
                noise = rng.gauss(0.0, volatility * intraday * urgency)
                price = clamp(price + drift + noise + shock, 0.01, 0.99)
                if minute_idx == total_minutes - 1:
                    price = current_price

                if minute_idx % 60 == 0:
                    prior_price = hourly_points[-1].price_a if hourly_points else price
                    momentum = price - prior_price
                    spread_for_hour = clamp(
                        0.006
                        + (35.0 / max(base_liquidity, 250.0))
                        + (abs(price - last_price) * 0.45)
                        + rng.uniform(0.0, 0.006),
                        0.006,
                        0.085,
                    )
                    liquidity_for_hour = max(
                        120.0,
                        base_liquidity * (0.78 + rng.random() * 0.48) * (1.0 + ((urgency - 1.0) * 0.12)),
                    )
                    hourly_points.append(
                        HourPoint(
                            ts=ts,
                            price_a=price,
                            price_b=clamp(1.0 - price, 0.01, 0.99),
                            spread=spread_for_hour,
                            liquidity=liquidity_for_hour,
                            momentum_6h=momentum,
                        )
                    )

                observed = rng.random() < clamp(intraday + ((base_volume / 25_000.0) * 0.08), 0.10, 0.55)
                observed = observed or minute_idx == total_minutes - 1
                if not observed:
                    continue

                jump = abs(price - last_price)
                spread = clamp(
                    0.006
                    + (40.0 / max(base_liquidity, 250.0))
                    + (jump * 0.55)
                    + rng.uniform(0.0, 0.008),
                    0.006,
                    0.09,
                )
                liquidity = max(
                    100.0,
                    base_liquidity * (0.72 + rng.random() * 0.55) * (1.0 + ((urgency - 1.0) * 0.10)),
                )
                volume_1m = max(
                    8.0,
                    (base_volume / 180.0) * intraday * urgency * (1.0 + (jump * 18.0)) * (0.45 + rng.random()),
                )
                trade_count = max(1, int(volume_1m / max(35.0, liquidity * 0.012)))

                for token_id, outcome, outcome_price in (
                    (market["token_a"], market["outcome_a"], price),
                    (market["token_b"], market["outcome_b"], 1.0 - price),
                ):
                    midpoint = clamp(outcome_price, 0.01, 0.99)
                    best_bid = clamp(midpoint - (spread / 2.0), 0.01, 0.99)
                    best_ask = clamp(midpoint + (spread / 2.0), 0.01, 0.99)
                    tick_writer.writerow([
                        ts.isoformat(),
                        market["market_id"],
                        token_id,
                        outcome,
                        fmt(midpoint),
                        fmt(midpoint),
                        fmt(best_bid),
                        fmt(best_ask),
                        fmt(spread),
                        fmt(liquidity),
                        fmt(volume_1m / 2.0),
                        trade_count,
                        "generated",
                    ])
                    row_counts["market_ticks_1m"] += 1

                if detail_market and minute_idx % 15 == 0 and minute_idx != last_observed_minute:
                    for row in build_orderbook_rows(ts, market, price, spread, liquidity, tick_size):
                        orderbook_writer.writerow(row)
                        row_counts["orderbook_topn_1m"] += 1
                    last_observed_minute = minute_idx

                last_price = price

            market_hourly[market["market_id"]] = hourly_points
    finally:
        tick_handle.close()
        orderbook_handle.close()

    return market_hourly, row_counts


def generate_accounts(account_count: int) -> list[dict[str, Any]]:
    """Create account dimension payload."""
    profiles = choose_profiles(account_count)
    return [
        {
            "account_id": profile["account_id"],
            "display_name": profile["display_name"],
            "risk_style": profile["risk_style"],
            "home_region": profile["home_region"],
            "base_currency": "USDC",
            "starting_cash_usd": profile["starting_cash_usd"],
            "notes": profile["notes"],
        }
        for profile in profiles
    ]


def generate_account_activity(
    markets: list[dict[str, Any]],
    market_hourly: dict[str, list[HourPoint]],
    account_count: int,
    rng: random.Random,
) -> dict[str, int]:
    """Generate trades and position snapshots consistent with market history."""
    tracked_markets = [market for market in markets if market.get("is_tracked")]
    market_lookup = {market["market_id"]: market for market in tracked_markets}
    hours = len(next(iter(market_hourly.values()))) if market_hourly else 0
    profiles = choose_profiles(account_count)

    write_json(PATHS.accounts, generate_accounts(account_count))

    trade_handle, trade_writer = write_csv_header(PATHS.account_trades_csv, TRADE_COLUMNS)
    pos_handle, pos_writer = write_csv_header(PATHS.account_positions_csv, POSITION_COLUMNS)

    row_counts = {"account_trades": 0, "account_positions_1h": 0}

    try:
        for profile in profiles:
            cash = float(profile["starting_cash_usd"])
            realized_to_date = 0.0
            positions: dict[str, dict[str, Any]] = {}
            trade_seq = 0

            for hour_idx in range(hours):
                account_ts = next(iter(market_hourly.values()))[hour_idx].ts

                # Exit decisions.
                for token_id in list(positions.keys()):
                    position = positions[token_id]
                    series = market_hourly[position["market_id"]][hour_idx]
                    mark_price = series.price_a if token_id == position["token_a"] else series.price_b
                    pnl_pct = 0.0 if position["avg_cost"] <= 0 else (mark_price - position["avg_cost"]) / position["avg_cost"]
                    age_hours = hour_idx - position["opened_hour"]
                    if not should_exit(profile, age_hours, pnl_pct, series.momentum_6h, rng):
                        continue

                    sell_fraction = 0.5 if rng.random() < profile["partial_exit_rate"] and position["qty"] > 25 else 1.0
                    qty = round(position["qty"] * sell_fraction, 4)
                    notional_hint = qty * mark_price
                    slippage_bps = compute_slippage_bps(series.liquidity, notional_hint, profile["execution_penalty_bps"], rng)
                    price = execution_price(mark_price, "SELL", slippage_bps)
                    proceeds = qty * price
                    fee = proceeds * 0.0006
                    realized = (price - position["avg_cost"]) * qty - fee

                    cash += proceeds - fee
                    realized_to_date += realized
                    position["qty"] = round(position["qty"] - qty, 4)
                    if position["qty"] <= 0.0001:
                        del positions[token_id]

                    trade_seq += 1
                    trade_writer.writerow([
                        account_ts.isoformat(),
                        f"{profile['account_id']}-{trade_seq:05d}",
                        profile["account_id"],
                        position["market_id"],
                        token_id,
                        position["outcome"],
                        "SELL",
                        fmt(price),
                        fmt(qty),
                        fmt(proceeds),
                        fmt(fee),
                        slippage_bps,
                        fmt(realized),
                        "TAKER",
                        "generated",
                    ])
                    row_counts["account_trades"] += 1

                # Entry decisions.
                max_actions = 1 if rng.random() < 0.65 else 2
                for _ in range(max_actions):
                    if rng.random() > profile["entry_rate"]:
                        continue
                    if len(positions) >= profile["max_open_positions"]:
                        break
                    market = choose_market(profile, tracked_markets, rng)
                    series = market_hourly[market["market_id"]][hour_idx]
                    choose_a = rng.random() < profile["momentum_bias"]
                    if series.momentum_6h < 0 and rng.random() < 0.65:
                        choose_a = not choose_a
                    token_id = market["token_a"] if choose_a else market["token_b"]
                    outcome = market["outcome_a"] if choose_a else market["outcome_b"]
                    mark_price = series.price_a if choose_a else series.price_b
                    if mark_price <= 0.01:
                        continue

                    target_notional = rng.uniform(profile["min_trade_usd"], profile["max_trade_usd"])
                    target_notional = min(target_notional, cash * profile["max_position_pct"])
                    if target_notional < 75.0 or cash < 150.0:
                        continue

                    slippage_bps = compute_slippage_bps(series.liquidity, target_notional, profile["execution_penalty_bps"], rng)
                    price = execution_price(mark_price, "BUY", slippage_bps)
                    qty = round(target_notional / max(price, 0.01), 4)
                    notional = qty * price
                    fee = notional * 0.0006
                    total_cost = notional + fee
                    if total_cost > cash:
                        continue

                    cash -= total_cost
                    trade_seq += 1
                    existing = positions.get(token_id)
                    if existing is None:
                        positions[token_id] = {
                            "market_id": market["market_id"],
                            "token_a": market["token_a"],
                            "token_b": market["token_b"],
                            "outcome": outcome,
                            "qty": qty,
                            "avg_cost": price,
                            "opened_hour": hour_idx,
                        }
                    else:
                        new_qty = existing["qty"] + qty
                        existing["avg_cost"] = ((existing["avg_cost"] * existing["qty"]) + (price * qty)) / max(new_qty, 0.0001)
                        existing["qty"] = round(new_qty, 4)

                    trade_writer.writerow([
                        account_ts.isoformat(),
                        f"{profile['account_id']}-{trade_seq:05d}",
                        profile["account_id"],
                        market["market_id"],
                        token_id,
                        outcome,
                        "BUY",
                        fmt(price),
                        fmt(qty),
                        fmt(notional),
                        fmt(fee),
                        slippage_bps,
                        fmt(0.0),
                        "TAKER",
                        "generated",
                    ])
                    row_counts["account_trades"] += 1

                # Position snapshots.
                open_rows: list[list[Any]] = []
                total_market_value = 0.0
                position_values: list[tuple[str, float, list[Any]]] = []
                for token_id, position in positions.items():
                    series = market_hourly[position["market_id"]][hour_idx]
                    mark_price = series.price_a if token_id == position["token_a"] else series.price_b
                    market_value = position["qty"] * mark_price
                    cost_basis = position["qty"] * position["avg_cost"]
                    unrealized = market_value - cost_basis
                    total_market_value += market_value
                    row = [
                        account_ts.isoformat(),
                        profile["account_id"],
                        position["market_id"],
                        token_id,
                        position["outcome"],
                        fmt(position["qty"]),
                        fmt(position["avg_cost"]),
                        fmt(mark_price),
                        fmt(cost_basis),
                        fmt(market_value),
                        "",
                        fmt(unrealized),
                        "",
                        "",
                        "",
                        "generated",
                    ]
                    position_values.append((token_id, market_value, row))

                max_concentration = 0.0
                if total_market_value > 0:
                    for _, market_value, row in position_values:
                        concentration = (market_value / total_market_value) * 100.0
                        row[14] = fmt(concentration)
                        open_rows.append(row)
                        max_concentration = max(max_concentration, concentration)
                else:
                    open_rows.extend(row for _, _, row in position_values)

                for row in open_rows:
                    pos_writer.writerow(row)
                    row_counts["account_positions_1h"] += 1

                equity = cash + total_market_value
                pos_writer.writerow([
                    account_ts.isoformat(),
                    profile["account_id"],
                    "__portfolio__",
                    "__portfolio__",
                    "PORTFOLIO",
                    fmt(0.0),
                    "",
                    "",
                    fmt(0.0),
                    fmt(total_market_value),
                    fmt(realized_to_date),
                    fmt(total_market_value - sum((row[8] and float(row[8])) or 0.0 for row in open_rows)),
                    fmt(cash),
                    fmt(equity),
                    fmt(max_concentration),
                    "generated",
                ])
                row_counts["account_positions_1h"] += 1
    finally:
        trade_handle.close()
        pos_handle.close()

    return row_counts


def generate_history(
    markets_path: Path,
    public_state_path: Path,
    days: int,
    account_count: int,
    detail_markets: int,
    seed: int,
) -> None:
    """Generate the full synthetic dataset."""
    ensure_directories()
    markets_payload = read_json(markets_path)
    public_state = read_json(public_state_path)
    markets = markets_payload["markets"]
    rng = random.Random(seed)

    market_hourly, market_counts = generate_market_history(
        markets=markets,
        public_state=public_state,
        days=days,
        detail_markets=detail_markets,
        rng=rng,
    )
    account_counts = generate_account_activity(
        markets=markets,
        market_hourly=market_hourly,
        account_count=account_count,
        rng=rng,
    )

    summary = {
        "generated_at": isoformat(datetime.now(timezone.utc)),
        "seed": seed,
        "days": days,
        "market_count": len(markets),
        "tracked_market_count": len([market for market in markets if market.get("is_tracked")]),
        "detail_market_count": detail_markets,
        "account_count": account_count,
        "row_counts": {**market_counts, **account_counts},
    }
    write_json(PATHS.generation_summary, summary)

    print("Generation complete")
    for key, value in summary["row_counts"].items():
        print(f"  {key}: {value}")
    print(f"Summary: {PATHS.generation_summary}")


def parse_args() -> argparse.Namespace:
    """CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", type=Path, default=PATHS.markets, help="Seed markets JSON")
    parser.add_argument("--public-state", type=Path, default=PATHS.public_state, help="Public state JSON")
    parser.add_argument("--days", type=int, default=60, help="History length")
    parser.add_argument("--accounts", type=int, default=10, help="Synthetic account count")
    parser.add_argument("--detail-markets", type=int, default=8, help="Markets with orderbook depth history")
    parser.add_argument("--seed", type=int, default=20260408, help="Random seed")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    generate_history(
        markets_path=args.markets,
        public_state_path=args.public_state,
        days=args.days,
        account_count=args.accounts,
        detail_markets=args.detail_markets,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
