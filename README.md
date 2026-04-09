# Polymarket Warehouse

A wallet-first Polymarket data warehouse with real-time ingestion, analytics, and 7 Grafana dashboards. Built on TimescaleDB.

## What It Does

Discovers, tracks, and analyzes Polymarket traders at scale:

- **2.59M wallets** discovered from leaderboards, market holder lists, and PolymarketAnalytics
- **Pre-computed trader intelligence** — PnL, win rate, positions, trader classification tags across all indexed wallets
- **100 tracked wallets** with deep coverage: trade events, position snapshots, daily performance
- **160K holder records** across 200 markets
- **Raw → derived architecture** — raw JSON payloads for audit, normalized trade events and continuous aggregates for analytics
- **Cron-ready pipeline** with deduplication, rate limiting, retry, and collector health monitoring

## Quick Start

```bash
docker compose up -d          # TimescaleDB + Grafana
conda activate athos          # or your Python 3.13 environment

# Load market baseline (data included, auto-decompresses)
python etl/load_dataset.py

# Load wallet warehouse (fetches live from Polymarket public APIs)
python etl/run_ingestors_once.py --pma-max 2600000 --holders-all-markets --tracked-count 100
```

Open **http://localhost:3000** (admin / admin) → Dashboards → select any dashboard.

## Dashboards

| Dashboard | What It Shows |
|---|---|
| **Wallet Leaderboard** | PnL distribution across 2.59M traders, color-coded rankings, whale holdings, trader tags |
| **Wallet Detail** | Per-wallet drilldown: positions, trades, daily volume, top markets (dropdown selector) |
| **P&L / Risk** | Cross-wallet performance comparison, win rate bands, daily metrics |
| **Market Flow** | Per-market buy/sell activity, top traders, net flow (dropdown selector) |
| **Collector Health** | Ingestor throughput, lag, run history, raw table stats |
| **Market Overview** | Category breakdown, volume leaders, 24h movers |
| **Market Detail** | Per-market OHLC, orderbook depth, spread analysis |

Cross-dashboard linking (clickable wallet names), threshold-based color coding (PnL green/red, win rate bands), variable selectors with smart defaults.

## Data Sources

All public, no authentication required:

| Source | Endpoint |
|---|---|
| Leaderboard | `data-api.polymarket.com/v1/leaderboard` |
| Positions | `data-api.polymarket.com/positions?user=...` |
| Activity | `data-api.polymarket.com/activity?user=...` |
| Holders | `data-api.polymarket.com/holders?market=...` |
| Profile | `gamma-api.polymarket.com/public-profile?address=...` |
| Markets | `gamma-api.polymarket.com/markets` |
| Orderbook | `clob.polymarket.com/book` |
| PMA Traders | `polymarketanalytics.com/api/traders-tag-performance` |

## Schema

**Wallet layer (real data):**

| Table | Type | Content |
|---|---|---|
| `bonus.wallets` | dimension | 2.59M wallet identities |
| `bonus.raw_pma_trader_snapshots` | hypertable | PMA trader profiles with PnL, win rate, tags |
| `bonus.raw_wallet_activity_events` | hypertable | Trade and activity events |
| `bonus.raw_wallet_positions_snapshots` | hypertable | Position snapshots per wallet |
| `bonus.raw_market_holders_snapshots` | hypertable | Market holder lists |
| `bonus.raw_wallet_leaderboard_snapshots` | hypertable | Leaderboard snapshots |
| `bonus.raw_wallet_profiles` | hypertable | Profile metadata |
| `bonus.wallet_trade_events` | derived hypertable | Normalized trade events |
| `bonus.wallet_performance_daily` | continuous aggregate | Daily PnL/volume per wallet |
| `bonus.v_wallet_summary` | materialized view | Dashboard-ready wallet stats |
| `bonus.v_market_flow` | view | Buy/sell flow analysis |
| `bonus.v_whale_concentration` | view | Whale concentration per market |
| `bonus.collector_runs` | table | Pipeline run audit log |
| `bonus.collector_health` | hypertable | Pipeline health metrics |

**Market layer (real catalog + generated history):**

| Table | Type |
|---|---|
| `bonus.markets` | 200 real markets |
| `bonus.market_ticks_1m` | 60-day generated minute prices |
| `bonus.orderbook_topn_1m` | Generated orderbook depth |
| + 5 continuous aggregates | OHLC, spreads, liquidity |

## Stack

- **TimescaleDB** (PG16, Docker) — hypertables, continuous aggregates, time bucketing
- **Grafana** (Docker) — 7 auto-provisioned dashboards
- **Python 3.13** (asyncio + aiohttp) — ETL pipeline with rate limiting
- **Docker Compose** — two containers, one command

## Scheduled Ingestion

```bash
./run_scheduler.sh --loop 900    # every 15 minutes
# or via cron:
# */15 * * * * cd /path/to/polymarket-warehouse && ./run_scheduler.sh >> data/scheduler.log 2>&1
```

## File Layout

```
polymarket-warehouse/
├── docker-compose.yml
├── run_scheduler.sh
├── sql/                    # 8 migration files (001-008)
├── etl/
│   ├── run_ingestors_once.py   # Pipeline orchestrator
│   ├── ingestors/              # 6 data ingestors
│   ├── derive/                 # Trade normalization + view refresh
│   └── sources/                # Rate-limited HTTP client
├── dashboards/             # 7 Grafana JSON dashboards
└── grafana/provisioning/   # Auto-provisioned datasource
```

