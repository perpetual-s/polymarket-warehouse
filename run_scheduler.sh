#!/usr/bin/env bash
# Sparta DB Bonus — simple cron-style scheduler.
# Runs the full ingestion + derivation pipeline on an interval.
#
# Usage:
#   ./run_scheduler.sh              # run once (good for cron)
#   ./run_scheduler.sh --loop 900   # run every 15 minutes (daemon)
#
# Cron example (every 15 min):
#   */15 * * * * cd /path/to/sparta-db-bonus && ./run_scheduler.sh >> data/scheduler.log 2>&1

set -euo pipefail
cd "$(dirname "$0")"

CONDA_ENV="athos"
TRACKED_COUNT="${TRACKED_COUNT:-100}"
PMA_MAX="${PMA_MAX:-100000}"
ACTIVITY_MAX_PAGES="${ACTIVITY_MAX_PAGES:-4}"
HOLDERS_ALL_MARKETS="${HOLDERS_ALL_MARKETS:-0}"
LOOP_INTERVAL="${2:-0}"

# Activate conda
eval "$(conda shell.bash hook)"
conda activate "$CONDA_ENV"

run_once() {
    echo "========================================"
    echo "Pipeline run: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "========================================"
    args=(python etl/run_ingestors_once.py --tracked-count "$TRACKED_COUNT" --pma-max "$PMA_MAX" --activity-max-pages "$ACTIVITY_MAX_PAGES")
    if [[ "$HOLDERS_ALL_MARKETS" == "1" ]]; then
        args+=(--holders-all-markets)
    fi
    "${args[@]}"
    echo ""
}

if [[ "${1:-}" == "--loop" ]] && [[ "$LOOP_INTERVAL" -gt 0 ]]; then
    echo "Scheduler starting: interval=${LOOP_INTERVAL}s, tracked=${TRACKED_COUNT}, pma_max=${PMA_MAX}, activity_pages=${ACTIVITY_MAX_PAGES}, holders_all_markets=${HOLDERS_ALL_MARKETS}"
    while true; do
        run_once || echo "ERROR: pipeline run failed at $(date -u)"
        echo "Sleeping ${LOOP_INTERVAL}s..."
        sleep "$LOOP_INTERVAL"
    done
else
    run_once
fi
