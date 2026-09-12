#!/usr/bin/env bash
#
# stop_all.sh — Stop all running generator processes by their PID files.
#
# Sends SIGTERM (which the GracefulShutdown handler catches), waits, then
# verifies they're gone.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROCESSES=(
    order_placer
    order_lifecycle
    late_tipper
    reviewer
    driver_status
    menu_changer
    payment_processor
)

echo "Stopping generator processes..."
for p in "${PROCESSES[@]}"; do
    pidfile="logs/${p}.pid"
    if [ ! -f "$pidfile" ]; then
        echo "  ${p}: no pid file"
        continue
    fi
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        echo "  ${p}: sending SIGTERM to pid $pid"
        kill -TERM "$pid"
    else
        echo "  ${p}: pid $pid not running"
    fi
    rm -f "$pidfile"
done

echo
echo "Waiting 5s for clean shutdown..."
sleep 5

# Force-kill anything still alive
for p in "${PROCESSES[@]}"; do
    pgrep -f "${p}.py" | while read -r leftover; do
        echo "  ${p}: force-killing leftover pid $leftover"
        kill -KILL "$leftover" 2>/dev/null || true
    done
done

echo "Done."
