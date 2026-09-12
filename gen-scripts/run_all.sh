#!/usr/bin/env bash
#
# run_all.sh — Launch all 6 generator processes in the background.
#
# Logs go to ./logs/<process>.log. PIDs go to ./logs/<process>.pid.
# Use stop_all.sh to stop them cleanly.
#
# Each process honors environment variables for tuning (see individual files).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p logs

PROCESSES=(
    order_placer
    order_lifecycle
    late_tipper
    reviewer
    driver_status
    menu_changer
    payment_processor
)

echo "Starting ${#PROCESSES[@]} generator processes..."
for p in "${PROCESSES[@]}"; do
    if [ -f "logs/${p}.pid" ] && kill -0 "$(cat logs/${p}.pid)" 2>/dev/null; then
        echo "  ${p} already running (pid $(cat logs/${p}.pid)); skipping"
        continue
    fi

    nohup python3 "${p}.py" >> "logs/${p}.log" 2>&1 &
    echo $! > "logs/${p}.pid"
    echo "  ${p} started (pid $!)"
done

echo
echo "All processes started. Logs in ./logs/"
echo "Tail combined logs:    tail -f logs/*.log"
echo "Stop everything:       ./stop_all.sh"
