#!/usr/bin/env bash
set -euo pipefail
root=$1
candidate=$2
shift 2
cpu=$(python3 -c 'import os; print(min(os.sched_getaffinity(0)))')
launch=$(python3 -c 'import time; print(time.monotonic())')
unit="rkb-bench-${candidate}-$(date +%s)"
systemd-run --user --unit="$unit" \
  -p WorkingDirectory="$(pwd)" -p MemoryMax=1G -p MemorySwapMax=0 -p CPUQuota=100% \
  -p MemoryAccounting=yes \
  -p "StandardOutput=append:$root/$unit.log" -p "StandardError=append:$root/$unit.log" \
  --setenv="BENCH_LAUNCH_MONOTONIC=$launch" \
  /usr/bin/taskset -c "$cpu" "$root/venv/bin/python" scripts/benchmarks/small_embedding_worker.py "$root" "$candidate" "$@"
printf '%s\n' "$unit"
