#!/usr/bin/env bash
set -euo pipefail
root=$1
previous=$2
wait_unit() {
  while systemctl --user is-active --quiet "$1"; do sleep 2; done
  systemctl --user show "$1" -p Result -p ExecMainStatus > "$root/$1.status"
  journalctl --user -u "$1" --no-pager > "$root/$1.journal"
}
wait_unit "$previous"
next=$(bash scripts/benchmarks/run_small_embedding.sh "$root" potion --potion-mmap | tail -n 1)
wait_unit "$next"
next=$(bash scripts/benchmarks/run_small_embedding.sh "$root" e5 --live-only | tail -n 1)
wait_unit "$next"
next=$(bash scripts/benchmarks/run_small_embedding.sh "$root" gemma --live-only | tail -n 1)
wait_unit "$next"
