#!/usr/bin/env bash
set -euo pipefail
# Reuse the pinned existing E5 environment and model; do not download weights.
ROOT="$(cd -- "$(dirname -- "$0")/../.." && pwd)"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export HF_HUB_OFFLINE=1 TOKENIZERS_PARALLELISM=false
PROGRAM=chunk_size_audit.py
case "${1:-}" in
  fixture) PROGRAM=chunk_audit_fixture.py; shift;;
  bge) PROGRAM=chunk_audit_bge.py; shift;;
  runtime) PROGRAM=chunk_audit_runtime.py; shift;;
  score) PROGRAM=chunk_audit_score.py; shift;;
  diagnostics) PROGRAM=chunk_audit_diagnostics.py; shift;;
  installed) PROGRAM=chunk_audit_installed.py; shift;;
  ablation) PROGRAM=chunk_audit_ablation.py; shift;;
esac
exec nice -n 10 /home/dev/.local/share/regional-knowledge-base/fast-e5/runtime/bin/python "$ROOT/scripts/benchmarks/$PROGRAM" "$@"
