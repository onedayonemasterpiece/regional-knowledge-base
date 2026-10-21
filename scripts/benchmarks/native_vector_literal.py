#!/usr/bin/env python3
"""Isolated A/B/C Kaggle-local 1024-f64 vector-literal microbenchmark.

No package installs, PG/BGE/provider/network work, or production imports.
A/B execute the exact AST function bodies from the tracked Python sources.
C includes Python validation, input conversion, ctypes FFI, output copy/decode.
"""
from __future__ import annotations

import argparse
import ast
import ctypes
import gc
import hashlib
import json
import math
from pathlib import Path
import platform
import random
import resource
import statistics
import struct
import time
from collections.abc import Sequence

ROOT = Path(__file__).resolve().parents[2]
DIM = 1024
SO = ROOT / "native/vector_literal/target/release/librkb_vector_literal.so"
BASE_MAIN = "f2c6ad0ba9df283a08240ee31de5ae771c2820eb"
OUT_CAPACITY = 65536


def source_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _extract_function(path: Path, name: str, namespace: dict) -> None:
    # Compile the exact tracked function body, without unrelated PG/Torch imports.
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    matches = [node for node in module.body if isinstance(node, ast.FunctionDef)
               and node.name == name]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one tracked function {name}")
    exec(compile(ast.Module(body=[matches[0]], type_ignores=[]),
                 str(path), "exec"), namespace)


def original_python():
    search_path = ROOT / "src/regional_knowledge/kaggle_search.py"
    bge_path = ROOT / "src/regional_knowledge/bge_contract.py"
    bge = ast.parse(bge_path.read_text(encoding="utf-8"))
    dimensions = [ast.literal_eval(node.value) for node in bge.body
                  if isinstance(node, ast.Assign)
                  and any(isinstance(t, ast.Name) and t.id == "DIMENSION"
                          for t in node.targets)]
    if dimensions != [DIM]:
        raise RuntimeError("pinned BGE dimension is not the benchmark dimension")
    scope = {"DIMENSION": DIM, "math": math, "Sequence": Sequence}
    _extract_function(bge_path, "validate_vector", scope)
    _extract_function(search_path, "vector_literal", scope)
    return scope["vector_literal"], {
        str(search_path.relative_to(ROOT)): source_sha(search_path),
        str(bge_path.relative_to(ROOT)): source_sha(bge_path),
    }


def a_untouched_double(original, values):
    # Discard first literal (kaggle_query.checked_encode); use second (PG parameter).
    original(values)
    return original(values)


def b_python_single(original, values):
    return original(values)


class RustVector:
    def __init__(self, path: Path = SO):
        self.path = path
        cold = time.perf_counter_ns()
        self.library = ctypes.CDLL(str(path))
        self.cold_load_ms = (time.perf_counter_ns() - cold) / 1e6
        function = self.library.rkb_vector_literal_v1
        function.argtypes = (
            ctypes.POINTER(ctypes.c_double), ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
        )
        function.restype = ctypes.c_int
        self._function = function
        self.stages = {"python_conversion_ns": [], "ffi_ns": [], "output_decode_ns": []}

    @staticmethod
    def _input(values):
        # Same type/length precedence as source; no bool/string accepted.
        if not isinstance(values, list) or any(type(v) not in (int, float) for v in values):
            raise ValueError("vector values must be numeric, excluding booleans")
        if len(values) != DIM:
            raise ValueError("BGE dimension mismatch")
        # Preserve Python float conversion and OverflowError for huge integers.
        return (ctypes.c_double * DIM)(*[float(v) for v in values])

    def __call__(self, values):
        begin = time.perf_counter_ns()
        numeric = self._input(values)
        output = (ctypes.c_ubyte * OUT_CAPACITY)()
        converted = time.perf_counter_ns()
        written = ctypes.c_size_t()
        code = self._function(numeric, DIM, output, OUT_CAPACITY, ctypes.byref(written))
        ffi_done = time.perf_counter_ns()
        if code:
            errors = {
                1: (ValueError, "BGE dimension mismatch"),
                2: (ValueError, "BGE nonfinite vector"),
                3: (ValueError, "BGE normalization mismatch"),
                4: (BufferError, "native output buffer insufficient"),
                5: (RuntimeError, "invalid native pointer"),
            }
            kind, reason = errors.get(code, (RuntimeError, "unknown native error"))
            raise kind(reason)
        if written.value > OUT_CAPACITY:
            raise RuntimeError("native length exceeds output allocation")
        literal = ctypes.string_at(output, written.value).decode("ascii", errors="strict")
        completed = time.perf_counter_ns()
        self.stages["python_conversion_ns"].append(converted - begin)
        self.stages["ffi_ns"].append(ffi_done - converted)
        self.stages["output_decode_ns"].append(completed - ffi_done)
        return literal

    def raw_capacity_status(self, values, capacity: int):
        numeric = self._input(values)
        output = (ctypes.c_ubyte * max(1, capacity))()
        written = ctypes.c_size_t(0)
        code = self._function(numeric, DIM, output, capacity, ctypes.byref(written))
        return code, written.value

    def raw_wrong_dimension_status(self, values):
        numeric = self._input(values)
        output = (ctypes.c_ubyte * OUT_CAPACITY)()
        written = ctypes.c_size_t(0)
        return self._function(numeric, DIM - 1, output, OUT_CAPACITY, ctypes.byref(written))


def to_f32(number: float) -> float:
    return struct.unpack("!f", struct.pack("!f", number))[0]


def valid_vectors():
    """Reproducible synthetic inputs; no private queries."""
    basis = [1.0] + [0.0] * (DIM - 1)
    negative = [-1.0] + [-0.0] * (DIM - 1)
    near_low = [0.99801] + [0.0] * (DIM - 1)
    near_high = [1.00199] + [0.0] * (DIM - 1)
    int_vector = [1] + [0] * (DIM - 1)
    tiny = [1.0, -0.0, 1e-4, 1e-5, 1e-6, 1e-7, 1e-15, 1e-16, 1e-30]
    tiny.extend([0.0] * (DIM - len(tiny)))
    vectors = [basis, negative, near_low, near_high, int_vector, tiny]
    rng = random.Random(10917)
    for _ in range(16):
        source = [to_f32(rng.gauss(0.0, 1.0)) for _ in range(DIM)]
        norm = math.sqrt(sum(v * v for v in source))
        normalized = [to_f32(v / norm) for v in source]
        vectors.append(normalized)
    sparse = [0.0] * DIM
    sparse[2] = to_f32(0.6)
    sparse[777] = to_f32(0.8)
    sparse[42] = -0.0
    vectors.append(sparse)
    original, _ = original_python()
    for vector in vectors:
        original(vector)
    return vectors


def percentile(values, p: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def microseconds(values_ns):
    return {
        "p50_us": round(percentile(values_ns, 0.50) / 1000, 3),
        "p95_us": round(percentile(values_ns, 0.95) / 1000, 3),
        "mean_us": round(statistics.mean(values_ns) / 1000, 3),
    }


def rss_kib():
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        pass
    return None


def benchmark(mode: str, iterations: int):
    if not 10 <= iterations <= 10000:
        raise ValueError("bounded benchmark requires 10..10000 calls")
    original, source_hashes = original_python()
    vectors = valid_vectors()
    native = RustVector() if mode == "c" else None
    run = {
        "a": lambda v: a_untouched_double(original, v),
        "b": lambda v: b_python_single(original, v),
        "c": native,
    }[mode]

    for vector in vectors:
        expect = original(vector)
        actual = run(vector)
        if actual != expect:
            raise AssertionError("exact original Python text parity failed before benchmark")

    for i in range(45):
        run(vectors[i % len(vectors)])
    if native is not None:
        for series in native.stages.values():
            series.clear()

    before_rss = rss_kib()
    before_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    wall_ns, cpu_ns, thread_cpu_ns = [], [], []
    gc_enabled = gc.isenabled()
    gc.disable()
    try:
        for i in range(iterations):
            v = vectors[i % len(vectors)]
            start_wall = time.perf_counter_ns()
            start_cpu = time.process_time_ns()
            start_thread = time.thread_time_ns()
            run(v)
            thread_cpu_ns.append(time.thread_time_ns() - start_thread)
            cpu_ns.append(time.process_time_ns() - start_cpu)
            wall_ns.append(time.perf_counter_ns() - start_wall)
    finally:
        if gc_enabled:
            gc.enable()

    return {
        "scope": "SYNTHETIC_MICROBENCH_NOT_MCP",
        "variant": mode,
        "semantics": {
            "a": "unchanged Python vector_literal twice",
            "b": "unchanged Python vector_literal once",
            "c": "strict Python list conversion + FFI + Rust + buffer decode",
        }[mode],
        "baseline_main": BASE_MAIN,
        "source_sha256": source_hashes,
        "rust_toolchain_pin": "1.85.1",
        "binary_sha256": hashlib.sha256(SO.read_bytes()).hexdigest() if SO.exists() else None,
        "binary_bytes": SO.stat().st_size if SO.exists() else None,
        "cold_ctypes_load_ms": native.cold_load_ms if native else None,
        "iterations": iterations,
        "fixture_vectors": len(vectors),
        "per_call_wall": microseconds(wall_ns),
        "per_call_process_cpu": microseconds(cpu_ns),
        "per_call_thread_cpu": microseconds(thread_cpu_ns),
        "rust_substages": {key: microseconds(values)
                           for key, values in native.stages.items()} if native else None,
        "rss_kib_start": before_rss,
        "rss_kib_finish": rss_kib(),
        "rss_peak_kib_process": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "rss_peak_kib_before_benchmark": before_peak,
        "platform": {"python": platform.python_version(), "machine": platform.machine(),
                     "system": platform.system()},
        "notes": "Per-call timing includes instrumentation; Rust includes allocations/copies. "
                 "RSS peaks include Python process startup and native load. "
                 "CI runner noise and CPU scaling prevent causal end-to-end claims.",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=("a", "b", "c"))
    parser.add_argument("--iterations", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = benchmark(args.mode, args.iterations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n",
                           encoding="utf-8")
    print(json.dumps({key: result[key] for key in (
        "variant", "iterations", "per_call_wall", "per_call_process_cpu",
        "rss_peak_kib_process", "cold_ctypes_load_ms", "binary_sha256")},
        sort_keys=True))


if __name__ == "__main__":
    main()
