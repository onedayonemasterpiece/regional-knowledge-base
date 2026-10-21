"""Isolated source-backed synthetic vector validation regression + honest benchmark.

No import of regional_knowledge runtime, PG, torch, tokenizers or pytest:
the AST loads the exact existing Python functions, including the nested
execute_query.checked_encode closure from both old and changed sources.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
from collections.abc import Sequence
import hashlib
import json
import math
from pathlib import Path
import random
import resource
import statistics
import struct
import subprocess
import sys
import textwrap
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BASE_MAIN = "f2c6ad0ba9df283a08240ee31de5ae771c2820eb"
SEARCH = "src/regional_knowledge/kaggle_search.py"
QUERY = "src/regional_knowledge/kaggle_query.py"
BGE = "src/regional_knowledge/bge_contract.py"
DIM = 1024


def read_source(path, *, baseline=False):
    if baseline:
        result = subprocess.run(["git", "show", f"{BASE_MAIN}:{path}"],
                                cwd=ROOT, capture_output=True, text=True,
                                timeout=10, check=True)
        return result.stdout
    return (ROOT / path).read_text(encoding="utf-8")


def extract(source, name, namespace):
    tree = ast.parse(source)
    found = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    if len(found) != 1:
        raise AssertionError(f"expected one tracked top-level function {name}")
    exec(compile(ast.Module(body=found, type_ignores=[]), f"<tracked:{name}>", "exec"), namespace)


class DenseUnavailable(RuntimeError):
    pass


def load_path(*, baseline=False):
    """Load real source functions without requiring production dependencies."""
    scope = {"Sequence": Sequence, "math": math, "asyncio": asyncio,
             "DenseUnavailable": DenseUnavailable}
    bge_source = read_source(BGE, baseline=baseline)
    bge_tree = ast.parse(bge_source)
    dimensions = [ast.literal_eval(n.value) for n in bge_tree.body
                  if isinstance(n, ast.Assign) and any(
                      isinstance(t, ast.Name) and t.id == "DIMENSION" for t in n.targets)]
    if dimensions != [DIM]:
        raise AssertionError("source BGE dimension drift")
    scope["DIMENSION"] = DIM
    extract(bge_source, "validate_vector", scope)
    search_source = read_source(SEARCH, baseline=baseline)
    if not baseline:
        extract(search_source, "validate_vector_values", scope)
    extract(search_source, "vector_literal", scope)
    query_source = read_source(QUERY, baseline=baseline)
    query_tree = ast.parse(query_source)
    parents = [n for n in query_tree.body if isinstance(n, ast.AsyncFunctionDef)
               and n.name == "execute_query"]
    if len(parents) != 1:
        raise AssertionError("execute_query missing in pinned source")
    children = [n for n in ast.walk(parents[0]) if isinstance(n, ast.AsyncFunctionDef)
                and n.name == "checked_encode"]
    if len(children) != 1:
        raise AssertionError("expected one nested checked_encode from production source")
    calls = {n.func.id for n in ast.walk(children[0])
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    required = "vector_literal" if baseline else "validate_vector_values"
    forbidden = "validate_vector_values" if baseline else "vector_literal"
    if required not in calls or forbidden in calls:
        raise AssertionError("checked_encode source has unexpected validation boundary")
    factory = "def make_checked(encode):\n" + textwrap.indent(
        ast.unparse(children[0]), "    ") + "\n    return checked_encode\n"
    exec(compile(factory, "<tracked nested checked_encode>", "exec"), scope)
    return scope, scope["make_checked"]


def to_f32(value):
    return struct.unpack("!f", struct.pack("!f", value))[0]


def valid_vectors():
    basis = [1.0] + [0.0] * (DIM - 1)
    values = [
        basis,
        [-1.0] + [-0.0] * (DIM - 1),
        [1] + [0] * (DIM - 1),
        [0.99801] + [0.0] * (DIM - 1),
        [1.00199] + [0.0] * (DIM - 1),
        [1.0, -0.0, 1e-4, 1e-5, 1e-16, 1e-50, 1e-250] + [0.0] * (DIM - 7),
    ]
    rng = random.Random(10461)
    for _ in range(36):
        sample = [to_f32(rng.gauss(0, 1)) for _ in range(DIM)]
        norm = math.sqrt(sum(x * x for x in sample))
        values.append([to_f32(x / norm) for x in sample])
    return values


def invalid_vectors():
    return [
        None, (1.0,) + (0.0,) * (DIM - 1), "1.0", [],
        [True] + [0.0] * (DIM - 1),
        ["1.0"] + [0.0] * (DIM - 1),
        [None] + [0.0] * (DIM - 1),
        [1.0] * (DIM - 1), [1.0] * (DIM + 1),
        [0.0] * DIM, [0.5] + [0.0] * (DIM - 1),
        [1.01] + [0.0] * (DIM - 1),
        [float("nan")] + [0.0] * (DIM - 1),
        [float("inf")] + [0.0] * (DIM - 1),
        [-float("inf")] + [0.0] * (DIM - 1),
        [10 ** 1000] + [0] * (DIM - 1),
    ]


async def encoder_returning(values):
    return values


class VectorValidationTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.old, old_factory = load_path(baseline=True)
        cls.new, new_factory = load_path(baseline=False)
        # Avoid unittest binding the extracted functions as instance methods.
        cls.old_factory = staticmethod(old_factory)
        cls.new_factory = staticmethod(new_factory)

    async def test_identical_pgvector_text_and_validated_list_identity(self):
        for i, vector in enumerate(valid_vectors()):
            with self.subTest(i=i):
                old_checked = self.old_factory(lambda _: encoder_returning(vector))
                new_checked = self.new_factory(lambda _: encoder_returning(vector))
                original = await old_checked("synthetic-query")
                changed = await new_checked("synthetic-query")
                self.assertIs(original, vector)
                self.assertIs(changed, vector)
                expected = self.old["vector_literal"](original)
                self.assertEqual(self.new["vector_literal"](changed), expected)
                self.assertEqual(
                    self.new["validate_vector_values"](vector),
                    self.old["validate_vector"](vector))
                self.assertTrue(expected.startswith("[") and expected.endswith("]"))

    async def test_bad_encoder_fails_early_before_hybrid_and_no_raw_reason_leak(self):
        for i, vector in enumerate(invalid_vectors()):
            with self.subTest(i=i):
                old_checked = self.old_factory(lambda _: encoder_returning(vector))
                new_checked = self.new_factory(lambda _: encoder_returning(vector))
                with self.assertRaises(DenseUnavailable) as old_error:
                    await old_checked("synthetic-query")
                with self.assertRaises(DenseUnavailable) as new_error:
                    await new_checked("synthetic-query")
                self.assertEqual(str(new_error.exception), str(old_error.exception))
                self.assertEqual(str(new_error.exception), "resident encoder is unavailable")

    def test_downstream_format_is_still_strict_for_every_future_caller(self):
        for i, values in enumerate(invalid_vectors()):
            with self.subTest(i=i):
                with self.assertRaises(Exception) as baseline:
                    self.old["vector_literal"](values)
                with self.assertRaises(type(baseline.exception)) as changed:
                    self.new["vector_literal"](values)
                self.assertEqual(str(changed.exception), str(baseline.exception))

    async def test_validation_remains_independent_of_formatting(self):
        async def encode(_):
            return [1.0] + [0.0] * (DIM - 1)
        original_literal = self.new["vector_literal"]
        try:
            self.new["vector_literal"] = lambda _: (_ for _ in ()).throw(
                AssertionError("discarded formatting unexpectedly invoked"))
            values = await self.new_factory(encode)("synthetic-query")
            self.assertEqual(len(values), DIM)
        finally:
            self.new["vector_literal"] = original_literal

    async def test_encoder_exceptions_sanitized_as_previous_dense_failure(self):
        async def failing(_):
            raise RuntimeError("synthetic-private-data")
        for factory in (self.old_factory, self.new_factory):
            with self.assertRaises(DenseUnavailable) as error:
                await factory(failing)("synthetic-query")
            self.assertNotIn("synthetic-private-data", str(error.exception))

    async def test_cancellation_propagates_and_shielded_owner_remains_running(self):
        for factory in (self.old_factory, self.new_factory):
            native_gate, started = asyncio.Event(), asyncio.Event()
            active = []
            async def long_forward():
                started.set()
                await native_gate.wait()
                return [1.0] + [0.0] * (DIM - 1)
            async def encode(_):
                native = asyncio.create_task(long_forward())
                active.append(native)
                return await asyncio.shield(native)
            task = asyncio.create_task(factory(encode)("synthetic-query"))
            try:
                await asyncio.wait_for(started.wait(), 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(len(active), 1)
                self.assertFalse(active[0].done())
            finally:
                native_gate.set()
                await asyncio.gather(*active)

    def test_exact_bge_contract_and_minimal_boundary(self):
        search_source, query_source = read_source(SEARCH), read_source(QUERY)
        self.assertIn("return validate_vector(values)", search_source)
        self.assertIn("validate_vector_values(values)", query_source)
        self.assertNotIn("vector_literal(values)  # Invalid dense output", query_source)



class ExecuteQueryBoundaryTests(unittest.IsolatedAsyncioTestCase):
    """Run the real execute_query AST with strictly in-memory fake SQL/inference.

    The injected fused-search callback calls its real checked_encode closure,
    so invalid dense results must take the existing DenseUnavailable branch
    and become unavailable/mode=None rather than returning hybrid evidence.
    """

    @classmethod
    def setUpClass(cls):
        from types import SimpleNamespace
        scope, _ = load_path(baseline=False)

        class Query:
            def __init__(self):
                self.request_id = "synthetic-executor"
                self.connection_generation = 1
                self.turn_generation = 0
                self.query = "synthetic"
                self.deadline_ms = 2000
                self.min_data_seq = 0
                self.evidence_limit = 3
                self.filters = SimpleNamespace(kinds=None, source_ids=None)

        class PinnedTokenizer:
            async def count(self, text):
                return 12

        class QueryResult:
            def __init__(self, **fields):
                self.__dict__.update(fields)

        class ReadUnavailable(Exception):
            def __init__(self, reason):
                self.reason = reason

        class TokenizerUnavailable(Exception):
            pass

        class SyntheticPsycopgError(Exception):
            pass

        async def fused_search(conn, text, *, encode, **kwargs):
            # Invalid encoder output cannot pass this callback's checked_encode.
            await encode(text)
            raise AssertionError("invalid vector incorrectly reached hybrid success")

        scope.update({
            "Query": Query,
            "AcceptanceReadRequest": type("AcceptanceReadRequest", (), {}),
            "PinnedTokenizer": PinnedTokenizer,
            "QueryResult": QueryResult,
            "ReadUnavailable": ReadUnavailable,
            "TokenizerUnavailable": TokenizerUnavailable,
            "DenseUnavailable": DenseUnavailable,
            "MAX_TOKENS": 512,
            "fused_search": fused_search,
            "_scope": lambda query: None,
            "psycopg": SimpleNamespace(Error=SyntheticPsycopgError, errors=SimpleNamespace(
                UndefinedTable=type("UndefinedTable", (SyntheticPsycopgError,), {}))),
            "encode_envelope": lambda result: b"synthetic",
        })
        extract(read_source(QUERY), "_identity", scope)
        extract(read_source(QUERY), "_unavailable", scope)
        extract(read_source(QUERY), "execute_query", scope)
        cls.execute = staticmethod(scope["execute_query"])
        cls.Query = Query
        cls.PinnedTokenizer = PinnedTokenizer

    async def _run_probe(self, encode):
        from contextlib import asynccontextmanager
        lease = {"entered": 0, "exited": 0}
        @asynccontextmanager
        async def acquire():
            lease["entered"] += 1
            try:
                yield object()
            finally:
                lease["exited"] += 1
        try:
            result = await self.execute(self.Query(), acquire=acquire,
                                        tokenizer=self.PinnedTokenizer(), encode=encode)
            return result, lease
        except BaseException:
            self.assertEqual(lease, {"entered": 1, "exited": 1})
            raise

    async def test_actual_execute_query_never_returns_hybrid_for_bad_dense_values(self):
        for i, vector in enumerate(invalid_vectors()):
            with self.subTest(i=i):
                async def encode(_):
                    return vector
                result, lease = await self._run_probe(encode)
                self.assertEqual((result.status, result.mode, result.reason),
                                 ("unavailable", None, "dense_unavailable"))
                self.assertEqual(lease, {"entered": 1, "exited": 1})

    async def test_actual_execute_query_propagates_cancel_and_cleans_lease(self):
        async def encode(_):
            raise asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await self._run_probe(encode)


def percentiles_ns(samples):
    ordered = sorted(samples)
    def p(value):
        return round(ordered[max(0, math.ceil(len(ordered) * value) - 1)] / 1000, 3)
    return {"p50_us": p(.5), "p95_us": p(.95),
            "mean_us": round(statistics.mean(ordered) / 1000, 3)}


def rss_kib():
    try:
        return next(int(line.split()[1]) for line in
                    Path("/proc/self/status").read_text().splitlines()
                    if line.startswith("VmRSS:"))
    except (StopIteration, OSError):
        return None


async def benchmark(iterations, *, smoke=False):
    if not 10 <= iterations <= 5000:
        raise ValueError("bounded 10..5000 iterations per variant")
    old, old_factory = load_path(baseline=True)
    new, new_factory = load_path(baseline=False)
    vectors = valid_vectors()
    selected = [vectors[0]]
    async def fake_encode(_):
        return selected[0]
    old_checked = old_factory(fake_encode)
    new_checked = new_factory(fake_encode)

    async def original_path():
        return old["vector_literal"](await old_checked("synthetic-query"))
    async def changed_path():
        return new["vector_literal"](await new_checked("synthetic-query"))
    for vector in vectors:
        selected[0] = vector
        if await original_path() != await changed_path():
            raise AssertionError("original and changed path textual parity failed")

    paths = {"a": original_path, "d": changed_path}
    for i in range(40):
        selected[0] = vectors[i % len(vectors)]
        await original_path()
        await changed_path()
    samples = {key: {"wall": [], "cpu": []} for key in paths}
    baseline_rss = rss_kib()
    baseline_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rng = random.Random(85271)
    for i in range(iterations):
        selected[0] = vectors[i % len(vectors)]
        order = ["a", "d"]
        rng.shuffle(order)
        for name in order:
            start_wall = time.perf_counter_ns()
            start_cpu = time.process_time_ns()
            await paths[name]()
            samples[name]["cpu"].append(time.process_time_ns() - start_cpu)
            samples[name]["wall"].append(time.perf_counter_ns() - start_wall)

    sha = {name: hashlib.sha256(read_source(name).encode()).hexdigest()
           for name in (SEARCH, QUERY, BGE)}
    return {
        "scope": "SYNTHETIC_ONLY_NOT_MCP_OR_KAGGLE_LIVE",
        "baseline_main": BASE_MAIN,
        "source_sha256": sha,
        "smoke": smoke,
        "iterations_each": iterations,
        "A_unmodified_two_format_checks": {
            "wall": percentiles_ns(samples["a"]["wall"]),
            "cpu": percentiles_ns(samples["a"]["cpu"])},
        "D_actual_checked_validation_then_downstream_format": {
            "wall": percentiles_ns(samples["d"]["wall"]),
            "cpu": percentiles_ns(samples["d"]["cpu"])},
        "process_rss_kib_before": baseline_rss,
        "process_rss_kib_after": rss_kib(),
        "process_peak_rss_kib_before": baseline_peak,
        "process_peak_rss_kib_after": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "comment": "Source-extracted nested checked_encode + downstream literal; no SQL, model, "
                   "network, tokenizer, admission, WSS or production performance conclusion. "
                   "Both paths still fully validate on both sides; changed path no longer "
                   "allocates/throws away an intermediate pgvector text string.",
    }


def main():
    if "--benchmark" not in sys.argv:
        unittest.main()
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(benchmark(args.iterations, smoke=args.smoke))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
