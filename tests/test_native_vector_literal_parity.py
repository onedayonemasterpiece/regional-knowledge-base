"""Exact non-network parity gates for the isolated RKB native vector prototype."""
import math
from pathlib import Path
import random
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/benchmarks"))
from native_vector_literal import (
    DIM, RustVector, a_untouched_double, b_python_single,
    original_python, to_f32, valid_vectors,
)


class TestNativeVectorLiteral(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original, cls.source_hashes = original_python()
        cls.native = RustVector()

    def assert_original_parity(self, values):
        expected = self.original(values)
        self.assertEqual(expected, a_untouched_double(self.original, values))
        self.assertEqual(expected, b_python_single(self.original, values))
        self.assertEqual(expected, self.native(values))

    def assert_same_error(self, values):
        with self.assertRaises(Exception) as reference:
            self.original(values)
        with self.assertRaises(Exception) as native:
            self.native(values)
        self.assertIs(type(native.exception), type(reference.exception))
        self.assertEqual(str(native.exception), str(reference.exception))

    def test_exact_valid_vectors(self):
        for index, values in enumerate(valid_vectors()):
            with self.subTest(index=index):
                self.assert_original_parity(values)

    def test_repeatable_random_fp32(self):
        rng = random.Random(8675309)
        for index in range(96):
            numbers = [to_f32(rng.gauss(0, 1)) for _ in range(DIM)]
            norm = math.sqrt(sum(v * v for v in numbers))
            values = [to_f32(v / norm) for v in numbers]
            with self.subTest(index=index):
                self.assert_original_parity(values)

    def test_float_repr_edge_cases(self):
        values = [1.0, -0.0, 0.0001, 1e-5, 1e-6, 1e-7, 1e-15, 1e-16,
                  1e-30, 1e-50, 1e-100, 1e-200] + [0.0] * (DIM - 12)
        self.assert_original_parity(values)
        txt = self.native(values)
        for token in ("1.0", "-0.0", "0.0001", "1e-05", "1e-06",
                      "1e-07", "1e-15", "1e-16", "1e-30"):
            self.assertIn(token, txt)

    def test_invalid_type_before_dimension(self):
        inputs = [
            None, tuple([1.0] * DIM), "not a list",
            [True] + [0.0] * (DIM - 1),
            [False], ["1.0"] + [0.0] * (DIM - 1),
            [None] + [0.0] * (DIM - 1),
            [object()] + [0.0] * (DIM - 1),
        ]
        for index, value in enumerate(inputs):
            with self.subTest(index=index):
                self.assert_same_error(value)

    def test_invalid_dimensions_nonfinite_norm_and_bigint(self):
        valid = [1.0] + [0.0] * (DIM - 1)
        invalid = [
            [], [1.0] * (DIM - 1), [1.0] * (DIM + 1),
            [0.0] * DIM, [1.1] + [0.0] * (DIM - 1),
            [0.5] + [0.0] * (DIM - 1),
            [float("nan")] + [0.0] * (DIM - 1),
            [float("inf")] + [0.0] * (DIM - 1),
            [-float("inf")] + [0.0] * (DIM - 1),
            [10 ** 1000] + [0.0] * (DIM - 1),
            [2] + [0.0] * (DIM - 1),
            [0.998] + [0.0] * (DIM - 1),
        ]
        invalid.append(valid[:-1] + [float("nan")])
        for index, values in enumerate(invalid):
            with self.subTest(index=index):
                self.assert_same_error(values)

    def test_output_buffer_guard_and_dimension_guard(self):
        valid = [1.0] + [0.0] * (DIM - 1)
        self.assertEqual(self.native.raw_capacity_status(valid, 1), (4, 0))
        self.assertEqual(self.native.raw_wrong_dimension_status(valid), 1)

    def test_source_identification(self):
        self.assertEqual(len(self.source_hashes), 2)
        for filename, sha in self.source_hashes.items():
            self.assertEqual(len(sha), 64)
            self.assertTrue((ROOT / filename).exists())


if __name__ == "__main__":
    unittest.main()
