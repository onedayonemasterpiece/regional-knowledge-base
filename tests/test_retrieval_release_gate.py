import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
PRODUCTION = ROOT / "scripts" / "production"


def load_gate():
    sys.path.insert(0, str(PRODUCTION))
    try:
        spec = importlib.util.spec_from_file_location(
            "retrieval_release_gate",
            PRODUCTION / "verify_retrieval_release_gate.py",
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(PRODUCTION))


def fixture_cases():
    return [
        {
            "id": "ru-de",
            "query_language": "ru",
            "source_language": "de",
            "target_ids": ["a"],
        },
        {
            "id": "de-de",
            "query_language": "de",
            "source_language": "de",
            "target_ids": ["b"],
        },
    ]


def test_score_keeps_modes_independent_and_counts_known_targets():
    gate = load_gate()
    cases = fixture_cases()
    results = {
        "lexical": {"ru-de": [], "de-de": ["x", "b"]},
        "e5": {"ru-de": ["x", "a"], "de-de": ["b"]},
        "bge": {"ru-de": ["a"], "de-de": ["b"]},
        "e5_bge": {"ru-de": ["x", "a"], "de-de": ["b"]},
    }
    scored = gate.score(cases, results)
    assert scored["lexical"]["hit10"] == 0.5
    assert scored["e5"]["hit1"] == 0.5
    assert scored["bge"]["hit1"] == 1.0
    assert scored["e5_bge"]["hit5"] == 1.0


def test_enforce_requires_ru_de_coverage_and_thresholds():
    gate = load_gate()
    cases = fixture_cases()
    metrics = {
        mode: {"hit1": 1.0, "hit5": 1.0, "hit10": 1.0, "mrr10": 1.0, "n": 2}
        for mode in ("lexical", "e5", "bge", "e5_bge")
    }
    gate.enforce(
        {"thresholds": {"bge": {"hit10": 0.8}}, "minimum_ru_de_cases": 1},
        metrics,
        cases,
    )
    with pytest.raises(ValueError, match="RU->DE"):
        gate.enforce(
            {"thresholds": {}, "minimum_ru_de_cases": 2},
            metrics,
            cases,
        )
    with pytest.raises(ValueError, match="retrieval gate failed"):
        gate.enforce(
            {"thresholds": {"bge": {"hit10": 1.01}}, "minimum_ru_de_cases": 1},
            metrics,
            cases,
        )
