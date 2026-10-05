"""Private release gate for retrieval modes over frozen source-grounded cases.

The fixture is operator/private data and never belongs in Git. It must contain
natural questions, explicit target chunk IDs, query/source languages and minimum
thresholds. This script measures BGE-only, E5-only, lexical-only and E5+BGE
without silently substituting one branch for another.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import time
from pathlib import Path

from operator_env import load_service_env
from regional_knowledge.bge_contract import SPACE as BGE_SPACE, validate_vector as validate_bge
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.contracts import Principal
from regional_knowledge.e5_contract import SPACE as E5_SPACE, validate_vector as validate_e5
from regional_knowledge.rank_fusion import fuse
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.supabase_backend import _embedder_from_env, _object_store_from_env


def vector_literal(values: list[float]) -> str:
    return "[" + ",".join(format(value, ".9g") for value in values) + "]"


async def wait_bge(queue: BgeQueue, actor: str, query: str, timeout: float) -> list[float]:
    digest = hashlib.sha256(query.encode()).hexdigest()
    job = queue.enqueue(
        actor,
        "release-gate:query:" + digest,
        [query],
        identity={"query_sha256": digest, "gate": "retrieval-v1"},
    )
    deadline = time.monotonic() + timeout
    while True:
        row = await asyncio.to_thread(queue.result, actor, job)
        if row["state"] == "done":
            result = row["result"]
            if result["space"] != BGE_SPACE:
                raise ValueError("BGE release-gate space mismatch")
            return validate_bge(result["vectors"][0])
        if time.monotonic() >= deadline:
            raise TimeoutError("BGE release-gate query timeout")
        await asyncio.sleep(0.1)


def rank(rows: list[dict], branches: list[str]) -> list[str]:
    ids, _ = fuse(rows, branches, limit=10)
    return ids


def score(cases: list[dict], results: dict[str, dict[str, list[str]]]) -> dict:
    output = {}
    for mode, by_case in results.items():
        ranks = []
        for case in cases:
            target = set(case["target_ids"])
            ordered = by_case[case["id"]]
            found = next((i for i, value in enumerate(ordered, 1) if value in target), None)
            ranks.append(found)
        output[mode] = {
            "n": len(cases),
            "hit1": sum(value == 1 for value in ranks) / len(ranks),
            "hit5": sum(value is not None and value <= 5 for value in ranks) / len(ranks),
            "hit10": sum(value is not None and value <= 10 for value in ranks) / len(ranks),
            "mrr10": sum(1 / value if value is not None and value <= 10 else 0 for value in ranks) / len(ranks),
        }
    return output


def enforce(fixture: dict, metrics: dict, cases: list[dict]) -> None:
    thresholds = fixture.get("thresholds") or {}
    for mode, expected in thresholds.items():
        if mode not in metrics:
            raise ValueError("threshold for unknown mode: " + mode)
        for metric, minimum in expected.items():
            actual = metrics[mode][metric]
            if actual < float(minimum):
                raise ValueError(
                    f"retrieval gate failed: {mode}.{metric}={actual:.4f} < {minimum}"
                )
    cross = [
        case for case in cases
        if case.get("query_language") == "ru" and case.get("source_language") == "de"
    ]
    minimum_cross = int(fixture.get("minimum_ru_de_cases", 1))
    if len(cross) < minimum_cross:
        raise ValueError(
            f"retrieval gate needs at least {minimum_cross} RU->DE cases, got {len(cross)}"
        )


async def run(args) -> None:
    load_service_env()
    fixture = json.loads(args.cases.read_text())
    cases = fixture.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("nonempty cases required")
    ids = [case.get("id") for case in cases]
    if any(not value for value in ids) or len(ids) != len(set(ids)):
        raise ValueError("unique case IDs required")
    for case in cases:
        if not case.get("query") or not case.get("target_ids"):
            raise ValueError("query and target_ids required")
        if case.get("query_language") not in {"ru", "de"}:
            raise ValueError("query_language must be ru/de for this gate")
        if case.get("source_language") not in {"ru", "de"}:
            raise ValueError("source_language must be ru/de for this gate")

    database = Path(os.environ["RKB_SQLITE_CORPUS_PATH"])
    backend = SQLiteBackend(
        os.environ["KB_SUPABASE_SESSION_CONNECTION"],
        corpus_path=database,
        embedder=_embedder_from_env(),
        object_store=_object_store_from_env(),
        pool_max_size=2,
    )
    document_id = fixture["document_id"]
    document = backend.corpus.one("rkb_documents", document_id)
    if not document:
        raise LookupError("release-gate document missing")
    actor = Principal(
        subject=document["owner_user_id"],
        client_id="retrieval-release-gate",
        issuer="application-actor-bridge",
        access_token="private-operator-gate",
    )
    headers = backend._headers(actor)
    queue = BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])
    results = {mode: {} for mode in ("lexical", "e5", "bge", "e5_bge")}
    details = []
    try:
        for case in cases:
            query = case["query"]
            e5 = validate_e5(await backend.embedder.embed(query))
            bge = await wait_bge(queue, actor.subject, query, args.bge_timeout)
            common = {
                "query_text": query,
                "depth": 100,
                "aliases": [],
                "lexical_timeout_ms": args.lexical_budget_ms,
            }
            lexical_rows = (
                await backend.local_rankings(
                    "rkb_multilingual_rankings",
                    {**common, "include_lexical": True},
                    headers,
                )
            ).json()
            e5_rows = (
                await backend.local_rankings(
                    "rkb_multilingual_rankings",
                    {
                        **common,
                        "include_lexical": False,
                        "e5_vector": vector_literal(e5),
                        "e5_space": E5_SPACE,
                    },
                    headers,
                )
            ).json()
            bge_rows = (
                await backend.local_rankings(
                    "rkb_multilingual_rankings",
                    {
                        **common,
                        "include_lexical": False,
                        "bge_vector": vector_literal(bge),
                        "bge_space": BGE_SPACE,
                    },
                    headers,
                )
            ).json()
            fused_rows = (
                await backend.local_rankings(
                    "rkb_multilingual_rankings",
                    {
                        **common,
                        "include_lexical": False,
                        "e5_vector": vector_literal(e5),
                        "e5_space": E5_SPACE,
                        "bge_vector": vector_literal(bge),
                        "bge_space": BGE_SPACE,
                    },
                    headers,
                )
            ).json()
            ranked = {
                "lexical": rank(lexical_rows, ["lexical"]),
                "e5": rank(e5_rows, ["e5"]),
                "bge": rank(bge_rows, ["bge"]),
                "e5_bge": rank(fused_rows, ["e5", "bge"]),
            }
            for mode, ordered in ranked.items():
                results[mode][case["id"]] = ordered
            details.append({
                "id": case["id"],
                "query_language": case["query_language"],
                "source_language": case["source_language"],
                "ranks": {
                    mode: next(
                        (
                            index for index, value in enumerate(ordered, 1)
                            if value in set(case["target_ids"])
                        ),
                        None,
                    )
                    for mode, ordered in ranked.items()
                },
            })

        metrics = score(cases, results)
        enforce(fixture, metrics, cases)
        report = {
            "schema_version": 1,
            "document_revision": document["active_revision"],
            "source_sha256": document["source_sha256"],
            "case_count": len(cases),
            "ru_de_cases": sum(
                case["query_language"] == "ru" and case["source_language"] == "de"
                for case in cases
            ),
            "metrics": metrics,
            "details": details,
            "passed": True,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        args.output.chmod(0o600)
        print(json.dumps({k: report[k] for k in ("case_count", "ru_de_cases", "metrics", "passed")}))
    finally:
        await backend.aclose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--bge-timeout", type=float, default=30.0)
    parser.add_argument("--lexical-budget-ms", type=int, default=100)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
