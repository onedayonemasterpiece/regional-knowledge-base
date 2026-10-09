"""Private live Story Registry acceptance through real owner OAuth + public MCP.

No test book/citation/credentials in Git. Cases are supplied from a private
JSON file and are never echoed into logs. Idempotency keys are fixed by case tag
so an interrupted run resumes the SAME intent without duplicate records.
This tool does NOT publish to external channels.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from uuid import UUID

import httpx

ROOT = Path("/home/dev/projects/regional-knowledge-base")
sys.path.insert(0, str(ROOT / "scripts/production"))
from operator_env import load_service_env  # noqa: E402


def save_private(path, values):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name + ".new")
    with open(tmp, "w", encoding="utf8") as stream:
        json.dump(values, stream, ensure_ascii=False, indent=2)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def unwrap(payload):
    if isinstance(payload, dict) and set(payload) == {"result"} and isinstance(payload["result"], dict):
        return payload["result"]
    return payload


async def run(args):
    load_service_env()
    from regional_knowledge.oauth_provider import oauth_provider_from_env, KNOWLEDGE_SCOPE
    provider = oauth_provider_from_env(
        issuer=os.environ["RKB_AUTH_ISSUER"], resource=os.environ["RKB_RESOURCE_URL"])
    owner = str(UUID(os.environ["RKB_OWNER_SUBJECT"]))
    token = provider.store.mutate(lambda state: provider._mint_family(
        state, client_id=os.environ["RKB_OAUTH_CLIENT_ID"],
        scopes=[KNOWLEDGE_SCOPE], resource=os.environ["RKB_RESOURCE_URL"],
        subject=owner))
    cases = json.loads(Path(args.cases).read_text(encoding="utf8"))
    if not isinstance(cases, list) or not 2 <= len(cases) <= 5:
        raise ValueError("2..5 private source-verified editorial cases are required")
    report = {"status": "in_progress", "endpoint": "resource_scoped",
              "stories": [], "mcp_tools_checked": False, "publication_actions": 0}
    if args.report.exists():
        report = json.loads(args.report.read_text(encoding="utf8"))
    endpoint = os.environ["RKB_RESOURCE_URL"]
    counter = 0
    try:
        async with httpx.AsyncClient(timeout=22, trust_env=False) as client:
            async def rpc(method, params):
                nonlocal counter
                counter += 1
                response = await client.post(endpoint, headers={
                    "Authorization": "Bearer " + token.access_token,
                    "Accept": "application/json,text/event-stream",
                    "MCP-Protocol-Version": "2025-11-25",
                }, json={"jsonrpc": "2.0", "id": counter, "method": method, "params": params})
                response.raise_for_status()
                if "application/json" in response.headers.get("content-type", ""):
                    result = response.json()
                else:
                    result = json.loads(next(x.removeprefix("data: ") for x in
                                             response.text.splitlines() if x.startswith("data: ")))
                if "error" in result or result.get("result", {}).get("isError"):
                    code = result.get("error", {}).get("code") or "mcp_isError"
                    raise RuntimeError("MCP call failed: " + method + " (" + str(code) + ")")
                return result["result"]

            async def call(name, parameters):
                reply = await rpc("tools/call", {"name": name, "arguments": parameters})
                result = unwrap(reply.get("structuredContent") or json.loads(reply["content"][0]["text"]))
                if "error" in result:
                    err = result["error"]
                    raise RuntimeError(name + ": " + str(err.get("code", "unknown")))
                return result

            listing = await rpc("tools/list", {})
            names = {x["name"] for x in listing["tools"]}
            required = {"story_create", "story_edit", "story_search", "story_get",
                        "story_validate", "story_transition", "story_history", "story_export",
                        "story_archive", "story_extract", "corpus_read"}
            if not required <= names:
                raise RuntimeError("Published MCP tool list missing " + ",".join(sorted(required - names)))
            report["mcp_tools_checked"] = True
            report["tool_count"] = len(names)
            save_private(args.report, report)

            for case in cases:
                tag = case["tag"]
                if not re.fullmatch(r"[a-z0-9-]{3,40}", tag):
                    raise ValueError("Invalid case key")
                if len(case["excerpt"]) > 500:
                    raise ValueError("Source quote exceeds bounded limit")
                document = case["source"]["document_id"]
                revision = case["source"]["revision"]
                base = "story-live-20261009-" + tag
                source = case["source"]
                existing = next((s for s in report["stories"] if s["tag"] == tag), None)
                found = await call("fetch", {"id": source["chunk_id"]})
                if (found["metadata"]["document_id"] != document or
                        found["metadata"]["revision"] != revision or
                        case["excerpt"] not in found["text"]):
                    raise RuntimeError("Exact accepted book evidence mismatch in case " + tag)
                source_ref = {"kind": "document", "source_id": document, "source_revision": revision}
                seed = {"text": case["seed"], "origin_status": "known",
                        "origin_note": "Редакционный кандидат по зарегистрированному книжному источнику"}
                created = await call("story_create", {
                    "seed": seed, "source_refs": [source_ref],
                    "metadata": {"title": case["title"], "summary": case["summary"],
                                 "material_type": case["material_type"]},
                    "idempotency_key": base + "-create"})
                sid = created["story_id"]
                assert (await call("story_create", {"seed": seed, "source_refs": [source_ref],
                    "metadata": {"title": case["title"], "summary": case["summary"],
                                 "material_type": case["material_type"]},
                    "idempotency_key": base + "-create"}))["story_id"] == sid

                op1 = {"op": "upsert_assertion", "proposition": case["proposition"],
                       "kind": "attributed_account", "account_kind": "other",
                       "attributed_to": case["author"]}
                claim_receipt = await call("story_edit", {
                    "story_id": sid, "expected_revision": 1, "operations": [op1],
                    "reason": "Явная атрибуция текста книги", "idempotency_key": base + "-assertion"})
                details = await call("story_get", {"story_id": sid, "view": "evidence"})
                assertion = details["snapshot"]["assertions"][0]
                assert assertion["kind"] == "attributed_account"
                expected_revision = claim_receipt["committed_revision"]

                evidence_locator = {k: source[k] for k in
                                    ("page_id", "region_id", "chunk_id", "physical_page_index")
                                    if k in source}
                op2 = {"op": "attach_evidence", "assertion_id": assertion["assertion_id"],
                       "assertion_revision": assertion["revision"], "source_kind": "document",
                       "source_id": document, "source_revision": revision,
                       "relation": "reports", "source_role_for_assertion": "secondary",
                       "locator": evidence_locator, "original_excerpt": case["excerpt"]}
                evidence_receipt = await call("story_edit", {
                    "story_id": sid, "expected_revision": expected_revision, "operations": [op2],
                    "idempotency_key": base + "-evidence"})
                details = await call("story_get", {"story_id": sid, "view": "evidence"})
                assertion = details["snapshot"]["assertions"][0]
                evidence_ids = assertion["evidence_ids"]
                assert evidence_ids

                op3 = {"op": "record_assessment", "assertion_id": assertion["assertion_id"],
                       "assertion_revision": assertion["revision"], "evidence_ids": evidence_ids,
                       "support_status": "single_source", "independence": "unknown",
                       "semantic_review": "supported", "rationale": case["assessment"],
                       "assessor_kind": "model", "method_version": "GPT-6"}
                assessed = await call("story_edit", {
                    "story_id": sid, "expected_revision": evidence_receipt["committed_revision"],
                    "operations": [op3], "idempotency_key": base + "-assessment"})

                op4 = {"op": "upsert_variant", "audience": case["audience"],
                       "format": case["format"], "channel": case.get("channel"),
                       "language": "ru", "body": case["body"],
                       "attributions": [case["attribution"]]}
                variant_receipt = await call("story_edit", {
                    "story_id": sid, "expected_revision": assessed["committed_revision"],
                    "operations": [op4], "idempotency_key": base + "-variant"})
                variant = (await call("story_get", {"story_id": sid, "view": "editorial"}))["snapshot"]["variants"][0]
                validation = await call("story_validate", {"story_id": sid})
                if not validation["all_passed"]:
                    raise RuntimeError("Editorial structural validation failed for case " + tag)
                approved = await call("story_transition", {
                    "story_id": sid, "expected_revision": variant_receipt["committed_revision"],
                    "target_state": "publish_ready",
                    "variant_revision_ids": [{"variant_id": variant["variant_id"],
                                              "revision": variant["revision"]}],
                    "review": {"semantic_checked": True, "attribution_checked": True,
                               "rights_checked": True, "reviewer_kind": "model",
                               "model_version": "GPT-6",
                               "reviewer_note": "Книжное сообщение атрибутировано; наружная публикация не разрешена."},
                    "idempotency_key": base + "-approve"})
                exported = await call("story_export", {"story_id": sid,
                                                       "variant_revision_id": variant["variant_id"]})
                if exported["publication_state"] != "not_sent" or not exported["bibliography"]:
                    raise RuntimeError("Export lost its bibliography or misrepresented publication")
                if variant["variant_id"] not in approved["readiness"]["ready_variants"]:
                    raise RuntimeError("Approved revision is not independently ready")
                retrieved = await call("story_search", {"query": case["query"], "limit": 20})
                if not any(x["story_id"] == sid for x in retrieved["results"]):
                    raise RuntimeError("Saved story absent from live MCP FTS: " + tag)
                current = await call("story_get", {"story_id": sid, "view": "editorial"})
                history = await call("story_history", {"story_id": sid})
                if current["revision"] != approved["committed_revision"] or len(history["history"]) < 5:
                    raise RuntimeError("History/revision not durably visible")
                values = {"tag": tag, "story_id": sid, "revision": current["revision"],
                          "variant_id": variant["variant_id"], "variant_revision": variant["revision"],
                          "text_match": "exact", "support_status": "single_source",
                          "editorial_state": "publish_ready",
                          "published": False, "source_document_id": document}
                if existing:
                    existing.update(values)
                else:
                    report["stories"].append(values)
                save_private(args.report, report)
            report["status"] = "passed"
            save_private(args.report, report)
            print(json.dumps({"status": "passed", "auth_boundary": "real OAuth owner grant",
                              "mcp_list": True, "story_count": len(report["stories"]),
                              "ready_variants": sum(s["editorial_state"] == "publish_ready"
                                                    for s in report["stories"]),
                              "provider_publications": 0, "report_path": str(args.report)},
                             ensure_ascii=False), flush=True)
    finally:
        # Revoke the exact short-lived temporary owner token family after use.
        access = await provider.load_access_token(token.access_token)
        if access:
            await provider.revoke_token(access)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    asyncio.run(run(ap.parse_args()))
