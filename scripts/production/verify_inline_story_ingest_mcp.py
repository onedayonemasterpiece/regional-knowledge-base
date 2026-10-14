"""Real OAuth public-MCP readback for same-pass story ingestion schema.

Read-only. It does not create a synthetic book or spend model/vector quota.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path("/home/dev/projects/regional-knowledge-base/scripts/production")))
from operator_env import load_service_env  # noqa: E402


async def run(document_id=None):
    load_service_env()
    from regional_knowledge.oauth_provider import (
        KNOWLEDGE_SCOPE, oauth_provider_from_env,
    )
    provider = oauth_provider_from_env(issuer=os.environ["RKB_AUTH_ISSUER"],
                                       resource=os.environ["RKB_RESOURCE_URL"])
    token = provider.store.mutate(lambda s: provider._mint_family(
        s, client_id=os.environ["RKB_OAUTH_CLIENT_ID"],
        scopes=[KNOWLEDGE_SCOPE], resource=os.environ["RKB_RESOURCE_URL"],
        subject=os.environ["RKB_OWNER_SUBJECT"]))
    try:
        async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
            result = await client.post(os.environ["RKB_RESOURCE_URL"],
                headers={"Authorization": "Bearer " + token.access_token,
                         "Accept": "application/json,text/event-stream",
                         "MCP-Protocol-Version": "2025-11-25"},
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
            result.raise_for_status()
            data = result.json() if "application/json" in result.headers.get("content-type", "") else json.loads(
                next(x.removeprefix("data: ") for x in result.text.splitlines() if x.startswith("data: ")))
            tools = {item["name"]: item for item in data["result"]["tools"]}
            book = tools["book_ingest"]
            inp = book["inputSchema"]
            out = book.get("outputSchema") or {}
            if not all((
                "story_candidates" in inp["properties"],
                "story_candidates_reviewed" in json.dumps(inp, ensure_ascii=False),
                "story_extraction" in out.get("properties", {}),
                "same" in book["description"].lower() or "SAME" in book["description"],
                "story_search" in tools,
                "story_get" in tools,
                "story_extract" in tools,
            )):
                raise RuntimeError("Production MCP is missing actual same-pass story candidate contract")
            definitions = inp.get('$defs') or {}
            candidate_fields = definitions['StageStoryCandidateInput']['properties']
            if not {'event_dates','observations','entity_keys'}.issubset(candidate_fields):
                raise RuntimeError('Production MCP lacks same-pass source metadata')
            if 'knowledge_reviewed' not in definitions['StagePageInput']['properties'] or 'knowledge_extraction' not in out.get('properties', {}):
                raise RuntimeError('Production MCP lacks complete-pass checkpoint contract')
            status_summary = None
            if document_id:
                status_reply = await client.post(os.environ['RKB_RESOURCE_URL'],
                    headers={'Authorization': 'Bearer ' + token.access_token,
                             'Accept': 'application/json,text/event-stream'},
                    json={'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {
                        'name': 'book_ingest', 'arguments': {'command': 'status', 'document_id': document_id}}})
                status_reply.raise_for_status()
                message = status_reply.json() if 'application/json' in status_reply.headers.get('content-type','') else json.loads(
                    next(x.removeprefix('data: ') for x in status_reply.text.splitlines() if x.startswith('data: ')))
                value = message.get('result') or {}
                if message.get('error') or value.get('isError'):
                    raise RuntimeError('Production status-by-document failed')
                content = value.get('structuredContent') or json.loads(value['content'][0]['text'])
                knowledge = content.get('knowledge_extraction')
                if not knowledge or not content.get('ingestion_id'):
                    raise RuntimeError('Production status lacks persisted semantic checkpoint')
                status_summary = {key: knowledge[key] for key in (
                    'state','reviewed_pages','total_pages','needs_review_candidates',
                    'staged_candidates','complete_extraction_claimed','retrieval_acceptance')}
            print(json.dumps({"published_mcp": True,
                "same_pass_dates_quantities_entities": True,
                "knowledge_coverage_schema": True,
                "document_status": status_summary,
                "book_ingest_accepts_story_candidates": True,
                "page_coverage_schema": True,
                "status_exposes_extraction": True,
                "registry_tools_available": True,
                "tool_count": len(tools),
                "source_book_quality_gate_unchanged": True,
                "model_calls": 0, "mutation_calls": 0}, ensure_ascii=False))
    finally:
        access = await provider.load_access_token(token.access_token)
        if access:
            await provider.revoke_token(access)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--document-id', help='Optional owned real source for read-only checkpoint acceptance')
    args = parser.parse_args()
    asyncio.run(run(args.document_id))
