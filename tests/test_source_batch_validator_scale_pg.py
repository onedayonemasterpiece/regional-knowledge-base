"""Bounded pure-helper profile in the existing disposable PG17 CI fixture.

No live SQL, prepare_source_batch_v1, accepted-command replay or provider call.
The original one-packet compiler supplies the identities and schema shapes;
only synthetic proof/knowledge arrays are scaled deterministically.
"""
import copy
from statistics import median
from time import perf_counter
from uuid import UUID, uuid5
import warnings

import pytest
from psycopg.types.json import Jsonb

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_stage import canonical, digest
from test_source_batch_compiler import passage_link_packet
from test_native_source_materializer import SOURCE, PERSON
from test_source_activation_pg import image_pg_sql18, TIME
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database  # exact disposable PG17 fixture dependency


def ident(label, ordinal):
    return str(uuid5(UUID(SOURCE), f"pg17-validator:{label}:{ordinal}"))


async def large_projection():
    original = passage_link_packet()
    raw = canonical(original)

    async def count(text):
        return min(512, max(1, len(text.split()) + 2))

    doc, _media = await NativeSourceCompiler(count=count, current=lambda: True).compile_batch(
        [raw], batch_index=0, source_id=SOURCE, person_id=PERSON,
        last_batch_sha256=digest(raw),
    )
    passage_template = doc["passages"][0]
    region_template = doc["semantic"]["regions"][0]
    finding_template = doc["semantic"]["findings"][0]
    assert finding_template["evidence"] and doc["research"]["state"] == "pending"

    passages = []
    for ordinal in range(1600):
        item = copy.deepcopy(passage_template)
        item.update(passage_id=ident("passage", ordinal), ordinal=ordinal)
        passages.append(item)

    regions = []
    for ordinal in range(700):
        item = copy.deepcopy(region_template)
        item.update(region_id=ident("region", ordinal))
        regions.append(item)

    findings, pending, poi_facts, finding_entities = [], [], [], []
    for ordinal in range(800):
        finding = copy.deepcopy(finding_template)
        fid = ident("finding", ordinal)
        key = f"synthetic-candidate-{ordinal:04d}"
        region = regions[ordinal % len(regions)]
        passage = passages[ordinal % len(passages)]
        finding.update(assertion_id=fid, candidate_key=key, entity_keys=[])
        finding["evidence"] = [dict(
            region_id=region["region_id"], passage_id=passage["passage_id"],
        )]
        finding["event_dates"] = []
        finding["observations"] = []
        findings.append(finding)
        pending.append(dict(candidate_key=key, finding_ids=[fid], reason="needs_context"))
        poi_facts.append(dict(evidence=[dict(
            region_id=region["region_id"], page_id=region["page_id"],
        )]))
        finding_entities.append(dict(assertion_id=fid, entity_keys=[], entity_ids=[]))

    doc["passages"] = passages
    doc["semantic"].update(regions=regions, findings=findings, stories=[], pending=pending)
    # SQL026 passes the stripped semantic payload to its pure SQL028 helper.
    # Manifest reconciliation is a distinct owner-DB path; do not fake stage rows.
    doc.pop("image_manifest", None)
    doc.pop("source_graph", None)
    semantic = dict(doc, codec="rkb.source-activation-semantic.v1")
    base = dict(semantic, codec="rkb.source-activation-base.v1")
    base.pop("semantic")
    graph = dict(doc, source_graph=dict(
        source_id=SOURCE, owner_person_id=PERSON,
        entities=[], references=[], relations=[],
        poi_facts=poi_facts, finding_entities=finding_entities,
    ))
    return graph, canonical(semantic), canonical(base), findings[-1]["evidence"][0]


@PG
@pytest.mark.asyncio
async def test_pure_sql026_028_029_helpers_at_observed_book_scale(image_pg_sql18):
    """Profile only SELECT helpers, never VOLATILE prepare_source_batch_v1."""
    graph, semantic, base, evidence = await large_projection()
    assert len(graph["passages"]) == 1600
    assert len(graph["semantic"]["regions"]) == 700
    assert len(graph["semantic"]["findings"]) == 800
    assert len(graph["source_graph"]["finding_entities"]) == 800
    conn = image_pg_sql18["connection"]
    measurements = {}
    expected = Jsonb({"absent_source": "true"})
    ids = Jsonb({"source_id": SOURCE})
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout = '25000ms'")
        await conn.execute(
            "CREATE TEMP TABLE rkb_sql032_profile (doc jsonb, semantic bytea, base bytea, evidence jsonb) ON COMMIT DROP",
        )
        await conn.execute(
            "INSERT INTO pg_temp.rkb_sql032_profile VALUES (%s,%s,%s,%s)",
            (Jsonb(graph), semantic, base, Jsonb(evidence)),
        )

        statements = (
            ("sql029_base", """SELECT rkb_v4.check_source_batch_base_v1(
                x.base,%s::jsonb,%s::jsonb,%s) FROM pg_temp.rkb_sql032_profile x""",
             (ids, expected, TIME), True),
            ("sql028_evidence", """SELECT rkb_v4.check_source_passage_evidence_v1(
                x.doc,x.evidence) FROM pg_temp.rkb_sql032_profile x""", (), True),
            ("sql028_semantic", """SELECT rkb_v4.check_source_batch_semantic_v1(
                x.semantic,%s::jsonb,%s::jsonb,%s) FROM pg_temp.rkb_sql032_profile x""",
             (ids, expected, TIME), True),
            ("sql028_graph_no_source_graph", """SELECT rkb_v4.check_source_batch_graph_v1(
                x.doc-'source_graph',%s::uuid,89) FROM pg_temp.rkb_sql032_profile x""", (SOURCE,), True),
            ("sql028_graph_no_poi_facts", """SELECT rkb_v4.check_source_batch_graph_v1(
                jsonb_set(x.doc,'{source_graph,poi_facts}','[]'::jsonb),%s::uuid,89)
                FROM pg_temp.rkb_sql032_profile x""", (SOURCE,), True),
            ("sql028_graph_no_finding_entities", """SELECT rkb_v4.check_source_batch_graph_v1(
                jsonb_set(x.doc,'{source_graph,finding_entities}','[]'::jsonb),%s::uuid,89)
                FROM pg_temp.rkb_sql032_profile x""", (SOURCE,), True),
            ("sql028_graph_only_finding_evidence", """SELECT rkb_v4.check_source_batch_graph_v1(
                jsonb_set(jsonb_set(x.doc,'{source_graph,poi_facts}','[]'::jsonb),
                  '{source_graph,finding_entities}','[]'::jsonb),%s::uuid,89)
                FROM pg_temp.rkb_sql032_profile x""", (SOURCE,), True),
            ("sql028_graph", """SELECT rkb_v4.check_source_batch_graph_v1(
                x.doc,%s::uuid,89) FROM pg_temp.rkb_sql032_profile x""", (SOURCE,), True),
            ("sql026_conflicts", """SELECT rkb_v4.source_batch_conflicts_v1(
                %s::uuid,x.doc) FROM pg_temp.rkb_sql032_profile x""", (SOURCE,), False),
        )
        for name, sql, params, value in statements:
            timings = []
            for _ in range(2):
                start = perf_counter()
                actual = (await (await conn.execute(sql, params)).fetchone())[0]
                timings.append(round(1000 * (perf_counter() - start), 2))
                if actual is not value:
                    raise AssertionError(f"{name} returned {actual!r}, expected {value!r}")
            measurements[name] = round(median(timings), 2)

    # The diagnostic baseline deliberately prints exact per-helper server-side
    # SELECT durations in the GitHub CI warning summary, not a live/RCA claim.
    warnings.warn(f"RKB_SQL032_BASELINE disposable_pg17 synthetic "
        f"passages=1600 regions=700 findings=800 timings_ms={measurements}",
        UserWarning, stacklevel=1)
