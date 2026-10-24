"""Synthetic typed QUERY tests; PG uses only the existing strict fixture guard."""
import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
from psycopg.pq import TransactionStatus
import pytest
import pytest_asyncio

from regional_knowledge.bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from regional_knowledge.kaggle_apply import Revisions, apply_accepted, HandlerRegistry, HandlerSpec, ApplyExecutionError
from pg_schema_metadata import initialize_fixture_database as initialize_database
from regional_knowledge.kaggle_protocol import (
    ItemDependencies, Query, QueryFilters, SourceDependency, encode_envelope, decode_envelope,
)
from regional_knowledge.kaggle_query import PinnedTokenizer, execute_query, read_continuation
from regional_knowledge.kaggle_query import _item
from regional_knowledge.kaggle_query import _source_label
from regional_knowledge.kaggle_search import SearchEvidence
from test_kaggle_pg_driver import database, connect
from test_kaggle_apply import pinned
from test_recovery_journal import ConditionalMemoryStore, command
from regional_knowledge.recovery_journal import RecoveryJournal

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "00000000-0000-0000-0000-000000000001"
PASSAGE = "10000000-0000-0000-0000-000000000001"
VECTOR = [1.] + [0.] * 1023
PG = pytest.mark.skipif(not (os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET") or os.environ.get("RKB_KAGGLE_TEST_DSN")),
                        reason="explicit checked disposable PG fixture required")


def query(**changes):
    values = dict(request_id="query-1", connection_generation=1, turn_generation=0,
                  query="semantic harbour", min_data_seq=5, deadline_ms=2000,
                  actor=dict(person_id="verified-fixture", audience="personal", auth_epoch=7, visibility_epoch=999))
    values.update(changes)
    return Query.model_validate(values)


async def count(_):
    return 12


def tokenizer(callback=count):
    return PinnedTokenizer(callback, REVISION, TOKENIZER_SHA256, SPACE)


async def encode(_):
    return VECTOR


@asynccontextmanager
async def forbidden_acquire():
    raise AssertionError("invalid input reached PostgreSQL")
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("filters", [QueryFilters(kinds=("fact",)), QueryFilters(source_ids=("not-uuid",))])
async def test_unsupported_filters_fail_before_tokenizer_inference_or_sql(filters):
    async def forbidden(_):
        raise AssertionError("invalid filter reached tokenizer/encoder")
    result = await execute_query(query(filters=filters), acquire=forbidden_acquire,
                                 tokenizer=tokenizer(forbidden), encode=forbidden)
    assert result.status == "invalid_query" and result.reason == "invalid_filters"


@pytest.mark.asyncio
async def test_exact_pinned_input_count_rejects_513_without_truncation_or_sql():
    seen = []

    async def oversized(text):
        seen.append(text)
        return 513

    original = query(query="𐀀" * 1000)
    result = await execute_query(original, acquire=forbidden_acquire, tokenizer=tokenizer(oversized), encode=encode)
    assert seen == [original.query]
    assert result.reason == "token_limit_exceeded" and result.observed_data_seq is None
    with pytest.raises(ValueError):
        PinnedTokenizer(count, REVISION, "0" * 64, SPACE)


@pytest.mark.asyncio
async def test_total_deadline_covers_tokenizer_and_pool_acquisition():
    async def slow_count(_):
        await asyncio.sleep(.05)
        return 12

    result = await execute_query(query(deadline_ms=10), acquire=forbidden_acquire,
                                 tokenizer=tokenizer(slow_count), encode=encode)
    assert result.reason == "deadline_exceeded"

    @asynccontextmanager
    async def slow_acquire():
        await asyncio.sleep(.05)
        raise AssertionError("deadline failed to bound acquire")
        yield

    telemetry = asyncio.Queue(maxsize=1)
    telemetry.put_nowait({"occupied": 1})
    result = await execute_query(query(deadline_ms=10), acquire=slow_acquire, tokenizer=tokenizer(),
                                 encode=encode, telemetry=telemetry)
    assert result.reason == "deadline_exceeded" and telemetry.qsize() == 1


@pytest.mark.asyncio
async def test_tokenizer_runtime_failure_is_unavailable_and_cancellation_propagates():
    async def failed(_):
        raise RuntimeError("synthetic tokenizer runtime failure")

    result = await execute_query(query(), acquire=forbidden_acquire, tokenizer=tokenizer(failed), encode=encode)
    assert result.status == "unavailable" and result.reason == "not_ready"

    async def cancelled(_):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await execute_query(query(), acquire=forbidden_acquire, tokenizer=tokenizer(cancelled), encode=encode)


def test_exact_source_dependency_coverage_is_strict_when_present():
    source = SourceDependency(source_id=SOURCE, content_revision=1, eligibility_revision=1)
    with pytest.raises(ValueError):
        ItemDependencies(source_ids=(SOURCE, "other"), variant_refs=(), grant_refs=(), source_revisions=(source,))
    with pytest.raises(ValueError):
        ItemDependencies(source_ids=(SOURCE,), variant_refs=(), grant_refs=(), source_revisions=(source, source))
    with pytest.raises(ValueError):
        ItemDependencies(source_ids=(SOURCE,), variant_refs=(), grant_refs=())


def test_json_escaped_metadata_and_terminal_excerpt_remain_explicit():
    source = SourceDependency(source_id=SOURCE, content_revision=1, eligibility_revision=1)
    hit = SearchEvidence(PASSAGE, SOURCE, 1, "\u0001" * 1000, "\u0001" * 16000, "\u0001" * 8000)
    first = _item(hit, source, 0, database_generation="fixture")
    assert len(first.metadata.model_dump_json().encode()) <= 12 * 1024
    assert first.source_continuation.bibliography_offset == 1024
    final = _item(hit, source, 0, database_generation="fixture", text_offset=15999, bibliography_offset=7999)
    assert final.text_form == "excerpt" and final.continuation_complete
    assert final.source_continuation.text_offset == 16000
    assert final.source_continuation.bibliography_offset == 8000



def test_source_labels_from_catalog_use_printed_page_or_distinct_scan_without_invention():
    hit = SearchEvidence(PASSAGE, SOURCE, 1, "pages 4", "text", "saved",
        catalog_metadata={"title": "Die Stadt", "authors": ["Ernst Wiechert"],
            "publication_year": 1975, "language": "de", "catalog": {"kind": "article"}},
        passage_language="de", physical_page_index=3, printed_page_number="14")
    title, kind, language, passage_language = _source_label(hit)
    assert title == "Ernst Wiechert. Die Stadt. 1975, с. 14"
    assert (kind, language, passage_language) == ("article", "de", "de")
    from dataclasses import replace
    assert _source_label(replace(hit, printed_page_number=None))[0].endswith("скан №4")
    # A locator like 'pages 4' is not proof of a printed-page number.
    unknown = SearchEvidence(PASSAGE, SOURCE, 1, "pages 4", "text", "")
    assert _source_label(unknown)[0] == "Источник без установленной библиографии"
    assert "с. 4" not in _source_label(unknown)[0]


@pytest_asyncio.fixture
async def env(database):
    async with database.transaction():
        await database.execute((ROOT / "sql/kaggle/002_recovery.sql").read_text(), prepare=False)
    await initialize_database(database, database_generation="query-fixture", writer_generation="writer-1",
                              revisions=Revisions("s1", "f1", "e1"), applied_seq=5)
    async with database.transaction():
        await database.execute((ROOT / "sql/kaggle/003_search_scope.sql").read_text(), prepare=False)
        await database.execute((ROOT / "sql/kaggle/004_read_fences.sql").read_text(), prepare=False)
    connection = await connect()

    @asynccontextmanager
    async def acquire():
        yield connection

    try:
        yield dict(admin=database, connection=connection, acquire=acquire)
    finally:
        await connection.close()


async def publish(env, seq=5):
    admin = env["admin"]
    async with admin.transaction():
        epoch = (await (await admin.execute("SELECT visibility_epoch FROM rkb_v4.read_frontier")).fetchone())[0]
        await admin.execute("SELECT rkb_v4.publish_read_frontier('query-fixture','writer-1',%s,%s,true,true)", (epoch, seq))


async def actual_apply(env, *, activate=False, reject=False, preparation="next"):
    """Real accepted journal + pinned PG handler, not manual ledger/cursor writes."""
    admin = env["admin"]
    async with admin.transaction():
        seq = (await (await admin.execute("SELECT applied_seq+1 FROM rkb_v4.recovery_state")).fetchone())[0]
    body = ""
    if activate:
        vector = "[" + ",".join(map(str, VECTOR)) + "]"
        body = f"""
            INSERT INTO rkb_v4.revisions VALUES ('{SOURCE}',2);
            INSERT INTO rkb_v4.passage_search
                (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            VALUES ('10000000-0000-0000-0000-000000000002','{SOURCE}',2,0,'page 2',
                convert_from(p,'UTF8'),'simple','pg_catalog.simple','{vector}');
            UPDATE rkb_v4.active_revision SET revision=2 WHERE source_id='{SOURCE}';
        """
        if preparation == "next":
            body += """PERFORM rkb_v4.prepare_read_frontier('query-fixture','writer-1',
                (SELECT visibility_epoch FROM rkb_v4.read_frontier),
                (SELECT applied_seq+1 FROM rkb_v4.recovery_state),true,true);"""
        elif preparation == "old":
            body += """PERFORM rkb_v4.publish_read_frontier('query-fixture','writer-1',
                (SELECT visibility_epoch FROM rkb_v4.read_frontier),
                (SELECT applied_seq FROM rkb_v4.recovery_state),true,true);"""
        elif preparation == "mutate_after":
            body += f"""PERFORM rkb_v4.prepare_read_frontier('query-fixture','writer-1',
                (SELECT visibility_epoch FROM rkb_v4.read_frontier),
                (SELECT applied_seq+1 FROM rkb_v4.recovery_state),true,true);
                UPDATE rkb_v4.sources SET bibliography=bibliography||' late change' WHERE source_id='{SOURCE}';"""
    async with admin.transaction():
        await admin.execute(f"""CREATE FUNCTION rkb_v4.frontier_check_{seq}
            (p bytea, ids jsonb, expected jsonb, logical_time text) RETURNS boolean
            LANGUAGE sql STABLE SECURITY INVOKER AS $$ SELECT expected->>'allow'='yes' $$;
            CREATE FUNCTION rkb_v4.frontier_apply_{seq}
            (p bytea, ids jsonb, expected jsonb, logical_time text) RETURNS bytea
            LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
            BEGIN {body} RETURN 'exact outcome'::bytea; END $$;""", prepare=False)
    payload = b"accepted atomic activation body"
    kind = "domain_command"
    registry = HandlerRegistry([HandlerSpec(kind, "fixture.frontier", Revisions("s1", "f1", "e1"),
        await pinned(admin, f"frontier_check_{seq}"), await pinned(admin, f"frontier_apply_{seq}"),
        frozenset({"allow"}))])
    cmd = command(seq, kind=kind, route="fixture.frontier", schema_revision="s1", function_revision="f1",
                  executor_revision="e1", ids={}, expected_revisions={"allow": "no" if reject else "yes"}, payload=payload)
    journal = RecoveryJournal(ConditionalMemoryStore(), stream=f"atomic-frontier-{seq}")
    seal = await journal.seal([*(command(i) for i in range(1, seq)), cmd], predecessor=None, writer_generation="writer-1")
    return await apply_accepted(env["connection"], journal=journal, seal=seal, command=cmd,
        database_generation="query-fixture", writer_generation="writer-1", registry=registry)


async def frontier_facts(env):
    async with env["admin"].transaction():
        return await (await env["admin"].execute("""SELECT s.applied_seq,f.projection_seq,f.text_ready,f.dense_ready,
            f.visibility_epoch,f.pending_seq,(SELECT count(*) FROM rkb_v4.applied_ops),
            (SELECT revision FROM rkb_v4.active_revision WHERE source_id=%s)
            FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f""", (SOURCE,))).fetchone()


@PG
@pytest.mark.asyncio
async def test_actual_atomic_apply_promotes_prepared_frontier_with_cursor_and_ledger(env):
    await publish(env)
    receipt = await actual_apply(env, activate=True)
    assert receipt.status == "applied"
    assert await frontier_facts(env) == (6, 6, True, True, 1, None, 1, 2)
    result = await execute_query(query(min_data_seq=6), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert result.evidence[0].text == "accepted atomic activation body"
    assert result.observed_data_seq == result.observed_projection_seq == 6
    assert result.evidence[0].dependencies.source_revisions[0].content_revision == 2


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize("late_visibility", [False, True])
async def test_atomic_frontier_promotion_rolls_back_on_late_commit_or_visibility_failure(env, late_visibility):
    await publish(env)
    if not late_visibility:
        async with env["admin"].transaction():
            await env["admin"].execute("""CREATE FUNCTION rkb_v4.fail_after_promotion() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic late commit failure'; END $$;
                CREATE CONSTRAINT TRIGGER fail_after_promotion AFTER INSERT ON rkb_v4.applied_ops
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION rkb_v4.fail_after_promotion()""", prepare=False)
    with pytest.raises(ApplyExecutionError):
        await actual_apply(env, activate=True, preparation="mutate_after" if late_visibility else "next")
    assert await frontier_facts(env) == (5, 5, True, True, 0, None, 0, 1)
    assert (await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)).evidence


@PG
@pytest.mark.asyncio
async def test_orphan_preparation_cannot_commit_or_bless_a_later_command(env):
    await publish(env)
    with pytest.raises(psycopg.Error):
        async with env["admin"].transaction():
            await env["admin"].execute("SELECT rkb_v4.prepare_read_frontier('query-fixture','writer-1',0,6,true,true)")
    assert await frontier_facts(env) == (5, 5, True, True, 0, None, 0, 1)
    await actual_apply(env)
    assert await frontier_facts(env) == (6, 6, True, True, 0, None, 1, 1)


@PG
@pytest.mark.asyncio
async def test_nonsearch_and_rejected_commands_carry_only_an_unchanged_ready_frontier(env):
    await publish(env)
    await actual_apply(env)
    rejected = await actual_apply(env, reject=True)
    assert rejected.status == "rejected_precondition"
    assert await frontier_facts(env) == (7, 7, True, True, 0, None, 2, 1)
    assert (await execute_query(query(min_data_seq=7), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)).evidence


@PG
@pytest.mark.asyncio
async def test_dirty_early_old_cursor_publish_cannot_enable_automatic_carry(env):
    await publish(env)
    await actual_apply(env, activate=True, preparation="old")
    assert await frontier_facts(env) == (6, 5, False, False, 1, None, 1, 2)
    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert result.reason == "not_ready" and not result.evidence




@PG
@pytest.mark.asyncio
async def test_continuation_database_generation_cannot_be_reused_on_a_restored_fork(env):
    await publish(env)
    first = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    locator = first.evidence[0].source_continuation
    async with env["admin"].transaction():
        await env["admin"].execute("UPDATE rkb_v4.recovery_state SET database_generation='different-synthetic-fork'")
        await env["admin"].execute("SELECT rkb_v4.publish_read_frontier('different-synthetic-fork','writer-1',0,5,true,true)")
    result = await read_continuation(query(), locator, acquire=env["acquire"])
    assert result.reason == "evidence_withdrawn" and not result.evidence
    assert result.observed_database_generation == "different-synthetic-fork"


@PG
@pytest.mark.asyncio
async def test_fresh_frontier_and_minseq_fail_before_encoder_with_actual_cursors(env):
    async def forbidden(_):
        raise AssertionError("unready query consumed inference")

    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=forbidden)
    assert result.reason == "not_ready" and result.observed_data_seq == 5
    assert result.observed_projection_seq is None and result.observed_visibility_epoch == 0
    await publish(env)
    result = await execute_query(query(min_data_seq=6), acquire=env["acquire"], tokenizer=tokenizer(), encode=forbidden)
    assert result.reason == "projection_lag" and result.observed_projection_seq == 5
    assert env["connection"].info.transaction_status == TransactionStatus.IDLE


@PG
@pytest.mark.asyncio
async def test_real_fused_query_completes_with_snapshot_source_dependencies(env):
    await publish(env)
    telemetry = asyncio.Queue(maxsize=1)
    result = await execute_query(query(filters=QueryFilters(source_ids=(SOURCE,), kinds=("source",))),
        acquire=env["acquire"], tokenizer=tokenizer(), encode=encode, telemetry=telemetry)
    assert result.status == "ok" and result.reason is None and result.mode == "hybrid"
    assert result.observed_data_seq == result.observed_projection_seq == 5
    assert result.observed_visibility_epoch == 0  # Never echo actor authority epoch999.
    assert result.observed_database_generation == "query-fixture"
    assert len(result.evidence) == 1
    source = result.evidence[0].dependencies.source_revisions[0]
    assert source == SourceDependency(source_id=SOURCE, content_revision=1, eligibility_revision=1)
    assert decode_envelope(encode_envelope(result)) == result
    record = telemetry.get_nowait()
    from hashlib import sha256
    assert record['event'] == 'kaggle_read_completed'
    assert record['request_ref'] == sha256(query().request_id.encode()).hexdigest()
    assert record['connection_generation'] == query().connection_generation
    assert record['turn_generation'] == query().turn_generation
    assert record['executed_branches'] == ('lexical', 'dense')
    assert query().query not in str(record)
    timings = record['stages_ms']
    assert {"encode_ms", "lexical_ms", "dense_ms", "hydrate_ms"} <= set(timings)
    assert all(type(value) is float and value >= 0 for value in timings.values())


@PG
@pytest.mark.asyncio
async def test_retrieval_and_dependencies_share_repeatable_snapshot_during_active_change(env):
    await publish(env)
    started, release = asyncio.Event(), asyncio.Event()

    async def gated(_):
        started.set()
        await release.wait()
        return VECTOR

    task = asyncio.create_task(execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=gated))
    await started.wait()
    async with env["admin"].transaction():
        await env["admin"].execute("INSERT INTO rkb_v4.revisions VALUES (%s,2)", (SOURCE,))
        await env["admin"].execute("""INSERT INTO rkb_v4.passage_search
            (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            VALUES ('10000000-0000-0000-0000-000000000002',%s,2,0,'page 2','new active body',
                    'simple','pg_catalog.simple',%s::vector)""", (SOURCE, "[" + ",".join(map(str, VECTOR)) + "]"))
        await env["admin"].execute("UPDATE rkb_v4.active_revision SET revision=2 WHERE source_id=%s", (SOURCE,))
        await env["admin"].execute("UPDATE rkb_v4.recovery_state SET applied_seq=6")
    await publish(env, 6)
    release.set()
    old = await task
    assert old.observed_data_seq == 5 and old.observed_visibility_epoch == 0
    assert old.evidence[0].dependencies.source_revisions[0].content_revision == 1
    assert old.evidence[0].dependencies.source_revisions[0].eligibility_revision == 1
    current = await execute_query(query(min_data_seq=6), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert current.observed_data_seq == 6 and current.observed_visibility_epoch > 0
    assert current.evidence[0].dependencies.source_revisions[0].content_revision == 2
    assert current.evidence[0].dependencies.source_revisions[0].eligibility_revision > 1


@PG
@pytest.mark.asyncio
async def test_long_unicode_combined_wire_bounds_and_real_reauthorizing_continuation(env):
    body, bibliography = "𐀀" * 16000, "𐀀" * 8000
    admin = env["admin"]
    async with admin.transaction():
        await admin.execute("DELETE FROM rkb_v4.passage_search")
        await admin.execute("UPDATE rkb_v4.sources SET bibliography=%s WHERE source_id=%s", (bibliography, SOURCE))
        for i in range(5):
            await admin.execute("""INSERT INTO rkb_v4.passage_search
                (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
                VALUES (%s,%s,1,%s,%s,%s,'simple','pg_catalog.simple',%s::vector)""",
                (f"10000000-0000-0000-0000-{i+1:012d}", SOURCE, i, "𐀀" * 1000, body,
                 "[" + ",".join(map(str, VECTOR)) + "]"))
        await admin.execute("UPDATE rkb_v4.recovery_state SET applied_seq=6")
    await publish(env, 6)
    q = query(min_data_seq=6, evidence_limit=5)
    result = await execute_query(q, acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert len(result.evidence) == 5 and sum(len(e.text) for e in result.evidence) == 24000
    assert len(encode_envelope(result)) <= 256 * 1024
    assert all(len(e.metadata.model_dump_json().encode()) <= 12 * 1024 for e in result.evidence)
    first = result.evidence[0]
    assert first.text_form == "excerpt" and first.source_continuation is not None
    continuation = await read_continuation(q, first.source_continuation, acquire=env["acquire"])
    assert len(continuation.evidence[0].text) == 8000
    async with admin.transaction():
        await admin.execute("UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s", (SOURCE,))
        await admin.execute("UPDATE rkb_v4.recovery_state SET applied_seq=7")
    await publish(env, 7)
    denied = await read_continuation(query(min_data_seq=7), first.source_continuation, acquire=env["acquire"])
    assert not denied.evidence and denied.reason == "evidence_withdrawn"


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_cancel_cleanup_and_actual_forward_permit_retention(env, cancel):
    await publish(env)
    semaphore = asyncio.Semaphore(1)
    started, finish = asyncio.Event(), asyncio.Event()

    async def occupied(_):
        async with semaphore:
            started.set()
            try:
                await finish.wait()
            except asyncio.CancelledError:
                await finish.wait()  # Actual shared forward still owns its permit.
            return VECTOR

    task = asyncio.create_task(execute_query(query(deadline_ms=100 if not cancel else 2000),
        acquire=env["acquire"], tokenizer=tokenizer(), encode=occupied))
    await started.wait()
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        result = await task
        assert result.reason == "deadline_exceeded" and result.observed_data_seq == 5
    assert semaphore.locked() and env["connection"].info.transaction_status == TransactionStatus.IDLE
    finish.set()
    await asyncio.wait_for(semaphore.acquire(), 1)
    semaphore.release()
    assert (await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)).evidence


@PG
@pytest.mark.asyncio
async def test_frontier_monotonic_constraints_and_projection_writes_invalidate_readiness(env):
    await publish(env)
    admin = env["admin"]
    for statement in ("UPDATE rkb_v4.read_frontier SET projection_seq=4", "UPDATE rkb_v4.read_frontier SET projection_seq=6"):
        with pytest.raises(psycopg.Error):
            async with admin.transaction():
                await admin.execute(statement)
    async with admin.transaction():
        await admin.execute("DELETE FROM rkb_v4.passage_search WHERE passage_id=%s", (PASSAGE,))

    async def forbidden(_):
        raise AssertionError("invalid projection consumed inference")

    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=forbidden)
    assert result.reason == "not_ready" and result.observed_visibility_epoch == 1
    assert env["connection"].info.transaction_status == TransactionStatus.IDLE


@PG
@pytest.mark.asyncio
async def test_inactive_revision_staging_preserves_ready_old_active_search(env):
    await publish(env)
    admin = env["admin"]
    async with admin.transaction():
        await admin.execute("INSERT INTO rkb_v4.revisions VALUES (%s,2)", (SOURCE,))
        await admin.execute("""INSERT INTO rkb_v4.passage_search
            (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            VALUES ('10000000-0000-0000-0000-000000000002',%s,2,0,'staged','not yet serving',
                    'simple','pg_catalog.simple',%s::vector)""", (SOURCE, "[" + ",".join(map(str, VECTOR)) + "]"))
    staged = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert staged.evidence[0].dependencies.source_revisions[0] == SourceDependency(
        source_id=SOURCE, content_revision=1, eligibility_revision=1)
    assert staged.observed_visibility_epoch == 0
    async with admin.transaction():
        await admin.execute("DELETE FROM rkb_v4.passage_search WHERE revision=2")
    assert (await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)).evidence
    async with admin.transaction():
        assert (await (await admin.execute("SELECT text_ready,dense_ready,visibility_epoch FROM rkb_v4.read_frontier")).fetchone()) == (True, True, 0)
        await admin.execute("DELETE FROM rkb_v4.passage_search WHERE revision=1")
    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert result.reason == "not_ready" and result.observed_visibility_epoch == 1


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize("accepted_source,withdrawn", [(False, False), (True, True)])
async def test_unaccepted_or_withdrawn_source_projection_staging_preserves_active_reader(env, accepted_source, withdrawn):
    other = "00000000-0000-0000-0000-000000000002"
    admin = env["admin"]
    async with admin.transaction():
        await admin.execute("INSERT INTO rkb_v4.sources VALUES (%s,'staged source',%s,%s)", (other, accepted_source, withdrawn))
        await admin.execute("INSERT INTO rkb_v4.revisions VALUES (%s,1)", (other,))
        await admin.execute("INSERT INTO rkb_v4.active_revision VALUES (%s,1)", (other,))
    await publish(env)
    baseline = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    async with admin.transaction():
        await admin.execute("""INSERT INTO rkb_v4.passage_search
            (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            VALUES ('10000000-0000-0000-0000-000000000002',%s,1,0,'staged','not serving',
                    'simple','pg_catalog.simple',%s::vector)""", (other, "[" + ",".join(map(str, VECTOR)) + "]"))
        await admin.execute("DELETE FROM rkb_v4.passage_search WHERE source_id=%s", (other,))
    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    assert result.evidence and result.observed_visibility_epoch == baseline.observed_visibility_epoch
    assert result.evidence[0].dependencies == baseline.evidence[0].dependencies


@PG
@pytest.mark.asyncio
async def test_continuation_sql_deadline_returns_unavailable_after_rollback(env):
    await publish(env)
    first = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=encode)
    locator = first.evidence[0].source_continuation
    blocker = await connect()
    try:
        await blocker.execute("LOCK TABLE rkb_v4.passage_search IN ACCESS EXCLUSIVE MODE")
        result = await read_continuation(query(deadline_ms=40), locator, acquire=env["acquire"])
        assert result.reason == "deadline_exceeded" and not result.evidence
        assert env["connection"].info.transaction_status == TransactionStatus.IDLE
        assert not hasattr(result, "mode")  # Exact-row read never claims hybrid search.
    finally:
        await blocker.rollback()
        await blocker.close()


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize("bad_vector", [False, True])
async def test_encoder_failure_never_returns_lexical_evidence_as_hybrid(env, bad_vector):
    await publish(env)
    called = []

    async def failed(text):
        called.append(text)
        if bad_vector:
            return [1.]  # Invalid actual vector cannot be replaced by a lexical hit.
        raise RuntimeError("synthetic resident encoder failure")

    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=failed)
    assert called and result.reason == "dense_unavailable" and result.status == "unavailable"
    assert result.mode is None and not result.evidence
    assert env["connection"].info.transaction_status == TransactionStatus.IDLE


@PG
@pytest.mark.asyncio
async def test_actual_dense_sql_failure_cannot_claim_hybrid_search(env):
    await publish(env)
    called = []

    async def actual_encoder(text):
        called.append(text)
        return VECTOR

    async with env["admin"].transaction():
        await env["admin"].execute("DROP FUNCTION rkb_v4.dense_candidates(public.vector,integer)")
    result = await execute_query(query(), acquire=env["acquire"], tokenizer=tokenizer(), encode=actual_encoder)
    assert called and result.status == "unavailable" and result.mode is None and not result.evidence
    assert env["connection"].info.transaction_status == TransactionStatus.IDLE
