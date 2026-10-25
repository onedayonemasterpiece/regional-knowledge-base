"""No-PG SQL contract tests for the unregistered reconciliation SourceReader."""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_story_reconcile import ReconcileError
from regional_knowledge.kaggle_story_reconcile_read import (
    NEXT_SQL, SOURCE_SQL, REVISION_SQL, PgReconciliationSourceReader,
    decode_source_cursor, encode_source_cursor, MIN_UUID,
)

ACTOR = "10000000-0000-0000-0000-000000000001"
OTHER = "10000000-0000-0000-0000-000000000002"
A = "00000000-0000-0000-0000-000000000001"
B = "00000000-0000-0000-0000-000000000002"
ASSERTION = "30000000-0000-0000-0000-000000000001"
FRONTIER = "60000000-0000-0000-0000-000000000001"
REVISION = ("synthetic-approved-schema", "synthetic-approved-functions")


class NativeFailure(Exception):
    def __init__(self, sqlstate):
        self.sqlstate = sqlstate


def row(source=A, key="a01", index=1, *, source_pass="complete",
        reconciliation="open", revision=1, eligibility=1, exact=True, quote=None):
    assertion = f"30000000-0000-0000-0000-{index:012d}"
    frontier = f"60000000-0000-0000-0000-{int(source[-1]):012d}"
    page = f"40000000-0000-0000-0000-{index:012d}"
    region = f"50000000-0000-0000-0000-{index:012d}"
    return (source, key, assertion, revision, frontier, source_pass,
            reconciliation, eligibility, "a" * 64, page, region, 4,
            quote or f"Literal witnessed passage {index}", exact, 1)


class FakeCursor:
    def __init__(self, database):
        self.db = database
        self.one = None
        self.many = None

    async def execute(self, statement, params=()):
        self.db.calls.append((statement, params))
        if statement == REVISION_SQL:
            self.one = self.db.revision
        elif "FROM rkb_v4.story_assertion_versions a" in statement:
            if self.db.sqlstate:
                raise NativeFailure(self.db.sqlstate)
            self.db.used_sql = statement
            self.db.used_params = params
            if statement == NEXT_SQL:
                source, candidate, limit = params
                self.many = [item for item in self.db.items
                             if (item[0], item[1]) > (source, candidate)][:limit]
            elif statement == SOURCE_SQL:
                source, assertion = params
                self.many = [item for item in self.db.items
                             if item[0] == source and item[2] == assertion][:2]
            else:
                raise AssertionError("unexpected source SQL")
        elif statement.startswith("SET TRANSACTION ISOLATION") or "set_config" in statement:
            pass
        else:
            raise AssertionError("unexpected SQL: " + statement)

    async def fetchone(self):
        return self.one

    async def fetchall(self):
        return list(self.many)


class FakeDatabase:
    def __init__(self, records):
        self.items = sorted(records,key=lambda r:(r[0],r[1]))
        self.revision = REVISION
        self.calls = []
        self.used_sql = None
        self.used_params = None
        self.sqlstate = None
        self.cursor_instance = FakeCursor(self)
        self.transaction_calls = 0
        self.leases = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_calls += 1
        yield

    @asynccontextmanager
    async def cursor(self):
        yield self.cursor_instance

    @asynccontextmanager
    async def acquire(self):
        self.leases += 1
        yield self


class FakeAuthority:
    def __init__(self, database, *, actor=ACTOR):
        self.actor = actor
        self.revoked = False
        self.revoked_after_sql = False
        self.require_calls = 0
        self.db = database

    async def require(self, verified, capability):
        self.require_calls += 1
        assert capability == "public_search"
        assert verified.person_id == self.actor
        if self.revoked or self.revoked_after_sql and self.db.used_sql:
            raise PermissionError("current canonical access revoked")
        return self.actor


class FakeSpool:
    def __init__(self):
        self.facts = SimpleNamespace(pending=False,unknown=False,
                                     scope_sequence=4,covered_sequence=4)
        self.changed = False

    async def read_facts(self, _):
        return self.facts

    def facts_current(self, old):
        return old is self.facts and not self.changed


async def synthetic_frontier(cursor, execute, query, observed, *, require_dense):
    assert require_dense is False
    assert query.min_data_seq <= 10
    observed.update(database="test-db-1", data=10, projection=10, visibility=2)


def reader(records, *, epoch=3, approved=True):
    db = FakeDatabase(records)
    verified = SimpleNamespace(person_id=ACTOR,context_ref="context-1",
                               auth_revision=epoch)
    authority = FakeAuthority(db)
    spool = FakeSpool()
    state = {"current": True}
    adapter = PgReconciliationSourceReader(
        acquire=db.acquire,read_authority=authority,
        verified_authority=verified,binding=SimpleNamespace(min_data_seq=10),
        binding_current=lambda _:state["current"],
        gate=SimpleNamespace(spool=spool),
        approved_revisions=REVISION if approved else None,
        frontier=synthetic_frontier,connection_check=lambda conn:conn is db,
    )
    return adapter, db, authority, spool, state, verified


def test_exact_frozen_schema_and_indexes_without_full_corpus_scan():
    for sql in (NEXT_SQL, SOURCE_SQL):
        assert "FROM rkb_v4.story_assertion_versions a" in sql
        assert "rkb_v4.source_research_frontier f" in sql
        assert "rkb_v4.source_import_origins o" in sql
        assert "rkb_v4.source_visibility v" in sql
        assert "rkb_v4.source_regions r" in sql
        assert "rkb_v4.active_revision ar" in sql
        assert "rkb_v4.story_reconciliation_cursor c" in sql, "missing independent cross-book cursor"
        assert "s.accepted AND NOT s.withdrawn" in sql
        assert "ar.revision=a.source_revision" in sql
        assert "v.eligibility_revision" in sql
        assert "o.original_sha256" in sql
        assert "substring(r.source_text" in sql
        assert "SELECT *" not in sql.upper() and "OFFSET " not in sql.upper()
        assert "text_start" not in sql, "uses actual SQL007 evidence.start, not a made-up source column"
    assert "(a.source_id,a.candidate_key) > (%s::uuid,%s)" in NEXT_SQL
    assert "ORDER BY a.source_id,a.candidate_key" in NEXT_SQL
    assert "LIMIT %s" in NEXT_SQL
    assert "a.assertion_id=%s::uuid" in SOURCE_SQL and "LIMIT 2" in SOURCE_SQL
    assert "INSERT" not in NEXT_SQL.upper() and "UPDATE" not in SOURCE_SQL.upper()
    assert NEXT_SQL.count("%s") == 3 and SOURCE_SQL.count("%s") == 2


@pytest.mark.asyncio
async def test_keyset_pages_all_assertions_without_skipping_long_source():
    records = [row(key=f"a{i:02d}",index=i) for i in range(1,12)]
    records += [row(B,key="b01",index=20,source_pass="pending")]
    adapter, db, auth, spool, state, verified = reader(records)
    curs = None
    found = []
    seen_page_sizes = []
    for _ in range(12):
        page = await adapter.next_sources(ACTOR,after=curs,limit=3)
        seen_page_sizes.append(sum(len(x.proofs) for x in page.items))
        found.extend((x.source_id,p.assertion_id) for x in page.items for p in x.proofs)
        if curs is None:
            assert page.items[0].source_pass_state == "complete"
            assert page.items[0].reconciliation_state == "open"
            assert page.items[0].proofs_next_cursor == page.next_cursor
        curs = page.next_cursor
        if curs is None:
            break
    assert len(found) == len(set(found)) == 12
    assert [v[0] for v in found].count(A) == 11
    assert [v[0] for v in found].count(B) == 1
    assert all(size <= 3 for size in seen_page_sizes)
    assert all("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in s
               for s,p in db.calls if s.startswith("SET TRANSACTION ISOLATION"))
    assert db.leases == 4 and db.transaction_calls == 4
    assert auth.require_calls == db.leases * 2
    assert db.used_sql == NEXT_SQL


@pytest.mark.asyncio
async def test_source_exact_assertion_with_indexed_lookup_and_source_pass_complete():
    candidate = row(key="a03",index=3)
    adapter, db, auth, spool, state, verified = reader([candidate])
    result = await adapter.source(ACTOR,A,candidate[2])
    assert result.source_id == A and result.source_revision == 1
    assert result.source_pass_state == "complete"
    assert result.reconciliation_state == "open"
    assert result.proofs[0].region_id == candidate[10]
    assert result.proofs[0].text_start == 4
    assert result.proofs[0].quote == candidate[12]
    assert db.used_sql == SOURCE_SQL and db.used_params == (A,candidate[2])


@pytest.mark.asyncio
async def test_closed_cross_book_is_not_confused_with_complete_source_pass():
    first = row(source_pass="pending",reconciliation="complete")
    adapter, db, *_ = reader([first])
    page = await adapter.next_sources(ACTOR,after=None,limit=3)
    assert page.items[0].source_pass_state == "pending"
    assert page.items[0].reconciliation_state == "complete"


@pytest.mark.parametrize("change", ["bad_original_sha","bad_source_revision","bad_eligibility",
                                    "bad_page","bad_region","bad_excerpt","bad_source_pass",
                                    "bad_reconciliation_state","missing_proof","too_many_proofs"])
@pytest.mark.asyncio
async def test_corrupt_native_provenance_fails_closed_not_silent_skip(change):
    original = list(row())
    if change == "bad_original_sha":original[8]="f" * 65
    if change == "bad_source_revision":original[3]=0
    if change == "bad_eligibility":original[7]=0
    if change == "bad_page":original[9]="not-a-uuid"
    if change == "bad_region":original[10]=None
    if change == "bad_excerpt":original[13]=False
    if change == "bad_source_pass":original[5]="made_up"
    if change == "bad_reconciliation_state":original[6]="unknown"
    if change == "missing_proof":original[14]=0
    if change == "too_many_proofs":original[14]=17
    adapter, db, *_ = reader([tuple(original)])
    with pytest.raises(ReconcileError):
        await adapter.source(ACTOR,A,original[2])


@pytest.mark.asyncio
async def test_stale_cursor_rejects_epoch_and_pg_visibility_change():
    adapter,db,auth,spool,state,verified = reader(
        [row(index=i,key=f"a{i:02d}") for i in range(1,5)])
    first = await adapter.next_sources(ACTOR,after=None,limit=1)
    assert first.next_cursor
    valid = await adapter.next_sources(ACTOR,after=first.next_cursor,limit=1)
    assert valid.items[0].proofs[0].assertion_id != first.items[0].proofs[0].assertion_id
    with pytest.raises(ReconcileError,match="invalid_or_stale_reconciliation_cursor"):
        decode_source_cursor(first.next_cursor,"f"*64)
    changed, *_ = reader(db.items,epoch=4)
    with pytest.raises(ReconcileError,match="invalid_or_stale_reconciliation_cursor"):
        await changed.next_sources(ACTOR,after=first.next_cursor,limit=1)


@pytest.mark.asyncio
async def test_binding_missing_revision_and_foreign_person_fail_before_sql():
    adapter,db,auth,spool,state,verified = reader([row()],approved=False)
    with pytest.raises(ReconcileError,match="reconciliation_schema_unapproved"):
        await adapter.next_sources(ACTOR,after=None,limit=1)
    assert db.leases == 0
    good,db,auth,spool,state,verified = reader([row()])
    with pytest.raises(ReconcileError,match="actor_binding_changed"):
        await good.source(OTHER,A,ASSERTION)
    assert db.leases == 0


@pytest.mark.asyncio
async def test_revocation_and_pending_source_tail_do_not_release_results():
    adapter,db,auth,spool,state,verified = reader([row()])
    auth.revoked = True
    with pytest.raises(PermissionError):
        await adapter.next_sources(ACTOR,after=None,limit=1)
    assert db.leases == 0
    auth.revoked = False
    spool.facts.pending = True
    with pytest.raises(ReconcileError,match="source_history_unavailable"):
        await adapter.next_sources(ACTOR,after=None,limit=1)
    assert db.leases == 0
    spool.facts.pending = False
    auth.revoked_after_sql = True
    with pytest.raises(PermissionError):
        await adapter.next_sources(ACTOR,after=None,limit=1)


@pytest.mark.asyncio
async def test_changed_visibility_or_writer_epoch_after_read_is_not_issued():
    adapter,db,auth,spool,state,verified = reader([row()])
    original = db.cursor_instance.fetchall

    async def change_after_fetch():
        spool.changed = True
        return await original()
    db.cursor_instance.fetchall = change_after_fetch
    with pytest.raises(ReconcileError,match="source_changed_before_issuance"):
        await adapter.next_sources(ACTOR,after=None,limit=1)
    spool.changed = False

    async def change_binding_after_fetch():
        state["current"] = False
        return await original()
    db.cursor_instance.fetchall = change_binding_after_fetch
    with pytest.raises(ReconcileError,match="source_changed_before_issuance"):
        await adapter.next_sources(ACTOR,after=None,limit=1)


@pytest.mark.parametrize("sqlstate", ["42P01","42501"])
@pytest.mark.asyncio
async def test_missing_unactivated_cursor_or_reader_grant_is_explicit_fail_closed(sqlstate):
    adapter,db,auth,spool,state,verified = reader([row()])
    db.sqlstate=sqlstate
    with pytest.raises(ReconcileError,match="reconciliation_cursor_or_reader_grant_unavailable"):
        await adapter.next_sources(ACTOR,after=None,limit=1)
    assert db.used_sql is None
    assert db.leases == 1


@pytest.mark.asyncio
async def test_revision_pin_and_idle_connection_are_required():
    adapter,db,auth,spool,state,verified = reader([row()])
    db.revision=("not-installed","wrong-functions")
    with pytest.raises(ReconcileError,match="reconciliation_revision_mismatch"):
        await adapter.next_sources(ACTOR,after=None,limit=1)
    adapter,db,auth,spool,state,verified = reader([row()])
    from dataclasses import replace
    broken = replace(adapter,connection_check=lambda _:False)
    with pytest.raises(ReconcileError,match="read_connection_not_idle"):
        await broken.next_sources(ACTOR,after=None,limit=1)


@pytest.mark.asyncio
async def test_parameterized_keyset_does_not_inject_sql_and_cursor_is_canonical():
    adapter,db,auth,spool,state,verified = reader([row()])
    page = await adapter.next_sources(ACTOR,after=None,limit=1)
    scope = "a" * 64
    evil = "' OR TRUE --"
    token=encode_source_cursor(scope,A,evil)
    assert decode_source_cursor(token,scope)==(A,evil)
    assert "OR TRUE" not in NEXT_SQL
    assert db.used_params[0] == MIN_UUID and db.used_params[1] == ""
    assert len(json.dumps(page.items[0].proofs[0].__dict__)) < 2048


@pytest.mark.asyncio
async def test_parent_next_accepts_long_valid_source_reader_cursor_without_truncation():
    from regional_knowledge.kaggle_story_reconcile import TargetStoryReconcile, ReconcileActor

    records = [row(key="a" * 128,index=1),row(key="z" * 128,index=2)]
    source,db,auth,spool,state,verified = reader(records)
    class PrincipalAuthority:
        async def resolve(self,principal,*,source_ids,mutate):
            return ReconcileActor(principal)
        async def current(self,actor,*,source_ids,mutate):
            return None
    parent=TargetStoryReconcile(sources=source,authority=PrincipalAuthority(),
                                store=object(),cases=object())
    first=await parent.next(ACTOR,limit=1)
    assert len(first["next_cursor"])>300
    second=await parent.next(ACTOR,after=first["next_cursor"],limit=1)
    assert second["next_cursor"] is None
    assert second["items"][0]["assertions"][0]["assertion_id"] != first["items"][0]["assertions"][0]["assertion_id"]
