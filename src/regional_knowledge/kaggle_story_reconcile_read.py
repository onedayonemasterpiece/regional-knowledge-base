"""Unwired PG SourceReader; no pool, credentials, writer, migrations or MCP route.

SQL001 sources/active_revision; SQL004 source_visibility; SQL006
source_research_frontier and source_import_origins; SQL007 assertions/regions.
A *separate* reconciliation state and rkb_reader SELECT on source_research_frontier
are missing from frozen f2c6ad0. Querying that future state fails closed, not
by treating source-pass complete as cross-book complete. All reads acquire the
already owned Kaggle PG connection through an injected factory.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass, replace
import hashlib
import json
from types import SimpleNamespace
from typing import Any, Callable
from uuid import UUID

from .kaggle_story_reconcile import Proof, ReconcileError, SourcePage, SourceSnapshot

# SQL007 UNIQUE(source_id,candidate_key) serves this keyset without OFFSET.
# SQL007 source_regions.region_id PK provides exact proof lookup; never
# return full 8k region text to Dev merely to validate a literal excerpt.
_PROOF_SQL = """
SELECT a.source_id::text, a.candidate_key, a.assertion_id::text,
       a.source_revision, f.frontier_id::text, f.state,
       c.reconciliation_state, v.eligibility_revision, o.original_sha256,
       r.page_id::text, r.region_id::text,
       (a.data#>>'{evidence,0,start}')::integer,
       a.data#>>'{evidence,0,original_excerpt}',
       coalesce(r.region_id IS NOT NULL
           AND (a.data#>>'{evidence,0,end}')::integer -
               (a.data#>>'{evidence,0,start}')::integer =
                   char_length(a.data#>>'{evidence,0,original_excerpt}')
           AND substring(r.source_text FROM
               (a.data#>>'{evidence,0,start}')::integer + 1 FOR
               (a.data#>>'{evidence,0,end}')::integer -
               (a.data#>>'{evidence,0,start}')::integer) =
                    a.data#>>'{evidence,0,original_excerpt}',false),
       jsonb_array_length(a.data->'evidence')
FROM rkb_v4.story_assertion_versions a
JOIN rkb_v4.sources s ON s.source_id=a.source_id
JOIN rkb_v4.active_revision ar
    ON ar.source_id=a.source_id AND ar.revision=a.source_revision
JOIN rkb_v4.source_visibility v ON v.source_id=a.source_id
JOIN rkb_v4.source_research_frontier f
    ON f.source_id=a.source_id AND f.revision=a.source_revision
JOIN rkb_v4.source_import_origins o
    ON o.source_id=a.source_id AND o.revision=a.source_revision
JOIN rkb_v4.story_reconciliation_cursor c
    ON c.source_id=a.source_id AND c.source_revision=a.source_revision
       AND c.source_frontier_id=f.frontier_id
LEFT JOIN rkb_v4.source_regions r
    ON r.region_id=(a.data#>>'{evidence,0,region_id}')::uuid
       AND r.source_id=a.source_id AND r.source_revision=a.source_revision
WHERE a.revision=1 AND s.accepted AND NOT s.withdrawn
"""
NEXT_SQL = _PROOF_SQL + """
  AND (a.source_id,a.candidate_key) > (%s::uuid,%s)
ORDER BY a.source_id,a.candidate_key
LIMIT %s
"""
SOURCE_SQL = _PROOF_SQL + """
  AND a.source_id=%s::uuid AND a.assertion_id=%s::uuid
LIMIT 2
"""
REVISION_SQL = "SELECT schema_revision,function_revision FROM rkb_v4.recovery_state WHERE singleton"
MIN_UUID = "00000000-0000-0000-0000-000000000000"
MAX_ASSERTIONS = 8


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _uuid(raw: str) -> str:
    try:
        normalized = str(UUID(raw))
    except (ValueError, TypeError, AttributeError):
        raise ReconcileError("invalid_source_identity") from None
    if raw != normalized:
        raise ReconcileError("invalid_source_identity")
    return raw


def _scope(actor: Any, observed: dict, min_data_seq: int) -> str:
    # Existing fact-read cursor pattern: an exact snapshot-bound locator,
    # NOT an authorization token. All current authority must be rechecked.
    return hashlib.sha256(_canonical(dict(
        person=actor.person_id, context_ref=actor.context_ref,
        auth_revision=actor.auth_revision, min_data_seq=min_data_seq,
        database=observed["database"], data=observed["data"],
        projection=observed["projection"], visibility=observed["visibility"],
    ))).hexdigest()


def encode_source_cursor(scope: str, source_id: str, candidate_key: str) -> str:
    _uuid(source_id)
    if (type(scope) is not str or len(scope) != 64 or
            set(scope) - set("0123456789abcdef") or
            type(candidate_key) is not str or not 1 <= len(candidate_key) <= 128):
        raise ReconcileError("invalid_reconciliation_cursor")
    return base64.urlsafe_b64encode(_canonical(dict(
        v=1,scope=scope,source_id=source_id,candidate_key=candidate_key,
    ))).decode().rstrip("=")


def decode_source_cursor(token: str, scope: str) -> tuple[str,str]:
    try:
        if type(token) is not str or not 1 <= len(token) <= 1024:
            raise ValueError
        body = json.loads(base64.b64decode(
            token + "=" * (-len(token) % 4), altchars=b"-_", validate=True))
        if (type(body) is not dict or
                set(body) != {"v","scope","source_id","candidate_key"} or
                type(body["v"]) is not int or body["v"] != 1 or
                body["scope"] != scope or type(body["candidate_key"]) is not str or
                not 1 <= len(body["candidate_key"]) <= 128):
            raise ValueError
        source = _uuid(body["source_id"])
        if encode_source_cursor(scope,source,body["candidate_key"]) != token:
            raise ValueError
        return source,body["candidate_key"]
    except (ValueError,KeyError,TypeError,UnicodeError,OverflowError,ReconcileError):
        raise ReconcileError("invalid_or_stale_reconciliation_cursor") from None


def _decode_row(row: tuple) -> tuple[SourceSnapshot,str]:
    if type(row) not in (tuple,list) or len(row) != 15:
        raise ReconcileError("source_proof_contract_mismatch")
    (source,candidate,assertion,revision,frontier,source_pass,reconcile,
     eligibility,source_sha,page,region,start,quote,exact,proof_count) = row
    for identifier in (source,assertion,frontier,page,region):
        _uuid(identifier)
    if (type(candidate) is not str or not 1 <= len(candidate) <= 128 or
            type(revision) is not int or revision < 1 or
            type(eligibility) is not int or eligibility < 1 or
            source_pass not in ("pending","complete") or
            reconcile not in ("open","complete") or
            type(start) is not int or not 0 <= start <= 8000 or
            type(quote) is not str or not 1 <= len(quote) <= 8000 or
            type(source_sha) is not str or len(source_sha) != 64 or
            set(source_sha) - set("0123456789abcdef") or
            type(proof_count) is not int or not 1 <= proof_count <= 16 or
            exact is not True):
        raise ReconcileError("source_proof_contract_mismatch")
    proof = Proof(assertion_id=assertion,source_sha256=source_sha,
                  page_id=page,region_id=region,text_start=start,quote=quote)
    proof.validate()
    return SourceSnapshot(source_id=source,source_revision=revision,
        eligibility_revision=eligibility,frontier_id=frontier,
        accepted=True,withdrawn=False,source_pass_state=source_pass,
        reconciliation_state=reconcile,proofs=(proof,)), candidate


def _connection_ready(connection) -> bool:
    # Native-only imports; the lightweight SQL-contract tests need no psycopg.
    from psycopg.pq import TransactionStatus
    from psycopg.rows import tuple_row
    return (not connection.closed and connection.info.transaction_status ==
            TransactionStatus.IDLE and connection.row_factory is tuple_row)


@dataclass(frozen=True)
class PgReconciliationSourceReader:
    """Per-verified-actor read adapter; future owner provides exact read-gate pins.

    Must be assembled with the canonical read authority and existing resident
    WSS worker's idle connection lease; never a standalone PG connector.
    Runtime registration and native migration are deliberately absent.
    """

    acquire: Callable
    read_authority: Any
    verified_authority: Any
    binding: Any
    binding_current: Callable
    gate: Any
    approved_revisions: tuple[str,str] | None = None
    frontier: Callable | None = None
    connection_check: Callable | None = None

    def __post_init__(self):
        if (not callable(self.acquire) or not callable(self.binding_current)
                or not callable(getattr(self.read_authority,"require",None))
                or not callable(getattr(self.gate.spool,"read_facts",None))
                or not callable(getattr(self.gate.spool,"facts_current",None))
                or type(self.verified_authority.person_id) is not str):
            raise TypeError("existing PG lease, read authority, and source gate required")

    async def _read(self, actor_id: str, sql: str, parameter_factory, *, limit: int):
        if actor_id != self.verified_authority.person_id:
            raise ReconcileError("actor_binding_changed")
        if (self.approved_revisions is None or
                type(self.approved_revisions) is not tuple or
                len(self.approved_revisions) != 2 or
                any(type(v) is not str or not v for v in self.approved_revisions)):
            raise ReconcileError("reconciliation_schema_unapproved")
        if not self.binding_current(self.binding):
            raise ReconcileError("read_runtime_changed")
        await self.read_authority.require(self.verified_authority,"public_search")
        facts = await self.gate.spool.read_facts(None)
        if (facts.pending or facts.unknown or
                facts.scope_sequence > facts.covered_sequence or
                not self.gate.spool.facts_current(facts)):
            raise ReconcileError("source_history_unavailable")
        if self.frontier is None:
            from .kaggle_query import _frontier
            frontier = _frontier
        else:
            frontier = self.frontier
        validator = self.connection_check or _connection_ready
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 2
        observed = {}
        try:
            async with asyncio.timeout_at(deadline):
                async with self.acquire() as connection:
                    if not validator(connection):
                        raise ReconcileError("read_connection_not_idle")
                    async with connection.transaction():
                        async with connection.cursor() as cursor:
                            await cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                            async def execute(statement,params=()):
                                ms=max(1,int((deadline-loop.time())*1000))
                                await cursor.execute("SELECT set_config('statement_timeout',%s,true)",(str(ms),))
                                await cursor.execute(statement,params)
                            await frontier(cursor,execute,SimpleNamespace(
                                min_data_seq=self.binding.min_data_seq),observed,require_dense=False)
                            await execute(REVISION_SQL)
                            if tuple(await cursor.fetchone() or ()) != self.approved_revisions:
                                raise ReconcileError("reconciliation_revision_mismatch")
                            scope=_scope(self.verified_authority,observed,self.binding.min_data_seq)
                            await execute(sql,parameter_factory(scope))
                            rows=tuple(await cursor.fetchall())
                            if len(rows)>limit+1:
                                raise ReconcileError("unbounded_source_result")
                if loop.time()>=deadline:
                    raise ReconcileError("deadline_exceeded")
        except TimeoutError:
            raise ReconcileError("deadline_exceeded") from None
        except ReconcileError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # Current PG has no story_reconciliation_cursor; current reader
            # grants also omit source_research_frontier. No fallback to fake
            # complete/open state or inappropriate writer credentials.
            if getattr(error,"sqlstate",None) in ("42P01","42501"):
                raise ReconcileError("reconciliation_cursor_or_reader_grant_unavailable") from None
            raise ReconcileError("source_read_unavailable") from None
        await self.read_authority.require(self.verified_authority,"public_search")
        if not self.binding_current(self.binding) or not self.gate.spool.facts_current(facts):
            raise ReconcileError("source_changed_before_issuance")
        return rows,scope

    async def next_sources(self,actor_id: str,*,after: str | None,limit: int) -> SourcePage:
        if type(limit) is not int or not 1<=limit<=MAX_ASSERTIONS:
            raise ReconcileError("bounded_next_required")
        def params(scope):
            source,key=((MIN_UUID,"") if after is None
                        else decode_source_cursor(after,scope))
            return (source,key,limit+1)
        rows,scope=await self._read(actor_id,NEXT_SQL,params,limit=limit)
        groups: list[SourceSnapshot]=[]
        last_key=None
        for row in rows[:limit]:
            snap,candidate=_decode_row(row)
            if groups and groups[-1].source_id==snap.source_id:
                former=groups[-1]
                if (former.source_revision!=snap.source_revision or
                        former.eligibility_revision!=snap.eligibility_revision or
                        former.frontier_id!=snap.frontier_id or
                        former.source_pass_state!=snap.source_pass_state or
                        former.reconciliation_state!=snap.reconciliation_state):
                    raise ReconcileError("source_page_inconsistent")
                groups[-1]=replace(former,proofs=former.proofs+snap.proofs)
            else:
                groups.append(snap)
            last_key=snap.source_id,candidate
        next_cursor=(encode_source_cursor(scope,*last_key)
                     if len(rows)>limit and last_key is not None else None)
        if next_cursor and groups and str(rows[limit][0])==groups[-1].source_id:
            groups[-1]=replace(groups[-1],proofs_next_cursor=next_cursor)
        return SourcePage(tuple(groups),next_cursor)

    async def source(self,actor_id: str,source_id: str,assertion_id: str) -> SourceSnapshot:
        source_id,assertion_id=_uuid(source_id),_uuid(assertion_id)
        rows,_=await self._read(actor_id,SOURCE_SQL,
            lambda scope:(source_id,assertion_id),limit=1)
        if len(rows)!=1:
            raise ReconcileError("source_assertion_unavailable")
        snap,_=_decode_row(rows[0])
        if (snap.source_id!=source_id or
                snap.proofs[0].assertion_id!=assertion_id):
            raise ReconcileError("source_proof_contract_mismatch")
        return snap
