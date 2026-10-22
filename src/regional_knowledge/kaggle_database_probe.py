"""Bounded ledger, runtime compatibility and read-frontier observations."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import math
import time

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg.errors import QueryCanceled


class DatabaseProbeUnavailable(RuntimeError):
    """Sanitized fixed code; never native diagnostics or private values."""


def native_probe_for_schema(schema_revision=None,*,timeout_seconds=2.0):
    """Legacy selector is ignored; readiness reads the installed integer ledger."""
    return NativeDatabaseProbe(timeout_seconds=timeout_seconds)


def _state(state):
    keys = ('database_generation','schema_revision','function_revision','executor_revision')
    if any(not isinstance(state.get(k),str) or not 1<=len(state[k].encode())<=256 for k in keys):
        raise DatabaseProbeUnavailable('database_state_invalid')
    if any(state.get(k) is not None and (type(state[k]) is not int or not 0<=state[k]<=2**63-1)
           for k in ('applied_seq','projection_seq','visibility_epoch')) or state.get('applied_seq') is None:
        raise DatabaseProbeUnavailable('database_state_invalid')
    if (state.get('projection_seq') is not None and state['projection_seq']>state['applied_seq']
            or type(state.get('text_search_ready')) is not bool or type(state.get('dense_search_ready')) is not bool
            or state.get('pending') is not False):
        raise DatabaseProbeUnavailable('database_state_invalid')
    current = state['projection_seq'] == state['applied_seq'] and state['visibility_epoch'] is not None
    return {**{key:state[key] for key in (*keys,'applied_seq','projection_seq','visibility_epoch')},
        'text_search_ready':state['text_search_ready'] and current,
        'dense_search_ready':state['dense_search_ready'] and current,
        'archive_read_ready':None,'proof_ready':None}


@dataclass(frozen=True)
class NativeDatabaseProbe:
    """Observe readiness and installed versions, never fingerprint the catalog.

    A short read-only statement supplies one consistent frontier/ledger view.
    Schema migrations establish structure; readiness does not enumerate owners,
    collation, definitions or ACLs and is independent of PG patch releases.
    """
    timeout_seconds: float = 2.0

    def __post_init__(self):
        if (type(self.timeout_seconds) not in (int,float) or not math.isfinite(self.timeout_seconds)
                or not 0 < self.timeout_seconds <= 10):
            raise ValueError('bounded native probe deadline required')

    async def __call__(self,*,acquire):
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with acquire() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout='2000ms'")
                        row=await (await connection.execute("""SELECT jsonb_build_object(
                            'database_generation',s.database_generation,
                            'schema_version',(SELECT max(version) FROM rkb_v4.schema_migrations),
                            'server_version_num',current_setting('server_version_num')::integer,
                            'pgvector_version',(SELECT extversion FROM pg_extension WHERE extname='vector'),
                            'postgis_version',(SELECT extversion FROM pg_extension WHERE extname='postgis'),
                            'schema_revision',s.schema_revision,'function_revision',s.function_revision,
                            'executor_revision',s.executor_revision,
                            'applied_seq',s.applied_seq,'projection_seq',f.projection_seq,
                            'visibility_epoch',f.visibility_epoch,'text_search_ready',f.text_ready,
                            'dense_search_ready',f.dense_ready,'pending',f.pending_seq IS NOT NULL)::text
                            FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
                            WHERE s.singleton AND f.singleton LIMIT 2""")).fetchall()
                        if len(row)!=1 or not isinstance(row[0][0],str) or len(row[0][0].encode())>4096:
                            raise DatabaseProbeUnavailable('database_state_unavailable')
                        state=json.loads(row[0][0])
                        if (type(state.get('schema_version')) is not int or state['schema_version']<1
                                or type(state.get('server_version_num')) is not int
                                or state['server_version_num']//10000!=17
                                or state.get('pgvector_version')!='0.8.2'):
                            raise DatabaseProbeUnavailable('database_native_runtime_mismatch')
                        fields=_state(state)
                        fields['schema_version']=state['schema_version']
                        # Keep the former two-item adapter shape during the
                        # protocol transition; these bytes are not a catalog
                        # and must never be used as a readiness/hash pin.
                        return b'{}',fields
        except asyncio.CancelledError:
            raise
        except DatabaseProbeUnavailable:
            raise
        except (TimeoutError,QueryCanceled):
            raise DatabaseProbeUnavailable('database_probe_deadline') from None
        except Exception:
            raise DatabaseProbeUnavailable('database_probe_failed') from None


# Bounded health operation over the same existing native PG acquisition.
HEALTH_SQL = """SELECT
    CASE WHEN octet_length(s.database_generation)<=256 THEN s.database_generation END,
    CASE WHEN octet_length(s.schema_revision)<=256 THEN s.schema_revision END,
    s.applied_seq,f.projection_seq,f.pending_seq IS NOT NULL
    FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
    WHERE s.singleton AND f.singleton LIMIT 2"""


def _health_budget(seconds):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 5:
        raise ValueError('bounded health deadline required')


class _HealthProbeFailure(Exception):
    pass


async def probe_sql_health(*, acquire, database_generation, min_data_seq, lease_current, timeout_seconds=2):
    """One bounded native transaction over the existing injected PG acquisition.

    The native WSS handler must first validate its existing request identity.
    This fixed SELECT reads only metadata, never corpus rows or private catalog.
    """
    _health_budget(timeout_seconds)
    if (type(database_generation) is not str or not 1 <= len(database_generation.encode()) <= 256
            or type(min_data_seq) is not int or not 0 <= min_data_seq < 2**63
            or not callable(lease_current)):
        raise ValueError('validated native generation, sequence and lease callback required')
    started = time.monotonic()
    observed = dict(sql_response=False)
    reason = None

    def lease():
        if lease_current() is not True:
            raise _HealthProbeFailure('pg_binding_changed')

    try:
        async with asyncio.timeout(timeout_seconds):
            lease()
            async with acquire() as connection:
                lease()
                if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
                        or connection.row_factory is not tuple_row):
                    raise _HealthProbeFailure('pg_lease_invalid')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        await cursor.execute('SET LOCAL search_path=pg_catalog')
                        await cursor.execute("SELECT set_config('statement_timeout',%s,true),set_config('lock_timeout',%s,true)",
                                             (str(max(1, int(timeout_seconds * 1000))), '100'))
                        lease()
                        await cursor.execute(HEALTH_SQL)
                        rows = await cursor.fetchmany(2)
                        lease()
                        observed['sql_response'] = True
                        if len(rows) != 1 or len(rows[0]) != 5:
                            raise _HealthProbeFailure('pg_metadata_invalid')
                        generation, schema, applied, projection, pending = rows[0]
                        if (type(generation) is not str or not 1 <= len(generation.encode()) <= 256
                                or type(schema) is not str or not 1 <= len(schema.encode()) <= 256
                                or any(type(n) is not int or n < 0 for n in (applied, projection))
                                or type(pending) is not bool):
                            raise _HealthProbeFailure('pg_metadata_invalid')
                        if generation != database_generation:
                            raise _HealthProbeFailure('pg_generation_mismatch')
                        observed.update(schema_revision=schema, applied_seq=applied,
                                        projection_seq=projection, pending=pending)
                        if pending:
                            reason = 'pg_prior_write_pending'
                        elif projection != applied:
                            reason = 'pg_projection_lag'
                        elif applied < min_data_seq:
                            reason = 'pg_sequence_behind'
            lease()  # Pool release / transaction exit is also a cancellation boundary.
    except asyncio.CancelledError:
        raise
    except (TimeoutError, QueryCanceled):
        reason = 'pg_timeout'
    except _HealthProbeFailure as error:
        reason = str(error)
    except Exception:
        reason = 'pg_query_failed'
    return dict(**observed, reason=reason,
                          elapsed_ms=(time.monotonic() - started) * 1000)
