"""Fixed read-only SQL health operation for the existing native PG owner.

No controller, MCP, auth, pool creation or WSS registration imports. The existing
handler authenticates its request and supplies a current-lease callback; this
helper does not grant read authority or define a replacement binding identity.
"""
from __future__ import annotations

import asyncio
import math
import time

from psycopg.errors import QueryCanceled
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

HEALTH_SQL = """SELECT
    CASE WHEN octet_length(s.database_generation)<=256 THEN s.database_generation END,
    CASE WHEN octet_length(s.schema_revision)<=256 THEN s.schema_revision END,
    s.applied_seq,f.projection_seq,f.pending_seq IS NOT NULL
    FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
    WHERE s.singleton AND f.singleton LIMIT 2"""


def _budget(seconds):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 5:
        raise ValueError('bounded health deadline required')


class _ProbeFailure(Exception):
    pass


async def probe_sql_health(*, acquire, database_generation, min_data_seq, lease_current, timeout_seconds=2):
    """One bounded native transaction over the existing injected PG acquisition.

    The native WSS handler must first validate its existing request identity.
    This fixed SELECT reads only metadata, never corpus rows or private catalog.
    """
    _budget(timeout_seconds)
    if (type(database_generation) is not str or not 1 <= len(database_generation.encode()) <= 256
            or type(min_data_seq) is not int or not 0 <= min_data_seq < 2**63
            or not callable(lease_current)):
        raise ValueError('validated native generation, sequence and lease callback required')
    started = time.monotonic()
    observed = dict(sql_response=False)
    reason = None

    def lease():
        if lease_current() is not True:
            raise _ProbeFailure('pg_binding_changed')

    try:
        async with asyncio.timeout(timeout_seconds):
            lease()
            async with acquire() as connection:
                lease()
                if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
                        or connection.row_factory is not tuple_row):
                    raise _ProbeFailure('pg_lease_invalid')
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
                            raise _ProbeFailure('pg_metadata_invalid')
                        generation, schema, applied, projection, pending = rows[0]
                        if (type(generation) is not str or not 1 <= len(generation.encode()) <= 256
                                or type(schema) is not str or not 1 <= len(schema.encode()) <= 256
                                or any(type(n) is not int or n < 0 for n in (applied, projection))
                                or type(pending) is not bool):
                            raise _ProbeFailure('pg_metadata_invalid')
                        if generation != database_generation:
                            raise _ProbeFailure('pg_generation_mismatch')
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
    except _ProbeFailure as error:
        reason = str(error)
    except Exception:
        reason = 'pg_query_failed'
    return dict(**observed, reason=reason,
                          elapsed_ms=(time.monotonic() - started) * 1000)


