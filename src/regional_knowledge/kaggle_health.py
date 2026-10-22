"""Read-only health adapters for the existing controller and native PG channel.

The authenticated status route supplies the existing queue and bound native
probe. This module neither registers a route nor allocates a pool/startup.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import math
import time

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .kaggle_public_backend import TargetReadBinding, TargetUnavailable, current_binding

HEALTH_SQL = """SELECT
    CASE WHEN octet_length(s.database_generation)<=256 THEN s.database_generation END,
    CASE WHEN octet_length(s.schema_revision)<=256 THEN s.schema_revision END,
    s.applied_seq,f.projection_seq,f.pending_seq IS NOT NULL
    FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
    WHERE s.singleton AND f.singleton LIMIT 2"""


def _budget(seconds):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 5:
        raise ValueError('bounded health deadline required')


@dataclass(frozen=True)
class DatabaseHealth:
    binding: TargetReadBinding
    sql_response: bool = False
    schema_revision: str | None = None
    applied_seq: int | None = None
    projection_seq: int | None = None
    pending: bool | None = None
    reason: str | None = None
    elapsed_ms: float = 0

    @property
    def ready(self):
        return (self.sql_response is True and self.reason is None
                and type(self.schema_revision) is str
                and 1 <= len(self.schema_revision.encode()) <= 256
                and type(self.applied_seq) is int and type(self.projection_seq) is int
                and self.applied_seq >= self.binding.min_data_seq
                and self.projection_seq == self.applied_seq and self.pending is False)

    def wire(self):
        result = asdict(self)
        # The complete identity remains internal for exact attachment validation.
        result['binding'] = {key: result['binding'][key] for key in
                             ('connection_generation', 'database_generation',
                              'min_data_seq', 'provider_version')}
        result['ready'] = self.ready
        return result


class _ProbeFailure(Exception):
    pass


async def probe_database_health(*, acquire, binding, binding_current, timeout_seconds=2):
    """One bounded native transaction over the existing injected PG acquisition.

    The native WSS handler must first validate its existing request identity.
    This fixed SELECT reads only metadata, never corpus rows or private catalog.
    """
    _budget(timeout_seconds)
    if type(binding) is not TargetReadBinding:
        raise TypeError('exact existing target read binding required')
    started = time.monotonic()
    observed = dict(sql_response=False)
    reason = None

    def lease():
        if binding_current(binding) is not True:
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
                        if generation != binding.database_generation:
                            raise _ProbeFailure('pg_generation_mismatch')
                        observed.update(schema_revision=schema, applied_seq=applied,
                                        projection_seq=projection, pending=pending)
                        if pending:
                            reason = 'pg_prior_write_pending'
                        elif projection != applied:
                            reason = 'pg_projection_lag'
                        elif applied < binding.min_data_seq:
                            reason = 'pg_sequence_behind'
            lease()  # Pool release / transaction exit is also a cancellation boundary.
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        reason = 'pg_timeout'
    except _ProbeFailure as error:
        reason = str(error)
    except Exception:
        reason = 'pg_query_failed'
    return DatabaseHealth(binding, **observed, reason=reason,
                          elapsed_ms=(time.monotonic() - started) * 1000)


def _control_snapshot(queue):
    """Never queue.status(): that API can perform maintenance and allocate work."""
    with queue.connect(write=False) as db:
        control = db.execute('SELECT current_run,startup_state FROM control WHERE id=1').fetchone()
        run = db.execute('''SELECT id,status,launch_state,provider_ref,provider_version,
            runtime_incarnation,connection_generation,channel_active,attachment_conflict,heartbeat
            FROM runs WHERE id=?''', (control['current_run'],)).fetchone() if control else None
        startup_state = control['startup_state'] if control else None
        notebook = ({key: run[key] for key in
                     ('status', 'launch_state', 'provider_version', 'connection_generation')}
                    if run else dict(status='absent'))
        notebook['source'] = 'controller_queue'
        if run:
            age = queue.clock() - run['heartbeat']
            notebook['heartbeat_age_seconds'] = round(max(0, age), 3) if math.isfinite(age) and age >= -5 else None
    try:
        binding = current_binding(queue) if notebook.get('heartbeat_age_seconds') is not None else None
    except TargetUnavailable:
        binding = None
    return startup_state, notebook, binding


async def collect_health(*, queue, native_probe, controller_state='unknown', timeout_seconds=2.5):
    """Controller-side callable for an already authenticated existing status route.

    native_probe(binding) uses the existing bound WSS request/result path and
    returns DatabaseHealth. Controller process liveness is an explicit trusted
    input; a queue row must never be represented as a running controller.
    """
    _budget(timeout_seconds)
    if controller_state not in ('active', 'inactive', 'unknown'):
        raise ValueError('explicit controller process state required')
    started = time.monotonic()
    output = dict(codec='rkb.health.v1', controller=dict(state=controller_state),
                  notebook=dict(status='unknown'), postgres=None, read_ready=False)
    try:
        async with asyncio.timeout(timeout_seconds):
            startup, notebook, binding = await asyncio.to_thread(_control_snapshot, queue)
            output['controller']['startup_state'] = startup
            output['notebook'] = notebook
            if binding is None:
                output['reason'] = 'notebook_not_ready'
            else:
                observed = await native_probe(binding)
                if type(observed) is not DatabaseHealth or observed.binding != binding:
                    output['reason'] = 'pg_response_binding_mismatch'
                elif await asyncio.to_thread(current_binding, queue) != binding:
                    output['reason'] = 'attachment_changed'
                else:
                    output.update(postgres=observed.wire(), read_ready=observed.ready, reason=observed.reason)
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        output['reason'] = 'health_timeout'
    except TargetUnavailable:
        output['reason'] = 'attachment_changed'
    except Exception:
        output['reason'] = 'health_probe_unavailable'
    output['elapsed_ms'] = (time.monotonic() - started) * 1000
    return output
