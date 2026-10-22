"""Read-only health adapters for the existing controller and native PG channel.

The authenticated status route supplies the existing queue and bound native
probe. This module neither registers a route nor allocates a pool/startup.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
import math
import time

from .kaggle_health_native import HEALTH_SQL, _budget, probe_sql_health

from .kaggle_public_backend import TargetReadBinding, TargetUnavailable, current_binding

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


async def probe_database_health(*, acquire, binding, binding_current, timeout_seconds=2):
    """Keep the controller's exact binding/receipt over the native-only probe."""
    if type(binding) is not TargetReadBinding:
        raise TypeError('exact existing target read binding required')
    observed = await probe_sql_health(acquire=acquire,
        database_generation=binding.database_generation, min_data_seq=binding.min_data_seq,
        lease_current=lambda: binding_current(binding), timeout_seconds=timeout_seconds)
    return DatabaseHealth(binding, **observed)


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


def heartbeat_health(queue):
    """Sanitized Dev-side health for a public status route (no native call).

    Reports the runtime observation the current run last delivered on its
    authenticated heartbeat, labelled as such; a native SQL probe needs a
    worker request kind and is not claimed here.
    """
    _, notebook, binding = _control_snapshot(queue)
    health = dict(codec='rkb.health.v1', source='heartbeat_observation',
                  notebook={key: notebook.get(key) for key in
                            ('status', 'launch_state', 'provider_version', 'connection_generation', 'heartbeat_age_seconds')},
                  read_ready=False)
    if binding is None:
        health['reason'] = 'notebook_not_ready'
        return health
    with queue.connect(write=False) as db:
        row = queue._runtime_row(db, binding.run_id)
    observed = json.loads(row['observed_json']) if row is not None and row['observed_json'] else {}
    try:
        current = current_binding(queue)
    except TargetUnavailable:
        current = None
    if (current != binding or row is None
            or row['accepted_generation'] != binding.connection_generation
            or observed.get('database_generation') != binding.database_generation):
        health['reason'] = 'attachment_changed'
        return health
    postgres = {key: observed.get(key) for key in ('schema_revision', 'applied_seq', 'projection_seq')}
    health['postgres'] = postgres
    applied, projection = postgres['applied_seq'], postgres['projection_seq']
    if type(applied) is not int or type(projection) is not int:
        health['reason'] = 'runtime_observation_missing'
    elif projection != applied:
        health['reason'] = 'pg_projection_lag'
    elif applied < binding.min_data_seq:
        health['reason'] = 'pg_sequence_behind'
    else:
        health['read_ready'] = True
    return health
