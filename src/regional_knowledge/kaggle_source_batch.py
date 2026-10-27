"""Packet-local preparation and BGE installation in the resident Kaggle worker."""
import json
import logging
import time

from .source_batch import validate_command
from .source_stage import canonical, digest, load_stage_batch
from .source_batch_context import load_current
from .source_activation import knowledge_bytes
from .bge_contract import validate_vector

LOG = logging.getLogger(__name__)


def inputs(knowledge):
    for p in knowledge['passages']:
        yield p['passage_id'], p['body']
    for s in knowledge['semantic']['stories']:
        v = s['default_variant']
        for f in v.get('fragments') or [dict(fragment_id=v['variant_id'],start=0,end=len(v['body']))]:
            yield f['fragment_id'], v['body'][f['start']:f['end']]


async def materialize_packet(worker, command, *, acquire, database):
    from .kaggle_source_materializer import NativeSourceCompiler, materializer_fingerprint, failure
    d = validate_command(command); source = command.ids['source_id']; idx = d['batch_index']
    part=d['component']
    worker._current(); started = time.monotonic()
    async with acquire() as connection:
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='5s'")
            if await (await connection.execute('SELECT 1 FROM rkb_v4.applied_ops WHERE operation_key=%s',
                    (command.operation_key,))).fetchone():
                return
            prepared = await (await connection.execute('''SELECT knowledge FROM rkb_v4.source_batch_prepared
                WHERE source_id=%s AND batch_index=%s AND component=%s AND command_sha256=%s''',
                (source,idx,part,digest(command.encode())))).fetchone()
            if prepared is None:
                raw = await load_stage_batch(connection,source_id=source,ingestion_id=d['ingestion_id'],batch_index=idx)
                if raw is None or digest(raw)!=d['last_batch_sha256']:
                    raise failure('native_source_accepted_batch_missing')
                try:
                    view=await load_current(connection,raw,source,text_only=part=='text')
                except (ValueError,TypeError,KeyError):
                    raise failure('native_source_packet_context_invalid') from None
    # An already applied historical command is an idempotent no-op. Do not
    # recompute it under the new compiler; only new/unapplied commands require
    # the exact current immutable materializer fingerprint.
    if d['materializer_sha256'] != materializer_fingerprint():
        raise PermissionError('accepted_native_materializer_unavailable')
    worker._current()
    if prepared is None:
        compiler = NativeSourceCompiler(count=worker.count,current=worker.current)
        knowledge, media = await compiler.compile_current(raw,view,source_id=source,
            person_id=command.person_id,text_only=part=='text')
        raw = knowledge_bytes(knowledge)
        try:
            async with acquire() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='5s'")
                    row = await (await connection.execute('''SELECT rkb_v4.prepare_source_batch_v1(
                        %s,%s,%s::jsonb,%s,%s)''',(command.encode(),raw,canonical(media).decode(),
                        database.database_generation,database.writer_generation))).fetchone()
                    if row != (digest(raw),): raise failure('native_source_packet_binding')
        except Exception as error:
            LOG.warning('native_source_packet state=validation_failed sequence=%s batch_index=%s component=%s error_type=%s sqlstate=%s',
                command.sequence,idx,part,type(error).__name__,getattr(error,'sqlstate',None))
            if getattr(error,'sqlstate',None) == '22023':
                raise failure('native_source_packet_requires_review') from None
            raise
    else:
        knowledge = prepared[0]
    encoded = 0; reused = 0
    for ident, text in inputs(knowledge):
        worker._current(); h = digest(text.encode())
        async with acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='5s'")
                row = await (await connection.execute('''SELECT embedding::real[] FROM rkb_v4.source_batch_vectors
                    WHERE source_id=%s AND input_id=%s AND input_sha256=%s ORDER BY batch_index DESC LIMIT 1''',
                    (source,ident,h))).fetchone()
        if row is None:
            vector = validate_vector(await worker.encode(text)); encoded += 1
        else:
            vector = validate_vector(row[0]); reused += 1
        worker._current()
        async with acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='5s'")
                row = await (await connection.execute('''SELECT rkb_v4.install_source_batch_vector_v1(
                    %s,%s,%s,%s,%s,%s::public.vector,%s,%s)''',
                    (source,idx,part,ident,h,json.dumps(vector,allow_nan=False),database.database_generation,
                        database.writer_generation))).fetchone()
                if row != (h,): raise failure('native_source_packet_vector_binding')
    LOG.info('native_source_packet state=prepared sequence=%s batch_index=%s component=%s encoded=%s reused=%s elapsed_seconds=%.3f',
        command.sequence,idx,part,encoded,reused,time.monotonic()-started)
