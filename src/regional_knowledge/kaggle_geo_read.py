"""Fixed indexed private identity/owner-reference reads, no geometry authority."""
from __future__ import annotations

import asyncio
import base64
import hashlib

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .geo_manifest_commands import GEO_REVISIONS
from .geo_migration import exact_uuid,normalized,canonical
from .kaggle_geo_contracts import (GeoReadRequest,GeoReadResult,GeoReadRecord,
    cursor_scope,decode_cursor,encode_cursor,decode_item)
from .kaggle_query import ReadUnavailable,_frontier,_identity

GEO_READER_FUNCTIONS=(
    'read_geo_identity_v1(uuid,uuid,text,text,text,integer,uuid,integer)',
    'read_geo_members_v1(uuid,uuid,text,text,integer)',
)


async def execute_geo_read(request:GeoReadRequest,*,acquire):
    if type(request) is not GeoReadRequest:raise TypeError('validated fixed GEO read required')
    deadline=asyncio.get_running_loop().time()+min(request.deadline_ms,2000)/1000
    observed={}
    def failed(reason):
        return GeoReadResult(**_identity(request),status='invalid_query' if reason=='invalid_input' else 'unavailable',
            observed_data_seq=observed.get('data'),observed_projection_seq=observed.get('projection'),
            observed_visibility_epoch=observed.get('visibility'),observed_database_generation=observed.get('database'),reason=reason)
    try:
        async with asyncio.timeout_at(deadline):
            exact_uuid(request.actor.person_id)
            async with acquire() as connection:
                if connection.closed or connection.info.transaction_status!=TransactionStatus.IDLE or connection.row_factory is not tuple_row:
                    raise ReadUnavailable('internal_error')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        async def execute(statement,params=()):
                            await cursor.execute("SELECT set_config('statement_timeout',%s,true)",
                                (str(max(1,int((deadline-asyncio.get_running_loop().time())*1000))),))
                            await cursor.execute(statement,params)
                        await _frontier(cursor,execute,request,observed,require_dense=False)
                        await execute('SELECT schema_revision,function_revision FROM rkb_v4.recovery_state WHERE singleton')
                        if await cursor.fetchone()!=(GEO_REVISIONS.schema,GEO_REVISIONS.function):raise ReadUnavailable('not_ready')
                        scope=cursor_scope(request,observed);after=None
                        if request.cursor:
                            try:after=decode_cursor(request.cursor,scope)
                            except ValueError:raise ReadUnavailable('invalid_input') from None
                        if request.action=='identity':
                            if after is not None:
                                try:exact_uuid(after)
                                except ValueError:raise ReadUnavailable('invalid_input') from None
                            name=normalized(request.name) if request.name is not None else None
                            await execute('SELECT * FROM rkb_v4.read_geo_identity_v1(%s::uuid,%s::uuid,%s,%s,%s,%s,%s::uuid,%s)',
                                (request.actor.person_id,request.entity_id,request.poi_ref,
                                 hashlib.sha256(name.encode()).hexdigest() if name is not None else None,
                                 name,request.year,after,request.limit+1))
                        else:
                            await execute('SELECT * FROM rkb_v4.read_geo_members_v1(%s::uuid,%s::uuid,%s,%s,%s)',
                                (request.actor.person_id,request.entity_id,request.category,after,request.limit+1))
                        rows=await cursor.fetchall()
                        if len(rows)>request.limit+1:raise ReadUnavailable('internal_error')
                        records=[];positions=[];record_bytes=0
                        for row in rows[:request.limit]:
                            if request.action=='identity':
                                entity,key,wire,matched,manifest,sequence,operation,command_hash=row
                                position=str(entity);exact_uuid(position)
                            else:
                                key,wire,manifest,sequence,operation,command_hash=row;matched=None;position=key
                            record=GeoReadRecord(item_key=key,wire_b64=base64.b64encode(bytes(wire)).decode(),
                                matched_wire_b64=base64.b64encode(bytes(matched)).decode() if matched is not None else None,
                                manifest_id=manifest,activation_sequence=sequence,activation_operation_key=operation,
                                activation_command_sha256=command_hash)
                            size=len(canonical(record.model_dump(mode='json')))
                            if record_bytes+size>240*1024:break
                            record_bytes+=size
                            item,_=decode_item(record.wire_b64)
                            if item.owner_person_id!=request.actor.person_id:raise ReadUnavailable('access_denied')
                            records.append(record);positions.append(position)
                        return GeoReadResult(**_identity(request),status='ok',records=tuple(records),
                            next_cursor=encode_cursor(scope,positions[-1]) if len(rows)>len(records) else None,
                            observed_data_seq=observed['data'],observed_projection_seq=observed['projection'],
                            observed_visibility_epoch=observed['visibility'],observed_database_generation=observed['database'])
    except ReadUnavailable as exc:return failed(exc.reason)
    except TimeoutError:return failed('deadline_exceeded')
    except asyncio.CancelledError:raise
    except psycopg.errors.QueryCanceled:return failed('deadline_exceeded' if asyncio.get_running_loop().time()>=deadline-.001 else 'internal_error')
    except (psycopg.Error,ValueError,TypeError,KeyError):return failed('internal_error')
