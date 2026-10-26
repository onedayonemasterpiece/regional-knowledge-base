"""Inert same-host PH receiver using existing verified participant links/read gates.

The dedicated credential scopes a technical caller. Only ReadAuthority's current
projects_hub link supplies human identity; neither a PH actor nor a service token
is a canonical RKB principal.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
from urllib.parse import quote
import stat
import time
from uuid import UUID

from mcp.server.mcpserver.exceptions import ToolError
from starlette.responses import JSONResponse

from .read_authority import TrustedReadDestination
from .read_issuance import capture_read_output, bind_json_output
from .kaggle_public_backend import parse_passage_fetch_id

CODEC = 'rkb.knowledge-search.v2'
PATH = '/internal/projects-hub/knowledge-search'
MAX_BYTES = 256 * 1024


def _canonical_uuid(value):
    return type(value) is str and str(UUID(value)) == value


class PrivateInput:
    def __init__(self, path, maximum):
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError('absolute private configuration required')
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 1 <= info.st_size <= maximum:
                raise ValueError('bounded private regular configuration required')
            self.identity = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            self.value = os.read(fd, maximum + 1)
            if len(self.value) != info.st_size:
                raise ValueError('configuration changed')
        finally:
            os.close(fd)

    def current(self):
        try:
            info = self.path.lstat()
            return (stat.S_ISREG(info.st_mode) and not info.st_mode & 0o077 and
                (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) == self.identity)
        except OSError:
            return False


@dataclass(frozen=True)
class ReadContext:
    request_id: str
    turn_generation: int
    min_data_seq: int
    deadline_monotonic: float


def failure(context, reason='context_gap', status='unavailable'):
    return dict(codec=CODEC, request_id=context.request_id, turn_generation=context.turn_generation,
        status=status, reason=reason, mode=None, evidence=[], min_data_seq=context.min_data_seq,
        observed_data_seq=None, observed_projection_seq=None, prior_write_pending=False,
        context_gap=True, continuation=None)


def render(value, context):
    value = value.model_dump(mode='json') if hasattr(value, 'model_dump') else value
    status = value.get('status')
    data, projection = value.get('observed_data_seq'), value.get('observed_projection_seq')
    pending = value.get('prior_write_pending', False)
    if (status not in ('ok', 'degraded') or value.get('mode') != 'hybrid' or pending
            or type(data) is not int or type(projection) is not int
            or min(data, projection) < context.min_data_seq):
        reason = value.get('reason') or ('prior_write_pending' if pending else 'context_gap')
        result = failure(context, reason, status if status in ('unavailable','cancelled','invalid_query','denied') else 'unavailable')
        result.update(observed_data_seq=data, observed_projection_seq=projection, prior_write_pending=bool(pending))
        return result
    result = failure(context)
    result.update(status=status, reason=value.get('reason'), mode='hybrid', context_gap=False,
        evidence=[{key: item.get(key) for key in ('id','title','text','url','metadata')}
                  for item in value['evidence']], observed_data_seq=data, observed_projection_seq=projection)
    if status == 'degraded' and not result['reason']:
        return failure(context)
    if len(json.dumps(result, ensure_ascii=False, allow_nan=False).encode()) > MAX_BYTES:
        return failure(context, 'response_bound')
    return result


def render_fetch(value, context):
    """Adapt existing exact/page fetch to the bounded v2 evidence envelope."""
    value = value.model_dump(mode='json') if hasattr(value, 'model_dump') else value
    if value.get('status') != 'ok':
        return failure(context, value.get('reason') or 'source_not_ready')
    evidence = []
    if 'chunks' in value:
        for raw in value['chunks']:
            item = raw.model_dump(mode='json') if hasattr(raw, 'model_dump') else raw
            pages = [page.model_dump(mode='json') if hasattr(page, 'model_dump') else page
                     for page in item['pages']]
            regions = [part.model_dump(mode='json') if hasattr(part, 'model_dump') else part
                       for part in item['regions']]
            evidence.append(dict(id=item['chunk_id'], title=f"Source {item['source_id']}, scans {value['page_start']}–{value['page_end']}",
                text=item['text'], url='urn:rkb:passage:'+quote(item['chunk_id'],safe=''),
                metadata=dict(source_id=item['source_id'],ordinal=item['ordinal'],pages=pages,
                    regions=regions,next_after_ordinal=value['next_after_ordinal'],has_more=value['has_more'])))
    else:
        evidence.append({key:value.get(key) for key in ('id','title','text','url','metadata')})
    return render(dict(status='ok',reason=None,mode='hybrid',evidence=evidence,
        observed_data_seq=value.get('observed_data_seq'),
        observed_projection_seq=value.get('observed_projection_seq')),
        context)


class _ServiceFence:
    def __init__(self, gate, current):
        self.gate, self.current = gate, current

    async def refresh(self, gated):
        if not self.current():
            raise PermissionError('service scope changed')
        return await self.gate.refresh(gated)

    def issuance_current(self, ticket):
        return self.current() and self.gate.issuance_current(ticket)

    def release(self, ticket):
        self.gate.release(ticket)


async def _connected_read(request, operation):
    async def disconnected():
        while True:
            if (await request.receive()).get('type') == 'http.disconnect':
                return
    read = asyncio.create_task(operation)
    connection = asyncio.create_task(disconnected())
    try:
        done, _ = await asyncio.wait((read, connection), return_when=asyncio.FIRST_COMPLETED)
        if connection in done:
            raise asyncio.CancelledError('consumer disconnected')
        return read.result()
    finally:
        for task in (read, connection):
            if not task.done():
                task.cancel()
        await asyncio.gather(read, connection, return_exceptions=True)


class ProjectsHubReader:
    def __init__(self, *, backend, read_authority, service_id, credential_id, credential_epoch, provider_tenant, bindings, current):
        if (backend.read_authority is not read_authority or not callable(getattr(backend,'search_adapter',None))
                or type(service_id) is not str or not re.fullmatch(r'[A-Za-z0-9._-]{1,80}',service_id)
                or not _canonical_uuid(credential_id) or type(credential_epoch) is not int or credential_epoch<1
                or type(provider_tenant) is not str or not re.fullmatch(r'[A-Za-z0-9._:/-]{1,120}',provider_tenant)
                or not callable(current)):
            raise ValueError('explicit target reader and technical scope required')
        parsed = {}
        for row in bindings:
            if (type(row) is not dict or set(row) != {'actor_sub','workspace_id','link_id','link_revision'}
                    or not re.fullmatch(r'usr_[A-Za-z0-9_-]{1,250}',row['actor_sub'])
                    or not re.fullmatch(r'ws_[A-Za-z0-9_-]{1,250}',row['workspace_id'])
                    or not _canonical_uuid(row['link_id']) or type(row['link_revision']) is not int or row['link_revision'] < 1):
                raise ValueError('exact verified-link descriptor required')
            key = row['actor_sub'], row['workspace_id']
            if key in parsed:
                raise ValueError('duplicate delegation scope')
            parsed[key] = (row['link_id'], row['link_revision'])
        if not 1 <= len(parsed) <= 32:
            raise ValueError('bounded nonempty delegation scope required')
        self.backend, self.authority = backend, read_authority
        self.service_id,self.credential_id,self.credential_epoch=service_id,credential_id,credential_epoch
        self.provider=read_authority.provider
        if not callable(getattr(self.provider,'verify_service_credential',None)):
            raise ValueError('existing provisioned service authority required')
        self.tenant, self.bindings, self.current = provider_tenant, parsed, current

    @classmethod
    def from_file(cls, path, *, backend):
        config = PrivateInput(path, 16384)
        value = json.loads(config.value)
        if type(value) is not dict or set(value) != {'codec','service_id','service_credential_id','service_credential_epoch','provider_tenant','bindings'} or value['codec'] != 'rkb.ph-reader.v2':
            raise ValueError('exact PH reader configuration required')
        return cls(backend=backend, read_authority=backend.read_authority, service_id=value['service_id'],
            credential_id=value['service_credential_id'],credential_epoch=value['service_credential_epoch'],
            provider_tenant=value['provider_tenant'], bindings=value['bindings'],current=config.current)

    def _query(self, value):
        common = {'codec','actor_sub','workspace_id','provider_tenant','link_id','link_revision',
            'request_id','turn_generation','min_data_seq','deadline_monotonic','max_evidence'}
        if type(value) is not dict:
            raise ValueError('invalid query')
        codec, operation = value.get('codec'), value.get('operation')
        if codec == 'rkb.knowledge-query.v2':
            if set(value) != common | {'query'}:
                raise ValueError('invalid query')
        elif codec == 'rkb.knowledge-query.v3':
            if operation == 'search':
                if set(value) not in (common | {'operation','query'},
                        common | {'operation','query','document_ids'}):
                    raise ValueError('invalid query')
                ids = value.get('document_ids')
                if ids is not None and (type(ids) is not list or not 1 <= len(ids) <= 5
                        or len(set(ids)) != len(ids) or not all(_canonical_uuid(v) for v in ids)):
                    raise ValueError('invalid source scope')
            elif operation == 'fetch':
                keys = common | {'operation'}
                by_id = keys | {'id'}
                by_pages = keys | {'source_id','page_start','page_end'}
                if set(value) not in (by_id, by_pages, by_pages | {'after_ordinal'}):
                    raise ValueError('invalid fetch selector')
                if 'id' in value:
                    parse_passage_fetch_id(value['id'])
                elif (not _canonical_uuid(value['source_id'])
                        or any(type(value[k]) is not int or not 0 <= value[k] <= 2499
                               for k in ('page_start','page_end'))
                        or not value['page_start'] <= value['page_end'] < value['page_start'] + 4
                        or ('after_ordinal' in value and (type(value['after_ordinal']) is not int
                            or not 0 <= value['after_ordinal'] < 2**63))):
                    raise ValueError('invalid page range')
            else:
                raise ValueError('invalid operation')
        else:
            raise ValueError('invalid codec')
        if (codec == 'rkb.knowledge-query.v2' or operation == 'search') and (
                type(value['query']) is not str or not value['query'].strip() or len(value['query']) > 1000
                or len(value['query'].encode()) > 4096):
            raise ValueError('invalid query')
        if (type(value['request_id']) is not str
                or not re.fullmatch(r'[A-Za-z0-9._:-]{1,256}',value['request_id'])
                or any(type(value[k]) is not int or not 0 <= value[k] < 2**63 for k in ('turn_generation','min_data_seq'))
                or type(value['max_evidence']) is not int or not 1 <= value['max_evidence'] <= 5
                or type(value['deadline_monotonic']) not in (int,float) or not math.isfinite(value['deadline_monotonic'])
                or value['deadline_monotonic'] > time.monotonic() + 2.05):
            raise ValueError('invalid query')
        expected = self.bindings.get((value['actor_sub'],value['workspace_id']))
        if (value['provider_tenant'] != self.tenant or expected is None
                or (value['link_id'],value['link_revision']) != expected or type(value['link_revision']) is not int):
            raise PermissionError('unverified delegation')
        return ReadContext(value['request_id'], value['turn_generation'], value['min_data_seq'], value['deadline_monotonic'])

    async def __call__(self, request):
        # No trusted forwarded headers: this route is a same-host technical API.
        client = request.scope.get('client')
        supplied = request.headers.get('authorization','')
        token = supplied[7:] if supplied.startswith('Bearer ') else ''
        if (not client or client[0] != '127.0.0.1' or not self.current()
                or request.headers.get('x-rkb-service') != self.service_id
                or request.headers.get('x-rkb-service-credential')!=self.credential_id
                or request.headers.get('x-rkb-service-epoch')!=str(self.credential_epoch) or not 32<=len(token)<=128):
            return JSONResponse({'error':'service_scope_required'},status_code=403)
        context = None
        authority = None
        calls = []
        try:
            raw = bytearray()
            async with asyncio.timeout(2):
                async for piece in request.stream():
                    raw.extend(piece)
                    if len(raw) > 16384:
                        raise ValueError('request bound')
            value = json.loads(raw)
            context = self._query(value)
            service=self.provider.verify_service_credential(token,service_id=self.service_id,
                credential_id=self.credential_id,epoch=self.credential_epoch,provider_tenant=self.tenant,workspace_id=value['workspace_id'])
            if service is None:
                raise PermissionError('provisioned service scope unavailable')
            if context.deadline_monotonic <= time.monotonic():
                return JSONResponse(failure(context,'deadline_exceeded'))
            destination_ref = hashlib.sha256(json.dumps([self.tenant,value['workspace_id'],value['actor_sub']],separators=(',',':')).encode()).hexdigest()
            destination = TrustedReadDestination('projects_hub','personal',destination_ref)
            async with asyncio.timeout_at(context.deadline_monotonic):
                authority = await self.authority.resolve_adapter(provider='projects_hub', tenant=self.tenant,
                    provider_subject=value['actor_sub'], destination=destination)
                if (authority.link_id,authority.link_revision) != (value['link_id'],value['link_revision']):
                    raise PermissionError('verified link changed')
                with capture_read_output() as calls:
                    # Authority ownership transfers to this exact backend attempt.
                    transferred, authority = authority, None
                    trusted = dict(authority=transferred,request_id=context.request_id,
                        turn_generation=context.turn_generation,min_data_seq=context.min_data_seq,
                        deadline=context.deadline_monotonic,destination=destination)
                    fetching = value.get('codec') == 'rkb.knowledge-query.v3' and value.get('operation') == 'fetch'
                    if fetching:
                        # Exactly the same Kaggle _search admission, read gate and issuance as MCP fetch.
                        selector = (dict(exact_locator=parse_passage_fetch_id(value['id']))
                            if 'id' in value else dict(catalog_request=dict(operation='accepted_chunks',
                                source_id=value['source_id'],page_start=value['page_start'],
                                page_end=value['page_end'],after_ordinal=value.get('after_ordinal'),
                                limit=value['max_evidence'])))
                        operation = self.backend._search(None,None,evidence=True,trusted=trusted,**selector)
                    else:
                        operation = self.backend.search_adapter(value['query'],authority=transferred,
                            request_id=context.request_id,turn_generation=context.turn_generation,
                            min_data_seq=context.min_data_seq,deadline_monotonic=context.deadline_monotonic,
                            destination=destination,source_ids=value.get('document_ids'),
                            limit=value['max_evidence'])
                    result = await _connected_read(request,operation)
                    if len(calls) != 1:
                        raise RuntimeError('current output provenance required')
                    calls[0].gate = _ServiceFence(calls[0].gate,lambda: self.current() and self.provider.service_credential_current(service) and time.monotonic() < context.deadline_monotonic)
                    projector = render_fetch if fetching else render
                    bind_json_output(request,calls,context.request_id,lambda output:projector(output,context),lambda:failure(context))
                    return JSONResponse(projector(result,context),headers={'Cache-Control':'no-store'})
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return JSONResponse(failure(context,'deadline_exceeded') if context else {'error':'request_timeout'},status_code=200 if context else 408)
        except PermissionError:
            return JSONResponse(failure(context,'access_denied','denied') if context else {'error':'delegation_required'},status_code=200 if context else 403)
        except ToolError as exc:
            reason, status = 'not_ready', 'unavailable'
            try:
                safe=json.loads(str(exc))
                if safe.get('status') in ('unavailable','cancelled','invalid_query','denied') and re.fullmatch(r'[a-z_]{1,80}',safe.get('reason','')):
                    reason,status=safe['reason'],safe['status']
            except (ValueError,TypeError):
                pass
            return JSONResponse(failure(context,reason,status))
        except Exception:
            return JSONResponse(failure(context) if context else {'error':'invalid_query'},status_code=200 if context else 422)
        finally:
            if authority is not None:
                self.authority.release(authority)
