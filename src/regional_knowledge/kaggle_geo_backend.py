"""Existing public GEO tools over the single native read/admission boundary."""
from __future__ import annotations

import asyncio
import json
import time

from mcp.server.mcpserver.exceptions import ToolError

from .geo_read_gate import GeoReadGate
from .kaggle_geo_contracts import GeoReadRequest,decode_item
from .kaggle_public_backend import current_binding
from .kaggle_protocol import TrustedActorContext
from .read_authority import TrustedReadDestination
from .read_issuance import retain_read_output


class KaggleGeoBackend:
    current_read_output_required=True

    def __init__(self,*,worker_queue,read_authority,geo_gate,destination_ref,current):
        if (type(geo_gate) is not GeoReadGate or geo_gate.authority is not read_authority
                or geo_gate.spool.queue is not worker_queue or worker_queue.shared_reads.authority is not read_authority
                or not callable(current)):
            raise ValueError('one canonical GEO authority/spool/queue/admission required')
        self.worker_queue,self.read_authority,self.geo_gate=worker_queue,read_authority,geo_gate
        self.destination=TrustedReadDestination('mcp','personal',destination_ref)
        self.current=current

    @staticmethod
    def _render(result):
        items=[]
        for record in result.records:
            item,digest=decode_item(record.wire_b64)
            value=item.wire()
            # These are exact minimal GEO provenance records, never old source
            # text, new source acceptance, canonical geometry or Story content.
            value.update(manifest_id=record.manifest_id,item_sha256=digest,
                activation_sequence=record.activation_sequence)
            items.append(value)
        return dict(status=result.status,reason=result.reason,items=items,next_cursor=result.next_cursor,
            coverage='private_geo_identity_and_owner_references',historical_geometry='unresolved_with_owner',
            accepted_target_source_evidence=False,observed_data_seq=result.observed_data_seq,
            observed_projection_seq=result.observed_projection_seq)

    async def _read(self,principal,**selector):
        deadline=time.monotonic()+2
        authority=permit=ticket=gated=None;retained=False
        try:
            async with asyncio.timeout_at(deadline):
                if not self.current():raise PermissionError('native history unavailable')
                authority=await self.read_authority.resolve_oauth(principal.access_token,destination=self.destination)
                binding=await asyncio.to_thread(current_binding,self.worker_queue)
                import secrets
                query=GeoReadRequest(request_id='geo-'+secrets.token_hex(16),connection_generation=binding.connection_generation,
                    turn_generation=0,min_data_seq=binding.min_data_seq,deadline_ms=max(1,int((deadline-time.monotonic())*1000)),
                    actor=TrustedActorContext(person_id=authority.person_id,audience=self.destination.key,
                        auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref),**selector)
                permit=await self.geo_gate.dispatch(authority,query,binding)
                ticket=await self.worker_queue.shared_reads.begin_read(authority,permit.query,
                    dialog_ref=self.destination.key,deadline=deadline)
                result=await asyncio.shield(ticket.result)
                gated=await self.geo_gate.finish(permit,result);permit=None
                if gated.result.status!='ok':raise ToolError(json.dumps(dict(status=gated.result.status,reason=gated.result.reason)))
                output=self._render(gated.result)
                if not retain_read_output(self.geo_gate,gated,self._render,
                        on_release=lambda:self.read_authority.release(authority)):
                    raise RuntimeError('public current-output boundary required')
                retained=True;return output
        except asyncio.CancelledError:raise
        except ToolError:raise
        except TimeoutError:raise ToolError('{"status":"unavailable","reason":"deadline_exceeded"}') from None
        except PermissionError:raise ToolError('{"status":"denied","reason":"access_denied"}') from None
        except (ValueError,TypeError):raise ToolError('{"status":"invalid_query","reason":"invalid_input"}') from None
        except Exception:raise ToolError('{"status":"unavailable","reason":"not_ready"}') from None
        finally:
            if ticket is not None:self.worker_queue.shared_reads.cancel_read(ticket)
            if permit is not None:self.geo_gate.abandon(permit)
            if not retained:
                if gated is not None:self.geo_gate.release(gated.ticket)
                if authority is not None:self.read_authority.release(authority)

    async def geo_lookup(self,name,principal,*,year=None,limit=3,cursor=None):
        return await self._read(principal,name=name,year=year,limit=limit,cursor=cursor)

    async def graph_fetch(self,entity_id,principal,*,limit=3,cursor=None,category=None,discovery_job_id=None):
        if discovery_job_id is not None:raise ToolError('{"status":"unavailable","reason":"target_graph_discovery_unavailable"}')
        return await self._read(principal,entity_id=entity_id,action='members' if category is not None else 'identity',
            category=category,limit=limit,cursor=cursor)

    async def poi_context(self,poi_ref,principal,*,limit=3,cursor=None):
        return await self._read(principal,poi_ref=poi_ref,limit=limit,cursor=cursor)
