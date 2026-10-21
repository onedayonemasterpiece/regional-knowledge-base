"""Fixed private original acquisition and direct archive transfer beside PG.

No original bytes, arbitrary paths, credentials or model choices cross control.
PDF metadata is produced by the pinned isolated parser, never caller labels.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import importlib.metadata
import json
import logging
import os
from pathlib import Path
import secrets
import shutil
import sys
from uuid import UUID,uuid5

from .original_intake import (OriginalIntakeSpec,OriginalIntakeError,OriginalIntakeRejected,
    VerifiedOriginalReceipt,PHASES,canonical,digest,hash_value)
from .native_snapshot_transfer import NativeSnapshotTransfer,SnapshotTicket,_blocking

PRIVATE_ROOT=Path('/kaggle/temp')
_LOG=logging.getLogger(__name__)


@dataclass(frozen=True)
class OriginalIntakeConfig:
    allowed_origins:tuple[str,...]
    parser_packages:tuple[tuple[str,str],...]
    max_bytes:int=128*1024*1024
    seconds:int=120

    def __post_init__(self):
        from .native_snapshot_transfer import origin
        if (type(self.allowed_origins) is not tuple or not 1<=len(self.allowed_origins)<=4
                or any(origin(s)!=s for s in self.allowed_origins)
                or type(self.parser_packages) is not tuple or not self.parser_packages
                or not any(k.lower().replace('_','-')=='pymupdf' for k,_ in self.parser_packages)
                or len(dict(self.parser_packages))!=len(self.parser_packages)
                or any(type(k) is not str or type(v) is not str or not k or not v for k,v in self.parser_packages)
                or type(self.max_bytes) is not int or not 1<=self.max_bytes<=512*1024*1024
                or type(self.seconds) is not int or not 1<=self.seconds<=120):
            raise OriginalIntakeError('original_deployment_invalid')
    def wire(self):return dict(allowed_origins=list(self.allowed_origins),parser_packages=[list(p) for p in self.parser_packages],max_bytes=self.max_bytes,seconds=self.seconds)
    @classmethod
    def from_wire(cls,v):
        if type(v) is not dict or set(v)!={'allowed_origins','parser_packages','max_bytes','seconds'}:
            raise OriginalIntakeError('original_deployment_invalid')
        return cls(tuple(v['allowed_origins']),tuple(tuple(p) for p in v['parser_packages']),v['max_bytes'],v['seconds'])


class NativeOriginalIntake:
    def __init__(self,*,config,database,identity,workspace,current,downloader=None,parser=None,transfer=None,decoder_bundle=None):
        if type(config) is not OriginalIntakeConfig or not callable(current):raise TypeError('retained original owner required')
        if decoder_bundle is not None:
            from .kaggle_native_bundle import VerifiedNativeBundle
            if type(decoder_bundle) is not VerifiedNativeBundle or not decoder_bundle.current():
                raise OriginalIntakeError('original_djvu_verified_decoder_required')
        self.decoder_bundle=decoder_bundle
        root=Path(workspace)
        if not root.is_absolute() or not root.is_relative_to(PRIVATE_ROOT) or root.resolve(strict=True)!=root or root.stat().st_mode&0o077:
            raise OriginalIntakeError('original_private_workspace_required')
        actual={k:importlib.metadata.version(k) for k,_ in config.parser_packages}
        if actual!=dict(config.parser_packages):raise OriginalIntakeError('original_parser_dependency_mismatch')
        # Historical PDF receipts retain the exact old parser bytes and pin.
        # Never relabel accepted original/page receipts after installing DjVu.
        legacy_code=digest(Path(__file__).with_name('kaggle_original_parser_pdf_v1.py').read_bytes())
        self.legacy_parser_sha256=digest(canonical(dict(packages=actual,code=legacy_code)))
        self.parser_sha256=digest(canonical(dict(packages=actual,
            code=digest(Path(__file__).with_name('kaggle_original_parser.py').read_bytes()),
            djvu_inspector_sha256=digest(Path(__file__).with_name('kaggle_djvu_inspect.py').read_bytes()),
            legacy_pdf_parser_sha256=legacy_code)))
        self.config,self.database,self.identity,self.root,self.current=config,database,identity,root,current
        from .file_ingress import ChatFileDownloader
        self.downloader=downloader or ChatFileDownloader()
        self.parser=parser or self._parse
        self.transfer=transfer or NativeSnapshotTransfer(allowed_origins=frozenset(config.allowed_origins),max_bytes=max(config.max_bytes,256*1024),timeout_seconds=config.seconds)
        self.generation=None;self.closed=False;self._records={};self._requests={};self._results={};self._lock=asyncio.Lock()

    def attached(self,generation):self.generation=generation
    def detached(self,generation):
        if self.generation==generation:self.generation=None

    def _parser_script(self,pin):
        # Allowlist comes only from code bytes and pinned installed packages,
        # not from a received claim or a filename supplied by a client.
        if pin==self.parser_sha256:
            return Path(__file__).with_name('kaggle_original_parser.py')
        if pin==self.legacy_parser_sha256:
            return Path(__file__).with_name('kaggle_original_parser_pdf_v1.py')
        raise OriginalIntakeError('original_parser_pin_unavailable')

    async def _parse_selected(self,path,action,index=0,*,cursor=None,parser_sha256):
        self._parser_script(parser_sha256)
        # Injected test parser follows the same pin admission; the native
        # production child executes the immutable script matching the receipt.
        if (getattr(self.parser,'__self__',None) is self
                and getattr(self.parser,'__func__',None) is type(self)._parse):
            return await self._parse(path,action,index,cursor=cursor,parser_sha256=parser_sha256)
        # Inspect/first-page peers historically take positional args only;
        # preserve their exact call form when no continuation is requested.
        if cursor is None:return await self.parser(path,action,index)
        return await self.parser(path,action,index,cursor=cursor)

    async def _parse(self,path,action,index=0,*,cursor=None,parser_sha256=None):
        cursor=cursor or dict(block=0,offset=0,text=0)
        script=self._parser_script(self.parser_sha256 if parser_sha256 is None else parser_sha256)
        child=await asyncio.create_subprocess_exec(sys.executable,'-I',str(script),
            action,str(path),str(index),*(str(cursor[k]) for k in ('block','offset','text')),
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL,
            env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'})
        try:
            async with asyncio.timeout(60):
                chunks=[];used=0
                while chunk:=await child.stdout.read(65536):
                    used+=len(chunk)
                    if used>12*1024*1024:raise OriginalIntakeError('original_parser_output_bound')
                    chunks.append(chunk)
                status=await child.wait()
            value=json.loads(b''.join(chunks))
            if status!=0:
                from .original_intake import TERMINAL_REJECTIONS
                if value.get('terminal') is True and value.get('error') in TERMINAL_REJECTIONS:
                    raise OriginalIntakeRejected(value['error'])
                if value.get('error')=='original_djvu_decoder_required' and value.get('terminal') is False:
                    raise OriginalIntakeError('original_djvu_decoder_required')
                raise OriginalIntakeError('original_parser_unavailable')
            return value
        finally:
            if child.returncode is None:
                try:child.kill()
                except ProcessLookupError:pass
            waiting=asyncio.create_task(child.wait())
            while not waiting.done():
                try:await asyncio.shield(waiting)
                except asyncio.CancelledError:continue
            waiting.result()

    def _validate(self,frame):
        fields={'type','protocol_version','connection_generation','id','claim','kind','run_id','launch_nonce',
            'runtime_incarnation','database_generation','writer_generation','phase','operation_key','request_sha256',
            'arguments','compute_claim','dispatch_generation'}
        if (type(frame) is not dict or set(frame)-{'original_transfer'}!=fields or frame['type']!='DISPATCH'
                or frame['protocol_version']!=2 or frame['kind']!='recovery_phase' or frame['phase'] not in PHASES
                or tuple(frame[k] for k in ('run_id','launch_nonce','runtime_incarnation'))!=self.identity
                or (frame['database_generation'],frame['writer_generation'])!=(self.database.database_generation,self.database.writer_generation)
                or type(frame['connection_generation']) is not int or frame['connection_generation']<1
                or type(frame['dispatch_generation']) is not int or not 1<=frame['dispatch_generation']<=frame['connection_generation']
                or frame['connection_generation']!=self.generation or self.closed
                or not self.current((frame['run_id'],frame['runtime_incarnation'],frame['connection_generation']))
                or any(type(frame[k]) is not str or not 1<=len(frame[k])<=256 for k in ('id','claim','compute_claim','operation_key'))
                or len(canonical(frame))>128*1024):raise OriginalIntakeError('original_dispatch_binding')
        if frame['request_sha256']!=digest(canonical(dict(phase=frame['phase'],arguments=frame['arguments']))):
            raise OriginalIntakeError('original_request_hash')

    async def __call__(self,frame):
        self._validate(frame);key=frame['operation_key'];request=frame['request_sha256']
        async with self._lock:
            if key in self._requests:
                if self._requests[key]!=request:raise OriginalIntakeError('original_operation_changed')
                if key not in self._results:raise OriginalIntakeError('original_operation_unknown_reconcile')
                return self._results[key]
            if len(self._requests)>=4096:raise OriginalIntakeError('original_retention_bound')
            self._requests[key]=request
            _LOG.info('native_original_phase state=start run_id=%s phase=%s operation_sha256=%s',self.identity[0],frame['phase'],digest(key.encode()))
            try:
                async with asyncio.timeout(self.config.seconds):value=await self._execute(frame)
            except OriginalIntakeRejected as exc:value={'error':exc.code,'terminal':True}
            except OriginalIntakeError as exc:value={'error':str(exc)}
            except Exception:value={'error':'original_intake_unavailable'}
            result=dict(phase=frame['phase'],request_sha256=request,value=value)
            if len(canonical(result))>120*1024:raise OriginalIntakeError('original_result_bound')
            self._results[key]=result
            _LOG.info('native_original_phase state=%s run_id=%s phase=%s operation_sha256=%s',
                'unavailable' if 'error' in value else 'complete',self.identity[0],frame['phase'],digest(key.encode()))
            return result

    def _record(self,args):
        if type(args) is not dict or type(args.get('handle')) is not str:raise OriginalIntakeError('original_handle_required')
        record=self._records.get(args['handle'])
        if record is None or args.get('original_sha256')!=record['sha256']:raise OriginalIntakeError('original_retained_bytes_unavailable')
        return record

    async def _execute(self,frame):
        phase,args,private=frame['phase'],frame['arguments'],frame.get('original_transfer')
        if phase=='original_acquire':
            restore=type(args) is dict and set(args)=={'spec','restore_receipt','restore_nonce'}
            if (type(args) is not dict or set(args) not in ({'spec'},{'spec','restore_receipt','restore_nonce'})
                    or type(private) is not dict or set(private)!=({'ticket'} if restore else {'download_url'})):
                raise OriginalIntakeError('original_acquire_contract')
            spec=OriginalIntakeSpec.from_wire(args['spec'])
            if spec.max_bytes>self.config.max_bytes or len(self._records)>=32:raise OriginalIntakeError('original_capacity_bound')
            folder=self.root/secrets.token_hex(24);folder.mkdir(mode=0o700)
            acquired=False
            try:
                if shutil.disk_usage(folder).free<2*spec.max_bytes+32*256*1024+16*1024*1024:
                    raise OriginalIntakeError('original_private_disk_reservation_unavailable')
                prior=None
                if restore:
                    prior=VerifiedOriginalReceipt.from_wire(args['restore_receipt'])
                    ticket=SnapshotTicket.from_wire(private['ticket'])
                    if ((prior.source_id,prior.ingestion_id,prior.request_id,prior.revision)!=(
                            spec.source_id,spec.ingestion_id,spec.request_id,spec.revision)
                            or prior.parser_sha256 not in (self.parser_sha256,self.legacy_parser_sha256)
                            or (prior.parser_sha256==self.legacy_parser_sha256 and prior.format!='pdf')
                            or prior.size>spec.max_bytes
                            or type(args['restore_nonce']) is not str or len(args['restore_nonce'])!=48
                            or (ticket.key,ticket.size,ticket.sha256)!=(prior.key,prior.size,prior.sha256)
                            or {k.lower():v for k,v in ticket.headers}.get('if-match')!=prior.native_version):
                        raise OriginalIntakeError('original_restore_receipt_mismatch')
                    restored=await self.transfer.download(ticket,folder/'restored.source',operation_key=frame['operation_key'])
                    from .file_ingress import sha256_file,DownloadedFile
                    actual_hash,actual_size=await _blocking(sha256_file,restored)
                    if (actual_hash,actual_size)!=(prior.sha256,prior.size):raise OriginalIntakeError('original_restore_bytes_mismatch')
                    downloaded=DownloadedFile(restored,actual_hash,actual_size)
                else:downloaded=await self.downloader.download(private['download_url'],folder,max_bytes=spec.max_bytes)
                with downloaded.path.open('rb') as stream:header=stream.read(16)
                if header.startswith(b'%PDF-'):
                    detected_format='pdf'
                elif header.startswith(b'AT&TFORM') and header[12:16] in (b'DJVM',b'DJVU'):
                    detected_format='djvu'
                else:
                    raise OriginalIntakeRejected('original_format_unsupported')
                if ((spec.expected_sha256 is not None and spec.expected_sha256!=downloaded.sha256)
                        or (spec.expected_size is not None and spec.expected_size!=downloaded.size_bytes)):
                    raise OriginalIntakeRejected('original_expected_bytes_mismatch')
                selected_pin=prior.parser_sha256 if prior is not None else self.parser_sha256
                info=await self._parse_selected(downloaded.path,'inspect',parser_sha256=selected_pin)
                if (info.get('format')!=detected_format or type(info.get('page_count')) is not int
                        or not 1<=info['page_count']<=2500):
                    raise OriginalIntakeError('original_parser_invalid')
                if info['page_count']>spec.max_pages:raise OriginalIntakeRejected('original_page_count_limit')
                if prior is not None and (info['format'],info['page_count'])!=(prior.format,prior.page_count):
                    raise OriginalIntakeError('original_restore_parser_mismatch')
                handle=secrets.token_hex(24)
                value=dict(handle=handle,spec=spec.wire(),sha256=downloaded.sha256,size=downloaded.size_bytes,
                    format=detected_format,page_count=info['page_count'],parser_sha256=selected_pin)
                value['acquisition_sha256']=digest(canonical(value))
                if prior is not None:
                    value['acquisition_sha256']=prior.acquisition_sha256
                    value['restored_from_receipt_sha256']=prior.receipt_sha256
                    value['native_restore_sha256']=digest(canonical(dict(original=prior.receipt_sha256,
                        sha256=downloaded.sha256,size=downloaded.size_bytes,parser_sha256=selected_pin)))
                self._records[handle]=dict(value,path=downloaded.path,pages={},verified=prior.wire() if prior is not None else None)
                acquired=True
                return value
            finally:
                if not acquired:await _blocking(shutil.rmtree,folder)
        record=self._record(args)
        if phase in ('original_archive','original_verify'):
            if (set(args) not in ({'handle','original_sha256'},{'handle','original_sha256','page_index'})
                    or type(private) is not dict or set(private)!={'ticket'}):
                raise OriginalIntakeError('original_archive_contract')
            artifact=record
            if 'page_index' in args:
                if type(args['page_index']) is not int or args['page_index'] not in record['pages']:
                    raise OriginalIntakeError('original_render_not_retained')
                artifact=record['pages'][args['page_index']]
            ticket=SnapshotTicket.from_wire(private['ticket'])
            if (ticket.sha256,ticket.size)!=(artifact['sha256'],artifact['size']):raise OriginalIntakeError('original_archive_bytes_changed')
            if phase=='original_archive':
                version=await self.transfer.upload(ticket,artifact['path'],operation_key=frame['operation_key'])
                return dict(key=ticket.key,native_version=version,size=ticket.size,sha256=ticket.sha256)
            path=record['path'].parent/('verify-'+secrets.token_hex(12))
            try:await self.transfer.download(ticket,path,operation_key=frame['operation_key'])
            finally:path.unlink(missing_ok=True)
            value=dict(key=ticket.key,native_version={k.lower():v for k,v in ticket.headers}['if-match'],size=ticket.size,sha256=ticket.sha256,
                acquisition_sha256=record['acquisition_sha256'],parser_sha256=record['parser_sha256'])
            value['native_verification_sha256']=digest(canonical(value));artifact['verified']=value
            return value
        if phase=='original_pages':
            if (set(args) not in ({'handle','original_sha256','index'},{'handle','original_sha256','index','cursor'})
                    or type(args['index']) is not int or not 0<=args['index']<record['page_count']):
                raise OriginalIntakeError('original_page_contract')
            cursor=args.get('cursor') or dict(block=0,offset=0,text=0)
            if (type(cursor) is not dict or set(cursor)!={'block','offset','text'}
                    or any(type(v) is not int or not 0<=v<2**31 for v in cursor.values())):
                raise OriginalIntakeError('original_page_cursor_invalid')
            if record['verified'] is None:raise OriginalIntakeError('original_archive_verification_required')
            index=args['index']
            if record['format']=='djvu' and (
                    self.decoder_bundle is None or not self.decoder_bundle.current()):
                # Archive admission is independent of decoder installation;
                # a missing verified decoder may never fabricate ImageContent.
                raise OriginalIntakeError('original_djvu_decoder_required')
            if index not in record['pages']:
                if record['format']=='djvu':
                    from .kaggle_djvu_inspect import inspect_djvu
                    from .kaggle_djvu_pixels import render_verified_djvu_page,DjvuPixelsUnavailable
                    from .file_ingress import sha256_file
                    # A retained path is not proof that its bytes still match
                    # the verified original after archival; verify locally in
                    # Kaggle before native decode, never via Dev roundtrip.
                    actual=await _blocking(sha256_file,record['path'])
                    if actual!=(record['sha256'],record['size']):
                        raise OriginalIntakeError('original_retained_bytes_changed')
                    inventory=await _blocking(inspect_djvu,record['path'])
                    if (inventory.page_count!=record['page_count']
                            or inventory.byte_length!=record['size']):
                        raise OriginalIntakeError('original_djvu_inventory_changed')
                    try:
                        result=await render_verified_djvu_page(
                            bundle=self.decoder_bundle,original_path=record['path'],
                            original_bytes=record['size'],inventory=inventory,index=index)
                    except DjvuPixelsUnavailable:
                        raise OriginalIntakeError('original_djvu_decoder_required') from None
                    if (type(result) is not dict
                            or type(result.get('native_text_info')) is not dict
                            or type(result.get('preview_render')) is not dict):
                        raise OriginalIntakeError('original_djvu_render_provenance_invalid')
                    info=result['native_text_info'];preview=result['preview_render']
                    if (result.get('format')!='djvu'
                            or result.get('physical_page_index')!=index
                            or result.get('page_count')!=record['page_count']
                            or (result.get('width'),result.get('height'))!=(
                                inventory.pages[index].width,inventory.pages[index].height)
                            or result.get('native_cursor')!=dict(block=0,offset=0,text=0)
                            or result.get('next_native') is not None
                            or result.get('native_text')!=''
                            or result.get('native_blocks')!=[]
                            or info.get('complete') is not True
                            or info.get('native_text_sha256')!=digest(b'')
                            or info.get('native_blocks_sha256')!=digest(canonical([]))
                            or preview.get('policy')!='rkb.djvu-preview.v1'
                            or preview.get('preview_only') is not True):
                        # A decoder can yield pixels, but cannot reidentify
                        # the physical page or forge a textual transcription.
                        raise OriginalIntakeError('original_djvu_render_provenance_invalid')
                    # Keep the B2 structural parser SHA in all archived
                    # original/page receipts. Render software has a separate
                    # immutable page-level pin inside the existing
                    # preview_render field, preserved by NativePagesProof.
                    sources=('kaggle_djvu_pixels.py','kaggle_djvu_elf.py',
                        'kaggle_djvu_decoder_child.py','kaggle_djvu_inspect.py')
                    software=digest(canonical(dict(codec='rkb.djvu-render-software.v1',
                        files=[dict(name=name,sha256=digest(Path(__file__).with_name(name).read_bytes()))
                               for name in sources])))
                    rendered=dict(result['preview_render'],
                        decoder_manifest_sha256=self.decoder_bundle.manifest_sha256,
                        render_software_sha256=software)
                    result['preview_render']=rendered
                    result['native_text_info']['preview_render']=rendered
                else:
                    result=await self._parse_selected(record['path'],'page',index,
                        parser_sha256=record['parser_sha256'])
                image=base64.b64decode(result.pop('image_b64'),validate=True)
                if not 1<=len(image)<=256*1024 or digest(image)!=result['image_sha256']:
                    raise OriginalIntakeError('original_page_render_invalid')
                page_path=record['path'].parent/f'page-{index}.jpg'
                with page_path.open('xb') as stream:stream.write(image)
                result.update(page_id=str(uuid5(UUID(record['spec']['source_id']),f'page:1:{index}')),
                    original_sha256=record['sha256'],parser_sha256=record['parser_sha256'],image_size=len(image),
                    mime_type='image/jpeg')
                result['page_receipt_sha256']=digest(canonical(result))
                record['pages'][index]=dict(result=result,path=page_path,sha256=digest(image),size=len(image),verified=None,
                    chunks={canonical(dict(block=0,offset=0,text=0)).decode():result})
            artifact=record['pages'][index];key=canonical(cursor).decode()
            if key not in artifact['chunks']:
                result=await self._parse_selected(record['path'],'text',index,cursor=cursor,
                    parser_sha256=record['parser_sha256'])
                for name in ('page_id','original_sha256','parser_sha256','image_size','image_sha256','mime_type','preview_render'):
                    result[name]=artifact['result'][name]
                result['native_text_info']['preview_render']=artifact['result']['preview_render']
                result['page_receipt_sha256']=digest(canonical(result));artifact['chunks'][key]=result
            return artifact['chunks'][key]
        raise OriginalIntakeError('original_phase_unsupported')

    async def aclose(self):
        self.closed=True
        async with self._lock:await _blocking(shutil.rmtree,self.root)
