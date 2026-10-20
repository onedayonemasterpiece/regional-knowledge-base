"""Full control/queue/native-file workflow with synthetic HTTP and parser peers."""
import asyncio
import io
from types import SimpleNamespace

import pytest

from regional_knowledge.contracts import ChatFile
from regional_knowledge.control_spool import ControlSpool
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.original_intake_controller import OriginalIntakeController
from regional_knowledge.original_intake import digest
from test_queue_writer import owner
from test_read_authority import auth
from test_participants import people
from test_control_spool import handler
from test_kaggle_original_intake import native,PDF,IMAGE,SOURCE,INGEST


@pytest.mark.asyncio
async def test_existing_queue_receipts_url_privacy_and_native_original_readback(owner,native):
    e,n=owner,native
    spool=ControlSpool(e.q,e.auth.p.registry,e.j,HandlerRegistry([handler()]))
    binding=current_binding(e.q)
    current=lambda b:b==binding and e.w.current(database_generation='synthetic-db',writer_generation='writer-1')
    with e.q.connect(write=False) as db:run=db.execute('SELECT * FROM runs WHERE id=?',(e.run,)).fetchone()
    bridge=RecoveryPhaseDispatch(e.q,database=SimpleNamespace(database_generation='synthetic-db',writer_generation='writer-1'),
        identity=(e.run,run['launch_nonce'],binding.runtime_incarnation),binding_current=current)
    n.handler.identity=bridge.identity;n.handler.database=bridge.database
    n.handler.current=lambda b:b==(binding.run_id,binding.runtime_incarnation,binding.connection_generation)
    n.handler.attached(e.generation)
    native_reads=[];uploads=[]
    async def upload(ticket,path,*,operation_key):
        uploads.append(ticket.key);e.c.objects[ticket.key]=(path.read_bytes(),'native-etag')
        if len(uploads)==1:raise TimeoutError('lost upload acknowledgment')
        return 'native-etag'
    async def download(ticket,path,*,operation_key):
        native_reads.append(ticket.key);data=e.c.objects[ticket.key][0]
        assert digest(data)==ticket.sha256;path.write_bytes(data);return path
    n.transfer.upload,n.transfer.download=upload,download
    def head(**kw):
        data,version=e.c.objects[kw['Key']]
        return dict(ContentLength=len(data),ETag=version)
    e.c.head_object=head
    e.c.generate_presigned_url=lambda *a,**k:'https://archive.invalid/signed-private-ticket'
    original_get=e.c.get_object
    dev_body_reads=[]
    def get(**kw):
        if kw['Key'].startswith('originals/'):
            dev_body_reads.append(kw['Key'])
            assert '/pages/' in kw['Key'],'Dev attempted original bulk download'
        return original_get(**kw)
    e.c.get_object=get
    controller=OriginalIntakeController(queue=e.q,spool=spool,participants=e.auth.p.registry,
        object_store=SimpleNamespace(client=e.c,bucket=e.s.bucket),binding_current=current,
        current_binding=lambda:current_binding(e.q),allowed_origins=frozenset({'https://archive.invalid'}))
    stop=asyncio.Event();phases=[]
    async def peer():
        while not stop.is_set():
            work=await e.a.next_channel_work(e.run,e.token,e.generation)
            if work is None:await asyncio.sleep(.001);continue
            e.a.mark_work_sent(work)
            f=dict(type='DISPATCH',protocol_version=2,connection_generation=e.generation,
                **dict(work.document_payload),compute_claim=work.compute_claim,dispatch_generation=work.dispatch_generation)
            phases.append(f['phase'])
            receipt=await n.handler(f)
            await bridge.receive(dict(type='RECOVERY_PHASE_RESULT',job_id=f['id'],claim=f['claim'],receipt=receipt,
                **{k:f[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')}),
                run_id=e.run,token=e.token,generation=e.generation)
            r=work.resource
            await e.a.resource_released(e.run,e.token,e.generation,runtime_incarnation=r.runtime_incarnation,
                resource_kind='document',resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
    task=asyncio.create_task(peer())
    try:
        file=ChatFile(download_url='https://attachment.invalid/private',file_id='file',mime_type='application/pdf')
        status=await controller.begin(e.auth.p.admin,operation_id='original-fixture',source_id=SOURCE,ingestion_id=INGEST,
            file=file,max_bytes=len(PDF))
        assert not status.verified and phases==['original_acquire','original_archive']
        status=await controller.status(e.auth.p.admin,INGEST)
        assert status.verified and status.receipt.original['sha256']==digest(PDF)
        assert phases==['original_acquire','original_archive','original_verify']
        assert len(uploads)==1  # Reconciliation does not PUT the original twice.
        assert len(native_reads)==1 and not dev_body_reads
        assert (await controller.begin(e.auth.p.admin,operation_id='original-fixture',source_id=SOURCE,ingestion_id=INGEST,
            file=file,max_bytes=len(PDF))).receipt==status.receipt
        assert n.download.calls==1
        batch=await controller.pages(e.auth.p.admin,INGEST)
        assert batch.pages[0].data==IMAGE and batch.pages[0].native_text=='actual fixture span'
        proof=await controller.native_pages(e.auth.p.admin,INGEST)
        assert proof.original_receipt_sha256==status.receipt.receipt_sha256
        assert proof.pages[0]['original_sha256']==digest(PDF)
        assert dev_body_reads and all('/pages/' in key for key in dev_body_reads)
        with e.q.connect(write=False) as db:
            rows=db.execute("SELECT identity,result FROM jobs WHERE kind='recovery_phase'").fetchall()
            assert 'attachment.invalid' not in str([tuple(row) for row in rows]) and 'signed-private-ticket' not in str([tuple(row) for row in rows])
        await e.w.retire('writer-1')
        with pytest.raises(Exception):await controller.verified(e.auth.p.admin,INGEST)
    finally:
        stop.set();await task;await bridge.aclose();spool.close();await n.handler.aclose()
