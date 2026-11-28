"""Retry one proven failed pre-upload native read after the measured capacity fix.
No provider call here; the existing checkpoint owner retains publication and verification.
"""
import asyncio,gc,json
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery as ExistingQuery
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotError,encode,sha
from regional_knowledge.kaggle_snapshot_io import SnapshotIo
_TASK=None
CAPTURE_KEY='a66a3f3b223cd8b97449263f4239726e60061bc7b9b9f684681fc6f17466c68d'
HANDLE='1f43fa3dd3830d82f098d05c5e96e7fc757d6d5d137291c599102f7c8178d837'
ARGUMENTS={'expected_sequence': 26103, 'versions': {'model_sha256': '9717af2b74842d4fcd66b02af690632ec1fa623acb4b8a9deba5bc35bbe7e886', 'pgvector_version': '0.8.4', 'schema_version': 49, 'server_version_num': 170011, 'tokenizer_sha256': 'e8fd8f190649399a0d152eb333f3c10d92e35a930f7926db86c502e472e93590'}}
SEED_KEY='dataset-checkpoint:71dd909c1a019cc7c38e6a5a65a2c2ce0c986bd6e1de6ab40ea2d4827be829b6'
async def retry_known_failed_read():
    report=dict(event='rkb_snapshot_known_failed_read_resumed_v304',success=False,provider_calls=0,worker_restarts=0)
    try:
        factories=[x for x in gc.get_objects() if type(x) is NativeSnapshotFactory and x.identity[0]=='0ff4ca18-eb68-4d07-b252-b9ca5faf0d9d']
        assert len(factories)==1
        f=factories[0];assert f.identity[2]=='gpNDWPiCeG4q12xaZaJD2civVTMoi0pm'
        owner=f.current.__self__;binding=(f.identity[0],f.identity[2],owner.generation);f._current(binding)
        ios=[x for x in gc.get_objects() if type(x) is SnapshotIo and x.factory is f];assert len(ios)==1
        io=ios[0];old=io.operations[HANDLE]
        assert old.phase=='snapshot' and old.task.done() and old.upload is None and old.upload_task is None
        error=old.task.exception();assert type(error) is SnapshotError and error.args==('snapshot_byte_budget',),'Only the proven pre-upload byte-budget failure can resume'
        assert (f.limits.max_archive_bytes,f.limits.max_unpacked_bytes,io.transfer.max_bytes)==(8589934592,17179869184,8589934592)
        operation=sha(encode([SEED_KEY,list(f.identity)]))
        assert sha(encode(dict(operation=operation,phase='capture',arguments=ARGUMENTS)))==CAPTURE_KEY
        assert sha(CAPTURE_KEY.encode())==HANDLE
        # Keep every durable job, status receipt and publishing package unchanged.
        # Replace only the completed failed native READ task, which never requested upload.
        del io.operations[HANDLE]
        try:pending=io.capture(CAPTURE_KEY,binding,workspace=owner._workspace(CAPTURE_KEY+':measured-capacity-fix','capture'),**ARGUMENTS)
        except BaseException:io.operations[HANDLE]=old;raise
        assert pending['handle']==HANDLE
        report.update(success=True,handle=HANDLE,expected_sequence=ARGUMENTS['expected_sequence'],same_logical_capture=True)
    except Exception as error:report.update(error_type=type(error).__name__,error=str(error)[:512])
    print('RKB_SNAPSHOT_RESUMED_V304 '+json.dumps(report),flush=True)
def KaggleWorkerQuery(**resources):
    global _TASK
    candidate=ExistingQuery(**resources)
    if _TASK is None:_TASK=asyncio.create_task(retry_known_failed_read(),name='rkb-known-preupload-budget-failure-v304')
    return candidate
