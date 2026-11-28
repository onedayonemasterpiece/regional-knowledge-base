"""One fixed live snapshot-capacity configuration update; no executor or handler activation.
Permanent values are also written to the existing host and next-worker configs.
"""
import asyncio,gc,json
from dataclasses import replace
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery as ExistingQuery
from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory
from regional_knowledge.kaggle_snapshot_io import SnapshotIo
_TASK=None

async def apply_capacity():
    report=dict(event='rkb_snapshot_capacity_applied_v304',success=False,provider_calls=0,worker_restarts=0)
    try:
        factories=[x for x in gc.get_objects() if type(x) is NativeSnapshotFactory and x.identity[0]=='0ff4ca18-eb68-4d07-b252-b9ca5faf0d9d']
        assert len(factories)==1,'Expected exactly the current owned snapshot factory'
        factory=factories[0];assert factory.identity[2]=='gpNDWPiCeG4q12xaZaJD2civVTMoi0pm' and factory.native_uid==65534
        owner=factory.current.__self__;factory._current((factory.identity[0],factory.identity[2],owner.generation))
        ios=[x for x in gc.get_objects() if type(x) is SnapshotIo and x.factory is factory]
        assert len(ios)==1
        assert all(op.task.done() for op in ios[0].operations.values() if op.phase!='original_pages'),'Snapshot operation still running; leave capacity unchanged'
        transfer=factory.dataset_transfer;assert transfer is ios[0].transfer
        assert (factory.limits.max_archive_bytes,factory.limits.max_unpacked_bytes,transfer.max_bytes)==(4294967296,4294967296,4294967296)
        # Existing supported budgets; no new limits, retries, queues or writer semantics.
        factory.limits=replace(factory.limits,max_archive_bytes=8589934592,max_unpacked_bytes=17179869184)
        transfer.max_bytes=8589934592
        report.update(success=True,archive_limit=factory.limits.max_archive_bytes,unpacked_limit=factory.limits.max_unpacked_bytes,transfer_limit=transfer.max_bytes,run_id=factory.identity[0],incarnation=factory.identity[2])
    except Exception as error:report.update(error_type=type(error).__name__,error=str(error)[:512])
    print('RKB_SNAPSHOT_CAPACITY_V304 '+json.dumps(report),flush=True)

def KaggleWorkerQuery(**resources):
    global _TASK
    candidate=ExistingQuery(**resources)
    if _TASK is None:_TASK=asyncio.create_task(apply_capacity(),name='rkb-fixed-snapshot-capacity-v304')
    return candidate
