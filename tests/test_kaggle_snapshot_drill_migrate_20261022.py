"""M2 first-book drill_migrate: immutable checkpoint / UNKNOWN / owned shutdown.

Synthetic native-process/transfer fakes in these unit cases do not establish
a real Dataset/Kaggle upgrade; the separate PG17 test exercises actual DDL.
"""
import base64
from pathlib import Path
from types import SimpleNamespace
from dataclasses import replace

import pytest

from regional_knowledge.kaggle_dataset_base import DatasetCheckpoint,DatasetFile
from regional_knowledge.kaggle_native_source_upgrade import NativeSourceUpgradePins
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError,encode,sha
from regional_knowledge.kaggle_snapshot_drill_migrate import NativeDrillMigrate


def fixture():
    blob=dict(key="pinned/dataset/archive.tar",size=100,sha256="a"*64,native_version="native")
    versions=dict(schema_revision="rkb-v4-sql001-011-v1",function_revision="original",
        executor_revision="original",catalog_sha256="b"*64)
    manifest=dict(codec="rkb.pg-physical-base.v1",blob=blob,versions=versions,
        database_generation="synthetic-generation",writer_generation="original-writer",
        applied_sequence=20,journal_head={"synthetic":"not accepted"},
        source_control={})
    control=dict(codec="rkb.source-base-control.v1",database_generation="synthetic-generation",
        sequence=20,journal_head=manifest["journal_head"])
    raw_control=encode(control)
    manifest["source_control"]={"sha256":sha(raw_control),"length":len(raw_control)}
    raw_manifest=encode(manifest)
    file=lambda name,raw: DatasetFile(name,len(raw),sha(raw))
    checkpoint=DatasetCheckpoint("owner","dataset",2,"synthetic-generation",20,
        file("manifest.json",raw_manifest),
        DatasetFile("archive.tar",100,"a"*64),
        file("control.json",raw_control),
        file("journal.json",b'{"codec":"rkb.dataset-journal.v1","objects":[]}'))
    database=SimpleNamespace(writer_generation="rkb-drill-writer",
        socket="/fixture/drill/socket",port=5432,wire=lambda:{"owned":"drill"})
    factory=SimpleNamespace(database=database,identity=("run","nonce","inc"),
        dataset_checkpoint=checkpoint,limits=SimpleNamespace(max_control_bytes=1<<20),
        _owned_candidates=set())
    current_calls=[]
    serving=SimpleNamespace(_current=lambda binding:current_calls.append(binding),
        database=SimpleNamespace(database_generation="synthetic-generation",socket="/fixture/serving/socket"),
        registry=None)
    counters=dict(download=0,prepared=0,closed=0,measured=0)
    class Candidate:
        def __init__(self):
            self.process=SimpleNamespace(returncode=None)
            self.database=database
        async def aclose(self):
            counters["closed"]+=1
            self.process.returncode=0
    candidate=Candidate()
    async def download(*args,**kwargs):
        counters["download"]+=1
        return Path("/fixture/pinned/archive.tar")
    async def prepare(binding,**kwargs):
        counters["prepared"]+=1
        factory._owned_candidates.add(candidate)
        return SimpleNamespace(candidate=candidate)
    factory.prepare=prepare
    owner=SimpleNamespace(serving=serving,factory=factory,download=download,
        workspace=lambda *_:Path("/fixture/drill"))
    pins=NativeSourceUpgradePins(*["c"*64]*4)
    migrate=NativeDrillMigrate(owner,sql_root=Path("/fixture/native/sql"),native_pins=pins)
    args=dict(manifest_b64=base64.b64encode(raw_manifest).decode(),
        manifest_ref={"sha256":sha(raw_manifest)},blob=blob,
        control_b64=base64.b64encode(raw_control).decode(),
        expected_versions=versions,database=database.wire(),
        identity=list(factory.identity),restore_operation="one-original-operation")
    return migrate,args,counters,candidate,current_calls


@pytest.mark.asyncio
async def test_checkpoint_pin_fails_before_download_or_native_candidate(monkeypatch):
    m,args,counters,candidate,calls=fixture()
    async def forbidden(*args,**kwargs):raise AssertionError("untrusted source effects")
    m.owner.download=forbidden
    m._artifacts=forbidden
    for override in (
        {"manifest_ref":{"sha256":"f"*64}},
        {"database":{"owned":"serving"}},
        {"identity":["other","nonce","inc"]},
        {"blob":{**args["blob"],"size":101}},
        {"control_b64":base64.b64encode(b'{"forged":"state"}').decode()},
    ):
        with pytest.raises(SnapshotError):
            await m({**args,**override},"key",("run","inc",1),
                {"download_ticket":{"exact":"synthetic"}})
    assert counters["download"]==counters["prepared"]==0
    assert not m.intents and candidate.process.returncode is None


@pytest.mark.asyncio
async def test_same_owned_drill_reconciles_exact_result_and_closed_process(monkeypatch):
    m,args,counters,candidate,calls=fixture()
    m._artifacts=lambda: ((),None,None)
    async def measured(*args,**kwargs):
        counters["measured"]+=1
        assert candidate in m.owner.factory._owned_candidates
        return dict(catalog_sha256="d"*64,acl_sha256="e"*64,
            original_applied_prefix_sha256="f"*64,base_sequence=20)
    m.measure_candidate=measured
    first=await m(args,"same-key",("run","inc",1),
        {"download_ticket":{"exact":"synthetic"}})
    assert first["closed"] is True and first["observation_only"] is True
    assert first["catalog_sha256"]=="d"*64 and first["acl_sha256"]=="e"*64
    assert candidate.process.returncode==0 and not m.candidates
    assert await m.verify_closed(first["reference"],("run","inc",2))==first
    assert await m(args,"same-key",("run","inc",3),
        {"download_ticket":{"exact":"synthetic"}})==first
    with pytest.raises(SnapshotError,match="operation_changed"):
        await m(args,"different-key",("run","inc",3),
            {"download_ticket":{"exact":"synthetic"}})
    assert counters["download"]==counters["prepared"]==counters["measured"]==counters["closed"]==1


@pytest.mark.asyncio
async def test_unknown_after_download_never_allocates_second_candidate(monkeypatch):
    m,args,counters,candidate,calls=fixture()
    m._artifacts=lambda: ((),None,None)
    async def failed_prepare(*args,**kwargs):
        counters["prepared"]+=1
        raise SnapshotError("synthetic_restore_failed")
    m.owner.factory.prepare=failed_prepare
    with pytest.raises(SnapshotError,match="synthetic_restore_failed"):
        await m(args,"stable-key",("run","inc",1),{"download_ticket":{"exact":"synthetic"}})
    with pytest.raises(SnapshotError,match="unknown_requires_owner_cleanup"):
        await m(args,"stable-key",("run","inc",1),{"download_ticket":{"exact":"synthetic"}})
    assert counters["download"]==1 and counters["prepared"]==1 and not m.proofs


@pytest.mark.asyncio
async def test_denied_artifact_pin_does_not_download(monkeypatch):
    m,args,counters,candidate,calls=fixture()
    with pytest.raises(SnapshotError,match="registry_unavailable"):
        await m(args,"stable-key",("run","inc",1),{"download_ticket":{"exact":"synthetic"}})
    assert counters["download"]==0 and not m.intents
