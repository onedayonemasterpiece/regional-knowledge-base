"""In-process warm read handler stage/drain/switch/rollback without PG/BGE effects."""
import asyncio
import hashlib
from pathlib import Path

import pytest

from regional_knowledge.kaggle_warm_read import (
    ReadHandlerCompatibility, ReadHandlerSlot, WarmHandlerRejected,
)


def context():
    return ReadHandlerCompatibility(database_generation="db-v1", schema_revision="sql1",
        function_revision="fn1", executor_revision="ex1", model_sha256="b"*64,
        dependency_sha256="c"*64)


def staged_module(tmp_path, *, marker="new"):
    root=tmp_path/"owned-handler-artifacts"
    root.mkdir(mode=0o700, exist_ok=True)
    root.chmod(0o700)
    source = (
        "class KaggleWorkerQuery:\n"
        "    def __init__(self, *, marker):\n"
        "        self.marker=marker\n"
        "    async def __call__(self, frame, *, encode):\n"
        "        return {'version':self.marker,'frame':frame}\n"
    ).encode()
    path=root/(marker+".py")
    path.write_bytes(source)
    path.chmod(0o600)
    return root,path,hashlib.sha256(source).hexdigest()


async def old(frame,*,encode):
    return dict(version="old",frame=frame)


async def good_probe(handler):
    assert (await handler({"probe":True},encode=None))["version"] in ("old","new")


async def good_stage(candidate):
    assert (await candidate({"stage":True},encode=None))["version"]=="new"


@pytest.mark.asyncio
async def test_hot_activation_and_rollback_use_verified_bytes_and_same_resources(tmp_path):
    root,path,digest=staged_module(tmp_path)
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    assert (await slot({"user":1},encode=None))["version"]=="old"
    stage=await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    assert stage["loaded_sha256"]=="a"*64 and stage["staged_sha256"]==digest
    assert stage["generation"]==0 and not stage["switching"]
    applied=await slot.activate(staged_sha256=digest,expected_loaded_sha256="a"*64,probe=good_probe)
    assert applied["loaded_sha256"]==digest and applied["generation"]==1
    assert not applied["switching"]
    assert (await slot({"user":2},encode=None))["version"]=="new"
    restored=await slot.rollback(expected_loaded_sha256=digest,probe=good_probe)
    assert restored["loaded_sha256"]=="a"*64 and restored["generation"]==2
    assert (await slot({"user":3},encode=None))["version"]=="old"
    with pytest.raises(WarmHandlerRejected,match="no_rollback"):
        await slot.rollback(expected_loaded_sha256="a"*64,probe=good_probe)


@pytest.mark.asyncio
async def test_bad_digest_does_not_execute_or_stage_untrusted_file(tmp_path):
    root,path,_=staged_module(tmp_path)
    source=path.read_text()+"\nopen(__file__ + '.executed','w').write('unsafe')\n"
    path.write_text(source)
    path.chmod(0o600)
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    with pytest.raises(WarmHandlerRejected,match="bytes_mismatch"):
        await slot.stage(path=path,trusted_root=root,expected_sha256="e"*64,
            compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    assert not Path(str(path)+".executed").exists()
    assert slot.receipt()["staged_sha256"] is None
    with pytest.raises(WarmHandlerRejected,match="incompatible"):
        await slot.stage(path=path,trusted_root=root,expected_sha256="e"*64,
            compatibility=ReadHandlerCompatibility(database_generation="DIFFERENT",
                schema_revision="sql1",function_revision="fn1",executor_revision="ex1",
                model_sha256="b"*64,dependency_sha256="c"*64),
            constructor={"marker":"new"},self_test=good_stage)
    root.chmod(0o777)
    with pytest.raises(WarmHandlerRejected,match="invalid_file"):
        await slot.stage(path=path,trusted_root=root,
            expected_sha256=hashlib.sha256(source.encode()).hexdigest(),
            compatibility=context(),constructor={"marker":"new"},self_test=good_stage)


@pytest.mark.asyncio
async def test_inflight_old_reader_drains_before_exact_generation_switch(tmp_path):
    gate=asyncio.Event()
    entered=asyncio.Event()
    async def slow(frame,*,encode):
        entered.set()
        await gate.wait()
        return {"version":"old"}
    root,path,digest=staged_module(tmp_path)
    slot=ReadHandlerSlot(slow,loaded_sha256="a"*64,compatibility=context())
    await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    first=asyncio.create_task(slot({},encode=None))
    await entered.wait()
    switching=asyncio.create_task(slot.activate(staged_sha256=digest,
        expected_loaded_sha256="a"*64,probe=good_probe))
    for _ in range(100):
        if slot.receipt()["switching"]:break
        await asyncio.sleep(.001)
    else:pytest.fail("switch gate never closed")
    next_read=asyncio.create_task(slot({"next":1},encode=None))
    await asyncio.sleep(.01)
    assert not switching.done() and not next_read.done()
    assert slot.receipt()["loaded_sha256"]=="a"*64
    gate.set()
    assert (await first)["version"]=="old"
    assert (await switching)["loaded_sha256"]==digest
    assert (await next_read)["version"]=="new"


@pytest.mark.asyncio
async def test_failed_first_probe_and_drain_timeout_preserve_old_handler(tmp_path):
    root,path,digest=staged_module(tmp_path)
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    async def failing_probe(_):
        raise AssertionError("synthetic candidate probe failed")
    with pytest.raises(AssertionError):
        await slot.activate(staged_sha256=digest,expected_loaded_sha256="a"*64,
            probe=failing_probe)
    assert slot.receipt()["loaded_sha256"]=="a"*64
    assert not slot.receipt()["switching"]
    assert (await slot({},encode=None))["version"]=="old"

    entered=asyncio.Event()
    gate=asyncio.Event()
    async def slow(frame,*,encode):
        entered.set()
        await gate.wait()
        return {"version":"old"}
    waiting=ReadHandlerSlot(slow,loaded_sha256="a"*64,compatibility=context())
    await waiting.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    running=asyncio.create_task(waiting({},encode=None))
    await entered.wait()
    with pytest.raises(TimeoutError):
        await waiting.activate(staged_sha256=digest,expected_loaded_sha256="a"*64,
            probe=good_probe,timeout_seconds=.03)
    assert waiting.receipt()["loaded_sha256"]=="a"*64
    assert not waiting.receipt()["switching"]
    gate.set()
    assert (await running)["version"]=="old"



@pytest.mark.asyncio
async def test_loader_executes_exact_checked_bytes_without_second_path_read(tmp_path,monkeypatch):
    root,path,digest=staged_module(tmp_path)
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    def forbid_second_read(self,*args,**kwargs):
        raise AssertionError("second read by path would permit TOCTOU")
    # The loader owns a no-follow FD and compiles exactly the SHA-checked bytes.
    monkeypatch.setattr(Path,"read_bytes",forbid_second_read)
    assert (await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
        )["staged_sha256"]==digest
    applied=await slot.activate(staged_sha256=digest,expected_loaded_sha256="a"*64,
        probe=good_probe)
    assert applied["loaded_kind"]=="module"
    assert applied["model_sha256"]=="b"*64 and applied["dependency_sha256"]=="c"*64
    assert (await slot.rollback(expected_loaded_sha256=digest,probe=good_probe)
        )["loaded_kind"]=="verified_bundle"


@pytest.mark.asyncio
async def test_untrusted_child_directory_or_symlink_is_not_executable(tmp_path):
    root,path,digest=staged_module(tmp_path)
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    indirect=root/"nested"
    indirect.mkdir(mode=0o700)
    (indirect/"handler.py").write_bytes(path.read_bytes())
    (indirect/"handler.py").chmod(0o600)
    with pytest.raises(WarmHandlerRejected,match="untrusted_path"):
        await slot.stage(path=indirect/"handler.py",trusted_root=root,expected_sha256=digest,
            compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    link=root/"linked.py"
    link.symlink_to(path)
    with pytest.raises(WarmHandlerRejected,match="untrusted_path|invalid_file"):
        await slot.stage(path=link,trusted_root=root,expected_sha256=digest,
            compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    assert slot.receipt()["staged_sha256"] is None


@pytest.mark.asyncio
async def test_relative_import_uses_verified_in_memory_module(tmp_path):
    root=tmp_path/"private"
    root.mkdir(mode=0o700)
    path=root/"v2.py"
    source=("from .bge_contract import SPACE\n"
            "class KaggleWorkerQuery:\n"
            "    def __init__(self, *, marker): self.marker=marker\n"
            "    async def __call__(self,frame,*,encode):\n"
            "        return {'version':self.marker,'space':SPACE}\n").encode()
    path.write_bytes(source);path.chmod(0o600)
    digest=hashlib.sha256(source).hexdigest()
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    async def staging(handler):
        assert (await handler({},encode=None))["version"]=="new"
    async def probe(handler):
        from regional_knowledge.bge_contract import SPACE
        assert (await handler({},encode=None))["space"]==SPACE
    await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=staging)
    assert (await slot.activate(staged_sha256=digest,expected_loaded_sha256="a"*64,
        probe=probe))["loaded_sha256"]==digest


@pytest.mark.asyncio
async def test_cannot_replace_owned_pool_tokenizer_telemetry_or_provenance(tmp_path):
    class Owned:
        def __init__(self,acquire,tokenizer,telemetry,source_provenance):
            self.acquire,self.tokenizer,self.telemetry,self.source_provenance=(
                acquire,tokenizer,telemetry,source_provenance)
        async def __call__(self,frame,*,encode):
            return {'version':'old'}
    pool,tokenizer,telemetry=object(),object(),object()
    current=Owned(pool,tokenizer,telemetry,True)
    root=tmp_path/'owned'
    root.mkdir(mode=0o700)
    path=root/'patched.py'
    source=(
        "class KaggleWorkerQuery:\n"
        "    def __init__(self,*,acquire,tokenizer,telemetry,source_provenance):\n"
        "        self.acquire=acquire;self.tokenizer=tokenizer\n"
        "        self.telemetry=telemetry;self.source_provenance=source_provenance\n"
        "    async def __call__(self,frame,*,encode):return {'version':'new'}\n"
    ).encode()
    path.write_bytes(source);path.chmod(0o600)
    digest=hashlib.sha256(source).hexdigest()
    slot=ReadHandlerSlot(current,loaded_sha256='a'*64,compatibility=context())
    async def accepted(handler):
        assert (await handler({},encode=None))['version']=='new'
    kwargs=dict(path=path,trusted_root=root,expected_sha256=digest,
                compatibility=context(),self_test=accepted)
    with pytest.raises(WarmHandlerRejected,match='context_changed'):
        await slot.stage(**kwargs,constructor=dict(acquire=pool,tokenizer=object(),
            telemetry=telemetry,source_provenance=True))
    with pytest.raises(WarmHandlerRejected,match='context_changed'):
        await slot.stage(**kwargs,constructor=dict(acquire=pool,tokenizer=tokenizer,
            telemetry=telemetry,source_provenance=False))
    assert slot.receipt()['staged_sha256'] is None
    await slot.stage(**kwargs,constructor=dict(acquire=pool,tokenizer=tokenizer,
        telemetry=telemetry,source_provenance=True))
    await slot.activate(staged_sha256=digest,expected_loaded_sha256='a'*64,
        probe=accepted)
    assert (await slot({},encode=None))['version']=='new'
    assert slot.receipt()['loaded_kind']=='module'


@pytest.mark.asyncio
async def test_cannot_activate_stale_or_unstaged_handler(tmp_path):
    root,path,digest=staged_module(tmp_path)
    slot=ReadHandlerSlot(old,loaded_sha256="a"*64,compatibility=context())
    with pytest.raises(WarmHandlerRejected,match="no_verified_stage"):
        await slot.activate(staged_sha256=digest,expected_loaded_sha256="a"*64,probe=good_probe)
    await slot.stage(path=path,trusted_root=root,expected_sha256=digest,
        compatibility=context(),constructor={"marker":"new"},self_test=good_stage)
    with pytest.raises(WarmHandlerRejected,match="stale_activation"):
        await slot.activate(staged_sha256=digest,expected_loaded_sha256="e"*64,probe=good_probe)
    assert slot.receipt()["loaded_sha256"]=="a"*64
    slot.close()
    with pytest.raises(WarmHandlerRejected,match="closed"):
        await slot({},encode=None)
