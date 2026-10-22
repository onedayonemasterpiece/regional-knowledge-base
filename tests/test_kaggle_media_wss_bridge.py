"""Real media WSS/read-admission class boundaries; only external PG/TG are fake.

No new pool/transport/IAM. Native reply comes through the SAME
SharedReadAdmission used by existing BGE reads, with a real KaggleWorkerQuery
and the actual ReadAuthority/SourceReadGate/KaggleSourceBackend/ASGI tool.
The fake remote worker pump is TEST ONLY and is not a second production WSS.
Other cases exercise actual native contracts, byte limits, lease/revoke and
fresh-process import without Dev controller/MCP modules.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import replace
import hashlib
import json
import os
import subprocess
import sys
from contextlib import suppress

import pytest

from regional_knowledge.kaggle_media_native_contracts import (
    NativeMediaSnapshot, canonical_media, native_binding_key)
from regional_knowledge.kaggle_media_wss_contracts import (
    IllustrationReadRequest, IllustrationReadResult, MEDIA_MAX_INLINE_BYTES)
from regional_knowledge.kaggle_protocol import decode_envelope,encode_envelope
from regional_knowledge.kaggle_public_illustration_native_read import READ_SQL_SHA256
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from test_kaggle_worker_query import pinned
from test_kaggle_public_illustration_gate import (
    snapshot, media_renderer, archive_validator, SOURCE, ILLUSTRATION,
)
from test_kaggle_public_illustration_native_read import _ExistingReaderPool, source_row
from test_telegram_illustrations_archive_read import (
    LeasedFakeTelethon, NativeTelegramIllustrations, png, message, PERSON)
from test_kaggle_public_media_wiring import call_http
from test_kaggle_service_startup import service
from test_kaggle_public_backend import target,release
from test_read_authority import auth
from test_participants import people,change
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.server import build_server


def request(*, action="snapshot", crop=None, **update):
    result=dict(
        request_id="media-request-1",
        connection_generation=1,turn_generation=0,
        action=action,source_id=SOURCE,illustration_id=ILLUSTRATION,
        expected_crop_sha256=crop,
        database_generation="db-source",binding_run_id="run",
        binding_runtime_incarnation="inc",
        provider_version="7",archive_scope="owner-owned-topic4",
        actor=dict(person_id=PERSON,audience="mcp:personal:fixture",
                   auth_epoch=1,visibility_epoch=0,
                   context_ref="resolver-test-context"),
        min_data_seq=0,deadline_ms=1850,
    )
    result.update(update)
    return IllustrationReadRequest(**result)


def native_fixture(*, generation="db-source", sequence=0):
    native=replace(snapshot(),database_generation=generation,
        observed_data_seq=sequence,observed_projection_seq=sequence)
    native.archive_state["photo"]["width"]=20
    native.archive_state["photo"]["height"]=20
    pool=_ExistingReaderPool(rows=[source_row(native)])
    client=LeasedFakeTelethon()
    client.message=message(width=20,height=20)
    lease=[True]
    archive=NativeTelegramIllustrations(client=client,chat_id=-100234,
        topic_id=4,current=lambda:lease[0],archive_scope="owner-owned-topic4")
    return pool,client,lease,archive,native


async def fake_encode(_):
    raise AssertionError("media cannot invoke encoder or model")


def test_fresh_native_import_without_dev_auth_controller_or_mcp():
    script=r"""
import builtins,sys
real=builtins.__import__
banned=("regional_knowledge.kaggle_public_illustration_gate",
"regional_knowledge.read_authority","regional_knowledge.source_read_gate",
"regional_knowledge.server","regional_knowledge.kaggle_public_backend",
"regional_knowledge.participants","mcp")
def strict(name,*args,**kwargs):
    if any(name==p or name.startswith(p+".") for p in banned):
        raise RuntimeError("FORBIDDEN_DEV_IMPORT:"+name)
    return real(name,*args,**kwargs)
builtins.__import__=strict
from regional_knowledge.kaggle_media_native_contracts import NativeMediaSnapshot
from regional_knowledge.kaggle_public_illustration_native_read import NativePublicIllustrationReader,READ_SQL_SHA256
from regional_knowledge.kaggle_media_wss_contracts import IllustrationReadRequest,IllustrationReadResult
from regional_knowledge.kaggle_media_native_execute import execute_illustration_read
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
assert NativeMediaSnapshot and NativePublicIllustrationReader and IllustrationReadRequest and KaggleWorkerQuery
assert not any(p in sys.modules for p in banned)
"""
    result=subprocess.run([sys.executable,"-c",script],text=True,
        capture_output=True,timeout=15,env={**os.environ,"PYTHONPATH":"src"})
    assert result.returncode==0, (result.stdout,result.stderr[-1800:])


def test_exact_media_wire_roundtrip_and_real_256k_base64_ceiling():
    base=request()
    assert type(decode_envelope(encode_envelope(base))) is IllustrationReadRequest
    native=snapshot()
    from dataclasses import asdict
    data=asdict(native)
    crop=hashlib.sha256(canonical_media(native.item["crop_binding"])).hexdigest()
    result=IllustrationReadResult(
        request_id=base.request_id,connection_generation=base.connection_generation,
        turn_generation=base.turn_generation,
        action="snapshot",source_id=SOURCE,illustration_id=ILLUSTRATION,
        status="ok",crop_sha256=crop,snapshot=data,
        observed_data_seq=0,observed_projection_seq=0,
        observed_visibility_epoch=0,observed_database_generation="db-source")
    assert type(decode_envelope(encode_envelope(result))) is IllustrationReadResult
    raw=bytes([123]) * MEDIA_MAX_INLINE_BYTES
    pixels=IllustrationReadResult(
        request_id=base.request_id,connection_generation=base.connection_generation,
        turn_generation=base.turn_generation,action="pixels",
        source_id=SOURCE,illustration_id=ILLUSTRATION,
        status="ok",crop_sha256=crop,
        pixels_b64=base64.b64encode(raw).decode(),mime_type="image/png",
        pixels_sha256=hashlib.sha256(raw).hexdigest(),
        observed_data_seq=0,observed_projection_seq=0,
        observed_visibility_epoch=0,observed_database_generation="db-source")
    encoded=encode_envelope(pixels)
    assert len(encoded)<256*1024
    assert type(decode_envelope(encoded)) is IllustrationReadResult
    with pytest.raises(ValueError):
        IllustrationReadResult(**{**pixels.model_dump(),"pixels_b64":
            base64.b64encode(raw+b"x").decode(),
            "pixels_sha256":hashlib.sha256(raw+b"x").hexdigest()})


@pytest.mark.asyncio
async def test_worker_actual_typed_snapshot_observe_and_real_telegram_download():
    pool,client,lease,archive,native=native_fixture()
    handler=KaggleWorkerQuery(
        acquire=pool.acquire,tokenizer=pinned(),
        media_archive=archive,media_binding_current=lambda _:True)
    src=request()
    result=await handler(src.model_copy(update={"compute_claim":"actual-claim"}).model_dump(mode="json"),
        encode=fake_encode,lease_current=lambda:lease[0])
    received=decode_envelope(json.dumps(result).encode())
    assert type(received) is IllustrationReadResult and received.status=="ok"
    assert received.snapshot["source_id"]==SOURCE
    digest=received.crop_sha256
    observe=request(action="observe",crop=digest)
    observed=await handler(observe.model_copy(update={"compute_claim":"actual-claim"}).model_dump(mode="json"),
        encode=fake_encode,lease_current=lambda:lease[0])
    assert observed["status"]=="ok"
    image=request(action="pixels",crop=digest)
    result=await handler(image.model_copy(update={"compute_claim":"actual-claim"}).model_dump(mode="json"),
        encode=fake_encode,lease_current=lambda:lease[0])
    assert result["status"]=="ok"
    assert base64.b64decode(result["pixels_b64"])==png()
    assert client.events.count("iter-download")==1
    assert pool.acquire_calls>=4 and pool.transactions>=4
    assert not archive._issued_proofs


@pytest.mark.asyncio
async def test_native_shared_media_lease_lost_before_worker_pixels_denies_without_token():
    pool,client,lease,archive,native=native_fixture()
    handler=KaggleWorkerQuery(
        acquire=pool.acquire,tokenizer=pinned(),
        media_archive=archive,media_binding_current=lambda _:True)
    sha=hashlib.sha256(canonical_media(native.item["crop_binding"])).hexdigest()
    lease[0]=False
    frame=request(action="pixels",crop=sha).model_copy(
        update={"compute_claim":"actual-claim"}).model_dump(mode="json")
    output=await handler(frame,encode=fake_encode,lease_current=lambda:lease[0])
    assert output["status"]=="unavailable"
    # The typed failure schema retains an explicit null slot; no pixel
    # bytes, SHA or private locator may be returned for an unavailable read.
    assert output["pixels_b64"] is None
    assert output["pixels_sha256"] is None
    assert pool.acquire_calls==0 and "iter-download" not in client.events


async def _worker_pump(service,handler,stop):
    """Test-only remote worker: actual shared admission and result type checking."""
    s=service.target
    queue=s.queue
    while not stop.is_set():
        work=await queue.next_channel_work(s.run,s.token,s.generation)
        if work is None:
            await asyncio.sleep(.002)
            continue
        try:
            if work.query is None:
                raise AssertionError("media test must not allocate other work")
            s.admission.mark_work_sent(work)
            result=await handler(work.query.model_dump(mode="json"),
                encode=fake_encode,lease_current=lambda:True)
            typed=decode_envelope(json.dumps(result,allow_nan=False).encode())
            await s.admission.finish_read(s.run,s.token,s.generation,typed)
            await release(s,work)
        except asyncio.CancelledError:
            raise
        except Exception:
            stop.set()
            raise


@pytest.mark.asyncio
async def test_real_source_backend_shared_admission_wss_media_to_mcp_image(
    service,monkeypatch):
    s=service
    s.adapter.available=True
    from regional_knowledge.kaggle_public_backend import current_binding
    from regional_knowledge.kaggle_public_illustration_native_read import READ_SQL_SHA256
    b=current_binding(s.target.queue)
    pool,client,lease,archive,native=native_fixture(
        generation=b.database_generation,sequence=b.min_data_seq)
    handler=KaggleWorkerQuery(acquire=pool.acquire,tokenizer=pinned(),
        media_archive=archive,media_binding_current=lambda _:True)
    gate=s.startup.backend.configure_native_media_wss(
        archive_scope="owner-owned-topic4",render_metadata=media_renderer,
        validate_archive_state=archive_validator,
        reader_sha256=READ_SQL_SHA256)
    monkeypatch.setattr("regional_knowledge.server._principal",
                        lambda:s.target.principal)
    server=build_server(target_startup=s.startup)
    stop=asyncio.Event()
    async with s.startup.lifespan(None):
        assert s.startup.reads_current()
        pump=asyncio.create_task(_worker_pump(service,handler,stop))
        try:
            result,raw=await asyncio.wait_for(call_http(
                type("Bound",(),{"server":server,"startup":s.startup})()),timeout=8)
            assert not result["result"]["isError"],result
            content=result["result"]["content"]
            assert len(content)==2
            assert content[1]["type"]=="image"
            assert base64.b64decode(content[1]["data"])==png()
            assert "crop_binding" not in raw.decode()
            assert "chat_id" not in raw.decode()
            assert "media_id" not in raw.decode()
            assert pool.transactions>=5
            assert client.events.count("iter-download")>=1
            assert not gate._tickets
        finally:
            stop.set()
            pump.cancel()
            with suppress(asyncio.CancelledError):
                await pump


@pytest.mark.asyncio
async def test_real_shared_media_result_wrong_crop_and_source_are_not_authority(
    service,monkeypatch):
    s=service;s.adapter.available=True
    from regional_knowledge.kaggle_public_backend import current_binding
    b=current_binding(s.target.queue)
    pool,client,lease,archive,native=native_fixture(
        generation=b.database_generation,sequence=b.min_data_seq)
    handler=KaggleWorkerQuery(acquire=pool.acquire,tokenizer=pinned(),
        media_archive=archive,media_binding_current=lambda _:True)
    gate=s.startup.backend.configure_native_media_wss(
        archive_scope="owner-owned-topic4",render_metadata=media_renderer,
        validate_archive_state=archive_validator,
        reader_sha256=READ_SQL_SHA256)
    monkeypatch.setattr("regional_knowledge.server._principal",
                        lambda:s.target.principal)
    server=build_server(target_startup=s.startup)
    stop=asyncio.Event()
    async with s.startup.lifespan(None):
        pump=asyncio.create_task(_worker_pump(service,handler,stop))
        try:
            # Uploaded bytes can change after native observation: fail entire
            # ImageContent and current gate does not send stale data.
            client.on_chunk=lambda:setattr(client.message.photo,"id",51)
            result,raw=await asyncio.wait_for(call_http(
                type("Bound",(),{"server":server,"startup":s.startup})()),timeout=8)
            assert result["result"]["isError"]
            assert b"iVBOR" not in raw and b'"type":"image"' not in raw
            assert not gate._tickets
        finally:
            stop.set()
            pump.cancel()
            with suppress(asyncio.CancelledError):
                await pump
