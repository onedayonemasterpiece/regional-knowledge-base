"""Bounded native issued illustration bytes over the ORIGINAL leased Telethon owner.

Real NativeTelegramIllustrations class, real encoded Pillow PNG, and EXACT
NativeMediaSnapshot from PR241. External Telethon I/O is fake; no provider call,
new auth client, WSS protocol, PG, credential or deployment. Send lifecycle
regressions remain in test_telegram_illustrations.py from exact media delta.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
from io import BytesIO
from types import SimpleNamespace as N

from PIL import Image
import pytest

from regional_knowledge.telegram_illustrations import (
    NativeTelegramIllustrations,
    NativeIllustrationArchiveProof, NativeIllustrationIssuedBytes,
    IllustrationArchiveReadUnavailable,
)
from test_kaggle_public_illustration_gate import snapshot, ILLUSTRATION, SOURCE


CHAT=-100234
TOPIC=4
PERSON="60000000-0000-0000-0000-000000000006"


def png(*, width=20, height=20):
    out=BytesIO()
    Image.new("RGB",(width,height),(21,42,63)).save(out,"PNG")
    return out.getvalue()


def message(*, message_id=16, media_id=50,kind="photo",width=20,height=20):
    media=N(id=media_id,sizes=[N(w=width,h=height)],
            attributes=[N(w=width,h=height)], size=len(png(width=width,height=height)))
    return N(id=message_id,chat_id=CHAT,reply_to=N(reply_to_top_id=TOPIC),
             photo=media if kind=="photo" else None,
             document=media if kind=="document" else None)


class NativeFakeStream:
    """Faithful async iter_download/close shape with per-chunk real awaits."""
    def __init__(self, owner, chunks):
        self.owner=owner
        self.chunks=iter(chunks)
        self.closed=False
    def __aiter__(self):
        return self
    async def __anext__(self):
        await asyncio.sleep(0)
        if self.owner.on_chunk:
            res=self.owner.on_chunk()
            if asyncio.iscoroutine(res):
                await res
        try:
            return next(self.chunks)
        except StopIteration:
            raise StopAsyncIteration
    async def close(self):
        await asyncio.sleep(0)
        self.closed=True
        if self.owner.on_close:
            res=self.owner.on_close()
            if asyncio.iscoroutine(res):
                await res


class LeasedFakeTelethon:
    def __init__(self, *, content=None):
        self.message=message()
        self.content=png() if content is None else content
        self.on_message=None
        self.on_chunk=None
        self.on_close=None
        self.events=[]
        self.stream=None
        self.chunks=None
    async def get_messages(self, chat, **args):
        self.events.append("read-message")
        assert chat==CHAT and args=={"ids":16}
        await asyncio.sleep(0)
        if self.on_message:
            outcome=self.on_message()
            if asyncio.iscoroutine(outcome):
                await outcome
        return self.message
    def iter_download(self, media, *, request_size, chunk_size):
        assert media is self.message.photo or media is self.message.document
        assert request_size==chunk_size==65536
        self.events.append("iter-download")
        chunks=self.chunks if self.chunks is not None else [self.content]
        self.stream=NativeFakeStream(self,chunks)
        return self.stream
    async def send_file(self, *a,**kw):
        raise AssertionError("media READ may NEVER create a second send")


def fixture(*, content=None, archive_scope="owner-owned-topic4"):
    client=LeasedFakeTelethon(content=content)
    lease=[True]
    clock=[100.]
    owner=NativeTelegramIllustrations(
        client=client,chat_id=CHAT,topic_id=TOPIC,current=lambda:lease[0],
        archive_scope=archive_scope,clock=lambda:clock[0])
    native=snapshot()
    native.archive_state["photo"]["width"]=20
    native.archive_state["photo"]["height"]=20
    return owner,client,lease,clock,native


@pytest.mark.asyncio
async def test_native_actual_issued_png_bytes_current_lease_same_existing_client_no_send():
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    assert type(proof) is NativeIllustrationArchiveProof
    assert proof.owner is owner and proof.actor_id==PERSON
    assert proof.source_id==SOURCE and proof.illustration_id==ILLUSTRATION
    assert owner.issuance_current(proof)
    value=await owner.read_issued_bytes(proof)
    assert type(value) is NativeIllustrationIssuedBytes
    assert value.proof is proof and value.mime_type=="image/png"
    assert value.data==png() and value.sha256==hashlib.sha256(value.data).hexdigest()
    assert client.events==["read-message","read-message","iter-download","read-message"]
    assert client.stream.closed
    owner.release_issued(proof)
    assert not owner.issuance_current(proof)
    with pytest.raises(IllustrationArchiveReadUnavailable):
        await owner.read_issued_bytes(proof)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason",[
    "wrong_actor","stale_scope","withdrawn","wrong_original","wrong_media_sha",
    "pending","unknown","no_locator","wrong_topic","wrong_crop","wrong_operation",
])
async def test_native_archive_mint_denies_untrusted_actor_source_crop_and_status(reason):
    owner,client,lease,clock,native=fixture()
    actor=PERSON
    if reason=="wrong_actor":actor="fake-actor"
    elif reason=="stale_scope":native=replace(native,archive_scope="wrong-owner")
    elif reason=="withdrawn":native=replace(native,withdrawn=True)
    elif reason=="wrong_original":native=replace(native,origin_original_sha256="a"*64)
    elif reason=="wrong_media_sha":native=replace(native,media_origin_sha256="e"*64)
    elif reason=="pending":native.archive_state["status"]="pending"
    elif reason=="unknown":native.archive_state["status"]="unknown"
    elif reason=="no_locator":native.archive_state["photo"]=None
    elif reason=="wrong_topic":native.archive_state["photo"]["topic_id"]=2
    elif reason=="wrong_crop":native.item["crop_binding"]["source_id"]="not-source"
    elif reason=="wrong_operation":native.archive_state["operation_key"]="illustration:"+"f"*64
    with pytest.raises((IllustrationArchiveReadUnavailable,ValueError)):
        await owner.read_opaque(native,actor)
    assert not owner._issued_proofs
    assert "iter-download" not in client.events


@pytest.mark.asyncio
async def test_native_issued_ref_is_not_a_bearer_and_copied_proof_does_not_grant_pixels():
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    copied=replace(proof)
    for candidate in (copied,proof.ref,replace(proof,actor_id=SOURCE)):
        with pytest.raises((IllustrationArchiveReadUnavailable,PermissionError)):
            await owner.read_issued_bytes(candidate)
    assert client.events.count("iter-download")==0
    assert owner.issuance_current(proof)
    await owner.read_issued_bytes(proof)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage",["before_observe","during_mint_observe",
                                  "before_pixels","during_chunk","after_pixels_cleanup"])
async def test_native_lease_revoke_at_every_await_never_issues_stale_bytes(stage):
    owner,client,lease,clock,native=fixture()
    if stage=="before_observe":lease[0]=False
    if stage=="during_mint_observe":client.on_message=lambda:lease.__setitem__(0,False)
    if stage in ("before_observe","during_mint_observe"):
        with pytest.raises((IllustrationArchiveReadUnavailable,PermissionError)):
            await owner.read_opaque(native,PERSON)
        assert "iter-download" not in client.events
        return
    proof=await owner.read_opaque(native,PERSON)
    if stage=="before_pixels":lease[0]=False
    elif stage=="during_chunk":client.on_chunk=lambda:lease.__setitem__(0,False)
    elif stage=="after_pixels_cleanup":client.on_close=lambda:lease.__setitem__(0,False)
    with pytest.raises((IllustrationArchiveReadUnavailable,PermissionError)):
        await owner.read_issued_bytes(proof)
    assert not (stage=="before_pixels" and "iter-download" in client.events)
    if client.stream is not None:assert client.stream.closed


@pytest.mark.asyncio
async def test_native_lease_reconnect_same_scope_new_owner_rejects_old_proof():
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    # An exact same chat/topic with a NEW native client object is not owner.
    new_client=LeasedFakeTelethon()
    other=NativeTelegramIllustrations(
        client=new_client,chat_id=CHAT,topic_id=TOPIC,
        current=lambda:True,archive_scope="owner-owned-topic4")
    assert not other.issuance_current(proof)
    with pytest.raises(IllustrationArchiveReadUnavailable):
        await other.read_issued_bytes(proof)
    lease[0]=False
    assert not owner.issuance_current(proof)


@pytest.mark.asyncio
async def test_native_expiry_caps_memory_and_expired_result_is_never_reused():
    owner,client,lease,clock,native=fixture()
    owner.max_proofs=1
    proof=await owner.read_opaque(native,PERSON)
    with pytest.raises(IllustrationArchiveReadUnavailable,match="proof_capacity"):
        await owner.read_opaque(native,PERSON)
    clock[0]=111
    assert not owner.issuance_current(proof)
    with pytest.raises(IllustrationArchiveReadUnavailable):
        await owner.read_issued_bytes(proof)
    renewed=await owner.read_opaque(native,PERSON)
    assert renewed is not proof and owner.issuance_current(renewed)


@pytest.mark.asyncio
async def test_native_locator_media_changed_before_and_after_download_denied():
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    client.message.photo.id=51
    with pytest.raises((IllustrationArchiveReadUnavailable,ValueError)):
        await owner.read_issued_bytes(proof)
    assert "iter-download" not in client.events

    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    client.on_chunk=lambda:setattr(client.message.photo,"id",51)
    with pytest.raises((IllustrationArchiveReadUnavailable,ValueError)):
        await owner.read_issued_bytes(proof)
    assert client.stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("case",["oversize","unexpected_chunk","invalid_pixels",
                                 "wrong_dimensions","changed_topic","truncated"])
async def test_native_stream_bounds_and_real_pixel_validation_deny_unknown(case):
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    if case=="oversize":client.chunks=[bytes(65536) for _ in range(5)]
    elif case=="unexpected_chunk":client.chunks=["not bytes"]
    elif case=="invalid_pixels":client.chunks=[b"provider-JSON-not-image"]
    elif case=="wrong_dimensions":client.chunks=[png(width=22)]
    elif case=="changed_topic":client.on_chunk=lambda:setattr(client.message.reply_to,"reply_to_top_id",2)
    elif case=="truncated":client.chunks=[png()[:20]]
    with pytest.raises((IllustrationArchiveReadUnavailable,ValueError)):
        await owner.read_issued_bytes(proof)
    assert client.stream.closed
    assert "iter-download" in client.events


@pytest.mark.asyncio
async def test_cancelled_native_chunk_read_closes_existing_iterator_without_issuer_effect():
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    started=asyncio.Event()
    pending=asyncio.Event()
    async def interrupt():
        started.set()
        await pending.wait()
    client.on_chunk=interrupt
    task=asyncio.create_task(owner.read_issued_bytes(proof))
    await asyncio.wait_for(started.wait(),2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.stream.closed
    assert owner.issuance_current(proof)  # Valid proof is NOT a successful byte receipt.


@pytest.mark.asyncio
async def test_native_provider_errors_redacted_without_leaking_private_session():
    owner,client,lease,clock,native=fixture()
    proof=await owner.read_opaque(native,PERSON)
    async def failed(*args,**kwargs):
        raise RuntimeError("PRIVATE SESSION TOKEN")
    client.get_messages=failed
    with pytest.raises(IllustrationArchiveReadUnavailable) as error:
        await owner.read_issued_bytes(proof)
    assert "PRIVATE" not in str(error.value)
