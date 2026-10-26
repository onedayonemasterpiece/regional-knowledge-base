"""Actual existing illustration_fetch → canonical backend → final ASGI media path.

Real ReadAuthority, SourceReadGate, ControlSpool, KnowledgeMCPServer and
ReadIssuanceMiddleware; ONLY native PostgreSQL/archive owner are synthetic.
The PNG fixture is real encoded pixels, but NOT a Telegram/provider receipt.
No Dev PG, Kaggle, provider, login secret output, WSS or runtime changes.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
from copy import deepcopy
from dataclasses import replace
from io import BytesIO
import json
from types import SimpleNamespace

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from PIL import Image
from starlette.requests import Request

from regional_knowledge.kaggle_public_illustration_gate import (
    PublicIllustrationOutputGate, IssuedArtifactBytes,
)
from regional_knowledge.read_issuance import ReadIssuanceMiddleware
from regional_knowledge.server import build_server
from test_kaggle_service_startup import service
# Register the REAL existing multi-layer fixture's dependencies. Pytest does
# not register re-exported fixtures from another module automatically.
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people
from test_kaggle_public_illustration_gate import (
    NativeRead, ExistingArchiveReader, snapshot, media_renderer,
    archive_validator, READER_SHA, SOURCE, ILLUSTRATION,
)
from test_participants import change
from regional_knowledge.participant_contracts import ParticipantGrant


def image_bytes():
    output = BytesIO()
    Image.new("RGB", (20, 20), (12, 25, 39)).save(output, format="PNG")
    return output.getvalue()


class SyntheticArchivePixels(ExistingArchiveReader):
    """TEST ONLY — not a production opaque artifact issuer or Telegram client."""

    def __init__(self, *, at_read=None, content=None):
        super().__init__()
        self.read_count = 0
        self.at_read = at_read
        self.payload = content if content is not None else image_bytes()

    async def read_issued_bytes(self, proof):
        self.read_count += 1
        await asyncio.sleep(0)  # actual I/O completion boundary
        if self.at_read is not None:
            value = self.at_read()
            if asyncio.iscoroutine(value):
                await value
        return IssuedArtifactBytes(
            proof=proof, data=self.payload,
            mime_type="image/png", sha256=hashlib.sha256(self.payload).hexdigest(),
        )


async def configured(service_fixture, monkeypatch, *, pixels=None):
    s=service_fixture
    s.adapter.available=True
    startup=s.startup
    from regional_knowledge.kaggle_public_backend import current_binding
    # Canonical backing generation/lease comes from the existing real queue.
    current=current_binding(s.target.queue)
    sampled=replace(snapshot(), database_generation=current.database_generation,
                    observed_data_seq=current.min_data_seq,
                    observed_projection_seq=current.min_data_seq)
    native=NativeRead(sampled)
    archive=pixels or SyntheticArchivePixels()
    gate=PublicIllustrationOutputGate(
        authority=startup.authority, source_gate=startup.gate,
        native_read=native, archive=archive,
        render_metadata=media_renderer,
        validate_archive_state=archive_validator,
        native_reader_sha256=READER_SHA)
    startup.backend.configure_public_illustrations(gate)
    monkeypatch.setattr("regional_knowledge.server._principal",lambda:s.target.principal)
    server=build_server(target_startup=startup)
    assert server._target_media_available is True
    return SimpleNamespace(server=server,gate=gate,archive=archive,native=native,
        startup=startup,service=s)


async def call_http(s, *, id=None, before_send=None, at_render=None):
    """Invoke REAL declared MCP tool + REAL request-local ASGI issuance."""
    id=id or f"rkb:illustration:v1:{SOURCE}:{ILLUSTRATION}"
    sent=[]
    request_id="real-media-fixture-id"
    if at_render is not None:
        render=s.startup.backend._render_issued_media
        def wrap(value):
            at_render()
            return render(value)
        s.startup.backend._render_issued_media=wrap
    async def app(scope, receive, send):
        context=SimpleNamespace(request_context=SimpleNamespace(
            request=Request(scope), request_id=request_id))
        called=await s.server.call_tool("illustration_fetch",{"id":id},context)
        assert called is not None
        raw=json.dumps({"jsonrpc":"2.0","id":request_id,
                        "result":called.model_dump(mode="json",
                                                   by_alias=True,
                                                   exclude_none=True)}).encode()
        await send({"type":"http.response.start","status":200,
                    "headers":[(b"content-type",b"application/json")]})
        await send({"type":"http.response.body","body":raw[:31],"more_body":True})
        await send({"type":"http.response.body","body":raw[31:],"more_body":False})
    async def sender(message):
        if before_send:
            before_send(message)
        sent.append(message)
    async def receive():
        return {"type":"http.disconnect"}
    await ReadIssuanceMiddleware(app)(
        {"type":"http","method":"POST","path":"/mcp","headers":[]},receive,sender)
    body=b"".join(x.get("body",b"") for x in sent)
    return json.loads(body),body


@pytest.mark.asyncio
async def test_existing_illustration_fetch_real_sdk_tool_and_final_imagecontent(service,monkeypatch):
    c=await configured(service,monkeypatch)
    async with c.startup.lifespan(None):
        assert c.startup.reads_current()
        data,wire=await call_http(c)
        result=data["result"]
        assert data["id"]=="real-media-fixture-id"
        assert result["isError"] is False
        assert len(result["content"])==2
        assert result["content"][0]["type"]=="text"
        public=json.loads(result["content"][0]["text"])
        assert public["status"]=="ready"
        assert public["metadata"]["caption_text"]=="Synthetic printed caption"
        assert public["artifact"]["kind"]=="server_opaque_illustration"
        assert result["content"][1]["type"]=="image"
        assert result["content"][1]["mimeType"]=="image/png"
        assert base64.b64decode(result["content"][1]["data"])==image_bytes()
        assert c.archive.read_count==1 and c.native.calls>=5
        assert not c.gate._tickets  # request-local release, not retained as bearer
        for secret in ("archive_locator","crop_binding","chat_id","topic_id",
                       "message_id","media_id","provider_ref","credential"):
            assert secret.encode() not in wire
        assert b'final_media_issuance_pending' not in wire


@pytest.mark.asyncio
async def test_missing_issuer_gate_remains_unadvertised_without_legacy_fallback(service,monkeypatch):
    s=service
    monkeypatch.setattr("regional_knowledge.server._principal",lambda:s.target.principal)
    server=build_server(target_startup=s.startup)
    assert not server._target_media_available
    context=SimpleNamespace(request_context=SimpleNamespace(
        request=Request({"type":"http","method":"POST","path":"/mcp","headers":[]}),
        request_id="closed-media"))
    with pytest.raises(ToolError,match="tool_not_supported_in_profile"):
        await server.call_tool("illustration_fetch",
            {"id":f"rkb:illustration:v1:{SOURCE}:{ILLUSTRATION}"},context)
    assert s.startup.backend.public_media_gate is None


@pytest.mark.asyncio
async def test_archive_has_no_real_byte_issuer_cannot_be_configured(service):
    s=service
    from regional_knowledge.kaggle_public_illustration_gate import PublicIllustrationOutputGate
    from test_kaggle_public_illustration_gate import ExistingArchiveReader
    gate=PublicIllustrationOutputGate(
        authority=s.startup.authority,source_gate=s.startup.gate,
        native_read=NativeRead(snapshot()),archive=ExistingArchiveReader(),
        render_metadata=media_renderer,validate_archive_state=archive_validator,
        native_reader_sha256=READER_SHA)
    with pytest.raises(TypeError, match="real image issuer required"):
        s.startup.backend.configure_public_illustrations(gate)


@pytest.mark.asyncio
@pytest.mark.parametrize("case",["revoke_at_bytes","source_withdraw_at_bytes","native_lease_lost","wrong_sha","bad_image"])
async def test_actual_handler_preserves_current_fences_without_media_hash_comparison(
    service,monkeypatch,case):
    s=service
    def changes():
        if case=="revoke_at_bytes":
            change(s.target.auth.p,ParticipantGrant(
                capability="public_search",granted=False,reason="synthetic after-image revoke"))
        elif case=="source_withdraw_at_bytes":
            s.startup.config.spool.invalidate_source_history()
        elif case=="native_lease_lost":
            s.adapter.revoked=True
    pixels=SyntheticArchivePixels(at_read=changes)
    if case=="bad_image":
        pixels.payload=b"not a PNG image"
    c=await configured(service,monkeypatch,pixels=pixels)
    if case=="wrong_sha":
        orig=pixels.read_issued_bytes
        async def corrupted(proof):
            value=await orig(proof)
            return replace(value,sha256="f"*64)
        pixels.read_issued_bytes=corrupted
    async with c.startup.lifespan(None):
        data,wire=await call_http(c)
        if case=='wrong_sha':
            assert data['result']['isError'] is False
            assert data['result']['content'][1]['type']=='image'
            return
        assert data["result"]["isError"] is True
        assert data["result"]["content"][0]["type"]=="text"
        assert '"status":"unavailable"' in data["result"]["content"][0]["text"]
        assert '"type":"image"' not in wire.decode()
        assert "iVBOR" not in wire.decode()
        assert not c.gate._tickets


@pytest.mark.asyncio
async def test_actual_handler_rechecks_sync_actor_after_renderer_before_final_send(
    service,monkeypatch):
    c=await configured(service,monkeypatch)
    async with c.startup.lifespan(None):
        def revoke():
            change(c.service.target.auth.p,ParticipantGrant(
                capability="public_search",granted=False,
                reason="synthetic sync final actor revoke"))
        data,wire=await call_http(c,at_render=revoke)
        assert data["result"]["isError"] is True
        assert b'iVBOR' not in wire
        assert not c.gate._tickets


@pytest.mark.asyncio
async def test_actual_handler_reconnect_with_same_visible_binding_fields_blocks_old_ticket(
    service,monkeypatch):
    c=await configured(service,monkeypatch)
    async with c.startup.lifespan(None):
        # Archive read changes the current native owner while the old read
        # ticket is outstanding. No issuer may refresh old generation.
        c.archive.at_read=lambda: setattr(c.service.adapter,"revoked",True)
        data,wire=await call_http(c)
        assert data["result"]["isError"] is True
        assert b'iVBOR' not in wire
        assert not c.gate._tickets


@pytest.mark.asyncio
async def test_target_media_selector_is_only_a_pair_of_ids_not_an_access_token(service,monkeypatch):
    c=await configured(service,monkeypatch)
    async with c.startup.lifespan(None):
        from regional_knowledge.kaggle_public_backend import _media_selector,PublicQueryInvalid
        for locator in ("knowledge://illustrations/"+ILLUSTRATION,
                        f"rkb:illustration:v1:{ILLUSTRATION}",
                        "rkb:illustration:v1:AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA:"+ILLUSTRATION,
                        f"rkb:illustration:v1:{SOURCE}:not-a-uuid"):
            with pytest.raises(PublicQueryInvalid):
                _media_selector(locator)
        assert _media_selector(f"rkb:illustration:v1:{SOURCE}:{ILLUSTRATION}")==(
            SOURCE,ILLUSTRATION)
