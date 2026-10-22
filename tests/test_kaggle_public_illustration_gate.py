"""Public media issuance on REAL ReadAuthority/SourceReadGate, fake native I/O only.

The exact existing media015 helper is injected as render_metadata and
validate_archive_state by the owner. These test callbacks have their *same*
pure contract; production module stays untouched/unregistered in this branch.
No PostgreSQL, Telegram, model, network, provider, credentials or sender.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
from uuid import UUID

import pytest

from regional_knowledge.kaggle_public_illustration_gate import (
    ArchiveOpaqueProof, NativeMediaSnapshot, PublicIllustrationFrame,
    PublicIllustrationOutputGate, PublicMediaUnavailable, _hash,
)
from regional_knowledge.participant_contracts import ParticipantGrant
from test_control_spool import env, SOURCE
from test_read_authority import auth
from test_source_read_gate import setup
from test_participants import change, people


ILLUSTRATION = "20000000-0000-0000-0000-000000000002"
PAGE = "30000000-0000-0000-0000-000000000003"
REGION = "40000000-0000-0000-0000-000000000004"
ALT_PAGE = "50000000-0000-0000-0000-000000000005"
READER_SHA = "e" * 64


def snapshot(*, source=SOURCE, status="ready"):
    bbox = dict(left=20, top=50, right=800, bottom=720)
    page = dict(page_id=PAGE, source_image_receipt_sha256="1" * 64,
                image_sha256="2" * 64)
    region = dict(region_id=REGION, page_id=PAGE, kind="figure", bbox=bbox)
    crop = dict(
        source_id=source, source_revision=1, illustration_id=ILLUSTRATION,
        page_id=PAGE, source_region_id=REGION, bbox=bbox,
        display_rotation_degrees=90,
        source_image_receipt_sha256=page["source_image_receipt_sha256"],
        source_raster_sha256=page["image_sha256"], original_sha256="3" * 64,
        rendition="preview")
    item = dict(illustration_key="synthetic-figure", illustration_id=ILLUSTRATION,
                page_id=PAGE, source_region_id=REGION, bbox=bbox, kind="photo",
                caption_region_ids=[], nearby_region_ids=[],
                caption_text="Synthetic printed caption",
                visual_description="Model observation only",
                visual_description_provenance="model_observation",
                visual_description_language="en",
                display_rotation_degrees=90, crop_binding=crop)
    state = dict(
        status=status, binding=deepcopy(crop),
        operation_key="illustration:" + _hash(crop),
        photo=dict(chat_id=-100234, topic_id=4, message_id=16, media_id=50,
                   kind="photo", width=800, height=670), document=None)
    public = dict(source_id=source, source_revision=1,
                  eligibility_revision=1, illustration_id=ILLUSTRATION,
                  data={k:deepcopy(v) for k,v in item.items() if k!="crop_binding"},
                  archive_status=status)
    return NativeMediaSnapshot(
        source_id=source, illustration_id=ILLUSTRATION,
        source_revision=1, eligibility_revision=1, accepted=True,
        withdrawn=False, original_sha256="3"*64, origin_original_sha256="3"*64,
        media_candidate_sha256="4"*64, media_origin_sha256="4"*64,
        item=item, manifest_page=page, manifest_region=region,
        public_row=public, archive_state=state, archive_chat_id=-100234,
        archive_topic_id=4, archive_scope="owner-owned-topic4",
        native_reader_sha256=READER_SHA, database_generation="db-source",
        observed_data_seq=0, observed_projection_seq=0,
        observed_visibility_epoch=0)


def media_renderer(item, *, status):
    """Test copy of the media leaf's pure allowed-fields contract, not a product renderer."""
    assert status == "ready"
    allowed = {"illustration_key", "illustration_id", "page_id", "source_region_id",
               "bbox", "kind", "caption_region_ids", "nearby_region_ids",
               "caption_text", "visual_description", "visual_description_provenance",
               "visual_description_language", "display_rotation_degrees"}
    result = {k:deepcopy(v) for k,v in item.items() if k in allowed}
    result.update(artifact_class="public_illustration", rendition="preview", status=status)
    return result


def archive_validator(state, *, binding, archive_chat_id, archive_topic_id):
    """Exact checks verified by the existing media leaf validate_archive_state."""
    if (type(state) is not dict or
            set(state) != {"status", "binding", "operation_key", "photo", "document"} or
            state["binding"] != binding or
            state["operation_key"] != "illustration:" + _hash(binding)):
        raise ValueError("wrong native archive binding")
    if state["status"] != "ready":
        raise ValueError("only ready is eligible for an output artifact")
    if not any(state.get(k) for k in ("photo","document")):
        raise ValueError("ready lacks a media locator")
    for kind in ("photo","document"):
        locator = state[kind]
        if locator is None:continue
        if (locator["kind"] != kind or locator["chat_id"] != archive_chat_id
                or locator["topic_id"] != archive_topic_id):
            raise ValueError("wrong native archive destination")
    return state


class NativeRead:
    def __init__(self, first):
        self.first = first
        self.second = first
        self.calls = 0
        self.on_call = None

    async def __call__(self, source, illustration, person, binding):
        self.calls += 1
        await asyncio.sleep(0) # simulate a genuine asynchronous native read
        if self.on_call:
            self.on_call(self.calls)
        assert source == SOURCE and illustration == ILLUSTRATION
        return deepcopy(self.first if self.calls == 1 else self.second)


class ExistingArchiveReader:
    """Simulates the current native archive's independent current-lease permit."""

    def __init__(self):
        self.enabled = True
        self.mint_count = 0
        self.issued = {}
        self.on_read = None
        self.scope_override = None
        self.owner_override = None

    async def read_opaque(self, native, actor_id):
        self.mint_count += 1
        await asyncio.sleep(0)
        if self.on_read:
            self.on_read()
        crop = native.item["crop_binding"]
        proof = ArchiveOpaqueProof(
            ref="rkb-art-" + "a" * 64,
            source_id=native.source_id,
            illustration_id=native.illustration_id,
            actor_id=actor_id,
            binding_sha256=_hash(crop),
            archive_scope=self.scope_override or native.archive_scope,
            owner=self if self.owner_override is None else self.owner_override)
        self.issued[id(proof)] = proof
        return proof

    def issuance_current(self, proof):
        return (self.enabled and self.issued.get(id(proof)) is proof)


@pytest.fixture
def supply():
    return snapshot


async def make_gate(env, auth, source=None):
    native = NativeRead(source or snapshot())
    archive = ExistingArchiveReader()
    sg, verified, binding = await setup(env, auth)
    gate = PublicIllustrationOutputGate(
        authority=auth.resolver, source_gate=sg,
        native_read=native, archive=archive,
        render_metadata=media_renderer, validate_archive_state=archive_validator,
        native_reader_sha256=READER_SHA)
    return gate, native, archive, verified, binding


async def issue(gate, verified, binding):
    return await gate.issue(verified=verified, binding=binding,
                            source_id=SOURCE, illustration_id=ILLUSTRATION)


@pytest.mark.asyncio
async def test_valid_existing_authority_and_archive_opaque_artifact_is_only_public_surface(env, auth):
    gate, native, archive, verified, binding = await make_gate(env, auth)
    output = await issue(gate, verified, binding)
    assert type(output.frame) is PublicIllustrationFrame
    assert output.frame.artifact.ref == "rkb-art-" + "a"*64
    assert output.frame.metadata["status"] == "ready"
    assert output.frame.metadata["caption_text"] == "Synthetic printed caption"
    assert native.calls == 2 and archive.mint_count == 1
    assert gate.issuance_current(output.ticket)
    text = output.frame.model_dump_json()
    for private in ("crop_binding","original_sha256","chat_id","topic_id",
                    "media_id","message_id","photo","document","archive_scope",
                    "archive_chat_id","credential","http://","https://"):
        assert private not in text
    gate.release(output.ticket)
    assert not gate.issuance_current(output.ticket)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending","unknown","registered","unavailable","failed"])
async def test_native_archive_status_is_never_implicitly_saved_or_public(env, auth, status):
    gate, native, archive, verified, binding = await make_gate(env, auth, snapshot(status=status))
    with pytest.raises(PublicMediaUnavailable):
        await issue(gate, verified, binding)
    assert archive.mint_count == 0 and native.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    "withdrawn","rev","eligibility","origin","media_sha",
    "page","region","kind","bbox","crop","raster_sha","receipt_sha",
    "public_row","image_reader","wrong_archive","wrong_topic",
    "noncurrent_pg","different_db","unaccepted"
])
async def test_exact_source_original_figure_and_archive_scope_mismatches_fail_closed(env, auth, change):
    original=snapshot()
    x=deepcopy(original)
    changes={}
    if change=="withdrawn":changes=dict(withdrawn=True)
    elif change=="rev":changes=dict(source_revision=2)
    elif change=="eligibility":changes=dict(eligibility_revision=2)
    elif change=="origin":changes=dict(origin_original_sha256="f"*64)
    elif change=="media_sha":changes=dict(media_origin_sha256="e"*64)
    elif change=="page":x.manifest_page["page_id"]=ALT_PAGE
    elif change=="region":x.manifest_region["region_id"]=ALT_PAGE
    elif change=="kind":x.manifest_region["kind"]="text"
    elif change=="bbox":x.manifest_region["bbox"]["left"]=1
    elif change=="crop":x.item["crop_binding"]["source_region_id"]=ALT_PAGE
    elif change=="raster_sha":x.manifest_page["image_sha256"]="f"*64
    elif change=="receipt_sha":x.manifest_page["source_image_receipt_sha256"]="f"*64
    elif change=="public_row":x.public_row["archive_status"]="pending"
    elif change=="image_reader":changes=dict(native_reader_sha256="f"*64)
    elif change=="wrong_archive":x.archive_state["photo"]["chat_id"]=-100888
    elif change=="wrong_topic":changes=dict(archive_topic_id=9)
    elif change=="noncurrent_pg":changes=dict(observed_projection_seq=2)
    elif change=="different_db":changes=dict(database_generation="stale")
    else:changes=dict(accepted=False)
    x=replace(x,**changes)
    gate,native,archive,verified,binding=await make_gate(env,auth,x)
    with pytest.raises(PublicMediaUnavailable):
        await issue(gate,verified,binding)
    assert archive.mint_count==0


@pytest.mark.asyncio
async def test_authority_revoked_during_first_native_await_does_not_release_content(env, auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    def revoke(n):
        if n==1:
            change(auth.p, ParticipantGrant(capability="public_search",granted=False,
                                             reason="test revoke before native callback"))
    native.on_call=revoke
    with pytest.raises(PermissionError):
        await issue(gate,verified,binding)
    assert archive.mint_count==0 and not gate._tickets


@pytest.mark.asyncio
async def test_authority_revoked_during_archive_await_no_stale_cached_frame(env, auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    archive.on_read=lambda:change(auth.p,ParticipantGrant(
        capability="public_search",granted=False,reason="test revoke during archive"))
    with pytest.raises(PermissionError):
        await issue(gate,verified,binding)
    assert native.calls==1 and not gate._tickets


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["withdraw","revision","raster","media_status","archive_scope"])
async def test_source_changed_during_archive_await_second_pg_read_rejects(env, auth, change):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    x=deepcopy(native.first)
    if change=="withdraw":x=replace(x,withdrawn=True)
    if change=="revision":x=replace(x,source_revision=2)
    if change=="raster":x.manifest_page["image_sha256"]="a"*64
    if change=="media_status":x=replace(x,archive_state={**x.archive_state,"status":"unknown"})
    if change=="archive_scope":x=replace(x,archive_scope="other-archive-owner")
    native.second=x
    with pytest.raises(PublicMediaUnavailable):
        await issue(gate,verified,binding)
    assert native.calls==2 and not gate._tickets


@pytest.mark.asyncio
async def test_archive_access_wrong_actor_scope_owner_or_lost_lease_is_denied(env, auth):
    for kind in ("scope","owner","lease"):
        gate,native,archive,verified,binding=await make_gate(env,auth)
        if kind=="scope":archive.scope_override="other-archive"
        if kind=="owner":archive.owner_override=object()
        if kind=="lease":archive.on_read=lambda:setattr(archive,"enabled",False)
        with pytest.raises(PublicMediaUnavailable):
            await issue(gate,verified,binding)
        assert not gate._tickets


@pytest.mark.asyncio
async def test_native_source_history_fence_invalidated_during_archive_rejects(env,auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    archive.on_read=env.spool.invalidate_source_history
    with pytest.raises(PublicMediaUnavailable):
        await issue(gate,verified,binding)
    assert native.calls==1 and not gate._tickets


@pytest.mark.asyncio
async def test_issuance_rejects_reconnect_withdrawal_fence_archive_loss_and_timeout(env,auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    output=await issue(gate,verified,binding)
    assert gate.issuance_current(output.ticket)
    archive.enabled=False
    assert not gate.issuance_current(output.ticket)
    archive.enabled=True
    assert gate.issuance_current(output.ticket)
    binding.runtime_incarnation="reconnected"
    assert not gate.issuance_current(output.ticket)
    # A separate verified output cannot reuse a stale ticket after source
    # history changes; no provider result is reinterpreted as auth.
    binding.runtime_incarnation="inc"
    env.spool.invalidate_source_history()
    assert not gate.issuance_current(output.ticket)


@pytest.mark.asyncio
async def test_wrong_renderer_cannot_leak_private_locator_even_after_archive_success(env,auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    def malicious(item,*,status):
        return {**media_renderer(item,status=status),
                "crop_binding":item["crop_binding"],
                "archive_locator":{"chat_id":-100234}}
    gate.render_metadata=malicious
    with pytest.raises(PublicMediaUnavailable,match="untrusted_public_media_renderer"):
        await issue(gate,verified,binding)
    assert archive.mint_count==1 and not gate._tickets


@pytest.mark.asyncio
async def test_unwired_or_wrong_authority_sourcegate_fails_closed_before_call(env,auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    from regional_knowledge.read_authority import ReadAuthority
    from regional_knowledge.source_read_gate import SourceReadGate
    assert isinstance(gate.authority,ReadAuthority)
    assert isinstance(gate.source_gate,SourceReadGate)
    with pytest.raises(TypeError):
        PublicIllustrationOutputGate(
            authority=auth.resolver,source_gate=gate.source_gate,
            native_read=native,archive=object(),render_metadata=media_renderer,
            validate_archive_state=archive_validator,native_reader_sha256=READER_SHA)
    from dataclasses import replace as replace_model
    with pytest.raises(PublicMediaUnavailable):
        await gate.issue(verified=replace_model(verified,person_id=SOURCE),
                         binding=binding,source_id=SOURCE,illustration_id=ILLUSTRATION)
    assert not gate._tickets


@pytest.mark.asyncio
async def test_nested_bbox_private_metadata_cannot_cross_serializer(env, auth):
    x=snapshot()
    x.item["bbox"]["chat_id"]=-100234
    x.manifest_region["bbox"]["chat_id"]=-100234
    x.item["crop_binding"]["bbox"]["chat_id"]=-100234
    x.public_row["data"]["bbox"]["chat_id"]=-100234
    gate,native,archive,verified,binding=await make_gate(env,auth,x)
    with pytest.raises(PublicMediaUnavailable,match="invalid_media_figure_bbox"):
        await issue(gate,verified,binding)
    assert not gate._tickets


@pytest.mark.asyncio
async def test_renderer_cannot_change_caption_without_private_field_leak(env,auth):
    gate,native,archive,verified,binding=await make_gate(env,auth)
    def changed_caption(item,*,status):
        result=media_renderer(item,status=status)
        result["caption_text"]="Other model statement"
        return result
    gate.render_metadata=changed_caption
    with pytest.raises(PublicMediaUnavailable,match="untrusted_public_media_renderer"):
        await issue(gate,verified,binding)
    assert not gate._tickets
