"""Small typed research surface; the adapter supplies authority on every call.

This surface does not expose import/scan or create provider observations. A
channel adapter must supply its verified, route-limited RequestAuthority rather
than inheriting this owner's MCP authentication.
"""
import asyncio
import logging
from typing import Annotated, Any, Literal
from uuid import UUID

from mcp.types import ToolAnnotations
from pydantic import Field

from .disagreement_contracts import (
    CaseCreate, CaseGrant, CaseRevise, CaseState, ConclusionAdd,
    ContributionAdd, ContributionCorrect, EvidenceAdd, PersonalDraft, DraftPublication,
)
from .disagreement_drafts import PersonalDrafts
from .disagreement_journal import DisagreementJournal, JournalError
from .participant_contracts import (
    AccountBegin, AccountConfirm, AccountUnlink, DomainWeight,
    ParticipantDisable, ParticipantGrant, ParticipantUpsert,
)
from .participants import ParticipantError


def register_research_tools(mcp, participants, authority_provider):
    journal = DisagreementJournal(participants.corpus, participants=participants)
    drafts = PersonalDrafts(journal)

    async def call(fn, *args, **kwargs):
        # Resolve inside each call: no cached actor or shared channel context.
        context = authority_provider()
        try:
            return await asyncio.to_thread(fn, context, *args, **kwargs)
        except (ParticipantError, JournalError) as exc:
            logging.getLogger(__name__).info("research_operation_rejected",extra={"operation":fn.__name__,"reason_code":exc.code})
            return {"error":{"code":exc.code}}

    def invalid():
        return {"error":{"code":"invalid_arguments"}}

    @mcp.tool(name="disagreement_participants", title="Manage research participants",
        description="Existing RKB person UUIDs, explicit expiring capabilities and domain weights; categories grant no access. Owner route alone can change profiles or verified external bindings. No external tokens. Use operation_status after a lost response. Supply a stable person UUID for a new participant and the same operation key for replay. Provider observations must already exist from a trusted verifier; client claims cannot create them.",
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def participant_tool(action:Literal["get","list","audit","change","operation_status"],
        person_id:UUID|None=None,
        command:ParticipantUpsert|ParticipantDisable|ParticipantGrant|DomainWeight|AccountBegin|AccountConfirm|AccountUnlink|None=None,
        expected_revision:Annotated[int,Field(ge=0)]|None=None,
        operation_key:str|None=None, after_id:str="",
        after_revision:Annotated[int,Field(ge=0)]=0,
        limit:Annotated[int,Field(ge=1,le=100)]=20) -> dict[str,Any]:
        if action=="get" and person_id: return await call(participants.get,person_id)
        if action=="list": return await call(participants.list,after_id=after_id,limit=limit)
        if action=="audit" and person_id: return await call(participants.audit,person_id,after_revision=after_revision,limit=limit)
        if action=="operation_status" and operation_key: return await call(participants.operation_status,operation_key)
        if action=="change" and person_id and command is not None and expected_revision is not None and operation_key:
            return await call(participants.change,person_id,expected_revision,command,operation_key)
        return invalid()

    @mcp.tool(name="disagreement_get",title="Read a research case",
        description="Read a case or complete paginated evidence, contributions, conclusions and history. after_seq is per-case; follow next_after_seq while has_more. Source ACL and current participant rights apply even to history and receipts. An accepted text match is not visual or human confirmation.",
        annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def get_tool(case_id:UUID, section:Literal["case","evidence","contributions","conclusions","history"]="case",
        after_seq:Annotated[int,Field(ge=0)]=0,limit:Annotated[int,Field(ge=1,le=100)]=20,
        revision:Annotated[int,Field(ge=1)]|None=None) -> dict[str,Any]:
        return await call(journal.get,str(case_id),section=section,after_seq=after_seq,limit=limit,revision=revision)

    @mcp.tool(name="disagreement_edit",title="Maintain a sourced research case",
        description="Manual research cases in the existing corpus. Typed create/revise/state/grant/add_evidence and operation_status. Exact source SHA, revision and literal quote are required for evidence. Case grants never grant a private book. Shared changes require expected_revision; every write needs a stable operation_key. This is not an import/scan checkpoint API.",
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def edit_tool(action:Literal["create","revise","state","grant","add_evidence","operation_status"],
        operation_key:str,case_id:UUID|None=None,
        command:CaseCreate|CaseRevise|CaseState|CaseGrant|EvidenceAdd|None=None,
        expected_revision:Annotated[int,Field(ge=1)]|None=None) -> dict[str,Any]:
        if action=="operation_status": return await call(journal.operation_status,operation_key)
        if action=="create" and type(command) is CaseCreate and case_id is None: return await call(journal.create,command,operation_key)
        if case_id is None: return invalid()
        case=str(case_id)
        if action=="add_evidence" and type(command) is EvidenceAdd: return await call(journal.add_evidence,case,command,operation_key)
        if expected_revision is None: return invalid()
        if (action=="revise" and type(command) is CaseRevise) or (action=="state" and type(command) is CaseState):
            return await call(journal.revise,case,expected_revision,command,operation_key)
        if action=="grant" and type(command) is CaseGrant: return await call(journal.grant,case,expected_revision,command,operation_key)
        return invalid()

    @mcp.tool(name="disagreement_contribute",title="Add or correct your research contribution",
        description="Independent own argument/opinion/question/reply, or a new correction/withdrawal version of your own contribution. Author and external-account provenance come from authenticated server context; no human reading or agreement is inferred. Corrections require contribution_id and expected_version. Stable operation_key gives exact replay.",
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def contribute_tool(case_id:UUID,operation_key:str,command:ContributionAdd|ContributionCorrect,
        contribution_id:UUID|None=None,expected_version:Annotated[int,Field(ge=1)]|None=None) -> dict[str,Any]:
        if type(command) is ContributionAdd and contribution_id is None and expected_version is None:
            return await call(journal.contribute,str(case_id),command,operation_key)
        if type(command) is ContributionCorrect and contribution_id and expected_version:
            return await call(journal.correct_contribution,str(case_id),str(contribution_id),expected_version,command,operation_key)
        return invalid()

    @mcp.tool(name="disagreement_conclusion",title="Record a versioned research conclusion",
        description="Record a shared model synthesis or separate individual opinion with exact evidence/contribution versions and material event boundary. Current synthesis requires curator and maintain_shared_summary. draft_list/get/save keep own private analysis versions (first expected_version=0, stable draft UUID/key); case history exposes none. draft_publish explicitly copies only the selected exact text span into a normal contribution, retaining the private draft. Requires current case/source ACL and case_contribute as well as personal_conclusion; extra private dependencies cannot be laundered by selecting a substring. Neither a draft nor publication implies human reading or support.",
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def conclusion_tool(case_id:UUID,expected_revision:Annotated[int,Field(ge=1)]|None=None,
        command:ConclusionAdd|PersonalDraft|DraftPublication|None=None,operation_key:str|None=None,
        action:Literal['record','draft_list','draft_get','draft_save','draft_publish']='record',
        draft_id:UUID|None=None,expected_version:Annotated[int,Field(ge=0)]|None=None,
        after_id:str='',limit:Annotated[int,Field(ge=1,le=100)]=20) -> dict[str,Any]:
        if action=='record' and type(command) is ConclusionAdd and expected_revision is not None and operation_key:
            return await call(journal.conclude,str(case_id),expected_revision,command,operation_key)
        if action=='draft_list':return await call(drafts.list,str(case_id),after_id=after_id,limit=limit)
        if draft_id is None:return invalid()
        if action=='draft_get':return await call(drafts.get,str(case_id),str(draft_id),version=expected_version)
        if expected_version is None or not operation_key:return invalid()
        if action=='draft_save' and type(command) is PersonalDraft:
            return await call(drafts.save,str(case_id),str(draft_id),expected_version,command,operation_key)
        if action=='draft_publish' and type(command) is DraftPublication:
            return await call(drafts.publish,str(case_id),str(draft_id),expected_version,command,operation_key)
        return invalid()
