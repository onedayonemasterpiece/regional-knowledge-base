"""Existing compact research tools bound to the native Case adapter only."""
from typing import Annotated,Any,Literal
from uuid import UUID
from pydantic import Field
from mcp.types import ToolAnnotations
from .disagreement_contracts import (CaseCreate,CaseRevise,CaseState,CaseGrant,EvidenceAdd,
    ContributionAdd,ContributionCorrect,ConclusionAdd,PersonalDraft,DraftPublication)


def register_case_tools(mcp,backend,principal):
    @mcp.tool(name='disagreement_search',title='Find existing research cases by subject',
        description='Find actor-authorized existing research Cases by question, place or source; '
            'bounded lexical candidates only, not an automatic finding of contradiction. '
            'Empty query lists all available Cases by case_id, paged using after_case_id/next_after_case. '
            'Inspect candidate Case with disagreement_get before adding evidence or creating a new case. '
            'One page of results is not exhaustive; use next_after_case while has_more. '
            'No matching result or pending read is not proof of absence. '
            'When an accepted page is visible via ordinary source search, compare assertions yourself, '
            'then disagreement_edit(add_evidence) on a matching Case or create and add_evidence. '
            'Preserve original operation_key after unknown result; pending is not Saved.',
        annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def search_tool(query:Annotated[str,Field(max_length=120)]='',
        place:Annotated[str|None,Field(max_length=120)]=None,
        source_id:UUID|None=None,after_case_id:UUID|None=None,
        limit:Annotated[int,Field(ge=1,le=8)]=5)->dict[str,Any]:
        return await backend.disagreement_search(query,principal(),place=place,
            source_id=str(source_id) if source_id is not None else None,
            after_case_id=str(after_case_id) if after_case_id is not None else None,limit=limit)

    @mcp.tool(name='disagreement_get',title='Read a research case',
        description='Read a Case or complete paginated evidence, contributions, conclusions and history. '
            'after_seq is per Case; follow next_after_seq while has_more. Current Case/source access applies to history too. '
            'Native pages contain at most20 whole records; request a smaller limit if the bounded native page rejects larger limits. '
            'An accepted text match is not visual or human confirmation.',
        annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def get_tool(case_id:UUID,section:Literal['case','evidence','contributions','conclusions','history']='case',
        after_seq:Annotated[int,Field(ge=0)]=0,limit:Annotated[int,Field(ge=1,le=100)]=20,
        revision:Annotated[int,Field(ge=1)]|None=None)->dict[str,Any]:
        return await backend.disagreement_get(str(case_id),principal(),section=section,after_seq=after_seq,limit=limit,revision=revision)

    @mcp.tool(name='disagreement_edit',title='Maintain a sourced research case',
        description='Typed create/revise/state/grant/add_evidence and operation_status on the native Case journal. '
            'Evidence requires exact retained original SHA, source revision, physical region/page IDs and literal quote. '
            'Shared changes require expected_revision. Reuse operation_key after a lost response; pending is not Saved. '
            'After a lost CREATE response, operation_status can recover the original case_id from the same actor-bound operation_key even after raw control retention, with native PG and durable receipt checks. For non-create operations retain case_id. '
            'Case grants do not grant a source original or private analysis.',
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def edit_tool(action:Literal['create','revise','state','grant','add_evidence','operation_status'],
        operation_key:str,case_id:UUID|None=None,command:CaseCreate|CaseRevise|CaseState|CaseGrant|EvidenceAdd|None=None,
        expected_revision:Annotated[int,Field(ge=1)]|None=None)->dict[str,Any]:
        return await backend.disagreement_edit(action,operation_key,principal(),case_id=str(case_id) if case_id else None,
            command=command,expected_revision=expected_revision)

    @mcp.tool(name='disagreement_contribute',title='Add or correct your research contribution',
        description='Add your argument/opinion/question/reply, or a new correction/withdrawal version of your own contribution. '
            'Author and verified external-account provenance come from authenticated context, never arguments. '
            'No human reading, agreement or support is inferred. Corrections require contribution_id and expected_version; '
            'reuse the same operation_key for recovery.',
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def contribute_tool(case_id:UUID,operation_key:str,command:ContributionAdd|ContributionCorrect,
        contribution_id:UUID|None=None,expected_version:Annotated[int,Field(ge=1)]|None=None)->dict[str,Any]:
        return await backend.disagreement_contribute(str(case_id),operation_key,command,principal(),
            contribution_id=str(contribution_id) if contribution_id else None,expected_version=expected_version)

    @mcp.tool(name='disagreement_conclusion',title='Record a versioned research conclusion',
        description='Record a shared model synthesis or individual opinion with exact evidence/contribution versions and material boundary. '
            'Current synthesis requires curator and maintain_shared_summary. Own private draft_list/get/save stays author-only; '
            'case history exposes none. draft_publish copies only the selected exact text span into a contribution, retaining the draft. '
            'Case managers do not inherit private drafts; publication implies no human reading or support. '
            'Follow next_after_id for bounded whole-record draft pages (at most20).',
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def conclusion_tool(case_id:UUID,expected_revision:Annotated[int,Field(ge=1)]|None=None,
        command:ConclusionAdd|PersonalDraft|DraftPublication|None=None,operation_key:str|None=None,
        action:Literal['record','draft_list','draft_get','draft_save','draft_publish']='record',
        draft_id:UUID|None=None,expected_version:Annotated[int,Field(ge=0)]|None=None,
        after_id:str='',limit:Annotated[int,Field(ge=1,le=100)]=20)->dict[str,Any]:
        return await backend.disagreement_conclusion(str(case_id),'conclude' if action=='record' else action,principal(),
            operation_key=operation_key,command=command,expected_revision=expected_revision,
            draft_id=str(draft_id) if draft_id else None,expected_version=expected_version,
            version=expected_version if action=='draft_get' else None,after_id=after_id or None,limit=limit)
