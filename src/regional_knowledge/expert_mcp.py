"""Saved-request operations registered on the existing RKB MCP resource.

Authentication and tool policy are shared with personal OAuth on the same
endpoint. The service selects an executor; only the inbox selects the human.
"""
import asyncio
import logging
from typing import Annotated,Any,Literal
from uuid import UUID

from mcp.types import ToolAnnotations
from pydantic import Field

from .disagreement_contracts import ConclusionAdd,ContributionAdd,ContributionCorrect
from .disagreement_journal import JournalError
from .expert_executor import ExpertExecutor
from .expert_requests import ExpertRequestError,ExpertResult
from .participants import ParticipantError

from .mcp_authority import current_service

log=logging.getLogger(__name__)


def register_executor_tools(mcp,requests,backend):
    executor=ExpertExecutor(requests,backend)

    async def call(fn,*args,**kwargs):
        try:
            result=await fn(*args,**kwargs)
            return result
        except (ExpertRequestError,ParticipantError,JournalError) as error:
            log.info("expert_mcp_rejected",extra={"operation":fn.__name__,"reason_code":error.code})
            return {"error":{"code":error.code}}
        except (LookupError,PermissionError):
            return {"error":{"code":"source_not_accessible"}}

    async def sync(fn,*args,**kwargs):
        return await call(asyncio.to_thread,fn,*args,**kwargs)

    def invalid():return {"error":{"code":"invalid_arguments"}}

    @mcp.tool(name="expert_request_delivery",title="Read and complete a saved expert request",
        description="Claim/get/status/record_result for the exact request. Service identity selects the executor; the stored verified binding selects the human. Use exact generation and execution_token from claim. The same record_result is replayable after a lost response. This tool never sends Slack or grants import/owner rights.",
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def request_tool(action:Literal["claim","get","status","record_result"],request_id:UUID,
        generation:Annotated[int,Field(ge=1)]|None=None,
        execution_token:Annotated[str,Field(min_length=16,max_length=256)]|None=None,
        event_id:UUID|None=None,
        result:ExpertResult|None=None) -> dict[str,Any]:
        credentials=current_service()
        if action=="claim":return await sync(requests.claim,credentials,request_id=str(request_id),lease_seconds=300,event_id=str(event_id) if event_id else None)
        if generation is None or execution_token is None:return invalid()
        if action=="record_result" and result is not None:
            return await sync(requests.record_result,credentials,str(request_id),generation,execution_token,result)
        if action in {"get","status"}:
            async def read():
                context=await asyncio.to_thread(requests.context,credentials,str(request_id),generation,execution_token,allow_result_replay=action=="status")
                return await asyncio.to_thread(requests.get,context,str(request_id))
            return await call(read)
        return invalid()

    @mcp.tool(name="expert_research",title="Research within one expert request",
        description="Bound source_search/source_fetch/case_get/contribute/conclude. No actor or owner field. Source reads use current source ACL and the saved personal/public route; server-known dependencies survive omitted result references. Group case reads require a future approved public projection and currently return a clear denial. No import, access grants, admin, arbitrary SQL or provider send.",
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def research_tool(operation:Literal["source_search","source_fetch","case_get","contribute","conclude"],
        request_id:UUID,generation:Annotated[int,Field(ge=1)],
        execution_token:Annotated[str,Field(min_length=16,max_length=256)],
        query:Annotated[str,Field(max_length=3000)]="",chunk_id:UUID|None=None,
        case_id:UUID|None=None,main_job_id:str|None=None,
        section:Literal["case","evidence","contributions","conclusions","history"]="case",
        after_seq:Annotated[int,Field(ge=0)]=0,limit:Annotated[int,Field(ge=1,le=20)]=8,
        command:ContributionAdd|ContributionCorrect|ConclusionAdd|None=None,
        operation_key:Annotated[str,Field(min_length=8,max_length=128)]|None=None,
        contribution_id:UUID|None=None,expected_version:Annotated[int,Field(ge=1)]|None=None,
        expected_revision:Annotated[int,Field(ge=1)]|None=None) -> dict[str,Any]:
        credentials=current_service()
        base=(credentials,str(request_id),generation,execution_token)
        if operation=="source_search" and query.strip():
            return await call(executor.search,*base,query.strip(),limit=limit,main_job_id=main_job_id)
        if operation=="source_fetch" and chunk_id:
            return await call(executor.fetch,*base,str(chunk_id))
        if not case_id:return invalid()
        if operation=="case_get":
            return await call(executor.case_get,*base,str(case_id),section=section,after_seq=after_seq,limit=limit)
        if operation=="contribute" and isinstance(command,(ContributionAdd,ContributionCorrect)) and operation_key:
            return await call(executor.contribute,*base,str(case_id),operation_key,command,contribution_id=contribution_id,expected_version=expected_version)
        if operation=="conclude" and isinstance(command,ConclusionAdd) and operation_key and expected_revision:
            return await call(executor.conclude,*base,str(case_id),expected_revision,operation_key,command)
        return invalid()

