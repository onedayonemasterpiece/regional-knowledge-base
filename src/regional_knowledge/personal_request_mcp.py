"""Personal managed requests on the existing authenticated RKB MCP resource."""
import asyncio
from typing import Annotated,Any,Literal
from uuid import UUID

from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.types import ToolAnnotations
from pydantic import Field

from .expert_requests import ExpertRequestError
from .participants import ParticipantError


def register_personal_requests(mcp,requests,authority,resource_url):
    @mcp.tool(name='research_request',title='Submit or collect a saved research request',
        description='submit/result/ack in your authenticated client and dialog. Reuse client_request_key only for the identical submission. Poll result without re-execution; acknowledge its exact result_hash after collecting it. The next turn in a dialog waits for this ACK. Ordinary search remains independent. No actor field or external message send.',
        annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def research_request(action:Literal['submit','result','ack'],
        dialog_key:Annotated[str,Field(min_length=8,max_length=128)]|None=None,
        client_request_key:Annotated[str,Field(min_length=8,max_length=128)]|None=None,
        text:Annotated[str,Field(min_length=1,max_length=12000)]|None=None,
        request_id:UUID|None=None,result_hash:Annotated[str,Field(pattern=r'^[a-f0-9]{64}$')]|None=None)->dict[str,Any]:
        token=get_access_token()
        grant=(token.claims or {}).get('rkb_request_grant') if token else None
        if not grant or token.resource!=resource_url:return {'error':{'code':'managed_authorization_required'}}
        try:
            context=authority()
            if action=='submit' and dialog_key and client_request_key and text is not None:
                return await asyncio.to_thread(requests.personal_submit,context,grant,resource_url,
                    dialog_key=dialog_key,client_request_key=client_request_key,text=text)
            if request_id is not None and (action=='result' or action=='ack' and result_hash):
                return await asyncio.to_thread(requests.personal_result,context,grant,resource_url,str(request_id),
                    ack_hash=result_hash if action=='ack' else None)
            return {'error':{'code':'invalid_arguments'}}
        except (ExpertRequestError,ParticipantError) as error:
            return {'error':{'code':error.code}}
