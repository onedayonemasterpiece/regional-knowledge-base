"""Owner-only Participants MCP via existing synthetic OAuth and SQLite policy.

No provider, native PG, active grants, or runtime state are accessed.
"""
from types import SimpleNamespace
from uuid import UUID

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from starlette.requests import Request

from regional_knowledge.contracts import Principal
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.server import build_server
from test_kaggle_native_source_importer import native_intake
from test_kaggle_public_backend import target  # fixture dependency of native_intake


def _context():
    request = Request({'type':'http','method':'POST','path':'/mcp','headers':[],
        'app':SimpleNamespace(state=SimpleNamespace()),'state':{}})
    return SimpleNamespace(request_context=SimpleNamespace(
        request=request,request_id='synthetic-owner-participants'))


@pytest.mark.asyncio
async def test_owner_mcp_uses_existing_participants_get_list_audit_change_and_idempotency(
        native_intake, monkeypatch):
    s = native_intake
    current = {'principal':s.principal}
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:current['principal'])
    server = build_server(target_startup=s.startup)
    tools = {tool.name:tool for tool in await server.list_tools()}
    assert 'participants_manage' in tools
    assert not tools['participants_manage'].annotations.read_only_hint
    assert 'scan_proof_read' in tools['participants_manage'].description
    person = s.t.auth.p.other.subject
    registry = s.t.auth.p.registry

    async def call(**args):
        return await server.call_tool('participants_manage',args,_context())

    owner_get = await call(action='get',person_id=person)
    assert person in str(owner_get)
    owner_list = await call(action='list',limit=5)
    assert person in str(owner_list)
    before = registry.get(s.t.auth.p.admin,person)
    assert 'scan_proof_read' in {g['capability'] for g in before['grants']}

    key='fixture-scan-proof-revoke-20261023'
    result = await call(action='change',person_id=person,expected_revision=before['revision'],
        operation_key=key,command=dict(capability='scan_proof_read',granted=False,
                                       reason='synthetic explicit owner revoke'))
    assert 'applied' in str(result)
    after = registry.get(s.t.auth.p.admin,person)
    assert after['revision']==before['revision']+1
    assert 'scan_proof_read' not in {g['capability'] for g in after['grants']}
    assert 'scan_proof_read' not in {g['capability'] for g in
        registry.get(s.t.auth.p.admin,person)['grants']}
    assert 'applied' in str(await call(action='operation_status',operation_key=key))
    assert 'ParticipantGrant' in str(await call(action='audit',person_id=person))

    # Exact same request, same key, same receipt: no duplicate mutation.
    await call(action='change',person_id=person,expected_revision=before['revision'],
        operation_key=key,command=dict(capability='scan_proof_read',granted=False,
                                       reason='synthetic explicit owner revoke'))
    assert registry.get(s.t.auth.p.admin,person)['revision']==after['revision']

    # Categories are profile metadata, never a new scan grant or second IAM.
    await call(action='change',person_id=person,expected_revision=after['revision'],
        operation_key='fixture-category-upsert-20261023',
        command=dict(name='Synthetic expert',categories=['expert','local_historian'],
            reason='synthetic category edit'))
    profile = registry.get(s.t.auth.p.admin,person)
    assert set(profile['profile']['categories'])=={'expert','local_historian'}
    assert 'scan_proof_read' not in {g['capability'] for g in profile['grants']}
    # An unrelated capability cannot be assigned via this narrowly scoped MCP.
    with pytest.raises(ToolError,match='participants_change_out_of_scope'):
        await call(action='change',person_id=person,expected_revision=profile['revision'],
            operation_key='fixture-unrelated-grant-20261023',
            command=dict(capability='case_read',granted=True,reason='must be rejected'))
    assert registry.get(s.t.auth.p.admin,person)['revision']==profile['revision']


@pytest.mark.asyncio
async def test_nonowner_cannot_read_list_audit_or_change_and_cannot_forge_owner(
        native_intake, monkeypatch):
    s=native_intake
    current={'principal':s.t.principal}
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:current['principal'])
    server=build_server(target_startup=s.startup)
    person=s.t.auth.p.other.subject
    before=s.t.auth.p.registry.get(s.t.auth.p.admin,person)
    args=(
        {'action':'get','person_id':person},
        {'action':'list','limit':3},
        {'action':'audit','person_id':person},
        {'action':'operation_status','operation_key':'fixture-scan-proof-revoke-20261023'},
        {'action':'change','person_id':person,'expected_revision':before['revision'],
         'operation_key':'fixture-forged-revoke-20261023',
         'command':{'capability':'scan_proof_read','granted':False,'reason':'forged'}},
    )
    for kwargs in args:
        with pytest.raises(ToolError,match='participants_owner_required'):
            await server.call_tool('participants_manage',kwargs,_context())
    # A forged owner subject with somebody else's valid bearer is still denied.
    current['principal']=Principal(subject=s.t.auth.p.owner.subject,
        client_id=s.t.principal.client_id,issuer=s.t.principal.issuer,
        access_token=s.t.principal.access_token)
    with pytest.raises(ToolError,match='participants_owner_required'):
        await server.call_tool('participants_manage',{'action':'list'},_context())
    after=s.t.auth.p.registry.get(s.t.auth.p.admin,person)
    assert before==after


@pytest.mark.asyncio
async def test_legacy_server_never_advertises_new_participant_admin(monkeypatch):
    monkeypatch.setenv('RKB_DEV_NOAUTH','1')
    server=build_server(backend_profile='legacy')
    tools={tool.name for tool in await server.list_tools()}
    assert 'participants_manage' not in tools
    with pytest.raises(ToolError,match='participants_manage|not found|Unknown'):
        await server.call_tool('participants_manage',{'action':'list'},_context())
