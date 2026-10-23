"""Register existing graph tools once, preserving independent GEO/source gates."""
from typing import Annotated,Literal

from pydantic import Field
from mcp.types import ToolAnnotations


def register_source_graph_tools(mcp,backend,geo_backend,principal):
    @mcp.tool(name='entity_list',title='Read accepted source entities',
        description='Bounded person, organization, event, historical thread and source-place identities in one exact accepted source. '
            'Explicit source claims and quotes; source entities do not require a Story. Source graph search and discovery remain unavailable.',
        annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def entity_list(document_ids:list[str]|None=None,kinds:list[str]|None=None,query:str='',
            limit:Annotated[int,Field(ge=1,le=3)]=3,cursor:str|None=None)->dict:
        return await backend.entity_list(principal(),document_ids=document_ids,kinds=kinds,query=query,limit=limit,cursor=cursor)

    @mcp.tool(name='graph_fetch',title='Read an exact source entity or retained geographic identity',
        description='One stable UUID. Accepted-source entities return model-authored identities, exact source mentions, aliases '
            'and directional relations. Retained GEO identities keep private owner scope and minimal legacy-unanchored provenance. '
            'Canonical geometry stays with its owner; graph discovery is unavailable.',
        annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def graph_fetch(entity_id:str,limit:Annotated[int,Field(ge=1,le=3)]=3,cursor:str|None=None,
            category:Literal['mentions','aliases','relations','alias','relation','owner_reference','provenance','source_geo_observation']|None=None,
            discovery_job_id:str|None=None)->dict:
        # This is only fixed routing by an immutable, already-verified ID index.
        # Each branch independently resolves/rechecks canonical authority and
        # current proof; a branch failure is never retried under another scope.
        if geo_backend is not None and geo_backend.geo_gate.knows_entity(entity_id):
            mapped={'aliases':'alias','relations':'relation','mentions':'provenance'}.get(category,category)
            return await geo_backend.graph_fetch(entity_id,principal(),limit=limit,cursor=cursor,category=mapped,
                discovery_job_id=discovery_job_id)
        return await backend.source_graph_fetch(entity_id,principal(),limit=limit,cursor=cursor,category=category,
            discovery_job_id=discovery_job_id)
