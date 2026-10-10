"""Model-owned POI selection and creation in the Street Story authority.

No POI is minted or merged by lexical heuristics. The authenticated model uses
one bounded MCP action: shortlist -> explicit select/create -> same RKB entity ID.
Remote retries are safe: owner receipt is committed before the local link.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import httpx

from .graph_service import GraphService
from .poi_reference import canonical_poi_key


class PoiRegistryError(ValueError):
    pass


def _owner_service() -> tuple[str, str]:
    url = os.getenv("RKB_STREET_STORY_POI_URL", "").rstrip("/")
    if url != "http://127.0.0.1:8188":
        raise PoiRegistryError("owner_poi_service_not_configured")
    token_file = os.getenv("RKB_STREET_STORY_POI_TOKEN_FILE")
    if token_file:
        path = Path(token_file)
        if not path.is_file() or path.stat().st_mode & 0o077:
            raise PoiRegistryError("owner_poi_private_credential_unavailable")
        token=path.read_text(encoding="utf8").strip()
    else:
        token=os.getenv("RKB_STREET_STORY_POI_TOKEN", "").strip()
    if len(token)<32:
        raise PoiRegistryError("owner_poi_private_credential_unavailable")
    return url,token


class PoiRegistry:
    def __init__(self, backend, *, owner_url=None, owner_token=None, client=None):
        self.graph=GraphService(backend)
        self.owner_url=owner_url
        self.owner_token=owner_token
        self.client=client

    async def _owner(self,payload:dict)->dict:
        if self.owner_url is None or self.owner_token is None:
            url,token=_owner_service()
        else:
            url,token=self.owner_url,self.owner_token
        if self.client is not None:
            response=await self.client.post(url+"/v1/internal/rkb-poi",json=payload,
                headers={"Authorization":"Bearer "+token},timeout=6)
        else:
            async with httpx.AsyncClient(timeout=6,trust_env=False) as client:
                response=await client.post(url+"/v1/internal/rkb-poi",json=payload,
                    headers={"Authorization":"Bearer "+token})
        if response.status_code==409:
            detail=response.json().get("detail","owner_poi_identity_conflict")
            raise PoiRegistryError(str(detail)[:120])
        response.raise_for_status()
        result=response.json()
        if not isinstance(result,dict):
            raise PoiRegistryError("owner_poi_invalid_result")
        return result

    async def _owned_source(self,principal,entity_id:str):
        eid=UUID(str(entity_id))
        async with self.graph.connection(principal) as db:
            entity=await(await db.execute(
                "SELECT id,kind,canonical_label,external_ref,metadata "
                "FROM rkb_entities WHERE id=%s AND owner_user_id=rkb_current_actor_id() "
                "AND rkb_graph_active(document_id,revision)",(eid,))).fetchone()
            if not entity or entity["kind"]!="poi_ref":
                raise LookupError("owned active source location not found")
            rows=await(await db.execute(
                "SELECT chunk_id FROM rkb_entity_mentions WHERE entity_id=%s "
                "AND rkb_graph_active(document_id,revision) "
                "AND exact_source_spelling<>'' "
                "AND coalesce(signals->>'identity_unresolved','false') NOT IN ('true','1') "
                "ORDER BY id LIMIT 1",(eid,))).fetchall()
        if not rows:
            raise PoiRegistryError("location_has_no_sourced_mention")
        return entity,"knowledge://chunks/"+str(rows[0]["chunk_id"])

    async def _link(self,principal,entity_id:str,poi_ref:str):
        canonical_poi_key(poi_ref)
        async with self.graph.connection(principal) as db:
            row=await(await db.execute(
                "SELECT id,external_ref,metadata,state FROM rkb_entities WHERE id=%s "
                "AND kind='poi_ref' AND owner_user_id=rkb_current_actor_id() "
                "AND rkb_graph_active(document_id,revision) FOR UPDATE",
                (UUID(str(entity_id)),))).fetchone()
            if not row:raise LookupError("owned active location unavailable")
            if row["external_ref"] and row["external_ref"]!=poi_ref:
                raise PoiRegistryError("location_already_linked_to_different_poi")
            metadata={**(row["metadata"] or {}),"resolution":"model_selected",
                      "poi_link_method":"street_story_owner_receipt"}
            if not row["external_ref"]:
                await db.execute("UPDATE rkb_entities SET external_ref=%s,metadata=%s, "
                    "state=CASE WHEN state='unresolved' THEN 'candidate' ELSE state END "
                    "WHERE id=%s",(poi_ref,__import__("psycopg").types.json.Jsonb(metadata),row["id"]))
        return poi_ref

    async def act(self,principal,action:Literal["search","get","select","create"],
                  *,entity_id:str|None=None,query:str|None=None,
                  poi_id:str|None=None,site_state:str|None=None,
                  site_context:str|None=None,model_reason:str|None=None,
                  external_ids:dict[str,str]|None=None,aliases:list[str]|None=None,
                  limit:int=8,idempotency_key:str|None=None)->dict[str,Any]:
        if action in ("search","get"):
            body={"action":action,"actor_sub":principal.subject,"limit":max(1,min(limit,12))}
            if action=="search":
                body.update(query=query,external_ids=external_ids or {})
            else:body["poi_id"]=poi_id
            return await self._owner(body)
        if not entity_id:
            raise ValueError("source location entity_id required")
        node,source_ref=await self._owned_source(principal,entity_id)
        # A timed-out first create may already have linked this RKB entity.
        # Replaying its exact owner idempotency key must return the same receipt,
        # while a different owner decision still conflicts on the owner side.
        if not model_reason or len(model_reason.strip())<12:
            raise ValueError("model reason required for POI identity decision")
        if action=="create" and (not site_state or not site_context):
            raise ValueError("site state and physical context required")
        if action=="select" and not poi_id:
            raise ValueError("existing POI ID required")
        if node["external_ref"] and poi_id and node["external_ref"]!="streetstory://poi/"+poi_id:
            raise PoiRegistryError("already_linked_to_different_poi")
        request_id=idempotency_key or ("rkb-poi:"+entity_id+":"+action+
                                       (":"+poi_id if poi_id else ""))
        if len(request_id)>128:
            raise ValueError("idempotency key too long")
        command={"action":action,"actor_sub":principal.subject,
                 "entity_id":entity_id,"idempotency_key":request_id,
                 "source_ref":source_ref,"model_reason":model_reason}
        if action=="select":
            command["poi_id"]=poi_id
        else:
            command.update(canonical_name=node["canonical_label"],
                           site_state=site_state,site_context=site_context,
                           external_ids=external_ids or {},aliases=aliases or [])
        receipt=await self._owner(command)
        selected=receipt.get("poi",{}).get("poi_ref")
        if not selected or not isinstance(selected,str):
            raise PoiRegistryError("owner_poi_did_not_confirm_identity")
        await self._link(principal,entity_id,selected)
        return {"state":"linked","rkb_entity_id":str(entity_id),
                "poi_ref":selected,"poi_state":receipt["poi"].get("status"),
                "replayed_owner_receipt":bool(receipt.get("replayed")),
                "owner":"street_story","source_ref":source_ref}

    async def context(self,principal,poi_ref:str,limit:int=5)->dict[str,Any]:
        """One-hop authorized factual/navigation projection. No unreviewed facts."""
        from .story_registry import StoryRegistry
        key=canonical_poi_key(poi_ref)
        take=max(1,min(limit,10))
        async with self.graph.connection(principal) as db:
            rows=await(await db.execute(
                "SELECT id FROM rkb_entities WHERE external_ref=%s "
                "AND rkb_graph_active(document_id,revision) ORDER BY id LIMIT %s",
                ("streetstory://poi/"+key,take+1))).fetchall()
        registry=StoryRegistry(self.graph.backend.corpus)
        result=[]
        for row in rows[:take]:
            eid=str(row["id"])
            graph=await self.graph.read(principal,eid,20)
            stories=registry.search(principal,filters={"entity_ref":eid},limit=take)
            metadata=graph["entity"].get("metadata") or {}
            # Only source-side model-selected cross-project references.
            # Do not advertise map geometry or physical POI identity as
            # accepted: Cartography and Street Story issue those verdicts.
            raw_refs=metadata.get("map_refs") or []
            map_refs=[ref for ref in raw_refs if isinstance(ref,str)
                      and ref.startswith("cartography://")][:20]
            result.append({
                "entity_id":eid,
                "label":graph["entity"]["canonical_label"],
                "aliases":[a["value"] for a in graph["aliases"]][:12],
                "place_kind":metadata.get("place_kind"),
                "place_context":metadata.get("place_context"),
                "map_refs":map_refs,
                "map_ref_verification":"not_verified",
                "historical_geometry":"not_verified",
                "evidence_refs":[m["evidence"] for m in graph["mentions"][:take]],
                "relations":[{"kind":e["kind"],"neighbor_kind":e["neighbor_kind"],
                              "neighbor_label":e["neighbor_label"],
                              "neighbor_id":str(e["neighbor_id"]),
                              "neighbor_poi_ref":e.get("external_ref"),
                              "source_relation_state":e.get("state"),
                              "source_time_scope":e.get("time_scope")}
                             for e in graph["neighbors"][:take]],
                "stories":[{"story_id":s["story_id"],"title":s["title"],
                            "state":s["state"]} for s in stories.get("results",[])[:take]],
            })
        return {"poi_ref":"streetstory://poi/"+key,"locations":result,
                "has_more":len(rows)>take,"source_scope":"actor_authorized",
                "complete_extraction_claimed":False,
                "cartography_acceptance_claimed":False}
