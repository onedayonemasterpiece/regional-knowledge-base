from __future__ import annotations

import json
import os
from typing import Any, Literal
from urllib.parse import urlsplit

from mcp.server.mcpserver import Image, MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.settings import (
    AuthSettings,
    ClientRegistrationOptions,
    RevocationOptions,
)
from mcp.types import ImageContent, TextContent, ToolAnnotations
from pydantic import AnyHttpUrl

from starlette.requests import Request
from starlette.responses import JSONResponse
from .entity_graph import GraphBundle,GraphAlias
from .graph_service import GraphService
from .local_e5 import LocalE5Embedder
from .auth import JwtResourceVerifier
from .backend import KnowledgeBackend
from .contracts import (
    BookIngestOutput,
    ChatFile,
    DocumentAccessOutput,
    EvidenceSearchOutput,
    FetchOutput,
    Principal,
    ProfileOutput,
    SearchOutput,
    StageChunkInput,
    StagePageInput,
    StagePoiFactInput,
    StagePoiMediaLinkInput,
    StartMetadataInput,
    Visibility,
)
from .oauth_provider import (
    KNOWLEDGE_SCOPE,
    RegionalOAuthProvider,
    oauth_provider_from_env,
)
from .supabase_backend import backend_from_env


def _principal() -> Principal:
    token = get_access_token()
    if token is None or token.subject is None:
        raise RuntimeError("authenticated user context is required")
    issuer = str((token.claims or {}).get("iss", ""))
    return Principal(
        subject=token.subject,
        client_id=token.client_id,
        issuer=issuer,
        access_token=token.token,
    )


def _transport_security(resource_url: str | None = None) -> TransportSecuritySettings:
    resource = (
        resource_url
        if resource_url is not None
        else os.getenv("RKB_RESOURCE_URL", "").strip()
    )
    allowed_hosts = [
        "127.0.0.1",
        "127.0.0.1:*",
        "localhost",
        "localhost:*",
        "[::1]",
        "[::1]:*",
    ]
    allowed_origins = [
        "http://127.0.0.1",
        "http://127.0.0.1:*",
        "http://localhost",
        "http://localhost:*",
        "http://[::1]",
        "http://[::1]:*",
    ]
    if resource:
        parsed = urlsplit(resource)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise RuntimeError("RKB_RESOURCE_URL must be an absolute HTTP(S) URL")
        allowed_hosts.append(parsed.netloc)
        allowed_origins.append(f"{parsed.scheme}://{parsed.netloc}")

    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
    )


def build_server(
    backend: KnowledgeBackend | None = None,
    *,
    issuer: str | None = None,
    resource_url: str | None = None,
    jwks_url: str | None = None,
    oauth_provider: RegionalOAuthProvider | None = None,
    profile: Literal["full", "live"] = "full",
) -> MCPServer:
    backend = backend or backend_from_env()
    issuer = (
        issuer
        or os.getenv("RKB_AUTH_ISSUER", "").strip()
    ).rstrip("/")
    resource_url = (
        resource_url
        or os.getenv("RKB_RESOURCE_URL", "").strip()
    ).rstrip("/")
    jwks_url = (
        jwks_url
        or os.getenv("RKB_AUTH_JWKS_URL", "").strip()
        or (f"{issuer}/.well-known/jwks.json" if issuer else "")
    )

    auth_mode = os.getenv("RKB_AUTH_MODE", "external").strip().lower()
    if oauth_provider is not None:
        auth_mode = "embedded"
    if auth_mode not in {"embedded", "external"}:
        raise RuntimeError("RKB_AUTH_MODE must be 'embedded' or 'external'")

    kwargs: dict[str, Any] = {}
    embedded_provider: RegionalOAuthProvider | None = None
    if auth_mode == "embedded":
        if not issuer or not resource_url:
            raise RuntimeError(
                "Embedded OAuth requires RKB_AUTH_ISSUER and RKB_RESOURCE_URL"
            )
        embedded_provider = oauth_provider or oauth_provider_from_env(
            issuer=issuer,
            resource=resource_url,
        )
        kwargs["auth_server_provider"] = embedded_provider
        kwargs["auth"] = AuthSettings(
            issuer_url=AnyHttpUrl(issuer),
            resource_server_url=AnyHttpUrl(resource_url),
            required_scopes=[KNOWLEDGE_SCOPE],
            client_registration_options=ClientRegistrationOptions(
                enabled=True,
                valid_scopes=[KNOWLEDGE_SCOPE],
                default_scopes=[KNOWLEDGE_SCOPE],
                client_secret_expiry_seconds=None,
            ),
            revocation_options=RevocationOptions(enabled=True),
            validate_token_resource=True,
        )
    elif issuer and resource_url and jwks_url:
        kwargs["token_verifier"] = JwtResourceVerifier(
            issuer=issuer,
            jwks_url=jwks_url,
            resource=resource_url,
        )
        kwargs["auth"] = AuthSettings(
            issuer_url=AnyHttpUrl(issuer),
            resource_server_url=AnyHttpUrl(resource_url),
            required_scopes=[],
            validate_token_resource=True,
        )
    elif os.getenv("RKB_DEV_NOAUTH") != "1":
        raise RuntimeError(
            "Application OAuth/JWT configuration is required; Supabase is only "
            "the data plane. Configure embedded/external app OAuth and "
            "RKB_RESOURCE_URL, or RKB_DEV_NOAUTH=1 for local tests only."
        )

    mcp = MCPServer(
        "Regional Knowledge Base",
        instructions=(
            "Search and retrieve sourced regional knowledge. Use search before fetch. "
            "Ingestion is explicit and resumable; never infer that a source is public."
        ),
        **kwargs,
    )

    if embedded_provider is not None:
        embedded_provider.register_routes(mcp)

    if os.getenv('RKB_BGE_QUEUE_PATH'):
        from .bge_queue import BgeQueue
        from .bge_broker import register_routes
        register_routes(mcp,BgeQueue(os.environ['RKB_BGE_QUEUE_PATH']))

    @mcp.custom_route("/fast-tier/health", methods=["GET"])
    async def fast_tier_health(request: Request):
        encoder = getattr(backend, "embedder", None)
        if isinstance(encoder, LocalE5Embedder):
            status = await encoder.status()
            return JSONResponse({k:status[k] for k in ("configured","ready","retrieval_mode")},headers={"Cache-Control":"no-store"})
        return JSONResponse({"configured":False,"ready":False,"retrieval_mode":"lexical_only"},headers={"Cache-Control":"no-store"})

    if profile == "live":
        @mcp.tool(
            name="knowledge_search",
            title="Search regional knowledge",
            description=(
                "Return a small ready-to-use evidence pack from accessible regional "
                "books and journals in one low-latency call. Use for factual Live answers."
            ),
            annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
        )
        async def knowledge_search(
            query: str,
            max_evidence: int = 3,
            main_job_id: str | None = None,
        ) -> EvidenceSearchOutput:
            return await backend.search_evidence(
                query.strip(),
                _principal(),
                max_evidence=max(1, min(max_evidence, 5)),
                **({'main_job_id': main_job_id} if main_job_id else {}),
            )

        return mcp

    @mcp.tool(
        title="Search regional knowledge",
        description=(
            "Search the user's accessible regional books and journals. "
            "Returns citable result IDs and stable URLs. Use fetch for full evidence."
        ),
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def search(query: str, main_job_id: str | None = None, aliases: list[dict[str,str]] | None = None) -> SearchOutput:
        options={}
        if main_job_id:options['main_job_id']=main_job_id
        if aliases:options['aliases']=aliases
        return await backend.search(query.strip(), _principal(),**options)

    @mcp.tool(
        title="Fetch regional evidence",
        description=(
            "Fetch the full evidence for an ID returned by search, including page "
            "provenance, footnotes and illustration descriptors when available."
        ),
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def fetch(id: str) -> FetchOutput:
        return await backend.fetch(id, _principal())

    @mcp.tool(title='Read an authorized illustration', description='Return the canonical source crop and labelled caption/model-observation metadata. Model observation is not printed evidence. No OCR or inference.', annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
    async def illustration_fetch(id: str) -> list[TextContent | ImageContent]:
        import base64
        from .illustrations import fetch_crop
        metadata, data, mime = await fetch_crop(backend, _principal(), id)
        return [TextContent(type='text', text=json.dumps(metadata, ensure_ascii=False)),
                ImageContent(type='image', data=base64.b64encode(data).decode(), mimeType=mime)]

    @mcp.tool(
        title="Add or continue a book",
        description=(
            "Start or continue a resumable book/journal ingestion. Use start with an "
            "attached PDF, then book_pages plus stage/validate/finalize/status as needed. "
            "Finalization is the only step that makes an indexed revision active. "
            "For large books finalize starts or resumes server-side work and returns "
            "processing promptly; do not poll in a tight loop or keep the same ChatGPT "
            "turn open waiting. Check status in a later turn."
        ),
        annotations=ToolAnnotations(
            read_only_hint=False,
            destructive_hint=False,
            idempotent_hint=True,
            open_world_hint=False,
        ),
        meta={"openai/fileParams": ["file"]},
    )
    async def book_ingest(
        command: Literal["start", "stage", "validate", "finalize", "status"],
        file: ChatFile | None = None,
        ingestion_id: str | None = None,
        cursor: str | None = None,
        metadata: StartMetadataInput | None = None,
        pages: list[StagePageInput] | None = None,
        chunks: list[StageChunkInput] | None = None,
        poi_facts: list[StagePoiFactInput] | None = None,
        poi_media_links: list[StagePoiMediaLinkInput] | None = None,
        entity_candidates: GraphBundle | None = None,
    ) -> BookIngestOutput:
        payload: dict[str, Any] | None = None
        if metadata is not None:
            payload = metadata.model_dump(mode="json", exclude_none=True)
        if (
            pages is not None
            or chunks is not None
            or poi_facts is not None
            or poi_media_links is not None
            or entity_candidates is not None
        ):
            payload = {
                "entity_candidates": entity_candidates.model_dump(mode="json") if entity_candidates else None,
                "pages": [
                    page.model_dump(mode="json", exclude_none=True)
                    for page in (pages or [])
                ],
                "chunks": [
                    chunk.model_dump(mode="json", exclude_none=True)
                    for chunk in (chunks or [])
                ],
                "poi_facts": [
                    fact.model_dump(mode="json", exclude_none=True)
                    for fact in (poi_facts or [])
                ],
                "poi_media_links": [
                    link.model_dump(mode="json", exclude_none=True)
                    for link in (poi_media_links or [])
                ],
            }
        return await backend.book_ingest(
            command=command,
            principal=_principal(),
            file=file,
            ingestion_id=ingestion_id,
            cursor=cursor,
            payload=payload,
        )

    @mcp.tool(title="Stage evidence-backed entity graph", description="Submit bounded model-authored graph candidates on an owned active document, or add a sourced alias to one owned entity. No automatic identity merge. Exact source chunk/page/region evidence required.", annotations=ToolAnnotations(read_only_hint=False,open_world_hint=False))
    async def graph_stage(document_id:str|None=None,revision:int|None=None,candidates:GraphBundle|None=None,entity_id:str|None=None,alias:GraphAlias|None=None,poi_discovery_ref:str|None=None)->dict[str,Any]:
        service=GraphService(backend);principal=_principal()
        if poi_discovery_ref is not None:return await service.discover_poi(principal,poi_discovery_ref)
        if alias is not None and entity_id is not None:
            return await service.add_alias(principal,entity_id,alias)
        if candidates is None or document_id is None or revision is None:raise ValueError("document/revision/candidates or entity/alias required")
        async with service.connection(principal) as db:
            row=await(await db.execute("select active_revision from rkb_documents where id=%s",(__import__('uuid').UUID(document_id),))).fetchone()
            if not row or row['active_revision']!=revision:raise ValueError("active document revision required")
        return await service.stage(principal,document_id,revision,candidates)

    @mcp.tool(title="Read one evidence-backed graph entity",description="One entity, authorized aliases/mentions and at most 20 one-hop relations with exact source evidence. No recursive traversal or graph dump.",annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def graph_fetch(entity_id:str|None=None,limit:int=20,discovery_job_id:str|None=None)->dict[str,Any]:
        if discovery_job_id is not None:return await GraphService(backend).job_read(_principal(),discovery_job_id,limit)
        if entity_id is None:raise ValueError("entity or discovery job required")
        return await GraphService(backend).read(_principal(),entity_id,limit)

    @mcp.tool(title="Find related entity evidence",description="One authorized entity context and bounded related evidence through the existing E5/BGE/lexical retrieval. Hits are identity candidates, not facts.",annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def graph_related(entity_id:str,query:str|None=None,limit:int=8)->dict[str,Any]:
        return await GraphService(backend).related(_principal(),entity_id,query,limit)

    @mcp.tool(title="Active source indexing status",description="Authorized active chunk/vector counts and automatic indexing/readiness state. Optional document scope; no private titles, text or other actors' inventory.",annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def indexing_status(document_id:str|None=None)->dict[str,Any]:
        from .index_readiness import status
        return (await status(backend,_principal(),document_id)).model_dump(mode='json')

    @mcp.tool(
        title="Read staged book pages",
        description=(
            "Return the next small batch of staged source pages as model-visible image "
            "content plus explicit native preview metadata. Read block continuation cursors "
            "with the same tool before using clipped text. Native extraction never proves "
            "visual completeness. Use only while parsing an existing ingestion."
        ),
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def book_pages(
        ingestion_id: str,
        cursor: str | None = None,
        batch_size: int = 4,
    ) -> list[TextContent | ImageContent]:
        batch_size = max(1, min(batch_size, 8))
        batch = await backend.book_pages(
            ingestion_id=ingestion_id,
            principal=_principal(),
            cursor=cursor,
            batch_size=batch_size,
        )
        manifest = {
            "ingestion_id": ingestion_id,
            "next_cursor": batch.next_cursor,
            "pages": [
                {
                    "page_id": page.page_id,
                    "physical_page_index": page.physical_page_index,
                    "printed_page_number": page.printed_page_number,
                    "mime_type": page.mime_type,
                    "native_text": page.native_text,
                    "native_blocks": list(page.native_blocks),
                    "native_text_info": page.native_text_info,
                }
                for page in batch.pages
            ],
        }
        result: list[TextContent | ImageContent] = [
            TextContent(type="text", text=json.dumps(manifest, ensure_ascii=False))
        ]
        for page in batch.pages:
            image_format = page.mime_type.removeprefix("image/")
            result.append(Image(data=page.data, format=image_format).to_image_content())
        return result

    @mcp.tool(
        title="Manage document access",
        description=(
            "Inspect or change access to a document. Public visibility is rejected "
            "unless the document already has a verified public rights basis."
        ),
        annotations=ToolAnnotations(
            read_only_hint=False,
            destructive_hint=False,
            idempotent_hint=True,
            open_world_hint=False,
        ),
    )
    async def document_access(
        document_id: str,
        visibility: Visibility | None = None,
        grantee_user_id: str | None = None,
    ) -> DocumentAccessOutput:
        return await backend.document_access(
            document_id=document_id,
            principal=_principal(),
            visibility=visibility,
            grantee_user_id=grantee_user_id,
        )

    @mcp.tool(
        title="Connected knowledge profile",
        description="Return the stable connected Regional Knowledge account identity.",
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
        meta={"openai/profile": True},
    )
    def profile() -> ProfileOutput:
        return ProfileOutput(id=_principal().subject)

    return mcp


def main() -> None:
    profile = os.getenv("RKB_MCP_PROFILE", "full").strip().lower()
    if profile not in {"full", "live"}:
        raise RuntimeError("RKB_MCP_PROFILE must be 'full' or 'live'")
    build_server(profile=profile).run(
        transport="streamable-http",
        stateless_http=True,
        json_response=True,
        transport_security=_transport_security(),
    )


if __name__ == "__main__":
    main()
