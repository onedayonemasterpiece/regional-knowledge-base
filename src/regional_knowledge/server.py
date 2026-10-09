from __future__ import annotations

import json
import asyncio
import logging
import os
from typing import Annotated, Any, Literal
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
from pydantic import AnyHttpUrl, Field

from starlette.routing import Route
from mcp.server.auth.routes import build_metadata, cors_middleware
from mcp.server.auth.handlers.metadata import MetadataHandler
from starlette.requests import Request
from starlette.responses import JSONResponse
from .entity_graph import GraphBundle,GraphAlias
from .graph_service import GraphService
from .local_e5 import LocalE5Embedder
from .auth import JwtResourceVerifier
from .backend import KnowledgeBackend
from .contracts import (
    BookFindOutput,
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
from .story_registry import StoryRegistry, StoryError
from .story_contracts import (
    Key, SeedInput, SourceRef, StoryMetadata, StoryOperation,
    ReviewDecision, RegisteredSource, ExtractRequest,
)


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


class KnowledgeMCPServer(MCPServer):
    """Advertise the public PKCE clients supported by our embedded provider."""

    def streamable_http_app(self, **kwargs):
        app = super().streamable_http_app(**kwargs)
        if isinstance(self._auth_server_provider, RegionalOAuthProvider):
            auth = self.settings.auth
            metadata = build_metadata(
                auth.issuer_url, auth.service_documentation_url,
                auth.client_registration_options, auth.revocation_options,
            )
            methods = ["client_secret_post", "client_secret_basic", "none"]
            metadata.token_endpoint_auth_methods_supported = methods
            metadata.revocation_endpoint_auth_methods_supported = methods
            for index, route in enumerate(app.routes):
                if getattr(route, "path", None) == "/.well-known/oauth-authorization-server":
                    app.routes[index] = Route(
                        route.path,
                        endpoint=cors_middleware(MetadataHandler(metadata).handle, ["GET", "OPTIONS"]),
                        methods=["GET", "OPTIONS"],
                    )
                elif getattr(route, "path", None) == "/revoke":
                    app.routes[index] = Route(
                        route.path,
                        endpoint=cors_middleware(self._auth_server_provider.revoke_endpoint, ["POST", "OPTIONS"]),
                        methods=["POST", "OPTIONS"],
                    )
        return app


def build_server(
    backend: KnowledgeBackend | None = None,
    *,
    issuer: str | None = None,
    resource_url: str | None = None,
    jwks_url: str | None = None,
    oauth_provider: RegionalOAuthProvider | None = None,
    profile: Literal["full", "live", "story_reader", "story_contributor", "story_editor"] = "full",
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

    mcp = KnowledgeMCPServer(
        "Regional Knowledge Base",
        instructions=(
            "Search and retrieve sourced regional knowledge. Use search before fetch. "
            "When a user asks to add an attached PDF/DjVu book, start ingestion and carry "
            "it through book_pages, model review/stage, validate and finalize by following "
            "next_action/status. When a user asks to reimport an existing book without an "
            "attachment, use book_find by title/author; if several plausible books remain, "
            "ask which one using title, author and year, never UUIDs. Then use "
            "book_ingest(reprocess) from its verified archived source; ask for a re-upload "
            "only when that archive is genuinely unavailable. Resume existing ingestion "
            "state after interruptions. The model performs semantic reading/recognition; "
            "the MCP only transports and stores deterministic source material. Hide internal "
            "workflow terms unless they are useful to explain a real blocker. Build coherent "
            "retrieval passages around roughly 256 encoder tokens, usually about 800-1000 "
            "characters and generally about 700-1100 for the current book corpus; never use "
            "page boundaries as chunk boundaries by default. Preserve complete semantic "
            "sentences/paragraphs and let validation enforce exact final-input token budgets. Never infer "
            "that a source is public."
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


    # Optional narrow function-call bundles for the client application's own Live agent.
    # The default read-only Live surface above remains unchanged.
    story_profiles = {"full", "story_reader", "story_contributor", "story_editor"}
    if profile in story_profiles:
        if not hasattr(backend, "corpus"):
            if profile != "full":
                raise RuntimeError("Story Registry requires the existing SQLite authority")
        else:
            registry = StoryRegistry(backend.corpus)

            async def story_call(fn, *args, **kwargs):
                try:
                    return await asyncio.to_thread(fn, _principal(), *args, **kwargs)
                except StoryError as exc:
                    return {"error": {"code": exc.code, "message_ru": exc.message,
                                      "detail": exc.detail}}

            @mcp.tool(name="story_search", title="Find editorial stories",
                description="ACL-scoped compact editorial story search; story content and book evidence remain distinct.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_search(query: str = "", filters: dict[str,str] | None = None,
                    mode: Literal["lexical","semantic","hybrid"] = "lexical",
                    order: Literal["updated","potential"] = "updated",
                    limit: Annotated[int, Field(ge=1,le=20)] = 3,
                    cursor: str | None = None) -> dict[str,Any]:
                return await story_call(registry.search, query, filters, mode, order, limit, cursor)

            @mcp.tool(name="story_get", title="Read a sourced editorial card",
                description="Read one authorized story revision, its typed assertions and editorial state.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_get(story_id: str, revision: int | None = None,
                    view: Literal["compact","editorial","evidence","review"] = "compact") -> dict[str,Any]:
                return await story_call(registry.get, story_id, revision, view)

            @mcp.tool(name="story_history", title="Read story change history",
                description="Bounded version authorship and actions, with current source-access recheck.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_history(story_id: str, from_revision: int | None = None,
                    to_revision: int | None = None, limit: Annotated[int,Field(ge=1,le=20)]=10,
                    cursor: str | None = None) -> dict[str,Any]:
                return await story_call(registry.history, story_id, from_revision, to_revision, cursor, limit)

            @mcp.tool(name="story_validate", title="Check editorial readiness",
                description="Read-only structural check. Text matching is not semantic or historical verification.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_validate(story_id: str,
                    variant_revision_ids: list[dict[str,str|int]] | None = None) -> dict[str,Any]:
                return await story_call(registry.validate, story_id, variant_revision_ids)

            @mcp.tool(name="story_job_get", title="Inspect extraction checkpoint",
                description="Real persisted state; awaiting_agent means that no model worker has started.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_job_get(job_id: str) -> dict[str,Any]:
                return await story_call(registry.job_get, job_id)

            @mcp.tool(name="entity_list", title="List accepted graph mentions",
                description="Authorized bounded entity list, not a complete claim of book coverage.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def entity_list(document_ids: list[str] | None = None, kinds: list[str] | None = None,
                    query: str = "", limit: Annotated[int,Field(ge=1,le=50)]=20,
                    cursor: str | None = None) -> dict[str,Any]:
                return await story_call(registry.entity_list, document_ids, kinds, query, cursor, limit)

            @mcp.tool(name="corpus_read", title="Read bounded accepted book context",
                description="Read selected corpus revision in bounded source chunks with immutable provenance.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def corpus_read(document_id: str, source_revision: Annotated[int,Field(ge=1)],
                    cursor: str | None = None, limit: Annotated[int,Field(ge=1,le=5)]=3) -> dict[str,Any]:
                return await story_call(registry.corpus_read, document_id, source_revision, cursor, limit)

            if profile in {"full", "story_contributor", "story_editor"}:
                @mcp.tool(name="story_create", title="Save a story seed",
                    description="Persist an unknown-origin seed or candidate, with actor taken only from OAuth.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_create(seed: SeedInput, idempotency_key: Key,
                        workspace_id: str | None = None,
                        source_refs: list[SourceRef] | None = None,
                        metadata: StoryMetadata | None = None) -> dict[str,Any]:
                    return await story_call(registry.create, seed, metadata, workspace_id, source_refs, idempotency_key)

                @mcp.tool(name="story_edit", title="Apply atomic editorial changes",
                    description="Typed, revision-guarded changes; cannot approve a variant.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_edit(story_id: str, expected_revision: Annotated[int,Field(ge=1)],
                        operations: Annotated[list[StoryOperation], Field(min_length=1,max_length=12)],
                        idempotency_key: Key, reason: str | None = None) -> dict[str,Any]:
                    return await story_call(registry.edit, story_id, expected_revision, operations, idempotency_key, reason)

                @mcp.tool(name="story_archive", title="Archive or restore an editorial story",
                    description="Reversible, idempotent archive/restore, no physical erasure.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_archive(story_id: str, expected_revision: Annotated[int,Field(ge=1)],
                        idempotency_key: Key, action: Literal["archive","restore"]="archive") -> dict[str,Any]:
                    return await story_call(registry.archive, story_id, expected_revision, action, idempotency_key)

                @mcp.tool(name="story_source_register", title="Register immutable testimony",
                    description="Persist external note provenance once; books are referenced by existing document IDs.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_source_register(source: RegisteredSource, idempotency_key: Key) -> dict[str,Any]:
                    return await story_call(registry.register_source, source, idempotency_key)

                @mcp.tool(name="story_extract", title="Track bounded source extraction",
                    description="Explicit start/claim/stage/cancel lease workflow. No hidden LLM calls.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_extract(request: ExtractRequest, idempotency_key: Key) -> dict[str,Any]:
                    return await story_call(registry.extract, request, idempotency_key)

            if profile in {"full", "story_editor"}:
                @mcp.tool(name="story_transition", title="Review or approve chosen story variants",
                    description="Checks exact variant revisions, reviewer decision, source provenance and permissions.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_transition(story_id: str, expected_revision: Annotated[int,Field(ge=1)],
                        target_state: Literal["candidate","researching","drafting","review","publish_ready","deferred","rejected"],
                        idempotency_key: Key, variant_revision_ids: list[dict[str,str|int]] | None = None,
                        review: ReviewDecision | None = None) -> dict[str,Any]:
                    return await story_call(registry.transition, story_id, expected_revision,
                                            target_state, variant_revision_ids, review, idempotency_key)

                @mcp.tool(name="story_merge", title="Merge related story seeds",
                    description="Explicit revision-guarded merge, preserving original story references.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_merge(target_id: str, source_ids: Annotated[list[str],Field(min_length=1,max_length=10)],
                        expected_revisions: dict[str,int], reason: str,
                        idempotency_key: Key) -> dict[str,Any]:
                    return await story_call(registry.merge, target_id, source_ids, expected_revisions, reason, idempotency_key)

                @mcp.tool(name="story_access", title="Grant or revoke story capabilities",
                    description="Only authorized story owner/manager can change per-story grants; source ACL remains independent.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_access(story_id: str, expected_revision: Annotated[int,Field(ge=1)],
                        grantee_user_id: str, capability: Literal[
                            "viewer","contributor","researcher","editor","publisher","manager","revoke"],
                        idempotency_key: Key) -> dict[str,Any]:
                    return await story_call(registry.access, story_id, expected_revision,
                                            grantee_user_id, capability, idempotency_key)

                @mcp.tool(name="story_export", title="Export an approved story variant",
                    description="Read-only attribution-aware package; does not send or publish.",
                    annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
                async def story_export(story_id: str, variant_revision_id: str,
                        target: Literal["editorial","social","video","narration"]="editorial") -> dict[str,Any]:
                    return await story_call(registry.export, story_id, variant_revision_id, target)

                @mcp.tool(name="story_publication_record", title="Record a reported publication",
                    description="Persist a separately observed publication; never sends content or self-verifies provider.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_publication_record(story_id: str, variant_revision_id: str,
                        publication: dict[str,str], idempotency_key: Key) -> dict[str,Any]:
                    return await story_call(registry.publication_record, story_id, variant_revision_id,
                                            publication, idempotency_key)

    if profile.startswith("story_"):
        return mcp

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
            document_ids: list[str] | None = None,
        ) -> EvidenceSearchOutput:
            return await backend.search_evidence(
                query.strip(),
                _principal(),
                max_evidence=max(1, min(max_evidence, 5)),
                **({'main_job_id': main_job_id} if main_job_id else {}),
                **({'document_ids': document_ids} if document_ids is not None else {}),
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
    async def search(query: str, main_job_id: str | None = None, aliases: list[dict[str,str]] | None = None, document_ids: list[str] | None = None) -> SearchOutput:
        options={}
        if document_ids is not None:options['document_ids']=document_ids
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

    @mcp.tool(title="Browse the source catalog", description="List/find/get accessible books, journal issues and articles; cover returns a registered real cover/title page. Empty query lists sources; cursor paginates. Detailed metadata is stored in SQLite.", annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
    async def catalog(command: Literal['list','find','get','cover'] = 'list', query: str = '', document_id: str | None = None, kind: Literal['book','journal_issue','article'] | None = None, cursor: str | None = None, limit: Annotated[int, Field(ge=1,le=100)] = 20) -> dict[str,Any] | list[TextContent | ImageContent]:
        method=getattr(backend,'catalog',None)
        if method is None:raise RuntimeError('SQLite catalog is not enabled')
        if command in ('get','cover') and not document_id:raise ValueError('document_id required')
        if command=='cover':
            import base64
            from .catalog_components import cover
            metadata,data=await cover(backend,_principal(),document_id)
            result=[TextContent(type='text',text=json.dumps(metadata,ensure_ascii=False))]
            if data:result.append(ImageContent(type='image',data=base64.b64encode(data).decode(),mimeType='image/webp'))
            return result
        return await method(_principal(),query,limit=limit,cursor=cursor,kind=kind,document_id=document_id if command=='get' else None)

    @mcp.tool(title="Verify a printed source quote", description="Render only the requested source page with validated yellow stripes. Native text first; optional bounded Flash-Lite scan reader. Original source ownership is required; failure leaves normal fetch available.", annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
    async def source_proof(id: str, quote: Annotated[str,Field(min_length=1,max_length=3000)], physical_page_index: Annotated[int,Field(ge=0)] | None = None) -> list[TextContent | ImageContent]:
        import base64
        from .quote_proof import source_proof as prove
        metadata,data=await prove(backend,_principal(),id,quote,physical_page_index)
        result=[TextContent(type='text',text=json.dumps(metadata,ensure_ascii=False))]
        if data:result.append(ImageContent(type='image',data=base64.b64encode(data).decode(),mimeType='image/webp'))
        return result

    @mcp.tool(
        name="book_find",
        title="Find an existing book",
        description=(
            "Deterministically find accessible books by title or author before reprocessing. "
            "Returns compact document metadata only. If several plausible matches remain, "
            "ask the user which book they mean using title, author and year; do not ask for document UUIDs."
        ),
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def book_find(query: str, limit: Annotated[int, Field(ge=1, le=8)] = 8) -> BookFindOutput:
        return await backend.book_find(
            query.strip(),
            _principal(),
            limit=max(1, min(int(limit), 8)),
        )

    @mcp.tool(
        title="Add, reprocess or continue a book",
        description=(
            "Carry one book/journal ingestion goal. For a new attached PDF/DjVu use start, "
            "then book_pages plus model-authored stage, validate and finalize while following "
            "next_action. For an existing book named by the user, call book_find first and "
            "use reprocess with its document_id; reprocess opens the verified archived source "
            "and creates/resumes the next revision on the same logical document without a "
            "re-upload. For unchanged already accepted sources, rechunk creates a derived-only "
            "pending revision without fetching the original archive again; validate/finalize "
            "must prove exact source/geometry/illustration identity. For new crops the original "
            "verified PDF/DjVu archive is still mandatory. "
            "Resume existing state after interruptions; when the user explicitly "
            "asks to replace a still-pending reprocess, pass metadata.duplicate_policy=new_revision "
            "to start one newer archived-source revision instead of resuming the pending job. "
            "Finalization alone activates "
            "the revision. If next_action is wait, return control to the user and check status "
            "later instead of tight polling. Stage coherent semantic retrieval passages near "
            "256 encoder tokens (typically 800-1000 characters, broadly 700-1100 here), not "
            "mechanical page-sized chunks. Exact E5/BGE token counts are validated over the "
            "final augmented search material before finalization."
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
        command: Literal["start", "reprocess", "rechunk", "stage", "validate", "finalize", "status"],
        file: ChatFile | None = None,
        document_id: str | None = None,
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
            document_id=document_id,
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
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("regional_knowledge.oauth_provider").setLevel(logging.INFO)
    profile = os.getenv("RKB_MCP_PROFILE", "full").strip().lower()
    if profile not in {"full", "live", "story_reader", "story_contributor", "story_editor"}:
        raise RuntimeError("RKB_MCP_PROFILE must be full, live or an enabled story capability bundle")
    build_server(profile=profile).run(
        transport="streamable-http",
        stateless_http=True,
        json_response=True,
        transport_security=_transport_security(),
    )


if __name__ == "__main__":
    main()