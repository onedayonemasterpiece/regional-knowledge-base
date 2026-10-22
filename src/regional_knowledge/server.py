from __future__ import annotations

import json
import asyncio
import logging
import os
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from mcp.server.mcpserver import Image, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.settings import (
    AuthSettings,
    ClientRegistrationOptions,
    RevocationOptions,
)
from mcp.types import CallToolResult, ImageContent, TextContent, ToolAnnotations
from pydantic import AnyHttpUrl, Field

from starlette.routing import Route
from mcp.server.auth.routes import build_metadata, cors_middleware
from mcp.server.auth.handlers.metadata import MetadataHandler
from starlette.requests import Request
from starlette.responses import JSONResponse
from .entity_graph import GraphBundle,GraphAlias
from .graph_service import GraphService
from .geo_resolution import GeoProposal
from .local_e5 import LocalE5Embedder
from .auth import JwtResourceVerifier
from .backend import KnowledgeBackend
from .kaggle_import_contracts import TargetSourceImportInput
from .contracts import TelegramOriginalInput
from .contracts import (
    BookFindOutput,
    BookIngestOutput,
    StoryExtractionStatus,
    ChatFile,
    DocumentAccessOutput,
    EvidenceSearchOutput,
    FetchOutput,
    Principal,
    ProfileOutput,
    SearchOutput,
    StartupStatus,
    StageChunkInput,
    StagePageInput,
    StagePoiFactInput,
    StagePoiMediaLinkInput,
    StartMetadataInput,
    CatalogUpdateInput,
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
    StageStoryCandidateInput, Key, SeedInput, SourceRef, StoryMetadata, StoryOperation,
    ReviewDecision, RegisteredSource, ExtractRequest, ReconcileRequest,
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


def _require_hybrid_read(result):
    """Public search never substitutes lexical evidence for unavailable BGE."""
    # Cold startup is an MCP response, not a fabricated QUERY_RESULT or hit.
    if (result.status == 'pending' and result.startup is not None
            and result.startup.protocol == 'rkb.startup.v1'
            and result.main_state == 'starting'):
        return result
    if (result.mode != "hybrid" or result.main_state != "ready"
            or result.retrieval_mode not in {"bge_lexical", "e5_bge_lexical"}):
        raise ToolError(json.dumps({"status": "unavailable", "reason": "hybrid_search_required",
            "main_state": result.main_state, "main_job_id": result.main_job_id,
            "read_trace": result.read_trace.model_dump() if result.read_trace else None},
            separators=(",", ":")))
    return result


async def _attach_story_extraction(
    backend: KnowledgeBackend,
    principal: Principal,
    result: BookIngestOutput,
) -> BookIngestOutput:
    """Expose persisted same-pass story coverage, never infer it from stage input.

    The ingestion row is resolved with the caller's identity first. This keeps
    source ownership and revision checks authoritative and works for every
    ingest command (stage, validate, finalize, status, reprocess and resume).
    """
    if not result.ingestion_id or getattr(backend, "corpus", None) is None:
        return result
    lookup = getattr(backend, "_ingestion_row", None)
    if lookup is None:
        return result
    row = await lookup(principal=principal, ingestion_id=result.ingestion_id)
    if row is None:
        return result
    from .story_ingestion import extraction_status

    from .contracts import KnowledgeExtractionStatus
    from .ingestion_enrichment import knowledge_status
    # SQLite reads run off-loop; no source graph load or network under a write lock.
    status = await asyncio.to_thread(extraction_status, backend.corpus, row)
    if status is None:
        return result
    knowledge = await asyncio.to_thread(knowledge_status, backend.corpus, row)
    update = {"story_extraction": StoryExtractionStatus.model_validate(status)}
    if knowledge is not None:
        update['knowledge_extraction'] = KnowledgeExtractionStatus.model_validate(knowledge)
        if result.next_action == 'done' and knowledge['state'] != 'pass_completed':
            update['message'] = result.message + '; source ready, semantic extraction still requires model work'
    return result.model_copy(update=update)


def _book_page_tool_result(ingestion_id, batch):
    manifest = {'ingestion_id': ingestion_id, 'next_cursor': batch.next_cursor,
        'pages': [{'page_id': page.page_id, 'physical_page_index': page.physical_page_index,
            'printed_page_number': page.printed_page_number, 'mime_type': page.mime_type,
            'native_text': page.native_text, 'native_blocks': list(page.native_blocks),
            'native_text_info': page.native_text_info} for page in batch.pages]}
    content = [TextContent(type='text', text=json.dumps(manifest, ensure_ascii=False))]
    for page in batch.pages:
        content.append(Image(data=page.data, format=page.mime_type.removeprefix('image/')).to_image_content())
    return CallToolResult(content=content)


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

    async def call_tool(self, name, arguments, context=None):
        allowed = {'search','fetch','knowledge_search','profile'}
        if getattr(self,'_target_story_available',False):
            allowed.update({'story_search','story_get','story_history','story_variants'})
        if getattr(self,'_target_editorial_available',False):allowed.update({'story_edit','story_access'})
        if getattr(self,'_target_facts_available',False):
            allowed.update({'story_calendar','story_observation_search','story_observation_compare'})
        if getattr(self,'_target_import_available',False):
            allowed.update({'book_ingest','book_pages'})
        if getattr(self,'_target_cases_available',False):
            allowed.update({'disagreement_get','disagreement_edit','disagreement_contribute','disagreement_conclusion'})
        if getattr(self,'_target_graph_available',False):
            allowed.update({'graph_fetch','entity_list'})
        if getattr(self,'_target_geo_available',False):
            allowed.update({'geo_lookup','graph_fetch','poi_context'})
        if getattr(self,'_target_catalog_available',False):
            allowed.add('book_find')
        if getattr(self,'_target_source_profile',False):allowed.add('startup_status')
        if getattr(self,'_target_source_profile',False) and name not in allowed:
            raise ToolError('{"status":"unavailable","reason":"target_domain_unavailable"}')
        from .read_issuance import capture_read_output, bind_request_output
        with capture_read_output() as calls:
            result = await super().call_tool(name, arguments, context)
            if calls:
                bind_request_output(context, calls)
            return result

    def streamable_http_app(self, **kwargs):
        guarded = getattr(self, '_current_read_output_required', False)
        if guarded:
            if kwargs.get('stateless_http', False) is not True or kwargs.get(
                    'json_response', False) is not True:
                raise RuntimeError('current evidence requires stateless JSON HTTP')
        app = super().streamable_http_app(**kwargs)
        if guarded:
            from .read_issuance import ReadIssuanceMiddleware
            app.add_middleware(ReadIssuanceMiddleware)
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
        channel = getattr(self, '_bge_channel_route', None)
        if channel is not None:
            app.routes.append(channel)
        return app


def build_server(
    backend: KnowledgeBackend | None = None,
    *,
    issuer: str | None = None,
    resource_url: str | None = None,
    jwks_url: str | None = None,
    oauth_provider: RegionalOAuthProvider | None = None,
    profile: Literal["full", "live", "story_reader", "story_contributor", "story_editor"] = "full",
    backend_profile: Literal['legacy','kaggle_source'] | None = None,
    target_startup=None,
    projects_hub_receiver=None,
    expert_evidence_receiver=None,
    expert_slack_ingress=None,
) -> MCPServer:
    selected=backend_profile or os.getenv('RKB_BACKEND_PROFILE','').strip() or (
        'kaggle_source' if target_startup is not None else 'legacy')
    if selected not in {'legacy','kaggle_source'}:
        raise RuntimeError('RKB_BACKEND_PROFILE must be legacy or kaggle_source')
    if selected=='kaggle_source':
        from .kaggle_service_startup import TargetServiceStartup
        if type(target_startup) is not TargetServiceStartup:
            raise RuntimeError('kaggle_source requires explicit typed TargetServiceStartup')
        if backend is not None or oauth_provider is not None:
            raise RuntimeError('target startup owns the configured backend and OAuth provider')
        if profile not in {'full','live'}:
            raise RuntimeError('target story capability bundle is unavailable')
        backend=target_startup.backend
        oauth_provider=target_startup.config.oauth_provider
        if ((issuer is not None and issuer.rstrip('/')!=oauth_provider.issuer)
                or (resource_url is not None and resource_url.rstrip('/')!=oauth_provider.resource)):
            raise RuntimeError('target OAuth issuer/resource must match existing authority')
        issuer,resource_url=oauth_provider.issuer,oauth_provider.resource
    elif target_startup is not None:
        raise RuntimeError('target startup cannot use legacy backend profile')
    else:
        backend = backend if backend is not None else backend_from_env()
    from .read_telemetry import ReadTelemetry
    read_telemetry = getattr(backend, 'read_telemetry', None) or ReadTelemetry()
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
    if selected=='kaggle_source':
        kwargs['lifespan']=target_startup.lifespan
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
            ("This configured target's fresh-source importer accepts PDF attachments up to 2 MiB and 32 pages, "
             "in batches of at most 8 staged pages. HTML, DjVu, reprocess, catalog corrections and entity/POI mutations "
             "are unavailable on this bounded path. Start requires source_import.public_rights with an explicit "
             "verified public-distribution basis. Follow every book_pages continuation before using source text. "
             "Stage exact observed native blocks or unique source spans with their actual bounding boxes; "
             "source_import.research must contain the calling model's explicit state and reachable continuation. "
             "Story candidates may supply a basic_variant with the actual authored body and complete evidence map; "
             "a summary never becomes a narrative. A facts_only outcome or needs_context frontier is explicit model work. "
             "Finalized source activation does not establish retrieval acceptance or exhaustive semantic coverage. "
             if selected=='kaggle_source' and getattr(target_startup,'importer',None) is not None else "") +
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
            "page boundaries as chunk boundaries by default. During the SAME model page-review pass, "
            "sentences/paragraphs and let validation enforce exact final-input token budgets. "
            "For every staged page also review possible story episodes, mark story_candidates_reviewed=true " +
            "even when there is no suitable candidate, and send bounded story_candidates in the " +
            "same book_ingest(stage) call. Attach source-region exact quotes; distinguish the book author's " +
            "account from historically proven facts. Do not make a candidate per chunk. " +
            "Complete page-by-page story review in the existing ingestion workflow without a second prompt. " +
            "In that SAME page-and-passage review, identify historical place names, "
            "toponyms and old addresses grounded in actually printed source text. "
            "Before adding a person, organization or place, use entity_list and graph_fetch "
            "to inspect existing identities, aliases and context. For locations use kinds=['poi_ref'] "
            "(or ['place']); compare place_kind, containing place, source time and narrative, "
            "not just spelling. Reuse the model-selected entity_id across pages and books; "
            "a new name for an existing place is an alias, not a new entity. "
            "Put a bounded set of DISTINCT, evidence-backed poi_ref graph nodes in "
            "book_ingest(stage).entity_candidates, with exact_source_spelling, "
            "poi_locator names and source chunk/page/region/exact_quote references. "
            "Set place_kind (for example city, island, street, square or building) and "
            "a short disambiguating place_context. Link places with located_in relations "
            "when the source supports them. entity_refs can refer to existing parent IDs "
            "without inventing another mention. Do not infer contains/located_in merely "
            "because two names share a passage. A chunk may mention several places, or "
            "only a broad island/city; keep that granularity instead of guessing a house. "
            "For EVERY source page, review all applicable stories, people, organizations, events, "
            "dates, places, quantities, captions and footnotes in the SAME semantic pass. "
            "Set knowledge_reviewed=true only after this work, including a genuinely empty page. "
            "Do not populate just poi_ref: include person, organization, event and historical_thread "
            "nodes where the source supports them, preserving aliases and relations. "
            "A story candidate can include event_dates and observations with evidence_indexes "
            "(zero-based positions in its own evidence_refs, default [0]); dates must retain "
            "precision, explicit calendar or unspecified, and exact printed date text. "
            "Use story candidate entity_keys to link only model-selected GraphBundle keys. "
            "These fields are stored with that same attributed story at source activation; "
            "no later copying of already read dates/numbers is needed. "
            "Web research can clarify current/historical names: record its URL and note "
            "in research_sources on the entity/alias, separately from exact book evidence. "
            "Map and coordinate fields are optional. Later model-selected map_refs or "
            "canonical_poi_ref enrich the SAME entity_id; they are not new local identities. "
            "For visitable or memorialized physical sites, also identify POIs during the "
            "same review. After source activation call poi_registry(search) for candidate "
            "real objects, then poi_registry(select or create) with the accepted RKB entity_id. "
            "Select an existing Street Story POI or create a Street Story candidate for "
            "a standing building, ruins, plaque, lost site or other grounded physical site. "
            "Do not turn every island, street or abstract topic into a POI. No automatic "
            "name-based merge; a candidate is not a verified historic assertion. "
            "For connected POIs use poi_context to retrieve related stories, people, events "
            "and exact source evidence under actor permissions. Missing geometry or a delayed "
            "POI owner must never block book activation. "
            "Do not generate one place per chunk or duplicate names. A unique "
            "Street Story POI is NOT required at book ingestion time: unresolved "
            "identity stays queued and must not block the source or Story Registry. "
            "Never invent coordinates, historical footprints, modern addresses, "
            "canonical POI IDs or time precision. If the page has no suitable "
            "sourced place, an empty graph candidate batch is fine. The accepted "
            "source activates the durable geo queue without a second model prompt, "
            "but this queue never decides identity automatically. The model selects existing "
            "local entities and external POI references explicitly; name matches remain search "
            "candidates. For already accepted material, graph_stage adds mentions and relations "
            "without reimporting, re-chunking or re-embedding the source. " +
            "Never infer that a source is public."
        ),
        **kwargs,
    )

    if embedded_provider is not None:
        embedded_provider.register_routes(mcp)

    mcp._target_source_profile=selected=='kaggle_source'
    mcp._target_import_available = selected=='kaggle_source' and getattr(target_startup,'importer',None) is not None
    mcp._target_story_available = (selected=='kaggle_source' and target_startup.gate.story_supported())
    mcp._target_facts_available = (selected=='kaggle_source' and target_startup.gate.facts_supported())
    mcp._target_catalog_available = (selected=='kaggle_source' and target_startup.gate.catalog_supported())
    mcp._target_cases_available = (selected=='kaggle_source' and getattr(target_startup,'cases_backend',None) is not None)
    mcp._target_editorial_available = (selected=='kaggle_source' and getattr(target_startup,'editorial',None) is not None)
    mcp._target_graph_available = (selected=='kaggle_source' and target_startup.gate.graph_supported())
    mcp._target_geo_available = (selected=='kaggle_source' and getattr(target_startup,'geo_backend',None) is not None)
    mcp._target_startup=target_startup
    mcp._current_read_output_required = bool(getattr(backend, 'current_read_output_required', False))
    worker_queue = getattr(backend, 'worker_queue', None)
    if worker_queue is not None or os.getenv('RKB_BGE_QUEUE_PATH'):
        from .bge_queue import BgeQueue
        from .bge_broker import register_routes
        from .bge_channel import channel_route
        if worker_queue is None:
            worker_queue = BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
        elif os.getenv('RKB_BGE_QUEUE_PATH'):
            from pathlib import Path
            if worker_queue.path.resolve() != Path(os.environ['RKB_BGE_QUEUE_PATH']).resolve():
                raise RuntimeError('backend and channel must share the same owned queue')
        register_routes(mcp,worker_queue)
        mcp._bge_channel_route = channel_route(worker_queue,read_telemetry=read_telemetry)

    if projects_hub_receiver is not None:
        from .projects_hub_reader import ProjectsHubReader, PATH
        if (selected != 'kaggle_source' or type(projects_hub_receiver) is not ProjectsHubReader
                or projects_hub_receiver.backend is not backend):
            raise RuntimeError('PH receiver requires the same configured target backend')
        mcp.custom_route(PATH, methods=['POST'])(projects_hub_receiver.__call__)

    if expert_evidence_receiver is not None:
        from .expert_evidence_receiver import ExpertEvidenceReceiver, PATH as EXPERT_PATH
        if (selected != 'kaggle_source' or type(expert_evidence_receiver) is not ExpertEvidenceReceiver
                or expert_evidence_receiver.backend is not backend
                or expert_evidence_receiver.authority is not target_startup.authority
                or expert_evidence_receiver.control.spool is not target_startup.config.spool):
            raise RuntimeError('expert evidence receiver requires exact existing target owners')
        mcp.custom_route(EXPERT_PATH, methods=['POST'])(expert_evidence_receiver.__call__)

    if expert_slack_ingress is not None:
        from .expert_slack_ingress import ExpertSlackIngress, PATH as SLACK_PATH
        if (selected != 'kaggle_source' or type(expert_slack_ingress) is not ExpertSlackIngress
                or expert_slack_ingress.control.spool is not target_startup.config.spool
                or expert_slack_ingress.control.people is not target_startup.config.participants):
            raise RuntimeError('Slack ingress requires exact existing target control owners')
        mcp.custom_route(SLACK_PATH, methods=['POST'])(expert_slack_ingress.__call__)

    @mcp.custom_route("/fast-tier/health", methods=["GET"])
    async def fast_tier_health(request: Request):
        encoder = getattr(backend, "embedder", None)
        if isinstance(encoder, LocalE5Embedder):
            status = await encoder.status()
            return JSONResponse({k:status[k] for k in ("configured","ready","retrieval_mode")},headers={"Cache-Control":"no-store"})
        return JSONResponse({"configured":False,"ready":False,"retrieval_mode":"lexical_only"},headers={"Cache-Control":"no-store"})


    @mcp.custom_route("/internal/street-story/poi-context", methods=["GET"])
    async def street_story_poi_context(request: Request):
        # Explicit single-owner service grant. Never accept the Street Story
        # device token, arbitrary actor IDs or forwarded end-user OAuth bearers.
        import hmac
        from .poi_registry_bridge import _owner_service, PoiRegistry
        try:
            _url, expected = _owner_service()
        except ValueError:
            return JSONResponse({"error":"bridge_unavailable"},status_code=503)
        bearer = request.headers.get("Authorization","")
        supplied = bearer[7:] if bearer.startswith("Bearer ") else ""
        if not supplied or not hmac.compare_digest(supplied,expected):
            return JSONResponse({"error":"service_auth_required"},status_code=401)
        actor=os.getenv("RKB_OWNER_SUBJECT","")
        if not actor or not hasattr(backend,"corpus"):
            return JSONResponse({"error":"scope_unavailable"},status_code=503)
        from .contracts import Principal
        principal=Principal(subject=actor,client_id="street-story-poi-reader",
                            issuer="trusted-service-grant",access_token="")
        try:
            poi_ref=str(request.query_params.get("poi_ref",""))[:140]
            value=await PoiRegistry(backend).context(principal,poi_ref,limit=8)
            return JSONResponse(value,headers={"Cache-Control":"no-store"})
        except (ValueError,LookupError):
            return JSONResponse({"error":"invalid_poi_ref"},status_code=422)

    # Optional narrow function-call bundles for the client application's own Live agent.
    # The default read-only Live surface above remains unchanged.
    story_profiles = {"full", "story_reader", "story_contributor", "story_editor"}
    if profile in story_profiles and mcp._target_cases_available:
        from .kaggle_case_tools import register_case_tools
        register_case_tools(mcp,target_startup.cases_backend,_principal)

    if profile in story_profiles and mcp._target_graph_available:
        from .kaggle_source_graph_tools import register_source_graph_tools
        register_source_graph_tools(mcp,backend,getattr(target_startup,'geo_backend',None),_principal)

    if profile in story_profiles and mcp._target_geo_available:
        @mcp.tool(name='geo_lookup',title='Find retained private geographic identities',
            description='Exact normalized name and optional declared source year. Minimal legacy-unanchored provenance; '
                'canonical geometry and map layers remain with their owners. Unknown validity remains unknown.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_geo_lookup(name:Annotated[str,Field(min_length=1,max_length=200)],
                year:Annotated[int,Field(ge=1,le=9999)]|None=None,
                limit:Annotated[int,Field(ge=1,le=3)]=3,cursor:str|None=None)->dict[str,Any]:
            return await target_startup.geo_backend.geo_lookup(name,_principal(),year=year,limit=limit,cursor=cursor)

        if not mcp._target_graph_available:
            @mcp.tool(name='graph_fetch',title='Read a retained geographic identity or metadata page',
                description='Exact private GEO entity UUID, with separately paginated minimal provenance, aliases, relations '
                    'and owner references. Old source text and graph discovery are unavailable.',
                annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
            async def native_geo_graph_fetch(entity_id:str,limit:Annotated[int,Field(ge=1,le=3)]=3,
                    cursor:str|None=None,category:Literal['owner_reference','provenance','alias','relation','source_geo_observation']|None=None,
                    discovery_job_id:str|None=None)->dict[str,Any]:
                return await target_startup.geo_backend.graph_fetch(entity_id,_principal(),limit=limit,cursor=cursor,
                    category=category,discovery_job_id=discovery_job_id)

        @mcp.tool(name='poi_context',title='Read retained links to a physical POI',
            description='Private RKB geographic identities linked to the exact existing Street Story POI key. '
                'Returns source locators and owner references; no migrated old book text or Story content.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_geo_poi_context(poi_ref:str,limit:Annotated[int,Field(ge=1,le=3)]=3,
                cursor:str|None=None)->dict[str,Any]:
            return await target_startup.geo_backend.poi_context(poi_ref,_principal(),limit=limit,cursor=cursor)

    if profile in story_profiles and mcp._target_facts_available:
        @mcp.tool(name='story_calendar',title='Historical events on a calendar day',
            description='Native indexed source-attributed exact event dates, including facts without a Story. '
                'Explicit stated calendar; no publication/approximate date substitution or calendar conversion.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_source_calendar(month:Annotated[int,Field(ge=1,le=12)],day:Annotated[int,Field(ge=1,le=31)],
                calendar:Literal['gregorian','julian','unspecified']='gregorian',
                limit:Annotated[int,Field(ge=1,le=10)]=3,cursor:str|None=None)->dict[str,Any]:
            return await backend.story_calendar(_principal(),month=month,day=day,calendar=calendar,limit=limit,cursor=cursor)

        @mcp.tool(name='story_observation_search',title='Find sourced numeric observations',
            description='Native exact metric/unit indexed source quantities, including facts without a Story. '
                'Preserves original wording and exact decimal strings; no aggregation, conversion or identity merge.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_source_observations(metric_key:str,unit_code:str|None=None,
                limit:Annotated[int,Field(ge=1,le=10)]=3,cursor:str|None=None)->dict[str,Any]:
            return await backend.story_observation_search(_principal(),metric_key=metric_key,
                unit_code=unit_code,limit=limit,cursor=cursor)

        @mcp.tool(name='story_observation_compare',title='Check whether numeric series can be compared',
            description='Read two exact source observations linked to this Story and explain metric, unit, '
                'subject, method and period compatibility. No arithmetic, unit conversion or identity inference.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_observation_compare(story_id:str,left_id:str,right_id:str)->dict[str,Any]:
            return await backend.story_observation_compare(story_id,left_id,right_id,_principal())

    if profile in story_profiles and mcp._target_story_available:
        @mcp.tool(name='story_get',title='Read saved source-bound narrative',
            description='Exact saved model-authored basic Variant body and default reference, with current source-access recheck. '
                        'Only compact/editorial views are supported; evidence/review/editing/private variants remain unavailable.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_story_get(story_id:str,revision:int|None=None,
                view:str='compact',cursor:str|None=None,limit:Annotated[int,Field(ge=1,le=10)]=8,
                assertion_id:str|None=None,relation_id:str|None=None)->dict[str,Any]:
            return await backend.story_get(story_id,_principal(),revision=revision,view=view,cursor=cursor,
                limit=limit,assertion_id=assertion_id,relation_id=relation_id)

        @mcp.tool(name='story_search',title='Find saved source-bound narratives',
            description='Bounded indexed hybrid search over actual saved basic narrative bodies; no summary/body fallback.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_story_search(query:str,filters:dict[str,str]|None=None,mode:str='hybrid',order:str='updated',
                limit:Annotated[int,Field(ge=1,le=5)]=3,cursor:str|None=None)->EvidenceSearchOutput:
            return await backend.story_search(query,_principal(),filters=filters,mode=mode,order=order,limit=limit,cursor=cursor)

        @mcp.tool(name='story_history',title='Read accepted narrative authorship',
            description='Bounded exact accepted source-import author/request/operation history with current source-access recheck.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_story_history(story_id:str,from_revision:int|None=None,to_revision:int|None=None,
                limit:Annotated[int,Field(ge=1,le=10)]=10,cursor:str|None=None)->dict[str,Any]:
            return await backend.story_history(story_id,_principal(),from_revision=from_revision,to_revision=to_revision,
                limit=limit,cursor=cursor)

        @mcp.tool(name='story_variants',title='Read saved basic narrative variant',
            description='Exact shared basic Variant body/revision; alternate/private/editorial variants remain unavailable.',
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def native_story_variants(story_id:str,limit:Annotated[int,Field(ge=1,le=10)]=10,
                cursor:str|None=None)->dict[str,Any]:
            return await backend.story_variants(story_id,_principal(),limit=limit,cursor=cursor)

    if profile in {'full','story_contributor','story_editor'} and mcp._target_editorial_available:
        @mcp.tool(name='story_edit',title='Apply atomic editorial changes',
            description='Revision-guarded native UpsertVariant with exact saved body. Currently one upsert_variant per operation; '
                'current per-Story editor grant is required, approvals are cleared, and original replay never reruns the model.',
            annotations=ToolAnnotations(read_only_hint=False,idempotent_hint=True,destructive_hint=False,open_world_hint=False))
        async def native_story_edit(story_id:str,expected_revision:Annotated[int,Field(ge=1)],
                operations:Annotated[list[StoryOperation],Field(min_length=1,max_length=12)],
                idempotency_key:Key,reason:str|None=None)->dict[str,Any]:
            return await backend.story_edit(story_id,_principal(),expected_revision=expected_revision,operations=operations,
                idempotency_key=idempotency_key,reason=reason)

        @mcp.tool(name='story_access',title='Grant or revoke story capabilities',
            description='Only the current native Story owner/manager may change an explicit per-Story grant. Source access stays independent.',
            annotations=ToolAnnotations(read_only_hint=False,idempotent_hint=True,destructive_hint=False,open_world_hint=False))
        async def native_story_access(story_id:str,expected_revision:Annotated[int,Field(ge=1)],grantee_user_id:str,
                capability:Literal['viewer','contributor','researcher','editor','publisher','manager','revoke'],
                idempotency_key:Key)->dict[str,Any]:
            return await backend.story_access(story_id,_principal(),expected_revision=expected_revision,grantee_user_id=grantee_user_id,
                capability=capability,idempotency_key=idempotency_key)

    if profile in story_profiles and not mcp._target_story_available:
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
                    mode: Literal["lexical","semantic","hybrid"] = "hybrid",
                    order: Literal["updated","potential"] = "updated",
                    limit: Annotated[int, Field(ge=1,le=20)] = 3,
                    cursor: str | None = None) -> dict[str,Any]:
                vector = None
                kaggle_state = None
                if mode == "lexical" or not query.strip():
                    raise ToolError("hybrid_search_required: provide a nonempty query and hybrid mode")
                mode = "hybrid"
                if mode != "lexical" and query.strip():
                    try:
                        from .bge_queue import BgeQueue
                        from .story_vector_index import query_vector
                        queue = BgeQueue(os.environ["RKB_BGE_QUEUE_PATH"])
                        vector, kaggle_state = await query_vector(
                            queue, _principal().subject, query.strip())
                    except Exception as error:
                        # Local BGE is deliberately forbidden. If Kaggle has
                        # not answered, never label a lexical result semantic.
                        kaggle_state = "kaggle_unavailable"
                        logging.getLogger(__name__).warning(json.dumps({
                            "event": "story_kaggle_query_degraded",
                            "error_type": type(error).__name__,
                        }))
                if vector is None:
                    return {"status": "unavailable", "reason": "hybrid_search_required",
                            "kaggle_query_state": kaggle_state or "kaggle_unavailable"}
                result = await story_call(
                    registry.search, query, filters, mode, order, limit, cursor, vector)
                if isinstance(result, dict) and "error" not in result:
                    if result.get("retrieval_mode") not in {"story_bge_lexical", "story_bge_lexical_partial"}:
                        return {"status": "unavailable", "reason": "hybrid_search_required",
                                "kaggle_query_state": kaggle_state or "projection_unavailable"}
                    result["kaggle_query_state"] = kaggle_state
                return result

            @mcp.tool(name="story_calendar", title="Historical events on a calendar day",
                description="SQL exact-day filter over CURRENT source-attributed event dates; "
                            "calendar=gregorian, julian or unspecified is explicit. "
                            "No year-only, approximate, publication-date or inferred conversions. "
                            "Returns short proof locators and a bounded ACL-scoped cursor.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_calendar(
                    month: Annotated[int, Field(ge=1,le=12)],
                    day: Annotated[int, Field(ge=1,le=31)],
                    calendar: Literal["gregorian","julian","unspecified"] = "gregorian",
                    limit: Annotated[int, Field(ge=1,le=10)] = 3,
                    cursor: str | None = None) -> dict[str,Any]:
                return await story_call(registry.calendar, month, day,
                                        calendar, limit, cursor)

            @mcp.tool(name="story_observation_search", title="Find sourced numeric observations",
                description="Exact indexed metric_key and optional unit_code. "
                            "No aggregation, no automatic conversion or identity merge. "
                            "Returns bounded, ACL-checked original source evidence.",
                annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
            async def story_observation_search(
                    metric_key: str,
                    unit_code: str | None = None,
                    limit: Annotated[int,Field(ge=1,le=10)] = 3,
                    cursor: str | None = None) -> dict[str,Any]:
                return await story_call(registry.observation_search,
                                        metric_key,unit_code,limit,cursor)

            @mcp.tool(name="story_observation_compare", title="Check whether numeric series can be compared",
                description="Read and explain why two existing sourced observations do "
                            "or do not have commensurate metrics and units. "
                            "No arithmetic until subject identity is independently reviewed.",
                annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
            async def story_observation_compare(
                    story_id: str,left_id: str,right_id: str) -> dict[str,Any]:
                return await story_call(registry.observation_compare,
                                        story_id,left_id,right_id)

            @mcp.tool(name="story_get", title="Read sourced stories and bounded evidence",
                description="Read one authorized story. For large cross-book dossiers use "
                            "view=evidence_page, assertion_page, assessments_page, event_dates_page, "
                            "observations_page, relation_history_page "
                            "or relation_reviews_page (requires relation_id) with a "
                            "revision-bound cursor and limit; sources are derived from actual "
                            "evidence/dependencies, including controlled merges.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_get(story_id: str, revision: int | None = None,
                    view: Literal["compact","editorial","evidence","review",
                                  "evidence_page","assertion_page","assessments_page","sources_page",
                                  "relations_page","relation_history_page","relation_reviews_page",
                                  "event_dates_page","event_date_history_page",
                                  "observations_page","observations_history_page"] = "compact",
                    cursor: str | None = None, limit: Annotated[int, Field(ge=1,le=10)] = 8,
                    assertion_id: str | None = None,
                    relation_id: str | None = None) -> dict[str,Any]:
                return await story_call(registry.get, story_id, revision, view, cursor,
                                        limit, assertion_id, relation_id)

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

            @mcp.tool(name="story_job_get", title="Inspect extraction and reconciliation state",
                description="Persisted bounded progress and paginated proposal IDs. "
                            "awaiting_agent means an external model must continue.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def story_job_get(job_id: str, cursor: str | None = None,
                    limit: Annotated[int,Field(ge=1,le=10)]=5,
                    proposal_id: str | None = None) -> dict[str,Any]:
                return await story_call(registry.job_get, job_id, cursor, limit, proposal_id)

            # Geography is a separate bounded SQLite outbox, not the older
            # BGE graph-discovery queue. Full/story readers may inspect scoped
            # source mentions; writes remain contributor/editor only.
            from .geo_resolution import GeoQueue,GeoError
            geo_queue=GeoQueue(backend.corpus)

            async def geo_call(fn,*args):
                try:
                    return await asyncio.to_thread(fn,_principal(),*args)
                except GeoError as exc:
                    return {"error":{"code":exc.code,
                            "message_ru":"Географическое действие недоступно или устарело"}}

            @mcp.tool(name="geo_status",title="Read geographic resolution queue and receipts",
                description="Authorized own source-backed geo intents and up to 10 "
                            "versioned attempts. No original chunk text or cartographic raster. "
                            "Unresolved is not absence of the historical place.",
                annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
            async def geo_status(request_id:str|None=None,entity_id:str|None=None,
                    limit:Annotated[int,Field(ge=1,le=10)]=5)->dict[str,Any]:
                return await geo_call(geo_queue.status,request_id,entity_id,limit)

            @mcp.tool(name="geo_receipt",title="Read a durable geographic mutation receipt",
                description="Authenticated lookup of an exact command receipt after "
                            "a lost response; never replays external operations.",
                annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
            async def geo_receipt(idempotency_key:str)->dict[str,Any]:
                return await geo_call(geo_queue.receipt,idempotency_key)

            @mcp.tool(name="geo_lookup",title="Find exact sourced place mentions with optional year",
                description="Fast exact normalized-name source lookup, actor-scoped. "
                            "Unknown temporal validity stays unknown; no radius, polygon "
                            "membership or accepted historical layer is implied.",
                annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
            async def geo_lookup(name:Annotated[str,Field(min_length=1,max_length=200)],
                    year:Annotated[int,Field(ge=1,le=9999)]|None=None,
                    limit:Annotated[int,Field(ge=1,le=10)]=3)->dict[str,Any]:
                return await geo_call(geo_queue.lookup,name,year,limit)

            if profile in {"full","story_contributor","story_editor"}:
                @mcp.tool(name="geo_request",title="Ensure an accepted source graph place has a queued geo intent",
                    description="Idempotently enqueue at most 20 exact owned source mentions "
                                "of one existing POI reference. No external cartography calls "
                                "or source reimport; normal graph staging also enqueues automatically.",
                    annotations=ToolAnnotations(read_only_hint=False,idempotent_hint=True,
                                                destructive_hint=False,open_world_hint=False))
                async def geo_request(entity_id:str,
                        cursor:str|None=None,
                        limit:Annotated[int,Field(ge=1,le=20)]=20)->dict[str,Any]:
                    return await geo_call(geo_queue.enqueue_existing,entity_id,cursor,limit)

                @mcp.tool(name="geo_claim",title="Claim bounded geo resolution jobs",
                    description="Lease/fence up to five jobs across source documents. "
                                "The authenticated source owner, not the model, sets scope. "
                                "Return original name, source refs, dependency revisions; "
                                "never return source text or private chunks.",
                    annotations=ToolAnnotations(read_only_hint=False,
                                                destructive_hint=False,open_world_hint=False))
                async def geo_claim(limit:Annotated[int,Field(ge=1,le=5)]=3,
                                    lease_seconds:Annotated[int,Field(ge=15,le=180)]=90)->dict[str,Any]:
                    return await geo_call(geo_queue.claim,limit,lease_seconds)

                @mcp.tool(name="geo_stage",title="Stage one fenced source-grounded place proposal",
                    description="Typed cartography.resolve.v1-compatible candidate/unresolved proposal. "
                                "Live Street Story owner binding is rechecked on apply. "
                                "No map geometry acceptance or public source transmission.",
                    annotations=ToolAnnotations(read_only_hint=False,idempotent_hint=True,
                                                destructive_hint=False,open_world_hint=False))
                async def geo_stage(attempt_id:str,lease_token:str,
                        lease_fence:Annotated[int,Field(ge=1)],
                        proposal:GeoProposal,
                        idempotency_key:Annotated[str,Field(min_length=8,max_length=128)]
                        )->dict[str,Any]:
                    return await geo_call(geo_queue.stage,attempt_id,lease_token,
                                          lease_fence,proposal,idempotency_key)

                @mcp.tool(name="geo_apply",title="Apply a fenced place candidate with durable receipt",
                    description="Revalidate exact source evidence, OAuth actor and Street Story "
                                "read-only owner alias. Update only the same RKB graph node, "
                                "never a different canonical ID or a Story revision. "
                                "Replay returns the previous durable receipt.",
                    annotations=ToolAnnotations(read_only_hint=False,idempotent_hint=True,
                                                destructive_hint=False,open_world_hint=False))
                async def geo_apply(attempt_id:str,lease_token:str,
                        lease_fence:Annotated[int,Field(ge=1)],
                        idempotency_key:Annotated[str,Field(min_length=8,max_length=128)]
                        )->dict[str,Any]:
                    return await geo_call(geo_queue.apply,attempt_id,lease_token,
                                          lease_fence,idempotency_key)

                @mcp.tool(name="geo_recheck",title="Schedule relevant changed-owner or map dependency",
                    description="Create one new attempt for exact matching place names "
                                "when an owner POI or map layer revision changes. This is "
                                "not an assertion that a cartographic layer was accepted.",
                    annotations=ToolAnnotations(read_only_hint=False,idempotent_hint=True,
                                                destructive_hint=False,open_world_hint=False))
                async def geo_recheck(
                    name:Annotated[str,Field(min_length=3,max_length=200)],
                    dependency_kind:Literal["owner_poi","cartography_layer"],
                    dependency_ref:Annotated[str,Field(min_length=1,max_length=160)],
                    dependency_revision:Annotated[str,Field(min_length=1,max_length=128)],
                    limit:Annotated[int,Field(ge=1,le=30)]=20)->dict[str,Any]:
                    return await geo_call(geo_queue.recheck,name,dependency_kind,
                                          dependency_ref,dependency_revision,limit)

            @mcp.tool(name="entity_list", title="Find existing people, organizations and locations",
                description="Authorized bounded entity list, not a complete claim of book coverage.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def entity_list(document_ids: list[str] | None = None, kinds: list[str] | None = None,
                    query: str = "", limit: Annotated[int,Field(ge=1,le=50)]=20,
                    cursor: str | None = None) -> dict[str,Any]:
                return await story_call(registry.entity_list, document_ids, kinds, query, cursor, limit)

            @mcp.tool(name="corpus_read", title="Read accepted passages and exact source evidence",
                description="Review up to 20 accepted source chunks and their exact printed page/region texts; "
                            "use source_regions page_id, region_id and original excerpt as proof "
                            "for grounded_candidates in story_extract(stage). No server-side model.",
                annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
            async def corpus_read(document_id: str, source_revision: Annotated[int,Field(ge=1)],
                    cursor: str | None = None, limit: Annotated[int,Field(ge=1,le=20)]=10) -> dict[str,Any]:
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

                @mcp.tool(name="story_extract", title="Extract evidence-backed stories from accepted books",
                    description="One model reads corpus_read page/region evidence and stages "
                                "up to 10 grounded_candidates per leased source batch with exact "
                                "page_id, region_id and original_excerpt. Server verifies all "
                                "proof, deduplicates, and atomically saves attributed candidates "
                                "with claim and evidence. No hidden LLM calls; legacy seed path "
                                "remains supported. Model must continue until the job is done.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True, destructive_hint=False, open_world_hint=False))
                async def story_extract(request: ExtractRequest, idempotency_key: Key) -> dict[str,Any]:
                    return await story_call(registry.extract, request, idempotency_key)

                @mcp.tool(name="story_reconcile", title="Reconcile source-backed episodes across books",
                    description="Durable next/start/enqueue/claim/stage/apply/cancel workflow. " +
                                "After book_ingest finalize or story_extract, next yields the next "
                                "accepted pending story and resumes unfinished runs without original chat history. "
                                "The same calling model searches story_search and knowledge_search, "
                                "compares exact original evidence via story_get evidence_page and "
                                "corpus_read, and saves a typed proposal. Application checks exact "
                                "story/source revisions, rights and reviewer intent before attaching "
                                "another source or linking episodes without automatic merge. "
                                "Never infer historical truth or independence from a mere quote match.",
                    annotations=ToolAnnotations(read_only_hint=False, idempotent_hint=True,
                                                destructive_hint=False, open_world_hint=False))
                async def story_reconcile(request: ReconcileRequest, idempotency_key: Key) -> dict[str,Any]:
                    from .story_reconcile import StoryReconciler
                    return await story_call(StoryReconciler(registry).dispatch, request, idempotency_key)

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

    if selected == 'kaggle_source':
        @mcp.tool(name='startup_status',title='Check Regional Knowledge startup',
            description='Check a versioned cold-start reference. Does not restart or replay search. Once ready, call knowledge_search/search again with your current access rights.',
            annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
        async def startup_status(startup_ref: str) -> StartupStatus:
            return await backend.startup_status(startup_ref,_principal())

    if profile == "live":
        @mcp.tool(
            name="knowledge_search",
            title="Search regional knowledge",
            description=(
                "Return a bounded evidence pack from accessible regional books and journals. "
                "When Kaggle is cold, returns MCP pending and a versioned startup ref; "
                "call startup_status, then issue a NEW search after ready."
            ),
            annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
        )
        async def knowledge_search(
            query: str,
            max_evidence: int = 3,
            main_job_id: str | None = None,
            document_ids: list[str] | None = None,
        ) -> EvidenceSearchOutput:
            return _require_hybrid_read(await read_telemetry.measure("knowledge_search", backend.search_evidence(
                query.strip(),
                _principal(),
                max_evidence=max(1, min(max_evidence, 5)),
                **({'main_job_id': main_job_id} if main_job_id else {}),
                **({'document_ids': document_ids} if document_ids is not None else {}),
            )))

        return mcp

    @mcp.tool(
        title="Search regional knowledge",
        description=(
            "Search accessible regional books and journals. Returns citable IDs and URLs. "
            "When Kaggle is cold, returns pending with startup ref; check startup_status "
            "and retry as a NEW read after ready. Use fetch for complete evidence."
        ),
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def search(query: str, main_job_id: str | None = None, aliases: list[dict[str,str]] | None = None, document_ids: list[str] | None = None) -> SearchOutput:
        options={}
        if document_ids is not None:options['document_ids']=document_ids
        if main_job_id:options['main_job_id']=main_job_id
        if aliases:options['aliases']=aliases
        return _require_hybrid_read(await read_telemetry.measure("search", backend.search(query.strip(), _principal(),**options)))

    @mcp.tool(
        title="Fetch regional evidence",
        description=(
            "Fetch evidence for an ID returned by search, including source provenance. "
            "A bounded source passage includes explicit offsets and next_id when more text "
            "or bibliography remains; fetch next_id until continuation_complete."
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

    @mcp.tool(title="Browse and correct the source catalog", description="List/find/get accessible books, journal issues and articles; cover returns a real cover/title page. Update corrects owned source descriptive metadata at expected_revision without reimport or changing source/ACL/vectors. For fragments label form explicitly; do not invent issue details. Article boundaries use book_ingest start with parent_id and exact region_ids. Empty query lists sources; cursor paginates.", annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True, open_world_hint=False))
    async def catalog(command: Literal['list','find','get','cover','update'] = 'list', query: str = '', document_id: str | None = None, kind: Literal['book','journal_issue','article'] | None = None, cursor: str | None = None, limit: Annotated[int, Field(ge=1,le=100)] = 20, metadata: CatalogUpdateInput | None = None) -> dict[str,Any] | list[TextContent | ImageContent]:
        method=getattr(backend,'catalog',None)
        if method is None:raise RuntimeError('SQLite catalog is not enabled')
        if command in ('get','cover','update') and not document_id:raise ValueError('document_id required')
        if command == 'update':
            if metadata is None:raise ValueError('metadata correction required')
            update = getattr(backend, 'catalog_update', None)
            if update is None:raise RuntimeError('catalog updates require SQLite authority')
            return await update(_principal(), document_id, metadata)
        if metadata is not None:raise ValueError('metadata is only accepted by update')
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
            "re-upload. If archival transport cannot be recovered and the owner has attached "
            "the original again, reprocess can accept file=... only for the EXACT registered "
            "SHA-256, format and physical page count. The same document and accepted revision "
            "are preserved; an altered edition is never substituted. "
            "For unchanged already accepted sources, rechunk creates a derived-only "
            "pending revision without fetching the original archive again; validate/finalize "
            "must prove exact source/geometry/illustration identity. For new crops the original "
            "verified PDF/DjVu archive is still mandatory. "
            "Resume existing state after interruptions; when the user explicitly "
            "asks to replace a still-pending reprocess, pass metadata.duplicate_policy=new_revision "
            "to start one newer archived-source revision instead of resuming the pending job. "
            "For a large model-authored batch, stage may use file=one UTF-8 JSON attachment "
            "instead of inline arrays. That JSON must contain ingestion_id, source_revision and "
            "the same stage fields, with at most 8 pages, 100 chunks and 12 stories, <=1 MiB. "
            "Never mix a stage file with inline metadata/arrays or put an original PDF in stage. "
            "File transport does not replace model source review or automatically mark pages reviewed. "
            "Finalization alone activates "
            "the revision. If next_action is wait, return control to the user and check status "
            "later instead of tight polling. Stage coherent semantic retrieval passages near "
            "256 encoder tokens (typically 800-1000 characters, broadly 700-1100 here), not "
            "mechanical page-sized chunks. Exact E5/BGE token counts are validated over the "
            "final augmented search material before finalization. During existing book_pages/stage " +
            "model review also extract story candidates by episode (not per retrieval chunk). " +
            "For every page review stories, dates, people, organizations, events, places, quantities " +
            "and source media in the SAME pass; set knowledge_reviewed=true when considered. " +
            "This also sets story_candidates_reviewed. In story_candidates send event_dates and " +
            "observations with zero-based evidence_indexes into that candidate's evidence_refs; " +
            "link selected graph nodes via entity_keys. Do not invent exact days or calendars. " +
            "Mark each staged page story_candidates_reviewed=true after reviewing it; an empty " +
            "story_candidates list is a valid explicit no-candidate review. Submit exact region excerpts " +
            "as story_candidates in the SAME stage call, without a separate prompt or LLM request. " +
            "They are saved when the accepted book revision activates; they are attributed candidates " +
            "and never automatically publish_ready. Review story_extraction and knowledge_extraction " +
            "in status; status(document_id) finds the owned book's latest checkpoint. " +
            "Source next_action=done is not completion of semantic extraction or retrieval acceptance. " +
            "For descriptive catalog corrections after activation, stage may instead accept metadata.catalog_update with expected_revision, exclusively without file/source/semantic payloads; it uses the same owner-authorized catalog update and preserves source/vectors. After activation stage otherwise accepts ONLY metadata (story_candidates/entity_candidates), " +
            "so a corrected candidate can clear its pending checkpoint without changing source/vectors. " +
            "Never restage accepted pages or chunks; use reprocess/rechunk for changed source layout. " +
            "After successful finalization, continue the SAME user goal by calling " +
            "story_reconcile(next, document_id=...) to claim accepted story candidates; " +
            "search existing story_search assertions and knowledge_search original sources, " +
            "stage evidence-grounded cross-book comparisons and apply permitted reviewed effects. " +
            "The server cannot semantically compare stories without this calling model; " +
            "persist pending frontier and report unfinished work explicitly rather than pretending " +
            "that acceptance of the book completed reconciliation. " +
            "During the SAME model page-review pass, use existing entity_candidates " +
            "for a bounded GraphBundle of source-backed poi_ref place/toponym/historical-address " +
            "nodes and person, organization, event, historical_thread nodes: " +
            "include exact_source_spelling and locator names where applicable " +
            "plus matching chunk_id/page_id/region_id/exact_quote. " +
            "Prefer distinct physical places, not a new node for every occurrence. " +
            "Do not guess historical geography, modern coordinates or canonical IDs. " +
            "An empty entity_candidates bundle is valid when there are no grounded " +
            "places. When the source activates (including delayed vector readiness), " +
            "its same-transaction geo outbox is claimable without a separate extraction " +
            "prompt or map availability. Private excerpts stay under RKB ACL; " +
            "a POI candidate is not a verified historical geometry."
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
        story_candidates: Annotated[list[StageStoryCandidateInput], Field(max_length=12)] | None = None,
        source_import: TargetSourceImportInput | None = None,
        telegram_original: TelegramOriginalInput | None = None,
    ) -> BookIngestOutput:
        if telegram_original is not None and (selected != 'kaggle_source' or command != 'start' or file is not None):
            raise ToolError('telegram_original_requires_target_start_without_attachment')
        payload: dict[str, Any] | None = None
        if metadata is not None:
            if metadata.source_import is not None:
                if source_import is not None:
                    raise ToolError('source_import_must_use_one_input_location')
                try:
                    source_import=TargetSourceImportInput.model_validate(metadata.source_import)
                except ValueError:
                    raise ToolError('invalid_source_import_input') from None
            payload = metadata.model_dump(mode="json", exclude_none=True,exclude={'source_import'})
            if metadata.catalog_update is not None:
                if command != 'stage' or file is not None or any(
                    value is not None for value in
                    (pages, chunks, poi_facts, poi_media_links, entity_candidates, story_candidates)
                ) or metadata.model_fields_set != {'catalog_update'}:
                    raise ToolError('catalog_update_requires_metadata_only_stage')
                payload = {'catalog_update': metadata.catalog_update.model_dump(
                    mode='json', exclude_unset=True)}

        if (
            pages is not None
            or chunks is not None
            or poi_facts is not None
            or poi_media_links is not None
            or entity_candidates is not None
            or story_candidates is not None
        ):
            payload = {
                "entity_candidates": entity_candidates.model_dump(mode="json") if entity_candidates else None,
                "story_candidates": [x.model_dump(mode="json") for x in (story_candidates or [])],
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
        principal = _principal()
        stage_file_sha = None
        if file is not None and command == 'stage':
            if payload is not None:
                raise ToolError('stage_file_cannot_mix_with_inline_payload')
            from .stage_file import read_stage_file, StageFileError
            try:
                payload, stage_file_sha = await read_stage_file(backend, principal, ingestion_id, file)
            except StageFileError as exc:
                raise ToolError(str(exc)) from exc
            except PermissionError as exc:
                raise ToolError('owned_ingestion_required') from exc
            except ValueError as exc:
                # Pydantic details can contain private source text. Surface only a
                # fixed actionable category; never stringify the supplied payload.
                raise ToolError('stage_file_invalid: use one bounded UTF-8 JSON batch for this exact ingestion and revision') from exc
            file = None
        elif file is not None and command not in {'start', 'reprocess'}:
            raise ToolError('file_only_supported_for_start_reprocess_or_stage')
        import_options = {}
        if source_import is not None:
            if selected != 'kaggle_source':
                raise ToolError('source_import_requires_configured_target_importer')
            import_options['source_import'] = source_import
        if stage_file_sha and selected == 'kaggle_source':
            import_options['stage_file_sha256'] = stage_file_sha
        if telegram_original is not None:
            file = telegram_original.locator()
        result = await backend.book_ingest(
            command=command,
            principal=principal,
            file=file,
            ingestion_id=ingestion_id,
            cursor=cursor,
            payload=payload,
            document_id=document_id,
            **import_options,
        )
        if stage_file_sha and selected != 'kaggle_source':
            result = result.model_copy(update={'message': result.message + '; stage_file_sha256=' + stage_file_sha})
        return await _attach_story_extraction(backend, principal, result)

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

    if not mcp._target_geo_available and not mcp._target_graph_available:
        @mcp.tool(title="Read one evidence-backed graph entity",description="One entity, authorized aliases/mentions and at most 20 one-hop relations with exact source evidence. No recursive traversal or graph dump.",annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def graph_fetch(entity_id:str|None=None,limit:int=20,discovery_job_id:str|None=None)->dict[str,Any]:
            if discovery_job_id is not None:return await GraphService(backend).job_read(_principal(),discovery_job_id,limit)
            if entity_id is None:raise ValueError("entity or discovery job required")
            return await GraphService(backend).read(_principal(),entity_id,limit)

    @mcp.tool(title="Find related entity evidence",description="One authorized entity context and bounded related evidence through the existing E5/BGE/lexical retrieval. Hits are identity candidates, not facts.",annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
    async def graph_related(entity_id:str,query:str|None=None,limit:int=8)->dict[str,Any]:
        return await GraphService(backend).related(_principal(),entity_id,query,limit)

    if not mcp._target_geo_available:
        @mcp.tool(name="poi_context",title="Historical knowledge linked to a Street Story POI",
            description="Read actor-authorized source passages, linked people/events and editorial stories "
                        "for an existing physical POI identity. Missing links are not proof of no history.",
            annotations=ToolAnnotations(read_only_hint=True,open_world_hint=False))
        async def poi_context(poi_ref:str,limit:Annotated[int,Field(ge=1,le=10)]=5)->dict[str,Any]:
            from .poi_registry_bridge import PoiRegistry
            return await PoiRegistry(backend).context(_principal(),poi_ref,limit=limit)

    @mcp.tool(name="poi_registry",title="Find, select or create Street Story POI for a source place",
        description="The calling model searches first, then explicitly selects an existing physical POI "
                    "or creates a candidate for a standing, ruined, commemorated or lost site. "
                    "Street Story alone owns POI IDs. One operation links the SAME RKB source entity "
                    "to the owner receipt. Requires accepted exact book evidence; no auto name merge.",
        annotations=ToolAnnotations(read_only_hint=False,destructive_hint=False,
                                    idempotent_hint=True,open_world_hint=False))
    async def poi_registry(
        action:Literal["search","get","select","create"],
        entity_id:str|None=None,query:str|None=None,poi_id:str|None=None,
        site_state:Literal["standing","ruins","memorial","lost_site","historic_site"]|None=None,
        site_context:str|None=None,model_reason:str|None=None,
        external_ids:dict[str,str]|None=None,aliases:list[str]|None=None,
        idempotency_key:str|None=None,limit:Annotated[int,Field(ge=1,le=12)]=8
    )->dict[str,Any]:
        from .poi_registry_bridge import PoiRegistry,PoiRegistryError
        if profile not in {"full","story_contributor","story_editor"}:
            raise PermissionError("POI mutations unavailable in reader profile")
        return await PoiRegistry(backend).act(_principal(),action,entity_id=entity_id,
            query=query,poi_id=poi_id,site_state=site_state,site_context=site_context,
            model_reason=model_reason,external_ids=external_ids,aliases=aliases,
            idempotency_key=idempotency_key,limit=limit)

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
        return _book_page_tool_result(ingestion_id, batch).content

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
        name="profile",
        title="Connected knowledge profile",
        description="Return the stable connected Regional Knowledge account identity.",
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
        meta={"openai/profile": True},
    )
    def connected_profile() -> ProfileOutput:
        # Distinct Python name: don't overwrite build_server(profile=...) captured
        # by capability authorization for other MCP tools.
        return ProfileOutput(id=_principal().subject)

    return mcp


def main(*,target_startup=None,projects_hub_receiver=None,expert_slack_ingress=None) -> None:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("regional_knowledge.oauth_provider").setLevel(logging.INFO)
    logging.getLogger("regional_knowledge.reads").setLevel(logging.INFO)
    profile = os.getenv("RKB_MCP_PROFILE", "full").strip().lower()
    if profile not in {"full", "live", "story_reader", "story_contributor", "story_editor"}:
        raise RuntimeError("RKB_MCP_PROFILE must be full, live or an enabled story capability bundle")
    if target_startup is None and os.getenv('RKB_BACKEND_PROFILE','').strip()=='kaggle_source':
        from .kaggle_source_owners import configured_source_service
        target_startup=configured_source_service()
    reader_config=os.getenv('RKB_PROJECTS_HUB_READER_CONFIG','').strip()
    if reader_config and projects_hub_receiver is None:
        if target_startup is None:
            raise RuntimeError('PH receiver requires explicit target composition')
        from .projects_hub_reader import ProjectsHubReader
        projects_hub_receiver=ProjectsHubReader.from_file(reader_config,backend=target_startup.backend)
    slack_config=os.getenv('RKB_EXPERT_SLACK_INGRESS_CONFIG','').strip()
    if slack_config:
        if expert_slack_ingress is not None:
            raise RuntimeError('Slack ingress has more than one configured owner')
        if target_startup is None:
            raise RuntimeError('Slack ingress requires the existing target startup')
        from .kaggle_source_owners import configured_expert_slack_ingress
        expert_slack_ingress=configured_expert_slack_ingress(target_startup,slack_config)
    build_server(profile=profile,target_startup=target_startup,projects_hub_receiver=projects_hub_receiver,
        expert_slack_ingress=expert_slack_ingress).run(
        transport="streamable-http",
        stateless_http=True,
        json_response=True,
        transport_security=_transport_security(
            target_startup.config.oauth_provider.resource if target_startup is not None else None),
    )


if __name__ == "__main__":
    main()
