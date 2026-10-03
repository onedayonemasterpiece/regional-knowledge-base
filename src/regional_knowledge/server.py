from __future__ import annotations

import json
import os
from typing import Any, Literal

from mcp.server.mcpserver import Image, MCPServer
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.settings import (
    AuthSettings,
    ClientRegistrationOptions,
    RevocationOptions,
)
from mcp.types import ImageContent, TextContent, ToolAnnotations
from pydantic import AnyHttpUrl

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
        ) -> EvidenceSearchOutput:
            return await backend.search_evidence(
                query.strip(),
                _principal(),
                max_evidence=max(1, min(max_evidence, 5)),
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
    async def search(query: str) -> SearchOutput:
        return await backend.search(query.strip(), _principal())

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

    @mcp.tool(
        title="Add or continue a book",
        description=(
            "Start or continue a resumable book/journal ingestion. Use start with an "
            "attached PDF, then book_pages plus stage/validate/finalize/status as needed. "
            "Finalization is the only step that makes an indexed revision active."
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
    ) -> BookIngestOutput:
        payload: dict[str, Any] | None = None
        if metadata is not None:
            payload = metadata.model_dump(mode="json", exclude_none=True)
        if (
            pages is not None
            or chunks is not None
            or poi_facts is not None
            or poi_media_links is not None
        ):
            payload = {
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

    @mcp.tool(
        title="Read staged book pages",
        description=(
            "Return the next small batch of staged source pages as model-visible image "
            "content plus page metadata. Use this only while parsing an existing ingestion."
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
    )


if __name__ == "__main__":
    main()
