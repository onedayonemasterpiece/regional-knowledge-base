"""One resource authentication and per-call authority for personal and service use."""
import asyncio

from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken

from .expert_requests import ExpertRequestError, ServiceCredentials
from .oauth_provider import KNOWLEDGE_SCOPE


EXECUTOR_TOOLS = frozenset({"expert_request_delivery", "expert_research"})
SOURCE_TOOLS = frozenset({"search", "fetch", "knowledge_search", "illustration_fetch", "book_find", "source_proof"})
PERSONAL_TOOLS = SOURCE_TOOLS | frozenset({"disagreement_participants", "disagreement_get", "disagreement_edit", "disagreement_contribute", "disagreement_conclusion", "profile", "research_request"})


def current_service():
    token = get_access_token()
    claims = token.claims or {} if token else {}
    service_id = claims.get("rkb_service_id")
    if not isinstance(service_id, str) or token.subject is not None:
        raise ExpertRequestError("service_denied")
    # Token is a private server context; never copy it into tool output.
    prefix, secret = token.token.split(":", 1)
    if prefix != service_id:
        raise ExpertRequestError("service_denied")
    return ServiceCredentials(service_id, secret)


def is_service():
    token = get_access_token()
    return bool(token and (token.claims or {}).get("rkb_service_id"))


class ResourceTokenVerifier:
    """Preserve the existing personal OAuth verifier; add revocable machine auth.

    Machine auth has no human subject. Only a saved request can supply one.
    Both kinds authenticate at the existing /mcp endpoint and resource.
    """
    def __init__(self, personal, requests, resource):
        self.personal, self.requests, self.resource = personal, requests, resource

    async def verify_token(self, token):
        if ":" not in token:
            return await self.personal.verify_token(token) if self.personal else None
        try:
            service_id, secret = token.split(":", 1)
            if not 5 <= len(service_id) <= 80 or not 32 <= len(secret) <= 256:
                return None
            credentials = ServiceCredentials(service_id, secret)
            def check():
                with self.requests.corpus.connect() as db:
                    return self.requests._service(db, credentials, "executor")
            await asyncio.to_thread(check)
        except (ExpertRequestError, ValueError):
            return None
        return AccessToken(token=token, client_id=service_id, subject=None,
            scopes=[KNOWLEDGE_SCOPE], resource=self.resource,
            claims={"rkb_service_id": service_id})
