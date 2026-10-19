"""Task-local read policy for the existing SQLite source adapters.

Only the authenticated executor constructs a scope. No HTTP/header field from
an end user selects it. Async tasks and to_thread inherit their own ContextVar;
no mutable global actor is shared between simultaneous requests.
"""
from contextvars import ContextVar
from dataclasses import dataclass

from .contracts import PUBLIC_RIGHTS


def public_document(document):
    return bool(document and document.get("content_visibility")=="public"
        and document.get("rights_status") in PUBLIC_RIGHTS
        and (document.get("rights_evidence") or {}).get("public_distribution") is True
        and document.get("rights_policy_version"))


@dataclass(frozen=True)
class ExpertSourceScope:
    participants:object
    authority:object

    def validate(self,db,actor,*,write=False):
        if write: raise PermissionError("expert_source_read_only")
        if actor is not None and actor!=self.authority.principal.subject:
            raise PermissionError("expert_source_actor_mismatch")
        self.participants.require(self.authority,"public_search",connection=db)


current_source_scope=ContextVar("rkb_expert_source_scope",default=None)
