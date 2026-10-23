"""Owner-only Kaggle MCP adapter for the existing Dev Participants authority.

Only an authenticated owner-personal MCP request can reach the existing
Participants get/list/audit/change/operation_status methods. This module
creates no users, grants, identities, storage, provider sessions or PG rights.
"""
from __future__ import annotations

import asyncio
from typing import Annotated, Literal
from uuid import UUID

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from .participant_contracts import ParticipantGrant, ParticipantUpsert
from .participants import ParticipantError, Participants, RequestAuthority
from .read_authority import ReadAuthorityError


def register_participant_tools(mcp, startup, principal):
    """Register only on a Kaggle target with the existing OAuth/Participants pair."""
    participants = startup.config.participants
    authority = startup.authority
    if (type(participants) is not Participants
            or authority.participants is not participants
            or startup.config.oauth_provider.personal_policy.participants is not participants):
        raise TypeError('current target Participants owner required')

    @mcp.tool(
        name='participants_manage',
        title='Owner administration of existing RKB participants',
        description='Owner-personal only. Read/get/list/audit or change current participant '
            'categories with ParticipantUpsert, and explicitly grant/revoke only '
            'scan_proof_read with ParticipantGrant. Changes use existing revision '
            'compare-and-set, audit events and idempotent operation_key. '
            'operation_status retrieves the same owner operation after an uncertain reply. '
            'Categories alone do not grant scan access; revocation is not restored '
            'by profile updates. This does not grant access to an original, private '
            'Case, scan or stripe without its independent current source policy.',
        annotations=ToolAnnotations(read_only_hint=False, open_world_hint=False),
    )
    async def participant_tool(
        action: Literal['get', 'list', 'audit', 'change', 'operation_status'],
        person_id: UUID | None = None,
        after_id: UUID | None = None,
        after_revision: Annotated[int, Field(ge=0)] = 0,
        limit: Annotated[int, Field(ge=1, le=100)] = 20,
        expected_revision: Annotated[int | None, Field(ge=0)] = None,
        command: ParticipantUpsert | ParticipantGrant | None = None,
        operation_key: Annotated[str | None, Field(min_length=8, max_length=128)] = None,
    ) -> dict:
        # The bearer comes from MCP middleware, never an argument. Resolve its
        # *current* OAuth family and owner route before inspecting any profile.
        caller = principal()
        try:
            verified = await authority.resolve_oauth(
                caller.access_token, destination=startup.backend.destination)
        except (ReadAuthorityError, ValueError, TypeError) as exc:
            raise ToolError('participants_owner_required') from None
        try:
            await authority.recheck(verified)
            if (verified.route != 'owner' or verified.public_only
                    or verified.person_id != participants.owner
                    or verified.person_id != caller.subject):
                raise ToolError('participants_owner_required')

            # Internal-only authority is constructed after validating the
            # actual bearer; the client cannot inject route or person identity.
            context = RequestAuthority(caller, 'owner')
            if action == 'get':
                if (person_id is None or after_id is not None or after_revision
                        or command is not None or expected_revision is not None
                        or operation_key is not None):
                    raise ToolError('invalid_participants_get')
                result = await asyncio.to_thread(participants.get, context, str(person_id))
            elif action == 'list':
                if (person_id is not None or after_revision or command is not None
                        or expected_revision is not None or operation_key is not None):
                    raise ToolError('invalid_participants_list')
                result = await asyncio.to_thread(participants.list, context,
                    after_id=str(after_id) if after_id is not None else '', limit=limit)
            elif action == 'audit':
                if (person_id is None or after_id is not None or command is not None
                        or expected_revision is not None or operation_key is not None):
                    raise ToolError('invalid_participants_audit')
                result = await asyncio.to_thread(participants.audit, context,
                    str(person_id), after_revision=after_revision, limit=limit)
            elif action == 'operation_status':
                if (person_id is not None or after_id is not None or after_revision
                        or command is not None or expected_revision is not None
                        or not operation_key):
                    raise ToolError('invalid_participants_operation_status')
                result = await asyncio.to_thread(
                    participants.operation_status, context, operation_key)
            else:
                if (person_id is None or expected_revision is None
                        or command is None or not operation_key
                        or after_id is not None or after_revision):
                    raise ToolError('invalid_participants_change')
                if (type(command) not in (ParticipantUpsert, ParticipantGrant)
                        or isinstance(command, ParticipantGrant)
                        and command.capability != 'scan_proof_read'):
                    raise ToolError('participants_change_out_of_scope')
                # The existing Participants.change itself checks owner/admin,
                # active canonical subject, revision, request hash, audit and
                # duplicate operation_key in its BEGIN IMMEDIATE transaction.
                result = await asyncio.to_thread(participants.change, context,
                    str(person_id), expected_revision, command, operation_key)
                # No post-write recheck: an owner changing its own profile can
                # invalidate the old authority revision after a *committed*
                # change. Returning an error then would invite an unsafe retry.
                return result

            # For sensitive reads, prevent issuance after an intervening
            # change of the authenticated owner's authority/OAuth family.
            await authority.recheck(verified)
            return result
        except ReadAuthorityError:
            raise ToolError('participants_owner_required') from None
        except ParticipantError as exc:
            raise ToolError(exc.code) from None
        finally:
            authority.release(verified)
