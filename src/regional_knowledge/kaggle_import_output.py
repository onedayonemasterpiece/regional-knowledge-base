"""Current owner/import fence at the existing MCP JSON issuance boundary."""
from dataclasses import dataclass

from .read_issuance import retain_read_output


@dataclass(frozen=True, eq=False)
class ImportOutput:
    result: object
    ticket: object


class OwnerImportOutputGate:
    def __init__(self, workflow):
        self.workflow = workflow
        self._tickets = {}

    async def retain(self, result, authority, context, binding, *, render=None):
        await self.workflow._check(authority, context, binding)
        ticket = object()
        self._tickets[ticket] = (result, authority, context, binding)
        gated = ImportOutput(result, ticket)
        try:
            retained = retain_read_output(self, gated, render or (lambda value: value))
        except BaseException:
            self.release(ticket)
            raise
        if not retained:
            if not self.issuance_current(ticket):
                self.release(ticket)
                raise PermissionError('current owner import authority required')
            self.release(ticket)
        return retained

    async def refresh(self, gated):
        value = self._tickets.get(gated.ticket)
        if value is None or value[0] is not gated.result:
            raise PermissionError('unknown original output')
        await self.workflow._check(*value[1:])
        return gated

    def issuance_current(self, ticket):
        value = self._tickets.get(ticket)
        if value is None:
            return False
        _, authority, context, binding = value
        resolver = self.workflow.read_authority
        try:
            with resolver._contexts_lock:
                if resolver._contexts.get(authority.context_ref) is not authority:
                    return False
            if authority.expires_at <= resolver.clock() or not self.workflow._binding_current(binding):
                return False
            with resolver._read() as db:
                current = resolver._state(context.principal, context.route,
                    family_ref=authority.oauth_family_ref, connection=db)
                self.workflow.participants.require(context, 'import', connection=db)
            return current.revision == authority.auth_revision
        except Exception:
            return False

    def release(self, ticket):
        value = self._tickets.pop(ticket, None)
        if value is not None:
            self.workflow.read_authority.release(value[1])
