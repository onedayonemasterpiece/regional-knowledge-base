"""Native-safe retained recovery capability; no IAM, journal or provider client."""
from dataclasses import dataclass


@dataclass(frozen=True,eq=False)
class VerifiedInitializedDatabase:
    _factory: object
    _candidate: object
    database: object
    identity: tuple
    manifest: object
    journal_head: object
    applied_sequence: int
    control: bytes
    _binding: object

    async def verify_for(self,database_spec,run_id,launch_nonce,runtime_incarnation):
        return await self._factory.verify_initialized(self,database_spec,(run_id,launch_nonce,runtime_incarnation))

    async def aclose(self):
        self._factory._capabilities.discard(self)
        await self._candidate.aclose()
        self._factory._owned_candidates.discard(self._candidate)
        for reference,value in tuple(getattr(self._factory,'_references',{}).items()):
            if value is self:del self._factory._references[reference]
        for session,value in tuple(getattr(self._factory,'_completed',{}).items()):
            if value is self:
                del self._factory._completed[session]
                for operation,(_,retained) in tuple(self._factory._operations.items()):
                    if retained is session:del self._factory._operations[operation]
