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
