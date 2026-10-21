"""Install one reviewed GEO migration beside PG, through its accepted command.

The artifact path comes from the verified notebook bundle, never from WSS or a
user request. Existing schema11 functions and accepted outcomes stay intact.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass,asdict
from pathlib import Path

from .geo_manifest_commands import (GEO_REVISIONS,GEO_V2_REVISIONS,GEO_DEPENDENCY_MIGRATION,
    validate_geo_dependency_transition)
from .kaggle_apply import HandlerRegistry

_LOG=logging.getLogger(__name__)
ARTIFACT_NAME='012_geo_dependency_bound.sql'


@dataclass(frozen=True)
class GeoUpgradePins:
    artifact_sha256:str
    transition_check_sha256:str
    transition_apply_sha256:str
    geo_check_sha256:str
    geo_apply_sha256:str
    origin_catalog_sha256:str
    origin_acl_sha256:str

    def __post_init__(self):
        for value in asdict(self).values():
            if type(value) is not str or len(value)!=64 or set(value)-set('0123456789abcdef'):
                raise ValueError('exact reviewed geographic upgrade pins required')

    def wire(self):return asdict(self)

    def origin_closure(self,registry,routes):
        from .geo_manifest_commands import canonical
        old=tuple(h.fingerprint for h in registry.handlers.values()
            if h.revisions==GEO_REVISIONS and h.kind=='domain_command')
        if not old:raise GeoUpgradeRejected('geo_upgrade_original_registry_required')
        return dict(catalog_sha256=self.origin_catalog_sha256,acl_sha256=self.origin_acl_sha256,
            registry_sha256=hashlib.sha256(canonical(sorted(old))).hexdigest(),routes=sorted(routes))

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__):
            raise ValueError('exact geographic upgrade deployment configuration required')
        return cls(**value)

    def registry(self,original):
        from .geo_manifest_commands import geo_dependency_transition_handler
        from .kaggle_domain_bindings import CURRENT_ROUTES,current_handler
        handlers=dict(original.handlers)
        for old in tuple(handlers.values()):
            if old.kind!='domain_command' or old.revisions!=GEO_REVISIONS or old.route not in CURRENT_ROUTES:continue
            new=current_handler(old.route,revisions=GEO_V2_REVISIONS,
                check_sha256=self.geo_check_sha256 if old.route=='geo.manifest.install.v1' else old.check.definition_sha256,
                apply_sha256=self.geo_apply_sha256 if old.route=='geo.manifest.install.v1' else old.apply.definition_sha256)
            if new.key in handlers and handlers[new.key]!=new:raise GeoUpgradeRejected('geo_upgrade_registry_changed')
            handlers[new.key]=new
        migration=geo_dependency_transition_handler(artifact_sha256=self.artifact_sha256,
            check_sha256=self.transition_check_sha256,apply_sha256=self.transition_apply_sha256)
        if migration.key in handlers and handlers[migration.key]!=migration:raise GeoUpgradeRejected('geo_upgrade_registry_changed')
        handlers[migration.key]=migration
        return HandlerRegistry(tuple(handlers.values()))


class GeoUpgradeRejected(ValueError):
    """Fixed operator diagnostic; never SQL, credentials or document content."""


class NativeGeoMigration:
    """Fixed pre-apply hook for the existing source-command executor."""
    def __init__(self,*,sql_root,registry,database_generation):
        if type(registry) is not HandlerRegistry or not isinstance(database_generation,str):
            raise TypeError('trusted existing native registry and database required')
        transitions=[h for h in registry.handlers.values() if h.route=='geo.dependencies.upgrade.v2']
        upgraded=[h for h in registry.handlers.values() if h.route=='geo.manifest.install.v1' and h.revisions==GEO_V2_REVISIONS]
        original=[h for h in registry.handlers.values() if h.route=='geo.manifest.install.v1' and h.revisions==GEO_REVISIONS]
        if len(transitions)!=1 or len(upgraded)!=1 or len(original)!=1:
            raise GeoUpgradeRejected('geo_upgrade_exact_registry_required')
        self.handler=transitions[0]
        transition=self.handler.transition
        if (transition is None or transition.migration_id!=GEO_DEPENDENCY_MIGRATION
                or transition.predecessor is not None or transition.target!=GEO_V2_REVISIONS
                or not transition.preserve_read_frontier):
            raise GeoUpgradeRejected('geo_upgrade_exact_transition_required')
        self.artifact_sha256=transition.artifact_sha256
        self.path=Path(sql_root)/ARTIFACT_NAME
        self.database_generation=database_generation
        self.functions=tuple(f for h in (self.handler,upgraded[0]) for f in (h.check,h.apply))
        self.original_functions=(original[0].check,original[0].apply)

    @staticmethod
    async def _definitions(connection,functions):
        values=[]
        for function in functions:
            row=await(await connection.execute('SELECT pg_get_functiondef(to_regprocedure(%s))',
                (function.signature,))).fetchone()
            values.append(None if row[0] is None else hashlib.sha256(row[0].encode()).hexdigest())
        return tuple(values)

    async def prepare(self,connection,command,handler):
        if handler.transition is None:return
        if handler!=self.handler:
            raise GeoUpgradeRejected('geo_upgrade_unregistered_transition')
        validate_geo_dependency_transition(command,artifact_sha256=self.artifact_sha256)
        # The fixed artifact is part of the same independently verified source
        # bundle. Read a bounded regular file; no supplied path or SQL reaches PG.
        if self.path.is_symlink() or not self.path.is_file() or not 1<=self.path.stat().st_size<=64*1024:
            raise GeoUpgradeRejected('geo_upgrade_artifact_unavailable')
        artifact=self.path.read_bytes()
        if hashlib.sha256(artifact).hexdigest()!=self.artifact_sha256:
            raise GeoUpgradeRejected('geo_upgrade_artifact_hash')
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='5000ms'")
            state=await(await connection.execute('''SELECT database_generation,schema_revision,function_revision,
                executor_revision,applied_seq,last_migration_id,last_migration_hash
                FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE''')).fetchone()
            if state is None or state[0]!=self.database_generation:
                raise GeoUpgradeRejected('geo_upgrade_database_changed')
            before=tuple(state[1:4])
            if before==GEO_REVISIONS.wire():
                if state[4]!=command.sequence-1 or state[5] is not None:
                    raise GeoUpgradeRejected('geo_upgrade_predecessor_changed')
            elif (before!=GEO_V2_REVISIONS.wire() or state[4]<command.sequence
                    or state[5:7]!=(GEO_DEPENDENCY_MIGRATION,self.artifact_sha256)):
                raise GeoUpgradeRejected('geo_upgrade_target_changed')
            old=await self._definitions(connection,self.original_functions)
            if old!=tuple(f.definition_sha256 for f in self.original_functions):
                raise GeoUpgradeRejected('geo_upgrade_original_functions_changed')
            observed=await self._definitions(connection,self.functions)
            if all(value is None for value in observed):
                await connection.execute(artifact.decode('utf-8'),prepare=False)
                observed=await self._definitions(connection,self.functions)
            if (observed!=tuple(f.definition_sha256 for f in self.functions)
                    or await self._definitions(connection,self.original_functions)!=old):
                raise GeoUpgradeRejected('geo_upgrade_definition_readback')
        _LOG.info('geo_migration state=installed migration_id=%s artifact_sha256=%s sequence=%s',
            GEO_DEPENDENCY_MIGRATION,self.artifact_sha256,command.sequence)
