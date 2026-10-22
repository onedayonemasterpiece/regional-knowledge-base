"""One deployment-pinned, ordered schema12 -> schema14 native source upgrade.

SQL013 and SQL014 install together in one transaction before the existing
accepted transition is applied. No genesis, wire SQL, new journal or provider.
The caller retains old handlers and owns admission/history-closure integration.
"""
from dataclasses import dataclass,asdict
from pathlib import Path
import hashlib
import json
from uuid import UUID

from .kaggle_apply import HandlerRegistry, HandlerSpec, MigrationTransition, PgFunction
from .recovery_journal import ExactCommand
from .geo_manifest_commands import GEO_V2_REVISIONS, GEO_DEPENDENCY_MIGRATION
from .source_activation import ACTIVATION_REVISIONS

ROUTE='source.native.upgrade.v1'
MIGRATION='native-source-stage-activation-v1'
ARTIFACTS=('013_source_stage.sql','014_source_stage_activation.sql')
CHECK='check_native_source_upgrade_v1'
APPLY='apply_native_source_upgrade_v1'
SIGNATURE='(bytea,jsonb,jsonb,text)'
PRECONDITIONS=frozenset({'schema','function','executor','migration','installer_person_id'})


def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(v):return hashlib.sha256(v).hexdigest()
def _hash(v):
    if type(v) is not str or len(v)!=64 or set(v)-set('0123456789abcdef'):
        raise ValueError('exact native source upgrade pin required')
    return v


@dataclass(frozen=True)
class NativeSourceUpgradePins:
    sql013_sha256:str
    sql014_sha256:str
    transition_check_sha256:str
    transition_apply_sha256:str

    def __post_init__(self):
        for name in self.__dataclass_fields__:_hash(getattr(self,name))

    def wire(self):return asdict(self)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__):
            raise ValueError("exact native source upgrade deployment pins required")
        return cls(**value)

    @property
    def manifest(self):
        return dict(codec='rkb.native-source-upgrade.v1',artifacts=[
            dict(name=ARTIFACTS[0],sha256=self.sql013_sha256),
            dict(name=ARTIFACTS[1],sha256=self.sql014_sha256)])

    @property
    def artifact_sha256(self):return sha(canonical(self.manifest))


def native_source_upgrade_handler(pins):
    if type(pins) is not NativeSourceUpgradePins:raise TypeError('fixed upgrade pins required')
    return native_source_transition_handler(artifact_sha256=pins.artifact_sha256,
        check_sha256=pins.transition_check_sha256,apply_sha256=pins.transition_apply_sha256)


def native_source_transition_handler(*,artifact_sha256,check_sha256,apply_sha256):
    return HandlerSpec('schema_transition',ROUTE,GEO_V2_REVISIONS,
        PgFunction(CHECK,_hash(check_sha256)),PgFunction(APPLY,_hash(apply_sha256)),
        PRECONDITIONS,MigrationTransition(MIGRATION,_hash(artifact_sha256),GEO_DEPENDENCY_MIGRATION,
            ACTIVATION_REVISIONS,preserve_read_frontier=True))


def validate_native_source_upgrade(command,*,pins=None,artifact_sha256=None):
    if type(command) is not ExactCommand or (pins is not None and type(pins) is not NativeSourceUpgradePins):
        raise ValueError('exact ordered native source upgrade command required')
    artifact_sha256=_hash(pins.artifact_sha256 if pins is not None else artifact_sha256)
    expected=dict(schema=GEO_V2_REVISIONS.schema,function=GEO_V2_REVISIONS.function,
        executor=GEO_V2_REVISIONS.executor,migration=GEO_DEPENDENCY_MIGRATION,
        installer_person_id=command.person_id)
    if (type(command) is not ExactCommand or command.kind!='schema_transition' or command.route!=ROUTE
        or (command.schema_revision,command.function_revision,command.executor_revision)!=GEO_V2_REVISIONS.wire()
        or command.ids!={} or command.expected_revisions!=expected
        or str(UUID(command.person_id))!=command.person_id
        or command.payload!=canonical(dict(migration_id=MIGRATION,artifact_sha256=artifact_sha256,
            predecessor=GEO_DEPENDENCY_MIGRATION))):
        raise ValueError('exact ordered native source upgrade command required')
    return json.loads(command.payload)


def resolved_native_source_upgrade(*,pins,**identity):
    command=ExactCommand(**identity,kind='schema_transition',route=ROUTE,
        schema_revision=GEO_V2_REVISIONS.schema,function_revision=GEO_V2_REVISIONS.function,
        executor_revision=GEO_V2_REVISIONS.executor,ids={},expected_revisions=dict(
            schema=GEO_V2_REVISIONS.schema,function=GEO_V2_REVISIONS.function,
            executor=GEO_V2_REVISIONS.executor,migration=GEO_DEPENDENCY_MIGRATION,
            installer_person_id=identity['person_id']),payload=canonical(dict(migration_id=MIGRATION,
                artifact_sha256=pins.artifact_sha256,predecessor=GEO_DEPENDENCY_MIGRATION)))
    validate_native_source_upgrade(command,pins=pins)
    return command


def transition_sql(pins):
    """Fixed deployment definitions; only verified digest literals vary.

    The canonical ordered manifest is returned in the accepted outcome, so both
    artifact hashes are retained in the existing applied_ops/journal ledger.
    """
    manifest=canonical(pins.manifest).decode()
    payload=canonical(dict(migration_id=MIGRATION,artifact_sha256=pins.artifact_sha256,
        predecessor=GEO_DEPENDENCY_MIGRATION)).decode()
    return f"""
CREATE FUNCTION rkb_v4.{CHECK}(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE s rkb_v4.recovery_state;
BEGIN
 SELECT * INTO STRICT s FROM rkb_v4.recovery_state WHERE singleton;
 RETURN p=convert_to('{payload}','UTF8') AND ids='{{}}'::jsonb
 AND expected ?& ARRAY['schema','function','executor','migration','installer_person_id']
 AND expected-ARRAY['schema','function','executor','migration','installer_person_id']='{{}}'::jsonb
 AND expected->>'schema'='{GEO_V2_REVISIONS.schema}' AND s.schema_revision=expected->>'schema'
 AND expected->>'function'='{GEO_V2_REVISIONS.function}' AND s.function_revision=expected->>'function'
 AND expected->>'executor'='{GEO_V2_REVISIONS.executor}' AND s.executor_revision=expected->>'executor'
 AND expected->>'migration'='{GEO_DEPENDENCY_MIGRATION}' AND s.last_migration_id=expected->>'migration'
 AND (expected->>'installer_person_id')::uuid::text=expected->>'installer_person_id'
 AND to_regclass('rkb_v4.source_stage_batches') IS NOT NULL
 AND to_regclass('rkb_v4.source_image_origins') IS NOT NULL;
EXCEPTION WHEN data_exception OR no_data_found THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.{APPLY}(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.{CHECK}(p,ids,expected,logical_time) THEN RAISE EXCEPTION 'native source upgrade precondition changed';END IF;
 RETURN convert_to('{manifest}','UTF8');
END $$;
REVOKE ALL ON FUNCTION rkb_v4.{CHECK}{SIGNATURE},rkb_v4.{APPLY}{SIGNATURE} FROM PUBLIC;
"""


def ordered_migration_prepare(*,sql_root,registry,database_generation,entries):
    hooks={}
    if any(h.route=='geo.dependencies.upgrade.v2' for h in registry.handlers.values()):
        from .kaggle_geo_upgrade import NativeGeoMigration
        hooks['geo.dependencies.upgrade.v2']=NativeGeoMigration(sql_root=sql_root,
            registry=registry,database_generation=database_generation).prepare
    native=[e for e in entries if e.get('route')==ROUTE]
    if native:
        if len(native)!=1:raise ValueError('one exact native source migration required')
        pins=NativeSourceUpgradePins.from_wire(native[0].get('native_source_upgrade'))
        hooks[ROUTE]=NativeSourceMigration(sql_root=sql_root,registry=registry,
            database_generation=database_generation,pins=pins).prepare
    if not hooks:return None
    async def prepare(connection,command,handler):
        if handler.transition is None:return
        hook=hooks.get(command.route)
        if hook is None:raise ValueError('unregistered ordered schema transition')
        await hook(connection,command,handler)
    return prepare


async def transition_definitions(connection):
    values=[]
    for name in (CHECK,APPLY):
        row=await(await connection.execute('SELECT pg_get_functiondef(to_regprocedure(%s))',
            ('rkb_v4.'+name+SIGNATURE,))).fetchone()
        values.append(None if row[0] is None else sha(row[0].encode()))
    return tuple(values)


class NativeSourceMigration:
    def __init__(self,*,sql_root,registry,database_generation,pins):
        if type(registry) is not HandlerRegistry or type(pins) is not NativeSourceUpgradePins:
            raise TypeError('retained registry and deployment pins required')
        self.handler=native_source_upgrade_handler(pins)
        if registry.handlers.get(self.handler.key)!=self.handler:
            raise ValueError('exact native source transition must be retained')
        self.root=Path(sql_root);self.pins=pins;self.database_generation=database_generation

    async def prepare(self,connection,command,handler):
        if handler.transition is None:return
        if handler!=self.handler:raise ValueError('native source transition not registered')
        validate_native_source_upgrade(command,pins=self.pins)
        artifacts=[]
        for name,wanted in zip(ARTIFACTS,(self.pins.sql013_sha256,self.pins.sql014_sha256),strict=True):
            path=self.root/name
            if path.is_symlink() or not path.is_file() or not 1<=path.stat().st_size<=256*1024:
                raise ValueError('native source artifact unavailable')
            raw=path.read_bytes()
            if sha(raw)!=wanted:raise ValueError('native source artifact pin changed')
            artifacts.append(raw)
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='5000ms'")
            state=await(await connection.execute('SELECT database_generation,schema_revision,function_revision,executor_revision,applied_seq,last_migration_id,last_migration_hash FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE')).fetchone()
            if state is None or state[0]!=self.database_generation:raise ValueError('native source migration database changed')
            if tuple(state[1:4])==GEO_V2_REVISIONS.wire():
                if state[4]!=command.sequence-1 or state[5]!=GEO_DEPENDENCY_MIGRATION:
                    raise ValueError('native source migration predecessor changed')
            elif (tuple(state[1:4])!=ACTIVATION_REVISIONS.wire() or state[4]<command.sequence
                or tuple(state[5:7])!=(MIGRATION,self.pins.artifact_sha256)):
                raise ValueError('native source migration target changed')
            observed=await transition_definitions(connection)
            if observed==(None,None):
                if tuple(state[1:4])!=GEO_V2_REVISIONS.wire():raise ValueError('native source target schema incomplete')
                # Both artifacts and their completion marker definitions commit
                # together. Existing CREATE statements reject partial/conflicting
                # installs; SQL014 failure rolls SQL013 back. Full schema/catalog
                # verification remains the existing native readiness probe.
                for raw in artifacts:await connection.execute(raw.decode(),prepare=False)
                await connection.execute(transition_sql(self.pins),prepare=False)
                observed=await transition_definitions(connection)
            if observed!=(self.pins.transition_check_sha256,self.pins.transition_apply_sha256):
                raise ValueError('native source transition definition readback differs')
