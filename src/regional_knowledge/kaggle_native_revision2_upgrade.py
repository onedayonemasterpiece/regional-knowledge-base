"""Additive pinned SQL015/016 -> SQL017 native successor transition.

Only the already-supplied, generation-fenced owned PG connection may install
these exact source artifacts during a separately authorized transition. This
module never connects to PG, Kaggle, Telegram, or provider on its own.
"""
from __future__ import annotations

from dataclasses import dataclass,asdict
from pathlib import Path
import hashlib
import json
from uuid import UUID

from .kaggle_apply import HandlerRegistry,HandlerSpec,MigrationTransition,PgFunction,Revisions
from .recovery_journal import ExactCommand
from .source_activation import ACTIVATION_REVISIONS

REVISION2_REVISIONS=Revisions('rkb-v4-sql001-017-v1',
    'rkb-v4-native-source-revision2-v1','rkb-python-apply-native-source-revision2-v1')
ROUTE='source.revision2.upgrade.v1'
MIGRATION='native-source-revision2-v1'
PREDECESSOR='native-source-stage-activation-v1'
ARTIFACTS=('015_source_illustrations.sql','016_story_reconcile.sql','017_source_revision2.sql')
CHECK='check_native_revision2_upgrade_v1'
APPLY='apply_native_revision2_upgrade_v1'
SIGNATURE='(bytea,jsonb,jsonb,text)'
PRECONDITIONS=frozenset({'schema','function','executor','migration','installer_person_id'})


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def sha(value):return hashlib.sha256(value).hexdigest()


def _hash(value):
    if type(value) is not str or len(value)!=64 or set(value)-set('0123456789abcdef'):
        raise ValueError('exact native revision2 immutable SQL pin required')
    return value


@dataclass(frozen=True)
class NativeRevision2UpgradePins:
    sql015_sha256:str
    sql016_sha256:str
    sql017_sha256:str
    transition_check_sha256:str
    transition_apply_sha256:str

    def __post_init__(self):
        for field in self.__dataclass_fields__:_hash(getattr(self,field))

    def wire(self):return asdict(self)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__):
            raise ValueError('complete versioned revision2 migration pins required')
        return cls(**value)

    @property
    def manifest(self):
        return dict(codec='rkb.native-source-revision2-upgrade.v1',predecessor=PREDECESSOR,
            artifacts=[dict(name=name,sha256=hash_value) for name,hash_value in
                zip(ARTIFACTS,(self.sql015_sha256,self.sql016_sha256,self.sql017_sha256),strict=True)])

    @property
    def artifact_sha256(self):return sha(canonical(self.manifest))


def revision2_upgrade_handler(pins):
    if type(pins) is not NativeRevision2UpgradePins:raise TypeError('exact immutable revision2 pins required')
    return HandlerSpec('schema_transition',ROUTE,ACTIVATION_REVISIONS,
        PgFunction(CHECK,pins.transition_check_sha256),PgFunction(APPLY,pins.transition_apply_sha256),
        PRECONDITIONS,
        MigrationTransition(MIGRATION,pins.artifact_sha256,PREDECESSOR,
            REVISION2_REVISIONS,preserve_read_frontier=True))


def validate_revision2_upgrade(command,*,pins):
    if type(pins) is not NativeRevision2UpgradePins or type(command) is not ExactCommand:
        raise ValueError('exact revision2 upgrade command required')
    expected=dict(schema=ACTIVATION_REVISIONS.schema,function=ACTIVATION_REVISIONS.function,
        executor=ACTIVATION_REVISIONS.executor,migration=PREDECESSOR,
        installer_person_id=command.person_id)
    if (command.kind!='schema_transition' or command.route!=ROUTE
        or (command.schema_revision,command.function_revision,command.executor_revision)!=ACTIVATION_REVISIONS.wire()
        or command.ids!={} or command.expected_revisions!=expected
        or str(UUID(command.person_id))!=command.person_id
        or command.payload!=canonical(dict(migration_id=MIGRATION,
            artifact_sha256=pins.artifact_sha256,predecessor=PREDECESSOR))):
        raise ValueError('historical schema or ordered migration predecessor differs')
    return expected


def resolved_revision2_upgrade(*,pins,**identity):
    if type(pins) is not NativeRevision2UpgradePins:raise TypeError('exact pinned successor required')
    value=ExactCommand(**identity,kind='schema_transition',route=ROUTE,
        schema_revision=ACTIVATION_REVISIONS.schema,function_revision=ACTIVATION_REVISIONS.function,
        executor_revision=ACTIVATION_REVISIONS.executor,ids={},
        expected_revisions=dict(schema=ACTIVATION_REVISIONS.schema,function=ACTIVATION_REVISIONS.function,
            executor=ACTIVATION_REVISIONS.executor,migration=PREDECESSOR,
            installer_person_id=identity['person_id']),
        payload=canonical(dict(migration_id=MIGRATION,artifact_sha256=pins.artifact_sha256,
            predecessor=PREDECESSOR)))
    validate_revision2_upgrade(value,pins=pins)
    return value


def transition_sql(pins):
    if type(pins) is not NativeRevision2UpgradePins:raise TypeError('fixed revision2 pins required')
    payload=canonical(dict(migration_id=MIGRATION,
        artifact_sha256=pins.artifact_sha256,predecessor=PREDECESSOR)).decode()
    manifest=canonical(pins.manifest).decode()
    return f"""
CREATE FUNCTION rkb_v4.{CHECK}(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE s rkb_v4.recovery_state;
BEGIN
 SELECT * INTO STRICT s FROM rkb_v4.recovery_state WHERE singleton;
 RETURN p=convert_to('{payload}','UTF8') AND ids='{{}}'::jsonb
  AND expected ?& ARRAY['schema','function','executor','migration','installer_person_id']
  AND expected-ARRAY['schema','function','executor','migration','installer_person_id']='{{}}'::jsonb
  AND expected->>'schema'='{ACTIVATION_REVISIONS.schema}' AND s.schema_revision=expected->>'schema'
  AND expected->>'function'='{ACTIVATION_REVISIONS.function}' AND s.function_revision=expected->>'function'
  AND expected->>'executor'='{ACTIVATION_REVISIONS.executor}' AND s.executor_revision=expected->>'executor'
  AND expected->>'migration'='{PREDECESSOR}' AND s.last_migration_id=expected->>'migration'
  AND (expected->>'installer_person_id')::uuid::text=expected->>'installer_person_id'
  AND to_regclass('rkb_v4.source_stage_sessions') IS NOT NULL
  AND to_regclass('rkb_v4.source_revision2_sessions') IS NOT NULL
  AND to_regclass('rkb_v4.story_reconciliation_cursor') IS NOT NULL;
EXCEPTION WHEN data_exception OR no_data_found THEN RETURN false;
END $$;
CREATE FUNCTION rkb_v4.{APPLY}(p bytea,ids jsonb,expected jsonb,logical_time text)
RETURNS bytea LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM 1 FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE;
 IF NOT rkb_v4.{CHECK}(p,ids,expected,logical_time) THEN
  RAISE EXCEPTION 'source revision2 exact migration predecessor changed';END IF;
 RETURN convert_to('{manifest}','UTF8');
END $$;
REVOKE ALL ON FUNCTION rkb_v4.{CHECK}{SIGNATURE},rkb_v4.{APPLY}{SIGNATURE} FROM PUBLIC;
"""


class NativeRevision2Migration:
    """Offline-prepared successor transition using the SAME owned writer/PG."""
    def __init__(self,*,sql_root,registry,database_generation,pins):
        if type(registry) is not HandlerRegistry or type(pins) is not NativeRevision2UpgradePins:
            raise TypeError('existing registry and exact 015/016/017 pins required')
        self.handler=revision2_upgrade_handler(pins)
        if registry.handlers.get(self.handler.key)!=self.handler:
            raise ValueError('expected revision2 transition not installed in registry')
        self.sql_root=Path(sql_root)
        self.database_generation=database_generation
        self.pins=pins

    async def prepare(self,connection,command,handler):
        if handler!=self.handler or handler.transition is None:
            raise ValueError('wrong revision2 migration owner')
        validate_revision2_upgrade(command,pins=self.pins)
        payloads=[]
        for name,hash_value in zip(ARTIFACTS,(
                self.pins.sql015_sha256,self.pins.sql016_sha256,self.pins.sql017_sha256),strict=True):
            path=self.sql_root/name
            if path.is_symlink() or not path.is_file() or not 1<=path.stat().st_size<=256*1024:
                raise ValueError('exact ordered 015/016/017 source artifact missing')
            raw=path.read_bytes()
            if sha(raw)!=hash_value:raise ValueError('immutable ordered source artifact changed')
            payloads.append(raw)
        async with connection.transaction():
            await connection.execute("SET LOCAL statement_timeout='5000ms'")
            state=await(await connection.execute('''SELECT database_generation,schema_revision,
                function_revision,executor_revision,applied_seq,last_migration_id,last_migration_hash
                FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE''')).fetchone()
            if (state is None or state[0]!=self.database_generation
                    or tuple(state[1:4])!=ACTIVATION_REVISIONS.wire()
                    or state[4]!=command.sequence-1 or state[5]!=PREDECESSOR):
                raise ValueError('active source schema/predecessor not closed for revision2')
            for raw in payloads:
                await connection.execute(raw.decode(),prepare=False)
            await connection.execute(transition_sql(self.pins),prepare=False)
            # No other process is allowed to read a half-applied SQL015/016/017:
            # only one transaction installs the full ordered immutable set.
            for name in (CHECK,APPLY):
                row=await(await connection.execute(
                    'SELECT pg_get_functiondef(to_regprocedure(%s))',
                    ('rkb_v4.'+name+SIGNATURE,))).fetchone()
                if row is None or row[0] is None:raise ValueError('revision2 transition definition missing')
