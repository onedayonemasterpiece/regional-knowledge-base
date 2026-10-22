"""Bounded native catalog/read-fence observation of an already owned Kaggle PG.

No creation, migration, restore, model/provider call or corpus scan occurs here.
Expected catalog hash and DB/revision binding remain controller authority.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import math
import time

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg.errors import QueryCanceled
from .kaggle_domain_schema import (TABLES_9,FUNCTIONS_9,TRIGGERS_9,TABLES_10,FUNCTIONS_10,TRIGGERS_10,
    TABLES_11,FUNCTIONS_11,TRIGGERS_11,FUNCTIONS_12,TABLES_13,FUNCTIONS_13,TRIGGERS_13,
    TABLES_14,FUNCTIONS_14,TRIGGERS_14,IMAGE_SOURCE_SCHEMA)


class DatabaseProbeUnavailable(RuntimeError):
    """Sanitized fixed code; never native diagnostics or private catalog values."""


TABLES = frozenset({'sources', 'revisions', 'active_revision', 'passage_search',
    'writer_generations', 'recovery_state', 'applied_ops', 'read_frontier', 'source_visibility',
    'source_import_origins', 'source_research_frontier'})
FUNCTIONS = frozenset('rkb_v4.' + signature for signature in (
    'reject_revision_update()', 'reject_source_id_update()', 'lexical_candidates(text,integer)',
    'dense_candidates(public.vector,integer)', 'hydrate(uuid[])', 'reject_applied_op_change()',
    'checked_source_scope(uuid[])', 'lexical_candidates_scoped(text,integer,uuid[])',
    'dense_candidates_scoped(public.vector,integer,uuid[])', 'hydrate_scoped(uuid[],uuid[])',
    'lock_read_fence_writer()', 'bump_source_visibility()', 'validate_read_frontier()',
    'publish_read_frontier(text,text,bigint,bigint,boolean,boolean)',
    'prepare_read_frontier(text,text,bigint,bigint,boolean,boolean)',
    'promote_applied_read_frontier()', 'reject_orphan_frontier_proof()',
    'check_source_withdraw_v1(bytea,jsonb,jsonb,text)', 'apply_source_withdraw_v1(bytea,jsonb,jsonb,text)',
    'check_source_import_v1(bytea,jsonb,jsonb,text)', 'apply_source_import_v1(bytea,jsonb,jsonb,text)'))
TRIGGERS = frozenset((table, name) for table, name in (
    ('sources', 'immutable_source_id'), ('revisions', 'immutable_revision'), ('passage_search', 'immutable_passage'),
    ('applied_ops', 'immutable_applied_ops'), ('sources', 'source_fence_lock'),
    ('active_revision', 'active_fence_lock'), ('passage_search', 'projection_fence_lock'),
    ('read_frontier', 'frontier_fence_lock'), ('sources', 'source_visibility_change'),
    ('active_revision', 'active_visibility_change'), ('passage_search', 'projection_visibility_change'),
    ('read_frontier', 'validate_read_frontier'), ('recovery_state', 'promote_applied_read_frontier'),
    ('read_frontier', 'reject_orphan_frontier_proof'),
    ('source_import_origins', 'immutable_source_origin')))

STORY_TABLES=frozenset({'source_regions','story_assertion_versions','stories','story_versions',
    'story_variant_versions','story_search','source_story_pending','story_history_receipts'})
STORY_FUNCTIONS=frozenset('rkb_v4.'+name for name in (
    'check_source_import_v2(bytea,jsonb,jsonb,text)','apply_source_import_v2(bytea,jsonb,jsonb,text)',
    'read_story_v1(uuid,text,bigint,bigint,integer)','story_lexical_candidates_v1(text,integer,uuid[])',
    'story_dense_candidates_v1(public.vector,integer,uuid[])','story_hydrate_v1(uuid[],uuid[])',
    'capture_story_import_receipt_v1()','verify_source_story_origins_v1(uuid[])'))
STORY_TRIGGERS=frozenset((table,name) for table,name in (
    ('source_regions','immutable_source_region'),('story_assertion_versions','immutable_story_assertion'),
    ('story_versions','immutable_story_version'),('story_variant_versions','immutable_story_variant'),
    ('stories','story_source_fence'),('story_search','story_projection_fence'),
    ('applied_ops','capture_story_import_receipt'),('story_history_receipts','immutable_story_receipt'),
    ('stories','immutable_story_current'),('story_search','immutable_story_projection')))
FACT_TABLES=frozenset({'source_event_dates','source_observations'})
FACT_FUNCTIONS=frozenset('rkb_v4.'+name for name in (
    'source_fact_proofs_v1(jsonb,jsonb)','check_source_facts_v1(jsonb)','project_source_facts_v1()',
    'check_source_import_v3(bytea,jsonb,jsonb,text)','apply_source_import_v3(bytea,jsonb,jsonb,text)',
    'capture_story_facts_receipt_v1()','verify_source_story_origins_v2(uuid[])',
    'read_source_calendar_v1(text,integer,integer,uuid,integer,uuid,integer,integer)',
    'read_source_observations_v1(text,text,uuid,uuid,integer,integer)',
    'read_source_assertions_v1(uuid,uuid,integer)'))
FACT_TRIGGERS=frozenset({('source_event_dates','immutable_source_date'),
    ('source_observations','immutable_source_observation'),
    ('story_assertion_versions','project_source_facts'),('applied_ops','capture_story_facts_receipt')})

# Only native metadata in the target namespace; no source/document rows. Each
# query is wrapped with C ordering, row LIMIT and payload octet-length guards.
CATALOG_QUERIES = (
    ('environment', """SELECT 'runtime' AS identifier, jsonb_build_object(
        'server_version_num',current_setting('server_version_num')::integer,
        'server_encoding',current_setting('server_encoding'),
        'lc_collate',d.datcollate,'lc_ctype',d.datctype,
        'collation_provider',d.datlocprovider,'collation_version',d.datcollversion,
        'actual_collation_version',pg_database_collation_actual_version(d.oid),
        'vector_version',e.extversion,'vector_schema',n.nspname)::text AS payload
        FROM pg_database d LEFT JOIN pg_extension e ON e.extname='vector'
        LEFT JOIN pg_namespace n ON n.oid=e.extnamespace WHERE d.datname=current_database()"""),
    ('tables', """SELECT c.relname AS identifier, jsonb_build_object('kind',c.relkind,
        'row_security',c.relrowsecurity,'force_row_security',c.relforcerowsecurity,
        'owner',pg_get_userbyid(c.relowner),'acl',c.relacl::text,'options',c.reloptions)::text AS payload
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='rkb_v4' AND c.relkind IN ('r','p')"""),
    ('columns', """SELECT c.relname||':'||a.attnum AS identifier,
        jsonb_build_object('table',c.relname,'name',a.attname,'position',a.attnum,
        'type',format_type(a.atttypid,a.atttypmod),'not_null',a.attnotnull,
        'identity',a.attidentity,'generated',a.attgenerated,
        'collation',CASE WHEN a.attcollation=0 THEN NULL ELSE a.attcollation::regcollation::text END,
        'default',pg_get_expr(d.adbin,d.adrelid))::text AS payload
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE n.nspname='rkb_v4' AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped"""),
    ('constraints', """SELECT c.relname||':'||k.conname AS identifier,
        jsonb_build_object('table',c.relname,'name',k.conname,'kind',k.contype,
        'validated',k.convalidated,'definition',pg_get_constraintdef(k.oid,false))::text AS payload
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='rkb_v4'"""),
    ('indexes', """SELECT idx.relname AS identifier, jsonb_build_object('table',c.relname,
        'method',am.amname,'valid',i.indisvalid,'ready',i.indisready,'live',i.indislive,
        'unique',i.indisunique,'primary',i.indisprimary,'options',idx.reloptions,
        'definition',pg_get_indexdef(i.indexrelid,0,false))::text AS payload
        FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_am am ON am.oid=idx.relam WHERE n.nspname='rkb_v4'"""),
    ('triggers', """SELECT c.relname||':'||CASE WHEN t.tgisinternal THEN
        'internal:'||coalesce(k.conname,'')||':'||t.tgtype||':'||p.proname ELSE t.tgname::text END AS identifier,
        jsonb_build_object('table',c.relname,'name',CASE WHEN t.tgisinternal THEN NULL ELSE t.tgname END,
        'internal',t.tgisinternal,'enabled',t.tgenabled,'function',p.oid::regprocedure::text,
        'type',t.tgtype,'deferrable',t.tgdeferrable,'initially_deferred',t.tginitdeferred,
        'constraint',k.conname,'definition',CASE WHEN t.tgisinternal THEN NULL ELSE pg_get_triggerdef(t.oid,false) END)::text AS payload
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_proc p ON p.oid=t.tgfoid LEFT JOIN pg_constraint k ON k.oid=t.tgconstraint
        WHERE n.nspname='rkb_v4'"""),
    ('functions', """SELECT p.oid::regprocedure::text AS identifier,
        jsonb_build_object('kind',p.prokind,'returns_set',p.proretset,
        'result',p.prorettype::regtype::text,'language',l.lanname,'volatility',p.provolatile,
        'parallel',p.proparallel,'security_definer',p.prosecdef,'settings',p.proconfig,
        'owner',pg_get_userbyid(p.proowner),'acl',p.proacl::text,
        'definition',CASE WHEN p.prokind IN ('f','p','w') AND octet_length(p.prosrc)<=65536
            THEN pg_get_functiondef(p.oid) ELSE NULL END)::text AS payload
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='rkb_v4'"""),
    ('text_config', """SELECT c.cfgname||':'||m.maptokentype||':'||m.mapseqno AS identifier,
        jsonb_build_object('config',c.cfgname,'parser',c.cfgparser::regproc::text,
        'token',m.maptokentype,'sequence',m.mapseqno,'dictionary',d.dictname,
        'dictionary_schema',dn.nspname,'template',t.tmplname,'template_schema',tn.nspname,
        'options',d.dictinitoption)::text AS payload
        FROM pg_ts_config c JOIN pg_namespace cn ON cn.oid=c.cfgnamespace
        JOIN pg_ts_config_map m ON m.mapcfg=c.oid JOIN pg_ts_dict d ON d.oid=m.mapdict
        JOIN pg_namespace dn ON dn.oid=d.dictnamespace JOIN pg_ts_template t ON t.oid=d.dicttemplate
        JOIN pg_namespace tn ON tn.oid=t.tmplnamespace
        WHERE cn.nspname='pg_catalog' AND c.cfgname IN ('russian','german','simple')"""),
)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()


def native_probe_for_schema(schema_revision, *, timeout_seconds=2.0):
    """Bounded metadata budget only; selecting it grants no database readiness.

    The exact PG17.11 fresh8 fixture measured545 records/218231 bytes.576 gives
    31 records of fixed headroom. Existing schema6/7 and unrecognized revisions
    retain512. The complete001..011 PG17.11 graph fixture measured1329 records /
    606416 bytes before role hardening; explicit11 uses1376. Byte/definition limits are unchanged.
    Schema14 uses the existing hard row ceiling while its combined native
    fixture is measured; this is not a claimed resource/performance acceptance.
    """
    return NativeDatabaseProbe(timeout_seconds=timeout_seconds)


@dataclass(frozen=True)
class _CatalogDiagnostic:
    timeout_seconds: float = 2.0
    max_catalog_bytes: int = 1024 * 1024
    max_catalog_rows: int = 512
    max_definition_bytes: int = 65536
    schema_revision: str | None = None

    def __post_init__(self):
        if (type(self.timeout_seconds) not in (int, float) or not math.isfinite(self.timeout_seconds)
                or not 0 < self.timeout_seconds <= 10
                or type(self.max_catalog_bytes) is not int or not 1 <= self.max_catalog_bytes <= 1024*1024
                or type(self.max_catalog_rows) is not int or not 1 <= self.max_catalog_rows <= 2048
                or type(self.max_definition_bytes) is not int or not 1 <= self.max_definition_bytes <= 65536):
            raise ValueError('bounded native probe budgets required')

    async def __call__(self, *, acquire):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.timeout_seconds
        records, observed = [], {}
        prefix, suffix = b'{"format":"rkb-native-catalog-v1","records":[', b']}'
        used = len(prefix) + len(suffix)
        def add(section, identifier, payload):
            nonlocal used
            encoded = _json([section, identifier, payload])
            used += len(encoded) + bool(records)
            if used > self.max_catalog_bytes or len(records) >= self.max_catalog_rows:
                raise DatabaseProbeUnavailable('database_catalog_bound')
            records.append(encoded)
        try:
            async with asyncio.timeout_at(deadline):
                async with acquire() as connection:
                    if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
                            or connection.row_factory is not tuple_row):
                        raise DatabaseProbeUnavailable('database_lease_invalid')
                    async with connection.transaction():
                        async with connection.cursor() as control:
                            await control.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                            await control.execute('SET LOCAL search_path=pg_catalog')
                            async def budget():
                                remaining = deadline-loop.time()
                                if remaining <= 0:
                                    raise TimeoutError
                                await control.execute("SELECT set_config('statement_timeout',%s,true)",
                                                      (str(max(1,int(remaining*1000))),))
                            for number, (section, body) in enumerate(CATALOG_QUERIES):
                                rows = {}
                                sql = """SELECT identifier, CASE WHEN octet_length(payload)<=%s THEN payload ELSE NULL END,
                                    octet_length(payload) FROM (""" + body + ") q ORDER BY identifier COLLATE \"C\" LIMIT %s"
                                async with connection.cursor(name=f'rkb_catalog_probe_{number}') as stream:
                                    await budget()
                                    await stream.execute(sql, (self.max_definition_bytes, self.max_catalog_rows-len(records)+1))
                                    while True:
                                        await budget()
                                        row = await stream.fetchone()
                                        if row is None:
                                            break
                                        key, raw, size = row
                                        if (not isinstance(key,str) or key in rows or not isinstance(raw,str)
                                                or type(size) is not int or size>self.max_definition_bytes
                                                or len(raw.encode())>self.max_definition_bytes):
                                            raise DatabaseProbeUnavailable('database_catalog_bound')
                                        payload = json.loads(raw)
                                        rows[key] = payload
                                        add(section,key,payload)
                                observed[section] = rows
                            self._validate_catalog(observed,self.schema_revision)
                            await budget()
                            await control.execute('SELECT public.vector_dims(array_fill(0.0::real,ARRAY[1024])::public.vector)')
                            if await control.fetchone() != (1024,):
                                raise DatabaseProbeUnavailable('database_vector_unavailable')
                            await budget()
                            await control.execute("""SELECT jsonb_build_object(
                                'database_generation',CASE WHEN octet_length(s.database_generation)<=256 THEN s.database_generation END,
                                'schema_revision',CASE WHEN octet_length(s.schema_revision)<=256 THEN s.schema_revision END,
                                'function_revision',CASE WHEN octet_length(s.function_revision)<=256 THEN s.function_revision END,
                                'executor_revision',CASE WHEN octet_length(s.executor_revision)<=256 THEN s.executor_revision END,
                                'applied_seq',s.applied_seq,'projection_seq',f.projection_seq,'visibility_epoch',f.visibility_epoch,
                                'text_search_ready',f.text_ready,'dense_search_ready',f.dense_ready,
                                'last_migration_id',CASE WHEN octet_length(s.last_migration_id)<=256 THEN s.last_migration_id END,
                                'last_migration_hash',s.last_migration_hash,'pending',f.pending_seq IS NOT NULL)::text
                                FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
                                WHERE s.singleton AND f.singleton LIMIT 2""")
                            row = await control.fetchone()
                            if row is None or not isinstance(row[0],str) or len(row[0].encode())>4096 or await control.fetchone() is not None:
                                raise DatabaseProbeUnavailable('database_state_unavailable')
                            state = json.loads(row[0])
                            fields = self._state(state)
                            if (bool(STORY_TABLES.intersection(observed['tables'])) !=
                                    (state['schema_revision'] in ('rkb-v4-sql001-007-v1','rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1',IMAGE_SOURCE_SCHEMA))):
                                raise DatabaseProbeUnavailable('database_story_revision_mismatch')
                            if (bool(FACT_TABLES.intersection(observed['tables'])) !=
                                    (state['schema_revision'] in ('rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1',IMAGE_SOURCE_SCHEMA))):
                                raise DatabaseProbeUnavailable('database_fact_revision_mismatch')
                            for layer,known in ((9,TABLES_9),(10,TABLES_10),(11,TABLES_11)):
                                required=state['schema_revision'] in {f'rkb-v4-sql001-{n:03d}-v1' for n in range(layer,12)}|{IMAGE_SOURCE_SCHEMA}
                                if bool(known.intersection(observed['tables']))!=required:
                                    raise DatabaseProbeUnavailable('database_domain_revision_mismatch')
                            for known in (TABLES_13,TABLES_14):
                                if bool(known.intersection(observed['tables']))!=(state['schema_revision']==IMAGE_SOURCE_SCHEMA):
                                    raise DatabaseProbeUnavailable('database_image_source_revision_mismatch')
                            add('semantics','current',{key:state[key] for key in (
                                'schema_revision','function_revision','executor_revision','last_migration_id','last_migration_hash')})
            return prefix + b','.join(records) + suffix, fields
        except asyncio.CancelledError:
            raise
        except DatabaseProbeUnavailable:
            raise
        except TimeoutError:
            raise DatabaseProbeUnavailable('database_probe_deadline') from None
        except QueryCanceled:
            raise DatabaseProbeUnavailable('database_probe_cancelled') from None
        except Exception:
            raise DatabaseProbeUnavailable('database_probe_failed') from None

    @staticmethod
    def _validate_catalog(observed,schema_revision=None):
        stories=bool(STORY_TABLES.intersection(observed['tables']))
        tables=TABLES|STORY_TABLES if stories else TABLES
        functions=FUNCTIONS|STORY_FUNCTIONS if stories else FUNCTIONS
        required_triggers=TRIGGERS|STORY_TRIGGERS if stories else TRIGGERS
        facts=bool(FACT_TABLES.intersection(observed['tables']))
        if facts:
            tables=tables|FACT_TABLES;functions=functions|FACT_FUNCTIONS
            required_triggers=required_triggers|FACT_TRIGGERS
        extra_functions={}
        for known_tables,known_functions,known_triggers in ((TABLES_9,FUNCTIONS_9,TRIGGERS_9),
                (TABLES_10,FUNCTIONS_10,TRIGGERS_10),(TABLES_11,FUNCTIONS_11,TRIGGERS_11),
                (TABLES_13,FUNCTIONS_13,TRIGGERS_13),(TABLES_14,FUNCTIONS_14,TRIGGERS_14)):
            if known_tables.intersection(observed['tables']):
                tables=tables|known_tables;functions=functions|known_functions.keys()
                required_triggers=required_triggers|known_triggers
                extra_functions.update(known_functions)
        if schema_revision=='rkb-v4-sql001-012-v1':
            upgrades={
                'rkb_v4.check_geo_manifest_install_v2(bytea,jsonb,jsonb,text)':('boolean',False),
                'rkb_v4.apply_geo_manifest_install_v2(bytea,jsonb,jsonb,text)':('bytea',False),
                'rkb_v4.check_geo_dependencies_v2_transition(bytea,jsonb,jsonb,text)':('boolean',False),
                'rkb_v4.apply_geo_dependencies_v2_transition(bytea,jsonb,jsonb,text)':('bytea',False)}
            functions=functions|upgrades.keys();extra_functions.update(upgrades)
        if (TABLES_13|TABLES_14).intersection(observed['tables']):
            functions=functions|FUNCTIONS_12.keys()
            extra_functions.update(FUNCTIONS_12)
        env = observed['environment'].get('runtime',{})
        if (type(env.get('server_version_num')) is not int or env['server_version_num']//10000 != 17
                or env.get('vector_version') != '0.8.2' or env.get('vector_schema') != 'public'
                or env.get('server_encoding') != 'UTF8'
                or env.get('collation_version') != env.get('actual_collation_version')):
            raise DatabaseProbeUnavailable('database_native_runtime_mismatch')
        if not tables <= observed['tables'].keys() or any(observed['tables'][name].get('kind') != 'r' for name in tables):
            raise DatabaseProbeUnavailable('database_schema_incomplete')
        if not functions <= observed['functions'].keys():
            raise DatabaseProbeUnavailable('database_functions_incomplete')
        for name in functions:
            function = observed['functions'][name]
            table_function = any(name.startswith('rkb_v4.' + stem) for stem in ('lexical_candidates(', 'dense_candidates(', 'hydrate(', 'lexical_candidates_scoped(', 'dense_candidates_scoped(', 'hydrate_scoped(', 'story_lexical_candidates_v1(', 'story_dense_candidates_v1(', 'story_hydrate_v1(', 'read_source_calendar_v1(', 'read_source_observations_v1(', 'read_source_assertions_v1('))
            result = ('record' if table_function else 'uuid[]' if 'checked_source_scope(' in name else
                      'jsonb' if any(stem in name for stem in ('read_story_v1(', 'source_fact_proofs_v1(')) else
                      'void' if any(stem in name for stem in ('publish_read_frontier(', 'prepare_read_frontier(')) else
                      'boolean' if any(stem in name for stem in ('check_source_withdraw_v1(', 'check_source_import_v1(', 'check_source_import_v2(', 'verify_source_story_origins_v1(', 'check_source_facts_v1(', 'check_source_import_v3(', 'verify_source_story_origins_v2(')) else
                      'bytea' if any(stem in name for stem in ('apply_source_withdraw_v1(', 'apply_source_import_v1(', 'apply_source_import_v2(', 'apply_source_import_v3(')) else 'trigger')
            if name in extra_functions:result,table_function=extra_functions[name]
            if (function.get('kind') != 'f' or function.get('security_definer') is not False
                    or function.get('returns_set') is not table_function or function.get('result') != result
                    or not isinstance(function.get('definition'),str)):
                raise DatabaseProbeUnavailable('database_function_contract_invalid')
        indexes = observed['indexes']
        if (indexes.get('passage_dense',{}).get('method') != 'hnsw'
                or indexes.get('passage_lexical',{}).get('method') != 'gin'
                or any(indexes.get(name,{}).get('table') != 'passage_search' for name in ('passage_dense','passage_lexical'))
                or any(not all(index.get(key) is True for key in ('valid','ready','live')) for index in indexes.values())):
            raise DatabaseProbeUnavailable('database_indexes_not_ready')
        if stories and (indexes.get('story_search_dense',{}).get('method')!='hnsw'
                or indexes.get('story_search_fts',{}).get('method')!='gin'
                or any(indexes.get(name,{}).get('table')!='story_search'
                    for name in ('story_search_dense','story_search_fts'))):
            raise DatabaseProbeUnavailable('database_story_indexes_not_ready')
        if facts and any(indexes.get(name,{}).get('method')!='btree' or
                indexes.get(name,{}).get('table')!=table for name,table in (
                    ('source_event_day','source_event_dates'),('source_event_source','source_event_dates'),
                    ('source_observation_metric','source_observations'),('source_observation_source','source_observations'))):
            raise DatabaseProbeUnavailable('database_fact_indexes_not_ready')
        triggers = {(row.get('table'),row.get('name')) for row in observed['triggers'].values() if not row.get('internal')}
        if not required_triggers <= triggers or any(row.get('enabled') not in ('O','A') for row in observed['triggers'].values()):
            raise DatabaseProbeUnavailable('database_triggers_not_ready')
        if {row.get('config') for row in observed['text_config'].values()} != {'russian','german','simple'}:
            raise DatabaseProbeUnavailable('database_text_config_incomplete')

    @staticmethod
    def _state(state):
        keys = ('database_generation','schema_revision','function_revision','executor_revision')
        if any(not isinstance(state.get(k),str) or not 1<=len(state[k].encode())<=256 for k in keys):
            raise DatabaseProbeUnavailable('database_state_invalid')
        if any(state.get(k) is not None and (type(state[k]) is not int or not 0<=state[k]<=2**63-1)
               for k in ('applied_seq','projection_seq','visibility_epoch')) or state.get('applied_seq') is None:
            raise DatabaseProbeUnavailable('database_state_invalid')
        if (state.get('projection_seq') is not None and state['projection_seq']>state['applied_seq']
                or type(state.get('text_search_ready')) is not bool or type(state.get('dense_search_ready')) is not bool
                or state.get('pending') is not False):
            raise DatabaseProbeUnavailable('database_state_invalid')
        current = state['projection_seq'] == state['applied_seq'] and state['visibility_epoch'] is not None
        return {**{key:state[key] for key in (*keys,'applied_seq','projection_seq','visibility_epoch')},
            'text_search_ready':state['text_search_ready'] and current,
            'dense_search_ready':state['dense_search_ready'] and current,
            'archive_read_ready':None,'proof_ready':None}


@dataclass(frozen=True)
class NativeDatabaseProbe:
    """Observe readiness and installed versions, never fingerprint the catalog.

    A short read-only statement supplies one consistent frontier/ledger view.
    Schema migrations establish structure; readiness does not enumerate owners,
    collation, definitions or ACLs and is independent of PG patch releases.
    """
    timeout_seconds: float = 2.0

    def __post_init__(self):
        if (type(self.timeout_seconds) not in (int,float) or not math.isfinite(self.timeout_seconds)
                or not 0 < self.timeout_seconds <= 10):
            raise ValueError('bounded native probe deadline required')

    async def __call__(self,*,acquire):
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with acquire() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout='2000ms'")
                        row=await (await connection.execute("""SELECT jsonb_build_object(
                            'database_generation',s.database_generation,
                            'schema_version',(SELECT max(version) FROM rkb_v4.schema_migrations),
                            'server_version_num',current_setting('server_version_num')::integer,
                            'pgvector_version',(SELECT extversion FROM pg_extension WHERE extname='vector'),
                            'postgis_version',(SELECT extversion FROM pg_extension WHERE extname='postgis'),
                            'schema_revision',s.schema_revision,'function_revision',s.function_revision,
                            'executor_revision',s.executor_revision,
                            'applied_seq',s.applied_seq,'projection_seq',f.projection_seq,
                            'visibility_epoch',f.visibility_epoch,'text_search_ready',f.text_ready,
                            'dense_search_ready',f.dense_ready,'pending',f.pending_seq IS NOT NULL)::text
                            FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
                            WHERE s.singleton AND f.singleton LIMIT 2""")).fetchall()
                        if len(row)!=1 or not isinstance(row[0][0],str) or len(row[0][0].encode())>4096:
                            raise DatabaseProbeUnavailable('database_state_unavailable')
                        state=json.loads(row[0][0])
                        if (type(state.get('schema_version')) is not int or state['schema_version']<1
                                or type(state.get('server_version_num')) is not int
                                or state['server_version_num']//10000!=17
                                or state.get('pgvector_version')!='0.8.2'):
                            raise DatabaseProbeUnavailable('database_native_runtime_mismatch')
                        fields=_CatalogDiagnostic._state(state)
                        fields['schema_version']=state['schema_version']
                        # Keep the former two-item adapter shape during the
                        # protocol transition; these bytes are not a catalog
                        # and must never be used as a readiness/hash pin.
                        return b'{}',fields
        except asyncio.CancelledError:
            raise
        except DatabaseProbeUnavailable:
            raise
        except (TimeoutError,QueryCanceled):
            raise DatabaseProbeUnavailable('database_probe_deadline') from None
        except Exception:
            raise DatabaseProbeUnavailable('database_probe_failed') from None


# Bounded health operation over the same existing native PG acquisition.
HEALTH_SQL = """SELECT
    CASE WHEN octet_length(s.database_generation)<=256 THEN s.database_generation END,
    CASE WHEN octet_length(s.schema_revision)<=256 THEN s.schema_revision END,
    s.applied_seq,f.projection_seq,f.pending_seq IS NOT NULL
    FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
    WHERE s.singleton AND f.singleton LIMIT 2"""


def _health_budget(seconds):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 5:
        raise ValueError('bounded health deadline required')


class _HealthProbeFailure(Exception):
    pass


async def probe_sql_health(*, acquire, database_generation, min_data_seq, lease_current, timeout_seconds=2):
    """One bounded native transaction over the existing injected PG acquisition.

    The native WSS handler must first validate its existing request identity.
    This fixed SELECT reads only metadata, never corpus rows or private catalog.
    """
    _health_budget(timeout_seconds)
    if (type(database_generation) is not str or not 1 <= len(database_generation.encode()) <= 256
            or type(min_data_seq) is not int or not 0 <= min_data_seq < 2**63
            or not callable(lease_current)):
        raise ValueError('validated native generation, sequence and lease callback required')
    started = time.monotonic()
    observed = dict(sql_response=False)
    reason = None

    def lease():
        if lease_current() is not True:
            raise _HealthProbeFailure('pg_binding_changed')

    try:
        async with asyncio.timeout(timeout_seconds):
            lease()
            async with acquire() as connection:
                lease()
                if (connection.closed or connection.info.transaction_status != TransactionStatus.IDLE
                        or connection.row_factory is not tuple_row):
                    raise _HealthProbeFailure('pg_lease_invalid')
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        await cursor.execute('SET LOCAL search_path=pg_catalog')
                        await cursor.execute("SELECT set_config('statement_timeout',%s,true),set_config('lock_timeout',%s,true)",
                                             (str(max(1, int(timeout_seconds * 1000))), '100'))
                        lease()
                        await cursor.execute(HEALTH_SQL)
                        rows = await cursor.fetchmany(2)
                        lease()
                        observed['sql_response'] = True
                        if len(rows) != 1 or len(rows[0]) != 5:
                            raise _HealthProbeFailure('pg_metadata_invalid')
                        generation, schema, applied, projection, pending = rows[0]
                        if (type(generation) is not str or not 1 <= len(generation.encode()) <= 256
                                or type(schema) is not str or not 1 <= len(schema.encode()) <= 256
                                or any(type(n) is not int or n < 0 for n in (applied, projection))
                                or type(pending) is not bool):
                            raise _HealthProbeFailure('pg_metadata_invalid')
                        if generation != database_generation:
                            raise _HealthProbeFailure('pg_generation_mismatch')
                        observed.update(schema_revision=schema, applied_seq=applied,
                                        projection_seq=projection, pending=pending)
                        if pending:
                            reason = 'pg_prior_write_pending'
                        elif projection != applied:
                            reason = 'pg_projection_lag'
                        elif applied < min_data_seq:
                            reason = 'pg_sequence_behind'
            lease()  # Pool release / transaction exit is also a cancellation boundary.
    except asyncio.CancelledError:
        raise
    except (TimeoutError, QueryCanceled):
        reason = 'pg_timeout'
    except _HealthProbeFailure as error:
        reason = str(error)
    except Exception:
        reason = 'pg_query_failed'
    return dict(**observed, reason=reason,
                          elapsed_ms=(time.monotonic() - started) * 1000)
