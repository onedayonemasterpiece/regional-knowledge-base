"""Bounded native catalog/read-fence observation of an already owned Kaggle PG.

No creation, migration, restore, model/provider call or corpus scan occurs here.
Expected catalog hash and DB/revision binding remain controller authority.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import math

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg.errors import QueryCanceled


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
        'internal:'||coalesce(k.conname,'')||':'||t.tgtype||':'||p.proname ELSE t.tgname END AS identifier,
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


@dataclass(frozen=True)
class NativeDatabaseProbe:
    timeout_seconds: float = 2.0
    max_catalog_bytes: int = 1024 * 1024
    max_catalog_rows: int = 512
    max_definition_bytes: int = 65536

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
                            self._validate_catalog(observed)
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
    def _validate_catalog(observed):
        env = observed['environment'].get('runtime',{})
        if (type(env.get('server_version_num')) is not int or env['server_version_num']//10000 != 17
                or env.get('vector_version') != '0.8.2' or env.get('vector_schema') != 'public'
                or env.get('server_encoding') != 'UTF8'
                or env.get('collation_version') != env.get('actual_collation_version')):
            raise DatabaseProbeUnavailable('database_native_runtime_mismatch')
        if not TABLES <= observed['tables'].keys() or any(observed['tables'][name].get('kind') != 'r' for name in TABLES):
            raise DatabaseProbeUnavailable('database_schema_incomplete')
        if not FUNCTIONS <= observed['functions'].keys():
            raise DatabaseProbeUnavailable('database_functions_incomplete')
        for name in FUNCTIONS:
            function = observed['functions'][name]
            table_function = any(name.startswith('rkb_v4.' + stem) for stem in ('lexical_candidates(', 'dense_candidates(', 'hydrate(', 'lexical_candidates_scoped(', 'dense_candidates_scoped(', 'hydrate_scoped('))
            result = ('record' if table_function else 'uuid[]' if 'checked_source_scope(' in name else
                      'void' if any(stem in name for stem in ('publish_read_frontier(', 'prepare_read_frontier(')) else
                      'boolean' if any(stem in name for stem in ('check_source_withdraw_v1(', 'check_source_import_v1(')) else
                      'bytea' if any(stem in name for stem in ('apply_source_withdraw_v1(', 'apply_source_import_v1(')) else 'trigger')
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
        triggers = {(row.get('table'),row.get('name')) for row in observed['triggers'].values() if not row.get('internal')}
        if not TRIGGERS <= triggers or any(row.get('enabled') not in ('O','A') for row in observed['triggers'].values()):
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
