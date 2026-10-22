"""Finite first-bootstrap observations and trusted one-time pin finalization.

An observation is evidence, never READY. Only the local controller can finalize
an independently reviewed pinset while the identical attachment is still owned.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import re
from types import MappingProxyType

ANCHORS=frozenset({'code','launch_config','executed_source','model'})
MAX_OBSERVATION_BYTES=1536*1024


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def digest(value):return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class BootstrapRuntimeExpectation:
    hashes: object
    database_generation: str
    schema_revision: str
    function_revision: str
    executor_revision: str
    packages: object
    native_manifest_sha256: str
    sql_hashes: object
    seconds: int=900
    min_data_seq: int=0

    def __post_init__(self):
        from .kaggle_readiness import SearchRuntimeExpectation
        hashes=dict(self.hashes)
        if set(hashes)!=ANCHORS:raise ValueError('four exact immutable bootstrap anchors required')
        SearchRuntimeExpectation(dict(hashes,dependency='a'*64,schema='b'*64),self.database_generation,
            self.schema_revision,self.function_revision,self.executor_revision,self.min_data_seq)
        if (self.min_data_seq!=0 or type(self.seconds) is not int or not 30<=self.seconds<=1800
                or not re.fullmatch('[0-9a-f]{64}',self.native_manifest_sha256)
                or not 1<=len(self.packages)<=64 or not 1<=len(self.sql_hashes)<=16
                or any(type(k) is not str or type(v) is not str or not 1<=len(k)<=128 or not 1<=len(v)<=128
                    for k,v in self.packages.items())
                or any(re.fullmatch('[0-9a-f]{64}',value) is None for value in self.sql_hashes.values())):
            raise ValueError('bounded exact bootstrap package/native/SQL plan required')
        for name,value in (('hashes',hashes),('packages',self.packages),('sql_hashes',self.sql_hashes)):
            object.__setattr__(self,name,MappingProxyType(dict(value)))

    def wire(self):
        return dict(codec='rkb.bootstrap-expectation.v1',hashes=dict(self.hashes),
            database_generation=self.database_generation,schema_revision=self.schema_revision,
            function_revision=self.function_revision,executor_revision=self.executor_revision,
            min_data_seq=self.min_data_seq,packages=dict(self.packages),native_manifest_sha256=self.native_manifest_sha256,
            sql_hashes=dict(self.sql_hashes),seconds=self.seconds)

    @classmethod
    def from_wire(cls,value):
        value=dict(value)
        if value.pop('codec',None)!='rkb.bootstrap-expectation.v1':raise ValueError('bootstrap expectation codec')
        return cls(**value)


def verify_observation(plan,value):
    """Recompute hashes from actual bounded bytes; never promote from this call."""
    from .kaggle_bootstrap import CandidateReadiness
    from .kaggle_database_probe import NativeDatabaseProbe
    if (type(plan) is not BootstrapRuntimeExpectation or type(value) is not dict
            or set(value)!={'runtime_readiness','dependencies','schema_version'}
            or len(canonical(value))>MAX_OBSERVATION_BYTES):
        raise ValueError('bounded exact bootstrap observation required')
    candidate=CandidateReadiness(**value['runtime_readiness'])
    if (any(candidate.hashes[k]!=v for k,v in plan.hashes.items())
            or any(getattr(candidate,k)!=getattr(plan,k) for k in
                ('database_generation','schema_revision','function_revision','executor_revision'))
            or candidate.applied_seq!=0 or candidate.projection_seq!=0
            or not candidate.text_search_ready or not candidate.dense_search_ready):
        raise ValueError('bootstrap anchors or empty native generation differ')
    dependencies=value['dependencies']
    if (type(dependencies) is not dict or set(dependencies)!={'python','machine','libc','packages'}
            or dependencies['packages']!=dict(plan.packages)
            or any(type(dependencies[k]) is not str or not 1<=len(dependencies[k].encode())<=1024 for k in ('python','machine'))
            or type(dependencies['libc']) is not list or len(dependencies['libc'])!=2
            or any(type(part) is not str or not 1<=len(part)<=128 for part in dependencies['libc'])
            or digest(canonical(dependencies))!=candidate.hashes['dependency']):
        raise ValueError('actual dependency bytes differ from pinned package closure')
    if (type(value['schema_version']) is not int or value['schema_version']!=candidate.schema_version
            or candidate.schema_version!=len(plan.sql_hashes)
            or digest(canonical(candidate.schema_version))!=candidate.hashes['schema']):
        raise ValueError('actual schema version differs from bundled SQL')
    return candidate


def expect_bootstrap(queue,run_id,expected):
    if type(expected) is not BootstrapRuntimeExpectation:raise TypeError('typed bootstrap expectation required')
    data=canonical(expected.wire()).decode()
    with queue.connect() as db:
        run=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
        prior=queue._runtime_row(db,run_id)
        if prior is not None:
            if prior['expected_json']!=data:raise ValueError('bootstrap manifest is immutable')
            return
        if run is None or run['status']!='starting' or run['connection_generation'] or run['channel_active']:
            raise ValueError('bootstrap expectation must precede first attachment')
        db.execute('''CREATE TABLE IF NOT EXISTS bge_runtime_expectations(run_id TEXT PRIMARY KEY,
            expected_json TEXT NOT NULL,accepted_generation INTEGER,observed_json TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS bge_runtime_bootstrap(run_id TEXT PRIMARY KEY,
            plan_json TEXT NOT NULL,deadline REAL NOT NULL,incarnation TEXT,connection_generation INTEGER,
            provider_version INTEGER,observation_json TEXT,observation_sha256 TEXT,review_evidence TEXT,finalized INTEGER NOT NULL DEFAULT 0)''')
        db.execute('INSERT INTO bge_runtime_expectations(run_id,expected_json) VALUES(?,?)',(run_id,data))
        db.execute('INSERT INTO bge_runtime_bootstrap(run_id,plan_json,deadline) VALUES(?,?,?)',
            (run_id,data,queue.clock()+expected.seconds))


def record_observation(queue,run_id,token,generation,value):
    encoded=canonical(value).decode()
    with queue.connect() as db:
        queue._shared_owner(db)
        run=queue._authorize(db,run_id,token,queue.clock());queue._dispatch_authorize(run,generation)
        row=db.execute('SELECT * FROM bge_runtime_bootstrap WHERE run_id=?',(run_id,)).fetchone()
        if row is not None and row['finalized']:
            if (row['incarnation']!=run['runtime_incarnation'] or row['observation_sha256']!=digest(encoded.encode())
                    or not queue._runtime_check(db,run,generation,value['runtime_readiness']).ok):
                raise ValueError('finalized bootstrap observation differs')
            return row['observation_sha256']
        if (row is None or row['deadline']<=queue.clock() or run['status']!='starting'
                or run['provider_version'] is None or queue._run_expired(run,queue.clock())):
            raise ValueError('current finite unfinalized bootstrap attachment required')
        plan=BootstrapRuntimeExpectation.from_wire(json.loads(row['plan_json']))
        verify_observation(plan,value)
        if row['connection_generation']==generation and row['observation_json']!=encoded:
            raise ValueError('one immutable observation per bootstrap attachment')
        db.execute('UPDATE bge_runtime_bootstrap SET incarnation=?,connection_generation=?,provider_version=?,observation_json=?,observation_sha256=? WHERE run_id=?',
            (run['runtime_incarnation'],generation,run['provider_version'],encoded,digest(encoded.encode()),run_id))
    return digest(encoded.encode())


def finalize_bootstrap(queue,run_id,expected,*,observation_sha256,review_evidence):
    """Trusted local controller operation, deliberately absent from WSS/MCP."""
    from .kaggle_readiness import SearchRuntimeExpectation
    if type(expected) is not SearchRuntimeExpectation:raise TypeError('independently reviewed full pinset required')
    if type(review_evidence) is not str or not 1<=len(review_evidence.encode())<=1024:
        raise ValueError('independent bootstrap review evidence reference required')
    data=canonical(expected.wire()).decode()
    with queue.connect() as db:
        run=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
        row=db.execute('SELECT * FROM bge_runtime_bootstrap WHERE run_id=?',(run_id,)).fetchone()
        prior=queue._runtime_row(db,run_id)
        if (run is None or row is None or row['finalized'] or row['deadline']<=queue.clock()
                or run['status']!='starting' or not run['channel_active'] or run['attachment_conflict']
                or (run['runtime_incarnation'],run['connection_generation'])!=(row['incarnation'],row['connection_generation'])
                or run['provider_version']!=row['provider_version']
                or row['observation_sha256']!=observation_sha256 or queue._run_expired(run,queue.clock())
                or prior['accepted_generation'] is not None or prior['expected_json']!=row['plan_json']):
            raise ValueError('exact current unfinalized bootstrap observation required')
        plan=BootstrapRuntimeExpectation.from_wire(json.loads(row['plan_json']))
        candidate=verify_observation(plan,json.loads(row['observation_json']))
        if (dict(expected.hashes)!=dict(candidate.hashes) or any(getattr(expected,k)!=getattr(plan,k) for k in
                ('database_generation','schema_revision','function_revision','executor_revision','min_data_seq'))):
            raise ValueError('reviewed final pins differ from exact bootstrap evidence')
        db.execute('UPDATE bge_runtime_expectations SET expected_json=? WHERE run_id=?',(data,run_id))
        db.execute('UPDATE bge_runtime_bootstrap SET finalized=1,review_evidence=? WHERE run_id=?',(review_evidence,run_id))
        db.execute('UPDATE runs SET bootstrap_finalized_at=?,useful=max(useful,?) WHERE id=?',
            (queue.clock(),queue.clock(),run_id))


def finalized_pins(queue,run_id,token,generation):
    """Authenticated prepared proof; never accepts READY or grants admission."""
    with queue.connect(write=False) as db:
        run=queue._authorize(db,run_id,token,queue.clock());queue._dispatch_authorize(run,generation)
        if run['bootstrap_finalized_at'] is None:return None
        row=db.execute('SELECT * FROM bge_runtime_bootstrap WHERE run_id=?',(run_id,)).fetchone()
        runtime=queue._runtime_row(db,run_id)
        if (row is None or not row['finalized'] or not row['review_evidence']
                or row['incarnation']!=run['runtime_incarnation']
                or row['provider_version']!=run['provider_version'] or run['provider_version'] is None
                or not 0<row['connection_generation']<=generation):
            raise ValueError('exact finalized bootstrap attachment required')
        value=json.loads(row['observation_json'])
        if digest(canonical(value))!=row['observation_sha256']:
            raise ValueError('finalized bootstrap observation integrity differs')
        candidate=verify_observation(BootstrapRuntimeExpectation.from_wire(json.loads(row['plan_json'])),value)
        expected=json.loads(runtime['expected_json'])
        if (expected['hashes']!=dict(candidate.hashes) or any(expected[k]!=getattr(candidate,k) for k in
                ('database_generation','schema_revision','function_revision','executor_revision'))):
            raise ValueError('finalized bootstrap pins changed')
        return dict(run_id=run_id,runtime_incarnation=run['runtime_incarnation'],
            provider_version=str(run['provider_version']),observation_sha256=row['observation_sha256'],hashes=expected['hashes'])
