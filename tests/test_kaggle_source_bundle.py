"""The shipped notebook package imports without the Dev checkout or inference."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

from regional_knowledge.kaggle_target_launch import MODULES, source_bundle




def test_compact_notebook_bundle_label_preserves_full_per_file_digests():
    import hashlib
    label,files,entries=source_bundle(Path(__file__).resolve().parents[1])
    assert label.startswith('rkb-') and len(label)==len('rkb-')+16
    for entry in entries:
        assert entry['name'].startswith(label+'/')
        relative=entry['name'][len(label)+1:]
        data=files[relative].encode('utf-8')
        assert entry['size']==len(data)
        assert entry['sha256']==hashlib.sha256(data).hexdigest()


def test_shipped_package_import_closure_is_self_contained(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    _, files, _ = source_bundle(repository)
    for relative, encoded in files.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(encoded.encode('utf-8'))
    # -I excludes cwd/PYTHONPATH. The package's own path must resolve only to
    # the shipped bundle; installed third-party dependencies remain available.
    script = """
import importlib,json,pathlib,socket,sys
root=pathlib.Path(sys.argv[1]).resolve()
def denied(*args,**kwargs):raise AssertionError('bundle import attempted network')
socket.socket.connect=denied
socket.create_connection=denied
sys.path.insert(0,str(root))
package=importlib.import_module('regional_knowledge')
assert list(package.__path__)==[str(root/'regional_knowledge')]
for name in json.loads(sys.argv[2]):
    loaded=importlib.import_module('regional_knowledge.'+name)
    assert pathlib.Path(loaded.__file__).resolve().is_relative_to(root)
assert not {'torch','transformers','tensorflow','flax'} & sys.modules.keys()
"""
    completed = subprocess.run(
        [sys.executable, '-I', '-c', script, str(tmp_path), json.dumps(MODULES)],
        cwd=tmp_path, env={'PATH': os.defpath}, capture_output=True,
        timeout=20, text=True,
    )
    assert completed.returncode == 0, completed.stderr[-4000:]


def test_shipped_original_page_executes_and_replays_without_controller_telemetry(tmp_path):
    _,files,_=source_bundle(Path(__file__).resolve().parents[1])
    for relative,encoded in files.items():
        target=tmp_path/relative;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(encoded.encode('utf-8'))
    script="""
import asyncio,pathlib,socket,sys
from types import SimpleNamespace
root=pathlib.Path(sys.argv[1]).resolve()
def denied(*args,**kwargs):raise AssertionError('page fixture attempted network')
socket.socket.connect=denied;socket.create_connection=denied
sys.path.insert(0,str(root))
from regional_knowledge.kaggle_original_native import NativeOriginalIntake,OriginalIntakeConfig
from regional_knowledge.original_intake import canonical,digest
assert pathlib.Path(sys.modules[NativeOriginalIntake.__module__].__file__).is_relative_to(root)
assert not (root/'regional_knowledge/product_telemetry.py').exists()
n=object.__new__(NativeOriginalIntake)
n.config=OriginalIntakeConfig(('https://archive.invalid',),(('pymupdf','fixture'),))
n.database=SimpleNamespace(database_generation='db',writer_generation='writer')
n.identity=('run','nonce','inc');n.generation=1;n.closed=False;n.current=lambda b:b==('run','inc',1)
n._requests={};n._results={};n._records={};n._prebinding_retry=set();n._lock=asyncio.Lock()
def frame(handle):
 args=dict(handle=handle,original_sha256='a'*64,index=0)
 return dict(type='DISPATCH',protocol_version=2,connection_generation=1,id='job-'+handle,
  claim='claim',kind='recovery_phase',run_id='run',launch_nonce='nonce',runtime_incarnation='inc',
  database_generation='db',writer_generation='writer',phase='original_pages',operation_key=handle,
  arguments=args,request_sha256=digest(canonical(dict(phase='original_pages',arguments=args))),
  compute_claim='existing-slot',dispatch_generation=1)
async def main():
 missing=await n(frame('missing'))
 assert missing['value']==dict(error='original_retained_bytes_unavailable')
 execute=n._execute
 async def broken_before_effect(_):raise ModuleNotFoundError('private optional dependency detail')
 n._execute=broken_before_effect
 rejected=await n(frame('unexpected'))
 assert rejected['value']==dict(error='original_intake_unavailable',error_class='ModuleNotFoundError')
 assert 'private optional' not in str(rejected)
 n._execute=execute
 expected=dict(physical_page_index=0,native_text='Synthetic bundled page')
 cursor=canonical(dict(block=0,offset=0,text=0)).decode()
 n._records['retained']=dict(sha256='a'*64,page_count=1,verified={},pages={0:dict(chunks={cursor:expected})})
 result=await n(frame('retained'))
 assert result['value']==expected
 assert await n(frame('retained'))==result
asyncio.run(main())
assert 'regional_knowledge.product_telemetry' not in sys.modules
assert not {'torch','transformers','tensorflow','flax'} & sys.modules.keys()
"""
    completed=subprocess.run([sys.executable,'-I','-c',script,str(tmp_path)],cwd=tmp_path,
        env={'PATH':os.defpath},capture_output=True,timeout=20,text=True)
    assert completed.returncode==0,completed.stderr[-4000:]


async def _accepted_graph():
    import hashlib
    from uuid import UUID,uuid5
    from regional_knowledge.entity_graph import GraphBundle
    from regional_knowledge.stage_graph import StagedPoiFact
    from regional_knowledge.source_graph import resolve_source_graph,resolved_graph_import_command
    from regional_knowledge.kaggle_source_commands import seal_wire
    from test_source_graph import payload,PERSON
    from test_source_import import SOURCE
    from test_recovery_journal import fixture
    value,graph=payload()
    wire=graph.entity_candidates[0].model_dump(mode='json')
    place=next(n for n in wire['entities'] if n['kind']=='poi_ref')
    place.update(state='reviewed',review_note='Explicit source-side review; owner geometry remains unverified')
    graph=graph.model_copy(update={'entity_candidates':[GraphBundle.model_validate(wire)],'poi_facts':[
        StagedPoiFact(candidate_key='source-poi',candidate_id=uuid5(UUID(SOURCE),'poi-fact:1:source-poi'),
            poi_locator=dict(names=['Unresolved source place']),kind='location',text='Source place hypothesis',
            region_ids=[graph.pages[0].regions[0].region_id],page_ids=[graph.pages[0].page_id],publication_method='source_edition')]})
    value['source_graph']=resolve_source_graph(graph,source_id=SOURCE,person_id=PERSON,semantic=value['semantic'])
    command=resolved_graph_import_command(sequence=1,operation_key='bundle-accepted-graph',request_id='bundle-request',
        person_id=PERSON,source_id=SOURCE,payload=value,logical_time='2026-10-20T19:00:00Z')
    _,journal=fixture();seal=await journal.seal([command],predecessor=None,writer_generation='writer-1')
    digest=await journal.verify_accepted(seal,command)
    assert digest==hashlib.sha256(command.encode()).hexdigest()
    return dict(command_b64=base64.b64encode(command.encode()).decode(),accepted=seal_wire(seal),command_sha256=digest)


def test_shipped_package_validates_exact_accepted_reviewed_poi_graph_and_hypothesis_without_checkout(tmp_path):
    import asyncio
    _,files,_=source_bundle(Path(__file__).resolve().parents[1])
    for relative,encoded in files.items():
        target=tmp_path/relative;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(encoded.encode('utf-8'))
    fixture=asyncio.run(_accepted_graph())
    (tmp_path/'accepted.json').write_text(json.dumps(fixture))
    script="""
import asyncio,base64,json,pathlib,socket,sys
root=pathlib.Path(sys.argv[1]).resolve()
def denied(*args,**kwargs):raise AssertionError('accepted graph validation attempted network')
socket.socket.connect=denied
socket.create_connection=denied
sys.path.insert(0,str(root))
from regional_knowledge.kaggle_source_commands import decode_command,decode_seal,ControllerAcceptedProof
raw=json.loads((root/'accepted.json').read_text())
command=decode_command(base64.b64decode(raw['command_b64'],validate=True))
seal=decode_seal(raw['accepted'],raw['command_sha256'],command.sequence)
proof=ControllerAcceptedProof(command.encode(),seal,raw['command_sha256'])
assert asyncio.run(proof.verify_accepted(seal,command))==raw['command_sha256']
doc=json.loads(command.payload)
graph=doc['source_graph']
assert graph['poi_facts'][0]['geometry_acceptance']=='source_hypothesis_not_owner_geometry'
place=next(n for n in graph['entities'] if n['kind']=='poi_ref')
assert place['state']=='reviewed' and place['metadata']['canonical_poi_ref'].startswith('streetstory://poi/poi_ss_')
assert place['metadata']['historical_geometry']=='not_verified'
for name in ('regional_knowledge.source_graph','regional_knowledge.stage_graph','regional_knowledge.poi_reference'):
 assert pathlib.Path(sys.modules[name].__file__).resolve().is_relative_to(root)
assert not {'torch','transformers','tensorflow','flax'} & sys.modules.keys()
"""
    completed=subprocess.run([sys.executable,'-I','-c',script,str(tmp_path)],cwd=tmp_path,
        env={'PATH':os.defpath},capture_output=True,timeout=20,text=True)
    assert completed.returncode==0,completed.stderr[-4000:]


def test_shipped_fact_compare_executes_lazy_path_without_dev_renderer_or_model(tmp_path):
    from test_kaggle_fact_read import row,request,ASSERTION,SOURCE
    _,files,_=source_bundle(Path(__file__).resolve().parents[1])
    for relative,encoded in files.items():
        target=tmp_path/relative;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(encoded.encode('utf-8'))
    story='40000000-0000-0000-0000-000000000001'
    q=request(action='observation_compare',month=None,day=None,calendar=None,story_id=story,
        left_id=f'{ASSERTION}:o0',right_id=f'{ASSERTION}:o1',limit=2,compute_claim='existing-read-slot')
    fixture=dict(request=q.model_dump(mode='json'),rows=[row(1,action='observations'),row(0,action='observations')],
        story=dict(story_id=story,revision=2,finding_ids=[ASSERTION],
            source=dict(source_id=SOURCE,content_revision=1,eligibility_revision=7)))
    (tmp_path/'compare.json').write_text(json.dumps(fixture,default=str))
    script="""
import asyncio,json,pathlib,socket,sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
root=pathlib.Path(sys.argv[1]).resolve()
def denied(*args,**kwargs):raise AssertionError('comparison attempted network/model access')
socket.socket.connect=denied;socket.create_connection=denied
sys.path.insert(0,str(root))
from psycopg.rows import tuple_row
from psycopg.pq import TransactionStatus
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.kaggle_query import PinnedTokenizer
from regional_knowledge.bge_contract import REVISION,SPACE,TOKENIZER_SHA256
raw=json.loads((root/'compare.json').read_text())
class Native:
 closed=False
 def __init__(self):self.row_factory=tuple_row
 info=SimpleNamespace(transaction_status=TransactionStatus.IDLE)
 @asynccontextmanager
 async def acquire(self):yield self
 @asynccontextmanager
 async def transaction(self):yield self
 @asynccontextmanager
 async def cursor(self):yield self
 async def execute(self,sql,args=()):self.sql=sql
 async def fetchone(self):
  if 'SELECT max(version)' in self.sql:return (11,)
  if 'read_editorial_story_v1' in self.sql:return (raw['story'],)
  return (9,9,4,True,False,'native-db')
 async def fetchall(self):return raw['rows']
async def forbidden(_):raise AssertionError('comparison counted or encoded')
handler=KaggleWorkerQuery(acquire=Native().acquire,
 tokenizer=PinnedTokenizer(forbidden,REVISION,TOKENIZER_SHA256,SPACE))
result=asyncio.run(handler(raw['request'],encode=forbidden))
assert result['status']=='ok',result.get('reason')
assert [r['id'] for r in result['records']]==[raw['request']['left_id'],raw['request']['right_id']]
assert result['records'][0]['fact']['value']=='12.50'
assert 'observation_comparison' not in json.dumps(list(sys.modules))
assert not {'torch','transformers','tensorflow','flax'} & sys.modules.keys()
assert not (root/'regional_knowledge'/'observation_comparison.py').exists()
for name in ('kaggle_fact_read','kaggle_fact_contracts','story_contracts'):
 assert pathlib.Path(sys.modules['regional_knowledge.'+name].__file__).resolve().is_relative_to(root)
"""
    completed=subprocess.run([sys.executable,'-I','-c',script,str(tmp_path)],cwd=tmp_path,
        env={'PATH':os.defpath},capture_output=True,timeout=20,text=True)
    assert completed.returncode==0,completed.stderr[-4000:]
