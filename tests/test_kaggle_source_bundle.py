"""The shipped notebook package imports without the Dev checkout or inference."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys

from regional_knowledge.kaggle_target_launch import MODULES, source_bundle


def test_shipped_package_import_closure_is_self_contained(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    _, files, _ = source_bundle(repository)
    for relative, encoded in files.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(encoded, validate=True))
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
        target.write_bytes(base64.b64decode(encoded,validate=True))
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
