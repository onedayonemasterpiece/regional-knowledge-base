"""The updated graph implementation must run without changing the resident module."""
import importlib.util
from pathlib import Path
import sys
import types
import pytest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('warm_bundle_builder',ROOT/'scripts/production/build_warm_read_handler.py')
builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)


def sources():
    folder=ROOT/'src/regional_knowledge'
    return ((folder/'kaggle_worker_query.py').read_text(),(folder/'kaggle_source_graph_read.py').read_text())


@pytest.mark.asyncio
async def test_updated_private_graph_runs_with_resident_wire_types_and_same_resources(monkeypatch):
    from regional_knowledge import kaggle_source_graph_read as resident
    from regional_knowledge.kaggle_query import PinnedTokenizer
    from regional_knowledge.bge_contract import REVISION,SPACE,TOKENIZER_SHA256
    from test_kaggle_source_graph_read import request
    async def forbidden(*a,**k):raise AssertionError('resident graph, model or PG must not run')
    monkeypatch.setattr(resident,'execute_source_graph_read',forbidden)
    query,baseline=sources()
    updated=baseline+'''
async def execute_source_graph_read(query,*,acquire):
    assert type(query) is SourceGraphReadRequest
    return SourceGraphReadResult(request_id=query.request_id,
        connection_generation=query.connection_generation,turn_generation=query.turn_generation,
        status='unavailable',reason='projection_lag',observed_data_seq=None,observed_projection_seq=None)
'''
    source=builder.bundle(query,updated,baseline)
    module=types.ModuleType('regional_knowledge._warm_bundle_test')
    module.__package__='regional_knowledge';module.__file__='<warm-handler>'
    monkeypatch.setitem(sys.modules,module.__name__,module)
    exec(compile(source,module.__file__,'exec'),module.__dict__)
    tokenizer=PinnedTokenizer(forbidden,REVISION,TOKENIZER_SHA256,SPACE)
    handler=module.KaggleWorkerQuery(acquire=forbidden,tokenizer=tokenizer)
    assert handler.acquire is forbidden and handler.tokenizer is tokenizer
    result=await handler(request().model_dump(mode='json'),encode=forbidden)
    assert result['reason']=='projection_lag' and result['type']=='SOURCE_GRAPH_READ_RESULT'
    assert resident.execute_source_graph_read is forbidden


def test_wire_abi_change_is_not_a_stateless_patch():
    query,graph=sources()
    with pytest.raises(ValueError,match='abi_changed'):
        builder.bundle(query,graph.replace("protocol_version:Literal[2]=2","protocol_version:Literal[3]=3",1),graph)
