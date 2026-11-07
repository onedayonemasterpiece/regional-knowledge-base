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


@pytest.mark.asyncio
async def test_warm_query_uses_reviewed_executor_and_existing_tokenizer_and_stage_collector(monkeypatch):
    import asyncio
    from regional_knowledge import kaggle_query as resident
    from regional_knowledge import kaggle_search as resident_search
    from regional_knowledge.query_telemetry import ReadRuntime, bind_read_runtime
    from test_kaggle_worker_query import Lease, frame, pinned, VECTOR
    async def forbidden(*args, **kwargs):
        raise AssertionError('resident query executor must remain untouched')
    monkeypatch.setattr(resident, 'execute_query', forbidden)
    monkeypatch.setattr(resident_search, 'fused_search', forbidden)
    worker, graph = sources()
    executor = (ROOT/'src/regional_knowledge/kaggle_query.py').read_text()
    search = (ROOT/'src/regional_knowledge/kaggle_search.py').read_text()
    source = builder.bundle(worker, graph, graph, executor, executor, search_source=search)
    module = types.ModuleType('regional_knowledge._warm_query_bundle_test')
    module.__package__ = 'regional_knowledge'; module.__file__ = '<warm-query-handler>'
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
    lease, tokenizer, collector = Lease(), pinned(), asyncio.Queue(maxsize=8)
    handler = module.KaggleWorkerQuery(acquire=lease.acquire, tokenizer=tokenizer, telemetry=collector)
    async def encode(text):return VECTOR
    with bind_read_runtime(ReadRuntime('same-run', 'same-incarnation')):
        result = await handler(frame(), encode=encode)
    assert result['status'] == 'ok' and result['reason'] is None
    assert handler.tokenizer is tokenizer and handler.telemetry is collector
    assert lease.entered == lease.exited == 1 and resident.execute_query is forbidden
    assert resident_search.fused_search is forbidden
    terminal = next(event for event in result['read_events'] if event['terminal'])
    assert {'encode_ms', 'lexical_ms', 'dense_ms', 'hydrate_ms'} <= terminal['stages_ms'].keys()
    with pytest.raises(ValueError, match='tokenizer_abi_changed'):
        builder.bundle(worker, graph, graph, executor.replace('tokenizer_sha256: str', 'tokenizer_sha256: int'), executor)


@pytest.mark.asyncio
async def test_warm_exact_fetch_uses_reviewed_executor_without_replacing_resident_module(monkeypatch):
    from regional_knowledge import kaggle_query as resident
    from test_kaggle_exact_fetch import ExactConnection, locator, request
    from test_kaggle_worker_query import Lease, pinned, forbidden_encode

    async def forbidden(*args, **kwargs):
        raise AssertionError('exact fetch must use the reviewed warm executor')

    monkeypatch.setattr(resident, 'execute_passage_read', forbidden)
    worker, graph = sources()
    executor = (ROOT/'src/regional_knowledge/kaggle_query.py').read_text()
    source = builder.bundle(worker, graph, graph, executor, executor)
    module = types.ModuleType('regional_knowledge._warm_exact_bundle_test')
    module.__package__ = 'regional_knowledge'; module.__file__ = '<warm-exact-handler>'
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
    lease, tokenizer = Lease(ExactConnection()), pinned(forbidden)
    handler = module.KaggleWorkerQuery(acquire=lease.acquire, tokenizer=tokenizer)
    result = await handler(request(locator(visibility_epoch=2)).model_dump(mode='json'),
        encode=forbidden_encode)
    assert result['status'] == 'ok' and result['reason'] is None
    assert result['evidence'][0]['text'] == 'x' * 8000
    assert lease.entered == lease.exited == 1
    assert handler.acquire is lease.acquire or handler.acquire == lease.acquire
    assert handler.tokenizer is tokenizer and resident.execute_passage_read is forbidden


@pytest.mark.asyncio
async def test_warm_case_uses_reviewed_reader_and_resident_wire_types_without_module_replacement(monkeypatch):
    from regional_knowledge import kaggle_case_read as resident
    from test_kaggle_case_read import Connection,request
    from test_kaggle_worker_query import Lease,pinned,forbidden_encode
    from test_disagreement_native import CASE

    async def forbidden(*args,**kwargs):
        raise AssertionError('Case read must use the isolated reviewed reader')

    monkeypatch.setattr(resident,'execute_case_read',forbidden)
    worker,graph=sources()
    case=(ROOT/'src/regional_knowledge/kaggle_case_read.py').read_text()
    source=builder.bundle(worker,graph,graph,case_source=case)
    module=types.ModuleType('regional_knowledge._warm_case_bundle_test')
    module.__package__='regional_knowledge';module.__file__='<warm-case-handler>'
    monkeypatch.setitem(sys.modules,module.__name__,module)
    exec(compile(source,module.__file__,'exec'),module.__dict__)
    record=dict(case_id=CASE,case_revision=1,acl_revision=1,event_seq=1,
        source_dependencies=[],section='case')
    lease=Lease(Connection(record));tokenizer=pinned(forbidden)
    handler=module.KaggleWorkerQuery(acquire=lease.acquire,tokenizer=tokenizer)
    result=await handler(request().model_dump(mode='json'),encode=forbidden_encode)
    assert result['status']=='ok'
    assert lease.entered==lease.exited==1
    assert handler.acquire==lease.acquire and handler.tokenizer is tokenizer
    assert resident.execute_case_read is forbidden

@pytest.mark.asyncio
@pytest.mark.parametrize('order', [(0, 1), (64, 126, 125)])
async def test_warm_source_pdf_uses_reviewed_folio_with_old_resident_renderer(monkeypatch, order):
    """One compiled warm handler: old resident rejects 135; reviewed chain returns exact MCP PDF."""
    import base64
    import re
    from regional_knowledge import (kaggle_source_read as resident_read,
        source_unit_export as resident_export, source_unit_pdf as resident_pdf,
        kaggle_worker_query as resident_worker)
    from regional_knowledge.contracts import RegionKind
    from regional_knowledge.kaggle_public_backend import source_unit_pdf_result
    from test_kaggle_worker_query import Lease, pinned, forbidden_encode
    from test_source_unit_export_p0b import Connection, fixture_pages, SOURCE

    pymupdf = pytest.importorskip('pymupdf')
    folder = ROOT / 'src/regional_knowledge'
    read = lambda name: (folder / (name + '.py')).read_text()
    worker, graph = sources()
    common = (worker, graph, graph, read('kaggle_query'), read('kaggle_query'))
    case = read('kaggle_case_read')

    def old_renderer(**kwargs):
        if any(region.kind == RegionKind.PAGE_NUMBER for region in kwargs['regions']):
            raise resident_pdf.SourceUnitPdfError('unsupported_structure:page_number')
        return resident_pdf.render_source_unit_pdf(**kwargs)

    monkeypatch.setattr(resident_export, 'render_source_unit_pdf', old_renderer)
    pages = fixture_pages(complete=True, order=order)
    pages[0]['regions'][-1].update(kind='page_number', source_text='135')

    async def frontier(cursor, execute, request, observed, require_dense=False):
        observed.update(data=1, projection=1, visibility=1, database='gen1')

    from regional_knowledge.kaggle_source_read import SourceReadRequest
    request = SourceReadRequest(request_id='warm-pdf-135',
        connection_generation=1, turn_generation=0, operation='source_unit_pdf',
        source_id=SOURCE, article_id='albinus-unit', limit=1, min_data_seq=0,
        deadline_ms=2000, compute_claim='same-resident-session',
        actor=dict(person_id='reader', audience='personal', auth_epoch=1,
            visibility_epoch=0, context_ref='test-reader'))

    def compiled(source):
        # Keep sys.modules and the existing resident module untouched.
        module = types.ModuleType(resident_worker.__name__)
        module.__package__ = 'regional_knowledge'
        module.__file__ = '<warm-pdf-handler>'
        exec(compile(source, module.__file__, 'exec'), module.__dict__)
        return module

    async def run(module):
        connection = Connection(pages)
        connection.cleaned = True  # Existing Lease fixture requires this synthetic cleanup marker.
        lease = Lease(connection)
        handler = module.KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned(forbidden_encode))
        result = await handler(request.model_dump(mode='json'), encode=forbidden_encode)
        assert lease.entered == lease.exited == 2
        return result

    monkeypatch.setattr(resident_read, '_frontier', frontier)
    old = compiled(builder.bundle(*common, case_source=case))
    # The older installed renderer is still resident even when reviewed sources are current.
    old_result = await run(old)
    assert old_result['status'] == 'unavailable'
    assert old_result['reason'] == 'pdf_render_unsupported'

    reviewed = compiled(builder.bundle(*common, case_source=case,
        source_read_source=read('kaggle_source_read'),
        baseline_source_read_source=read('kaggle_source_read'),
        source_export_source=read('source_unit_export'),
        source_pdf_source=read('source_unit_pdf'),
        baseline_source_pdf_source=read('source_unit_pdf')))
    assert reviewed._warm_source_read['SourceReadRequest'] is resident_read.SourceReadRequest
    assert reviewed._warm_source_read['SourceReadResult'] is resident_read.SourceReadResult
    assert reviewed._warm_source_pdf['SourceUnitPdfError'] is resident_pdf.SourceUnitPdfError
    assert reviewed._warm_source_read['execute_source_read'] is not resident_read.execute_source_read
    assert reviewed._warm_source_export['render_accepted_source_pdf'] is not resident_export.render_accepted_source_pdf
    assert reviewed._warm_source_export['render_source_unit_pdf'] is reviewed._warm_source_pdf['render_source_unit_pdf']
    reviewed._warm_source_read['_frontier'] = frontier
    updated = await run(reviewed)
    assert updated['status'] == 'ok' and len(updated['records']) == 1
    result = source_unit_pdf_result(updated['records'][0])
    resource = result.content[1].resource
    assert resource.mime_type == 'application/pdf'
    with pymupdf.open(stream=base64.b64decode(resource.blob), filetype='pdf') as pdf:
        authored = ''.join(block[4] for page in list(pdf)[1:]
            for block in page.get_text('blocks')
            if block[6] == 0 and 49 <= block[1] < page.rect.height - 43)
        authored = authored.removeprefix('Немецкая статья / Eintrag\n')
        expected = ''.join(region['source_text'] for page in pages for region in page['regions'])
        assert re.sub(r'\s+', '', authored) == re.sub(r'\s+', '', expected)
        assert sum(word[4] == '135' for page in pdf for word in page.get_text('words')) == 1
        assert pdf[-1].get_text().endswith(str(len(pdf)) + '\n')
    # The isolated renderer raises the resident exception type, so the existing
    # native SOURCE_READ catch still returns a coarse, payload-free failure.
    pages[0]['regions'][0]['kind'] = 'footnote'
    unsupported = await run(reviewed)
    assert unsupported['status'] == 'unavailable' and unsupported['reason'] == 'pdf_render_unsupported'
    assert not unsupported['records']
    assert resident_export.render_source_unit_pdf is old_renderer
