"""Two-page native-stage fixture to report proof; no provider or book data."""
import hashlib
import importlib.util
import json
from pathlib import Path

from regional_knowledge import product_telemetry as telemetry
from regional_knowledge.source_stage import ZERO_HASH, canonical, digest, validate_stage_payload
from test_source_stage import INGESTION, SOURCE, batch


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'import_timeline.py'
spec = importlib.util.spec_from_file_location('rkb_import_timeline', SCRIPT)
timeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(timeline)


def test_two_native_fixture_pages_have_independent_receipts_and_private_values_vanish(tmp_path):
    ingestion_ref = hashlib.sha256(INGESTION.encode()).hexdigest()
    source_ref = hashlib.sha256(SOURCE.encode()).hexdigest()
    spool = telemetry.SegmentedLogSink(tmp_path, segment_bytes=1024,
                                       disk_limit_bytes=32 * 1024 * 1024)
    base = 1_800_000_000_000
    all_emitted = []
    previous = ZERO_HASH

    def emit(event, phase, status, at, **fields):
        record = {'event': event, 'phase': phase, 'status': status,
                  'ingestion_ref': ingestion_ref, 'source_ref': source_ref,
                  'event_time_unix_ms': at,
                  'source_text': 'PRIVATE_SYNTHETIC_TEXT_MUST_NOT_LEAK', **fields}
        clean = telemetry._clean(record, 'standard')
        clean['loss_before'] = {'dropped': 0, 'failed': 0}
        spool(clean)
        all_emitted.append(clean)

    # Each stage is the EXISTING validated synthetic native packet shape,
    # with one model-reviewed physical page and a separate batch index.
    for number in (0, 1):
        source = batch(index=number, previous=previous, start=number, count=1)
        assert validate_stage_payload(canonical(source), source_id=SOURCE) == source
        previous = digest(canonical(source))
        payload = source['model_payload']
        page = payload['pages'][0]['physical_page_index']
        assert page == number and payload['chunks']
        t = base + number * 1000
        stage, text, semantic = [hashlib.sha256(f'{kind}-{number}'.encode()).hexdigest()
                                 for kind in ('stage', 'text', 'semantic')]
        emit('import.call', 'total', 'returned', t + 100, tool='book_pages',
             trace_id=f'pages{number}', physical_page_index=page,
             page_count=1, image_bytes=200_000, call_started_unix_ms=t,
             duration_ms=100)
        emit('import.phase', 'admission', 'staged', t + 210,
             trace_id=f'stage{number}', component='stage', operation_ref=stage,
             packet_bytes=4500, command_sequence=number*3+1)
        emit('import.phase', 'admission', 'staged', t + 220,
             trace_id=f'stage{number}', component='text', operation_ref=text,
             command_sequence=number*3+2)
        emit('import.call', 'total', 'returned', t + 250,
             trace_id=f'stage{number}', tool='book_ingest', command='stage',
             physical_page_index=page,physical_page_indices=[page],page_count=1,batch_index=number,
             operation_ref=text,  # Nested batch overwrote outer request metadata.
             region_count=len(payload['pages'][0]['regions']),
             chunk_count=len(payload['chunks']), story_candidate_count=0,
             unit_marker_count=0, next_action='wait', result_state='processing',
             call_started_unix_ms=t + 150, duration_ms=100)
        emit('import.call', 'total', 'returned', t + 280,
             trace_id=f'status{number}', tool='book_ingest', command='status',
             call_started_unix_ms=t + 260, duration_ms=20)
        emit('command.phase', 'seal', 'acknowledged', t + 290, operation_ref=stage)
        emit('import.phase', 'stage', 'applied', t + 300,
             operation_ref=stage, batch_count=number + 1)
        emit('import.phase', 'text', 'applied', t + 350,
             operation_ref=text, batch_count=number + 1)
        emit('import.phase', 'admission', 'staged', t + 360,
             trace_id=f'status{number}', component='semantic', operation_ref=semantic)
        emit('import.phase', 'semantic', 'applied', t + 420,
             operation_ref=semantic, batch_count=number + 1)
        # An explicit per-page fetch is currently not guaranteed by the SDK.
        # Here it is present to exercise positive correlation without guessing.
        emit('import.call', 'total', 'returned', t + 470,
             trace_id=f'fetch{number}', tool='fetch', physical_page_index=page,
             ingestion_ref=None, physical_page_end=page,
             call_started_unix_ms=t + 440, duration_ms=30)
        for n, op in enumerate((stage, text, semantic)):
            emit('command.phase', 'saved', 'saved', t + 510 + n * 5,
                 operation_ref=op, ingestion_ref=None,source_ref=None,
                 command_sequence=number*3+n+1)

    spool.seal()
    raw = b''.join(path.read_bytes() for path in (tmp_path / 'sealed').glob('*.jsonl'))
    assert b'PRIVATE_SYNTHETIC' not in raw and b'source_text' not in raw
    records, bad = timeline.load(tmp_path)
    rows, summary = timeline.report(records, ingestion_ref)
    csv_text, markdown = timeline.render(rows, summary)
    assert bad == 0 and len(rows) == 2 and summary['pages_observed'] == 2
    assert [r['page'] for r in rows] == [0, 1]
    for row in rows:
        assert row['image_bytes'] == 200_000 and row['packet_bytes'] == 4500
        assert row['stage_accepted'] == 'yes'
        assert (row['stage_applied'], row['text_applied'], row['semantic_applied']) == ('yes',)*3
        assert row['saved'] == 'yes' and row['status_observed'] == 1
        assert row['fetch_observed'] == 1
    assert 'PRIVATE_SYNTHETIC' not in csv_text + markdown
    assert ',yes,yes,yes,yes,' in csv_text

    # Missing receipt must NEVER be interpreted as a negative or zero.
    missing = [e for e in records if not (e.get('event') == 'command.phase'
                and e.get('phase') == 'saved' and e.get('operation_ref') ==
                hashlib.sha256(b'semantic-1').hexdigest())]
    rows2, summary2 = timeline.report(missing, ingestion_ref)
    assert rows2[1]['saved'] is None and rows2[0]['saved'] == 'yes'
    assert summary2['orphan_apply_events'] == 0

    # A single real stage packet may cover two (even nonadjacent) pages.
    later_ops = {hashlib.sha256(f'{kind}-1'.encode()).hexdigest()
                 for kind in ('stage', 'text', 'semantic')}
    combined = [dict(e) for e in records if e.get('trace_id') not in ('stage1','status1')
                and e.get('operation_ref') not in later_ops]
    for event in combined:
        if event.get('trace_id') == 'stage0' and event.get('event') == 'import.call':
            event['physical_page_indices'], event['page_count'] = [0,1], 2
    shared_rows, shared_summary = timeline.report(combined, ingestion_ref)
    assert len(shared_rows) == 2 and all(r['saved'] == 'yes' for r in shared_rows)
    assert shared_summary['packet_counts_are_shared'] is True

    # Total volume estimate for the fixture and a 200-page book: no rotation
    # or arbitrary deletion is needed for this event rate.
    assert sum(len(json.dumps(e)) for e in all_emitted) * 100 < 32 * 1024 * 1024
