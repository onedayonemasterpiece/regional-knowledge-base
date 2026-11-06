"""Synthetic ordered SQLite receipts; no corpus or executor effects."""
import base64
import hashlib
import json
import sqlite3
import pytest

from regional_knowledge.source_page_progress import enrichment_gaps


def test_empty_precondition_result_keeps_the_semantic_gap_readable():
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    db.execute('CREATE TABLE rkb_control_operations(sequence INTEGER,operation_key TEXT,source_id TEXT,'
               'person_id TEXT,seal_state TEXT,apply_state TEXT,command_bytes BLOB,outcome_bytes BLOB,progress_json TEXT)')
    stage = json.dumps({'ingestion_id':'ing','batch_index':0,
                       'model_payload':{'pages':[{'physical_page_index':4}]}}).encode()
    sha = hashlib.sha256(stage).hexdigest()
    for sequence, component, state, payload, outcome in (
            (1, 'stage', 'APPLIED', stage, None),
            (2, 'text', 'APPLIED', None, None),
            (3, 'semantic', 'REJECTED_PRECONDITION', None, json.dumps({'result_b64':''}))):
        payload = payload or json.dumps({'ingestion_id':'ing','batch_index':0,
                                        'component':component,'last_batch_sha256':sha}).encode()
        key = 'source-stage:ing:0' if component == 'stage' else 'source-packet:ing:0:'+component
        command = json.dumps({'payload_b64':base64.b64encode(payload).decode()})
        db.execute('INSERT INTO rkb_control_operations(sequence,operation_key,source_id,person_id,seal_state,apply_state,command_bytes,outcome_bytes) VALUES(?,?,?,?,?,?,?,?)',
                   (sequence,key,'s','u','ACCEPTED',state,command,outcome))
    assert enrichment_gaps(db, {'source_id':'s','person_id':'u','ingestion_id':'ing'}) == {4}
    db.execute("UPDATE rkb_control_operations SET outcome_bytes=? WHERE sequence=3",
               (json.dumps({'result_b64':base64.b64encode(b'broken').decode()}),))
    with pytest.raises(json.JSONDecodeError):
        enrichment_gaps(db, {'source_id':'s','person_id':'u','ingestion_id':'ing'})


def test_gap_projection_reads_each_stage_once_and_keeps_component_receipts():
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    db.execute('CREATE TABLE rkb_control_operations(sequence INTEGER,operation_key TEXT,source_id TEXT,'
               'person_id TEXT,seal_state TEXT,apply_state TEXT,command_bytes BLOB,outcome_bytes BLOB,progress_json TEXT)')
    seq = 0
    def add(key, doc, state='APPLIED', outcome=None, owner='u'):
        nonlocal seq
        seq += 1
        raw = json.dumps(doc).encode()
        result = None if outcome is None else json.dumps({'result_b64':base64.b64encode(json.dumps(outcome).encode()).decode()})
        db.execute('INSERT INTO rkb_control_operations(sequence,operation_key,source_id,person_id,seal_state,apply_state,command_bytes,outcome_bytes) VALUES(?,?,?,?,?,?,?,?)', (
            seq,key,'s',owner,'ACCEPTED',state,json.dumps({'payload_b64':base64.b64encode(raw).decode()}),result))
        return hashlib.sha256(raw).hexdigest()
    def packet(i, page, text='APPLIED', semantic=None, reason=None, owner='u'):
        sha = add(f'source-stage:ing:{i}', {'ingestion_id':'ing','batch_index':i,
            'model_payload':{'pages':[{'physical_page_index':page}]}}, owner=owner)
        for component,state in [('text',text),('semantic',semantic)]:
            if state:
                add(f'source-packet:ing:{i}:{component}', {'ingestion_id':'ing','batch_index':i,
                    'component':component,'last_batch_sha256':sha}, state,reason if component=='semantic' else None,owner)
    packet(0, 9)  # Reverse reading preserves physical identity.
    packet(1, 2, semantic='APPLIED')
    packet(2, 7, text='REJECTED_PRECONDITION')
    rollback={'reason':'native_source_sql_rolled_back','error_type':'ValueError','sqlstate':None}
    packet(3, 5, semantic='REJECTED_PRECONDITION', reason=rollback)  # one ordinary masked retry
    packet(4, 5, semantic='REJECTED_PRECONDITION', reason=rollback)  # second stays excluded
    packet(5, 8, semantic='REJECTED_PRECONDITION', reason=rollback)
    packet(6, 6, owner='other')
    queries=[]
    db.set_trace_callback(queries.append)
    assert enrichment_gaps(db, {'source_id':'s','person_id':'u','ingestion_id':'ing'}) == {9,8}
    scans=[q for q in queries if 'SELECT' in q and 'rkb_control_operations' in q]
    assert len(scans)==1, 'one gap calculation decoded each historical stage in two scans'
    # A valid source supports up to 2500 stages: stage+TEXT+SEM fits the
    # unchanged 10000-row bound in one pass, including the last open gap.
    from regional_knowledge.source_stage import MAX_SOURCE_PAGES
    for i in range(7, MAX_SOURCE_PAGES):
        packet(i, i, semantic='APPLIED' if i < MAX_SOURCE_PAGES-1 else None)
    assert enrichment_gaps(db, {'source_id':'s','person_id':'u','ingestion_id':'ing'}) == {MAX_SOURCE_PAGES-1}
