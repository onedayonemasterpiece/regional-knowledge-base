"""Synthetic metadata projection: no private corpus and no executor replay."""
import hashlib
import json
import sqlite3

from regional_knowledge.source_page_progress import publication_progress, enrichment_gaps


def test_progress_keeps_stage_hash_and_rejection_semantics_without_decoding_raw_models():
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    db.execute('CREATE TABLE rkb_control_operations(sequence INTEGER, operation_key TEXT, source_id TEXT,'
               'person_id TEXT, seal_state TEXT, apply_state TEXT, command_bytes BLOB, outcome_bytes BLOB, progress_json TEXT)')
    seq = 0
    def add(index, page, component, state='APPLIED', *, sha=None, reason=None, person='u'):
        nonlocal seq
        seq += 1
        stage = component == 'stage'
        sha = sha or hashlib.sha256(str((index,page)).encode()).hexdigest()
        compact = dict(ingestion_id='ing',batch_index=index,component=component,
                       stage_sha256=sha, last_batch_sha256=sha,
                       model_payload={'pages':[{'physical_page_index':page}]} if stage else {},
                       rejection=reason or {})
        key = f'source-stage:ing:{index}' if stage else f'source-packet:ing:{index}:{component}'
        # A retired payload is not an input to progress. The identity/receipt facts remain.
        db.execute('INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?,?,?,?)',
                   (seq,key,'s',person,'ACCEPTED',state,b'raw not consulted',b'raw not consulted',json.dumps(compact)))
        return sha
    for i,page in enumerate((9,2,5,5,8)):
        sha = add(i,page,'stage')
        add(i,page,'text',sha=sha)
        if page == 2: add(i,page,'semantic',sha=sha)
        if page in (5,8):
            add(i,page,'semantic','REJECTED_PRECONDITION',sha=sha,
                reason=dict(reason='native_source_sql_rolled_back',error_type='ValueError',sqlstate=None))
    add(6,1,'stage',person='other');add(6,1,'text',person='other')
    sha = add(7,7,'stage');add(7,7,'text',sha='wrong parent hash')
    row = dict(source_id='s',person_id='u',ingestion_id='ing')
    assert publication_progress(db,row)[0] == {9,2,5,8}
    assert publication_progress(db,row,'semantic')[0] == {2}
    assert enrichment_gaps(db,row) == {9,8}  # second masked rollback stays excluded


def test_migration_fills_once_without_rebuilding_history_on_warm_open():
    import base64
    from regional_knowledge.source_page_progress import initialize_progress_receipts
    db = sqlite3.connect(':memory:');db.row_factory = sqlite3.Row
    db.execute('CREATE TABLE rkb_control_operations(operation_key TEXT,command_bytes BLOB,outcome_bytes BLOB)')
    page = {'physical_page_index':4,'source_text':'synthetic private text ' * 10000}
    raw = json.dumps(dict(ingestion_id='ing',batch_index=0,model_payload={'pages':[page]})).encode()
    db.execute('INSERT INTO rkb_control_operations VALUES(?,?,NULL)',
               ('source-stage:ing:0',json.dumps({'payload_b64':base64.b64encode(raw).decode()})))
    initialize_progress_receipts(db)
    saved = db.execute('SELECT command_bytes,progress_json FROM rkb_control_operations').fetchone()
    assert 'synthetic private text' not in saved['progress_json']
    assert len(saved['progress_json']) < 300
    assert json.loads(saved['progress_json'])['stage_sha256'] == hashlib.sha256(raw).hexdigest()
    trace=[];db.set_trace_callback(trace.append)
    initialize_progress_receipts(db)
    assert not any('SELECT operation_key' in sql or 'UPDATE ' in sql for sql in trace)
    assert db.execute('SELECT command_bytes,progress_json FROM rkb_control_operations').fetchone() == saved
