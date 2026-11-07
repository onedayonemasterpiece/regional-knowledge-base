import base64
import contextlib
import hashlib
import json
import sqlite3

from regional_knowledge.source_page_progress import read_accepted_model_page

# Columns of ControlSpool's rkb_control_operations read by source_page_progress; the SQL runs for real.
SCHEMA = '''CREATE TABLE rkb_control_operations(operation_key TEXT PRIMARY KEY,command_bytes BLOB NOT NULL,
    sequence INTEGER NOT NULL UNIQUE,source_id TEXT NOT NULL,person_id TEXT NOT NULL,
    seal_state TEXT NOT NULL,apply_state TEXT NOT NULL,outcome_bytes BLOB,progress_json TEXT);
    CREATE TABLE rkb_native_source_intake_state(ingestion_id TEXT PRIMARY KEY,phase TEXT NOT NULL);'''
ROW = {'source_id': 's', 'person_id': 'u', 'ingestion_id': 'ing', 'phase': 'staging'}


class _Queue:
    def __init__(self, phase='finalized'):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.db.execute('INSERT INTO rkb_native_source_intake_state VALUES(?,?)', ('ing', phase))
        self.sequence = 0

    def connect(self, write):
        return contextlib.nullcontext(self.db)

    def add(self, key, packet, state='APPLIED', owner=('s', 'u')):
        raw = json.dumps(packet).encode()
        self.sequence += 1
        self.db.execute('INSERT INTO rkb_control_operations(operation_key,command_bytes,sequence,source_id,person_id,seal_state,apply_state,outcome_bytes) VALUES(?,?,?,?,?,?,?,?)', (
            key, json.dumps({'payload_b64': base64.b64encode(raw).decode()}), self.sequence,
            *owner, 'ACCEPTED', state, None))
        return hashlib.sha256(raw).hexdigest()

    def stage(self, index, text, chunks, text_state='APPLIED', semantic_state=None, owner=('s', 'u')):
        page = {'page_id': 'p49', 'physical_page_index': 49,
                'regions': [{'region_key': 'r1', 'source_text': text}]}
        sha = self.add(f'source-stage:ing:{index}:{owner[1]}', {'ingestion_id': 'ing', 'batch_index': index, 'model_payload': {
            'pages': [page], 'chunks': [{'chunk_key': key, 'region_refs': [{'page_id': 'p49', 'region_key': 'r1'}]}
                                        for key in chunks]}}, owner=owner)
        for component, state in (('text', text_state), ('semantic', semantic_state)):
            if state:
                self.add(f'source-packet:ing:{index}:{owner[1]}:{component}', {'ingestion_id': 'ing',
                         'batch_index': index, 'component': component, 'last_batch_sha256': sha}, state, owner)
        return self


def _text(queue, index=49):
    found = read_accepted_model_page(queue, ROW, index)
    return found and (found['page']['regions'][0]['source_text'], [c['chunk_key'] for c in found['chunks']])


def test_newer_stage_with_rejected_text_does_not_replace_the_published_page():
    queue = _Queue().stage(0, 'old', ['a']).stage(1, 'new', ['b'], text_state='REJECTED_PRECONDITION')
    assert _text(queue) == ('old', ['a'])


def test_latest_text_applied_stage_wins():
    assert _text(_Queue().stage(0, 'old', ['a']).stage(1, 'accepted', ['c', 'd'])) == ('accepted', ['c', 'd'])


def test_staging_semantic_gap_gets_the_projection_before_finalize():
    assert _text(_Queue(phase='staging').stage(0, 'old', ['a'])) == ('old', ['a'])


def test_only_a_semantic_gap_gets_the_projection():
    assert _text(_Queue().stage(0, 'old', ['a'], semantic_state='APPLIED')) is None
    assert _text(_Queue().stage(0, 'old', ['a']), index=50) is None


def test_another_owners_journal_is_ignored():
    queue = _Queue().stage(0, 'old', ['a']).stage(1, 'foreign', ['x'], owner=('s', 'other'))
    assert _text(queue) == ('old', ['a'])


def test_accepted_page_result_withholds_the_next_cursor():
    # The model must restage this gap page first; following next_cursor skipped page 49 of Bau.
    from types import SimpleNamespace
    from regional_knowledge.server import _book_page_tool_result
    batch = SimpleNamespace(next_cursor='50', pages=[])
    manifest = lambda accepted: json.loads(_book_page_tool_result('ing', batch, accepted).content[0].text)
    assert manifest(None)['next_cursor'] == '50'
    assert manifest({'page': {}, 'chunks': []})['next_cursor'] is None
