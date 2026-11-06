import base64
import hashlib
import json

from regional_knowledge.source_page_progress import accepted_model_page, read_accepted_model_page


def _stage(index, page_index, text, chunks):
    page_id = f'p{page_index}'
    return {'ingestion_id': 'ing', 'batch_index': index, 'model_payload': {
        'pages': [{'page_id': page_id, 'physical_page_index': page_index,
                   'regions': [{'region_key': 'r1', 'source_text': text}]}],
        'chunks': [{'chunk_key': key, 'region_refs': [{'page_id': page_id, 'region_key': 'r1'}]}
                   for key in chunks]}}


def _row(key, packet, state='APPLIED'):
    raw = json.dumps(packet).encode()
    return {'operation_key': key, 'apply_state': state, 'outcome_bytes': None,
            'command_bytes': json.dumps({'payload_b64': base64.b64encode(raw).decode()})}, hashlib.sha256(raw).hexdigest()


def _journal(*stages):
    """stages: (index, page, text, chunks, text_state, semantic_state|None) in journal order."""
    rows = []
    for index, page, text, chunks, text_state, semantic_state in stages:
        stage, sha = _row(f'source-stage:ing:{index}:{text}', _stage(index, page, text, chunks))
        rows.append(stage)
        for component, state in (('text', text_state), ('semantic', semantic_state)):
            if state:
                rows.append(_row(f'source-packet:ing:{index}:{text}:{component}', {
                    'ingestion_id': 'ing', 'batch_index': index, 'component': component,
                    'last_batch_sha256': sha}, state)[0])
    return rows


class _Db:
    def __init__(self, rows, phase='finalized'):
        self.rows, self.phase = rows, phase

    def execute(self, sql, params):
        if 'rkb_native_source_intake_state' in sql:
            return _Result([{'phase': self.phase}])
        rows = [r for r in self.rows
                if any(r['operation_key'].startswith(p.rstrip('%').split('%')[0]) and
                       r['operation_key'].endswith(p.split('%')[-1]) for p in params[2:])
                and ('APPLIED' == r['apply_state'] or "REJECTED_PRECONDITION" in sql)]
        return list(reversed(rows)) if 'DESC' in sql else rows


class _Result(list):
    def fetchone(self):
        return self[0] if self else None


class _Queue:
    def __init__(self, db):
        self.db = db

    def connect(self, write):
        import contextlib
        return contextlib.nullcontext(self.db)


ROW = {'source_id': 's', 'person_id': 'u', 'ingestion_id': 'ing', 'phase': 'staging'}


def test_newer_stage_with_rejected_text_does_not_replace_the_published_page():
    db = _Db(_journal((0, 49, 'old', ['a'], 'APPLIED', None),
                      (1, 49, 'new', ['b'], 'REJECTED_PRECONDITION', None)))
    found = accepted_model_page(db, ROW, 49)
    assert found['page']['regions'][0]['source_text'] == 'old'
    assert [c['chunk_key'] for c in found['chunks']] == ['a']


def test_latest_text_applied_stage_wins():
    db = _Db(_journal((0, 49, 'old', ['a'], 'APPLIED', None), (1, 49, 'accepted', ['c', 'd'], 'APPLIED', None)))
    assert [c['chunk_key'] for c in accepted_model_page(db, ROW, 49)['chunks']] == ['c', 'd']


def test_only_a_finalized_semantic_gap_gets_the_projection():
    gap = _journal((0, 49, 'old', ['a'], 'APPLIED', None))
    assert read_accepted_model_page(_Queue(_Db(gap)), ROW, 49)['page']['page_id'] == 'p49'
    assert read_accepted_model_page(_Queue(_Db(gap, phase='staging')), ROW, 49) is None
    done = _journal((0, 49, 'old', ['a'], 'APPLIED', 'APPLIED'))
    assert read_accepted_model_page(_Queue(_Db(done)), ROW, 49) is None
    assert read_accepted_model_page(_Queue(_Db(gap)), ROW, 50) is None
