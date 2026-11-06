import base64
import json

from regional_knowledge.source_page_progress import accepted_model_page


class _Db:
    def __init__(self, packets):
        self.rows = [{'command_bytes': json.dumps({'payload_b64': base64.b64encode(
            json.dumps(p).encode()).decode()})} for p in packets]

    def execute(self, sql, params):
        assert "apply_state='APPLIED'" in sql and params[2] == 'source-stage:ing:%'
        return self.rows


def _packet(page_index, text, chunks):
    page_id = f'p{page_index}'
    return {'ingestion_id': 'ing', 'model_payload': {
        'pages': [{'page_id': page_id, 'physical_page_index': page_index,
                   'regions': [{'region_key': 'r1', 'source_text': text}]}],
        'chunks': [{'chunk_key': key, 'region_refs': [{'page_id': page_id, 'region_key': 'r1'}]}
                   for key in chunks]}}


ROW = {'source_id': 's', 'person_id': 'u', 'ingestion_id': 'ing'}


def test_semantic_retry_gets_the_exact_latest_accepted_page_and_its_chunks():
    db = _Db([_packet(49, 'old', ['a']), _packet(50, 'other', ['b']), _packet(49, 'accepted', ['c', 'd'])])
    found = accepted_model_page(db, ROW, 49)
    assert found['page']['regions'][0]['source_text'] == 'accepted'
    assert [c['chunk_key'] for c in found['chunks']] == ['c', 'd']


def test_unaccepted_page_has_no_accepted_projection():
    assert accepted_model_page(_Db([_packet(50, 'other', ['b'])]), ROW, 49) is None
