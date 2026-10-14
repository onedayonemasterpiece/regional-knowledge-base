"""Source-neutral transport tests: no real books, secret URLs or automatic review."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from regional_knowledge.contracts import BookIngestOutput, ChatFile, Principal
from regional_knowledge.file_ingress import DownloadedFile
from regional_knowledge.stage_file import MAX_STAGE_FILE_BYTES, parse_stage_file, read_stage_file
from regional_knowledge.ingestion import native_encoding_notice


@pytest.fixture
def batch():
    return {'ingestion_id': str(uuid4()), 'source_revision': 1,
        'pages': [{'page_id': str(uuid4()), 'physical_page_index': 0,
                   'source_material': 'visual_reviewed', 'source_review_note': 'Synthetic visual review',
                   'knowledge_reviewed': True, 'regions': [
                       {'region_key': 'body', 'kind': 'body',
                        'bbox': {'left': 10, 'top': 10, 'right': 900, 'bottom': 900},
                        'reading_order': 0, 'source_text': 'Источник сообщает об открытии школы.'}]}]}


def encode(value):
    return json.dumps(value, ensure_ascii=False).encode('utf-8')


def test_parse_retains_exact_model_content_and_never_infers_review(batch):
    payload = parse_stage_file(encode(batch), batch['ingestion_id'], 1)
    assert payload['pages'][0]['regions'][0]['source_text'] == batch['pages'][0]['regions'][0]['source_text']
    assert payload['pages'][0]['knowledge_reviewed'] is True
    assert 'ingestion_id' not in payload and 'source_revision' not in payload
    batch['pages'][0].pop('knowledge_reviewed')
    payload = parse_stage_file(encode(batch), batch['ingestion_id'], 1)
    assert payload['pages'][0]['knowledge_reviewed'] is False
    assert payload['pages'][0]['story_candidates_reviewed'] is False


def test_file_binds_exact_job_revision_and_rejects_control_fields(batch):
    with pytest.raises(ValueError, match='mismatch'):
        parse_stage_file(encode(batch), str(uuid4()), 1)
    with pytest.raises(ValueError, match='mismatch'):
        parse_stage_file(encode(batch), batch['ingestion_id'], 2)
    for key in ['command', 'document_id', 'file', 'metadata']:
        with pytest.raises(ValueError):
            parse_stage_file(encode({**batch, key: 'not-a-stage-field'}), batch['ingestion_id'], 1)


def test_schema_diagnostic_does_not_echo_private_input_or_extra_keys(batch):
    from regional_knowledge.stage_file import StageFileError
    private = 'DO-NOT-RETURN-PRIVATE-SOURCE'
    bad = {**batch, private: private}
    with pytest.raises(StageFileError) as error:
        parse_stage_file(encode(bad), batch['ingestion_id'], 1)
    assert private not in str(error.value)
    assert 'extra_forbidden' in str(error.value)
    batch['pages'][0]['physical_page_index'] = -1
    with pytest.raises(StageFileError) as error:
        parse_stage_file(encode(batch), batch['ingestion_id'], 1)
    assert 'pages.0.physical_page_index' in str(error.value)


def test_file_is_bounded_and_json_is_unambiguous(batch):
    with pytest.raises(ValueError):
        parse_stage_file(encode({**batch, 'pages': batch['pages']*9}), batch['ingestion_id'], 1)
    for invalid in [b'', b' '* (MAX_STAGE_FILE_BYTES+1), b'%PDF-', b'\xff',
                    b'{"pages":[],"pages":[]}', b'{"source_revision":NaN}']:
        with pytest.raises(ValueError):
            parse_stage_file(invalid, batch['ingestion_id'], 1)


class Downloader:
    def __init__(self, data):
        self.data = data
        self.calls = []
        self.path = None
    async def download(self, url, directory, *, max_bytes=None):
        self.calls.append((url, max_bytes))
        self.path = Path(directory)/'stage.json'
        self.path.write_bytes(self.data)
        return DownloadedFile(self.path, hashlib.sha256(self.data).hexdigest(), len(self.data))


@pytest.mark.asyncio
async def test_authority_precedes_download_and_temp_is_removed(batch, tmp_path, monkeypatch):
    monkeypatch.setenv('RKB_WORK_DIR', str(tmp_path))
    actor = Principal(subject=str(uuid4()), client_id='synthetic', issuer='synthetic', access_token='not-a-secret')
    row = {'id': batch['ingestion_id'], 'staged_revision': 1, 'owner_user_id': actor.subject}
    downloader = Downloader(encode(batch))
    async def lookup(**kwargs):
        return row
    backend = SimpleNamespace(_ingestion_row=lookup, file_downloader=downloader)
    file = ChatFile(download_url='https://example.org/synthetic-stage.json', file_id='synthetic', mime_type='application/json')
    payload, sha = await read_stage_file(backend, actor, row['id'], file)
    assert payload['pages'][0]['knowledge_reviewed'] is True
    assert downloader.calls == [(str(file.download_url), MAX_STAGE_FILE_BYTES)]
    assert sha == hashlib.sha256(encode(batch)).hexdigest()
    assert not downloader.path.exists() and not list(tmp_path.iterdir())
    row['owner_user_id'] = str(uuid4())
    with pytest.raises(PermissionError):
        await read_stage_file(backend, actor, row['id'], file)
    assert len(downloader.calls) == 1
    row['owner_user_id'] = actor.subject
    with pytest.raises(ValueError, match='not_original_source'):
        await read_stage_file(backend, actor, row['id'], file.model_copy(update={'mime_type': 'application/pdf'}))
    assert len(downloader.calls) == 1
    downloader.data = b'not json'
    with pytest.raises(ValueError):
        await read_stage_file(backend, actor, row['id'], file)
    assert not downloader.path.exists() and not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_mcp_file_and_inline_stage_share_backend_and_cannot_mix(batch, monkeypatch):
    from regional_knowledge import server as module
    actor = Principal(subject=str(uuid4()), client_id='synthetic', issuer='synthetic', access_token='not-a-secret')
    monkeypatch.setenv('RKB_DEV_NOAUTH', '1')
    monkeypatch.setattr(module, '_principal', lambda: actor)
    calls = []
    async def ingest(**kwargs):
        calls.append(kwargs)
        return BookIngestOutput(state='processing', ingestion_id=batch['ingestion_id'], message='Synthetic stored')
    backend = SimpleNamespace(book_ingest=ingest)
    server = module.build_server(backend=backend)
    from regional_knowledge import stage_file
    async def from_file(*args):
        return parse_stage_file(encode(batch), batch['ingestion_id'], 1), 'a'*64
    monkeypatch.setattr(stage_file, 'read_stage_file', from_file)
    args = {'command': 'stage', 'ingestion_id': batch['ingestion_id'], 'file': {
        'download_url': 'https://example.org/stage.json', 'file_id': 'synthetic'}}
    await server._tool_manager.call_tool('book_ingest', args, None)
    assert len(calls) == 1 and calls[0]['file'] is None
    assert calls[0]['payload']['pages'][0]['knowledge_reviewed'] is True
    with pytest.raises(Exception, match='cannot_mix'):
        await server._tool_manager.call_tool('book_ingest', {**args, 'pages': []}, None)
    assert len(calls) == 1
    # Same old inline operation remains intact; no file download path involved.
    await server._tool_manager.call_tool('book_ingest', {
        'command': 'stage', 'ingestion_id': batch['ingestion_id'], 'pages': batch['pages']}, None)
    assert len(calls) == 2 and calls[1]['payload']['pages'][0]['knowledge_reviewed'] is True


def test_missing_cid_unicode_warns_without_guessing_or_blocking():
    doc = SimpleNamespace(xref_get_key=lambda x, _k: ('null','null') if x==7 else ('xref','9 0 R'))
    page = SimpleNamespace(get_fonts=lambda **_: [
        (7, 'ttf', 'Type0'), (7, 'ttf', 'Type0'), (8, 'ttf', 'Type0'), (10, 'ttf', 'TrueType')])
    notice = native_encoding_notice(doc, page)
    assert notice['code'] == 'cid_font_without_unicode_map'
    assert notice['font_xrefs'] == [7] and notice['font_count'] == 1
    assert 'original PDF' in notice['guidance']
    assert native_encoding_notice(doc, SimpleNamespace(get_fonts=lambda **_: [])) is None
