"""Current importer/control budget regression: terminal receipts must not consume active slots.

The existing owner OAuth, BgeQueue SQLite, native intake and original acquisition
interfaces are real classes; the *external original* is an existing synthetic
fixture. Explicit fixture-only terminal phase transitions below simulate prior
already-settled books; they do NOT claim a provider/remote accepted outcome,
Dataset Saved or a model run. No control row or terminal identity is deleted.
"""
import json
from types import MethodType, SimpleNamespace
from uuid import uuid4

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.kaggle_importer import MAX_CONTROL_BYTES, MAX_CONTROL_ROWS
from regional_knowledge.contracts import ChatFile
from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.original_intake import OriginalIntakeError, canonical, digest
from regional_knowledge.original_intake_controller import OriginalIntakeController
from test_kaggle_native_source_importer import native_intake, start
from test_kaggle_source_importer import options
# Register the real dependency fixtures of the existing native importer.
from test_kaggle_public_backend import target
from test_participants import people
from test_read_authority import auth


def _terminal_fixture(s, ingestion_id, *, extra=None, failed=False):
    """Exercise production _save, rather than insert handcrafted SQLite rows.

    Simulates a *previously* durable finished control phase after its external
    journal result was verified. This unit boundary does not supply that proof.
    """
    row = s.importer._row(s.principal.subject, ingestion_id)
    changes = dict(phase='failed' if failed else 'finalized')
    if extra is not None:
        changes['semantic_json'] = json.dumps({'fixture_retained':extra})
    done = s.importer._save(row, **changes)
    assert done['ingestion_id'] == ingestion_id
    assert done['source_id'] == row['source_id']
    assert done['file_id'] == row['file_id']
    assert done['request_id'] == row['request_id']
    assert done['rights_json'] == row['rights_json']
    return done


@pytest.mark.asyncio
async def test_sequential_100_owner_intakes_after_32_completed_rows_preserve_history(
        native_intake):
    s = native_intake
    history = []
    for ordinal in range(100):
        # The existing owner-only native start and synthetic original acquire
        # are exercised at every step. No second concurrent active owner.
        result = await start(s, file_id=f'prior-original-{ordinal:03d}')
        assert result.ingestion_id not in history
        assert result.ingestion_id in s.originals.receipts
        row = s.importer._row(s.principal.subject, result.ingestion_id)
        assert row['phase'] == 'staging'
        history.append(result.ingestion_id)
        _terminal_fixture(s, result.ingestion_id, failed=(ordinal % 11 == 10))
        if ordinal in (31, 32, 99):
            with s.t.queue.connect(write=False) as db:
                count, active = db.execute("""
                    SELECT count(*),sum(CASE WHEN phase NOT IN ('finalized','failed')
                                            THEN 1 ELSE 0 END)
                    FROM rkb_source_intake_control
                """).fetchone()
                assert (count,active) == (ordinal + 1, 0)
    assert len(set(history)) == 100
    assert len(s.originals.calls) == 100

    first = s.importer._row(s.principal.subject, history[0])
    middle = s.importer._row(s.principal.subject, history[32])
    last = s.importer._row(s.principal.subject, history[-1])
    assert first['file_id'] == 'prior-original-000'
    assert middle['file_id'] == 'prior-original-032'
    assert last['file_id'] == 'prior-original-099'
    assert first['phase'] == 'finalized' and middle['phase'] == 'failed'
    assert last['source_id'] == s.originals.receipts[history[-1]].source_id
    assert (await s.importer.owned_row(s.principal, history[0]))['id'] == history[0]
    with pytest.raises(ToolError, match='owned_ingestion_required'):
        s.importer._row('00000000-0000-0000-0000-000000000077', history[0])

    # Idempotent same file identity wins even after 100 historical rows.
    duplicate = await start(s, file_id='prior-original-000')
    assert duplicate.ingestion_id == history[0]
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM rkb_source_intake_control').fetchone()[0] == 100

    # A verified original no longer holds the owner's upload slot while its
    # source research is still open. Both existing identities remain usable.
    current = await start(s, file_id='currently-unsettled-original')
    active = s.importer._row(s.principal.subject, current.ingestion_id)
    assert active['phase'] == 'staging'
    following=await start(s, file_id='next-verified-upload')
    assert following.ingestion_id!=current.ingestion_id
    with s.t.queue.connect(write=False) as db:
        assert db.execute("""
            SELECT count(*) FROM rkb_source_intake_control
            WHERE phase NOT IN ('finalized','failed')
        """).fetchone()[0] == 2
        assert db.execute('SELECT count(*) FROM rkb_source_intake_control').fetchone()[0] == 102
    # The active row remains addressable and its original receipt unchanged.
    assert s.importer._row(s.principal.subject,current.ingestion_id)['phase'] == 'staging'
    assert s.originals.receipts[current.ingestion_id].source_id == current.document_id
    assert MAX_CONTROL_ROWS == 32


@pytest.mark.asyncio
async def test_terminal_history_bytes_do_not_consume_active_control_byte_limit(
        native_intake):
    s = native_intake
    ids = []
    for ordinal in range(10):
        result = await start(s, file_id=f'large-finished-{ordinal:03d}')
        ids.append(result.ingestion_id)
        _terminal_fixture(s, result.ingestion_id, extra='x'*925000)
    with s.t.queue.connect(write=False) as db:
        saved_bytes, total, active = db.execute("""
            SELECT coalesce(sum(length(CAST(semantic_json AS BLOB))),0),count(*),
                   sum(CASE WHEN phase NOT IN ('finalized','failed') THEN 1 ELSE 0 END)
            FROM rkb_source_intake_control
        """).fetchone()
        assert saved_bytes > MAX_CONTROL_BYTES
        assert (total,active) == (10,0)
        # Test the actual bounded production budget method on the full
        # persisted table, not a mocking query that ignores old large rows.
        s.importer._budget(db)

    current = await start(s, file_id='small-unsettled-control')
    assert s.importer._row(s.principal.subject,current.ingestion_id)['phase']=='staging'
    # Prior result/status lookup remains possible after entering fresh work.
    assert s.importer._row(s.principal.subject, ids[0])['semantic_json'] is not None
    assert s.importer._row(s.principal.subject, ids[-1])['phase']=='finalized'



@pytest.mark.asyncio
@pytest.mark.parametrize('nonterminal_reason', [
    'original_telegram_binding_required', 'original_native_receipt_unavailable',
])
async def test_real_original_controller_terminal_rejection_durable_and_owner_can_restart(
        native_intake, nonterminal_reason):
    """A persisted fixed original failure is terminal, but UNKNOWN never is.

    Use the ACTUAL OriginalIntakeController constructor, begin(), durable
    _reject()/status() and the ACTUAL importer public book_ingest(start/status).
    Only the *external native peer* of the controller is a synthetic fixture:
    it returns the same fixed terminal/nonterminal response envelope that the
    real original_acquire phase validates. No direct test-only importer _save,
    private attachment download, remote provider or journal fabrication.
    """
    s = native_intake
    # ConditionalMemoryStore is the existing fixture's journal store.
    # Give its original-controller storage-identity validator a matching fake
    # client, but NEVER invoke its object-store operations in this scenario.
    archive_client = SimpleNamespace(
        meta=SimpleNamespace(endpoint_url='https://archive.test.invalid'))
    s.store.client = archive_client
    s.store.bucket = 'synthetic-original-controller'
    archive = SimpleNamespace(client=archive_client, bucket=s.store.bucket)
    current = lambda: current_binding(s.t.queue)

    def controller():
        return OriginalIntakeController(
            queue=s.t.queue, spool=s.spool, participants=s.t.auth.p.registry,
            object_store=archive, current_binding=current,
            binding_current=s.startup.binding_current,
            allowed_origins=frozenset({'https://archive.test.invalid'}))

    original = controller()
    observed = []

    class NativeOriginalPeer:
        async def request(self, binding, phase, args, *, operation_key):
            assert s.startup.binding_current(binding)
            assert phase == 'original_acquire'
            assert 'spec' in args and 'file_id' in args['spec']
            assert isinstance(operation_key, str) and len(operation_key) == 64
            file_id = args['spec']['file_id']
            observed.append(file_id)
            if file_id == 'rejected-pdf':
                return {'error': 'original_pdf_invalid', 'terminal': True}
            if file_id in ('next-book-pending', 'third-book-pending'):
                return {'error': nonterminal_reason, 'terminal': False}
            pytest.fail('a second unresolved owner intake reached native acquisition')

    peer = NativeOriginalPeer()
    original._bridge = MethodType(lambda self, binding: peer, original)
    # Keep the SAME real class/DB reader for restart; only inject the synthetic
    # upstream native response at the narrow existing transfer boundary.
    s.importer.originals = original

    async def begin(file_id):
        return await s.importer.book_ingest(
            command='start', principal=s.principal,
            file=ChatFile(file_id=file_id,
                download_url='https://files.example.test/private.pdf?fixture=1',
                mime_type='application/pdf', file_name='source.pdf'),
            payload={'title': 'Distinct synthetic book'},
            source_import=options())

    terminal = await begin('rejected-pdf')
    assert terminal.state == 'failed' and 'original_pdf_invalid' in terminal.warnings
    saved = s.importer._row(s.principal.subject, terminal.ingestion_id)
    assert saved['phase'] == 'failed'
    assert saved['metadata_json'] and saved['rights_json']
    with s.t.queue.connect(write=False) as db:
        row = db.execute("""
            SELECT failure_code,receipt_json,spec_json,actor
            FROM rkb_original_intakes WHERE ingestion_id=?
        """, (terminal.ingestion_id,)).fetchone()
        assert row['failure_code'] == 'original_pdf_invalid'
        assert row['receipt_json'] is None
        assert json.loads(row['spec_json'])['file_id'] == 'rejected-pdf'
        assert row['actor'] == s.principal.subject
    # A new controller uses durable SQLite failure_code, not the original
    # Python exception, a mock status response or a transient memory dict.
    restarted = controller()
    restarted._bridge = MethodType(lambda self, binding: peer, restarted)
    s.importer.originals = restarted
    assert (await restarted.status(s.t.auth.p.admin, terminal.ingestion_id)).reason == 'original_pdf_invalid'
    public_status = await s.importer.book_ingest(
        command='status', principal=s.principal, ingestion_id=terminal.ingestion_id)
    assert public_status.state == 'failed'
    assert 'original_pdf_invalid' in public_status.warnings
    assert public_status.ingestion_id == terminal.ingestion_id
    assert public_status.document_id == terminal.document_id

    # The failed row does not monopolize the sole owner intake lease.
    next_book = await begin('next-book-pending')
    assert next_book.state == 'processing'
    assert next_book.ingestion_id != terminal.ingestion_id
    assert s.importer._row(s.principal.subject, next_book.ingestion_id)['phase'] == 'acquiring'
    unresolved = await s.importer.book_ingest(
        command='status', principal=s.principal, ingestion_id=next_book.ingestion_id)
    assert unresolved.state == 'processing'
    assert s.importer._row(s.principal.subject, next_book.ingestion_id)['phase'] == 'acquiring'
    # No acquisition job survived the API restart, so this owner slot is abandoned.
    assert unresolved.warnings == ['original_attachment_retry_same_file_required']
    assert not restarted._private
    third = await begin('third-book-pending')
    assert third.state == 'processing' and third.ingestion_id != next_book.ingestion_id
    assert s.importer._row(s.principal.subject, next_book.ingestion_id)['phase'] == 'failed'
    # A live attachment URL without a job still holds the slot before cold-run admission.
    with s.t.queue.connect(write=False) as db:
        orig = db.execute('SELECT spec_json FROM rkb_original_intakes WHERE ingestion_id=?',
                          (third.ingestion_id,)).fetchone()
    key = digest(canonical(dict(phase='original_acquire', args={'spec': json.loads(orig['spec_json'])})))
    restarted._private[key] = {'download_url': 'https://files.example.test/pending'}
    live = await restarted.status(s.t.auth.p.admin, third.ingestion_id)
    assert (live.state, live.reason) == ('pending', 'original_acquisition_pending')
    with pytest.raises(ToolError, match='another_native_ingestion_active'):
        await begin('fourth-book-must-block')
    assert s.importer._row(s.principal.subject, third.ingestion_id)['phase'] == 'acquiring'
    restarted._private.clear()
    # A durable unfinished maintenance job is different: UNKNOWN must retain the slot.
    with s.t.queue.connect() as db:
        now = s.t.queue.clock()
        db.execute('''INSERT INTO jobs(id,actor,idempotency,kind,texts,identity,state,created,updated)
                      VALUES(?,?,?,?,?,?,?,?,?)''',
                   (str(uuid4()), 'maintenance:'+s.startup.binding.run_id, 'recovery:'+key,
                    'recovery_phase', '[]', '{}', 'pending', now, now))
    ongoing = await restarted.status(s.t.auth.p.admin, third.ingestion_id)
    assert (ongoing.state, ongoing.reason) == ('pending', 'original_acquisition_pending')
    with pytest.raises(ToolError, match='another_native_ingestion_active'):
        await begin('fourth-book-must-block')
    assert observed.count('rejected-pdf') == 1
    assert observed.count('next-book-pending') == 1
    assert observed.count('third-book-pending') == 1
    assert 'fourth-book-must-block' not in observed
    with s.t.queue.connect(write=False) as db:
        old = db.execute("SELECT phase,rights_json FROM rkb_source_intake_control WHERE ingestion_id=?",
                         (terminal.ingestion_id,)).fetchone()
        assert old['phase'] == 'failed' and old['rights_json'] == saved['rights_json']
        assert db.execute('SELECT count(*) FROM rkb_source_intake_control').fetchone()[0] == 3
