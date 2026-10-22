"""Current importer/control budget regression: terminal receipts must not consume active slots.

The existing owner OAuth, BgeQueue SQLite, native intake and original acquisition
interfaces are real classes; the *external original* is an existing synthetic
fixture. Explicit fixture-only terminal phase transitions below simulate prior
already-settled books; they do NOT claim a provider/remote accepted outcome,
Dataset Saved or a model run. No control row or terminal identity is deleted.
"""
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.kaggle_importer import MAX_CONTROL_BYTES, MAX_CONTROL_ROWS
from test_kaggle_native_source_importer import native_intake, start
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
        assert db.execute('SELECT count(*) FROM rkb_source_intake_control').fetchone() == (100,)

    # Real native owner exclusion remains; an UNKNOWN/unsettled live import
    # must keep its row and reject a new unrelated original.
    current = await start(s, file_id='currently-unsettled-original')
    active = s.importer._row(s.principal.subject, current.ingestion_id)
    assert active['phase'] == 'staging'
    with pytest.raises(ToolError, match='another_native_ingestion_active'):
        await start(s, file_id='must-not-start-concurrently')
    with s.t.queue.connect(write=False) as db:
        assert db.execute("""
            SELECT count(*) FROM rkb_source_intake_control
            WHERE phase NOT IN ('finalized','failed')
        """).fetchone() == (1,)
        assert db.execute('SELECT count(*) FROM rkb_source_intake_control').fetchone() == (101,)
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
