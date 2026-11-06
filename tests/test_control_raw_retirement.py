"""Synthetic actual intake/journal; no corpus, provider or executor replay."""
from types import SimpleNamespace as NS
from dataclasses import replace
import sqlite3
import pytest
from regional_knowledge.recovery_journal import JournalUnknown
from test_kaggle_native_source_importer import (
    native_intake, target, auth, people, start, stage, batch, finish, call, state,
)
from regional_knowledge.control_spool import ControlSpool, SpoolConflict


@pytest.mark.asyncio
async def test_saved_raw_retirement_keeps_duplicate_exact_and_unsaved_suffix(native_intake):
    s = native_intake
    book = await start(s)
    await stage(s, book, batch(s, book, count=1))
    originals = []
    for component in range(3):
        before = await finish(s, book)
        originals.append(await s.spool.get(before.command.operation_key))
        await call(s, book, 'status')
    await stage(s, book, batch(s, book, first=1, count=1))
    latest = state(s, book)['pending_operation_key']
    s.spool.clock = lambda: 10**12
    saved = NS(base_sequence=3, database_generation='synthetic-db')
    assert s.spool._retire_raw(saved, 16) == 3
    with s.t.queue.connect(write=False) as db:
        rows = db.execute('SELECT * FROM rkb_control_operations ORDER BY sequence').fetchall()
        assert all(r['command_bytes'] is None and r['outcome_bytes'] is None for r in rows[:3])
        assert rows[3]['operation_key'] == latest and rows[3]['command_bytes'] is not None
    for old in originals:
        got = await s.spool.get(old.command.operation_key)
        assert got == old
        frozen, data, handler = s.spool._validate(old.command, 'synthetic-db')
        same = await s.spool.stage_validated(frozen, data, handler, 'synthetic-db')
        assert same == old
        # An already committed command never reaches a SQL executor again.
        assert await s.spool.apply(old.command.operation_key, None, writer_generation='writer-1') == old.apply_receipt
        await s.spool.record_apply(old.command.operation_key, old.apply_receipt)
    with s.t.queue.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_control_operations').fetchone()[0] == 4
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('UPDATE rkb_control_operations SET command_bytes=? WHERE sequence=1', (b'changed',))
    assert s.spool._retire_raw(saved, 16) == 0
    # A physically restored older cursor sees only the still-unsaved suffix.
    applied,pending,changes=s.spool._dataset_cursor_suffix(dict(sequence=0,heads=[]),4)
    assert applied==3 and len(pending)==1 and pending[0][0]==rows[3]['command_bytes']
    assert len(changes)==1 and changes[0].sequence==3
    # An unavailable existing journal cannot erase the compact duplicate identity.
    old = originals[0]
    objects = s.spool.journal.store.objects
    retained = dict(objects)
    objects.clear()
    with pytest.raises(JournalUnknown, match='retired_control_bytes_unavailable'):
        await s.spool.get(old.command.operation_key)
    objects.update(retained)
    assert (await s.spool.get(old.command.operation_key)).command_bytes == old.command_bytes
    # Warm reopen does not rebuild or restore retired raw bodies.
    s.spool.close()
    s.spool = ControlSpool(s.t.queue,s.spool.participants,s.spool.journal,s.spool.registry)
    assert (await s.spool.get(old.command.operation_key)) == old
    with s.t.queue.connect(write=False) as db:
        assert db.execute('SELECT command_bytes FROM rkb_control_operations WHERE sequence=1').fetchone()[0] is None


@pytest.mark.asyncio
async def test_retirement_protects_semantic_gap_rejection_latest_and_unverified_prefix(native_intake):
    s=native_intake; book=await start(s)
    await stage(s,book,batch(s,book,count=1))
    for component in range(3):
        old=await finish(s,book,status='rejected_precondition' if component==2 else 'applied',
                         reason='source_packet_requires_review' if component==2 else None)
        await call(s,book,'status')
    with s.t.queue.connect(write=False) as db:
        first=dict(db.execute('SELECT * FROM rkb_control_operations WHERE sequence=1').fetchone())
    # Move on with the ordinary corrected stage; the earlier accepted model is
    # still needed for the missing semantic page, regardless of its age.
    await stage(s,book,batch(s,book,first=1,count=1))
    s.spool.clock=lambda:10**12
    saved=NS(base_sequence=3,database_generation='synthetic-db')
    assert s.spool._retire_raw(replace_checkpoint(saved,base_sequence=0),16)==0
    assert s.spool._retire_raw(replace_checkpoint(saved,database_generation='other'),16)==0
    assert s.spool._retire_raw(saved,16)==1  # Only the old successful TEXT body.
    with s.t.queue.connect(write=False) as db:
        rows=db.execute('SELECT * FROM rkb_control_operations ORDER BY sequence').fetchall()
        assert rows[0]['command_bytes']==first['command_bytes']
        assert rows[1]['command_bytes'] is None
        assert rows[2]['command_bytes'] is not None and rows[2]['apply_state']=='REJECTED_PRECONDITION'
        assert rows[3]['command_bytes'] is not None and rows[3]['outcome_state']=='NOT_SENT'
        from regional_knowledge.source_page_progress import accepted_model_page,publication_progress
        assert publication_progress(db,dict(first,ingestion_id=book.ingestion_id))[0]=={0}
        assert accepted_model_page(db,dict(first,ingestion_id=book.ingestion_id),0)['page']['physical_page_index']==0


def replace_checkpoint(saved,**changes):
    return NS(**(vars(saved)|changes))


def test_nullable_migration_is_transactional_preserves_rows_and_warm_open_is_noop():
    from regional_knowledge.control_raw_retention import initialize_nullable_raw
    db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
    db.executescript('''CREATE TABLE rkb_control_operations(
        operation_key TEXT PRIMARY KEY, command_bytes BLOB NOT NULL CHECK(length(command_bytes)>0),
        sequence INTEGER UNIQUE, progress_json TEXT);
        CREATE INDEX original_index ON rkb_control_operations(progress_json);
        CREATE TRIGGER original_guard BEFORE UPDATE OF operation_key ON rkb_control_operations
        BEGIN SELECT RAISE(ABORT,'immutable key'); END;
        INSERT INTO rkb_control_operations VALUES('key',x'616263',7,'{}');''')
    db.execute('BEGIN IMMEDIATE')
    initialize_nullable_raw(db)
    db.rollback()
    assert next(r[3] for r in db.execute('PRAGMA table_info(rkb_control_operations)') if r[1]=='command_bytes')==1
    db.execute('BEGIN IMMEDIATE')
    initialize_nullable_raw(db);db.commit()
    assert dict(db.execute('SELECT * FROM rkb_control_operations').fetchone())==dict(
        operation_key='key',command_bytes=b'abc',sequence=7,progress_json='{}')
    assert {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE name IN ('original_index','original_guard')")}=={'original_index','original_guard'}
    with pytest.raises(sqlite3.IntegrityError):db.execute("UPDATE rkb_control_operations SET operation_key='other'")
    queries=[];db.set_trace_callback(queries.append)
    initialize_nullable_raw(db)
    assert not any('SELECT * FROM' in q or 'INSERT' in q or 'CREATE' in q for q in queries)


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_turn_verified_saved_into_failed_stop(native_intake,monkeypatch,caplog):
    import asyncio
    from unittest.mock import AsyncMock
    from regional_knowledge.kaggle_dataset_checkpoint import SourceDatasetCheckpoints
    owner=object.__new__(SourceDatasetCheckpoints)
    owner.startup=NS(config=NS(spool=native_intake.spool))
    owner.store=NS(latest_checkpoint=NS(base_sequence=3))
    owner._refresh_lock=asyncio.Lock()
    owner._refresh=AsyncMock(return_value=True)
    monkeypatch.setattr(native_intake.spool,'retire_raw',AsyncMock(side_effect=sqlite3.OperationalError('busy')))
    assert await owner.refresh('binding',planned_stop=True)
    owner._refresh.assert_awaited_once_with('binding',planned_stop=True)
    assert 'control_raw_retirement_deferred base_sequence=3 error_type=OperationalError' in caplog.text


@pytest.mark.asyncio
async def test_populated_legacy_schema_upgrade_keeps_exact_records_and_public_counters(native_intake):
    s=native_intake;book=await start(s)
    await stage(s,book,batch(s,book,count=1));await finish(s,book)
    s.spool.close()
    with s.t.queue.connect() as db:
        before=[tuple(r) for r in db.execute('SELECT * FROM rkb_control_operations ORDER BY sequence')]
        counters=tuple(db.execute('SELECT * FROM rkb_control_public_source_meta').fetchone())
        schema=db.execute("SELECT sql FROM sqlite_master WHERE name='rkb_control_operations'").fetchone()[0]
        objects=[r[0] for r in db.execute("SELECT sql FROM sqlite_master WHERE tbl_name='rkb_control_operations' AND type IN ('index','trigger') AND sql IS NOT NULL")]
        db.execute(schema.replace('rkb_control_operations','legacy_raw',1).replace('command_bytes BLOB','command_bytes BLOB NOT NULL',1))
        db.execute('INSERT INTO legacy_raw SELECT * FROM rkb_control_operations')
        db.execute('DROP TABLE rkb_control_operations');db.execute('ALTER TABLE legacy_raw RENAME TO rkb_control_operations')
        for sql in objects:db.execute(sql)
        db.execute('DROP TRIGGER rkb_control_intent_immutable')
        db.execute("CREATE TRIGGER rkb_control_intent_immutable BEFORE UPDATE OF operation_key,command_bytes,command_sha256 ON rkb_control_operations BEGIN SELECT RAISE(ABORT,'immutable old intent'); END")
    s.spool=ControlSpool(s.t.queue,s.spool.participants,s.spool.journal,s.spool.registry)
    with s.t.queue.connect(write=False) as db:
        assert [tuple(r) for r in db.execute('SELECT * FROM rkb_control_operations ORDER BY sequence')]==before
        assert tuple(db.execute('SELECT * FROM rkb_control_public_source_meta').fetchone())==counters
    assert (await s.spool.get(before[0][0])).command_bytes==before[0][1]
