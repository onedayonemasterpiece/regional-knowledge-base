"""Retained provider receipts recover only their exact current database/figure."""
from contextlib import contextmanager
import json
import sqlite3
from types import SimpleNamespace as N

import pytest

from regional_knowledge.kaggle_read_binding import TargetReadBinding
from regional_knowledge.original_intake_controller import OriginalIntakeController
from test_source_stage import original, SOURCE


@pytest.mark.asyncio
@pytest.mark.parametrize('database,state,known,expected',[
    ('foreign-db','claimed',False,None),
    ('foreign-db','done',True,None),
    ('current-db','claimed',False,{'status':'unknown'}),
    ('current-db','done',True,{'status':'ready','binding':{'source_id':SOURCE},'photo':{'kind':'photo'}}),
])
async def test_public_crop_recovery_does_not_reuse_foreign_database_receipt(
        tmp_path,database,state,known,expected):
    path=tmp_path/'control.sqlite'
    @contextmanager
    def connect(**kwargs):
        connection=sqlite3.connect(path);connection.row_factory=sqlite3.Row
        try:
            with connection:yield connection
        finally:connection.close()
    receipt=original();figure='20000000-0000-0000-0000-000000000002'
    importer='30000000-0000-0000-0000-000000000003'
    reader='40000000-0000-0000-0000-000000000004'
    saved=dict(status='ready',binding={'source_id':SOURCE},photo={'kind':'photo'})
    identity=dict(phase='original_illustration',database_generation=database,
        arguments=dict(source_id=SOURCE,illustration_id=figure))
    result=dict(value=dict(snapshot=dict(source_id=SOURCE,illustration_id=figure,
        archive_state=saved))) if known else None
    with connect() as db:
        db.execute('''CREATE TABLE rkb_original_intakes(spec_json TEXT,receipt_json TEXT,
            storage_sha256 TEXT,acquisition_json TEXT,actor TEXT)''')
        db.execute('INSERT INTO rkb_original_intakes VALUES(?,?,?,?,?)',(
            json.dumps({'source_id':SOURCE}),json.dumps(receipt.wire()),'same-storage',
            json.dumps(dict(handle='retained-handle',sha256=receipt.sha256)),importer))
        db.execute('CREATE TABLE jobs(kind TEXT,identity TEXT,state TEXT,result TEXT,created REAL)')
        db.execute('INSERT INTO jobs VALUES(?,?,?,?,1)',('recovery_phase',json.dumps(identity),
            state,json.dumps(result) if result else None))
    binding=TargetReadBinding('run','inc',1,'current-db',0,'fixture:1')
    owner=object.__new__(OriginalIntakeController)
    owner.queue=N(connect=connect);owner._storage='same-storage'
    owner.binding_current=lambda value:value is binding
    calls=[]
    async def phase(current,phase,args):
        calls.append(args)
        assert phase=='original_illustration' and current is binding
        return {'snapshot':'synthetic-native-result'}
    owner._phase=phase
    await owner.public_illustration(SOURCE,figure,reader,binding)
    assert len(calls)==1 and calls[0]['recovery']==expected
    assert calls[0]['actor_id']==reader and calls[0]['actor_id']!=importer
