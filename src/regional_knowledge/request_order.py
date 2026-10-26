"""Dialog order and fair head selection inside the existing request authority.

This module owns no worker, queue or connection. ExpertRequests calls these
helpers in the same corpus transaction as intake, claim and delivery receipts.
"""
import hashlib
import json

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def schema(corpus):
    with corpus.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        columns={r[1] for r in db.execute('PRAGMA table_info(rkb_expert_requests)')}
        for name,kind in (('dialog_id','TEXT'),('dialog_seq','INTEGER'),('client_kind',"TEXT NOT NULL DEFAULT 'slack'"),
            ('tenant_id','TEXT'),('client_request_key','TEXT'),('context_seq','INTEGER')):
            if name not in columns:db.execute('ALTER TABLE rkb_expert_requests ADD COLUMN '+name+' '+kind)
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_request_dialogs(
            ordinal INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,
            client_kind TEXT NOT NULL,tenant_id TEXT NOT NULL,conversation_key TEXT NOT NULL,
            owner_person_id TEXT,next_seq INTEGER NOT NULL DEFAULT 0,
            head_seq INTEGER NOT NULL DEFAULT 1,head_person_id TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_request_people(
            ordinal INTEGER PRIMARY KEY AUTOINCREMENT,person_id TEXT NOT NULL UNIQUE,last_dialog INTEGER NOT NULL DEFAULT 0)''')
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_request_order_cursor(
            id INTEGER PRIMARY KEY CHECK(id=1),last_person INTEGER NOT NULL DEFAULT 0)''')
        db.execute('INSERT OR IGNORE INTO rkb_request_order_cursor(id) VALUES(1)')
        db.execute('CREATE UNIQUE INDEX IF NOT EXISTS rkb_request_dialog_seq ON rkb_expert_requests(dialog_id,dialog_seq) WHERE dialog_id IS NOT NULL')
        db.execute('CREATE INDEX IF NOT EXISTS rkb_dialog_head_person ON rkb_request_dialogs(head_person_id,ordinal)')
        db.execute('CREATE INDEX IF NOT EXISTS rkb_request_person_work ON rkb_expert_requests(person_id,state,lease_until)')
    # Preserve existing raw origin/event hashes. Reconstruction only adds order
    # metadata to legacy rows; all new intake assigns seq atomically at commit.
    while True:
        with corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            rows=db.execute('SELECT * FROM rkb_expert_requests WHERE dialog_id IS NULL ORDER BY created_at,rowid LIMIT 100').fetchall()
            if not rows:break
            for row in rows:
                origin=json.loads(row['origin'])
                assign_slack(db,row['id'],row['person_id'],origin)
            for dialog in {db.execute('SELECT dialog_id FROM rkb_expert_requests WHERE id=?',(r['id'],)).fetchone()[0] for r in rows}:
                advance(db,dialog)


def assign(db,request_id,person_id,*,client_kind,tenant_id,conversation_key,client_request_key,owner_person_id=None):
    ident=hashlib.sha256(canonical([client_kind,tenant_id,conversation_key,owner_person_id]).encode()).hexdigest()
    db.execute('''INSERT OR IGNORE INTO rkb_request_dialogs(id,client_kind,tenant_id,conversation_key,owner_person_id)
        VALUES(?,?,?,?,?)''',(ident,client_kind,tenant_id,conversation_key,owner_person_id))
    dialog=db.execute('SELECT * FROM rkb_request_dialogs WHERE id=?',(ident,)).fetchone()
    seq=dialog['next_seq']+1
    db.execute('UPDATE rkb_request_dialogs SET next_seq=?,head_person_id=CASE WHEN head_seq=? THEN ? ELSE head_person_id END WHERE id=?',
        (seq,seq,person_id,ident))
    db.execute('INSERT OR IGNORE INTO rkb_request_people(person_id) VALUES(?)',(person_id,))
    db.execute('''UPDATE rkb_expert_requests SET dialog_id=?,dialog_seq=?,client_kind=?,tenant_id=?,client_request_key=? WHERE id=?''',
        (ident,seq,client_kind,tenant_id,client_request_key,request_id))
    return ident,seq


def assign_slack(db,request_id,person_id,origin):
    tenant=hashlib.sha256(canonical(['slack',origin['app_id'],origin['team_id']]).encode()).hexdigest()
    return assign(db,request_id,person_id,client_kind='slack',tenant_id=tenant,
        conversation_key=canonical([origin['channel_id'],origin['thread_ts']]),client_request_key=origin['event_id'])


def advance(db,dialog_id):
    row=db.execute("SELECT dialog_seq,person_id FROM rkb_expert_requests WHERE dialog_id=? AND state NOT IN ('delivered','abandoned_no_resend') ORDER BY dialog_seq LIMIT 1",(dialog_id,)).fetchone()
    if row:
        db.execute('UPDATE rkb_request_dialogs SET head_seq=?,head_person_id=? WHERE id=?',(row['dialog_seq'],row['person_id'],dialog_id))
    else:
        db.execute('UPDATE rkb_request_dialogs SET head_seq=next_seq+1,head_person_id=NULL WHERE id=?',(dialog_id,))


def is_head(db,row):
    dialog=db.execute('SELECT head_seq FROM rkb_request_dialogs WHERE id=?',(row['dialog_id'],)).fetchone()
    return bool(dialog and dialog[0]==row['dialog_seq'])


def fair_head(db,now,scope_sql,scope_args):
    """Read the next eligible head; explicit-ID callers cannot pick another.

    Give waiting people without active work a first turn, then cycle again
    without imposing an artificial one-active-job-per-person ceiling.
    """
    # Only a pre-send expired lease can be reclaimed. Once sending/unknown,
    # the provider might have committed; preserve the dialog head until exact
    # receipt/reconciliation, never let another executor resend automatically.
    ready="(r.state='pending' OR (r.state IN ('leased','result_unsealed','result_ready','send_intent_ready') AND r.lease_until<=?))"
    heads=f'''SELECT r.id FROM rkb_request_dialogs d JOIN rkb_expert_requests r
        ON r.dialog_id=d.id AND r.dialog_seq=d.head_seq
        WHERE d.head_person_id=p.person_id AND {ready} AND ({scope_sql})'''
    cursor=db.execute('SELECT last_person FROM rkb_request_order_cursor WHERE id=1').fetchone()[0]
    common=(now,*scope_args,cursor)
    person=None
    for first_turn in (True,False):
        extra=" AND NOT EXISTS(SELECT 1 FROM rkb_expert_requests a WHERE a.person_id=p.person_id AND a.state IN ('leased','result_unsealed','result_ready','send_intent_ready') AND a.lease_until>?)" if first_turn else ''
        args=(now,*scope_args,now,cursor) if first_turn else common
        person=db.execute(f'''SELECT p.* FROM rkb_request_people p WHERE EXISTS({heads}){extra}
            ORDER BY CASE WHEN p.ordinal>? THEN 0 ELSE 1 END,p.ordinal LIMIT 1''',args).fetchone()
        if person:break
    if not person:return None
    return db.execute(f'''SELECT r.* FROM rkb_request_dialogs d JOIN rkb_expert_requests r
        ON r.dialog_id=d.id AND r.dialog_seq=d.head_seq
        WHERE d.head_person_id=? AND {ready} AND ({scope_sql})
        ORDER BY CASE WHEN d.ordinal>? THEN 0 ELSE 1 END,d.ordinal LIMIT 1''',
        (person['person_id'],now,*scope_args,person['last_dialog'])).fetchone()


def claimed(db,row):
    person=db.execute('SELECT ordinal FROM rkb_request_people WHERE person_id=?',(row['person_id'],)).fetchone()[0]
    dialog=db.execute('SELECT ordinal FROM rkb_request_dialogs WHERE id=?',(row['dialog_id'],)).fetchone()[0]
    db.execute('UPDATE rkb_request_order_cursor SET last_person=? WHERE id=1',(person,))
    db.execute('UPDATE rkb_request_people SET last_dialog=? WHERE person_id=?',(dialog,row['person_id']))
