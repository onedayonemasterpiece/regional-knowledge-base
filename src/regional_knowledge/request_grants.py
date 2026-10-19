"""Revocation projection of existing OAuth families into the corpus authority.

No login, person or permission is created here. OAuth holds its file lock before
writing this projection; request workers only read SQLite, never the auth file.
This keeps delayed work revocable without retaining an expiring bearer token.
"""
import hashlib


def reference(family):return hashlib.sha256(('rkb-request-family:'+family).encode()).hexdigest()


def schema(corpus):
    with corpus.connect() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_request_oauth_grants(
            id TEXT PRIMARY KEY,person_id TEXT NOT NULL,client_id TEXT NOT NULL,resource TEXT NOT NULL,
            credential_id TEXT,credential_epoch INTEGER,expires_at REAL NOT NULL,enabled INTEGER NOT NULL)''')


def save(corpus,record,expires_at):
    ident=reference(record['family'])
    with corpus.connect() as db:
        old=db.execute('SELECT * FROM rkb_request_oauth_grants WHERE id=?',(ident,)).fetchone()
        if old and not old['enabled']:return None
        if old and old['expires_at']>=expires_at and tuple(old[k] for k in ('person_id','client_id','resource','credential_id','credential_epoch'))==(
            record['subject'],record['client_id'],record['resource'],record.get('personal_credential_id'),record.get('personal_credential_epoch')):
            return ident
        db.execute('BEGIN IMMEDIATE')
        old=db.execute('SELECT * FROM rkb_request_oauth_grants WHERE id=?',(ident,)).fetchone()
        values=(record['subject'],record['client_id'],record['resource'],record.get('personal_credential_id'),record.get('personal_credential_epoch'))
        if old:
            if not old['enabled'] or tuple(old[k] for k in ('person_id','client_id','resource','credential_id','credential_epoch'))!=values:
                return None
            if expires_at>old['expires_at']:db.execute('UPDATE rkb_request_oauth_grants SET expires_at=? WHERE id=?',(expires_at,ident))
        else:
            db.execute('INSERT INTO rkb_request_oauth_grants VALUES(?,?,?,?,?,?,?,1)',(ident,*values,expires_at))
    return ident


def revoke(corpus,*,family=None,credential_id=None):
    with corpus.connect() as db:
        if family is not None:db.execute('UPDATE rkb_request_oauth_grants SET enabled=0 WHERE id=?',(reference(family),))
        if credential_id is not None:db.execute('UPDATE rkb_request_oauth_grants SET enabled=0 WHERE credential_id=?',(credential_id,))


def allowed(db,ident,person_id,resource,now,client_id=None):
    row=db.execute('SELECT * FROM rkb_request_oauth_grants WHERE id=?',(ident,)).fetchone()
    return bool(row and row['enabled'] and row['expires_at']>now and row['person_id']==str(person_id)
        and row['resource']==resource and (client_id is None or row['client_id']==client_id))
