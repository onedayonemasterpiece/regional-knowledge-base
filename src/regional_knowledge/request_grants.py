"""Current revocation projection of existing OAuth families, never another IAM.

The existing OAuth mint/refresh/revocation path writes this table while
holding its encrypted-state lock. An explicit LOCAL reviewed owner-family
projection migration may insert missing rows for exact currently issued owner
authorizations under that same lock; it never mints/extends/re-enables authority. Ready reads require an existing exact family
projection; they never create or extend it.
"""
import hashlib
import math


def reference(family):
    return hashlib.sha256(('rkb-request-family:' + family).encode()).hexdigest()


def schema(corpus):
    with corpus.connect() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS rkb_request_oauth_grants(
            id TEXT PRIMARY KEY,person_id TEXT NOT NULL,client_id TEXT NOT NULL,resource TEXT NOT NULL,
            credential_id TEXT,credential_epoch INTEGER,expires_at REAL NOT NULL,enabled INTEGER NOT NULL,
            issuer TEXT NOT NULL DEFAULT '')''')
        if 'issuer' not in {r['name'] for r in db.execute('PRAGMA table_info(rkb_request_oauth_grants)')}:
            # Legacy projections remain unverified until a real OAuth refresh.
            db.execute("ALTER TABLE rkb_request_oauth_grants ADD COLUMN issuer TEXT NOT NULL DEFAULT ''")


def save(corpus, record, expires_at):
    """OAuth mint/refresh only; caller holds the existing auth-file lock."""
    values = (record['subject'], record['client_id'], record['resource'],
              record.get('personal_credential_id'), record.get('personal_credential_epoch'))
    issuer = record['issuer']
    if (not all(isinstance(v, str) and v for v in (record['family'], *values[:3], issuer))
            or not math.isfinite(expires_at)):
        raise ValueError('exact OAuth family projection required')
    ident = reference(record['family'])
    with corpus.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        old = db.execute('SELECT * FROM rkb_request_oauth_grants WHERE id=?', (ident,)).fetchone()
        if old:
            if (not old['enabled'] or old['issuer'] not in ('', issuer)
                    or tuple(old[k] for k in ('person_id', 'client_id', 'resource', 'credential_id', 'credential_epoch')) != values):
                return None
            db.execute('UPDATE rkb_request_oauth_grants SET issuer=?, expires_at=max(expires_at,?) WHERE id=?',
                       (issuer, expires_at, ident))
        else:
            db.execute('''INSERT INTO rkb_request_oauth_grants
                (id,person_id,client_id,resource,credential_id,credential_epoch,expires_at,enabled,issuer)
                VALUES(?,?,?,?,?,?,?,1,?)''', (ident, *values, expires_at, issuer))
    return ident


def revoke(corpus, *, family=None, credential_id=None):
    with corpus.connect() as db:
        if family is not None:
            db.execute('UPDATE rkb_request_oauth_grants SET enabled=0 WHERE id=?', (reference(family),))
        if credential_id is not None:
            db.execute('UPDATE rkb_request_oauth_grants SET enabled=0 WHERE credential_id=?', (credential_id,))


def allowed(db, ident, person_id, resource, now, *, client_id, issuer):
    """Read-only exact current projection check; unknown/legacy fails closed."""
    row = db.execute('SELECT * FROM rkb_request_oauth_grants WHERE id=?', (ident,)).fetchone()
    return bool(row and row['enabled'] and row['expires_at'] > now
                and row['person_id'] == str(person_id) and row['resource'] == resource
                and row['client_id'] == client_id and row['issuer'] == issuer)
