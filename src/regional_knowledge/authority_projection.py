"""Offline, explicit infrastructure projection; never activates a runtime.

No OAuth state is read here. Run only against a quiesced authority at cutover;
preparing a candidate does not settle legacy control work or authorize activation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID

# Column lists are fixed, not source sqlite_master SQL or caller SQL.
COLUMNS = {
    'rkb_participant_profiles': ('person_id', 'revision', 'disabled', 'payload'),
    'rkb_participant_grants': ('person_id', 'capability', 'expires_at', 'assigned_by', 'assigned_at'),
    'rkb_participant_weights': ('person_id', 'domain', 'payload', 'revision'),
    'rkb_external_identities': ('id', 'person_id', 'provider', 'tenant', 'provider_subject', 'state',
        'revision', 'method', 'confirmed_by', 'confirmed_at', 'attribution_review_required'),
    'rkb_identity_observations': ('id', 'provider', 'tenant', 'provider_subject', 'link_id',
        'link_revision', 'audience', 'expires_at', 'receipt_id', 'used_by'),
    'rkb_participant_events': ('person_id', 'revision', 'actor_id', 'action', 'payload', 'created_at'),
    'rkb_participant_operations': ('actor_id', 'operation_key', 'person_id', 'request_hash', 'result'),
    'rkb_request_oauth_grants': ('id', 'person_id', 'client_id', 'resource', 'credential_id',
        'credential_epoch', 'expires_at', 'enabled', 'issuer'),
    'rkb_oauth_owner_projection_migrations': ('plan_sha256', 'receipt_json'),
}
USER_COLUMNS = ('table_name', 'row_key', 'document_id', 'revision', 'payload')


def _regular(path: Path):
    if path.is_symlink() or not path.is_file():
        raise ValueError('regular source required')


def _validate(rows, owner):
    users = {}
    for row in rows['corpus_rows']:
        value = json.loads(row['payload'])
        if (not isinstance(value, dict) or set(value) - {'id', 'status', 'metadata', 'created_at', 'updated_at'}
            or str(UUID(value['id'])) != row['row_key'] or value['id'] != row['row_key']
            or value.get('status') not in ('active', 'disabled')
            or row['document_id'] is not None or row['revision'] is not None):
            raise ValueError('canonical user projection required')
        users[row['row_key']] = value
    if owner not in users or users[owner]['status'] != 'active':
        raise ValueError('configured active owner missing')
    for table, values in rows.items():
        if table == 'corpus_rows':
            continue
        for row in values:
            for field in ('person_id', 'actor_id', 'assigned_by', 'confirmed_by'):
                if row.get(field) is not None and row[field] not in users:
                    raise ValueError('authority person reference missing')
    links = {r['id']: r for r in rows['rkb_external_identities']}
    for row in rows['rkb_identity_observations']:
        link = links.get(row['link_id'])
        if not link or any(row[k] != link[k] for k in ('provider', 'tenant', 'provider_subject')):
            raise ValueError('identity observation reference missing')
        if row['used_by'] is not None and row['used_by'] not in links:
            raise ValueError('identity observation use reference missing')
    for row in rows['rkb_oauth_owner_projection_migrations']:
        receipt=json.loads(row['receipt_json'])
        fields={'codec','plan_sha256','applied_at','projected_families','preserved_existing',
            'rejected','issued_tokens','changed_participant_grants','state_or_key_modified'}
        reasons={'non_owner_family','unknown_issued_metadata','issued_scope_or_issuer_differs',
            'mixed_family_metadata','registered_client_unknown'}
        if (type(receipt) is not dict or set(receipt)!=fields
                or not re.fullmatch('[0-9a-f]{64}',row['plan_sha256'])
                or receipt['plan_sha256']!=row['plan_sha256']
                or receipt['codec']!='rkb.owner-issued-family-projection-receipt.v1'
                or type(receipt['applied_at']) is not str or not 1<=len(receipt['applied_at'])<=64
                or any(type(receipt[k]) is not int or not 0<=receipt[k]<=256
                    for k in ('projected_families','preserved_existing'))
                or receipt['projected_families']+receipt['preserved_existing']>256
                or type(receipt['rejected']) is not dict or set(receipt['rejected'])-reasons
                or any(type(v) is not int or not 0<=v<=256 for v in receipt['rejected'].values())
                or type(receipt['issued_tokens']) is not int or receipt['issued_tokens']!=0
                or type(receipt['changed_participant_grants']) is not int or receipt['changed_participant_grants']!=0
                or receipt['state_or_key_modified'] is not False
                or json.dumps(receipt,sort_keys=True,separators=(',',':'),allow_nan=False)!=row['receipt_json']):
            raise ValueError('exact owner projection migration receipt required')
        from datetime import datetime
        if datetime.fromisoformat(receipt['applied_at']).tzinfo is None:
            raise ValueError('dated owner projection migration receipt required')


def export_authority(source: Path, destination: Path, *, owner_subject: str,
                     max_rows: int = 10000, max_bytes: int = 8 * 1024 * 1024):
    """Prepare an exclusive private candidate. No grants or identities are invented.

    Limits cover selected logical bytes; ordinary legacy corpus tables are never
    loaded. Existing workspace/content grants require separate review, not loss.
    Returns only counts and digests, never personal payloads or OAuth secrets.
    """
    from .sqlite_corpus import SQLiteCorpus
    from .participants import Participants
    from .personal_auth import PersonalPolicy

    source, destination = Path(source).absolute(), Path(destination).absolute()
    owner = str(UUID(owner_subject))
    _regular(source)
    if (not 1 <= max_rows <= 100000 or not 1 <= max_bytes <= 32 * 1024 * 1024
        or destination.exists() or destination.is_symlink()):
        raise ValueError('exclusive bounded candidate required')
    parent = destination.parent
    if (not parent.is_dir() or parent.resolve() != parent or parent.stat().st_uid != os.getuid()
        or parent.stat().st_mode & 0o077):
        raise ValueError('owned private destination directory required')
    rows, missing = {}, []
    started = time.monotonic()
    with sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=1) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('PRAGMA trusted_schema=OFF')
        db.set_progress_handler(lambda: int(time.monotonic() - started > 10), 1000)
        db.execute('BEGIN')
        tables = {r['name'] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        unexpected = {t for t in tables if t.startswith(('rkb_participant_', 'rkb_external_', 'rkb_identity_', 'rkb_request_', 'rkb_oauth_'))} - set(COLUMNS)
        if unexpected:
            raise ValueError('unknown authority table requires explicit review')
        if 'corpus_rows' not in tables:
            raise ValueError('canonical user storage missing')
        # These are neither silently discarded nor converted to global grants.
        for name in ('rkb_workspaces', 'rkb_workspace_members', 'rkb_document_grants'):
            if db.execute('SELECT 1 FROM corpus_rows WHERE table_name=? LIMIT 1', (name,)).fetchone():
                raise ValueError('workspace/content authority requires separate review')
        total_rows = total_bytes = 0
        for table, columns in {'corpus_rows': USER_COLUMNS, **COLUMNS}.items():
            if table not in tables:
                rows[table] = []
                missing.append(table)
                continue
            actual = tuple(r['name'] for r in db.execute(f'PRAGMA table_info({table})'))
            # A legacy issuer-less projection remains unverified (empty issuer),
            # matching the existing request_grants.schema migration semantics.
            legacy = table == 'rkb_request_oauth_grants' and actual == columns[:-1]
            if actual != columns and not legacy:
                raise ValueError('authority schema requires explicit review')
            selected = columns[:-1] if legacy else columns
            query = f'SELECT {",".join(selected)} FROM {table}'
            if table == 'corpus_rows':
                query += " WHERE table_name='rkb_users'"
            # Refuse huge cells before Python allocates any source payload.
            oversized = ' OR '.join(f'length(CAST({c} AS BLOB))>1048576' for c in selected)
            filter_sql = "table_name='rkb_users' AND (" + oversized + ')' if table == 'corpus_rows' else oversized
            if db.execute(f'SELECT 1 FROM {table} WHERE {filter_sql} LIMIT 1').fetchone():
                raise ValueError('authority projection cell bound exceeded')
            values = []
            for record in db.execute(query):
                value = dict(record)
                if legacy:
                    value['issuer'] = ''
                encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
                total_rows += 1
                total_bytes += len(encoded)
                if len(encoded) > 1024 * 1024 or total_rows > max_rows or total_bytes > max_bytes:
                    raise ValueError('authority projection bound exceeded')
                values.append(value)
            rows[table] = values
    _validate(rows, owner)
    # A candidate is unpublished while built. Only a completed, checked file is
    # linked to the exclusive destination; source schemas/triggers are not copied.
    import tempfile
    with tempfile.TemporaryDirectory(prefix='.authority-', dir=parent) as leaf:
        candidate = Path(leaf) / 'candidate.sqlite3'
        corpus = SQLiteCorpus(candidate)
        PersonalPolicy(Participants(corpus, owner))
        with corpus.connect() as target:
            target.execute('BEGIN IMMEDIATE')
            # Reconstruct only the fixed native receipt protection. Never copy
            # source sqlite_master/trigger SQL into the candidate authority.
            from .oauth_family_projection import owner_projection_receipt_schema
            owner_projection_receipt_schema(target)
            for table, values in rows.items():
                columns = USER_COLUMNS if table == 'corpus_rows' else COLUMNS[table]
                target.executemany(f'INSERT INTO {table}({",".join(columns)}) '
                    f'VALUES({",".join("?" for _ in columns)})',
                    [tuple(r[c] for c in columns) for r in values])
        with corpus.connect() as target:
            if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('candidate integrity failed')
            for table, values in rows.items():
                actual = [dict(r) for r in target.execute(f'SELECT * FROM {table}')]
                key = lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False, allow_nan=False)
                if sorted(map(key, actual)) != sorted(map(key, values)):
                    raise ValueError('candidate readback failed')
            target.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            target.execute('PRAGMA journal_mode=DELETE')
        with candidate.open('rb') as stream:
            os.fsync(stream.fileno())
        digest = hashlib.sha256(candidate.read_bytes()).hexdigest()
        size = candidate.stat().st_size
        os.link(candidate, destination)  # exclusive; never overwrite a destination
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return {'format': 'rkb-authority-projection-v1', 'counts': {k: len(v) for k, v in rows.items()},
            'missing_source_tables': missing, 'bytes': size, 'sha256': digest,
            'activated': False, 'legacy_control_disposition_required': True,
            'oauth_continuity_review_required': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--owner-subject', required=True)
    parser.add_argument('--prepare-unreferenced', action='store_true', required=True)
    args = parser.parse_args()
    print(json.dumps(export_authority(args.source, args.candidate, owner_subject=args.owner_subject), sort_keys=True))


if __name__ == '__main__':
    main()
