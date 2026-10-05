"""Resumable, exact snapshot migration. No OCR, encoders, or destructive SQL.

Usage: python scripts/production/migrate_sqlite_corpus.py --database PATH --evidence DIR
A repeatable-read remote snapshot feeds short local batches. Local backup API is
restored and compared before any later operator may retire remote details.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from operator_env import load_service_env
from regional_knowledge.sqlite_corpus import SQLiteCorpus,TABLES,COLUMNS,canonical,row_key
import hashlib

async def run(args):
    load_service_env();corpus=SQLiteCorpus(args.database);counts={};expected=[]
    async with await psycopg.AsyncConnection.connect(os.environ['KB_SUPABASE_SESSION_CONNECTION'],row_factory=dict_row) as db:
        await db.execute('set transaction isolation level repeatable read read only')
        await db.execute("set local statement_timeout='60s'")
        before=(await(await db.execute('select pg_database_size(current_database()) bytes')).fetchone())['bytes']
        for table in sorted(TABLES):
            names=[c['column_name'] for c in COLUMNS[table] if c['column_name']!='catalog']
            query=sql.SQL('select {} from public.{}').format(sql.SQL(',').join(map(sql.Identifier,names)),sql.Identifier(table))
            counts[table]=0
            async with db.cursor(name='snapshot_'+table) as cursor:
                await cursor.execute(query)
                while rows:=await cursor.fetchmany(256):
                    clean=[]
                    for row in rows:
                        row=dict(row)
                        if table=='rkb_chunks':
                            # Derived vectors/PG FTS stay on the vector plane.
                            row.pop('embedding',None);row.pop('fts',None)
                        row=json.loads(canonical(row));clean.append(row)
                        expected.append((table,row_key(table,row),canonical(row)))
                    await asyncio.to_thread(corpus.put,table,clean)
                    counts[table]+=len(clean)
        after=(await(await db.execute('select pg_database_size(current_database()) bytes')).fetchone())['bytes']
    expected.sort();digest=hashlib.sha256()
    for row in expected:digest.update(canonical(list(row)).encode())
    parity=corpus.digest()
    if parity!={'rows':len(expected),'sha256':digest.hexdigest()}:raise ValueError('snapshot parity failed (stale local rows or mismatch)')
    fragments=await asyncio.to_thread(corpus.build_fragments)
    args.evidence.mkdir(parents=True,exist_ok=True)
    backup=await asyncio.to_thread(corpus.backup,args.evidence/'corpus-backup.sqlite3')
    restored=SQLiteCorpus(backup)
    if restored.digest()!=parity:raise ValueError('restored backup parity failed')
    report={'counts':counts,'parity':parity,'backup_restored':True,'fragments':fragments,
      'supabase_before_bytes':before,'supabase_after_bytes':after,'sqlite_bytes':corpus.path.stat().st_size,
      'sqlite_wal_bytes':Path(str(corpus.path)+'-wal').stat().st_size if Path(str(corpus.path)+'-wal').exists() else 0,
      're_ocr':False,'re_embed':False,'remote_details_deleted':False}
    (args.evidence/'migration.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--database',type=Path,required=True);p.add_argument('--evidence',type=Path,required=True)
    asyncio.run(run(p.parse_args()))
