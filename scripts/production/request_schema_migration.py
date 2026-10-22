"""Queue one schema update for the existing API/controller; never start a notebook.

Run from the deployed source with its normal PYTHONPATH. --sql-files contains a
JSON map of new sql/NNN_name.sql files to UTF-8 SQL text. The authenticated
controller sends it through schema_migrate; the worker verifies the manifest
and applied-file ledger. The API retains all writer/phase authority.
"""
import argparse
import json
import os
from pathlib import Path

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_schema_owner import request_migration


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue',default=os.getenv('RKB_BGE_QUEUE_PATH'))
    parser.add_argument('--target-version',required=True,type=int)
    parser.add_argument('--bundle-sha256',required=True)
    delivery=parser.add_mutually_exclusive_group()
    delivery.add_argument('--artifact-approval',type=Path)
    delivery.add_argument('--sql-files',type=Path)
    args=parser.parse_args()
    if not args.queue or not Path(args.queue).is_file():
        parser.error('the existing configured queue is required')
    queue=BgeQueue(args.queue)
    with queue.connect(write=False) as db:
        row=db.execute("""SELECT w.run_id,w.database_generation,w.writer_generation
            FROM rkb_writer_control w JOIN control c ON c.current_run=w.run_id
            JOIN runs r ON r.id=w.run_id
            WHERE w.active=1 AND r.status='ready' AND r.channel_active=1
            AND w.publication_json IS NULL""").fetchone()
    if row is None:
        parser.error('a current ready writer without unresolved publication is required')
    artifact=json.loads(args.artifact_approval.read_text()) if args.artifact_approval else None
    sql_files=json.loads(args.sql_files.read_text()) if args.sql_files else None
    operation=request_migration(queue,target_schema_version=args.target_version,
        bundle_sha256=args.bundle_sha256,artifact=artifact,sql_files=sql_files,**dict(row))
    print(json.dumps(dict(operation_key=operation,status='requested')))


if __name__=='__main__':main()
