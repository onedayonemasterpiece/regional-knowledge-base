"""Snapshot of launcher book tasks: accepted pages per ingestion, appended to snapshots.jsonl."""
import json, os, subprocess, sys, time

LMCP = '/home/dev/projects/agent-crew/bin/lmcp'
ENV = dict(os.environ, LAUNCHER_AUTH_FILE='/home/dev/.config/chatgpt-launcher/mcp-authorization.header')
OUT = os.environ.get('RKB_SNAPSHOTS', os.path.expanduser('~/rkb-import-snapshots.jsonl'))


def call(tool, args):
    raw = subprocess.run([LMCP, tool, json.dumps(args)], env=ENV, capture_output=True, text=True, timeout=60).stdout
    return json.loads(json.loads(raw)['result']['content'][0]['text'])


tasks = {}
chats = call('chatgpt_list_chats', {})
for chat in (chats if isinstance(chats, list) else chats['chats']):
    for job in {chat['last_job_id'], chat['source_job_id']}:
        task = (call('chatgpt_task_status', {'job_id': job}).get('book_task') or {})
        if task.get('book_task_id'):
            tasks[task['book_task_id']] = task
for task_id in sys.argv[1:]:
    task = call('chatgpt_task_status', {'job_id': task_id}).get('book_task') or {}
    if task.get('book_task_id'):
        tasks[task['book_task_id']] = task
now = time.time()
rows = [dict(t=now, task=t['book_task_id'][:8], ingestion=t['ingestion_id'][:8], state=t['state'],
             pages=t['accepted_pages'], cursor=t['next_cursor'], last_op=t['last_operation_at'],
             activity=t['activity_state'], book=f"{t['current_book_index'] + 1}/{t['total_books']}")
        for t in tasks.values()]
with open(OUT, 'a') as out:
    for row in rows:
        out.write(json.dumps(row) + '\n')
for row in sorted(rows, key=lambda r: r['ingestion']):
    print(row['task'], row['ingestion'], row['state'], row['book'], row['pages'], row['cursor'], (row['last_op'] or '-')[11:19], row['activity'])
