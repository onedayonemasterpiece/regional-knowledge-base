#!/usr/bin/env python3
"""Continue the single reader window of one RKB ingestion through ChatGPT Launcher.

The reader chat is an explicit assignment in <state>/<ing8>.chat (one chat_url);
only RKB-Import changes it. A Launcher successor chat (continuation limit) is
followed by writing the job's result_chat_url there. continue is sent only after
the watched job finished its turn; a busy chat is not an error, its job is watched.
After blocked/failed: at most once per 20 min, at most 3 times in a row.
"""
from __future__ import annotations
import datetime, json, os, re, subprocess, sys, time

LMCP = os.environ.get('RKB_LAUNCHER_LMCP', '/home/dev/projects/agent-crew/bin/lmcp')
STATE_DIR = os.environ.get('RKB_AUTOCONTINUE_STATE_DIR', os.path.expanduser('~/.local/state/rkb-import-autocontinue'))
BLOCKED_RETRY_S, BLOCKED_MAX, CONTINUE_GAP_S = 1200, 3, 170


def finished_turn(job):
    """Model reported done, or Launcher proved the exact job's turn completed."""
    return job.get('model_status') == 'done' or job.get('execution') == 'page_completed'


def decide(job, blocked_tries, since_last_try):
    """Return 'continue' or 'wait' for the watched job (None: nothing watched yet)."""
    if since_last_try < CONTINUE_GAP_S:
        return 'wait'
    if job is None or finished_turn(job):
        return 'continue'
    if job.get('model_status') in ('blocked', 'failed') and blocked_tries < BLOCKED_MAX \
            and since_last_try > BLOCKED_RETRY_S:
        return 'continue'
    return 'wait'


def prompt(ingestion, book):
    return (f'Заказчик RKB-Импорт: продолжай {book} в том же ingestion {ingestion} дальше без пауз, '
            'по инструкциям RKB MCP. Начни с book_ingest(status) и следуй next_action; '
            'принятые страницы не повторяй. В тред — как раньше (STARTING_STAGE / APPLIED / BLOCKED).')


def main(ingestion, book, hours=720.0):
    chat_file = os.path.join(STATE_DIR, f'{ingestion[:8]}.chat')
    log_file = os.path.join(STATE_DIR, f'{ingestion[:8]}.log')
    now = lambda: datetime.datetime.now(datetime.timezone.utc)

    def log(msg):
        with open(log_file, 'a') as f:
            f.write(now().strftime('%m-%d %H:%M:%S ') + msg + '\n')

    def call(tool, args):
        out = subprocess.run([LMCP, tool, json.dumps(args, ensure_ascii=False)],
                             capture_output=True, text=True, timeout=60).stdout
        return json.loads(json.loads(out)['result']['content'][0]['text'])

    deadline, watched, blocked_tries, last_try = time.time() + hours * 3600, None, 0, 0.0
    log(f'start ing={ingestion}')
    while time.time() < deadline:
        try:
            chat = open(chat_file).read().strip()
            job = None
            if watched:
                job = call('chatgpt_task_status', {'job_id': watched})
                job = job.get('job', job)
                moved = job.get('result_chat_url')
                if moved and moved.rsplit('/c/', 1)[-1][:8] != chat.rsplit('/c/', 1)[-1][:8]:
                    with open(chat_file, 'w') as f:
                        f.write(moved)
                    chat = moved
                    log(f'chat moved -> {moved}')
                if finished_turn(job):
                    blocked_tries = 0
            if decide(job, blocked_tries, time.time() - last_try) == 'continue':
                if job is not None and not finished_turn(job):
                    blocked_tries += 1
                last_try = time.time()
                res = call('chatgpt_continue', {'chat_url': chat, 'prompt': prompt(ingestion, book),
                           'request_id': f'rkbimp-auto-{ingestion[:8]}-{now():%Y%m%dT%H%M}'})
                busy = re.search(r'job ([0-9a-f-]{36})', str(res.get('error', '')))
                if res.get('job_id'):
                    watched = res['job_id']
                    log(f'continue -> job {watched}')
                elif busy and busy.group(1) != watched:
                    watched = busy.group(1)
                    log(f'chat busy, watching job {watched}')
                else:
                    log(f'continue refused: {str(res)[:160]}')
            time.sleep(90)
        except Exception as exc:  # keep the single coordinator alive; the journal shows why
            log(f'error {type(exc).__name__}: {str(exc)[:160]}')
            time.sleep(120)
    log('exit')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], float(sys.argv[3]) if len(sys.argv) > 3 else 720.0)
