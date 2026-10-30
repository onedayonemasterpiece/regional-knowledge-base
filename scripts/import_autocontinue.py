#!/usr/bin/env python3
"""Continue the single reader window of one RKB ingestion through ChatGPT Launcher.

The reader chat is an explicit assignment in <state>/<ing8>.chat (one chat_url);
only RKB-Import changes it. A Launcher successor chat (continuation limit) is
followed by writing the job's result_chat_url there. The watched job and the
request_id of a pending continue live in <state>/<ing8>.json, written before the
send, so a restart resumes the same job and repeats a lost send with the same key.
continue is sent only after the watched turn finished (model done, or Launcher
reconcile proved page_completed); a busy chat is not an error, its job is watched.
After blocked/failed: at most once per 20 min, at most 3 times in a row.
"""
from __future__ import annotations
import datetime, json, os, re, subprocess, sys, time

LMCP = os.environ.get('RKB_LAUNCHER_LMCP', '/home/dev/projects/agent-crew/bin/lmcp')
STATE_DIR = os.environ.get('RKB_AUTOCONTINUE_STATE_DIR', os.path.expanduser('~/.local/state/rkb-import-autocontinue'))
BLOCKED_RETRY_S, BLOCKED_MAX, CONTINUE_GAP_S, RECONCILE_GAP_S = 1200, 3, 170, 600


def finished_turn(job):
    """Model reported done, or Launcher proved the exact job's turn completed."""
    return job.get('model_status') == 'done' or job.get('execution') == 'page_completed'


def decide(job, blocked_tries, since_last_try):
    """Return 'continue', 'reconcile' or 'wait' for the watched job (None: nothing watched yet)."""
    if job is None or finished_turn(job):
        return 'continue' if since_last_try >= CONTINUE_GAP_S else 'wait'
    if job.get('model_status') in ('blocked', 'failed'):
        ok = blocked_tries < BLOCKED_MAX and since_last_try > BLOCKED_RETRY_S
        return 'continue' if ok else 'wait'
    if job.get('execution') == 'unconfirmed' or job.get('status') == 'needs_review':
        return 'reconcile' if since_last_try >= RECONCILE_GAP_S else 'wait'
    return 'wait'


def prompt(ingestion, book):
    return (f'Заказчик RKB-Импорт: продолжай {book} в том же ingestion {ingestion} дальше без пауз, '
            'по инструкциям RKB MCP. Начни с book_ingest(status) и следуй next_action; '
            'принятые страницы не повторяй. В тред — как раньше (STARTING_STAGE / APPLIED / BLOCKED).')


def main(ingestion, book, hours=720.0):
    base = os.path.join(STATE_DIR, ingestion[:8])
    now = lambda: datetime.datetime.now(datetime.timezone.utc)

    def log(msg):
        with open(base + '.log', 'a') as f:
            f.write(now().strftime('%m-%d %H:%M:%S ') + msg + '\n')

    def load():
        try:
            return json.load(open(base + '.json'))
        except FileNotFoundError:
            return {'watched': None, 'pending_request': None, 'blocked_tries': 0, 'last_try': 0.0}

    def save(state):
        with open(base + '.json.tmp', 'w') as f:
            json.dump(state, f)
        os.replace(base + '.json.tmp', base + '.json')

    def call(tool, args):
        out = subprocess.run([LMCP, tool, json.dumps(args, ensure_ascii=False)],
                             capture_output=True, text=True, timeout=60).stdout
        return json.loads(json.loads(out)['result']['content'][0]['text'])

    state, deadline = load(), time.time() + hours * 3600
    log(f'start ing={ingestion} watched={state["watched"]} pending={state["pending_request"]}')
    while time.time() < deadline:
        try:
            chat = open(base + '.chat').read().strip()
            job = None
            if state['watched']:
                job = call('chatgpt_task_status', {'job_id': state['watched']})
                job = job.get('job', job)
                moved = job.get('result_chat_url')
                if moved and moved.rsplit('/c/', 1)[-1][:8] != chat.rsplit('/c/', 1)[-1][:8]:
                    with open(base + '.chat', 'w') as f:
                        f.write(moved)
                    chat = moved
                    log(f'chat moved -> {moved}')
                if finished_turn(job):
                    state['blocked_tries'] = 0
            action = 'continue' if state['pending_request'] else \
                decide(job, state['blocked_tries'], time.time() - state['last_try'])
            if action == 'reconcile':
                state['last_try'] = time.time()
                save(state)
                res = call('chatgpt_reconcile', {'job_id': state['watched'],
                           'request_id': f'rkbimp-reconcile-{state["watched"][:8]}-{now():%Y%m%dT%H}'})
                log(f'reconcile {state["watched"][:8]} -> {str(res)[:120]}')
            elif action == 'continue':
                if job is not None and not finished_turn(job) and not state['pending_request']:
                    state['blocked_tries'] += 1
                state['pending_request'] = state['pending_request'] or \
                    f'rkbimp-auto-{ingestion[:8]}-{now():%Y%m%dT%H%M%S}'
                state['last_try'] = time.time()
                save(state)  # the key survives a lost response or restart
                res = call('chatgpt_continue', {'chat_url': chat, 'prompt': prompt(ingestion, book),
                           'request_id': state['pending_request']})
                busy = re.search(r'job ([0-9a-f-]{36})', str(res.get('error', '')))
                if res.get('job_id'):
                    state.update(watched=res['job_id'], pending_request=None)
                    log(f'continue -> job {res["job_id"]}')
                elif busy:
                    state.update(watched=busy.group(1), pending_request=None)
                    log(f'chat busy, watching job {busy.group(1)}')
                else:
                    state['pending_request'] = None
                    log(f'continue refused: {str(res)[:160]}')
                save(state)
            time.sleep(90)
        except Exception as exc:  # keep the single coordinator alive; the journal shows why
            log(f'error {type(exc).__name__}: {str(exc)[:160]}')
            time.sleep(120)
    log('exit')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], float(sys.argv[3]) if len(sys.argv) > 3 else 720.0)
