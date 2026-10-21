"""One operator-owned development launch; historical outcomes stay UNKNOWN.

Internal queue integration only. No public RPC, credentials, provider transport,
status rewrite, job cleanup or automatic retry is introduced here.
"""
import hashlib
import json
import math

FIELDS=('id','provider_ref','provider_version','status','launch_state','token_hash',
        'launch_nonce','runtime_incarnation','connection_generation','channel_active',
        'attachment_conflict','heartbeat','started')


def fingerprint(row):
    return hashlib.sha256(json.dumps({k:row[k] for k in FIELDS},sort_keys=True,
        separators=(',',':')).encode()).hexdigest()


def historical(db,run_id,provider_ref):
    return db.execute("""select * from runs where id IS NOT ?
        and launch_state in ('dispatching','dispatched')
        and (provider_ref IS NULL or provider_ref=?) order by id""",(run_id,provider_ref)).fetchall()


def prepare(queue,run_id,*,provider_ref,operation_key,reason_reference,seconds=900):
    """Persist exact fenced old rows before one named development attempt.

    The local operator has the same authority as the existing controller. This
    is not an end-user tool or a caller-supplied compute-termination assertion.
    An ordinary development launch needs no additional per-iteration approval.
    """
    for text,limit in ((provider_ref,256),(operation_key,128),(reason_reference,1024)):
        if type(text) is not str or not 1<=len(text)<=limit or any(ord(c)<32 for c in text):
            raise ValueError('bounded development launch identity required')
    if provider_ref.count('/')!=1 or not all(provider_ref.split('/')):
        raise ValueError('exact notebook reference required')
    if type(seconds) is not int or not 1<=seconds<=900:raise ValueError('bounded launch lease required')
    with queue.connect() as db:
        previous=db.execute('select * from bge_debug_launches where operation_key=?',(operation_key,)).fetchone()
        if previous:
            if (previous['run_id'],previous['provider_ref'],previous['reason_reference'])!=(run_id,provider_ref,reason_reference):
                raise ValueError('development launch key reused with changed intent')
            return receipt(previous)
        run=db.execute('select * from runs where id=?',(run_id,)).fetchone()
        if (not run or run['status']!='starting' or run['launch_state']!='pending'
                or run['channel_active'] or run['provider_ref'] not in (None,provider_ref)
                or queue._run_expired(run,queue.clock())):
            raise ValueError('fresh pending development candidate required')
        rows=historical(db,run_id,provider_ref)
        if not 1<=len(rows)<=512:raise ValueError('bounded historical reservations required')
        if any(row['status']!='expired' or row['channel_active'] or row['attachment_conflict'] for row in rows):
            raise ValueError('historical worker authority must be fenced first')
        writer=db.execute("select 1 from sqlite_master where type='table' and name='rkb_writer_control'").fetchone()
        if writer and db.execute('select 1 from rkb_writer_control where active!=0 or publication_json IS NOT NULL').fetchone():
            raise ValueError('active writer or publication must reconcile first')
        for row in rows:
            if db.execute("select 1 from jobs where run_id=? and state='claimed'",(row['id'],)).fetchone():
                raise ValueError('historical claims must reconcile first')
        inventory=json.dumps({row['id']:fingerprint(row) for row in rows},sort_keys=True,separators=(',',':'))
        now=queue.clock()
        db.execute('insert into bge_debug_launches values(?,?,?,?,?,?,?,NULL)',
            (operation_key,run_id,provider_ref,reason_reference,inventory,now,now+seconds))
        return receipt(db.execute('select * from bge_debug_launches where operation_key=?',(operation_key,)).fetchone())


def receipt(row):
    return {'operation_key':row['operation_key'],'run_id':row['run_id'],
        'provider_ref':row['provider_ref'],'historical_unknown_count':len(json.loads(row['inventory_json'])),
        'inventory_sha256':hashlib.sha256(row['inventory_json'].encode()).hexdigest(),
        'expires_at':row['expires_at'],'consumed_at':row['consumed_at'],
        'historical_outcomes_preserved':True}


def scoped_ids(db,run_id,provider_ref,now,*,claim=False):
    row=db.execute('select * from bge_debug_launches where run_id=?',(run_id,)).fetchone()
    if not row or provider_ref not in (None,row['provider_ref']):return None
    if row['consumed_at'] is None:
        if not math.isfinite(now) or now>=row['expires_at']:return None
    elif claim:return None
    inventory=json.loads(row['inventory_json'])
    current=historical(db,run_id,row['provider_ref'])
    # Check EVERY enrolled ID, including rows that moved outside this slug's
    # query. Only a genuine native receipt may remove an enrolled reservation.
    for old_id,expected in inventory.items():
        enrolled=db.execute('select * from runs where id=?',(old_id,)).fetchone()
        if enrolled is None:return None
        if fingerprint(enrolled)==expected:continue
        terminal=db.execute('select receipt_json from bge_provider_terminal_receipts where run_id=?',
                            (old_id,)).fetchone()
        if enrolled['launch_state']!='terminal' or terminal is None:return None
        native=json.loads(terminal['receipt_json'])['identity']
        original=dict(enrolled);original['launch_state']=native['launch_state']
        if (any(original[k]!=v for k,v in native.items())
                or fingerprint(original)!=expected):return None
    if any(inventory.get(old['id'])!=fingerprint(old) for old in current):return None
    if claim:
        writer=db.execute("select 1 from sqlite_master where type='table' and name='rkb_writer_control'").fetchone()
        if writer and db.execute('select 1 from rkb_writer_control where active!=0 or publication_json IS NOT NULL').fetchone():return None
        for old in current:
            if db.execute("select 1 from jobs where run_id=? and state='claimed'",(old['id'],)).fetchone():return None
    return {old['id'] for old in current}
