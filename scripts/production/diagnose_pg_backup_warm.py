"""One fixed backup self-test staged through the existing privileged Python owner.
The existing read handler is returned unchanged and is never activated.
No shell, provider request, upload, writer change or Saved promotion.
"""
import asyncio,json,os,shutil,signal,tempfile,time
from pathlib import Path
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery as ExistingQuery
from regional_knowledge.kaggle_pg_snapshot_native import pack_tree,SnapshotLimits
_TASK=None

async def tail(stream):
    value=b''
    while chunk:=await stream.read(4096):value=(value+chunk)[-4096:]
    return value

async def diagnose(acquire):
    result=dict(event='rkb_snapshot_diagnostic_v304',stage='database',success=False,provider_calls=0,saved_promoted=False,commands=[])
    root=None;started=time.monotonic()
    try:
        async with acquire() as connection:
            cursor=await connection.execute("SELECT pg_backend_pid(),current_setting('max_connections')::int,(SELECT count(*) FROM pg_stat_activity),applied_seq FROM rkb_v4.recovery_state WHERE singleton")
            pid,maximum,connections,sequence=await cursor.fetchone()
            result['database']=dict(applied_sequence=sequence,max_connections=maximum,connections=connections)
            socket,port=connection.info.host,connection.info.port
            assert socket.startswith('/kaggle/temp/') and port>0
            parent=int(next(line.split(':')[1] for line in Path('/proc',str(pid),'status').read_text().splitlines() if line.startswith('PPid:')))
            proc=Path('/proc',str(parent));postgres=(proc/'exe').resolve(strict=True)
            assert postgres.name=='postgres'
            info=proc.stat();uid,gid=info.st_uid,info.st_gid
            allowed={b'PATH',b'LANG',b'LC_ALL',b'LD_LIBRARY_PATH'}
            env={k.decode():v.decode() for item in (proc/'environ').read_bytes().split(b'\0') if b'=' in item for k,v in [item.split(b'=',1)] if k in allowed}
        result['stage']='workspace'
        free=shutil.disk_usage('/kaggle/temp').free;result['free_bytes']=free
        assert free>8*1024**3,'Insufficient private scratch for one backup and archive'
        root=Path(tempfile.mkdtemp(prefix='rkb-snapshot-diagnostic-v304-',dir='/kaggle/temp'));os.chmod(root,0o700)
        identity={}
        if os.geteuid()==0:os.chown(root,uid,gid);identity=dict(user=uid,group=gid,extra_groups=[])
        else:assert os.geteuid()==uid and os.getegid()==gid
        commands=(('pg_basebackup',('--pgdata',str(root/'base'),'--wal-method=stream','--checkpoint=fast','--manifest-checksums=SHA256','--no-password','--host',socket,'--port',str(port),'--username','rkb_bootstrap')),('pg_verifybackup',(str(root/'base'),)))
        for label,args in commands:
            result['stage']=label
            print(json.dumps(dict(event='rkb_snapshot_diagnostic_v304_started',stage=label)),flush=True)
            process=await asyncio.create_subprocess_exec(str(postgres.parent/label),*args,stdin=asyncio.subprocess.DEVNULL,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,env=env,start_new_session=True,**identity)
            try:
                async with asyncio.timeout(900):
                    _,stderr=await asyncio.gather(tail(process.stdout),tail(process.stderr));await process.wait()
            finally:
                if process.returncode is None:os.killpg(process.pid,signal.SIGKILL);await process.wait()
            result['commands'].append(dict(program=label,exit_code=process.returncode,stderr=stderr.decode('utf8',errors='replace')))
            if process.returncode:return
        result['stage']='archive'
        _,unpacked,_,size,_=await asyncio.to_thread(pack_tree,root/'base',root/'base.tar',SnapshotLimits())
        result.update(stage='complete',success=True,archive_bytes=size,unpacked_bytes=unpacked)
    except Exception as error:
        result.update(error_type=type(error).__name__,error=str(error)[:1024])
    finally:
        if root is not None:await asyncio.to_thread(shutil.rmtree,root)
        result['elapsed_seconds']=round(time.monotonic()-started,3)
        print('RKB_SNAPSHOT_DIAGNOSTIC_V304 '+json.dumps(result,ensure_ascii=False),flush=True)

def KaggleWorkerQuery(**resources):
    global _TASK
    candidate=ExistingQuery(**resources)
    if _TASK is None:_TASK=asyncio.create_task(diagnose(resources['acquire']),name='rkb-snapshot-fixed-diagnostic-v304')
    return candidate
