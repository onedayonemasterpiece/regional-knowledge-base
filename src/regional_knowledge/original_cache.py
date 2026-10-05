"""Bounded private originals cache with cross-process leases and idle expiry.

An access renews last_used; existing maintenance owns periodic cleanup. File
locks protect installs, reads and cleanup across MCP/worker processes. Permission
checks are the caller's responsibility and must happen before opening a lease.
"""
import asyncio
import fcntl
import hashlib
import os
import re
import shutil
import sqlite3
import time
from contextlib import asynccontextmanager,contextmanager
from pathlib import Path

class OriginalCache:
    def __init__(self,root,*,idle_seconds=14400,max_bytes=512*1024*1024,clock=time.time):
        if idle_seconds<=0 or max_bytes<=0:raise ValueError('positive cache limits required')
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.idle_seconds=idle_seconds;self.max_bytes=max_bytes;self.clock=clock
        with self.db() as db:db.execute('create table if not exists entries(sha text primary key,size integer not null,last_used real not null)')
        (self.root/'cache.sqlite3').chmod(0o600)

    @classmethod
    def from_env(cls):
        return cls(os.getenv('RKB_ORIGINAL_CACHE_DIR','/home/dev/.local/state/regional-knowledge-base/originals'),
          idle_seconds=float(os.getenv('RKB_ORIGINAL_CACHE_IDLE_HOURS','4'))*3600,
          max_bytes=int(os.getenv('RKB_ORIGINAL_CACHE_MAX_BYTES',str(512*1024*1024))))

    @contextmanager
    def db(self):
        db=sqlite3.connect(self.root/'cache.sqlite3',timeout=15)
        try:
            with db:yield db
        finally:db.close()

    @contextmanager
    def lock(self,key,*,exclusive=False,blocking=True):
        # Fixed stripes bound lock-file growth. Their contents carry no source IDs.
        stripe=int(key[:8],16)%64
        fd=os.open(self.root/f'lock-{stripe:02d}',os.O_CREAT|os.O_RDWR,0o600)
        try:
            fcntl.flock(fd,(fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)|(0 if blocking else fcntl.LOCK_NB))
            yield
        finally:os.close(fd)

    def touch_existing(self,sha):
        """Renew an authorized warm proof's retained original, never download."""
        if not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('invalid source hash')
        with self.lock(sha):
            if not (self.root/sha).is_file():return False
            with self.db() as db:
                return db.execute('update entries set last_used=? where sha=?',(self.clock(),sha)).rowcount==1

    def cleanup(self,*,reserve=0):
        deleted=0
        # Crash leftovers are collectible only after their installation/read
        # lease is gone. No PID reuse/liveness heuristic can delete a live file.
        for partial in self.root.glob('*.partial-*'):
            sha=partial.name.split('.partial-',1)[0]
            if not re.fullmatch('[a-f0-9]{64}',sha):continue
            try:
                with self.lock(sha,exclusive=True,blocking=False):partial.unlink(missing_ok=True)
            except BlockingIOError:continue
        with self.db() as db:entries=db.execute('select sha,size,last_used from entries order by last_used,sha').fetchall()
        total=sum(row[1] for row in entries)
        for sha,size,last in entries:
            if self.clock()-last<self.idle_seconds and total+reserve<=self.max_bytes:continue
            try:
                with self.lock(sha,exclusive=True,blocking=False):
                    with self.db() as db:
                        row=db.execute('select size,last_used from entries where sha=?',(sha,)).fetchone()
                        if not row:continue
                        if self.clock()-row[1]<self.idle_seconds and total+reserve<=self.max_bytes:continue
                        (self.root/sha).unlink(missing_ok=True)
                        db.execute('delete from entries where sha=?',(sha,));total-=row[0];deleted+=1
            except BlockingIOError:continue
        return {'deleted':deleted,'bytes':total}

    @asynccontextmanager
    async def access(self,sha,loader):
        if not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('invalid source hash')
        path=self.root/sha
        # Never block the event loop while another process downloads. Exclusive
        # lock acquisition runs in a thread; downgrade after verified install.
        stripe=int(sha[:8],16)%64
        fd=os.open(self.root/f'lock-{stripe:02d}',os.O_CREAT|os.O_RDWR,0o600)
        try:
            await asyncio.to_thread(fcntl.flock,fd,fcntl.LOCK_EX)
            if not path.exists():
                temp=self.root/(sha+'.partial-'+str(os.getpid()))
                try:
                    await loader(temp)
                    size=temp.stat().st_size
                    if size>self.max_bytes:raise ValueError('original exceeds bounded cache capacity')
                    actual=await asyncio.to_thread(self.hash_file,temp)
                    if actual!=sha:raise ValueError('source_mismatch')
                    await asyncio.to_thread(self.install,sha,temp,size)
                finally:temp.unlink(missing_ok=True)
            with self.db() as db:db.execute('insert into entries values(?,?,?) on conflict(sha) do update set last_used=excluded.last_used,size=excluded.size',(sha,path.stat().st_size,self.clock()))
            fcntl.flock(fd,fcntl.LOCK_SH)
            yield path
        finally:os.close(fd)

    def install(self,sha,temp,size):
        budget_fd=os.open(self.root/'budget-lock',os.O_CREAT|os.O_RDWR,0o600)
        try:
            fcntl.flock(budget_fd,fcntl.LOCK_EX)
            self.cleanup(reserve=size)
            with self.db() as db:
                total=db.execute('select coalesce(sum(size),0) from entries').fetchone()[0]
                if total+size>self.max_bytes:raise RuntimeError('original_cache_capacity_in_use')
                temp.chmod(0o600);temp.replace(self.root/sha)
                db.execute('insert into entries values(?,?,?) on conflict(sha) do update set last_used=excluded.last_used,size=excluded.size',(sha,size,self.clock()))
        finally:os.close(budget_fd)

    @staticmethod
    def hash_file(path):
        h=hashlib.sha256()
        with path.open('rb') as source:
            while block:=source.read(1024*1024):h.update(block)
        return h.hexdigest()
