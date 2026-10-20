"""Stdlib-only retained ticket request on the authenticated worker channel."""
import asyncio
from contextlib import contextmanager
import secrets


class SnapshotTicketChannel:
    def __init__(self,*,timeout_seconds=600):
        if type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=600:
            raise ValueError('bounded snapshot ticket deadline required')
        self.timeout=timeout_seconds;self._assignment=None;self._intent=None;self._future=None
        self.changed=None

    @contextmanager
    def assignment(self,frame):
        if self._assignment is not None:raise ValueError('snapshot transfer resource already assigned')
        self._assignment=frame
        try:yield
        finally:self._assignment=None

    @property
    def pending(self):return self._intent

    async def upload_ticket(self,*,size,sha256):
        frame=self._assignment
        if (frame is None or frame.get('kind')!='recovery_phase' or frame.get('phase')!='capture'
                or type(size) is not int or size<1 or not isinstance(sha256,str)
                or len(sha256)!=64 or set(sha256)-set('0123456789abcdef') or self._future is not None):
            raise ValueError('snapshot upload requires actual capture resource')
        intent=dict(type='SNAPSHOT_UPLOAD_INTENT',request_id=secrets.token_urlsafe(24),
            operation_key=frame['operation_key'],size=size,sha256=sha256,job_id=frame['id'],claim=frame['claim'],
            **{k:frame[k] for k in ('run_id','launch_nonce','runtime_incarnation','compute_claim','dispatch_generation')})
        self._future=asyncio.get_running_loop().create_future();self._intent=intent
        if self.changed is not None:self.changed.set()
        try:
            # A lost socket does not cancel the retained intent or start capture
            # again. Each reattached session sends this same request identity.
            return await asyncio.wait_for(asyncio.shield(self._future),self.timeout)
        finally:
            self._intent=None
            if self._future is not None and not self._future.done():self._future.cancel()
            self._future=None
            if self.changed is not None:self.changed.set()

    def receive(self,frame):
        if (self._intent is None or self._future is None or self._future.done()
                or frame.get('request_id')!=self._intent['request_id']):
            return False
        self._future.set_result(frame['ticket'])
        return True
