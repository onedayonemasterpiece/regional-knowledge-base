"""Real loopback HTTP bodies using the installed, declared httpx dependency.

The thin session/adapter surface models the SDK's Requests boundary;
httpx supplies actual socket reads, buffering and redirect responses. No Kaggle.
"""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from regional_knowledge import kaggle_provider_terminal as native


@pytest.fixture
def server():
    seen=[]
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*args): pass
        def do_GET(self):
            seen.append(self.path)
            if self.path.startswith('/redirect'):
                self.send_response(302)
                self.send_header('Location','/target')
                self.send_header('Content-Length','100000000')
                self.end_headers()
                # Fail if a transport reads intermediate content before
                # rejecting the redirect. The first body byte is delayed.
                time.sleep(.7)
                return
            if self.path=='/target':
                self.send_response(200);self.send_header('Content-Length','2')
                self.end_headers();self.wfile.write(b'[]');return
            body=b'[{"stream_name":"stderr","time":1,"data":"fixture"}]'
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)))
            self.end_headers()
            try:
                if self.path=='/drip':
                    # No inactivity gap exceeds the configured read timeout,
                    # but neither the body nor an 8192-byte chunk completes
                    # within the total elapsed-time budget.
                    for byte in body:
                        self.wfile.write(bytes([byte]));self.wfile.flush();time.sleep(.025)
                else: self.wfile.write(body)
            except (BrokenPipeError,ConnectionResetError): pass
    httpd=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    httpd.daemon_threads=True
    thread=threading.Thread(target=httpd.serve_forever,daemon=True);thread.start()
    try: yield f'http://127.0.0.1:{httpd.server_port}',seen
    finally: httpd.shutdown();httpd.server_close();thread.join(1)


class Response:
    def __init__(self,response):
        self.response=response;self.status_code=response.status_code;self.headers=response.headers
        self.closed=False
    def iter_content(self,chunk_size=8192):
        if getattr(self,'_content_consumed',False):
            for offset in range(0,len(self._content),chunk_size):yield self._content[offset:offset+chunk_size]
        else: yield from self.response.iter_bytes(chunk_size=chunk_size)
    def close(self): self.closed=True;self.response.close()
    def json(self):return json.loads(self._content)


@contextlib.contextmanager
def sdk_boundary():
    with httpx.Client(trust_env=False) as http:
        state=SimpleNamespace(session_calls=0,responses=[],adapter_calls=0)
        def adapter_send(request,**kwargs):
            state.adapter_calls+=1
            connect,read=kwargs['timeout']
            req=http.build_request('GET',request.url,timeout=httpx.Timeout(read,connect=connect))
            response=Response(http.send(req,stream=True,follow_redirects=False))
            state.responses.append(response);return response
        def session_send(request,**kwargs):
            state.session_calls+=1
            response=adapter_send(request,**kwargs)
            # Requests Session.send may consume redirects even when preparing
            # its next request with allow_redirects=False. Deliberately model
            # that dangerous behavior to prove it is not called.
            if 300<=response.status_code<400:
                b''.join(response.iter_content())
            return response
        session=SimpleNamespace(send=session_send,
            get_adapter=lambda url:SimpleNamespace(send=adapter_send))
        client=SimpleNamespace(_http_client=SimpleNamespace(_init_session=lambda:None,
            _session=session,_verbose=False))
        yield client,session,state


def test_nonchunked_slow_drip_has_total_deadline_and_closes_response(server):
    base,_=server
    with sdk_boundary() as (client,session,state):
        original=session.send;started=time.monotonic()
        with pytest.raises((TimeoutError,httpx.HTTPError)):
            with native._bounded_transport(client,started+.12,time.monotonic) as budget:
                budget[0]=native.MAX_STATUS_BYTES
                session.send(SimpleNamespace(url=base+'/drip'))
        assert time.monotonic()-started<.6
        assert session.send is original
        assert state.responses and all(r.closed for r in state.responses)


def test_redirect_body_is_not_read_and_location_not_followed(server):
    base,seen=server
    with sdk_boundary() as (client,session,state):
        original=session.send;started=time.monotonic()
        with pytest.raises(ValueError):
            with native._bounded_transport(client,started+.12,time.monotonic):
                session.send(SimpleNamespace(url=base+'/redirect'))
        assert time.monotonic()-started<.3
        assert state.session_calls==0 and state.adapter_calls==1
        assert seen==['/redirect']
        assert all(r.closed for r in state.responses)
        assert session.send is original


def test_successful_real_http_body_remains_available_for_sdk_decode(server):
    base,_=server
    with sdk_boundary() as (client,session,state):
        started=time.monotonic()
        with native._bounded_transport(client,started+1,time.monotonic) as budget:
            budget[0]=native.MAX_STATUS_BYTES
            response=session.send(SimpleNamespace(url=base+'/complete'))
            assert response.json()[0]['data']=='fixture'
        assert state.session_calls==0 and all(r.closed for r in state.responses)


def test_deadline_restores_alarm_and_refuses_nonmain_thread_before_io(server):
    import signal
    base,seen=server
    before=signal.getsignal(signal.SIGALRM)
    with sdk_boundary() as (client,session,state):
        with pytest.raises((TimeoutError,httpx.HTTPError)):
            with native._bounded_transport(client,time.monotonic()+.12,time.monotonic):
                session.send(SimpleNamespace(url=base+'/drip'))
        assert signal.getsignal(signal.SIGALRM)==before
        assert signal.getitimer(signal.ITIMER_REAL)==(0.,0.)
        calls=state.adapter_calls;errors=[]
        def attempt():
            try:
                with native._bounded_transport(client,time.monotonic()+1,time.monotonic):
                    session.send(SimpleNamespace(url=base+'/complete'))
            except Exception as error:errors.append(type(error))
        thread=threading.Thread(target=attempt);thread.start();thread.join(1)
        assert not thread.is_alive() and errors==[RuntimeError]
        assert state.adapter_calls==calls


def test_occupied_alarm_fails_closed_without_replacing_owners_timer():
    import signal
    prior=signal.getsignal(signal.SIGALRM)
    assert signal.getitimer(signal.ITIMER_REAL)==(0.,0.)
    signal.setitimer(signal.ITIMER_REAL,5)
    try:
        with pytest.raises(RuntimeError):
            with native._total_deadline(time.monotonic()+1,time.monotonic):
                pytest.fail('occupied timer must reject before I/O')
        assert signal.getsignal(signal.SIGALRM)==prior
        assert 0<signal.getitimer(signal.ITIMER_REAL)[0]<=5
    finally:signal.setitimer(signal.ITIMER_REAL,0)
