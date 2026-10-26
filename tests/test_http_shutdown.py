"""Real HTTP/WebSocket restart; external worker/schema/Saved are synthetic.

No model, provider, production data or PostgreSQL is used by this transport test.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import socket

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route,WebSocketRoute
import uvicorn
import websockets

from regional_knowledge.http_shutdown import HttpDrainingServer


@pytest.mark.asyncio
async def test_migration_saved_restart_drains_search_before_worker_close_then_accepts_page():
    durable={'schema':40,'saved_schema':40,'pages':[]}
    search_received=asyncio.Event();release_search=asyncio.Event()

    @asynccontextmanager
    async def api():
        connected=asyncio.Event();closed=asyncio.Event();channel={}
        async def worker_socket(ws):
            await ws.accept();channel['ws']=ws;connected.set()
            try:
                while True:
                    value=await ws.receive_json()
                    pending=channel.get('pending')
                    if pending is not None and not pending.done():pending.set_result(value)
            except Exception:
                pass
            finally:
                closed.set()
                pending=channel.get('pending')
                if pending is not None and not pending.done():
                    pending.set_exception(ConnectionError('worker closed before response'))
        async def operation(request):
            channel['pending']=asyncio.get_running_loop().create_future()
            await channel['ws'].send_json(await request.json())
            try:
                value=await asyncio.wait_for(channel['pending'],2)
            except ConnectionError:return JSONResponse({'status':'unavailable'},status_code=503)
            return JSONResponse(value)
        app=Starlette(routes=[Route('/operation',operation,methods=['POST']),
            WebSocketRoute('/worker',worker_socket)])
        sock=socket.socket();sock.bind(('127.0.0.1',0));sock.listen(10)
        port=sock.getsockname()[1]
        server=HttpDrainingServer(uvicorn.Config(app,log_level='error',lifespan='off',
            ws='websockets-sansio',timeout_graceful_shutdown=.8))
        serving=asyncio.create_task(server.serve(sockets=[sock]))
        async def external_worker():
            async with websockets.connect(f'ws://127.0.0.1:{port}/worker',close_timeout=.2)as ws:
                async for raw in ws:
                    command=json.loads(raw)
                    if command['op']=='migrate':durable['schema']=42
                    elif command['op']=='save':durable['saved_schema']=durable['schema']
                    elif command['op']=='search':
                        search_received.set();await release_search.wait()
                    elif command['op']=='page':
                        assert durable['schema']==durable['saved_schema']==42
                        durable['pages'].append(command['page'])
                    await ws.send(json.dumps({'status':'ok',**durable}))
        worker=None
        try:
            async with asyncio.timeout(3):
                while not server.started:
                    if serving.done():await serving
                    await asyncio.sleep(.01)
            worker=asyncio.create_task(external_worker())
            await asyncio.wait_for(connected.wait(),2)
            async with httpx.AsyncClient(base_url=f'http://127.0.0.1:{port}',timeout=3)as client:
                yield server,serving,client,closed
        finally:
            release_search.set();server.should_exit=True
            try:await asyncio.wait_for(serving,3)
            finally:
                if worker is not None:
                    worker.cancel();await asyncio.gather(worker,return_exceptions=True)
                sock.close()

    async with api()as(server,serving,client,closed):
        assert(await client.post('/operation',json={'op':'migrate'})).json()['schema']==42
        assert(await client.post('/operation',json={'op':'save'})).json()['saved_schema']==42
        searching=asyncio.create_task(client.post('/operation',json={'op':'search'}))
        await asyncio.wait_for(search_received.wait(),2)
        server.should_exit=True
        try:
            await asyncio.sleep(.2)
            closed_before_response=closed.is_set()
        finally:release_search.set()
        response=await searching
        assert not closed_before_response, 'shutdown closed the worker needed by the accepted HTTP read'
        assert response.status_code==200 and response.json()['status']=='ok'
        await asyncio.wait_for(serving,3)
    async with api()as(_,_,client,_):
        response=await client.post('/operation',json={'op':'page','page':15})
        assert response.status_code==200
        assert response.json()=={'status':'ok','schema':42,'saved_schema':42,'pages':[15]}
