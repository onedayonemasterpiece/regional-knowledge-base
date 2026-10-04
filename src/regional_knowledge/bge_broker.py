"""Narrow run-authenticated worker routes. Never exposes search/corpus credentials."""
import asyncio
import json
from starlette.responses import JSONResponse
from .bge_contract import SPACE,REVISION

def register_routes(mcp,queue):
    for action in ('heartbeat','claim','complete'):
        def endpoint(action):
            async def handler(request):
                try:
                    if int(request.headers.get('content-length','0'))>65536:
                        return JSONResponse({'error':'request_size'},status_code=413)
                    authorization=request.headers.get('authorization','')
                    if not authorization.startswith('Bearer '):raise PermissionError('worker credential required')
                    raw=bytearray()
                    async for chunk in request.stream():
                        raw.extend(chunk)
                        if len(raw)>65536:return JSONResponse({'error':'request_size'},status_code=413)
                    payload=json.loads(raw)
                    token=authorization[7:];run_id=payload['run_id']
                    if payload.get('space')!=SPACE:raise ValueError('BGE space mismatch')
                    if action=='heartbeat':
                        if payload.get('ready') is True and payload.get('model_revision')!=REVISION:raise ValueError('BGE revision mismatch')
                        result=await asyncio.to_thread(queue.heartbeat,run_id,token,ready=payload.get('ready') is True)
                    elif action=='claim':
                        result={'job':await asyncio.to_thread(queue.claim,run_id,token)}
                    else:
                        result={'status':await asyncio.to_thread(queue.complete,run_id,token,payload['job_id'],payload['claim'],payload['space'],payload['vectors'],payload.get('timings',{}))}
                    return JSONResponse(result,headers={'Cache-Control':'no-store'})
                except PermissionError:return JSONResponse({'error':'invalid_or_stale_worker'},status_code=403)
                except (ValueError,KeyError,TypeError):return JSONResponse({'error':'invalid_worker_payload'},status_code=400)
            return handler
        mcp.custom_route('/bge-worker/'+action,methods=['POST'])(endpoint(action))
