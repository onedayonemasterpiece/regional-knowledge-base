"""Pinned local BGE-M3 INT8 query encoder sidecar."""
import os
for key in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","NUMEXPR_NUM_THREADS"):
    os.environ[key]="1"
os.environ["TOKENIZERS_PARALLELISM"]="false"
os.environ["CUDA_VISIBLE_DEVICES"]=""
os.environ["HF_HUB_OFFLINE"]="1"

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import time
from pathlib import Path
from .bge_contract import SPACE,MAX_TOKENS,validate_vector

log=logging.getLogger(__name__)
MODEL_SHA256="2237f770aad5c71bbc1fc2d361a57f9a37400574cc9eff32626f0cdb49234730"
TOKENIZER_SHA256="249df0778f236f6ece390de0de746838ef25b9d6954b68c2ee71249e0a9d8fd4"
CONFIG_SHA256="70dae5884ced999af00244f776ac9eaa71538d68497d3d6a6091e0318cd32905"
ENCODER_REVISION="onnx-community/bge-m3-ONNX:int8:2237f770"

class PinnedBGEQueryEncoder:
    def __init__(self,root):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer
        self.np=np
        root=Path(root)
        for relative,expected in (("onnx/model_int8.onnx",MODEL_SHA256),("tokenizer.json",TOKENIZER_SHA256),("config.json",CONFIG_SHA256)):
            path=root/relative
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
                raise ValueError("pinned BGE INT8 asset hash mismatch")
        options=ort.SessionOptions()
        options.intra_op_num_threads=1
        options.inter_op_num_threads=1
        options.execution_mode=ort.ExecutionMode.ORT_SEQUENTIAL
        options.add_session_config_entry("session.intra_op.allow_spinning","0")
        options.add_session_config_entry("session.inter_op.allow_spinning","0")
        self.session=ort.InferenceSession(str(root/"onnx/model_int8.onnx"),sess_options=options,providers=["CPUExecutionProvider"])
        self.tokenizer=Tokenizer.from_file(str(root/"tokenizer.json"))

    def encode(self,text):
        np=self.np
        item=self.tokenizer.encode(text)
        ids=item.ids[:MAX_TOKENS]
        mask=item.attention_mask[:MAX_TOKENS]
        hidden=self.session.run(["last_hidden_state"],{
            "input_ids":np.asarray([ids],dtype=np.int64),
            "attention_mask":np.asarray([mask],dtype=np.int64),
        })[0][:,0,:].astype(np.float32)
        hidden/=np.maximum(np.linalg.norm(hidden,axis=1,keepdims=True),1e-12)
        return validate_vector(hidden[0].tolist())

class QueryOverloaded(RuntimeError):
    """Bounded, retryable local encoder capacity or deadline failure."""


class QueryQueue:
    """Single-encoder, bounded sidecar with duplicate collapse and short-lived cache.

    All query keys are SHA-256 digests: retained cache and inflight tracking do
    not store plaintext user queries. Once the local interaction budget is
    exhausted, reject explicitly instead of silently holding a request.
    """

    def __init__(self,encode,capacity=16,deadline_seconds=1.45,cache_capacity=96):
        from collections import OrderedDict,deque
        if not 1<=capacity<=128 or not .2<=deadline_seconds<=1.6:
            raise ValueError("invalid local query limits")
        if not 0<=cache_capacity<=256:
            raise ValueError("invalid local query cache")
        self.encode=encode
        self.deadline_seconds=float(deadline_seconds)
        self.queue=asyncio.Queue(maxsize=capacity)
        self.executor=concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.worker=None
        self.busy=False
        self.completed=0
        self.rejected=0
        self.expired=0
        self.coalesced=0
        self.cache_hits=0
        self.max_queue=0
        self._inflight={}
        self._cache=OrderedDict()
        self._cache_capacity=cache_capacity
        self._encoder_samples=deque(maxlen=128)
        self._ewma_duration=.16

    def start(self):
        if self.worker is None or self.worker.done():
            self.worker=asyncio.create_task(self.run())

    def _key(self,text):
        return hashlib.sha256(text.encode("utf-8")).digest()

    def _cached(self,key,now):
        hit=self._cache.get(key)
        if not hit:return None
        when,vector,encoder_duration=hit
        if now-when>180:
            del self._cache[key]
            return None
        self._cache.move_to_end(key)
        self.cache_hits+=1
        return {
            "space":SPACE,"vectors":[vector],"queue_wait_seconds":0.0,
            "encoder_seconds":0.0,"encoder_revision":ENCODER_REVISION,
            "cache_hit":True,"original_encoder_seconds":encoder_duration,
        }

    def _cache_result(self,key,vector,elapsed):
        if not self._cache_capacity:return
        self._cache[key]=(time.monotonic(),vector,elapsed)
        self._cache.move_to_end(key)
        while len(self._cache)>self._cache_capacity:
            self._cache.popitem(last=False)

    async def submit(self,text):
        self.start()
        now=time.monotonic()
        key=self._key(text)
        hit=self._cached(key,now)
        if hit is not None:return hit
        future=self._inflight.get(key)
        if future is None:
            # The sidecar has one CPU-bound model worker. Reject requests
            # unlikely to meet the same interaction budget instead of
            # accepting a deep queue and later returning HTTP 503.
            waiting=self.queue.qsize()+(1 if self.busy else 0)
            if self.queue.full() or waiting>=4 or waiting*self._ewma_duration>=max(.05,self.deadline_seconds-.20):
                self.rejected+=1
                raise QueryOverloaded("bge_query_overloaded")
            future=asyncio.get_running_loop().create_future()
            future.add_done_callback(lambda f:f.exception() if not f.cancelled() else None)
            self._inflight[key]=future
            try:self.queue.put_nowait((key,text,now,future))
            except asyncio.QueueFull:
                self._inflight.pop(key,None)
                self.rejected+=1
                raise QueryOverloaded("bge_query_overloaded")
            self.max_queue=max(self.max_queue,self.queue.qsize())
        else:
            self.coalesced+=1
        try:
            return await asyncio.wait_for(asyncio.shield(future),self.deadline_seconds)
        except asyncio.TimeoutError as exc:
            self.expired+=1
            raise QueryOverloaded("bge_query_deadline") from exc

    async def run(self):
        while True:
            key,text,submitted,future=await self.queue.get()
            wait=time.monotonic()-submitted
            self.busy=True
            try:
                if wait>=self.deadline_seconds-.12:
                    self.expired+=1
                    raise QueryOverloaded("bge_query_queue_expired")
                started=time.monotonic()
                vector=await asyncio.get_running_loop().run_in_executor(
                    self.executor,self.encode,text
                )
                elapsed=time.monotonic()-started
                self._encoder_samples.append(elapsed)
                self._ewma_duration=max(.02,min(1.5,.75*self._ewma_duration+.25*elapsed))
                result={
                    "space":SPACE,"vectors":[vector],
                    "queue_wait_seconds":wait,"encoder_seconds":elapsed,
                    "encoder_revision":ENCODER_REVISION,
                    "cache_hit":False,
                }
                if not future.done():future.set_result(result)
                self._cache_result(key,vector,elapsed)
                self.completed+=1
            except Exception as exc:
                if not future.done():future.set_exception(exc)
                if not isinstance(exc,QueryOverloaded):
                    log.warning(json.dumps({
                        "event":"local_bge_query_failed",
                        "error_type":type(exc).__name__,
                    }))
            finally:
                self._inflight.pop(key,None)
                self.busy=False
                self.queue.task_done()

    def metrics(self):
        durations=sorted(self._encoder_samples)
        p95=durations[min(len(durations)-1,int(.95*(len(durations)-1)))] if durations else None
        return {
            "coalesced":self.coalesced,"cache_hits":self.cache_hits,
            "cache_size":len(self._cache),"expired":self.expired,
            "deadline_seconds":self.deadline_seconds,
            "encoder_p95_seconds":p95,
            "inflight_unique":len(self._inflight),
        }

class Service:
    def __init__(self,queue):
        self.queue=queue

    def status(self):
        return {
            "ready":True,
            "space":SPACE,
            "dimension":1024,
            "encoder_revision":ENCODER_REVISION,
            "queue_depth":self.queue.queue.qsize(),
            "queue_capacity":self.queue.queue.maxsize,
            "busy":self.queue.busy,
            "completed":self.queue.completed,
            "rejected":self.queue.rejected,
            "max_queue_depth":self.queue.max_queue,
            "pid":os.getpid(),
            **self.queue.metrics(),
        }

    async def handle(self,reader,writer):
        status=200
        try:
            headers=await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"),2)
            if len(headers)>8192:raise ValueError("headers_too_large")
            lines=headers.decode("ascii").split("\r\n")
            method,path,_=lines[0].split(" ")
            fields={k.lower():v.strip() for k,v in (line.split(":",1) for line in lines[1:] if ":" in line)}
            if "transfer-encoding" in fields:raise ValueError("unsupported_encoding")
            if method=="GET" and path=="/health":
                payload=self.status()
            elif method=="POST" and path=="/embed":
                size=int(fields.get("content-length","0"))
                if not 0<size<=65536:raise ValueError("request_size")
                data=json.loads(await asyncio.wait_for(reader.readexactly(size),2))
                texts=data.get("texts")
                if data.get("space")!=SPACE or not isinstance(texts,list) or len(texts)!=1:
                    raise ValueError("query_contract")
                text=texts[0]
                if not isinstance(text,str) or not text.strip() or len(text)>16000:
                    raise ValueError("text_size")
                payload=await self.queue.submit(text)
            else:
                status,payload=404,{"error":"not_found"}
        except QueryOverloaded:
            status,payload=429,{"error":"encoder_capacity_or_deadline"}
        except (TimeoutError,asyncio.TimeoutError):
            status,payload=429,{"error":"encoder_deadline"}
        except RuntimeError as exc:
            status,payload=503,{"error":"encoder_unavailable","error_type":type(exc).__name__}
        except (ValueError,KeyError,UnicodeError,asyncio.IncompleteReadError,asyncio.LimitOverrunError):
            status,payload=400,{"error":"invalid_request"}
        except Exception:
            log.exception("local_bge_query_request_failed")
            status,payload=503,{"error":"encoder_unavailable"}
        raw=json.dumps(payload).encode()
        writer.write(f"HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n".encode()+raw)
        try:await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

async def serve():
    logging.basicConfig(level=logging.INFO)
    encoder=PinnedBGEQueryEncoder(os.environ["RKB_BGE_QUERY_MODEL_DIR"])
    encoder.encode("readiness")
    queue=QueryQueue(encoder.encode)
    service=Service(queue)
    server=await asyncio.start_server(service.handle,"127.0.0.1",8768,limit=8192)
    log.info(json.dumps({"event":"local_bge_query_ready","space":SPACE,"encoder_revision":ENCODER_REVISION,"pid":os.getpid()}))
    async with server:
        await server.serve_forever()

if __name__=="__main__":
    asyncio.run(serve())
