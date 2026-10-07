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

class QueryQueue:
    def __init__(self,encode,capacity=16):
        self.encode=encode
        self.queue=asyncio.Queue(maxsize=capacity)
        self.executor=concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.worker=None
        self.busy=False
        self.completed=0
        self.rejected=0
        self.max_queue=0

    def start(self):
        if self.worker is None:self.worker=asyncio.create_task(self.run())

    async def submit(self,text):
        self.start()
        future=asyncio.get_running_loop().create_future()
        future.add_done_callback(lambda f:f.exception() if not f.cancelled() else None)
        try:self.queue.put_nowait((text,time.monotonic(),future))
        except asyncio.QueueFull:
            self.rejected+=1
            raise RuntimeError("bge_query_queue_full")
        self.max_queue=max(self.max_queue,self.queue.qsize())
        return await asyncio.wait_for(asyncio.shield(future),1.0)

    async def run(self):
        while True:
            text,submitted,future=await self.queue.get()
            wait=time.monotonic()-submitted
            self.busy=True
            try:
                if wait>=.85:raise TimeoutError("bge_query_queue_deadline")
                started=time.monotonic()
                vector=await asyncio.get_running_loop().run_in_executor(self.executor,self.encode,text)
                result={
                    "space":SPACE,
                    "vectors":[vector],
                    "queue_wait_seconds":wait,
                    "encoder_seconds":time.monotonic()-started,
                    "encoder_revision":ENCODER_REVISION,
                }
                if not future.done():future.set_result(result)
                self.completed+=1
            except Exception as exc:
                if not future.done():future.set_exception(exc)
                log.warning(json.dumps({"event":"local_bge_query_failed","error_type":type(exc).__name__}))
            finally:
                self.busy=False
                self.queue.task_done()

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
        except (TimeoutError,asyncio.TimeoutError):
            status,payload=503,{"error":"encoder_deadline"}
        except RuntimeError as exc:
            status,payload=(429 if "queue_full" in str(exc) else 503),{"error":str(exc)}
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
