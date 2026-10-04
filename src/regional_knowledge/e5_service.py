"""One local pinned encoder. Standard-library HTTP, bounded FIFO, one inference thread."""
import os
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[key] = '1'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['CUDA_VISIBLE_DEVICES'] = ''
os.environ['HF_HUB_OFFLINE'] = '1'
import asyncio
import concurrent.futures
import hashlib
import json
import logging
import time
from pathlib import Path
from .e5_contract import SPACE, MODEL_SHA256, TOKENIZER_SHA256, prepare_text

log = logging.getLogger(__name__)

class PinnedEncoder:
    def __init__(self, root):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer
        self.np = np
        root = Path(root)
        for relative, expected in [('onnx/model_quantized.onnx',MODEL_SHA256),('tokenizer.json',TOKENIZER_SHA256)]:
            if hashlib.sha256((root/relative).read_bytes()).hexdigest() != expected:
                raise ValueError('pinned asset hash mismatch')
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = opts.inter_op_num_threads = 1
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        opts.add_session_config_entry('session.intra_op.allow_spinning','0')
        opts.add_session_config_entry('session.inter_op.allow_spinning','0')
        self.session = ort.InferenceSession(str(root/'onnx/model_quantized.onnx'),sess_options=opts,providers=['CPUExecutionProvider'])
        self.tokenizer = Tokenizer.from_file(str(root/'tokenizer.json'))
        self.tokenizer.enable_truncation(max_length=512)
        self.tokenizer.enable_padding(pad_id=1,pad_token='<pad>')

    def encode(self, texts, role):
        np = self.np
        encoded = self.tokenizer.encode_batch([prepare_text(role,t) for t in texts])
        data = {'input_ids':np.asarray([x.ids for x in encoded],dtype=np.int64),'attention_mask':np.asarray([x.attention_mask for x in encoded],dtype=np.int64),'token_type_ids':np.asarray([x.type_ids for x in encoded],dtype=np.int64)}
        names = [o.name for o in self.session.get_outputs()]
        outputs = self.session.run(None,{i.name:data[i.name] for i in self.session.get_inputs()})
        hidden = outputs[names.index('last_hidden_state')]
        mask = data['attention_mask'][...,None]
        vectors = np.asarray((hidden*mask).sum(axis=1)/np.maximum(mask.sum(axis=1),1),dtype=np.float32)
        vectors /= np.linalg.norm(vectors,axis=1,keepdims=True)
        assert vectors.shape == (len(texts),384) and np.isfinite(vectors).all()
        return vectors.tolist()

class QueueFull(Exception): pass

class SerialEncoder:
    def __init__(self, encode, capacity=10):
        self.encode = encode
        self.queue = asyncio.Queue(maxsize=capacity)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.busy = False
        self.max_queue = 0
        self.completed = self.rejected = 0
        self.worker = None

    def start(self):
        if self.worker is None:
            self.worker = asyncio.create_task(self.run())

    async def submit(self, texts, role):
        self.start()
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        try:self.queue.put_nowait((texts,role,time.monotonic(),future))
        except asyncio.QueueFull:
            self.rejected += 1
            raise QueueFull('encoder_queue_full')
        self.max_queue = max(self.max_queue,self.queue.qsize())
        return await asyncio.wait_for(asyncio.shield(future),3)

    async def run(self):
        while True:
            texts,role,submitted,future = await self.queue.get()
            wait = time.monotonic()-submitted
            self.busy = True
            try:
                if wait >= 2.5:
                    raise TimeoutError('encoder_queue_deadline')
                start = time.monotonic()
                vectors = await asyncio.get_running_loop().run_in_executor(self.executor,self.encode,texts,role)
                result = {'space':SPACE,'vectors':vectors,'queue_wait_seconds':wait,'encoder_seconds':time.monotonic()-start}
                if not future.done():future.set_result(result)
                self.completed += 1
                log.info(json.dumps({'event':'encoded','role':role,'count':len(texts),'queue_wait_seconds':wait,'encoder_seconds':result['encoder_seconds'],'space':SPACE}))
            except Exception as exc:
                if not future.done():future.set_exception(exc)
                log.warning(json.dumps({'event':'encoding_failed','error_type':type(exc).__name__,'space':SPACE}))
            finally:
                self.busy = False
                self.queue.task_done()

    async def close(self):
        if self.worker:self.worker.cancel()
        self.executor.shutdown(wait=True)

class LocalService:
    def __init__(self, encoder):self.encoder = encoder
    def status(self):
        return {'ready':True,'space':SPACE,'dimension':384,'queue_depth':self.encoder.queue.qsize(),'queue_capacity':self.encoder.queue.maxsize,'busy':self.encoder.busy,'max_queue_depth':self.encoder.max_queue,'completed':self.encoder.completed,'rejected':self.encoder.rejected,'encoder_processes':1,'pid':os.getpid(),'external_embedding_calls':0}

    async def handle(self, reader, writer):
        status = 200
        try:
            headers = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),2)
            if len(headers)>8192:raise ValueError('headers_too_large')
            lines = headers.decode('ascii').split('\r\n')
            method,path,_ = lines[0].split(' ')
            fields = dict(line.split(':',1) for line in lines[1:] if ':' in line)
            fields = {k.lower():v.strip() for k,v in fields.items()}
            if 'transfer-encoding' in fields:raise ValueError('unsupported_encoding')
            if method=='GET' and path=='/health':payload = self.status()
            elif method=='POST' and path=='/embed':
                size = int(fields.get('content-length','0'))
                if not 0<size<=65536:raise ValueError('request_size')
                data = json.loads(await asyncio.wait_for(reader.readexactly(size),2))
                texts = data.get('texts');role = data.get('role')
                if data.get('space')!=SPACE:raise ValueError('embedding_space_mismatch')
                if role not in ('query','passage') or not isinstance(texts,list) or not texts or len(texts)>(1 if role=='query' else 4):raise ValueError('role_batch_contract')
                if any(not isinstance(t,str) or not t.strip() or len(t)>(8000 if role=='query' else 40000) for t in texts):raise ValueError('text_size')
                payload = await self.encoder.submit(texts,role)
            else:status,payload = 404,{'error':'not_found'}
        except QueueFull:status,payload = 429,{'error':'encoder_queue_full'}
        except (TimeoutError,asyncio.TimeoutError):status,payload = 503,{'error':'encoder_deadline'}
        except (ValueError,KeyError,UnicodeError,asyncio.IncompleteReadError,asyncio.LimitOverrunError):status,payload = 400,{'error':'invalid_request'}
        except Exception:
            log.exception('request_failed');status,payload = 503,{'error':'encoder_unavailable'}
        raw = json.dumps(payload).encode()
        writer.write(f'HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n'.encode()+raw)
        try:await writer.drain()
        finally:writer.close();await writer.wait_closed()

async def main():
    logging.basicConfig(level=logging.INFO)
    encoder = PinnedEncoder(os.environ['RKB_E5_MODEL_DIR'])
    encoder.encode(['readiness'], 'query')
    queue = SerialEncoder(encoder.encode)
    service = LocalService(queue)
    server = await asyncio.start_server(service.handle,'127.0.0.1',8767,limit=8192)
    log.info(json.dumps({'event':'fast_e5_ready','space':SPACE,'pid':os.getpid()}))
    async with server:await server.serve_forever()

if __name__=='__main__':asyncio.run(main())
