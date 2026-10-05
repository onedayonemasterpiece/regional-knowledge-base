"""MCP-side local client: no model imports and no external provider fallback."""
import contextvars
import json
import httpx
from urllib.parse import urlparse
from .e5_contract import SPACE,MAX_TOKENS,TARGET_PASSAGE_TOKENS,validate_vector
from .bge_contract import SPACE as BGE_SPACE,MAX_TOKENS as BGE_MAX_TOKENS

encoding_timings = contextvars.ContextVar('e5_encoding_timings',default={})

class LocalE5Embedder:
    embedding_space = SPACE
    def __init__(self, endpoint='http://127.0.0.1:8767'):
        parsed = urlparse(endpoint)
        if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.port!=8767 or parsed.username or parsed.path not in ('','/'):
            raise ValueError('E5 endpoint must be the fixed loopback service')
        self.client = httpx.AsyncClient(base_url=endpoint,timeout=httpx.Timeout(3.2,connect=.15),trust_env=False,follow_redirects=False)

    async def embed(self,text):
        encoding_timings.set({})
        response = await self.client.post('/embed',json={'space':SPACE,'role':'query','texts':[text]})
        response.raise_for_status()
        data = response.json()
        if data.get('space')!=SPACE or len(data.get('vectors',[]))!=1:raise ValueError('local E5 space/batch mismatch')
        vector = validate_vector(data['vectors'][0])
        encoding_timings.set({k:float(data[k]) for k in ('queue_wait_seconds','encoder_seconds')})
        return vector

    async def status(self):
        try:
            r = await self.client.get('/health',timeout=.3);r.raise_for_status();data=r.json()
            data['ready']=bool(data.get('ready') and data.get('space')==SPACE)
            return {'configured':True,**{k:data.get(k) for k in ('ready','space','token_budget_ready','queue_depth','busy','queue_capacity')},'retrieval_mode':'fast_e5' if data.get('ready') and data.get('space')==SPACE else 'lexical_only'}
        except (httpx.HTTPError,ValueError):return {'configured':True,'ready':False,'space':SPACE,'token_budget_ready':False,'retrieval_mode':'lexical_only'}

    async def embed_passages(self,texts):
        if not 1<=len(texts)<=4:raise ValueError('E5 passage batch1..4 required')
        response=await self.client.post('/embed',json={'space':SPACE,'role':'passage','texts':texts})
        response.raise_for_status();data=response.json()
        if data.get('space')!=SPACE or len(data.get('vectors',[]))!=len(texts):raise ValueError('local E5 passage contract')
        return [validate_vector(v) for v in data['vectors']]

    async def passage_token_counts(self,texts):
        if not texts:return []
        output=[];batch=[]
        async def send(values):
            raw=json.dumps({'space':SPACE,'role':'passage','texts':values},ensure_ascii=False,separators=(',',':')).encode()
            if len(raw)>240000:raise ValueError('token count request too large')
            response=await self.client.post('/token-count',content=raw,headers={'Content-Type':'application/json'})
            response.raise_for_status();data=response.json()
            if data.get('e5_space')!=SPACE or data.get('bge_space')!=BGE_SPACE:raise ValueError('token count space mismatch')
            if data.get('e5_max_tokens')!=MAX_TOKENS or data.get('bge_max_tokens')!=BGE_MAX_TOKENS or data.get('target_passage_tokens')!=TARGET_PASSAGE_TOKENS:raise ValueError('token count budget mismatch')
            e5=data.get('e5_tokens');bge=data.get('bge_tokens')
            if not isinstance(e5,list) or not isinstance(bge,list) or len(e5)!=len(values) or len(bge)!=len(values):raise ValueError('token count batch mismatch')
            return [{'e5':int(a),'bge':int(b)} for a,b in zip(e5,bge,strict=True)]
        for text in texts:
            candidate=[*batch,text]
            raw=json.dumps({'space':SPACE,'role':'passage','texts':candidate},ensure_ascii=False,separators=(',',':')).encode()
            if batch and (len(candidate)>32 or len(raw)>240000):
                output.extend(await send(batch));batch=[text]
            else:
                batch=candidate
        if batch:output.extend(await send(batch))
        return output

    async def aclose(self):await self.client.aclose()
