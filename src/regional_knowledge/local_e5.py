"""MCP-side local client: no model imports and no external provider fallback."""
import contextvars
import httpx
from urllib.parse import urlparse
from .e5_contract import SPACE,validate_vector

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
            return {'configured':True,**{k:data.get(k) for k in ('ready','space','queue_depth','busy','queue_capacity')},'retrieval_mode':'fast_e5' if data.get('ready') and data.get('space')==SPACE else 'lexical_only'}
        except (httpx.HTTPError,ValueError):return {'configured':True,'ready':False,'space':SPACE,'retrieval_mode':'lexical_only'}

    async def aclose(self):await self.client.aclose()
