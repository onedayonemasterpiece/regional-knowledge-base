"""Loopback client for the pinned local BGE-M3 INT8 query encoder."""
import asyncio
import contextvars
from urllib.parse import urlparse
import httpx
from .bge_contract import SPACE,validate_vector

query_timings=contextvars.ContextVar("bge_query_timings",default={})

class LocalBGEOverloaded(RuntimeError):
    """Explicit bounded sidecar overload/deadline signal; not model corruption."""


class LocalBGEQueryEmbedder:
    embedding_space=SPACE
    encoder_revision="onnx-community/bge-m3-ONNX:int8:2237f770"
    def __init__(self,endpoint="http://127.0.0.1:8768"):
        parsed=urlparse(endpoint)
        if parsed.scheme!="http" or parsed.hostname!="127.0.0.1" or parsed.port!=8768 or parsed.username or parsed.path not in ("","/"):
            raise ValueError("BGE query endpoint must be the fixed loopback service")
        self.client=httpx.AsyncClient(base_url=endpoint,timeout=httpx.Timeout(1.65,connect=.35),trust_env=False,follow_redirects=False)

    async def embed(self,text):
        query_timings.set({})
        # CPU contention on the shared host can delay loopback connection
        # scheduling. Allow some connect headroom, while capping the *whole*
        # HTTP operation within the product's interaction budget.
        async with asyncio.timeout(1.7):
            response=await self.client.post("/embed",json={"space":SPACE,"texts":[text]})
        if response.status_code==429:
            raise LocalBGEOverloaded("local BGE queue cannot meet the interaction deadline")
        response.raise_for_status()
        data=response.json()
        if data.get("space")!=SPACE or len(data.get("vectors",[]))!=1:
            raise ValueError("local BGE query space/batch mismatch")
        vector=validate_vector(data["vectors"][0])
        query_timings.set({
            "bge_queue_seconds":float(data.get("queue_wait_seconds",0)),
            "bge_encoder_seconds":float(data.get("encoder_seconds",0)),
            "bge_query_local":1.0,
            "bge_query_cache_hit":float(bool(data.get("cache_hit",False))),
        })
        return vector

    async def status(self):
        try:
            response=await self.client.get("/health",timeout=.25)
            response.raise_for_status()
            data=response.json()
            ready=bool(data.get("ready") and data.get("space")==SPACE and data.get("dimension")==1024)
            return {"configured":True,"ready":ready,"space":SPACE,"encoder_revision":data.get("encoder_revision"),"queue_depth":data.get("queue_depth"),"busy":data.get("busy")}
        except (httpx.HTTPError,ValueError):
            return {"configured":True,"ready":False,"space":SPACE}

    async def aclose(self):
        await self.client.aclose()
