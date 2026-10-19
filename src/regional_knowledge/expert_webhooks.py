"""Standard Webhooks protocol, fixed loopback peers or DNS-pinned external TLS.

The wire/verification contract follows Projects Hub; no new relay service or
human OAuth identity is introduced. Secrets and callback URLs are never logged.
"""
import asyncio
import base64
import hashlib
import hmac
import http.client
import ipaddress
import json
import secrets
import socket
import ssl
import time
from urllib.parse import urlsplit


class WebhookError(Exception):pass


def secret_bytes(value):
    try:
        if not isinstance(value,str) or not value.startswith("whsec_"):raise ValueError()
        key=base64.b64decode(value[6:],validate=True)
        if not 24<=len(key)<=64:raise ValueError()
        return key
    except (TypeError,ValueError):raise WebhookError("invalid_webhook_secret") from None


def signed_headers(subscription_id,event_id,body,secret,*,old_secret=None,timestamp=None):
    stamp=str(int(time.time() if timestamp is None else timestamp))
    signed=event_id.encode()+b"."+stamp.encode()+b"."+body
    signatures=["v1,"+base64.b64encode(hmac.new(secret_bytes(key),signed,hashlib.sha256).digest()).decode()
                for key in (secret,old_secret) if key is not None]
    return {"Content-Type":"application/json","webhook-id":event_id,"webhook-timestamp":stamp,
            "webhook-signature":" ".join(signatures),"X-MCP-Subscription-Id":subscription_id}


def callback_parts(url,local_callbacks=()):
    try:
        p=urlsplit(url)
        if not isinstance(url,str) or len(url)>2048 or any(ord(c)<33 or ord(c)>126 for c in url) or p.username or p.password or p.fragment:raise ValueError()
        if p.scheme=="http" and p.hostname=="127.0.0.1" and p.port and url in local_callbacks:return p,True
        if p.scheme!="https" or not p.hostname or p.port not in (None,443):raise ValueError()
        return p,False
    except (TypeError,ValueError):raise WebhookError("invalid_callback_url") from None


class PinnedHTTPS(http.client.HTTPSConnection):
    def connect(self):
        addresses=list(dict.fromkeys(r[4][0] for r in socket.getaddrinfo(self.host,443,type=socket.SOCK_STREAM)))
        if not addresses:raise WebhookError("callback_dns_unavailable")
        for address in addresses:
            ip=ipaddress.ip_address(address)
            if not ip.is_global or ip.is_multicast or getattr(ip,"ipv4_mapped",None) or getattr(ip,"sixtofour",None) or getattr(ip,"teredo",None):
                raise WebhookError("callback_address_denied")
        raw=socket.create_connection((addresses[0],443),timeout=self.timeout)
        try:self.sock=self._context.wrap_socket(raw,server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


class WebhookSender:
    def __init__(self,*,local_callbacks=()):self.local_callbacks=frozenset(local_callbacks)

    async def send(self,url,subscription_id,event_id,payload,secret,old_secret=None):
        body=json.dumps(payload,ensure_ascii=False,separators=(",",":")).encode()
        if len(body)>262144:raise WebhookError("event_too_large")
        headers=signed_headers(subscription_id,event_id,body,secret,old_secret=old_secret)
        def post():
            parts,local=callback_parts(url,self.local_callbacks)
            connection=(http.client.HTTPConnection("127.0.0.1",parts.port,timeout=10) if local else
                        PinnedHTTPS(parts.hostname,timeout=10,context=ssl.create_default_context()))
            try:
                path=parts.path or "/"
                if parts.query:path+="?"+parts.query
                connection.request("POST",path,body=body,headers=headers)
                response=connection.getresponse();data=response.read(65537)
                if len(data)>65536:raise WebhookError("callback_response_too_large")
                return response.status,data,response.getheader("Retry-After")
            finally:connection.close()
        try:return await asyncio.wait_for(asyncio.to_thread(post),12)
        except (OSError,TimeoutError,http.client.HTTPException):raise WebhookError("callback_unavailable") from None

    async def verify(self,url,subscription_id,secret):
        challenge=secrets.token_urlsafe(32)
        status,body,_=await self.send(url,subscription_id,"verification_"+secrets.token_hex(16),{"type":"verification","challenge":challenge},secret)
        try:
            returned=json.loads(body).get("challenge")
            if not 200<=status<300 or not isinstance(returned,str) or not secrets.compare_digest(returned,challenge):raise ValueError()
        except (ValueError,AttributeError):raise WebhookError("callback_verification_failed") from None
