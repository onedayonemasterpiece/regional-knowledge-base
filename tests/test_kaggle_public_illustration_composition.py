"""Production composition through the actual MCP image issuance middleware."""
import base64
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace as N

import pytest

from regional_knowledge.kaggle_public_backend import current_binding
from regional_knowledge.kaggle_public_illustration_composition import configure_public_illustrations
from regional_knowledge.kaggle_public_illustration_native_read import READ_SQL_SHA256
from regional_knowledge.server import build_server
from test_kaggle_public_media_wiring import call_http, image_bytes
from test_kaggle_public_illustration_gate import snapshot
from test_kaggle_service_startup import service
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people


@pytest.mark.asyncio
async def test_configure_before_server_returns_real_pixels_and_physical_page(service,monkeypatch):
    s=service;s.adapter.available=True
    binding=current_binding(s.target.queue)
    sampled=snapshot()
    sampled=replace(sampled,database_generation=binding.database_generation,
        observed_data_seq=binding.min_data_seq,observed_projection_seq=binding.min_data_seq,
        native_reader_sha256=READ_SQL_SHA256,
        manifest_page=dict(sampled.manifest_page,physical_page_index=0))
    calls=[]
    async def public_illustration(source,figure,actor,current,*,pixels=False):
        assert source==sampled.source_id and figure==sampled.illustration_id
        assert current_binding(s.target.queue)==current
        calls.append(pixels)
        result=dict(snapshot=asdict(sampled))
        if pixels:result.update(image_b64=base64.b64encode(image_bytes()).decode(),mime_type='image/png')
        return result
    # Only native I/O is substituted. The production factory, authority, queue,
    # source fences, declared MCP tool and final ImageContent issuer are real.
    configure_public_illustrations(N(config=N(import_originals=N(public_illustration=public_illustration)),
        backend=s.startup.backend))
    monkeypatch.setattr('regional_knowledge.server._principal',lambda:s.target.principal)
    server=build_server(target_startup=s.startup)
    assert server._target_media_available
    async with s.startup.lifespan(None):
        data,wire=await call_http(N(startup=s.startup,server=server))
    result=data['result']
    assert result['isError'] is False
    assert result['content'][1]['type']=='image'
    assert base64.b64decode(result['content'][1]['data'])==image_bytes()
    metadata=json.loads(result['content'][0]['text'])['metadata']
    assert metadata['physical_page_index']==0
    assert metadata['page_id']==sampled.item['page_id']
    assert any(calls)
    for private in (b'crop_binding',b'archive_chat_id',b'message_id',b'image_b64'):
        assert private not in wire


def test_native_illustration_import_has_no_mcp_oauth_or_dev_corpus_dependencies():
    source='''
import importlib.abc,sys
class Guard(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name=='mcp' or name in ('regional_knowledge.read_authority','regional_knowledge.sqlite_corpus'):
   raise AssertionError('native imported Dev-only dependency: '+name)
sys.meta_path.insert(0,Guard())
from regional_knowledge.kaggle_illustration_native import NativeIllustrationService
from regional_knowledge.kaggle_original_native import NativeOriginalIntake
'''
    result=subprocess.run([sys.executable,'-c',source],cwd=Path(__file__).resolve().parents[1],
        env={**os.environ,'PYTHONPATH':'src'},capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
