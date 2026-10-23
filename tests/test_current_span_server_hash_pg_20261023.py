"""PG17 CI validates current packets that carry offsets without a model SHA."""
import pytest

from regional_knowledge.source_stage import digest
from test_current_span_server_hash_20261023 import packet as model_packet
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18,accepted,stage_command
from test_source_batch_pg import native,packet


@PG
@pytest.mark.asyncio
async def test_native_sql_publication_uses_server_span_hash(image_pg_sql18):
    env=image_pg_sql18;doc=model_packet()
    # Ordinary typed API serialization supplies the optional None default.
    doc['model_payload']['chunks'][0]['span_refs'][0]['source_text_sha256']=None
    assert (await accepted(env,stage_command(doc,env['sequence']+1))).status=='applied'
    prepare,_=native(env);command=packet(env,doc)
    await prepare(command)
    assert (await accepted(env,command)).status=='applied'
    async with env['connection'].transaction():
        knowledge=(await (await env['connection'].execute(
            "SELECT knowledge FROM rkb_v4.source_batch_prepared WHERE component='text'")).fetchone())[0]
        span=knowledge['image_manifest']['passages'][0]['source_spans'][0]
        text=doc['model_payload']['pages'][0]['regions'][0]['source_text']
        assert span['source_text_sha256']==digest(text.encode())
