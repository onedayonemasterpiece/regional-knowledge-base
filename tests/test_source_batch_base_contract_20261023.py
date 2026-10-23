"""The SQL base gate protects persistence, without revalidating optional JSON."""
import copy

import pytest

from regional_knowledge.source_stage import canonical
from test_kaggle_apply import PG
from test_source_activation_pg import image_pg_sql18, accepted, stage_command
from test_source_batch_compiler import passage_link_packet
from test_source_batch_pg import native, packet
from test_source_stage import SOURCE


@PG
@pytest.mark.asyncio
async def test_current_base_ignores_optional_fields_but_rejects_invalid_text(image_pg_sql18):
    from psycopg.types.json import Jsonb
    env = image_pg_sql18
    doc = passage_link_packet()
    assert (await accepted(env, stage_command(doc, 1))).status == 'applied'
    prepare, _ = native(env)
    await prepare(packet(env, doc))
    async with env['connection'].transaction():
        saved = (await (await env['connection'].execute(
            "SELECT knowledge FROM rkb_v4.source_batch_prepared WHERE component='text'"
        )).fetchone())[0]
        base = {k: v for k, v in saved.items()
                if k not in ('semantic', 'image_manifest', 'source_graph', 'public_rights')}
        base['codec'] = 'rkb.source-activation-base.v1'
        base['optional_review'] = {'story_candidates': [{'bad_evidence': True}]}
        base['passages'][0]['review_note'] = 'Opaque model annotation, not text.'
        async def checked(value):
            return (await (await env['connection'].execute(
                'SELECT rkb_v4.check_source_batch_base_v1(%s,%s,%s,%s)',
                (canonical(value), Jsonb({'source_id': SOURCE}),
                 Jsonb({'absent_source': 'true'}), '2026-10-23T00:00:00Z'))).fetchone())[0]
        assert await checked(base) is True
        for field, invalid in [('body', ''), ('body', None), ('body', ['not text']), ('passage_id', None),
                               ('ordinal', 4), ('input_tokens', 513), ('input_tokens', None)]:
            broken = copy.deepcopy(base)
            broken['passages'][0][field] = invalid
            assert await checked(broken) is False, field
        broken = copy.deepcopy(base)
        broken['model']['dimension'] = 768
        assert await checked(broken) is False
        broken = copy.deepcopy(base)
        broken['original']['receipt_sha256'] = 'invalid'
        assert await checked(broken) is False
        broken = copy.deepcopy(base)
        broken['passages'].append(copy.deepcopy(broken['passages'][0]))
        broken['passages'][1]['ordinal'] = 1
        assert await checked(broken) is False
