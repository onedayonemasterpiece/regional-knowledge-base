"""Regression cases for accumulated graph evidence, using synthetic pages only.

No provider, original bytes, database or encoder is used. The expected projection
keeps one canonical entity and every distinct source mention. Explicit corrected
pages replace their dependent projected evidence, not accepted batch bytes.
"""
import copy
import json
import unittest
from uuid import UUID, uuid5

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler
from regional_knowledge.source_stage import canonical, digest, ZERO_HASH
from test_native_source_materializer import exact_batch, PERSON
from test_source_stage import SOURCE, original


def with_entity(data, *, key='shared-person'):
    page = data['model_payload']['pages'][0]
    text = page['regions'][0]['source_text']
    chunk_key = data['model_payload']['chunks'][0]['chunk_key']
    data['model_payload']['entity_candidates'] = {
        'entities': [{
            'key': key,
            'kind': 'person',
            'canonical_label': 'Synthetic person',
            'exact_source_spelling': 'Synthetic',
            'evidence': {
                'chunk_id': str(uuid5(UUID(SOURCE), 'chunk:1:' + chunk_key)),
                'page_id': page['page_id'],
                'region_id': str(uuid5(UUID(page['page_id']), 'region:body')),
                'exact_quote': text,
            },
        }],
    }
    return data


def all_mentions(graph):
    """Allow a new explicit collection without blessing first-mention loss."""
    if 'mentions' in graph:
        return graph['mentions']
    return [mention for entity in graph['entities']
            for mention in entity.get('mentions', [entity['mention']] if 'mention' in entity else [])]


class NativeGraphContinuationTests(unittest.IsolatedAsyncioTestCase):
    async def compile(self, raws):
        async def count(text):
            return len(text.split()) + 2
        return await NativeSourceCompiler(count=count, current=lambda: True).compile(
            raws, source_id=SOURCE, person_id=PERSON, last_batch_sha256=digest(raws[-1]))

    async def test_same_explicit_entity_across_batches_retains_both_source_mentions(self):
        receipt = original(page_count=2)
        first = with_entity(exact_batch(0, ZERO_HASH, start=0, count=1, receipt=receipt))
        raw1 = canonical(first)
        second = with_entity(exact_batch(1, digest(raw1), start=1, count=1, receipt=receipt))
        raw2 = canonical(second)
        result = await self.compile([raw1, raw2])
        graph = result['source_graph']
        self.assertEqual(len(graph['entities']), 1)
        self.assertEqual(graph['entities'][0]['key'], 'shared-person')
        mentions = all_mentions(graph)
        self.assertEqual(len(mentions), 2)
        self.assertEqual(len({m['mention_id'] for m in mentions}), 2)
        self.assertEqual({m['evidence']['page_id'] for m in mentions}, {
            first['model_payload']['pages'][0]['page_id'],
            second['model_payload']['pages'][0]['page_id'],
        })
        self.assertEqual(len(graph['bundles']), 2)

    async def test_corrected_page_replaces_dependent_graph_projection_not_accepted_history(self):
        receipt = original(page_count=1)
        first = with_entity(exact_batch(count=1, receipt=receipt))
        raw1 = canonical(first)
        immutable_first = copy.deepcopy(first)
        second = exact_batch(1, digest(raw1), count=1, receipt=receipt)
        corrected = 'Synthetic corrected source testimony about the same person.'
        second['model_payload']['pages'][0]['regions'][0]['source_text'] = corrected
        second = with_entity(second)
        raw2 = canonical(second)
        result = await self.compile([raw1, raw2])
        graph = result['source_graph']
        self.assertEqual(len(graph['entities']), 1)
        mentions = all_mentions(graph)
        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0]['evidence']['exact_quote'], corrected)
        self.assertEqual(result['passages'][0]['body'], corrected)
        self.assertEqual(json.loads(raw1), immutable_first)
        self.assertNotEqual(raw1, raw2)

    async def test_same_normalized_alias_retains_both_distinct_source_proofs(self):
        receipt = original(page_count=2)
        chain = []
        for index in range(2):
            data = with_entity(exact_batch(index, digest(chain[-1]) if chain else ZERO_HASH,
                start=index, count=1, receipt=receipt))
            entity = data['model_payload']['entity_candidates']['entities'][0]
            entity['aliases'] = [dict(value='Synthetic', language='en', alias_type='spelling_variant',
                evidence=copy.deepcopy(entity['evidence']))]
            chain.append(canonical(data))
        graph = (await self.compile(chain))['source_graph']
        aliases = graph['entities'][0]['aliases']
        self.assertEqual(len(aliases), 1)
        self.assertEqual(len(aliases[0]['evidences']), 2)
        self.assertEqual(aliases[0]['evidence'], aliases[0]['evidences'][0])
        self.assertEqual(len({p['page_id'] for p in aliases[0]['evidences']}), 2)
