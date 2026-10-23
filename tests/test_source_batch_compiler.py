"""Packets compile independently with exact sparse original-page identity."""
import copy
import unittest

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler, SourceMaterializationRejected
from regional_knowledge.source_stage import canonical, digest
from test_native_source_materializer import exact_batch, SOURCE, PERSON
from test_source_stage import original


class BatchCompilerTests(unittest.IsolatedAsyncioTestCase):
    def compiler(self):
        self.inputs = []
        async def count(text):
            self.inputs.append(text)
            return len(text.split()) + 2
        return NativeSourceCompiler(count=count, current=lambda: True)

    async def compile(self, documents, index):
        raws = list(map(canonical, documents))
        return await self.compiler().compile_batch(raws, batch_index=index, source_id=SOURCE,
            person_id=PERSON, last_batch_sha256=digest(raws[-1]))

    async def test_last_physical_page_does_not_require_other_173_pages(self):
        doc = exact_batch(start=173, count=1)
        result, media = await self.compile([doc], 0)
        self.assertEqual(len(result['passages']), 1)
        self.assertEqual(result['image_manifest']['pages'][0]['physical_page_index'], 173)
        self.assertEqual(len(self.inputs), 1)
        self.assertEqual(media['illustrations'], [])

    async def test_unrelated_bad_packet_does_not_block_good_packet(self):
        first = exact_batch(count=1)
        first['model_payload']['pages'][0]['regions'][0]['source_text'] = ''
        second = exact_batch(1, digest(canonical(first)), start=8, count=1)
        result, _ = await self.compile([first, second], 1)
        self.assertEqual(len(result['passages']), 1)
        self.assertEqual(result['image_manifest']['pages'][0]['physical_page_index'], 8)
        with self.assertRaises(SourceMaterializationRejected):
            await self.compile([first, second], 0)

    async def test_explicit_later_correction_is_used_without_rewriting_accepted_bytes(self):
        first = exact_batch(count=1)
        first['model_payload']['chunks'][0]['span_refs'] = []
        second = copy.deepcopy(first)
        second.update(batch_index=1, previous_batch_sha256=digest(canonical(first)))
        second['model_payload']['pages'][0]['regions'][0]['source_text'] = 'Corrected explicit model source.'
        frozen = canonical(first)
        result, _ = await self.compile([first, second], 0)
        self.assertEqual(result['passages'][0]['body'], 'Corrected explicit model source.')
        self.assertEqual(canonical(first), frozen)

    async def test_story_can_reference_prior_accepted_packet(self):
        first = exact_batch(count=1)
        second = exact_batch(1, digest(canonical(first)), start=8, count=1)
        p = first['model_payload']['pages'][0]
        second['model_payload']['story_candidates'] = [dict(candidate_key='later-fact', title='Later finding',
            summary='An explicit model-authored finding.', proposition='A source-supported finding.',
            narrative_outcome='facts_only', evidence_refs=[dict(page_id=p['page_id'], region_key='body',
                original_excerpt=p['regions'][0]['source_text'])])]
        result, _ = await self.compile([first, second], 1)
        self.assertEqual(len(result['passages']), 2)
        self.assertEqual(len(result['semantic']['findings']), 1)
        self.assertEqual(len(result['image_manifest']['pages']), 2)

    async def test_original_chain_forgery_still_rejected(self):
        first = exact_batch(count=1)
        second = exact_batch(1, 'e'*64, start=8, count=1)
        with self.assertRaises(SourceMaterializationRejected):
            await self.compile([first, second], 1)
