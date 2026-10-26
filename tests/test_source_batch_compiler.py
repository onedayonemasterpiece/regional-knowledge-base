"""Packets compile independently with exact sparse original-page identity."""
import copy
import unittest
from uuid import UUID, uuid5

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler, SourceMaterializationRejected
from regional_knowledge.source_stage import canonical, digest
from test_native_source_materializer import exact_batch, SOURCE, PERSON
from test_source_stage import original


def passage_link_packet():
    doc = exact_batch(count=1)
    page = doc['model_payload']['pages'][0]
    page['regions'][0]['source_text'] = 'Berlin 1930. Berlin 1933.'
    chunk = doc['model_payload']['chunks'][0]
    passage = str(uuid5(UUID(SOURCE), 'chunk:1:'+chunk['chunk_key']))
    region = str(uuid5(UUID(page['page_id']), 'region:body'))
    doc['model_payload']['entity_candidates'] = dict(entities=[dict(
        key='berlin', kind='poi_ref', canonical_label='Berlin', exact_source_spelling='Berlin',
        poi_locator=dict(names=['Berlin']), place_context='Synthetic source place; no coordinates.',
        evidence=dict(chunk_id=passage, page_id=page['page_id'], region_id=region))], relations=[])
    body = 'Синтетическая запись называет Берлин. В ней указаны 1930 и 1933 годы.'
    doc['model_payload']['story_candidates'] = [dict(candidate_key='berlin-years',
        title='Синтетические даты Берлина', summary='Две даты одного места.',
        proposition='В синтетическом источнике указаны Берлин и два года.', entity_keys=['berlin'],
        evidence_refs=[dict(page_id=page['page_id'], region_key='body', chunk_key=chunk['chunk_key'])],
        basic_variant=dict(body=body, language='ru', blocks=[dict(start=0,end=len(body),evidence_indexes=[0])]))]
    return doc


class BatchCompilerTests(unittest.IsolatedAsyncioTestCase):
    def test_legacy_evidence_defaults_preserve_accepted_wire_fields(self):
        from regional_knowledge.story_contracts import StageStoryEvidenceInput
        legacy=dict(page_id=exact_batch(count=1)['model_payload']['pages'][0]['page_id'],
            region_key='body',original_excerpt='Synthetic accepted quotation.',start=None,end=None)
        self.assertEqual(StageStoryEvidenceInput.model_validate(legacy).model_dump(mode='json'),legacy)

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

    async def test_page81_heading_prefixed_without_losing_page82_passage(self):
        doc = exact_batch(start=80, count=2)
        page = doc['model_payload']['pages'][0]
        body = page['regions'][0]
        body['reading_order'] = 1
        body['bbox'] = dict(left=10, top=110, right=900, bottom=900)
        page['regions'].insert(0, {**body, 'region_key': 'p81_heading',
            'reading_order': 0, 'kind': 'heading',
            'bbox': dict(left=10, top=20, right=900, bottom=100),
            'source_text': 'Первая мировая война'})

        with self.assertRaisesRegex(SourceMaterializationRejected, 'native_source_stage_requires_review'):
            await self.compile([doc], 0)
        self.assertEqual(self.inputs, [])

        chunk = doc['model_payload']['chunks'][0]
        chunk['region_refs'].insert(0, dict(page_id=page['page_id'], region_key='p81_heading'))
        result, _ = await self.compile([doc], 0)
        self.assertEqual(len(result['passages']), 2)
        self.assertTrue(result['passages'][0]['body'].startswith('Первая мировая война'))
        self.assertIn(body['source_text'], result['passages'][0]['body'])
        self.assertIn('page 81.', result['passages'][1]['body'])
        self.assertEqual([p['physical_page_index'] for p in result['image_manifest']['pages']], [80, 81])

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

    async def test_repeated_place_and_story_use_passage_links_without_copying_quotes(self):
        doc = passage_link_packet()
        page=doc['model_payload']['pages'][0]
        passage=str(uuid5(UUID(SOURCE),'chunk:1:'+doc['model_payload']['chunks'][0]['chunk_key']))
        region=str(uuid5(UUID(page['page_id']),'region:body'))
        body=doc['model_payload']['story_candidates'][0]['basic_variant']['body']
        frozen = canonical(doc)
        result, _ = await self.compile([doc], 0)
        self.assertEqual(canonical(doc), frozen)
        self.assertEqual(result['semantic']['stories'][0]['default_variant']['body'], body)
        self.assertEqual(result['semantic']['findings'][0]['evidence'],
            [dict(region_id=region, passage_id=passage)])
        self.assertNotIn(b'"original_excerpt"', canonical(result))
        self.assertNotIn(b'"exact_quote"', canonical(result))

    async def test_foreign_passage_link_rejects_enrichment_without_blocking_text(self):
        doc=passage_link_packet()
        doc['model_payload']['story_candidates'][0]['evidence_refs'][0]['chunk_key']='absent-passage'
        raws=[canonical(doc)]
        result,_=await self.compiler().compile_batch(raws,batch_index=0,source_id=SOURCE,
            person_id=PERSON,last_batch_sha256=digest(raws[0]),text_only=True)
        self.assertEqual(result['passages'][0]['body'],'Berlin 1930. Berlin 1933.')
        with self.assertRaisesRegex(SourceMaterializationRejected,'finding_requires_source_passage'):
            await self.compile([doc],0)

    async def test_printed_footnote_span_is_text_and_can_supply_fact_evidence(self):
        doc=exact_batch(count=1)
        p=doc['model_payload']['pages'][0]
        note=copy.deepcopy(p['regions'][0]);note.update(region_key='note',kind='footnote',
            reading_order=1,source_text='A printed synthetic footnote.')
        p['regions'].append(note)
        p['relations']=[dict(kind='footnote_of',source_region_key='note',target_region_key='body')]
        doc['model_payload']['chunks'].append(dict(chunk_key='printed-note',title='Printed note',span_refs=[
            dict(page_id=p['page_id'],region_key='note',start=0,end=len(note['source_text']),
                source_text_sha256=digest(note['source_text'].encode()))]))
        doc['model_payload']['story_candidates']=[dict(candidate_key='note-fact',title='Printed fact',
            summary='A printed note supplies an independent fact.',proposition='The synthetic source has a printed note.',
            narrative_outcome='facts_only',evidence_refs=[dict(page_id=p['page_id'],region_key='note',chunk_key='printed-note')])]
        result,_=await self.compile([doc],0)
        self.assertEqual(len(result['passages']),2)
        self.assertEqual(result['passages'][1]['body'],note['source_text'])
        self.assertEqual(len(result['semantic']['findings']),1)
