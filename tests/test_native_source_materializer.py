"""Synthetic 174-page model/image batches; no real source, provider or weights."""
import copy
import json
import unittest
from uuid import UUID,uuid5

from regional_knowledge.source_stage import canonical,digest,ZERO_HASH
from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler,SourceMaterializationRejected
from regional_knowledge.source_activation import knowledge_bytes
from test_source_stage import batch,original,SOURCE,INGESTION

PERSON='00000000-0000-0000-0000-000000000099'


def exact_batch(index=0,previous=ZERO_HASH,*,start=0,count=8,receipt=None):
    value=batch(index,previous,start=start,count=count,receipt=receipt)
    for page in value['model_payload']['pages']:
        for region in page['regions']:
            region['bbox']=dict(left=10,top=20,right=900,bottom=100)
    return value


class NativeCompilerTests(unittest.IsolatedAsyncioTestCase):
    def compiler(self,*,maximum=512,current=lambda:True):
        self.counted=[]
        async def count(text):
            self.counted.append(text)
            return min(maximum,len(text.split())+2)
        return NativeSourceCompiler(count=count,current=current)

    async def test_174_page_compilation_keeps_native_model_text_and_no_vectors(self):
        raws=[];head=ZERO_HASH
        for index,offset in enumerate(range(0,174,8)):
            raw=canonical(exact_batch(index,head,start=offset))
            raws.append(raw);head=digest(raw)
        compiler=self.compiler()
        resolved=await compiler.compile(raws,source_id=SOURCE,person_id=PERSON,last_batch_sha256=head)
        self.assertEqual(len(resolved['passages']),174)
        self.assertEqual(len(resolved['image_manifest']['pages']),174)
        self.assertEqual(len(self.counted),174)
        self.assertTrue(all(p['input_tokens']<=512 and 'embedding' not in p for p in resolved['passages']))
        self.assertTrue(all(r['source_text_provenance']=='model_transcription' for r in resolved['image_manifest']['regions']))
        self.assertEqual(resolved['image_manifest']['pages'][-1]['physical_page_index'],173)
        self.assertEqual(knowledge_bytes(resolved),canonical(resolved))
        self.assertEqual(resolved['research']['state'],'pending')
        self.assertNotIn('native_blocks',json.dumps(resolved))

    async def test_exact_repeated_page_adds_chunks_without_deleting_prior(self):
        receipt=original(page_count=1)
        first=exact_batch(count=1,receipt=receipt);raw1=canonical(first)
        second=exact_batch(1,digest(raw1),count=1,receipt=receipt)
        second['model_payload']['chunks'][0]['chunk_key']='additional-window'
        raw2=canonical(second)
        value=await self.compiler().compile([raw1,raw2],source_id=SOURCE,person_id=PERSON,last_batch_sha256=digest(raw2))
        self.assertEqual(len(value['passages']),2)

    async def test_explicit_later_transcription_replaces_dependent_old_chunks(self):
        receipt=original(page_count=1)
        first=exact_batch(count=1,receipt=receipt);raw1=canonical(first)
        second=exact_batch(1,digest(raw1),count=1,receipt=receipt)
        second['model_payload']['pages'][0]['regions'][0]['source_text']='Corrected synthetic model transcription.'
        raw2=canonical(second)
        value=await self.compiler().compile([raw1,raw2],source_id=SOURCE,person_id=PERSON,last_batch_sha256=digest(raw2))
        self.assertEqual(len(value['passages']),1)
        self.assertEqual(value['passages'][0]['body'],'Corrected synthetic model transcription.')

    async def test_gap_or_changed_head_rejected_before_tokenization(self):
        raw=canonical(exact_batch(count=1))
        with self.assertRaises(SourceMaterializationRejected):
            await self.compiler().compile([raw],source_id=SOURCE,person_id=PERSON,last_batch_sha256='e'*64)
        self.assertEqual(self.counted,[])

    async def test_missing_pages_not_activated_by_partial_seal(self):
        raw=canonical(exact_batch(count=1))
        with self.assertRaises(SourceMaterializationRejected):
            await self.compiler().compile([raw],source_id=SOURCE,person_id=PERSON,last_batch_sha256=digest(raw))
        self.assertEqual(self.counted,[])

    async def test_513_complete_input_rejected_without_truncation(self):
        raw=canonical(exact_batch(count=1,receipt=original(page_count=1)))
        async def count(text):return 513
        with self.assertRaisesRegex(SourceMaterializationRejected,'512_tokens'):
            await NativeSourceCompiler(count=count,current=lambda:True).compile([raw],source_id=SOURCE,
                person_id=PERSON,last_batch_sha256=digest(raw))

    async def test_current_owner_rechecked_after_tokenizer_await(self):
        raw=canonical(exact_batch(count=1,receipt=original(page_count=1)))
        state={'current':True}
        async def count(text):state['current']=False;return 10
        with self.assertRaises(PermissionError):
            await NativeSourceCompiler(count=count,current=lambda:state['current']).compile([raw],source_id=SOURCE,
                person_id=PERSON,last_batch_sha256=digest(raw))

    async def test_whole_source_story_limit_is_not_twelve(self):
        receipt=original(page_count=16);raws=[];head=ZERO_HASH
        for index in range(16):
            data=exact_batch(index,head,start=index,count=1,receipt=receipt)
            page=data['model_payload']['pages'][0];text=page['regions'][0]['source_text']
            data['model_payload']['story_candidates']=[dict(candidate_key='finding-'+str(index),
                title='Synthetic finding',summary='Independent synthetic source evidence.',proposition=text,
                narrative_outcome='facts_only',evidence_refs=[dict(page_id=page['page_id'],region_key='body',original_excerpt=text)])]
            raw=canonical(data);raws.append(raw);head=digest(raw)
        value=await self.compiler().compile(raws,source_id=SOURCE,person_id=PERSON,last_batch_sha256=head)
        self.assertEqual(len(value['semantic']['findings']),16)
        self.assertEqual(value['semantic']['stories'],[])

    async def test_graph_over_legacy_32_bundle_limit_is_new_explicit_codec(self):
        receipt=original(page_count=33);raws=[];head=ZERO_HASH
        for index in range(33):
            data=exact_batch(index,head,start=index,count=1,receipt=receipt)
            page=data['model_payload']['pages'][0];text=page['regions'][0]['source_text']
            data['model_payload']['entity_candidates']=dict(entities=[dict(key='person-'+str(index),kind='person',
                canonical_label='Synthetic person '+str(index),exact_source_spelling='Synthetic',
                evidence=dict(chunk_id=str(uuid5(UUID(SOURCE),f'chunk:1:page-{index}')),
                    page_id=page['page_id'],region_id=str(uuid5(UUID(page['page_id']),'region:body')),exact_quote=text))])
            raw=canonical(data);raws.append(raw);head=digest(raw)
        value=await self.compiler().compile(raws,source_id=SOURCE,person_id=PERSON,last_batch_sha256=head)
        self.assertEqual(value['source_graph']['codec'],'rkb.source-graph.v2')
        self.assertEqual(len(value['source_graph']['bundles']),33)
        self.assertEqual(len(value['source_graph']['entities']),33)

    async def test_unknown_nested_model_material_is_not_silently_dropped(self):
        for target in ('page','region','chunk'):
            data=exact_batch(count=1,receipt=original(page_count=1))
            row=(data['model_payload']['pages'][0] if target=='page' else
                data['model_payload']['pages'][0]['regions'][0] if target=='region' else
                data['model_payload']['chunks'][0])
            row['unsupported_source_knowledge']='This must not disappear.'
            raw=canonical(data)
            with self.assertRaisesRegex(SourceMaterializationRejected,'unsupported_model_field'):
                await self.compiler().compile([raw],source_id=SOURCE,person_id=PERSON,last_batch_sha256=digest(raw))
            self.assertEqual(self.counted,[])

    async def test_nonempty_valid_illustration_is_rejected_not_silently_dropped(self):
        data=exact_batch(count=1,receipt=original(page_count=1))
        page=data['model_payload']['pages'][0]
        figure=copy.deepcopy(page['regions'][0])
        figure.update(region_key='photo',kind='figure',reading_order=1,source_text='')
        page['regions'].append(figure)
        page['illustrations']=[dict(illustration_key='photo',source_region_key='photo',
            kind='photo',visual_description='Synthetic model observation.',
            visual_description_provenance='model_observation',display_rotation_degrees=90)]
        raw=canonical(data)
        with self.assertRaisesRegex(SourceMaterializationRejected,'native_illustration_activation_unavailable'):
            await self.compiler().compile([raw],source_id=SOURCE,person_id=PERSON,last_batch_sha256=digest(raw))
        self.assertEqual(self.counted,[])
