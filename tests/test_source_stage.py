"""Synthetic 174-page metadata; no real book bytes, provider or model call."""
import base64
import copy
import json
import unittest
from uuid import UUID, uuid5

from regional_knowledge.original_intake import VerifiedOriginalReceipt, issued_image_receipt
from regional_knowledge.contracts import StartMetadataInput
from regional_knowledge.source_stage import (
    BATCH_CODEC, SEAL_CODEC, ZERO_HASH, MAX_BATCH_BYTES, canonical, digest,
    encode_issued_image, validate_stage_payload, validate_seal_payload, load_stage_batch,
)

SOURCE = '00000000-0000-0000-0000-000000000002'
INGESTION = '00000000-0000-0000-0000-000000000003'
REQUEST = '00000000-0000-0000-0000-000000000004'
WORK = '00000000-0000-0000-0000-000000000005'
EDITION = '00000000-0000-0000-0000-000000000006'
FRONTIER = '00000000-0000-0000-0000-000000000007'


def original(*, page_count=174, format='djvu'):
    return VerifiedOriginalReceipt(source_id=SOURCE, ingestion_id=INGESTION, request_id=REQUEST,
        revision=1, sha256='a'*64, size=174000, key='original/synthetic.djvu', native_version='synthetic-v1',
        format=format, page_count=page_count, parser_sha256='b'*64, acquisition_sha256='c'*64,
        native_verification_sha256='d'*64)


def batch(index=0, previous=ZERO_HASH, *, start=0, count=8, receipt=None):
    receipt = receipt or original()
    pages = []; images = []; chunks = []
    for physical in range(start, min(start+count, receipt.page_count)):
        page_id = str(uuid5(UUID(SOURCE), f'page:1:{physical}'))
        image = issued_image_receipt(receipt, dict(page_id=page_id, physical_page_index=physical,
            original_sha256=receipt.sha256, parser_sha256=receipt.parser_sha256, width=595., height=842.,
            image_sha256=digest(str(physical).encode()), mime_type='image/jpeg',
            preview_render={'width':724,'height':1024,'preview_only':True}))
        pages.append(dict(page_id=page_id, physical_page_index=physical,
            source_image_receipt_sha256=image['source_image_receipt_sha256'], source_material='visual_reviewed',
            source_review_note='Synthetic model transcription of this exact issued image.',
            knowledge_reviewed=True, story_candidates_reviewed=True,
            regions=[dict(region_key='body', kind='body', reading_order=0,
                bbox=dict(left=100,top=100,right=900,bottom=900),
                source_text=f'Synthetic model-authored source text on page {physical}.')]))
        chunks.append(dict(chunk_key=f'page-{physical}',title='Synthetic chapter',
            region_refs=[dict(page_id=page_id, region_key='body')]))
        images.append(encode_issued_image(image))
    return dict(codec=BATCH_CODEC, ingestion_id=INGESTION, batch_index=index,
        previous_batch_sha256=previous,
        source=dict(bibliography='Synthetic 174-page scanned book metadata', work_id=WORK, edition_id=EDITION,
            metadata=StartMetadataInput(title='Synthetic scanned source',authors=['Synthetic author'],
                publication_year=1901,language='de',catalog={'kind':'book','publisher':'Synthetic publisher'}).model_dump(mode='json'),
            original_receipt_b64=base64.b64encode(canonical(receipt.wire())).decode(),
            public_rights=dict(basis='public_domain', note='Synthetic fixture declaration')),
        issued_images_b64=images,
        model_payload=dict(pages=pages, chunks=chunks, story_candidates=[]),
        research=dict(frontier_id=FRONTIER,state='pending',continuation=dict(tool='book_pages',
            arguments=dict(ingestion_id=INGESTION, cursor=str(start+len(pages))))))


def seal(count, head):
    return dict(codec=SEAL_CODEC, ingestion_id=INGESTION, batch_count=count, last_batch_sha256=head)


class StagePayloadTests(unittest.TestCase):
    def validate(self, data):
        return validate_stage_payload(canonical(data), source_id=SOURCE)

    def test_174_pages_are_twenty_two_bounded_batches_not_whole_book_native_proof(self):
        previous=ZERO_HASH; pages=[]; hashes=[]
        for index, start in enumerate(range(0,174,8)):
            data=batch(index,previous,start=start); raw=canonical(data)
            accepted=self.validate(data)
            self.assertEqual(accepted, data)
            self.assertLess(len(raw), MAX_BATCH_BYTES)
            pages.extend(p['physical_page_index'] for p in accepted['model_payload']['pages'])
            # Exact retry/resume preserves bytes and chain identity.
            self.assertEqual(canonical(self.validate(copy.deepcopy(data))),raw)
            previous=digest(raw); hashes.append(previous)
        self.assertEqual(pages,list(range(174)))
        self.assertEqual(len(hashes),22)
        self.assertEqual(validate_seal_payload(canonical(seal(22,previous)))['last_batch_sha256'],previous)

    def test_model_transcription_does_not_require_native_text(self):
        data=batch(count=1)
        data['model_payload']['pages'][0]['regions'][0]['source_text']='A model transcription, not parser text.'
        self.assertEqual(self.validate(data),data)

    def test_full_story_and_semantic_payload_are_retained_without_lowering(self):
        data=batch(count=1)
        data['model_payload']['story_candidates']=[dict(candidate_key='story-1',
            proposition='Synthetic attributed story', evidence_refs=[dict(page_id=data['model_payload']['pages'][0]['page_id'],
            region_key='body')], narrative={'body':'A supplied story, not generated here.','attribution':'Synthetic witness'})]
        data['model_payload']['entity_candidates']={'entities':[{'key':'synthetic-author'}],'relations':[]}
        self.assertEqual(self.validate(data),data)

    def test_partial_candidate_seal_does_not_assert_complete_coverage(self):
        data=batch(count=1,start=100); self.validate(data)
        self.assertEqual(validate_seal_payload(canonical(seal(1,digest(canonical(data)))))['batch_count'],1)

    def test_pdf_and_djvu_receipts_remain_typed(self):
        for format in ('pdf','djvu'):
            with self.subTest(format=format): self.validate(batch(receipt=original(format=format)))

    def test_ninth_page_rejected(self):
        with self.assertRaises(ValueError): self.validate(batch(count=9))

    def test_payload_bytes_are_per_batch_bounded(self):
        data=batch(count=1); data['model_payload']['pages'][0]['regions'][0]['source_text']='x'*MAX_BATCH_BYTES
        with self.assertRaises(ValueError): self.validate(data)

    def test_unknown_model_field_is_not_silently_dropped(self):
        data=batch(); data['model_payload']['unknown_semantic_section']={'retained':False}
        with self.assertRaises(ValueError): self.validate(data)

    def test_derived_vectors_never_cross_stage_contract(self):
        for key in ('embedding','embeddings','input_tokens'):
            data=batch(); data['model_payload']['pages'][0]['regions'][0][key]=[1.0]
            with self.subTest(key=key), self.assertRaises(ValueError): self.validate(data)

    def test_exact_issued_image_binding_rejects_drift(self):
        for key,value in [('page_id',REQUEST),('physical_page_index',1),
                          ('source_image_receipt_sha256','e'*64),('source_material','unreviewed')]:
            data=batch(count=1); data['model_payload']['pages'][0][key]=value
            with self.subTest(key=key), self.assertRaises(ValueError): self.validate(data)

    def test_missing_review_note_rejected(self):
        data=batch(); del data['model_payload']['pages'][0]['source_review_note']
        with self.assertRaises(ValueError): self.validate(data)

    def test_duplicate_page_rejected(self):
        data=batch(count=1); data['model_payload']['pages']*=2; data['issued_images_b64']*=2
        with self.assertRaises(ValueError): self.validate(data)

    def test_wrong_ingestion_cannot_rebind_original(self):
        data=batch(); data['ingestion_id']=REQUEST
        with self.assertRaises(ValueError): self.validate(data)

    def test_batch_predecessor_is_explicit(self):
        for index,head in [(0,'1'*64),(1,ZERO_HASH),(True,ZERO_HASH)]:
            data=batch(); data.update(batch_index=index,previous_batch_sha256=head)
            with self.subTest(index=index), self.assertRaises(ValueError): self.validate(data)

    def test_unknown_codec_rejected(self):
        data=batch(); data['codec']='rkb.source-import.v4'
        with self.assertRaises(ValueError): self.validate(data)

    def test_noncanonical_bytes_rejected(self):
        with self.assertRaises(ValueError): validate_stage_payload(json.dumps(batch()).encode(),source_id=SOURCE)

    def test_bad_research_does_not_disappear(self):
        data=batch(); data['research']['continuation']={}
        with self.assertRaises(ValueError): self.validate(data)

    def test_image_encoder_does_not_repair_forged_hash(self):
        with self.assertRaises(ValueError): encode_issued_image({'source_image_receipt_sha256':'f'*64,'codec':'forged'})

    def test_empty_seal_rejected(self):
        with self.assertRaises(ValueError): validate_seal_payload(canonical(seal(0,ZERO_HASH)))


class StageReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_exact_native_reader_preserves_payload(self):
        raw=canonical(batch()); statements=[]
        class Connection:
            async def execute(self,sql,args):
                statements.append((sql,args)); return self
            async def fetchone(self): return raw,digest(raw)
        self.assertEqual(await load_stage_batch(Connection(),source_id=SOURCE,ingestion_id=INGESTION,batch_index=0),raw)
        self.assertEqual(statements[0][1],(SOURCE,INGESTION,0))
        self.assertIn('op.sequence=b.accepted_sequence',statements[0][0])

    async def test_native_reader_rejects_persisted_hash_drift(self):
        class Connection:
            async def execute(self,*args): return self
            async def fetchone(self): return canonical(batch()),'0'*64
        with self.assertRaises(ValueError):
            await load_stage_batch(Connection(),source_id=SOURCE,ingestion_id=INGESTION,batch_index=0)

    async def test_native_reader_returns_absence_not_invented_stage(self):
        class Connection:
            async def execute(self,*args): return self
            async def fetchone(self): return None
        self.assertIsNone(await load_stage_batch(Connection(),source_id=SOURCE,ingestion_id=INGESTION,batch_index=0))


if __name__=='__main__': unittest.main()
