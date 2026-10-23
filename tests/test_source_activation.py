"""Image-origin contract fixtures: synthetic metadata, no original bytes or models."""
import base64
import copy
import json
import unittest
from uuid import UUID, uuid5

from regional_knowledge.source_activation import (
    ACTIVATION_CODEC, ACTIVATION_REVISIONS, IMAGE_MANIFEST_CODEC, MATERIALIZE_CODEC,
    PROJECTION_CODEC, knowledge_bytes, validate_image_manifest,
    validate_activation_payload, validate_materialize_payload,
    resolved_activation_command, resolved_materialize_command,
    validate_activation_command, validate_materialize_command,
)
from regional_knowledge.source_stage import canonical, digest, ZERO_HASH
from test_source_stage import SOURCE, INGESTION, REQUEST, batch, original

PERSON='60000000-0000-0000-0000-000000000002'
MATERIALIZER='e'*64


def synthetic_knowledge(page_count=174, *, story=False):
    """A deterministic test materializer, not a replacement for native compilation."""
    receipt=original(page_count=page_count); chain=[]; head=ZERO_HASH
    for index,start in enumerate(range(0,page_count,8)):
        d=batch(index,head,start=start,receipt=receipt)
        chain.append(d);head=digest(canonical(d))
    source=chain[0]['source']; pages=[];regions=[];passages=[];provenance=[]
    for d in chain:
        for page,encoded in zip(d['model_payload']['pages'],d['issued_images_b64'],strict=True):
            image=json.loads(base64.b64decode(encoded)); raw=page['regions'][0]
            pid=page['page_id']; rid=str(uuid5(UUID(pid),'region:body'))
            passage=str(uuid5(UUID(SOURCE),'test-passage:'+str(page['physical_page_index'])))
            text=raw['source_text']; h=digest(text.encode())
            pages.append(dict(page_id=pid,physical_page_index=page['physical_page_index'],printed_page_number=None,
                width=image['width'],height=image['height'],source_image_receipt_sha256=page['source_image_receipt_sha256'],
                issued_image_receipt_b64=encoded,image_sha256=image['image_sha256'],preview_render=image['preview_render'],
                source_material='visual_reviewed',source_review_note=page['source_review_note']))
            regions.append(dict(region_id=rid,page_id=pid,source_text=text,source_text_sha256=h))
            passages.append(dict(passage_id=passage,ordinal=len(passages),locator=f"page {len(passages)+1}",
                body=text,language='de',input_tokens=15))
            provenance.append(dict(passage_id=passage,region_ids=[rid],footnote_region_ids=[],page_ids=[pid],source_spans=[]))
    manifest=dict(codec=IMAGE_MANIFEST_CODEC,source_id=SOURCE,source_revision=1,
        original_receipt_b64=source['original_receipt_b64'],
        metadata={k:source['metadata'][k] for k in ('title','authors','publication_year','language','catalog')},
        pages=pages,regions=[dict(region_id=r['region_id'],region_key='body',page_id=r['page_id'],kind='body',
            bbox=dict(left=100,top=100,right=900,bottom=900),reading_order=0,column_id=None,
            text_sha256=r['source_text_sha256'],source_text_provenance='model_transcription') for r in regions],passages=provenance)
    semantic=dict(regions=regions,findings=[],stories=[],pending=[])
    if story:
        rid=regions[0]['region_id']; pid=passages[0]['passage_id']; text=regions[0]['source_text']
        finding=str(uuid5(UUID(SOURCE),'finding:test')); story_id=str(uuid5(UUID(SOURCE),'story:test'))
        variant=str(uuid5(UUID(story_id),'variant:1')); body='A distinct model-authored narrative about the synthetic source.'
        semantic['findings']=[dict(assertion_id=finding,candidate_key='test',proposition='The synthetic source contains a model transcription.',
            account_kind='other',attributed_to=None,reported_by=None,
            evidence=[dict(region_id=rid,passage_id=pid,start=0,end=len(text),original_excerpt=text)],
            event_dates=[],observations=[],entity_keys=[])]
        semantic['stories']=[dict(story_id=story_id,finding_ids=[finding],title='Synthetic story',
            summary='Distinct summary, not the actual body.',material_type='other',default_variant=dict(
            variant_id=variant,revision=1,audience='general',format='story',language='en',body=body,
            blocks=[dict(start=0,end=len(body),assertion_ids=[finding])],input_tokens=16))]
    doc=dict(codec=PROJECTION_CODEC,bibliography=source['bibliography'],revision=1,work_id=source['work_id'],
        edition_id=source['edition_id'],original=receipt.original,model=dict(
        revision='5617a9f61b028005a4858fdac845db406aefb181',space='bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1',
        tokenizer_sha256='21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08',dimension=1024,max_tokens=512),
        passages=passages,semantic=semantic,research=chain[-1]['research'],public_rights=source['public_rights'],image_manifest=manifest)
    return chain,head,doc


def activation_payload(chain,head):
    return dict(codec=ACTIVATION_CODEC,ingestion_id=INGESTION,batch_count=len(chain),
        last_batch_sha256=head,materializer_sha256=MATERIALIZER)


class ImageOriginTests(unittest.TestCase):
    def test_174_pages_have_exact_issued_image_proof_without_native_block_or_text_claim(self):
        chain,head,doc=synthetic_knowledge()
        self.assertEqual(len(chain),22)
        self.assertIs(validate_image_manifest(doc['image_manifest'],source_id=SOURCE,original=doc['original'],
            semantic=doc['semantic'],passages=doc['passages']),doc['image_manifest'])
        self.assertEqual(len(doc['passages']),174)
        self.assertNotIn('native_manifest',doc)
        self.assertTrue(all(r['source_text_provenance']=='model_transcription' for r in doc['image_manifest']['regions']))
        self.assertLess(len(canonical(activation_payload(chain,head))),2048)

    def test_incomplete_original_page_coverage_cannot_be_called_activated(self):
        _,_,doc=synthetic_knowledge();doc['image_manifest']['pages'].pop()
        with self.assertRaises(ValueError): validate_image_manifest(doc['image_manifest'],source_id=SOURCE,
            original=doc['original'],semantic=doc['semantic'],passages=doc['passages'])

    def test_forged_image_or_native_text_provenance_fails_closed(self):
        for field in ('image','provenance','body'):
            with self.subTest(field=field):
                _,_,doc=synthetic_knowledge(1)
                if field=='image':doc['image_manifest']['pages'][0]['source_image_receipt_sha256']='f'*64
                elif field=='provenance':doc['image_manifest']['regions'][0]['source_text_provenance']='native_text'
                else:doc['passages'][0]['body']='Invented text absent from the selected model transcription.'
                with self.assertRaises(ValueError):validate_image_manifest(doc['image_manifest'],source_id=SOURCE,
                    original=doc['original'],semantic=doc['semantic'],passages=doc['passages'])

    def test_only_embedding_float_drift_is_excluded_from_canonical_knowledge(self):
        _,_,doc=synthetic_knowledge(1,story=True); raw=knowledge_bytes(doc)
        with_vectors=copy.deepcopy(doc)
        with_vectors['passages'][0]['embedding']=[1.,0.]
        with_vectors['semantic']['stories'][0]['default_variant']['embedding']=[.999999,.00001]
        self.assertEqual(knowledge_bytes(with_vectors),raw)
        self.assertIn('embedding',with_vectors['passages'][0]) # no caller mutation
        for field in ('body','input_tokens'):
            changed=copy.deepcopy(doc);changed['passages'][0][field]='Different text' if field=='body' else 16
            self.assertNotEqual(knowledge_bytes(changed),raw)
        changed=copy.deepcopy(doc);changed['semantic']['stories'][0]['default_variant']['body']='Not the summary.'
        self.assertNotEqual(knowledge_bytes(changed),raw)

    def test_commands_are_small_versioned_chain_pins_not_domain_or_vector_roundtrips(self):
        chain,head,doc=synthetic_knowledge();p=activation_payload(chain,head)
        args=dict(sequence=1,operation_key='source-activate:test',request_id=REQUEST,person_id=PERSON,
            logical_time='2026-10-21T19:00:00Z',source_id=SOURCE)
        c=resolved_activation_command(**args,payload=p)
        self.assertEqual(validate_activation_command(c),p)
        m={**p,'codec':MATERIALIZE_CODEC,'ordinal':173}
        self.assertEqual(validate_materialize_command(resolved_materialize_command(**args,payload=m)),m)
        self.assertNotIn(b'embedding',c.payload);self.assertNotIn(b'body',c.payload)
        for validator,payload in ((validate_activation_payload,p),(validate_materialize_payload,m)):
            with self.subTest(validator=validator):
                with self.assertRaises(ValueError):validator(canonical({**payload,'embedding':[]}))
                with self.assertRaises(ValueError):validator(json.dumps(payload).encode())
        with self.assertRaises(ValueError):validate_materialize_payload(canonical({**m,'ordinal':True}))

if __name__=='__main__':unittest.main()
