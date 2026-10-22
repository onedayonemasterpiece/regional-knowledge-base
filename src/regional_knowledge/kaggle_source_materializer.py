"""Native deterministic lowering of exact accepted model/image batches.

No original download, renderer, LLM call or Dev vector result. Legacy import
codecs remain unchanged. One bounded source.materialize dispatch owns at most
one resident forward; canonical knowledge excludes only derived embeddings.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from pathlib import Path
from uuid import UUID, uuid5
from pydantic import Field

from .bge_contract import DIMENSION, MAX_TOKENS, REVISION, SPACE, TOKENIZER_SHA256
from .contracts import StagePageInput, StageChunkInput, StagePoiFactInput, StagePoiMediaLinkInput, StartMetadataInput
from .entity_graph import GraphBundle, validate_staged_bundle, digest as graph_digest
from .kaggle_import_contracts import SourceResearchInput
from .original_intake import VerifiedOriginalReceipt
from .source_stage import canonical, digest, validate_stage_payload, ZERO_HASH
from .source_activation import build_image_manifest, knowledge_bytes, PROJECTION_CODEC
from .source_graph_native import resolve_native_source_graph
from .stage_graph import StagedGraph, IngestStagePayload, compile_model_stage, validate_graph
from .story_contracts import StageStoryCandidateInput


class SourceMaterializationRejected(ValueError):
    """Fixed deterministic validation code, never private source diagnostics."""


def failure(reason, **unused):
    return SourceMaterializationRejected(reason)


class NativeStagedGraph(StagedGraph):
    # Bounds derive from the checked accepted batch chain, not old pilot caps.
    entity_candidates: list[GraphBundle] = Field(default_factory=list)



def _typed(model, raw):
    """Keep old input models unchanged, but never drop accepted new fields."""
    value=model.model_validate(raw)
    def retained(before, after):
        if type(before) is dict:
            if type(after) is not dict or set(before)-set(after):
                raise failure('native_source_unsupported_model_field')
            for key, item in before.items():retained(item,after[key])
        elif type(before) is list:
            if type(after) is not list or len(before)!=len(after):
                raise failure('native_source_unsupported_model_field')
            for a,b in zip(before,after,strict=True):retained(a,b)
    retained(raw,value.model_dump(mode='json'))
    return value


def materializer_fingerprint():
    root=Path(__file__).parent
    names=('kaggle_source_materializer.py','source_graph_native.py','source_stage.py',
        'source_activation.py','stage_graph.py','contracts.py','entity_graph.py',
        'story_contracts.py','search_material.py','region_policy.py','continuation_context.py',
        'source_manifest.py','kaggle_import_contracts.py','bge_contract.py')
    return digest(canonical({name:digest((root/name).read_bytes()) for name in names}))


def merge_native_stage(graph: StagedGraph, payload: IngestStagePayload) -> StagedGraph:
    # Reissuing the exact page for additional chunks is enrichment, not a
    # request to delete existing chunks. An explicit changed page replaces its
    # dependent material; earlier accepted batch bytes remain immutable.
    old = {str(p.page_id):p for p in graph.pages}
    changed = [p for p in payload.pages if old.get(str(p.page_id)) != p]
    payload = payload.model_copy(update={'pages':changed})
    replaced_page_ids = {str(page.page_id) for page in payload.pages}
    replaced_region_ids = {
        str(region.region_id)
        for page in graph.pages
        if str(page.page_id) in replaced_page_ids
        for region in page.regions
    }
    replaced_illustration_ids = {
        str(item.illustration_id)
        for item in graph.illustrations
        if str(item.page_id) in replaced_page_ids
    }

    pages = {
        str(page.page_id): page
        for page in graph.pages
        if str(page.page_id) not in replaced_page_ids
    }
    for page in payload.pages:
        pages[str(page.page_id)] = page

    relations = {
        (str(item.source_region_id), str(item.target_region_id), item.kind.value): item
        for item in graph.relations
        if str(item.source_region_id) not in replaced_region_ids
        and str(item.target_region_id) not in replaced_region_ids
    }
    for item in payload.relations:
        relations[
            (str(item.source_region_id), str(item.target_region_id), item.kind.value)
        ] = item

    illustrations = {
        str(item.illustration_id): item
        for item in graph.illustrations
        if str(item.page_id) not in replaced_page_ids
        and str(item.source_region_id) not in replaced_region_ids
    }
    for item in payload.illustrations:
        illustrations[str(item.illustration_id)] = item

    chunks = {
        str(item.chunk_id): item
        for item in graph.chunks
        if not any(str(page_id) in replaced_page_ids for page_id in item.page_ids)
        and not any(str(region_id) in replaced_region_ids for region_id in item.region_ids)
        and not any(
            str(region_id) in replaced_region_ids
            for region_id in item.footnote_region_ids
        )
        and not any(
            str(illustration_id) in replaced_illustration_ids
            for illustration_id in item.illustration_ids
        )
    }
    for item in payload.chunks:
        chunks[str(item.chunk_id)] = item

    poi_facts = {
        str(item.candidate_id): item
        for item in graph.poi_facts
        if not any(str(page_id) in replaced_page_ids for page_id in item.page_ids)
        and not any(str(region_id) in replaced_region_ids for region_id in item.region_ids)
    }
    for item in payload.poi_facts:
        poi_facts[str(item.candidate_id)] = item

    poi_media_links = {
        str(item.link_id): item
        for item in graph.poi_media_links
        if str(item.page_id) not in replaced_page_ids
        and str(item.illustration_id) not in replaced_illustration_ids
    }
    for item in payload.poi_media_links:
        poi_media_links[str(item.link_id)] = item

    return NativeStagedGraph(
        revision=graph.revision,
        entity_candidates=graph.entity_candidates,
        pages=sorted(pages.values(), key=lambda item: item.physical_page_index),
        relations=list(relations.values()),
        illustrations=list(illustrations.values()),
        chunks=list(chunks.values()),
        poi_facts=list(poi_facts.values()),
        poi_media_links=list(poi_media_links.values()),
    )



def refresh_native_graph_evidence(graph, replacement):
    """Refresh only superseded evidence; exact accepted batches stay immutable.

    A changed entity mention requires the calling model's explicit replacement
    node. Unrelated nodes/aliases and still-valid relation evidence survive.
    Without a replacement the stale proof stays visible and preflight rejects,
    so a later corrective batch can repair it rather than silently losing it.
    """
    from .entity_graph import validate_staged_bundle
    nodes={node.key:node for node in replacement.entities} if replacement else {}
    def current(proof):
        try:
            # Same exact checks as ordinary GraphBundle admission.
            chunk=next(c for c in graph.chunks if c.chunk_id==proof.chunk_id)
            region=next(r for p in graph.pages if p.page_id==proof.page_id
                        for r in p.regions if r.region_id==proof.region_id)
            return (proof.page_id in chunk.page_ids and proof.region_id in chunk.region_ids
                and proof.exact_quote in region.source_text and proof.exact_quote in chunk.text)
        except StopIteration:return False
    refreshed=[]
    for bundle in graph.entity_candidates:
        entities=[]
        for node in bundle.entities:
            valid=current(node.evidence)
            new=nodes.get(node.key)
            if not valid and new is not None:
                if (node.entity_id,node.kind)!=(new.entity_id,new.kind):
                    raise failure('native_source_correction_identity_changed')
                node=new.model_copy(update={'aliases':[a for a in node.aliases if current(a.evidence)]})
            else:
                node=node.model_copy(update={'aliases':[a for a in node.aliases if current(a.evidence)]})
            entities.append(node)
        relations=[]
        for edge in bundle.relations:
            evidence=[proof for proof in edge.evidence if current(proof)]
            if evidence:relations.append(edge.model_copy(update={'evidence':evidence}))
        value=bundle.model_copy(update={'entities':entities,'relations':relations})
        if value.entities or value.entity_refs or value.relations:
            if value not in refreshed:refreshed.append(value)
    graph.entity_candidates=refreshed
    return graph


class NativeSourceCompiler:
    def __init__(self, *, count, current):
        if not callable(count) or not callable(current):
            raise TypeError('retained resident tokenizer/current binding required')
        self.count,self.current=count,current

    def _current(self):
        if not self.current():
            raise PermissionError('native_source_materializer_generation_changed')

    async def _counts(self,texts):
        result=[]
        for text in texts:
            self._current()
            count=await self.count(text)
            self._current()
            if type(count) is not int or not 1<=count<=MAX_TOKENS:
                raise failure('complete_native_input_exceeds_512_tokens')
            result.append(count)
        return result

    async def _narrative_fragments(self, variant_id, body):
        """Keep the authored body intact; count only actual full encoder inputs.

        Offsets use Python/PG Unicode characters. No tokenizer truncation or
        summary substitute is permitted; a long story reuses the same native
        ordinal/vector path with one stable ID per contiguous source fragment.
        """
        self._current()
        whole = await self.count(body)
        self._current()
        if type(whole) is not int or not 1 <= whole <= 100000:
            raise failure('complete_native_input_token_count_invalid')
        if whole <= MAX_TOKENS:
            return whole, None
        start = 0
        fragments = []
        while start < len(body):
            lo, hi = start + 1, len(body)
            while lo < hi:
                middle = (lo + hi + 1) // 2
                self._current()
                candidate_count = await self.count(body[start:middle])
                self._current()
                if type(candidate_count) is not int or not 1 <= candidate_count <= 100000:
                    raise failure('complete_native_input_token_count_invalid')
                if candidate_count <= MAX_TOKENS:
                    lo = middle
                else:
                    hi = middle - 1
            end = lo
            if end <= start:
                raise failure('complete_narrative_fragment_exceeds_512_tokens')
            # Prefer an actual word boundary without dropping whitespace. A
            # language with no spaces still gets exact lossless char offsets.
            if end < len(body):
                break_at = body.rfind(' ', start, end)
                if break_at >= start + (end - start) // 2:
                    end = break_at + 1
            self._current()
            tokens = await self.count(body[start:end])
            self._current()
            if type(tokens) is not int or not 1 <= tokens <= MAX_TOKENS:
                raise failure('complete_narrative_fragment_exceeds_512_tokens')
            fragments.append(dict(fragment_id=str(uuid5(UUID(variant_id),
                f'fragment:{start}:{end}')),start=start,end=end,input_tokens=tokens))
            start = end
        if ''.join(body[f['start']:f['end']] for f in fragments) != body:
            raise failure('complete_narrative_fragment_coverage_changed')
        return fragments[0]['input_tokens'], fragments

    async def compile(self,batches,*,source_id,person_id,last_batch_sha256):
        try:
            return await self._compile(batches,source_id=source_id,person_id=person_id,
                last_batch_sha256=last_batch_sha256)
        except SourceMaterializationRejected:
            raise
        except (ValueError,TypeError,KeyError):
            raise failure('native_source_model_input_invalid') from None

    async def _compile(self,batches,*,source_id,person_id,last_batch_sha256):
        self._current()
        if type(batches) is not list or not 1<=len(batches)<=2500:
            raise failure('native_source_batch_chain_required')
        graph=NativeStagedGraph(revision=1)
        source=None;previous=ZERO_HASH;images={};reviews={};candidates={};research=None
        for index,raw in enumerate(batches):
            doc=validate_stage_payload(raw,source_id=source_id)
            if doc['batch_index']!=index or doc['previous_batch_sha256']!=previous:
                raise failure('native_source_batch_chain_changed')
            previous=digest(raw)
            if source is None:source=doc['source']
            elif source!=doc['source']:raise failure('native_source_descriptor_changed')
            payload=doc['model_payload']
            pages=[_typed(StagePageInput,p) for p in payload['pages']]
            chunks=[_typed(StageChunkInput,c) for c in payload['chunks']]
            facts=[_typed(StagePoiFactInput,f) for f in payload.get('poi_facts',[])]
            media=[_typed(StagePoiMediaLinkInput,m) for m in payload.get('poi_media_links',[])]
            if media or any(page.illustrations for page in pages):
                raise failure('native_illustration_activation_unavailable')
            partial=compile_model_stage(graph,document_id=source_id,revision=1,pages=pages,chunks=chunks,
                poi_facts=facts,poi_media_links=media)
            graph=merge_native_stage(graph,partial)
            bundle=_typed(GraphBundle,payload['entity_candidates']) if payload.get('entity_candidates') else None
            graph=refresh_native_graph_evidence(graph,bundle)
            if bundle is not None:
                validate_staged_bundle(bundle,graph)
                key=graph_digest(bundle.model_dump(mode='json'))
                if key not in {graph_digest(b.model_dump(mode='json')) for b in graph.entity_candidates}:
                    graph.entity_candidates.append(bundle)
            for page,encoded in zip(pages,doc['issued_images_b64'],strict=True):
                image_raw=base64.b64decode(encoded,validate=True)
                image=json.loads(image_raw)
                image['source_image_receipt_sha256']=digest(image_raw)
                old=images.get(page.page_id)
                if old is not None and old!=image:raise failure('native_source_image_changed')
                images[page.page_id]=image
                reviews[page.page_id]=bool(page.knowledge_reviewed and page.story_candidates_reviewed)
            for value in payload.get('story_candidates',[]):
                candidate=_typed(StageStoryCandidateInput,value)
                candidates[candidate.candidate_key]=candidate.model_dump(mode='json')
            research=doc['research']
        if previous!=last_batch_sha256:raise failure('native_source_batch_chain_changed')
        receipt=VerifiedOriginalReceipt.from_wire(json.loads(base64.b64decode(source['original_receipt_b64'],validate=True)))
        validation=validate_graph(graph,expected_page_count=receipt.page_count)
        if not validation.ready:raise failure('native_source_stage_requires_review')
        if research['state']=='complete' and not all(reviews.get(str(p.page_id)) for p in graph.pages):
            raise failure('complete_research_requires_explicit_same_pass_page_review')
        metadata=StartMetadataInput.model_validate(source['metadata'])
        language=(metadata.language or '').lower().replace('_','-').split('-',1)[0]
        language=language if language in ('ru','de') else 'simple'
        pages={str(p.page_id):p for p in graph.pages}
        counts=await self._counts([chunk.text for chunk in graph.chunks])
        passages=[]
        for ordinal,(chunk,tokens) in enumerate(zip(graph.chunks,counts,strict=True)):
            if chunk.article_id or chunk.illustration_ids:raise failure('native_source_component_activation_unavailable')
            locators=[pages[str(p)].printed_page_number or str(pages[str(p)].physical_page_index+1) for p in chunk.page_ids]
            passages.append(dict(passage_id=str(chunk.chunk_id),ordinal=ordinal,locator='pages '+', '.join(locators),
                body=chunk.text,language=language,input_tokens=tokens))
        row=dict(source_id=source_id,person_id=person_id,
            semantic_json=canonical(dict(candidates=candidates,page_reviews=reviews)).decode(),
            research_json=canonical({k:research[k] for k in ('state','continuation')}).decode())
        semantic,_=await self._semantic(row,graph,passages)
        self._current()
        resolved=dict(codec=PROJECTION_CODEC,bibliography=source['bibliography'],revision=1,
            work_id=source['work_id'],edition_id=source['edition_id'],original=receipt.original,
            public_rights=source['public_rights'],
            model=dict(revision=REVISION,space=SPACE,tokenizer_sha256=TOKENIZER_SHA256,
                dimension=DIMENSION,max_tokens=MAX_TOKENS),
            passages=passages,semantic=semantic,research=research,
            image_manifest=build_image_manifest(metadata=metadata,receipt=receipt,graph=graph,
                semantic=semantic,passages=passages,issued_images=list(images.values())))
        if graph.entity_candidates or graph.poi_facts or any(f['entity_keys'] for f in semantic['findings']):
            resolved['source_graph']=resolve_native_source_graph(graph,source_id=source_id,person_id=person_id,
                semantic=semantic,batch_count=len(batches))
        # Resource capacity is explicit; it is not the old32-page codec limit.
        knowledge_bytes(resolved)
        return resolved

    async def _semantic(self, row, graph, passages):
        staged = json.loads(row['semantic_json'])
        regions = {str(region.region_id): region for page in graph.pages for region in page.regions}
        by_ref = {(str(region.page_id), region.region_key): region for region in regions.values()}
        result = dict(regions=[dict(region_id=key, page_id=str(region.page_id), source_text=region.source_text,
            source_text_sha256=digest(region.source_text.encode())) for key, region in regions.items()],
            findings=[], stories=[], pending=[])
        narrative_inputs = []
        candidates = [_typed(StageStoryCandidateInput,raw) for raw in staged['candidates'].values()]
        # The complete authored body is not itself a model input when it needs
        # multiple bounded embeddings. Keep one scalar token count for the
        # representative first input; the fragment ledger retains every other.
        vector_inputs = {}
        for candidate in candidates:
            if candidate.basic_variant is not None:
                variant_id = str(uuid5(UUID(row['source_id']), 'variant:'+candidate.candidate_key))
                vector_inputs[candidate.candidate_key] = await self._narrative_fragments(
                    variant_id, candidate.basic_variant.body)
        for candidate in candidates:
            key = candidate.candidate_key
            assertion_id = str(uuid5(UUID(row['source_id']), 'finding:'+key))
            evidence = []
            for ref in candidate.evidence_refs:
                region = by_ref.get((ref.page_id, ref.region_key))
                if region is None:
                    raise failure('finding_requires_staged_native_region', status='invalid_query')
                begin, end = ref.start, ref.end
                if begin is None:
                    begin = region.source_text.find(ref.original_excerpt)
                    if begin < 0 or region.source_text.find(ref.original_excerpt, begin+1) >= 0:
                        raise failure('finding_requires_unambiguous_exact_source_offsets', status='invalid_query')
                    end = begin+len(ref.original_excerpt)
                if region.source_text[begin:end] != ref.original_excerpt:
                    raise failure('finding_excerpt_mismatch', status='invalid_query')
                matching = []
                for chunk in graph.chunks:
                    if region.region_id not in chunk.region_ids:
                        continue
                    spans = [span for span in chunk.source_spans if span.region_id == region.region_id]
                    if spans and not any(span.start <= begin < end <= span.end for span in spans):
                        continue
                    if ref.original_excerpt in chunk.text:
                        matching.append(str(chunk.chunk_id))
                if not matching:
                    raise failure('finding_requires_exact_source_passage', status='invalid_query')
                evidence.append(dict(region_id=str(region.region_id), passage_id=matching[0], start=begin,
                    end=end, original_excerpt=ref.original_excerpt))
            result['findings'].append(dict(assertion_id=assertion_id, candidate_key=key,
                proposition=candidate.proposition, account_kind=candidate.account_kind,
                attributed_to=candidate.attributed_to, reported_by=candidate.reported_by, evidence=evidence,
                event_dates=[date.model_dump(mode='json') for date in candidate.event_dates],
                observations=[value.model_dump(mode='json') for value in candidate.observations], entity_keys=list(candidate.entity_keys)))
            if any(index >= len(evidence) for item in [*candidate.event_dates, *candidate.observations]
                    for index in item.evidence_indexes):
                raise failure('finding_metadata_requires_existing_evidence', status='invalid_query')
            variant = candidate.basic_variant
            if variant is None:
                if candidate.narrative_outcome != 'facts_only':
                    result['pending'].append(dict(candidate_key=key, finding_ids=[assertion_id], reason='needs_context'))
                continue
            count, fragments = vector_inputs[key]
            variant_id = str(uuid5(UUID(row['source_id']), 'variant:'+key))
            saved_variant = dict(variant_id=variant_id, revision=1,
                audience=variant.audience, format='story', language=variant.language, body=variant.body,
                blocks=[dict(start=block.start, end=block.end, assertion_ids=[assertion_id]) for block in variant.blocks],
                input_tokens=count)
            if fragments is not None:
                saved_variant['fragments'] = fragments
            result['stories'].append(dict(story_id=str(uuid5(UUID(row['source_id']), 'story:'+key)),
                finding_ids=[assertion_id], title=candidate.title, summary=candidate.summary,
                material_type=candidate.material_type, default_variant=saved_variant))
            for fragment in fragments or (dict(fragment_id=variant_id,
                    start=0,end=len(variant.body),input_tokens=count),):
                narrative_inputs.append(dict(passage_id=fragment['fragment_id'],
                    body=variant.body[fragment['start']:fragment['end']],
                    input_tokens=fragment['input_tokens']))
        if result['pending'] and SourceResearchInput.model_validate_json(row['research_json']).state != 'pending':
            raise failure('unfinished_narratives_require_explicit_research_continuation', status='invalid_query')
        return result, narrative_inputs



class NativeSourceMaterializer:
    """One admitted source command, one local input vector, no wire vectors.

    acquire belongs to the executor's owned writer pool (or restore candidate).
    Current writer identity is checked natively again at both private effects.
    No PG transaction or connection is held across tokenizer/model awaits.
    """
    def __init__(self, *, count, encode, current):
        if not all(callable(v) for v in (count,encode,current)):
            raise TypeError('retained native tokenizer/encoder/current owner required')
        self.count,self.encode,self.current=count,encode,current

    def _current(self):
        if not self.current():raise PermissionError('native_source_owner_changed')

    async def __call__(self,command,*,acquire,database):
        from .source_activation import MATERIALIZE_ROUTE,validate_materialize_command
        from .source_stage import load_stage_batch,SEAL_ROUTE
        from .bge_contract import validate_vector
        sealing=command.route==SEAL_ROUTE
        if command.route not in (MATERIALIZE_ROUTE,SEAL_ROUTE):return
        if sealing:
            from .kaggle_native_source_bindings import validate_native_source_command
            doc=dict(validate_native_source_command(command),materializer_sha256=materializer_fingerprint())
        else:doc=validate_materialize_command(command)
        self._current()
        if doc['materializer_sha256']!=materializer_fingerprint():
            raise PermissionError('accepted_native_materializer_unavailable')
        source_id=command.ids['source_id']
        exact=(source_id,doc['ingestion_id'],doc['batch_count'],doc['last_batch_sha256'],doc['materializer_sha256'])
        batches=None
        async with acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='5s'")
                row=await (await connection.execute('SELECT command_sha256 FROM rkb_v4.applied_ops WHERE operation_key=%s',
                    (command.operation_key,))).fetchone()
                if row is not None:
                    # apply_accepted alone verifies duplicates/conflicts. Never
                    # repeat a model call just to reconstruct an old receipt.
                    return
                candidate=await (await connection.execute('''SELECT knowledge_sha256,total_inputs
                    FROM rkb_v4.source_activation_candidates WHERE source_id=%s AND ingestion_id=%s
                    AND batch_count=%s AND last_batch_sha256=%s AND materializer_sha256=%s''',exact)).fetchone()
                if candidate is None:
                    batches=[]
                    size=0
                    for index in range(doc['batch_count']):
                        raw=await load_stage_batch(connection,source_id=source_id,ingestion_id=doc['ingestion_id'],batch_index=index)
                        if raw is None:raise failure('native_source_accepted_batch_missing')
                        size+=len(raw)
                        if size>128*1024*1024:raise failure('native_source_materialization_capacity')
                        batches.append(raw)
        self._current()
        if batches is not None:
            compiler=NativeSourceCompiler(count=self.count,current=self.current)
            resolved=await compiler.compile(batches,source_id=source_id,person_id=command.person_id,
                last_batch_sha256=doc['last_batch_sha256'])
            knowledge=knowledge_bytes(resolved)
            self._current()
            try:
                async with acquire() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout='5s'")
                        row=await (await connection.execute('''SELECT rkb_v4.prepare_source_activation_v1(
                            %s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s)''',
                            (*exact,knowledge,database.database_generation,database.writer_generation))).fetchone()
                        if row is None or row[0]!=digest(knowledge):raise failure('native_source_candidate_binding')
            except Exception as error:
                if getattr(error,'sqlstate',None)=='22023':
                    raise failure('native_source_stage_requires_review') from None
                raise
            candidate=(digest(knowledge),len(resolved['passages'])+len(resolved['semantic']['stories']))
        self._current()
        if sealing:return
        knowledge_hash,total=candidate
        if type(total) is not int or not 0<=doc['ordinal']<total:
            raise failure('native_source_materialize_ordinal')
        async with acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='5s'")
                item=await (await connection.execute('''SELECT input_id,input_sha256,body,installed
                    FROM rkb_v4.read_source_activation_input_v1(%s::uuid,%s,%s)''',
                    (source_id,knowledge_hash,doc['ordinal']))).fetchone()
        self._current()
        if (item is None or type(item[2]) is not str or not item[2]
                or digest(item[2].encode())!=item[1] or type(item[3]) is not bool):
            raise failure('native_source_exact_input_missing')
        if item[3]:return
        vector=validate_vector(await self.encode(item[2]))
        self._current()
        async with acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout='5s'")
                row=await (await connection.execute('''SELECT rkb_v4.install_source_activation_vector_v1(
                    %s::uuid,%s,%s,%s::public.vector,%s,%s)''',
                    (source_id,knowledge_hash,doc['ordinal'],json.dumps(vector,allow_nan=False),
                        database.database_generation,database.writer_generation))).fetchone()
                if row is None or row[0]!=item[1]:raise failure('native_source_vector_input_changed')
        self._current()


def resident_source_materializer(encoder,*,current):
    """Reuse the verified resident handle and drain real inference on cancel."""
    from .kaggle_bootstrap import ResidentEncoder
    if type(encoder) is not ResidentEncoder:
        raise TypeError('same verified resident BGE/tokenizer required')
    async def forward(text):
        task=asyncio.create_task(asyncio.to_thread(encoder,text))
        cancelled=False
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError:cancelled=True
            except Exception:break
        if cancelled:
            if not task.cancelled():task.exception()
            raise asyncio.CancelledError
        return task.result()
    return NativeSourceMaterializer(count=encoder.count,encode=forward,current=current)
