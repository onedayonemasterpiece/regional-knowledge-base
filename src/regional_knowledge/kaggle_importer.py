"""Bounded owner intake on the existing MCP, queue, journal and source executor.

Original acquisition/storage belongs to the supplied native intake controller.
These are short control records, never writes to the SQLite corpus or a second
domain queue. No model inference or private-original download runs on Dev.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import struct
from uuid import UUID, uuid4, uuid5

from mcp.server.mcpserver.exceptions import ToolError

from .bge_contract import DIMENSION, MAX_TOKENS, REVISION, SPACE, TOKENIZER_SHA256, validate_vector
from .contracts import (BookIngestOutput, KnowledgeExtractionStatus, Principal,
    StageChunkInput, StagePageInput, StagePoiFactInput, StoryExtractionStatus)
from .kaggle_apply import Revisions
from .kaggle_import_contracts import PublicImportRights, SourceResearchInput, TargetSourceImportInput
from .kaggle_import_output import OwnerImportOutputGate
from .kaggle_import_tokenizer import HfImportTokenCounter
from .kaggle_public_backend import current_binding
from .original_intake import OriginalIntakeError, TERMINAL_REJECTIONS, VerifiedOriginalReceipt
from .participants import RequestAuthority
from .source_import import MAX_IMPORT_BYTES
from .source_import_v2 import IMPORT_V2_DEPENDENCY, SOURCE_IMPORT_V2, STORY_REVISIONS, resolved_import_v2_command
from .source_import_v3 import IMPORT_V3_DEPENDENCY, SOURCE_IMPORT_V3, FACT_REVISIONS, resolved_import_v3_command
from .source_import_v4 import IMPORT_V4_DEPENDENCY, SOURCE_IMPORT_V4, resolved_import_v4_command
from .story_editorial import EDITORIAL_REVISIONS
from .kaggle_domain_bindings import DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS,resolved_current_import_v4_command
from .story_contracts import StageStoryCandidateInput
from .stage_graph import StagedGraph, compile_model_stage, merge_stage, validate_graph

_LOG = logging.getLogger(__name__)
MAX_BYTES = 128 * 1024 * 1024
MAX_PAGES = 2500
MAX_PASSAGES = 32
MAX_STAGE_PASSAGES = 100
MAX_BODY_BYTES = 65536
MAX_CONTROL_BYTES = 8 * 1024 * 1024
MAX_CONTROL_ROWS = 32


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def failure(reason, *, status='unavailable'):
    return ToolError(json.dumps({'status': status, 'reason': reason}))


def uuid_value(value):
    try:
        if type(value) is not str or str(UUID(value)) != value:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError):
        raise failure('invalid_ingestion_identity', status='invalid_query') from None


@dataclass
class ImportAccess:
    authority: object
    context: RequestAuthority
    binding: object
    retained: bool = False


class KaggleSourceImporter:
    def __init__(self, startup, *, originals, tokenizer):
        self.startup = startup
        config = startup.config
        self.queue, self.spool, self.participants = config.queue, config.spool, config.participants
        self.read_authority = startup.authority
        self.parser_sha256 = config.import_parser_sha256
        self.launch_config_sha256 = config.import_launch_config_sha256
        self.destination = startup.backend.destination
        if (originals.queue is not self.queue or originals.spool is not self.spool
                or originals.participants is not self.participants or type(tokenizer) is not HfImportTokenCounter):
            raise TypeError('existing native intake, control owners and exact tokenizer required')
        choices = [(versions, builder) for route,versions,dependency,builder in (
            (SOURCE_IMPORT_V2,STORY_REVISIONS,IMPORT_V2_DEPENDENCY,resolved_import_v2_command),
            (SOURCE_IMPORT_V3,FACT_REVISIONS,IMPORT_V3_DEPENDENCY,resolved_import_v3_command),
            (SOURCE_IMPORT_V4,EDITORIAL_REVISIONS,IMPORT_V4_DEPENDENCY,resolved_import_v4_command),
            (SOURCE_IMPORT_V4,DOMAIN_REVISIONS,IMPORT_V4_DEPENDENCY,resolved_current_import_v4_command),
            (SOURCE_IMPORT_V4,DOMAIN_V2_REVISIONS,IMPORT_V4_DEPENDENCY,resolved_current_import_v4_command))
            if (handler:=self.spool.registry.handlers.get(route)) is not None
            and handler.revisions==versions and handler.dependency == dependency]
        from .source_activation import ACTIVATION_REVISIONS,ACTIVATE_ROUTE,resolved_activation_command
        from .kaggle_native_source_bindings import native_source_handler
        native=self.spool.registry.handlers.get(ACTIVATE_ROUTE)
        if native is not None:
            expected=native_source_handler(ACTIVATE_ROUTE,check_sha256=native.check.definition_sha256,
                apply_sha256=native.apply.definition_sha256)
            if native!=expected:raise TypeError('one exact native source profile required')
            choices.append((ACTIVATION_REVISIONS,resolved_activation_command))
        if native is not None:
            choices=[(ACTIVATION_REVISIONS,resolved_activation_command)]
        if len(choices)!=1:
            raise TypeError('exact future source findings/narrative handler required before intake')
        self.import_revisions,self.import_command=choices[0]
        self.originals, self.tokenizer = originals, tokenizer
        self._lock = asyncio.Lock()
        self.output_gate = OwnerImportOutputGate(self)
        with self.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_source_intake_control(
                ingestion_id TEXT PRIMARY KEY,source_id TEXT UNIQUE NOT NULL,person_id TEXT NOT NULL,
                file_id TEXT NOT NULL,request_id TEXT NOT NULL,metadata_json TEXT NOT NULL,
                rights_json TEXT NOT NULL,stage_json TEXT,research_json TEXT,phase TEXT NOT NULL,
                frozen_sha256 TEXT,created REAL NOT NULL,updated REAL NOT NULL)''')
            columns = {value[1] for value in db.execute('PRAGMA table_info(rkb_source_intake_control)')}
            for name in ('native_proof_sha256', 'semantic_json'):
                if name not in columns:
                    db.execute('ALTER TABLE rkb_source_intake_control ADD COLUMN '+name+' TEXT')
        self.native_intake=None
        if self.import_revisions==ACTIVATION_REVISIONS:
            from .kaggle_native_source_intake import NativeSourceIntake
            self.native_intake=NativeSourceIntake(self)

    def _binding_current(self, binding):
        if not self.startup.binding_current(binding):
            return False
        snapshot=getattr(self.startup,'snapshot',None)
        if snapshot is not None and not snapshot.verified:return False
        try:
            with self.queue.connect(write=False) as db:
                runtime = self.queue._runtime_row(db,binding.run_id)
                if runtime is None:
                    return False
                expected = self.queue._runtime_expected(runtime)
                return (expected.hashes['launch_config'] == self.launch_config_sha256
                    and expected.database_generation == binding.database_generation)
        except Exception:
            return False

    async def _check(self, authority, context, binding):
        await self.read_authority.recheck(authority)
        if authority.route != 'owner' or authority.public_only or context.route != 'owner':
            raise PermissionError('owner personal import route required')
        await asyncio.to_thread(self.participants.require, context, 'import')
        if not self._binding_current(binding):
            raise failure('target_runtime_changed')

    @asynccontextmanager
    async def _access(self, principal):
        authority = await self.read_authority.resolve_oauth(principal.access_token, destination=self.destination)
        access = None
        try:
            if authority.person_id != principal.subject or authority.route != 'owner' or authority.public_only:
                raise PermissionError('current owner import authority required')
            context = RequestAuthority(Principal(subject=authority.person_id, client_id=authority.client_id,
                issuer=authority.issuer, access_token=''), route='owner')
            binding = await asyncio.to_thread(current_binding, self.queue)
            access = ImportAccess(authority, context, binding)
            await self._check(authority, context, binding)
            yield access
        finally:
            if access is None or not access.retained:
                self.read_authority.release(authority)

    async def _reply(self, access, output, *, render=None):
        access.retained = await self.output_gate.retain(output, access.authority,
            access.context, access.binding, render=render)
        return output

    def _row(self, actor, ingestion_id=None, document_id=None):
        if ingestion_id is None and document_id is None:
            raise failure('ingestion_id_required', status='invalid_query')
        column, identity = ('ingestion_id', uuid_value(ingestion_id)) if ingestion_id else ('source_id', uuid_value(document_id))
        with self.queue.connect(write=False) as db:
            row = db.execute(f'SELECT * FROM rkb_source_intake_control WHERE {column}=? AND person_id=?',
                (identity, actor)).fetchone()
            if row is None:
                raise failure('owned_ingestion_required', status='denied')
            if ingestion_id and document_id and row['source_id'] != uuid_value(document_id):
                raise failure('ingestion_source_mismatch', status='invalid_query')
            return dict(row)

    def _save(self, row, **changes):
        allowed = {'stage_json', 'research_json', 'phase', 'frozen_sha256', 'native_proof_sha256', 'semantic_json'}
        if not changes or set(changes) - allowed:
            raise ValueError('bounded intake transition required')
        changes['updated'] = self.queue.clock()
        if any(isinstance(value, str) and len(value.encode()) > 1024 * 1024 for value in changes.values()):
            raise failure('small_import_control_bound')
        with self.queue.connect() as db:
            fields = ','.join(key+'=?' for key in changes)
            db.execute(f'UPDATE rkb_source_intake_control SET {fields} WHERE ingestion_id=? AND person_id=?',
                (*changes.values(), row['ingestion_id'], row['person_id']))
            self._budget(db)
        if changes.get('phase',row['phase']) != row['phase']:
            _LOG.info('source_import_control ingestion_id=%s source_id=%s state=%s previous=%s',
                row['ingestion_id'],row['source_id'],changes['phase'],row['phase'])
        return self._row(row['person_id'], row['ingestion_id'])

    @staticmethod
    def _budget(db):
        # Keep terminal intake identities, receipts and status addressable;
        # only unsettled control rows consume active row/byte capacity.
        row = db.execute('''SELECT count(*),coalesce(sum(length(CAST(metadata_json AS BLOB))+
            length(CAST(rights_json AS BLOB))+coalesce(length(CAST(stage_json AS BLOB)),0)+
            coalesce(length(CAST(research_json AS BLOB)),0)+
            coalesce(length(CAST(semantic_json AS BLOB)),0)+2048),0) FROM rkb_source_intake_control
            WHERE phase NOT IN ('finalized','failed')''').fetchone()
        if row[0] > MAX_CONTROL_ROWS or row[1] > MAX_CONTROL_BYTES:
            raise failure('small_import_control_capacity')

    async def owned_row(self, principal, ingestion_id):
        async with self._access(principal) as access:
            row = await asyncio.to_thread(self._row, access.authority.person_id, ingestion_id)
            return {'id': row['ingestion_id'], 'owner_user_id': row['person_id'],
                'document_id': row['source_id'], 'staged_revision': 1}

    async def _receipt(self, access, row):
        receipt = await self.originals.verified(access.context, row['ingestion_id'])
        await self._check(access.authority, access.context, access.binding)
        if receipt is None:
            return None
        if (type(receipt) is not VerifiedOriginalReceipt or VerifiedOriginalReceipt.from_wire(receipt.wire()) != receipt
                or receipt.source_id != row['source_id'] or receipt.ingestion_id != row['ingestion_id']
                or receipt.request_id != str(uuid5(UUID(row['ingestion_id']), 'original:source-original:'+row['ingestion_id']))
                # Actor-verified, archived PDF receipts may retain a
                # historical parser hash. Native restore/page dispatch
                # independently permits only the exact old PDF executable;
                # unknown DjVu parser hashes are never accepted.
                or (receipt.parser_sha256 != self.parser_sha256 and receipt.format != 'pdf')
                or receipt.format not in ('pdf','djvu') or receipt.size > MAX_BYTES or receipt.page_count > MAX_PAGES):
            raise failure('verified_small_original_required')
        return receipt

    @staticmethod
    def _output(row, state, message, action, **kwargs):
        return BookIngestOutput(ingestion_id=row['ingestion_id'], document_id=row['source_id'],
            state=state, message=message, next_action=action, **kwargs)

    async def _start(self, access, *, file, metadata, source_import):
        if file is None or source_import is None or source_import.public_rights is None:
            raise failure('attached_source_and_explicit_public_rights_required', status='invalid_query')
        mime = (file.mime_type or '').lower().split(';', 1)[0]
        if mime not in {'application/pdf', 'application/x-pdf','image/vnd.djvu','image/x.djvu',
                'application/x-djvu','application/vnd.djvu','application/octet-stream'}:
            raise failure('target_pdf_or_djvu_only')
        if not metadata.get('title') or metadata.get('duplicate_policy', 'reuse') != 'reuse' or metadata.get('catalog_update'):
            raise failure('new_small_source_metadata_required', status='invalid_query')
        catalog = metadata.get('catalog') or {}
        if catalog.get('parent_id') or catalog.get('region_ids') or catalog.get('kind', 'book') != 'book':
            raise failure('target_source_components_unavailable')
        actor = access.authority.person_id
        metadata_bytes = canonical(metadata)
        rights = canonical(source_import.public_rights.model_dump(mode='json')).decode()
        if len(metadata_bytes) > 16384:
            raise failure('small_import_metadata_bound')
        with self.queue.connect() as db:
            prior = db.execute('SELECT * FROM rkb_source_intake_control WHERE person_id=? AND file_id=?',
                (actor, file.file_id)).fetchone()
            if prior is not None:
                if prior['metadata_json'] != metadata_bytes.decode() or prior['rights_json'] != rights:
                    raise failure('existing_file_intake_differs', status='invalid_query')
                row = dict(prior)
            else:
                if self.native_intake is not None and db.execute(
                        "SELECT 1 FROM rkb_source_intake_control WHERE person_id=? AND phase NOT IN ('finalized','failed') LIMIT 1",
                        (actor,)).fetchone():
                    raise failure('another_native_ingestion_active')
                source_id, ingestion_id, request_id = (str(uuid4()) for _ in range(3))
                now = self.queue.clock()
                db.execute('''INSERT INTO rkb_source_intake_control(ingestion_id,source_id,person_id,file_id,
                    request_id,metadata_json,rights_json,stage_json,research_json,phase,frozen_sha256,created,updated)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (ingestion_id, source_id, actor, file.file_id, request_id, metadata_bytes.decode(), rights,
                     None, None, 'acquiring', None, now, now))
                self._budget(db)
                row = dict(db.execute('SELECT * FROM rkb_source_intake_control WHERE ingestion_id=?', (ingestion_id,)).fetchone())
        await self._check(access.authority, access.context, access.binding)
        status = await self.originals.begin(access.context, operation_id='source-original:'+row['ingestion_id'],
            source_id=row['source_id'], ingestion_id=row['ingestion_id'], file=file, max_bytes=MAX_BYTES)
        receipt = await self._receipt(access, row)
        if receipt is None:
            return await self._original_pending(row,status)
        if row['phase'] == 'acquiring':
            row = await asyncio.to_thread(self._save, row, phase='staging')
        if await self._entry(row) is not None or row['phase']=='embedding':
            return await self._finalize(access,row)
        if row['phase']=='validated':
            return self._output(row,'ready','Validated source stage retained; finalize to acquire document vectors',
                'finalize',source_archive_status='verified')
        return self._output(row, 'staged', 'Verified original; review source pages and stage exact source-backed chunks',
            'continue_pages', next_cursor='0', source_archive_status='verified')

    async def _original_pending(self,row,status):
        # Only the existing controller's fixed, durable terminal rejection
        # can finish an unstarted intake. UNKNOWN/temporary/pending results
        # must not release the single active owner slot.
        if status.state == 'rejected' and status.reason in TERMINAL_REJECTIONS:
            if row['phase'] == 'acquiring':
                row = await asyncio.to_thread(self._save, row, phase='failed')
            return self._output(row,'failed','Native original intake rejected this source within the supported bounds',
                'blocker',source_archive_status='pending',warnings=[status.reason])
        if status.state == 'rejected':
            raise failure('native_original_terminal_rejection_unverified')
        if status.reason == 'original_telegram_binding_required':
            return self._output(row,'processing',
                'The owned Kaggle runtime requires its existing authorized Telegram archive client binding',
                'blocker',source_archive_status='pending',warnings=[status.reason])
        if status.reason == 'original_attachment_retry_same_file_required':
            return self._output(row,'processing',
                'Resume book_ingest(start) with the same file_id, metadata and rights, and a refreshed attachment URL',
                'blocker',source_archive_status='pending',warnings=[status.reason])
        return self._output(row,'processing','Original acquisition and verified durable readback pending',
            'wait',source_archive_status='pending')

    async def _native_research_status(self, access, row, output):
        """Expose the SAME accepted model's exact page continuation on status.

        A finalized source is not automatically a completed investigation.
        This is a read-only, retained owner MCP output; the old source is
        neither restaged nor given a new activation/revision or inference.
        """
        await self._check(access.authority, access.context, access.binding)
        if (output.state != 'finalized' or output.ingestion_id != row['ingestion_id']
                or output.document_id != row['source_id']):
            raise failure('native_research_status_identity_changed')
        current = await asyncio.to_thread(self._row, row['person_id'], row['ingestion_id'])
        if current['phase'] != 'finalized':
            raise failure('native_research_terminal_state_changed')
        if not current['research_json']:
            raise failure('native_research_accepted_frontier_missing')
        try:
            research = SourceResearchInput.model_validate_json(current['research_json'])
        except (ValueError, TypeError):
            raise failure('native_research_accepted_frontier_invalid') from None
        if research.state == 'complete':
            if research.continuation:
                raise failure('native_research_accepted_frontier_invalid')
            return output
        step = research.continuation
        args = step.get('arguments')
        if (step.get('tool') != 'book_pages' or type(args) is not dict
                or set(args) - {'ingestion_id', 'cursor', 'batch_size'}
                or args.get('ingestion_id') != row['ingestion_id']):
            raise failure('native_research_continuation_unreachable')
        cursor = args.get('cursor')
        if ('cursor' in args and cursor is not None and
                (type(cursor) is not str or not 1 <= len(cursor) <= 256
                 or any(ord(char) < 32 or ord(char) > 126 for char in cursor))):
            raise failure('native_research_continuation_unreachable')
        if 'batch_size' in args and (type(args['batch_size']) is not int or args['batch_size'] != 1):
            raise failure('native_research_continuation_unreachable')
        # Recover an exact existing owner-bound archived original instead of
        # asking the user to upload it again. The actual next book_pages call
        # independently checks current actor, source, render/pixel provenance.
        if await self._receipt(access, current) is None:
            raise failure('native_research_archived_original_unavailable')
        await self._check(access.authority, access.context, access.binding)
        return output.model_copy(update={
            'next_action': 'continue_pages', 'next_cursor': cursor,
            'message': output.message + '; model-authored source research remains pending: '
                       'resume the saved book_pages continuation with this ingestion_id',
            'warnings': list(dict.fromkeys([*output.warnings, 'model_authored_research_pending']))})

    async def _native_proof(self, access, row, receipt, graph):
        proof = await self.originals.native_pages(access.context, row['ingestion_id'])
        await self._check(access.authority, access.context, access.binding)
        if (proof.original_receipt_sha256 != receipt.receipt_sha256
                or row.get('native_proof_sha256') not in (None, proof.sha256)):
            raise failure('native_original_provenance_changed')
        by_id = {page['page_id']: page for page in proof.pages}
        if len(by_id) != receipt.page_count:
            raise failure('native_original_page_proof_incomplete')
        for page in graph.pages:
            native = by_id.get(str(page.page_id))
            if (native is None or native['physical_page_index'] != page.physical_page_index
                    or native['original_sha256'] != receipt.sha256 or native['parser_sha256'] != receipt.parser_sha256):
                raise failure('native_source_page_binding_mismatch', status='invalid_query')
            # The bounded first path accepts only complete observed native
            # blocks. Larger OCR/visual transcription requires another explicit
            # provenance contract, never a caller-supplied "native" flag.
            blocks = native['native_blocks']
            for region in page.regions:
                if not any(region.source_text.strip() and block['text'].count(region.source_text) == 1 and not block['truncated']
                        and block['original_length'] == len(block['text'])
                        and block['bbox'] == region.bbox.model_dump()
                        for block in blocks):
                    raise failure('stage_requires_exact_observed_native_span', status='invalid_query')
        return proof

    @staticmethod
    def _uses_issued_images(pages):
        return any(page.source_image_receipt_sha256 is not None for page in pages)

    async def _issued_images(self,access,row,receipt,pages):
        # These receipts bind actual issued pixels to this exact original.
        # The model's source_text/bbox remain explicitly model-authored. They
        # are not compared with (or labelled as) a native-text extraction.
        if any(page.source_image_receipt_sha256 is None
                or page.source_material!='visual_reviewed' or not page.source_review_note
                for page in pages):
            raise failure('stage_requires_reviewed_issued_image',status='invalid_query')
        for offset in range(0,len(pages),8):
            selected=pages[offset:offset+8]
            issued=await self.originals.selected_image_receipts(access.context,row['ingestion_id'],
                tuple((page.physical_page_index,page.source_image_receipt_sha256) for page in selected))
            await self._check(access.authority,access.context,access.binding)
            if len(issued)!=len(selected) or any(
                    value['original_receipt_sha256']!=receipt.receipt_sha256
                    or value['page_id']!=str(page.page_id)
                    or value['source_image_receipt_sha256']!=page.source_image_receipt_sha256
                    for page,value in zip(selected,issued,strict=True)):
                raise failure('source_image_provenance_changed')

    @staticmethod
    def _operation(row):
        return 'source-import:'+row['ingestion_id']

    async def _entry(self, row):
        from .control_spool import SpoolError
        try:
            return await self.spool.get(self._operation(row))
        except SpoolError as error:
            if str(error) == 'control operation not found':
                return None
            raise

    async def _stage(self, access, row, payload, source_import):
        if self.native_intake is not None:return await self.native_intake.stage(access,row,payload,source_import)
        receipt = await self._receipt(access, row)
        if receipt is None:
            return self._output(row, 'processing', 'Original verification pending', 'wait')
        if await self._entry(row) is not None:
            raise failure('accepted_import_is_immutable')
        if not payload:
            raise failure('model_authored_stage_required', status='invalid_query')
        if len(canonical(payload))>1024*1024:
            raise failure('stage_envelope_bound',status='invalid_query')
        if (source_import is not None and source_import.public_rights is not None
                and canonical(source_import.public_rights.model_dump(mode='json')).decode() != row['rights_json']):
            raise failure('original_public_rights_basis_is_immutable', status='invalid_query')
        if payload.get('poi_media_links') or (self.import_revisions not in (DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS) and any(
                payload.get(key) for key in ('poi_facts','entity_candidates'))):
            raise failure('target_semantic_domain_unavailable')
        pages = [StagePageInput.model_validate(value) for value in payload.get('pages', [])]
        chunks = [StageChunkInput.model_validate(value) for value in payload.get('chunks', [])]
        poi_facts=[StagePoiFactInput.model_validate(value) for value in payload.get('poi_facts',[])]
        if (len(pages)>8 or len(chunks)>MAX_STAGE_PASSAGES or len(payload.get('story_candidates',[]))>12
                or any(page.illustrations for page in pages)):
            raise failure('target_small_text_stage_only')
        graph = StagedGraph.model_validate_json(row['stage_json']) if row['stage_json'] else StagedGraph(revision=1)
        image_mode=self._uses_issued_images(pages) or self._uses_issued_images(graph.pages)
        if image_mode:
            if any(page.source_image_receipt_sha256 is None for page in graph.pages):
                raise failure('source_provenance_mode_changed',status='invalid_query')
            await self._issued_images(access,row,receipt,pages)
        partial = compile_model_stage(graph, document_id=row['source_id'], revision=1, pages=pages, chunks=chunks,poi_facts=poi_facts)
        graph = merge_stage(graph, partial)
        if payload.get('entity_candidates'):
            from .entity_graph import GraphBundle,validate_staged_bundle,digest as graph_digest
            bundle=GraphBundle.model_validate(payload['entity_candidates'])
            validate_staged_bundle(bundle,graph)
            if graph_digest(bundle.model_dump(mode='json')) not in {graph_digest(b.model_dump(mode='json')) for b in graph.entity_candidates}:
                graph=graph.model_copy(update={'entity_candidates':[*graph.entity_candidates,bundle]})
        if (len(poi_facts)>32 or len((payload.get('entity_candidates') or {}).get('entities',[]))>32
                or (not image_mode and (len(graph.entity_candidates)>32 or len(graph.poi_facts)>32))):
            raise failure('source_graph_stage_bound',status='invalid_query')
        if len(graph.pages)>MAX_PAGES or (not image_mode and len(graph.chunks)>MAX_PASSAGES):
            raise failure('small_source_stage_bound')
        # Image mode validates only the selected batch above. No implicit call
        # to book_pages/native_pages, including on resume or exact replay.
        proof = None if image_mode else await self._native_proof(access,row,receipt,graph)
        semantic = json.loads(row['semantic_json']) if row['semantic_json'] else {'candidates': {}, 'page_reviews': {}}
        for value in payload.get('story_candidates', []):
            candidate = StageStoryCandidateInput.model_validate(value)
            if candidate.entity_keys and self.import_revisions not in (DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS):
                raise failure('target_entity_resolution_unavailable')
            semantic['candidates'][candidate.candidate_key] = candidate.model_dump(mode='json')
        for page in pages:
            semantic['page_reviews'][page.page_id] = bool(page.knowledge_reviewed and page.story_candidates_reviewed)
        if not image_mode and len(semantic['candidates']) > 12:
            raise failure('small_source_semantic_bound')
        research = source_import.research if source_import is not None else None
        research_bytes = canonical(research.model_dump(mode='json')).decode() if research else row['research_json']
        if research is not None and research.state == 'pending':
            continuation = research.continuation
            args = continuation.get('arguments', {})
            if (continuation.get('tool') != 'book_pages' or type(args) is not dict
                    or args.get('ingestion_id') != row['ingestion_id']):
                raise failure('research_requires_reachable_owned_page_continuation', status='invalid_query')
        row = await asyncio.to_thread(self._save, row, stage_json=graph.model_dump_json(),
            research_json=research_bytes, phase='staging', native_proof_sha256=proof.sha256 if proof else None,
            semantic_json=canonical(semantic).decode())
        return self._output(row, 'staged', 'Model-authored source stage retained; validate complete source coverage', 'validate',
            source_archive_status='verified')

    async def _passages(self, access, row, receipt):
        if row['stage_json'] is None or row['research_json'] is None:
            raise failure('source_stage_and_model_authored_research_required', status='invalid_query')
        graph = StagedGraph.model_validate_json(row['stage_json'])
        if self._uses_issued_images(graph.pages):
            await self._issued_images(access,row,receipt,graph.pages)
            # source-manifest.v1 declares exact native-block offsets and PDF
            # <=32 pages. Never smuggle model text or a larger source through
            # its frozen accepted-command semantics. The native batch/activate
            # revision is a separate explicit capability, not an inferred pin.
            raise failure('source_image_activation_revision_required')
        await self._native_proof(access, row, receipt, graph)
        validation = validate_graph(graph, expected_page_count=receipt.page_count)
        if not validation.ready:
            raise failure('source_stage_requires_review', status='invalid_query')
        semantic = json.loads(row['semantic_json'])
        research = SourceResearchInput.model_validate_json(row['research_json'])
        if research.state == 'complete' and not all(semantic['page_reviews'].get(str(page.page_id)) for page in graph.pages):
            raise failure('complete_research_requires_explicit_same_pass_page_review', status='invalid_query')
        if not 1 <= len(graph.chunks) <= MAX_PASSAGES:
            raise failure('small_source_passage_bound', status='invalid_query')
        metadata = json.loads(row['metadata_json'])
        # Passage language selects the installed FTS configuration; the exact
        # bibliographic language remains unchanged in the durable manifest.
        source_language = (metadata.get('language') or '').lower().replace('_','-').split('-',1)[0]
        language = source_language if source_language in ('ru','de') else 'simple'
        pages = {str(page.page_id): page for page in graph.pages}
        passages = []
        token_counts = await self._counts([chunk.text for chunk in graph.chunks])
        for ordinal, (chunk, tokens) in enumerate(zip(graph.chunks, token_counts, strict=True)):
            if chunk.article_id or chunk.illustration_ids:
                raise failure('target_small_text_stage_only')
            body = chunk.text
            if not 1 <= tokens <= MAX_TOKENS:
                raise failure('complete_source_input_exceeds_512_tokens', status='invalid_query')
            locators = [pages[str(page)].printed_page_number or str(pages[str(page)].physical_page_index+1)
                for page in chunk.page_ids]
            passages.append(dict(passage_id=str(chunk.chunk_id), ordinal=ordinal,
                locator='pages '+', '.join(locators), body=body, language=language, input_tokens=tokens))
        if sum(len(value['body'].encode()) for value in passages) > MAX_BODY_BYTES:
            raise failure('small_source_body_bound', status='invalid_query')
        return graph, passages

    async def _counts(self, texts):
        try:
            return await self.tokenizer.counts(texts)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise failure('pinned_tokenizer_unavailable') from None

    async def _semantic(self, row, graph, passages):
        staged = json.loads(row['semantic_json'])
        regions = {str(region.region_id): region for page in graph.pages for region in page.regions}
        by_ref = {(str(region.page_id), region.region_key): region for region in regions.values()}
        result = dict(regions=[dict(region_id=key, page_id=str(region.page_id), source_text=region.source_text,
            source_text_sha256=digest(region.source_text.encode())) for key, region in regions.items()],
            findings=[], stories=[], pending=[])
        narrative_inputs = []
        candidates = [StageStoryCandidateInput.model_validate(raw) for raw in staged['candidates'].values()]
        variants = [candidate for candidate in candidates if candidate.basic_variant is not None]
        variant_counts = dict(zip((value.candidate_key for value in variants),
            await self._counts([value.basic_variant.body for value in variants]) if variants else [], strict=True))
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
            count = variant_counts[key]
            if not 1 <= count <= MAX_TOKENS:
                raise failure('complete_narrative_input_exceeds_512_tokens', status='invalid_query')
            variant_id = str(uuid5(UUID(row['source_id']), 'variant:'+key))
            result['stories'].append(dict(story_id=str(uuid5(UUID(row['source_id']), 'story:'+key)),
                finding_ids=[assertion_id], title=candidate.title, summary=candidate.summary,
                material_type=candidate.material_type, default_variant=dict(variant_id=variant_id, revision=1,
                    audience=variant.audience, format='story', language=variant.language, body=variant.body,
                    blocks=[dict(start=block.start, end=block.end, assertion_ids=[assertion_id]) for block in variant.blocks],
                    input_tokens=count)))
            narrative_inputs.append(dict(passage_id=variant_id, body=variant.body, input_tokens=count))
        if result['pending'] and SourceResearchInput.model_validate_json(row['research_json']).state != 'pending':
            raise failure('unfinished_narratives_require_explicit_research_continuation', status='invalid_query')
        if self.import_revisions in (DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS):
            from .source_graph import resolve_source_graph
            resolve_source_graph(graph,source_id=row['source_id'],person_id=row['person_id'],semantic=result)
        return result, narrative_inputs

    async def _validate(self, access, row):
        if self.native_intake is not None:return await self.native_intake.validate(access,row)
        receipt = await self._receipt(access, row)
        if receipt is None:
            return self._output(row, 'processing', 'Original verification pending', 'wait')
        graph, passages = await self._passages(access, row, receipt)
        await self._semantic(row, graph, passages)
        await self._check(access.authority, access.context, access.binding)
        row = await asyncio.to_thread(self._save, row, phase='validated')
        return self._output(row, 'ready', 'Complete source stage and exact tokenizer bounds validated; finalize to acquire document vectors',
            'finalize', source_archive_status='verified')

    async def _vectors(self, access, row, receipt, passages):
        await self.queue.shared_reads.register_document_actor(access.context)
        result = []
        enqueued = 0
        stage_hash = digest(row['stage_json'].encode())
        for passage in passages:
            text_hash = digest(passage['body'].encode())
            identity = dict(ingestion_id=row['ingestion_id'], source_id=row['source_id'], revision=1,
                passage_id=passage['passage_id'], text_sha256=text_hash, stage_sha256=stage_hash,
                original_sha256=receipt.sha256, original_receipt_sha256=receipt.receipt_sha256,
                parser_sha256=receipt.parser_sha256, model_revision=REVISION, space=SPACE)
            key = 'source-import-document:'+digest(canonical(identity))
            job = await asyncio.to_thread(self.queue.lookup, row['person_id'], key)
            if job is None:
                if await asyncio.to_thread(self.queue.document_pending) >= 64:
                    continue
                await self._check(access.authority, access.context, access.binding)
                await asyncio.to_thread(self.queue.enqueue, row['person_id'], key, [passage['body']],
                    kind='document', identity=identity)
                enqueued += 1
                continue
            if job['kind'] != 'document' or job['identity'] != identity:
                raise failure('source_document_job_binding_mismatch')
            if job['state'] != 'done':
                continue
            value = job['result']
            if value.get('space') != SPACE or len(value.get('vectors', ())) != 1:
                raise failure('source_document_vector_contract')
            raw = value['vectors'][0]
            if (type(raw) is not list or any(type(item) not in (int, float)
                    or struct.unpack('f', struct.pack('f', item))[0] != item for item in raw)):
                raise failure('source_document_job_requires_exact_fp32')
            vector = validate_vector(raw)
            result.append({**passage, 'embedding': vector})
        self.queue.shared_reads.wake()
        if enqueued:
            _LOG.info('source_import_documents ingestion_id=%s state=pending enqueued=%s total=%s',
                row['ingestion_id'],enqueued,len(passages))
        return result if len(result) == len(passages) else None

    def _revisions(self, binding):
        with self.queue.connect(write=False) as db:
            runtime = self.queue._runtime_row(db, binding.run_id)
            expected = self.queue._runtime_expected(runtime)
            if (runtime['accepted_generation'] != binding.connection_generation
                    or expected.database_generation != binding.database_generation):
                raise failure('target_runtime_changed')
            return self.import_revisions

    def _intake_coverage_current(self,binding):
        # New intake needs the actual startup capability installed by native
        # genesis/history verification, rather than READY/writer status alone.
        return (self.startup.imports_current() and self.startup.binding==binding
            and self.startup.coverage is self.spool._source_anchor)

    async def _stage_command(self, access, row, payload):
        # Same canonical intake lock as source history/genesis. Reject unresolved
        # predecessors rather than inventing a competing sequence allocator.
        async with self.spool._lock:
            entry = await self._entry(row)
            if entry is not None:
                return entry
            await self._check(access.authority, access.context, access.binding)
            scope = await self.spool.scope_state()
            if scope['pending'] or scope['unknown'] or scope['sequence'] != scope['accepted_sequence']:
                raise failure('prior_write_pending')
            head = await self.spool.journal.head()
            sequence = head.reference.last_sequence if head else 0
            if sequence != scope['accepted_sequence']:
                raise failure('source_history_unavailable')
            command = self.import_command(sequence=sequence+1, operation_key=self._operation(row),
                request_id=row['request_id'], person_id=row['person_id'],
                logical_time=datetime.now(timezone.utc).isoformat(), revisions=self._revisions(access.binding),
                source_id=row['source_id'], payload=payload)
            if len(command.payload) > MAX_IMPORT_BYTES:
                raise failure('small_source_resolved_payload_bound')
            frozen, data, handler = self.spool._validate(command, access.binding.database_generation)
            await self._check(access.authority, access.context, access.binding)
            facts=await self.spool.read_facts(None)
            if (not self._intake_coverage_current(access.binding)
                    or facts.database_generation!=access.binding.database_generation
                    or facts.scope_sequence>facts.covered_sequence or facts.pending or facts.unknown
                    or not self.spool.facts_current(facts)):
                raise failure('source_history_unavailable')
            # Same intake lock as genesis/history, with no await between this
            # synchronous capability fence and the new write's invalidation.
            entry = await self.spool._write(self.spool._stage, frozen, data, handler, access.binding.database_generation)
            _LOG.info('source_import_command ingestion_id=%s state=staged route=%s sequence=%s command_sha256=%s',
                row['ingestion_id'],entry.command.route,entry.command.sequence,entry.command_sha256)
            return entry

    async def _finalize(self, access, row):
        if self.native_intake is not None:return await self.native_intake.finalize(access,row)
        entry = await self._entry(row)
        if entry is None:
            if not self._intake_coverage_current(access.binding):
                raise failure('source_history_unavailable')
            if row['phase'] not in ('validated', 'embedding'):
                raise failure('validate_before_finalize', status='invalid_query')
            receipt = await self._receipt(access, row)
            if receipt is None:
                return self._output(row, 'processing', 'Original verification pending', 'wait')
            graph, passages = await self._passages(access, row, receipt)
            semantic, narratives = await self._semantic(row, graph, passages)
            resolved = await self._vectors(access, row, receipt, passages+narratives)
            row = await asyncio.to_thread(self._save, row, phase='embedding')
            if resolved is None:
                return self._output(row, 'processing', 'Exact Kaggle document vectors pending', 'wait',
                    source_archive_status='verified')
            vectors = {value['passage_id']: value['embedding'] for value in resolved}
            for story in semantic['stories']:
                variant = story['default_variant']
                variant['embedding'] = vectors[variant['variant_id']]
            metadata = json.loads(row['metadata_json'])
            rights = PublicImportRights.model_validate_json(row['rights_json'])
            research = SourceResearchInput.model_validate_json(row['research_json'])
            bibliography = '; '.join(filter(None, [', '.join(metadata.get('authors') or []), metadata['title'],
                str(metadata['publication_year']) if metadata.get('publication_year') else None]))
            payload = dict(codec='rkb.source-import.v2', bibliography=bibliography, revision=1,
                work_id=str(uuid5(UUID(row['source_id']), 'work')), edition_id=str(uuid5(UUID(row['source_id']), 'edition')),
                original=receipt.original, model=dict(revision=REVISION, space=SPACE, tokenizer_sha256=TOKENIZER_SHA256,
                    dimension=DIMENSION, max_tokens=MAX_TOKENS), passages=resolved[:len(passages)], semantic=semantic,
                research=dict(frontier_id=str(uuid5(UUID(row['ingestion_id']), 'research')),
                    **research.model_dump(mode='json')))
            if self.import_revisions in (EDITORIAL_REVISIONS,DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS):
                from .contracts import StartMetadataInput
                from .source_manifest import build_source_manifest
                proof=await self._native_proof(access,row,receipt,graph)
                payload['codec']='rkb.source-import.v4'
                payload['source_manifest']=build_source_manifest(metadata=StartMetadataInput.model_validate(metadata),
                    receipt=receipt,proof=proof,graph=graph,semantic=semantic,passages=payload['passages'])
            if self.import_revisions in (DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS) and (graph.entity_candidates or graph.poi_facts
                    or any(finding['entity_keys'] for finding in semantic['findings'])):
                from .source_graph import resolve_source_graph
                payload['source_graph']=resolve_source_graph(graph,source_id=row['source_id'],person_id=row['person_id'],semantic=semantic)
            entry = await self._stage_command(access, row, payload)
            row = await asyncio.to_thread(self._save, row, phase='accepted', frozen_sha256=entry.command_sha256)
        await self._check(access.authority, access.context, access.binding)
        if entry.accepted is None:
            head = await self.spool.journal.head()
            if (head.reference.last_sequence if head else 0) != entry.command.sequence-1:
                # SENT_UNKNOWN is reconciled from the original stored predecessor
                # by seal; never replace it with a newly sampled head.
                if entry.seal_state != 'SENT_UNKNOWN':
                    raise failure('prior_write_pending')
                await self.spool.seal(access.context, entry.command.operation_key,
                    writer_generation=self.startup.config.writer_generation)
            else:
                await self.spool.seal(access.context, entry.command.operation_key, predecessor=head,
                    writer_generation=self.startup.config.writer_generation)
        entry = await self.spool.get(entry.command.operation_key)
        if entry.outcome_state == 'DURABLE':
            state = 'finalized' if entry.apply_receipt.status == 'applied' else 'failed'
            row = await asyncio.to_thread(self._save, row, phase=state)
            _LOG.info('source_import_terminal ingestion_id=%s state=%s command_sha256=%s',
                row['ingestion_id'], state, entry.command_sha256)
            accepted = json.loads(entry.command.payload)
            research = accepted['research']
            semantic = accepted['semantic']
            staged = json.loads(row['semantic_json'])
            reviewed = sum(staged['page_reviews'].values())
            total = len(StagedGraph.model_validate_json(row['stage_json']).pages)
            pending = len(semantic['pending'])
            progress = {}
            if state == 'finalized':
                progress = dict(knowledge_extraction=KnowledgeExtractionStatus(
                    state='awaiting_agent' if research['state']=='pending' else 'pass_completed',
                    reviewed_pages=reviewed,total_pages=total,needs_review_candidates=pending,
                    staged_candidates=len(semantic['findings']),
                    pending_candidates=[dict(kind='source_research',continuation=research['continuation'])]
                        if research['state']=='pending' else [],
                    follow_up=['Source activation does not establish retrieval acceptance.'],
                    complete_extraction_claimed=False),story_extraction=StoryExtractionStatus(
                    state='awaiting_agent' if pending else 'completed' if reviewed==total else 'reviewing',
                    reviewed_pages=reviewed,total_pages=total,staged_candidates=len(semantic['findings']),
                    saved_candidates=len(semantic['findings']),needs_review_candidates=pending,
                    coverage_note='Model-authored findings and supplied narratives activated atomically; no exhaustive recall claim.'))
            return self._output(row, state, 'Applied outcome remotely durable' if state == 'finalized' else
                'Source import precondition rejected; no source activation', 'done' if state == 'finalized' else 'blocker',
                source_archive_status='verified', warnings=([] if self.startup.reads_current() else ['source_history_refresh_pending'])
                    + (['model_authored_research_pending'] if research['state']=='pending' else []),**progress)
        await self.startup.source_dispatch.enqueue(access.context, entry.command.operation_key,
            writer_generation=self.startup.config.writer_generation)
        return self._output(row, 'processing', 'Accepted source command pending native apply and durable remote outcome', 'wait',
            source_archive_status='verified')

    async def _native_reuse_or_resume(self, access, row, command):
        """Reuse the *owned* archived original; never create a second book.

        The current SQL013/014 native profile installs only source revision 1.
        It would be unsafe to mint another source_id, restart ingestion, or
        claim a revised source Saved while those exact functions still enforce
        absent_source/revision=1. Existing in-flight commands may be resumed
        through the same NativeSourceIntake, not replayed with fresh IDs.
        """
        native = getattr(self, 'native_intake', None)
        if native is None:
            raise failure('native_reuse_requires_installed_source_stage')
        from .source_activation import ACTIVATION_REVISIONS
        if self.import_revisions != ACTIVATION_REVISIONS:
            raise failure('native_reuse_requires_installed_source_stage')
        await self._check(access.authority, access.context, access.binding)
        receipt = await self._receipt(access, row)
        if receipt is None:
            raise failure('verified_archived_original_required')
        if (receipt.source_id, receipt.ingestion_id) != (row['source_id'], row['ingestion_id']):
            raise failure('archived_source_identity_changed')

        # This is a reader of the EXISTING source/control state, not a second
        # ingest lease or alternative corpus. A different active source cannot
        # be bypassed by invoking reprocess instead of start.
        def another_active():
            with self.queue.connect(write=False) as db:
                return db.execute('''SELECT 1 FROM rkb_source_intake_control
                    WHERE person_id=? AND ingestion_id<>? AND phase NOT IN ('finalized','failed')
                    LIMIT 1''',(row['person_id'],row['ingestion_id'])).fetchone() is not None
        if await asyncio.to_thread(another_active):
            raise failure('another_native_ingestion_active')
        state = await asyncio.to_thread(native._state, row)
        if state is None:
            raise failure('native_source_stage_state_unavailable')
        if state['phase'] != 'finalized' or row['phase'] != 'finalized':
            if command == 'rechunk':
                raise failure('rechunk_requires_finalized_source', status='invalid_query')
            if state['phase'] == 'failed' or row['phase'] == 'failed':
                raise failure('native_source_terminal_failure')
            # This resumes a preexisting original intent, including UNKNOWN
            # reconciliation. It never calls originals.begin or mints a
            # replacement revision, command key, source ID, or actor lease.
            result = await native.status(access, row)
            await self._check(access.authority, access.context, access.binding)
            if result.state != 'finalized':
                return result.model_copy(update={'message':
                    'Existing owner-native intake resumed under the original operation; no second revision was started. '
                    + result.message})
            # A concurrent terminal transition is not a new reprocess result.
            row = await asyncio.to_thread(self._row, row['person_id'], row['ingestion_id'])
            state = await asyncio.to_thread(native._state, row)

        # Finalized local state alone is NOT Saved. Require the original
        # applied source.activate outcome, exact replay identity and the
        # independently verified Dataset-covered remote outcome bytes.
        if (row['phase'] != 'finalized' or state is None or state['phase'] != 'finalized'
                or state['pending_operation_key'] is not None or state['pending_kind'] is not None):
            raise failure('native_source_terminal_intent_unverified')
        key = native._operation(row, 'activate')
        entry = await native._entry(key)
        from .recovery_journal import RemoteOutcomeReceipt
        if (entry is None or entry.command.operation_key != key
                or entry.command.route != 'source.activate.v1'
                or entry.command.person_id != row['person_id']
                or entry.command.ids != {'source_id': row['source_id']}
                or entry.database_generation != access.binding.database_generation
                or entry.accepted is None or entry.apply_receipt is None
                or entry.apply_receipt.status != 'applied' or entry.outcome_state != 'DURABLE'
                or type(entry.remote_outcome) is not RemoteOutcomeReceipt
                or (entry.remote_outcome.operation_key, entry.remote_outcome.command_sha256,
                    entry.remote_outcome.database_generation) !=
                   (key, entry.command_sha256, access.binding.database_generation)):
            raise failure('native_source_accepted_outcome_not_saved')
        from .dataset_recovery_store import DatasetRecoveryStore
        store = self.spool.journal.store
        if type(store) is not DatasetRecoveryStore:
            raise failure('native_source_dataset_coverage_required')
        published = await store.read(entry.remote_outcome.key,
            max_bytes=self.spool.journal.max_outcome_bytes)
        if (published is None or len(published.data) != entry.remote_outcome.length
                or not store.remote_durable(entry.remote_outcome.key, published.data)):
            raise failure('native_source_dataset_coverage_required')
        try:
            import base64
            saved = json.loads(published.data)
            if (canonical(saved) != published.data or saved['codec'] != 'rkb.outcome.v1'
                    or saved['stream'] != self.spool.journal.stream
                    or saved['operation_key'] != key
                    or saved['command_sha256'] != entry.command_sha256
                    or saved['sequence'] != entry.command.sequence
                    or saved['database_generation'] != access.binding.database_generation
                    or saved['outcome_sha256'] != entry.remote_outcome.outcome_sha256
                    or base64.b64decode(saved['outcome_b64'], validate=True) != entry.apply_receipt.outcome
                    or digest(entry.apply_receipt.outcome) != saved['outcome_sha256']):
                raise ValueError
        except (ValueError, KeyError, TypeError, UnicodeError):
            raise failure('native_source_saved_outcome_changed') from None
        await self._check(access.authority, access.context, access.binding)
        # Source revision 2 and accepted_reuse re-chunk require a NEW ordered
        # native SQL/activation contract. SQL013/014 explicitly disallow
        # source_id already present in rkb_v4.sources. A source-only importer
        # patch must not fake that capability or mutate immutable old pages.
        raise failure('native_source_revision_upgrade_required')

    async def book_ingest(self, *, command, principal, file=None, ingestion_id=None,
                          cursor=None, payload=None, document_id=None, source_import=None, stage_file_sha256=None):
        if command not in ('start', 'reprocess', 'rechunk', 'stage', 'validate', 'finalize', 'status'):
            raise failure('target_import_command_unavailable')
        if cursor is not None:
            raise failure('ingest_stage_cursor_unavailable', status='invalid_query')
        if source_import is not None and type(source_import) is not TargetSourceImportInput:
            raise failure('invalid_source_import_input', status='invalid_query')
        try:
            if source_import is not None:
                source_import = TargetSourceImportInput.model_validate_json(source_import.model_dump_json())
                if command not in ('start', 'stage'):
                    raise failure('source_import_only_supported_for_start_or_stage', status='invalid_query')
            async with self._access(principal) as access:
                async with self._lock:
                    if command == 'start':
                        if ingestion_id or document_id:
                            raise failure('fresh_source_identity_is_server_owned', status='invalid_query')
                        output = await self._start(access, file=file, metadata=payload or {}, source_import=source_import)
                    else:
                        if file is not None:
                            raise failure('file_only_supported_for_original_start_or_stage_json', status='invalid_query')
                        row = await asyncio.to_thread(self._row, access.authority.person_id, ingestion_id, document_id)
                        if command in ('reprocess', 'rechunk'):
                            if payload is not None or stage_file_sha256 is not None:
                                raise failure('native_reuse_cannot_replace_accepted_input', status='invalid_query')
                            output = await self._native_reuse_or_resume(access, row, command)
                        elif command == 'stage':
                            output = await self._stage(access, row, payload, source_import)
                        elif command == 'validate':
                            output = await self._validate(access, row)
                        elif self.native_intake is not None and command=='status':
                            output=await self.native_intake.status(access,row)
                        elif command == 'finalize' or await self._entry(row) is not None or row['phase'] == 'embedding':
                            output = await self._finalize(access, row)
                        elif row['phase']=='validated':
                            output = self._output(row,'ready','Validated source stage retained; finalize to acquire document vectors',
                                'finalize',source_archive_status='verified')
                        else:
                            status = await self.originals.status(access.context, row['ingestion_id'])
                            receipt = await self._receipt(access, row)
                            if receipt:
                                output = self._output(row,'staged','Review actual original pages and stage source-backed chunks',
                                    'continue_pages',next_cursor='0',source_archive_status='verified')
                            else:
                                output = await self._original_pending(row,status)
                if (command == 'status' and getattr(self, 'native_intake', None) is not None
                        and output.state == 'finalized'):
                    output = await self._native_research_status(access, row, output)
                if stage_file_sha256 is not None:
                    if (command != 'stage' or type(stage_file_sha256) is not str or len(stage_file_sha256) != 64
                            or set(stage_file_sha256) - set('0123456789abcdef')):
                        raise failure('invalid_stage_file_digest', status='invalid_query')
                    output = output.model_copy(update={'message': output.message+'; stage_file_sha256='+stage_file_sha256})
                return await self._reply(access, output)
        except ToolError:
            raise
        except PermissionError:
            raise failure('access_denied', status='denied') from None
        except OriginalIntakeError:
            raise failure('native_original_unavailable') from None
        except (ValueError, KeyError, TypeError):
            # Never include source text, URLs or validation input in diagnostics.
            raise failure('invalid_small_source_input', status='invalid_query') from None

    async def book_pages(self, *, principal, ingestion_id, cursor=None, batch_size=1):
        try:
            return await self._pages(principal=principal,ingestion_id=ingestion_id,cursor=cursor)
        except ToolError:
            raise
        except PermissionError:
            raise failure('access_denied',status='denied') from None
        except OriginalIntakeError:
            raise failure('native_original_page_unavailable') from None
        except (ValueError,KeyError,TypeError):
            raise failure('invalid_original_page_request',status='invalid_query') from None

    async def _pages(self, *, principal, ingestion_id, cursor):
        async with self._access(principal) as access:
            row = await asyncio.to_thread(self._row, access.authority.person_id, ingestion_id)
            if await self._receipt(access, row) is None:
                raise failure('verified_original_pending')
            batch = await self.originals.pages(access.context, row['ingestion_id'], cursor=cursor, batch_size=1)
            if len(batch.pages) != 1 or sum(len(page.data) for page in batch.pages) > 256 * 1024:
                raise failure('bounded_original_page_output_required')
            for page in batch.pages:
                if (not 0 <= page.physical_page_index < MAX_PAGES
                        or page.page_id != str(uuid5(UUID(row['source_id']), f'page:1:{page.physical_page_index}'))
                        or page.mime_type != 'image/jpeg'):
                    raise failure('native_original_page_binding_mismatch')
            from .server import _book_page_tool_result
            return await self._reply(access, batch, render=lambda value: _book_page_tool_result(ingestion_id, value))
