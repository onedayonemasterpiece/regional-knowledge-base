"""Inline source metadata for the existing book -> Story Registry transaction.

The importing model selects dates, quantities and graph identities. This module
only resolves exact references and applies existing validated editorial commands.
One invalid enrichment is withheld; its useful source-backed story survives.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import logging
import sqlite3
from uuid import UUID

from .entity_graph import entity_id
from .story_contracts import LinkEntity, RecordEventDate, RecordObservation
from .story_registry import StoryError, digest

log = logging.getLogger(__name__)


def resolve_entity_keys(graph, document_id, keys):
    """Resolve only explicit GraphBundle keys; identical spelling selects nothing."""
    if not keys:
        return [], []
    index, ambiguous = {}, set()
    for bundle in graph.entity_candidates:
        entries = [(n.key, str(n.entity_id or entity_id(UUID(document_id), n.key)))
                   for n in bundle.entities]
        entries.extend((key, str(value)) for key, value in bundle.entity_refs.items())
        for key, ident in entries:
            if key in index and index[key] != ident:
                ambiguous.add(key)
            index[key] = ident
    links, issues = [], []
    for key in dict.fromkeys(keys):
        if key not in index or key in ambiguous:
            issues.append({'kind': 'entity', 'key': key,
                           'reason': 'ambiguous_graph_key' if key in ambiguous else 'unknown_graph_key'})
        else:
            links.append({'key': key, 'entity_id': index[key]})
    return links, issues


def _citation_ids(registry, db, claim, document_id, data, indexes):
    if not indexes or len(indexes) != len(set(indexes)):
        raise ValueError('unique evidence indexes required')
    found = []
    for index in indexes:
        proof = data['evidence'][index]
        # A byte-identical source reimport may reuse an earlier immutable quote.
        # Same source SHA/fingerprint was already checked by activate_stories.
        rows = db.execute("""SELECT id,locator FROM story_evidence
            WHERE assertion_id=? AND assertion_revision=? AND source_kind='document'
              AND source_id=? AND original_excerpt=?
              AND json_extract(locator,'$.physical_page_index')=?
              AND json_extract(locator,'$.start')=? AND json_extract(locator,'$.end')=?
            ORDER BY source_revision DESC,id LIMIT 8""",
            (claim['assertion_id'], claim['revision'], document_id,
             proof['original_excerpt'], proof['physical_page_index'], proof['start'], proof['end'])).fetchall()
        match = None
        for row in rows:
            region = registry._corpus_row(db, 'rkb_regions', json.loads(row['locator']).get('region_id'))
            if (row['id'] in claim['evidence_ids'] and region and
                    hashlib.sha256((region.get('source_text') or '').encode('utf8')).hexdigest()
                    == proof['source_region_sha256']):
                match = row['id']
                break
        if match is None:
            raise ValueError('candidate evidence is not on the exact current assertion source region')
        found.append(match)
    return list(dict.fromkeys(found))


def apply_inline_enrichment(registry, db, principal, snap, claim, document_id, data):
    """Bounded per-item savepoints, no network/model calls under the write lock."""
    issues = list(data.get('enrichment_issues') or [])
    changed = False
    for field, cls, operation, table, original_field, supersedes in (
        ('event_dates', RecordEventDate, 'record_event_date', 'story_event_dates',
         'original_date_text', 'supersedes_date_id'),
        ('observations', RecordObservation, 'record_observation', 'story_observations',
         'original_value_text', 'supersedes_observation_id'),
    ):
        for index, raw in enumerate(data.get(field) or []):
            before = deepcopy(snap)
            db.execute('SAVEPOINT inline_enrichment')
            try:
                positions = raw.get('evidence_indexes', [0])
                citations = _citation_ids(registry, db, claim, document_id, data, positions)
                # Do not let a neighbouring number/date acquire an unrelated citation.
                if not any(raw[original_field] in data['evidence'][i]['original_excerpt']
                           for i in positions):
                    raise ValueError('literal metadata absent from selected source evidence')
                values = {k: v for k, v in raw.items() if k != 'evidence_indexes'}
                op = cls(op=operation, assertion_id=claim['assertion_id'],
                         assertion_revision=claim['revision'], evidence_ids=citations, **values)
                fingerprint = digest(op.model_dump(mode='json', exclude={'op', supersedes, 'rationale'}))
                exists = db.execute(f"""SELECT 1 FROM {table}
                    WHERE assertion_id=? AND assertion_revision=? AND fingerprint=?""",
                    (claim['assertion_id'], claim['revision'], fingerprint)).fetchone()
                if not exists:
                    registry._apply_op(db, principal, snap, op)
                    changed = True
                db.execute('RELEASE inline_enrichment')
            except (ValueError, KeyError, IndexError, StoryError, sqlite3.IntegrityError):
                db.execute('ROLLBACK TO inline_enrichment')
                db.execute('RELEASE inline_enrichment')
                snap.clear()
                snap.update(before)
                issues.append({'kind': field, 'index': index, 'reason': 'source_proof_or_identity_mismatch'})
    for link in data.get('entity_links') or []:
        before = deepcopy(snap)
        db.execute('SAVEPOINT inline_entity_link')
        try:
            node = registry._corpus_row(db, 'rkb_entities', link['entity_id'])
            if not node or str(node.get('owner_user_id')) != str(principal.subject):
                raise ValueError('owned source entity required')
            source = registry._corpus_row(db, 'rkb_documents', node['document_id'])
            if not source or int(source.get('active_revision') or 0) != int(node['revision']):
                raise ValueError('active source entity required')
            kinds = {'poi_ref': 'place', 'historical_thread': 'thread',
                     'person': 'person', 'organization': 'organization', 'event': 'event'}
            kind = kinds[node['kind']]
            if not any(x['entity_ref'] == link['entity_id'] for x in snap['links']):
                registry._apply_op(db, principal, snap, LinkEntity(
                    op='link_entity', entity_ref=link['entity_id'], kind=kind))
                changed = True
            db.execute('RELEASE inline_entity_link')
        except (ValueError, KeyError, StoryError, sqlite3.IntegrityError):
            db.execute('ROLLBACK TO inline_entity_link')
            db.execute('RELEASE inline_entity_link')
            snap.clear()
            snap.update(before)
            issues.append({'kind': 'entity', 'key': link['key'], 'reason': 'owned_active_entity_required'})
    if issues:
        log.warning(json.dumps({'event': 'ingestion_enrichment_needs_review',
                                'story_id': snap['story_id'], 'items': len(issues)}))
    return changed, issues


def knowledge_status(corpus, row):
    """Read one indexed ingestion checkpoint, not every book or full source graph."""
    document = corpus.one('rkb_documents', str(row.get('document_id')))
    if not document:
        return None
    total = int(document.get('page_count') or 0)
    with corpus.connect() as db:
        rows = db.execute("""SELECT physical_page_index FROM story_ingest_coverage
            WHERE ingestion_id=? AND source_revision=? AND knowledge_reviewed=1
            ORDER BY physical_page_index""", (str(row['id']), int(row['staged_revision']))).fetchall()
        reviewed = {r[0] for r in rows if 0 <= r[0] < total}
        pending = db.execute("""SELECT candidate_key,story_id,payload,state FROM story_ingest_candidates
            WHERE ingestion_id=? AND state IN ('needs_review','staged') ORDER BY candidate_key LIMIT 10""",
            (str(row['id']),)).fetchall()
        pending_count = db.execute("""SELECT count(*) FROM story_ingest_candidates
            WHERE ingestion_id=? AND state='needs_review'""", (str(row['id']),)).fetchone()[0]
        staged_count = db.execute("""SELECT count(*) FROM story_ingest_candidates
            WHERE ingestion_id=? AND state='staged'""", (str(row['id']),)).fetchone()[0]
    next_page = next((i for i in range(total) if i not in reviewed), None)
    active = int(document.get('active_revision') or 0) == int(row['staged_revision'])
    complete = total > 0 and next_page is None and not pending_count and not staged_count
    state = ('pass_completed' if active and complete else 'awaiting_agent' if active
             else 'reviewing' if reviewed else 'not_reviewed')
    follow_up = []
    if next_page is not None:
        follow_up.append('For a pending import continue book_pages/stage; for an accepted legacy source use corpus_read/story_extract/graph_stage or explicit reprocess. Never claim old pages reviewed automatically.')
    if pending_count or (active and staged_count):
        follow_up.append('Resubmit corrected metadata-only book_ingest(stage, story_candidates, entity_candidates) to apply and clear its checkpoint without changing the source.')
    if active:
        follow_up.extend(['Continue story_reconcile(next) for accepted episodes.',
                          'Use poi_registry for model-selected physical sites; no map or POI is required for source activation.',
                          'Run source-grounded retrieval checks on this exact active revision; vector readiness is not retrieval acceptance.'])
    return {'state': state, 'reviewed_pages': len(reviewed), 'total_pages': total,
            'next_page_index': next_page, 'needs_review_candidates': pending_count,
            'staged_candidates': staged_count,
            'pending_candidates': [{'candidate_key': r['candidate_key'], 'story_id': r['story_id'], 'state': r['state'],
                'issues': (json.loads(r['payload']).get('enrichment_issues') or [])[:8]}
                for r in pending], 'follow_up': follow_up,
            'complete_extraction_claimed': False, 'retrieval_acceptance': 'not_evaluated'}
