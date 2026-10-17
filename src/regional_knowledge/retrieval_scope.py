"""Resolve catalog articles to existing ACL/revision authority without widening scope."""
import json


def resolve_scope(corpus, requested, allowed):
    whole=[]; components=[]; parents={}
    with corpus.connect() as db:
        for ident in requested:
            if ident in allowed:
                whole.append(ident);parents[ident]=allowed[ident]
                continue
            found=db.execute('SELECT payload FROM catalog_components WHERE id=?',(ident,)).fetchone()
            row=json.loads(found[0]) if found else None
            if (not row or row['parent_id'] not in allowed
                    or row['active_revision'] != allowed[row['parent_id']]
                    or (row.get('catalog') or {}).get('searchable',True) is False):
                raise PermissionError('document scope not readable or searchable')
            components.append(ident);parents[row['parent_id']]=allowed[row['parent_id']]
    return parents,(whole,components)


def scope_clause(scope):
    """Internal SQL alias t is fixed; all user IDs are bound parameters."""
    if scope is None:
        return '1',[]
    whole,components=scope
    clauses=[];params=[]
    if whole:
        clauses.append('t.document_id IN ('+','.join('?' for _ in whole)+')')
        params.extend(whole)
    if components:
        clauses.append('t.chunk_id IN (SELECT chunk_id FROM component_chunks WHERE component_id IN ('+','.join('?' for _ in components)+'))')
        params.extend(components)
    return '('+' OR '.join(clauses)+')' if clauses else '0',params
