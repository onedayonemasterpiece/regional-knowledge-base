"""Explicit ranking fusion with inspectable independent signals."""
def fuse(rows, branches, *, limit=20):
    branches=set(branches);scores={};diagnostics={};seen=set()
    for row in rows:
        branch=row['branch'];key=(str(row['chunk_id']),branch)
        if branch not in branches or key in seen:continue
        seen.add(key);chunk=key[0];rank=int(row['rank'])
        if rank<1:raise ValueError('invalid retrieval rank')
        scores[chunk]=scores.get(chunk,0)+1/(60+rank)
        diagnostics.setdefault(chunk,[]).append({'branch':branch,'rank':rank,'matched_alias':row.get('matched_alias')})
    ranked=sorted(scores,key=lambda chunk:(-scores[chunk],chunk))[:limit]
    return ranked,{chunk:diagnostics[chunk] for chunk in ranked}

MODES={'lexical':('lexical',),'e5':('e5',),'bge':('bge',),'e5_lexical':('e5','lexical'),'bge_lexical':('bge','lexical'),'e5_bge':('e5','bge'),'e5_bge_lexical':('e5','bge','lexical')}
