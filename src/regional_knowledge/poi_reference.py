"""Read-only adapter to Street Story's owned POI identity/alias contract.

No claims, books or bearer tokens cross this boundary. Deployment explicitly
selects the local canonical identity projection; there is no POI write/create.
"""
import os,sqlite3
from pathlib import Path
from .entity_graph import normalize_alias,digest

class StreetStoryPoiResolver:
    def __init__(self,path=None):self.path=path or os.getenv('RKB_STREET_STORY_POI_DB','')
    def _connect(self):
        if not self.path:raise RuntimeError('poi_resolver_unavailable')
        p=Path(self.path).resolve()
        return sqlite3.connect(p.as_uri()+'?mode=ro',uri=True,timeout=2)
    def resolve(self,locator):
        names={normalize_alias(n) for n in locator.names};external={(k,str(v).casefold()) for k,v in locator.external_ids.items()}
        with self._connect() as db:
            pois=db.execute("select id,canonical_name,status from pois where status!='merged'").fetchall()
            aliases=db.execute("select poi_id,namespace,value,normalized_value from poi_aliases").fetchall()
        hits={pid for pid,label,status in pois if normalize_alias(label) in names}
        external_hits={pid for pid,ns,value,norm in aliases if (ns,value.casefold()) in external}
        hits|={pid for pid,ns,value,norm in aliases if ns=='name' and normalize_alias(value) in names}
        if external_hits:
            if hits and not external_hits.issubset(hits):return {'state':'ambiguous','external_ref':None}
            hits=external_hits
        if len(hits)!=1:return {'state':'ambiguous' if hits else 'unresolved','external_ref':None}
        pid=next(iter(hits));return {'state':'resolved','external_ref':'streetstory://poi/'+pid}
    def version(self,external_ref):
        from uuid import UUID
        pid=str(UUID(external_ref.removeprefix('streetstory://poi/')))
        with self._connect() as db:
            row=db.execute("select canonical_name,status from pois where id=?",(pid,)).fetchone()
            if not row or row[1]=='merged':return None
            names=[row[0],*[r[0] for r in db.execute("select value from poi_aliases where poi_id=? and namespace='name' order by normalized_value",(pid,))]]
        names=list(dict.fromkeys(names))[:20]
        return {'names':names,'version':digest(names)}
