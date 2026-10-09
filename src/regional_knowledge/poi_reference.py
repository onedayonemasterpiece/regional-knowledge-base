"""Read-only adapter to Street Story's owned canonical POI/alias projection.

Street Story owns real POI keys, including legacy UUID and the actual
`poi_ss_<24 hex>` deterministic key family. RKB stores only references,
not duplicated place identity, map geometry or evidence ownership.
A single owner match is a *candidate*, not historical/geometric proof.
"""
from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from uuid import UUID

from .entity_graph import normalize_alias, digest

# Deployed Street Story currently has BOTH key families. Never coerce or
# generate another UUID inside RKB for a poi_ss identity.
_OPAQUE_POI = re.compile(r"poi_ss_[0-9a-f]{24}\Z")


def canonical_poi_key(external_ref: str) -> str:
    if not isinstance(external_ref, str) or not external_ref.startswith(
        "streetstory://poi/"
    ):
        raise ValueError("canonical Street Story POI reference required")
    suffix = external_ref[len("streetstory://poi/"):]
    if _OPAQUE_POI.fullmatch(suffix):
        return suffix
    try:
        parsed = UUID(suffix)
    except (TypeError, ValueError, AttributeError):
        raise ValueError("Invalid canonical Street Story POI key") from None
    if str(parsed) != suffix:
        raise ValueError("Noncanonical UUID spelling is not allowed")
    return suffix


class StreetStoryPoiResolver:
    def __init__(self, path=None):
        self.path = path or os.getenv("RKB_STREET_STORY_POI_DB", "")

    def _connect(self):
        if not self.path:
            raise RuntimeError("poi_resolver_unavailable")
        path = Path(self.path).resolve()
        return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)

    def resolve(self, locator):
        # Alias and exact external-reference matches are candidates. Identity
        # is not proved by an identical name or nearest centroid alone.
        names = {normalize_alias(str(n)) for n in locator.names}
        external = {
            (str(k).lower(), str(v).casefold())
            for k, v in locator.external_ids.items()
        }
        with self._connect() as db:
            pois = db.execute(
                "SELECT id,canonical_name,status FROM pois WHERE status!='merged'"
            ).fetchall()
            aliases = db.execute(
                "SELECT poi_id,namespace,value,normalized_value FROM poi_aliases"
            ).fetchall()
        active = {str(p[0]) for p in pois}
        name_hits = {
            str(pid) for pid, label, status in pois
            if normalize_alias(label) in names
        }
        name_hits |= {
            str(pid) for pid,ns,value,norm in aliases
            if ns == "name" and normalize_alias(value) in names and str(pid) in active
        }
        external_hits = {
            str(pid) for pid,ns,value,norm in aliases
            if (str(ns).lower(), str(value).casefold()) in external
            and str(pid) in active
        }
        if external_hits:
            if name_hits and not external_hits.issubset(name_hits):
                return {"state":"ambiguous","external_ref":None}
            hits = external_hits
        else:
            # Explicit supplied external ID that does not exist cannot silently
            # be replaced with a similar place name.
            if external:
                return {"state":"unresolved","external_ref":None}
            hits = name_hits
        if len(hits) != 1:
            return {"state":"ambiguous" if hits else "unresolved","external_ref":None}
        pid = next(iter(hits))
        try:
            canonical_poi_key("streetstory://poi/" + pid)
        except ValueError:
            return {"state":"unresolved_invalid_canonical_key","external_ref":None}
        # "resolved" means unique identity catalog *candidate* match only.
        # Higher-level callers must carry the POI status and historical scope.
        return {"state":"resolved","external_ref":"streetstory://poi/"+pid}

    def version(self, external_ref):
        pid = canonical_poi_key(external_ref)
        with self._connect() as db:
            row = db.execute(
                "SELECT id,canonical_name,status,latitude,longitude FROM pois WHERE id=?",
                (pid,),
            ).fetchone()
            if not row or row[2] == "merged":
                return None
            names = [row[1], *[
                r[0] for r in db.execute(
                    "SELECT value FROM poi_aliases WHERE poi_id=? AND namespace='name' "
                    "ORDER BY normalized_value LIMIT 40", (pid,)
                )
            ]]
        names = list(dict.fromkeys(names))[:20]
        position = (
            {"latitude":row[3],"longitude":row[4],
             "type":"representative_point_not_historical_geometry"}
            if row[3] is not None and row[4] is not None else None
        )
        return {
            "names":names,"version":digest({
                "names":names,"key":pid,"status":row[2],
                "representative_position":position,
            }),
            "identity_state":row[2],
            "external_ref":external_ref,
            "representative_position":position,
            "historical_geometry":"not_verified",
        }
