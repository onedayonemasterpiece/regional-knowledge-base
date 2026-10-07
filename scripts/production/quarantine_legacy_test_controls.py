"""Quarantine legacy synthetic acceptance controls from normal retrieval.

Dry-run by default. --apply only flips catalog.searchable=false for exact,
narrowly identified educational synthetic controls. It never deletes corpus,
vectors, provenance, or catalog visibility.
"""
from __future__ import annotations
import argparse,json,os,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"src"))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from operator_env import load_service_env
from regional_knowledge.sqlite_corpus import SQLiteCorpus

def candidate(doc):
    catalog=doc.get("catalog") or {}
    return (
        int(doc.get("active_revision") or 0)>0
        and str(doc.get("title") or "").startswith("Synthetic ")
        and catalog.get("purpose")=="educational"
        and catalog.get("searchable",True) is not False
    )

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--apply",action="store_true")
    p.add_argument("--max-controls",type=int,default=32)
    a=p.parse_args()
    if not 1<=a.max_controls<=100:raise ValueError("invalid control bound")
    load_service_env()
    corpus=SQLiteCorpus(os.environ["RKB_SQLITE_CORPUS_PATH"])
    targets=sorted((d for d in corpus.rows("rkb_documents") if candidate(d)),key=lambda d:d["id"])
    if len(targets)>a.max_controls:raise RuntimeError("synthetic control bound exceeded")
    if a.apply:
        with corpus.connect() as db:
            db.execute("begin immediate")
            for doc in targets:
                current=corpus.one("rkb_documents",doc["id"])
                if not current or not candidate(current):
                    raise RuntimeError("control changed during quarantine")
                updated=dict(current)
                updated["catalog"]={**(current.get("catalog") or {}),"searchable":False}
                corpus.put("rkb_documents",[updated],connection=db)
            db.commit()
    remaining=[d["id"] for d in corpus.rows("rkb_documents") if candidate(d)]
    if a.apply and remaining:raise RuntimeError("synthetic controls remain searchable")
    print(json.dumps({
        "apply":a.apply,
        "candidate_count":len(targets),
        "document_ids":[d["id"] for d in targets],
        "remaining_searchable_synthetic_controls":remaining,
    },indent=2))

if __name__=="__main__":main()
