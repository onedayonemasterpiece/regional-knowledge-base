"""Own private analysis versions; selected publication uses the journal writer."""
import hashlib
import json
import re
from uuid import UUID

from .disagreement_contracts import ContributionAdd
from .disagreement_journal import JournalError,fail,now
from .request_authority import track_resources
from .sqlite_corpus import canonical


class PersonalDrafts:
    def __init__(self,journal):
        self.journal=journal;self.corpus=journal.corpus
        with self.corpus.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS disagreement_drafts(
                  id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,case_id TEXT NOT NULL,version INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS disagreement_draft_versions(
                  draft_id TEXT NOT NULL,version INTEGER NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL,
                  PRIMARY KEY(draft_id,version));
                CREATE TABLE IF NOT EXISTS disagreement_draft_dependencies(
                  draft_id TEXT NOT NULL,version INTEGER NOT NULL,kind TEXT NOT NULL,resource_id TEXT NOT NULL,
                  PRIMARY KEY(draft_id,version,kind,resource_id));
                CREATE TABLE IF NOT EXISTS disagreement_draft_operations(
                  actor_id TEXT NOT NULL,operation_key TEXT NOT NULL,case_id TEXT NOT NULL,
                  draft_id TEXT NOT NULL,version INTEGER NOT NULL,request_hash TEXT NOT NULL,result TEXT NOT NULL,
                  PRIMARY KEY(actor_id,operation_key));
                CREATE TABLE IF NOT EXISTS disagreement_draft_publications(
                  draft_id TEXT NOT NULL,version INTEGER NOT NULL,contribution_id TEXT NOT NULL,
                  PRIMARY KEY(draft_id,version,contribution_id));
            ''')

    def _auth(self,db,principal,case_id):
        actor=self.journal._actor(db,principal,'personal_conclusion')
        self.journal._actor(db,principal,'case_read')
        case=self.journal._case(db,actor,str(case_id))
        track_resources(db,principal,case_ids=[str(case_id)])
        return actor,case

    def _head(self,db,actor,case_id,draft_id):
        row=db.execute('SELECT * FROM disagreement_drafts WHERE id=?',(str(draft_id),)).fetchone()
        if not row or row['actor_id']!=actor or row['case_id']!=str(case_id):fail('not_found_or_not_accessible')
        return row

    def _dependencies(self,db,principal,actor,draft_id,version):
        rows=db.execute('SELECT kind,resource_id FROM disagreement_draft_dependencies WHERE draft_id=? AND version=?',
            (str(draft_id),version)).fetchall()
        documents=[];cases=[]
        for row in rows:
            if row['kind']=='document':
                if not self.journal.sources._document_allowed(db,actor,row['resource_id']):fail('not_found_or_not_accessible')
                documents.append(row['resource_id'])
            else:
                self.journal._case(db,actor,row['resource_id']);cases.append(row['resource_id'])
        track_resources(db,principal,document_ids=documents,case_ids=cases)
        return set(documents),set(cases)

    def list(self,principal,case_id,*,after_id='',limit=20):
        if not isinstance(after_id,str) or type(limit) is not int or not 1<=limit<=100:fail('invalid_page')
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE' if getattr(principal,'request_id',None) else 'BEGIN')
            actor,_=self._auth(db,principal,case_id)
            rows=db.execute('''SELECT id,version FROM disagreement_drafts WHERE actor_id=? AND case_id=?
                AND id>? ORDER BY id LIMIT ?''',(actor,str(case_id),after_id,limit+1)).fetchall()
            items=[]
            for row in rows[:limit]:
                try:self._dependencies(db,principal,actor,row['id'],row['version'])
                except JournalError:continue
                items.append({'draft_id':row['id'],'version':row['version'],'visibility':'personal'})
            return {'case_id':str(case_id),'items':items,
                'next_after_id':rows[limit-1]['id'] if len(rows)>limit else None}

    def get(self,principal,case_id,draft_id,*,version=None):
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE' if getattr(principal,'request_id',None) else 'BEGIN')
            actor,_=self._auth(db,principal,case_id)
            head=self._head(db,actor,case_id,draft_id)
            version=head['version'] if version is None else version
            row=db.execute('SELECT * FROM disagreement_draft_versions WHERE draft_id=? AND version=?',(str(draft_id),version)).fetchone()
            if not row:fail('not_found_or_not_accessible')
            self._dependencies(db,principal,actor,draft_id,version)
            return {'draft_id':str(draft_id),'case_id':str(case_id),'version':version,'current_version':head['version'],
                'visibility':'personal','payload':json.loads(row['payload']),
                'publication_ids':[r[0] for r in db.execute('SELECT contribution_id FROM disagreement_draft_publications WHERE draft_id=? AND version=? ORDER BY contribution_id',(str(draft_id),version))]}

    def save(self,principal,case_id,draft_id,expected_version,command,operation_key):
        if type(expected_version) is not int or expected_version<0:fail('invalid_arguments')
        try:draft_id=str(UUID(str(draft_id)))
        except (ValueError,TypeError,AttributeError):fail('invalid_arguments')
        if not isinstance(operation_key,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{7,127}',operation_key):fail('invalid_operation_key')
        payload=command.model_dump(mode='json')
        digest=hashlib.sha256(canonical([str(case_id),draft_id,expected_version,payload]).encode()).hexdigest()
        request_id=getattr(principal,'request_id',None)
        key=request_id+':'+operation_key if request_id else operation_key
        with self.corpus.connect() as db:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            actor,case=self._auth(db,principal,case_id)
            saved=db.execute('SELECT * FROM disagreement_draft_operations WHERE actor_id=? AND operation_key=?',(actor,key)).fetchone()
            if saved:
                if saved['request_hash']!=digest:fail('idempotency_conflict')
                self._dependencies(db,principal,actor,draft_id,saved['version'])
                return json.loads(saved['result'])
            old=db.execute('SELECT * FROM disagreement_drafts WHERE id=?',(draft_id,)).fetchone()
            if old:
                self._head(db,actor,case_id,draft_id)
                self._dependencies(db,principal,actor,draft_id,old['version'])
            if (old['version'] if old else 0)!=expected_version:fail('revision_conflict')
            if command.based_on_seq>case['event_seq']:fail('invalid_material_snapshot')
            self.journal._references(db,case,command.evidence_ids,command.position_ids)
            for ident in command.evidence_ids:
                if db.execute('SELECT event_seq FROM disagreement_evidence WHERE id=?',(str(ident),)).fetchone()[0]>command.based_on_seq:fail('material_outside_snapshot')
            version=expected_version+1
            db.execute('INSERT INTO disagreement_drafts VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version',
                (draft_id,actor,str(case_id),version))
            db.execute('INSERT INTO disagreement_draft_versions VALUES(?,?,?,?)',(draft_id,version,canonical(payload),now()))
            deps={('case',str(case_id))}
            deps.update(('document',r[0]) for r in db.execute('SELECT DISTINCT document_id FROM disagreement_evidence WHERE case_id=?',(str(case_id),)))
            if old:deps.update((r[0],r[1]) for r in db.execute('SELECT kind,resource_id FROM disagreement_draft_dependencies WHERE draft_id=? AND version=?',(draft_id,old['version'])))
            if request_id:
                deps.update((r[0],r[1]) for r in db.execute('SELECT DISTINCT kind,resource_id FROM rkb_expert_request_resources WHERE request_id=?',(request_id,)))
            db.executemany('INSERT INTO disagreement_draft_dependencies VALUES(?,?,?,?)',[(draft_id,version,*dep) for dep in deps])
            self._dependencies(db,principal,actor,draft_id,version)
            result={'draft_id':draft_id,'case_id':str(case_id),'version':version,'visibility':'personal','state':'saved'}
            db.execute('INSERT INTO disagreement_draft_operations VALUES(?,?,?,?,?,?,?)',(actor,key,str(case_id),draft_id,version,digest,canonical(result)))
            return result

    def publish(self,principal,case_id,draft_id,expected_version,command,operation_key):
        if type(expected_version) is not int or expected_version<1:fail('invalid_arguments')
        payload={'draft_id':str(draft_id),'expected_version':expected_version,**command.model_dump(mode='json')}
        def authorize(db,actor,case):
            self._auth(db,principal,case_id)
            self.journal._actor(db,principal,'case_contribute')
            self._head(db,actor,case_id,draft_id)
            self._dependencies(db,principal,actor,draft_id,expected_version)
        def apply(db,actor,case):
            self._auth(db,principal,case_id)
            head=self._head(db,actor,case_id,draft_id)
            if head['version']!=expected_version:fail('revision_conflict')
            documents,cases=self._dependencies(db,principal,actor,draft_id,expected_version)
            case_documents={r[0] for r in db.execute('SELECT DISTINCT document_id FROM disagreement_evidence WHERE case_id=?',(str(case_id),))}
            # A selected substring is not proof that unrelated private context
            # became public. The first release publishes only within the same
            # already-authorized case evidence boundary.
            if not documents<=case_documents or not cases<={str(case_id)}:fail('draft_publication_requires_case_evidence')
            draft=json.loads(db.execute('SELECT payload FROM disagreement_draft_versions WHERE draft_id=? AND version=?',(str(draft_id),expected_version)).fetchone()[0])
            if command.text_end>len(draft['text']) or command.text_end-command.text_start>12000:fail('invalid_draft_span')
            if (not {str(v) for v in command.evidence_ids}<=set(draft['evidence_ids'])
                or not set(command.position_ids)<=set(draft['position_ids'])):fail('invalid_draft_reference')
            selected=draft['text'][command.text_start:command.text_end]
            if not selected.strip():fail('invalid_draft_span')
            result=self.journal._append_contribution(db,principal,actor,case,ContributionAdd(kind=command.kind,
                original_text=selected,evidence_ids=command.evidence_ids,position_ids=command.position_ids))
            db.execute('INSERT INTO disagreement_draft_publications VALUES(?,?,?)',(str(draft_id),expected_version,result['contribution_id']))
            return {**result,'draft_id':str(draft_id),'draft_version':expected_version}
        return self.journal._mutate(principal,str(case_id),operation_key,'publish_draft',payload,apply,
            policy_capability='personal_conclusion',authorize=authorize)
