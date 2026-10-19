"""Restricted research operations over a saved expert request.

This adapter never substitutes the caller's owner OAuth context. Service auth
selects the executor; the immutable inbox binding selects the human principal.
"""
import asyncio

from .disagreement_contracts import ConclusionAdd,ContributionAdd,ContributionCorrect
from .expert_requests import ExpertRequestError
from .expert_source_scope import ExpertSourceScope,current_source_scope,public_document
from .request_authority import track_resources


class ExpertExecutor:
    def __init__(self,requests,backend):
        from .sqlite_backend import SQLiteBackend
        if not isinstance(backend,SQLiteBackend) or backend.corpus.path!=requests.corpus.path:
            raise ValueError("expert_source_requires_same_sqlite_authority")
        self.requests=requests
        self.backend=backend

    async def context(self,credentials,request_id,generation,execution_token):
        return await asyncio.to_thread(self.requests.context,credentials,str(request_id),generation,execution_token)

    def _complete_source_read(self,context,chunk_ids):
        documents=set()
        with self.requests.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self.requests.people.require(context,"public_search",connection=db)
            for chunk_id in chunk_ids:
                chunk=self.requests.people.sources._corpus_row(db,"rkb_chunks",chunk_id)
                if not chunk:raise ExpertRequestError("source_not_accessible")
                document=self.requests.people.sources._document_allowed(db,context.principal.subject,chunk["document_id"])
                if not document:raise ExpertRequestError("source_not_accessible")
                if not chunk.get("revision") or chunk["revision"]!=document.get("active_revision"):
                    raise ExpertRequestError("source_revision_not_current")
                if context.public_only and (not public_document(document) or chunk.get("revision")!=document.get("active_revision")):
                    raise ExpertRequestError("public_projection_required")
                documents.add(chunk["document_id"])
            track_resources(db,context,document_ids=documents)
        return documents

    async def search(self,credentials,request_id,generation,execution_token,query,*,limit=8,main_job_id=None):
        # Leave a small response-conversion margin inside the protected 2s
        # search budget; dependency bookkeeping is part of this same deadline.
        deadline=asyncio.timeout(1.9)
        try:
            async with deadline:
                return await self._search(credentials,request_id,generation,execution_token,query,limit=limit,main_job_id=main_job_id)
        except TimeoutError:
            if not deadline.expired():raise
            raise ExpertRequestError('search_deadline') from None

    async def _search(self,credentials,request_id,generation,execution_token,query,*,limit=8,main_job_id=None):
        context=await self.context(credentials,request_id,generation,execution_token)
        scope=current_source_scope.set(ExpertSourceScope(self.requests.people,context,
            self.requests.dependencies.consumer(request_id) if self.requests.dependencies else None))
        try:
            found=await self.backend.search(query,context.principal,match_count=limit,main_job_id=main_job_id)
            await asyncio.to_thread(self._complete_source_read,context,[r.id for r in found.results])
            if found.main_job_id and self.requests.dependencies:
                await asyncio.to_thread(self.requests.dependencies.observe,context,found.main_job_id,query)
            return found.model_dump(mode="json")
        finally:
            current_source_scope.reset(scope)

    async def fetch(self,credentials,request_id,generation,execution_token,chunk_id):
        context=await self.context(credentials,request_id,generation,execution_token)
        scope=current_source_scope.set(ExpertSourceScope(self.requests.people,context))
        try:
            await asyncio.to_thread(self._complete_source_read,context,[str(chunk_id)])
            found=await self.backend.fetch(str(chunk_id),context.principal)
            if found.id!=str(chunk_id):raise ExpertRequestError("source_identity_mismatch")
            documents=await asyncio.to_thread(self._complete_source_read,context,[str(chunk_id)])
            if not found.metadata or found.metadata.get("document_id") not in documents:
                raise ExpertRequestError("source_identity_mismatch")
            if context.public_only:
                # Private original/figure descriptors and arbitrary source
                # metadata are not automatically public with accepted text.
                allowed={"document_id","revision","source_pages","regions","footnotes"}
                found=found.model_copy(update={"metadata":{k:v for k,v in found.metadata.items() if k in allowed}})
            return found.model_dump(mode="json")
        finally:
            current_source_scope.reset(scope)

    async def case_get(self,credentials,request_id,generation,execution_token,case_id,*,section="case",after_seq=0,limit=20):
        context=await self.context(credentials,request_id,generation,execution_token)
        return await asyncio.to_thread(self.requests.journal.get,context,str(case_id),section=section,after_seq=after_seq,limit=limit)

    async def contribute(self,credentials,request_id,generation,execution_token,case_id,operation_key,command,*,contribution_id=None,expected_version=None):
        context=await self.context(credentials,request_id,generation,execution_token)
        if isinstance(command,ContributionAdd) and contribution_id is None and expected_version is None:
            return await asyncio.to_thread(self.requests.journal.contribute,context,str(case_id),command,operation_key)
        if isinstance(command,ContributionCorrect) and contribution_id and expected_version:
            return await asyncio.to_thread(self.requests.journal.correct_contribution,context,str(case_id),str(contribution_id),expected_version,command,operation_key)
        raise ExpertRequestError("invalid_arguments")

    async def conclude(self,credentials,request_id,generation,execution_token,case_id,expected_revision,operation_key,command:ConclusionAdd):
        context=await self.context(credentials,request_id,generation,execution_token)
        return await asyncio.to_thread(self.requests.journal.conclude,context,str(case_id),expected_revision,command,operation_key)
