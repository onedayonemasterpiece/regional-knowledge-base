"""Source admission shares the mirror disk reserve; receipt recovery stays live."""
import hashlib
from types import SimpleNamespace
from uuid import uuid4
import pytest
from test_illustration_mirror_sqlite import mirror_source
from regional_knowledge import source_archive
from regional_knowledge.sqlite_data import defaults


@pytest.mark.asyncio
async def test_source_put_defers_low_disk_and_resumed_read_reuses_key(mirror_source,monkeypatch):
    source=mirror_source;backend=source.backend;payload=b'%PDF-1.7 synthetic source'
    digest=hashlib.sha256(payload).hexdigest()
    doc=backend.corpus.one('rkb_documents',source.document)
    backend.corpus.put('rkb_documents',[{**doc,'source_sha256':digest,'source_archive_status':'pending'}])
    backend.corpus.put('rkb_objects',[{**defaults('rkb_objects'),'id':str(uuid4()),'document_id':source.document,
        'object_key':'source','kind':'source_pdf','sha256':digest,'mime_type':'application/pdf','size_bytes':len(payload)}])
    class Store:
        calls=0
        async def get_bytes(self,key):self.calls+=1;return payload
    class Client:
        owner=source.owner;issuer='https://vibe.test'
        grant={'destination_alias':'kb','source_thread_ref':'topic2'}
        puts=0;reads={};lost=True
        async def request(self,*a,**kw):return SimpleNamespace(json=lambda:{'asset_id':'asset'})
        async def call(self,name,args):
            cmd=args['command']
            if cmd['kind']=='put':self.puts+=1;return {'operation_id':'put'}
            if cmd['kind']=='search':return {'media_store_items':[self.entry()]}
            key=args['request_key'];self.reads.setdefault(key,'read')
            if self.lost:self.lost=False;raise TimeoutError('lost GET admission reply')
            return {'operation_id':self.reads[key]}
        def entry(self):return {'entry_ref':'entry','thread_ref':'topic2','origin':{'system':'regional_knowledge',
            'ref':'knowledge://documents/'+source.document+'/source','sha256':digest}}
        async def receipt(self,op):
            return {'operation_complete':True,'state':'verified','media_store_items':[self.entry()],
                'items':[{'media_evidence':[{'media_kind':'document','sha256':digest}]}]}
    client=Client();backend.object_store=Store()
    def reset_attempt():
        doc=backend.corpus.one('rkb_documents',source.document)
        backend.corpus.put('rkb_documents',[{**doc,'source_archive_attempt_at':None}])
    monkeypatch.setattr(source_archive.shutil,'disk_usage',lambda _:SimpleNamespace(free=2*1024**3-1))
    try:
        assert await source_archive.SourceArchive(backend,client).tick()==0
        doc=backend.corpus.one('rkb_documents',source.document)
        assert doc['source_archive_error']=='SourceAdmissionDeferred'
        assert backend.object_store.calls==client.puts==0
        reset_attempt()
        monkeypatch.setattr(source_archive.shutil,'disk_usage',lambda _:SimpleNamespace(free=2*1024**3))
        assert await source_archive.SourceArchive(backend,client).tick()==0
        assert len(client.reads)==1 and client.puts==1
        reset_attempt()
        # Receipt recovery consumes no source bytes and cannot be disk-gated.
        monkeypatch.setattr(source_archive.shutil,'disk_usage',lambda _:(_ for _ in ()).throw(AssertionError('probe on resumed PUT')))
        assert await source_archive.SourceArchive(backend,client).tick()==1
        assert len(client.reads)==1 and client.puts==1
    finally:await backend.aclose()
