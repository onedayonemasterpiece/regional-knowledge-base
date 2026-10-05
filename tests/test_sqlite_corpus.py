import hashlib
from uuid import uuid4
import pytest
from regional_knowledge.sqlite_corpus import SQLiteCorpus

@pytest.fixture
def corpus(tmp_path):
    c=SQLiteCorpus(tmp_path/'corpus.sqlite3');owner=str(uuid4());doc=str(uuid4());page=str(uuid4());region=str(uuid4());chunk=str(uuid4());text='Кёнигсберг 1945. Exact citation ﬂower.';sha=hashlib.sha256(text.encode()).hexdigest()
    c.put('rkb_users',[{'id':owner,'status':'active'}])
    c.put('rkb_documents',[{'id':doc,'owner_user_id':owner,'title':'Book','authors':['A','B'],'active_revision':1,'source_sha256':'a'*64,'content_visibility':'private'}])
    c.put('rkb_pages',[{'id':page,'document_id':doc,'revision':1,'physical_page_index':0}])
    c.put('rkb_regions',[{'id':region,'page_id':page,'source_text':text,'reading_order':0,'kind':'body','bbox':{'left':0,'top':0,'right':1000,'bottom':100}}])
    c.put('rkb_chunks',[{'id':chunk,'document_id':doc,'revision':1,'title':'Evidence','region_ids':[region],'page_ids':[page],'source_text':text,'text_sha256':sha,'search_material':text,'search_material_sha256':sha}])
    return c,owner,doc,chunk

def test_exact_idempotent_restore_fts(corpus,tmp_path):
    c,owner,doc,chunk=corpus;before=c.digest();row=c.one('rkb_chunks',chunk);c.put('rkb_chunks',[row]);assert c.digest()==before
    assert c.lexical(owner,'Кёнигсберг "1945" *')[0]['chunk_id']==chunk
    assert c.lexical(owner,'Exact citation',phrase=True)[0]['chunk_id']==chunk
    assert c.build_fragments()=={'fragments':1,'unmapped':0}
    restored=SQLiteCorpus(c.backup(tmp_path/'restore.sqlite3'));assert restored.digest()==before
    assert restored.one('rkb_chunks',chunk)==row
    with c.connect() as db:
        assert db.execute('pragma foreign_keys').fetchone()[0]==1
        assert db.execute('pragma journal_mode').fetchone()[0]=='wal'
        db.execute("insert into chunk_fts(chunk_fts,rank) values('integrity-check',1)")

def test_acl_revision_and_hash_fail_closed(corpus):
    c,owner,doc,chunk=corpus;other=str(uuid4());c.put('rkb_users',[{'id':other,'status':'active'}])
    assert c.lexical(other,'Exact')==[]
    with pytest.raises(LookupError):c.authorize(other,doc)
    c.put('rkb_document_grants',[{'document_id':doc,'grantee_user_id':other,'role':'viewer'}])
    assert c.lexical(other,'Exact')
    with pytest.raises(PermissionError):c.authorize(other,doc,owner=True)
    c.put('rkb_users',[{'id':other,'status':'disabled'}]);assert c.lexical(other,'Exact')==[]
    row=c.one('rkb_chunks',chunk)
    with pytest.raises(ValueError):c.put('rkb_chunks',[{**row,'source_text':'changed'}])
    assert c.one('rkb_chunks',chunk)==row
    d=c.one('rkb_documents',doc);c.put('rkb_documents',[{**d,'active_revision':2}]);assert c.lexical(owner,'Exact')==[]

def test_catalog_empty_lists_unknown_metadata(corpus):
    c,owner,doc,chunk=corpus
    page=c.catalog(owner);assert page['items'][0]['authors']==['A','B'];assert page['items'][0]['catalog']=={'kind':'book'}
    assert c.catalog(owner,'no match')['items']==[]
