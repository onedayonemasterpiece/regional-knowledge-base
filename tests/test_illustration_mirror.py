"""Real durable RKB mirror state with a paced fake VibePublish boundary."""
import hashlib,io,json
from uuid import uuid4,UUID
import pytest
from test_graph_postgres import graph_db
from test_robust_import import Objects
from regional_knowledge.postgres_backend import PostgresBackend
from regional_knowledge.contracts import Principal
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from regional_knowledge.illustration_mirror import IllustrationMirror


class Vibe:
    def __init__(self,owner):
        self.owner=owner;self.grant={'thread_ref':'https://t.me/c/123456/4'}
        self.operations={};self.entries={};self.now=0;self.admissions=[];self.fail=True;self.wrong_topic=False
    async def put(self,key,metadata,crop,mime):
        if self.fail:raise OSError('boundary unavailable')
        provider_sha=hashlib.sha256(b'provider-sanitized:'+crop).hexdigest()
        self.operations.setdefault(key,{'operation_id':key,'operation_complete':False,'state':'running','uri':metadata['uri'],'provider_sha':provider_sha})
        return self.operations[key]
    def advance(self,now):
        self.now=now
        for key,operation in self.operations.items():
            if operation['operation_complete']:continue
            if sum(t>now-60 for t in self.admissions)>=20:break
            self.admissions.append(now);self.entries[key]={'entry_ref':key,'thread_ref':self.grant['thread_ref'],'origin':{'system':'regional_knowledge','ref':operation['uri']},'provider_sha':operation['provider_sha']}
            operation.update(operation_complete=True,state='verified')
    async def receipt(self,key):
        if key.startswith('read:'):
            entry=dict(self.entries[key[5:]])
            if self.wrong_topic:entry['thread_ref']='https://t.me/c/123456/9'
            return {'operation_complete':True,'state':'verified','media_store_items':[entry],'items':[{'media_evidence':[{'media_kind':'document','sha256':entry['provider_sha']}]}]}
        return self.operations[key]
    async def call(self,name,args):
        command=args['command']
        if command['kind']=='search':return {'media_store_items':[r for r in self.entries.values() if r['origin']['ref']==command['text']]}
        return {'operation_id':'read:'+command['entry_ref']}


@pytest.mark.asyncio
async def test_27_private_mirrors_outage_restart_replay_topic_and_budget(graph_db):
    owner,foreign,doc,page,obj=([uuid4() for _ in range(5)]);objects=Objects()
    fitz=pytest.importorskip('fitz');pdf=fitz.open();p=pdf.new_page(width=16,height=16);p.draw_rect(p.rect,fill=(1,0,0));crop=p.get_pixmap().tobytes('png');pdf.close();crop_sha=hashlib.sha256(crop).hexdigest()
    b=PostgresBackend(graph_db,embedder=LexicalOnlyEmbedder(),object_store=objects)
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('insert into rkb_users(id) values(%s),(%s)',(owner,foreign))
            await db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Synthetic mirror',%s,1,1)",(doc,owner,'d'*64))
            await db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
            for i in range(27):
                iid,rid,oid=uuid4(),uuid4(),uuid4();key=str(oid);objects.data[key]=crop
                await db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order) values(%s,%s,'figure','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',%s)",(rid,page,i))
                await db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'illustration_crop',%s,%s,'image/png',%s)",(oid,doc,key,crop_sha,len(crop)))
                await db.execute("insert into rkb_illustrations(id,document_id,page_id,source_region_id,crop_object_id,kind,source_crop_sha256) values(%s,%s,%s,%s,%s,'drawing',%s)",(iid,doc,page,rid,oid,crop_sha))
        client=Vibe(str(owner));worker=IllustrationMirror(b,client)
        assert await worker.tick()==0 and not client.operations
        client.fail=False
        for _ in range(8):await worker.tick()
        assert len(client.operations)==27
        client.advance(0);assert len(client.entries)==20
        client.wrong_topic=True;assert await worker.tick()==0
        client.wrong_topic=False
        worker=IllustrationMirror(b,client) # All durable operation IDs survive owner restart.
        for _ in range(20):await worker.tick()
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            assert (await(await db.execute('select count(*) n from rkb_illustrations where document_id=%s and vibepublish_entry_ref is not null',(doc,))).fetchone())['n']==20
            # A lost put response/database write is recovered with the same key.
            await db.execute('update rkb_illustrations set mirror_operation_id=null where document_id=%s and vibepublish_entry_ref is null',(doc,))
        client.advance(61)
        for _ in range(10):await IllustrationMirror(b,client).tick()
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            assert (await(await db.execute('select count(*) n from rkb_illustrations where document_id=%s and vibepublish_entry_ref is not null',(doc,))).fetchone())['n']==27
            assert (await(await db.execute('select count(*) n from rkb_illustrations where document_id=%s and provider_crop_sha256 is not null',(doc,))).fetchone())['n']==27
            assert (await(await db.execute('select active_revision from rkb_documents where id=%s',(doc,))).fetchone())['active_revision']==1
        assert len(client.operations)==len(client.entries)==len(client.admissions)==27
        assert all(sum(t-60<s<=t for s in client.admissions)<=20 for t in client.admissions)
        assert await worker.tick()==0
        denied=Vibe(str(foreign));denied.fail=False;assert await IllustrationMirror(b,denied).tick()==0 and not denied.operations
    finally:await b.aclose()
