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
        self.blocked_reads={};self.recovered_reads={};self.read_origins={}
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
        if key in self.blocked_reads:
            return {'operation_complete':True,'state':'blocked',
                    'error':{'code':self.blocked_reads[key]}}
        if key.startswith('read:') or key in self.read_origins:
            entry=dict(self.entries[self.read_origins.get(key, key[5:])])
            if self.wrong_topic:entry['thread_ref']='https://t.me/c/123456/9'
            return {'operation_complete':True,'state':'verified','media_store_items':[entry],'items':[{'media_evidence':[{'media_kind':'document','sha256':entry['provider_sha']}]}]}
        return self.operations[key]
    async def call(self,name,args):
        command=args['command']
        if command['kind']=='search':return {'media_store_items':[r for r in self.entries.values() if r['origin']['ref']==command['text']]}
        if command['kind']=='get' and args.get('request_key'):
            key=args['request_key']
            if key not in self.recovered_reads:
                operation='recovered-read:'+str(len(self.recovered_reads)+1)
                self.recovered_reads[key]=operation
                self.read_origins[operation]=command['entry_ref']
            return {'operation_id':self.recovered_reads[key]}
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


@pytest.mark.asyncio
async def test_terminal_floodwait_recovers_same_verified_put_with_one_keyed_get(graph_db):
    """Real DB state survives restart; a FloodWait GET cannot strand a verified PUT."""
    from datetime import datetime,timedelta,timezone
    from regional_knowledge.illustration_mirror import _read_retry_due

    owner,doc,page,region,obj,illustration=[uuid4() for _ in range(6)]
    put_id='verified-existing-put'
    provider_sha=hashlib.sha256(b'exact provider image bytes').hexdigest()
    b=PostgresBackend(graph_db,embedder=LexicalOnlyEmbedder(),object_store=Objects())
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('insert into rkb_users(id) values(%s)',(owner,))
            await db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Illustrated test',%s,1,1)",(doc,owner,'a'*64))
            await db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
            await db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order) values(%s,%s,'figure','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',0)",(region,page))
            await db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'illustration_crop',%s,%s,'image/png',123)",(obj,doc,str(obj),'b'*64))
            await db.execute("""insert into rkb_illustrations(
                id,document_id,page_id,source_region_id,crop_object_id,kind,
                source_crop_sha256,mirror_operation_id,mirror_read_operation_id)
                values(%s,%s,%s,%s,%s,'drawing',%s,%s,%s)""",
                (illustration,doc,page,region,obj,'b'*64,put_id,'read:'+put_id))
        vibe=Vibe(str(owner));vibe.fail=False
        vibe.operations[put_id]={'operation_id':put_id,'operation_complete':True,
                                  'state':'verified'}
        vibe.entries[put_id]={'entry_ref':put_id,'thread_ref':vibe.grant['thread_ref'],
                              'origin':{'system':'regional_knowledge',
                                        'ref':'knowledge://illustrations/'+str(illustration)},
                              'provider_sha':provider_sha}
        vibe.blocked_reads['read:'+put_id]='telegram_transport_unhealthy'
        mirror=IllustrationMirror(b,vibe)
        assert await mirror.tick()==0
        saved=vibe.recovered_reads['rkb:mirror:read-retry:read:'+put_id]
        assert len(vibe.recovered_reads)==1
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            row=await(await db.execute('select mirror_operation_id,mirror_read_operation_id,mirror_error_type,mirror_attempt_at from rkb_illustrations where id=%s',(illustration,))).fetchone()
            assert row['mirror_operation_id']==put_id
            assert row['mirror_read_operation_id']==saved
            assert row['mirror_error_type']=='MirrorReadTransient'
            assert not _read_retry_due(row,datetime.now(timezone.utc))
        # A second recoverable terminal failure respects the persistent 15-minute
        # cooldown: status polling cannot mint another GET every indexer tick.
        vibe.blocked_reads[saved]='telegram_transport_unhealthy'
        assert await IllustrationMirror(b,vibe).tick()==0
        assert len(vibe.recovered_reads)==1
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute("update rkb_illustrations set mirror_attempt_at=now()-interval '16 minutes' where id=%s",(illustration,))
        assert await IllustrationMirror(b,vibe).tick()==0
        assert len(vibe.recovered_reads)==2
        assert 'rkb:mirror:read-retry:'+saved in vibe.recovered_reads
        assert await IllustrationMirror(b,vibe).tick()==1
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            stored=await(await db.execute('select mirror_operation_id,vibepublish_entry_ref,provider_crop_sha256,mirror_error_type from rkb_illustrations where id=%s',(illustration,))).fetchone()
        assert stored['mirror_operation_id']==put_id
        assert stored['vibepublish_entry_ref']==put_id
        assert stored['provider_crop_sha256']==provider_sha
        assert stored['mirror_error_type'] is None
        assert len(vibe.operations)==1  # Not one extra PUT.
    finally:
        await b.aclose()


@pytest.mark.asyncio
async def test_terminal_access_denied_does_not_create_a_new_read(graph_db):
    """Authorization and UNKNOWN outcomes never enter transport-only recovery."""
    owner,doc,page,region,obj,illustration=[uuid4() for _ in range(6)]
    b=PostgresBackend(graph_db,embedder=LexicalOnlyEmbedder(),object_store=Objects())
    try:
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            await db.execute('insert into rkb_users(id) values(%s)',(owner,))
            await db.execute("insert into rkb_documents(id,owner_user_id,title,source_sha256,active_revision,page_count) values(%s,%s,'Denied mirror',%s,1,1)",(doc,owner,'a'*64))
            await db.execute('insert into rkb_pages(id,document_id,physical_page_index,width,height,revision) values(%s,%s,0,1000,1000,1)',(page,doc))
            await db.execute("insert into rkb_regions(id,page_id,kind,bbox,reading_order) values(%s,%s,'figure','{\"left\":0,\"top\":0,\"right\":1000,\"bottom\":1000}',0)",(region,page))
            await db.execute("insert into rkb_objects(id,document_id,kind,object_key,sha256,mime_type,size_bytes) values(%s,%s,'illustration_crop',%s,%s,'image/png',123)",(obj,doc,str(obj),'b'*64))
            await db.execute("""insert into rkb_illustrations(id,document_id,page_id,source_region_id,crop_object_id,kind,source_crop_sha256,mirror_operation_id,mirror_read_operation_id)
                values(%s,%s,%s,%s,%s,'drawing',%s,'existing-put','read:existing-put')""",(illustration,doc,page,region,obj,'b'*64))
        vibe=Vibe(str(owner));vibe.operations['existing-put']={
            'operation_id':'existing-put','operation_complete':True,'state':'verified'}
        vibe.blocked_reads['read:existing-put']='access_denied'
        assert await IllustrationMirror(b,vibe).tick()==0
        assert vibe.recovered_reads=={}
        async with b.data_client._connection({'x-rkb-service':'1'}) as db:
            state=await(await db.execute('select mirror_read_operation_id,mirror_error_type from rkb_illustrations where id=%s',(illustration,))).fetchone()
        assert state['mirror_read_operation_id']=='read:existing-put'
        assert state['mirror_error_type']=='RuntimeError'
    finally:
        await b.aclose()
