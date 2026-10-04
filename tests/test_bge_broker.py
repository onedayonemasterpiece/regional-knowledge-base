from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient
from regional_knowledge.bge_broker import register_routes
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.bge_contract import SPACE,REVISION

def test_worker_routes_bound_credentials_contract_and_payload(tmp_path):
    queue=BgeQueue(tmp_path/'queue.sqlite');queue.enqueue('owner','one',['authorized query'])
    with queue.connect() as db:run=db.execute('select id from runs').fetchone()[0]
    token=(queue.path.parent/'bge-worker-credentials'/run).read_text()
    class Registrar:
        routes=[]
        def custom_route(self,path,methods):
            def register(handler):self.routes.append(Route(path,handler,methods=methods))
            return register
    registrar=Registrar();register_routes(registrar,queue)
    with TestClient(Starlette(routes=registrar.routes)) as client:
        url='/bge-worker/heartbeat';payload={'run_id':run,'space':SPACE,'ready':True,'model_revision':REVISION}
        assert client.post(url,json=payload).status_code==403
        assert client.post(url,json=payload,headers={'Authorization':'Bearer ordinary-user-token'}).status_code==403
        headers={'Authorization':'Bearer '+token}
        assert client.post(url,json={**payload,'model_revision':'wrong'},headers=headers).status_code==400
        assert queue.status()['state']=='starting'
        assert client.post(url,json={**payload,'diagnostics':{'rss_kib':-1}},headers=headers).status_code==400
        assert client.post(url,json={**payload,'diagnostics':['invalid']},headers=headers).status_code==400
        assert client.post(url,content=b'x'*65537,headers=headers).status_code==413
        assert client.post(url,json=payload,headers=headers).status_code==200
        assert queue.status()['state']=='ready'
        claimed=client.post('/bge-worker/claim',json={'run_id':run,'space':SPACE},headers=headers)
        assert claimed.status_code==200 and claimed.json()['job']['texts']==['authorized query']
        assert client.post('/bge-worker/execute',json=payload,headers=headers).status_code==404
