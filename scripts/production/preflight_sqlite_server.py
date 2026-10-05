"""Private-loopback preflight using the existing application OAuth authority."""
import os,shlex
from pathlib import Path
from operator_env import load_service_env
load_service_env()
for raw in Path('/home/dev/.env').read_text().splitlines():
    try:parts=shlex.split(raw)
    except ValueError:continue
    if len(parts)==1 and parts[0].startswith(os.getenv('RKB_PROOF_KEY_ENV','GOOGLE_API_KEY')+'='):os.environ['RKB_GEMINI_API_KEY']=parts[0].split('=',1)[1]
os.environ['RKB_SCAN_PROOF_ENABLED']='1'
os.environ['RKB_PROOF_MODEL']='gemini-2.5-flash-lite'
os.environ['RKB_SQLITE_CORPUS_PATH']='/home/dev/.local/state/regional-knowledge-base/corpus.sqlite3'
from regional_knowledge.server import build_server,_transport_security
build_server().run(transport='streamable-http',host='127.0.0.1',port=8115,stateless_http=True,json_response=True,transport_security=_transport_security())
