"""Read-only production probe of the dedicated RKB <-> Street Story POI bridge.

Only prints status codes, boolean service environment presence and POI counts.
Never prints tokens, user/actor IDs, private source excerpts or environment dumps.
"""
from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BRIDGE_ENV=Path("/home/dev/.local/state/rkb-street-story-poi/bridge.env")
RKB_ENV=Path("/home/dev/.local/state/regional-knowledge-base/service.env")
BASE="http://127.0.0.1:8188"
RKB="http://127.0.0.1:8000"


def setting(path, name):
    for line in path.read_text(encoding="utf8").splitlines():
        if line.strip().startswith(name+"="):
            val=line.split("=",1)[1].strip()
            return val[1:-1] if val.startswith(("'",'"')) and val[-1:]==val[:1] else val
    raise RuntimeError("required private setting unavailable: "+name)


def http(method,url,token=None,data=None):
    headers={"Accept":"application/json"}
    if token:
        headers["Authorization"]="Bearer "+token
    if data is not None:
        headers["Content-Type"]="application/json"
    req=urllib.request.Request(url,method=method,headers=headers,
                               data=json.dumps(data,ensure_ascii=False).encode("utf8") if data is not None else None)
    try:
        with urllib.request.urlopen(req,timeout=7) as resp:
            return resp.status,json.load(resp)
    except urllib.error.HTTPError as exc:
        try:
            val=json.load(exc)
        except (ValueError,UnicodeDecodeError):
            val={}
        return exc.code,val


def service_presence(service):
    env={**os.environ,"XDG_RUNTIME_DIR":"/run/user/"+str(os.getuid())}
    env["DBUS_SESSION_BUS_ADDRESS"]="unix:path="+env["XDG_RUNTIME_DIR"]+"/bus"
    result=subprocess.run(["systemctl","--user","show",service,
                           "-p","MainPID","--value"],capture_output=True,
                          text=True,timeout=6,check=True,env=env)
    pid=int(result.stdout.strip())
    if pid<=0:
        return {"active":False}
    binary=(Path("/proc")/str(pid)/"environ").read_bytes()
    keys={part.split(b"=",1)[0] for part in binary.split(b"\0") if b"=" in part}
    return {"active":True,"bridge_token":b"RKB_STREET_STORY_POI_TOKEN" in keys,
            "owner_token":b"STREET_STORY_RKB_POI_SERVICE_TOKEN" in keys,
            "rkb_endpoint":b"STREET_STORY_RKB_CONTEXT_URL" in keys,
            "source_marker":b"RKB_RELEASE_SHA" in keys}


def main():
    token=setting(BRIDGE_ENV,"RKB_STREET_STORY_POI_TOKEN")
    if token!=setting(BRIDGE_ENV,"STREET_STORY_RKB_POI_SERVICE_TOKEN"):
        raise RuntimeError("private grants do not match")
    actor=setting(RKB_ENV,"RKB_OWNER_SUBJECT")
    result={"services":{
       "rkb":service_presence("regional-knowledge-base.service"),
       "street_story":service_presence("street-story.service")}}
    for key,url in (("rkb_health",RKB+"/health"),("street_story_health",BASE+"/healthz")):
        status,body=http("GET",url)
        result[key]={"http":status,"healthy":body.get("status")=="ok" or body.get("ok") is True}
    body={"action":"search","actor_sub":actor,"query":"Кафедральный собор","limit":12}
    for key,auth in (("ss_without_auth",None),("ss_with_auth",token)):
        status,value=http("POST",BASE+"/v1/internal/rkb-poi",auth,body)
        result[key]={"http":status,"state":value.get("state"),
                     "candidates":len(value.get("candidates",[])),
                     "error_code":value.get("error",{}).get("code") if isinstance(value.get("error"),dict) else None,
                     "detail_type":type(value.get("detail")).__name__ if "detail" in value else None}
        if auth and status==200:
            results=value.get("candidates",[])
            if results:
                result["ss_candidate_refs"]=[x.get("poi_ref") for x in results[:10]]
    poiref="streetstory://poi/poi_ss_"+"0"*24
    location=RKB+"/internal/street-story/poi-context?"+urllib.parse.urlencode({"poi_ref":poiref})
    for key,auth in (("rkb_without_auth",None),("rkb_with_auth",token)):
        status,value=http("GET",location,auth)
        result[key]={"http":status,"scope":value.get("source_scope"),
                     "locations":len(value.get("locations",[])),
                     "error":value.get("error")}
    print(json.dumps(result,ensure_ascii=False))
if __name__=="__main__":
    import sys
    if "--poi-id" in sys.argv:
        import re
        pos=sys.argv.index("--poi-id")
        pid=sys.argv[pos+1]
        if not re.fullmatch(r"poi_ss_[0-9a-f]{24}",pid):
            raise RuntimeError("exact canonical POI ID required")
        file=Path("/home/dev/.local/state/street-story/device-token.txt")
        if file.stat().st_mode & 0o077:
            raise RuntimeError("device token is not private")
        private_device_token=file.read_text(encoding="utf8").strip()
        bridge_token=setting(BRIDGE_ENV,"RKB_STREET_STORY_POI_TOKEN")
        status,payload=http("GET",BASE+"/v1/pois/"+pid+"/knowledge",private_device_token)
        ref="streetstory://poi/"+pid
        internal,status_payload=http("GET",RKB+"/internal/street-story/poi-context?"+
            urllib.parse.urlencode({"poi_ref":ref}),bridge_token)
        if status!=200 or internal!=200:
            raise RuntimeError("RKB/Street Story bidirectional POI readback failed "+
                               str({"device_http":status,"rkb_http":internal}))
        locations=payload.get("locations",[])
        ids=[x.get("entity_id") for x in locations]
        story_ids=[story.get("story_id") for x in locations for story in x.get("stories",[])]
        exact_sources=[loc.get("evidence_refs",[]) for loc in locations]
        expected_fields={"entity_id","label","place_kind","place_context",
                         "map_refs","map_ref_verification","historical_geometry",
                         "evidence_refs","relations","stories"}
        fields_present=bool(locations) and all(
            expected_fields.issubset(loc) and
            loc["map_ref_verification"]=="not_verified" and
            loc["historical_geometry"]=="not_verified"
            and isinstance(loc["map_refs"],list)
            for loc in locations)
        not_accepted=payload.get("cartography_acceptance_claimed") is False
        relations_scoped=all(all(
            {"source_relation_state","source_time_scope","neighbor_poi_ref"}.issubset(edge)
            for edge in loc.get("relations",[])) for loc in locations)
        if "--assert-projection" in sys.argv and not (
                status==200 and internal==200 and payload==status_payload and
                ids and story_ids and fields_present and not_accepted and relations_scoped):
            raise RuntimeError("RKB/Street Story source geography projection contract failed")
        print(json.dumps({"owner_device_http":status,"rkb_service_http":internal,
                          "responses_equal":payload==status_payload,
                          "poi_ref":ref,"locations":len(ids),
                          "story_ids":story_ids,
                          "has_exact_source_evidence":all(bool(x) for x in exact_sources),
                          "place_projection_fields_present":fields_present,
                          "cartography_acceptance_claimed":payload.get("cartography_acceptance_claimed"),
                          "relation_provenance_fields_present":relations_scoped,
                          "has_more":payload.get("has_more"),
                          "readback": "passed" if payload==status_payload and ids and story_ids else "partial"},
                          ensure_ascii=False))
    elif "--diagnose" in sys.argv:
        env={**os.environ,"XDG_RUNTIME_DIR":"/run/user/"+str(os.getuid())}
        env["DBUS_SESSION_BUS_ADDRESS"]="unix:path="+env["XDG_RUNTIME_DIR"]+"/bus"
        p=subprocess.run(["journalctl","--user",
            "-u","regional-knowledge-base.service","--since","10 minutes ago",
            "--no-pager","-o","cat","-n","220"],capture_output=True,
            timeout=8,env=env,text=True,check=True)
        import re
        matched=[line[:450] for line in p.stdout.splitlines()
             if re.search(r"(Error executing tool poi_registry|Traceback|poi_registry_bridge|_owner_service|AttributeError|TypeError|KeyError|LookupError|RuntimeError|ValueError|HTTPStatusError|ConnectError|File .*\.py|Exception|ServerError)",line,re.I)]
        print(json.dumps({"diagnostic_lines":matched[-65:]},ensure_ascii=False))
    else:
        main()
