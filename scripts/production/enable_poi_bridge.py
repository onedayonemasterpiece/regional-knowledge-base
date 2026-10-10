"""Enable the existing owner's loopback POI bridge without sharing user OAuth or device tokens.

Run after both exact source releases are installed. It creates one private
scoped service credential and small systemd drop-ins; no database is copied.
Safe to replay, never logs the credential.
"""
from __future__ import annotations

import argparse
import os
import re
import secrets
import subprocess
from pathlib import Path

ROOT = Path("/home/dev/.local/state/rkb-street-story-poi")
ENV = ROOT / "bridge.env"
UNIT_ROOT = Path("/home/dev/.config/systemd/user")
UNITS = ("regional-knowledge-base.service", "street-story.service")
ENV_VALUE = ("EnvironmentFile=/home/dev/.local/state/rkb-street-story-poi/bridge.env\n")


def atomic(path: Path, content: str, mode: int):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp=path.with_name(path.name+".new-"+str(os.getpid()))
    fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,mode)
    try:
        with os.fdopen(fd,"w",encoding="utf8") as out:
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp,path)
    finally:
        tmp.unlink(missing_ok=True)


def run(apply: bool):
    if not apply:
        return {"status":"preflight","scope":"loopback_service_credential",
                "units":list(UNITS),"ready_to_install":True}
    ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
    if ROOT.is_symlink() or ENV.is_symlink():
        raise RuntimeError("secret storage may not be a symlink")
    if ENV.exists():
        if ENV.stat().st_mode & 0o077:
            raise RuntimeError("existing POI credential is not private")
        values={}
        for line in ENV.read_text(encoding="utf8").splitlines():
            if "=" in line:
                k,v=line.split("=",1)
                values[k]=v.strip()
        token=values.get("RKB_STREET_STORY_POI_TOKEN","")
        if (len(token)<32 or
                values.get("STREET_STORY_RKB_POI_SERVICE_TOKEN")!=token):
            raise RuntimeError("existing POI service credential must remain unchanged")
    else:
        token=secrets.token_urlsafe(48)
        atomic(ENV,
            "RKB_STREET_STORY_POI_TOKEN="+token+"\n"
            "STREET_STORY_RKB_POI_SERVICE_TOKEN="+token+"\n"
            "RKB_STREET_STORY_POI_URL=http://127.0.0.1:8188\n"
            "STREET_STORY_RKB_CONTEXT_URL=http://127.0.0.1:8000/internal/street-story/poi-context\n",
            0o600)
    for unit in UNITS:
        target=UNIT_ROOT/(unit+".d")/"99-rkb-poi-bridge.conf"
        content="[Service]\n"+ENV_VALUE
        if target.exists() and target.read_text()!=content:
            raise RuntimeError("conflicting service override: "+unit)
        if not target.exists():
            atomic(target,content,0o644)
    subprocess.run(["systemctl","--user","daemon-reload"],check=True,timeout=20,
                   stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    return {"status":"configured","units":list(UNITS),
            "scope":"shared_private_loopback_poi_only",
            "requires_service_restart":True,"credential":"redacted"}


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--apply",action="store_true")
    args=parser.parse_args()
    import json
    print(json.dumps(run(args.apply)))
