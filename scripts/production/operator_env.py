"""Operator-only secret loading; values never logged or copied to run evidence."""
import os,shlex
from pathlib import Path

def load_service_env():
    for raw in Path('/home/dev/.local/state/regional-knowledge-base/service.env').read_text().splitlines():
        line=raw.strip()
        if line and not line.startswith('#') and '=' in line:
            parsed=shlex.split(line)
            if len(parsed)==1 and '=' in parsed[0]:
                key,value=parsed[0].split('=',1);os.environ[key]=value
