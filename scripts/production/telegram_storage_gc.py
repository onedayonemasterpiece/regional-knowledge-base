"""Operator dry-run/apply of guarded database-owned cache objects."""
import argparse,asyncio,json
from pathlib import Path
from operator_env import load_service_env
from regional_knowledge.supabase_backend import backend_from_env
from regional_knowledge.storage_gc import collect
async def run(args):
    load_service_env();b=backend_from_env()
    try:
        result=await collect(b,apply=args.apply)
        args.output.write_text(json.dumps(result,indent=2));print(json.dumps(result))
    finally:await b.aclose()
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('output',type=Path);p.add_argument('--apply',action='store_true');asyncio.run(run(p.parse_args()))
