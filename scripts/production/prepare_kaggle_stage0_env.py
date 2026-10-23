"""One-time offline Dataset-preinstalled archive packer; no provider operations."""
import argparse
import json
from pathlib import Path
from regional_knowledge.kaggle_stage0_preinstalled import build_archive


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--site-packages',required=True)
    parser.add_argument('--packages-json',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--artifact-name',required=True)
    args=parser.parse_args(argv)
    lock=json.loads(Path(args.packages_json).read_text())
    if (type(lock) is not dict or lock.get('codec')!='rkb.candidate_wheel_lock.v1'
            or type(lock.get('packages')) is not list):
        parser.error('exact pinned wheel-lock required')
    packages={}
    for item in lock['packages']:
        if (type(item) is not dict or not {'package','version'}<=set(item)
                or item['package'] in packages):
            parser.error('invalid or duplicate package lock')
        packages[item['package']]=item['version']
    result=build_archive(args.site_packages,packages,args.output,
        artifact_name=args.artifact_name)
    # This is not publication: only an authorized owner can upload, choose
    # version-qualified Dataset refs, and perform an independent import smoke.
    print(json.dumps(result,sort_keys=True,separators=(',',':')))


if __name__=='__main__':
    main()
