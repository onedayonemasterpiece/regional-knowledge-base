"""Read-only, public-Git source inventory for the 2026-10-21 RKB audit.

No imports of application code, credentials, runtime, databases or providers.
Read only exact committed Git objects; source presence is not live acceptance.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

OLD = '481241fa36db30e3f0e6c4c4b88fb3ce84964676'
NEW = 'bb7f8ae41dbede8c0af83aafffe794f193244a9e'
SPEC = '85676094cf1a019fd34a29daa930d4ebb16eedb7'
ROOT = Path(__file__).resolve().parents[1]


def git(*args: str) -> str:
    return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True,
                                   encoding='utf-8', timeout=30)


def show(ref: str, path: str) -> str:
    if not re.fullmatch(r'[0-9a-f]{40}', ref):
        raise ValueError('exact commit required')
    if path.startswith('/') or '..' in Path(path).parts:
        raise ValueError('repository-relative path required')
    return git('show', ref + ':' + path)


def tree(ref: str) -> list[str]:
    return git('ls-tree', '-r', '--name-only', ref).splitlines()


def inventory() -> dict:
    before, after = tree(OLD), tree(NEW)
    paths = ['src/regional_knowledge/' + name + '.py' for name in (
        'bge_contract','stage_graph','source_span_planner','ingestion','source_adapter',
        'illustrations','illustration_mirror','source_archive','quote_proof',
        'story_ingestion','story_registry','story_reconcile','contracts','server',
        'sqlite_backend','supabase_backend','source_graph','accepted_source_reuse')]
    modules = []
    for path in paths:
        values = {}
        for label, ref, all_paths in [('old', OLD, before), ('new', NEW, after)]:
            if path not in all_paths:
                values[label] = None
                continue
            data = show(ref, path)
            values[label] = dict(lines=len(data.splitlines()),
                                 sha256=hashlib.sha256(data.encode()).hexdigest())
        modules.append(dict(path=path, **values, identical=values['old']==values['new']))
    patterns = {
        'go_rust': r'\.go$|\.rs$|(^|/)go\.mod$|(^|/)Cargo\.toml$',
        'kaggle': r'(^src/|^scripts/).*kaggle',
        'tests': r'^tests/.*(kaggle|source_import|original_intake|retrieval_acceptance|telegram|illustration|story|snapshot|recovery)',
        'benchmark': r'^scripts/.*(bench|latency|profile)',
    }
    result = dict(codec='rkb.source-audit-inventory.v1', old=OLD, new=NEW, original_spec=SPEC,
                  old_file_count=len(before), new_file_count=len(after),
                  added_count=len(set(after)-set(before)), removed_count=len(set(before)-set(after)),
                  key_modules=modules)
    result.update({key:[p for p in after if re.search(pattern,p)] for key,pattern in patterns.items()})
    result['changed_summary'] = git('diff','--shortstat',OLD,NEW).strip()
    result['claims'] = 'Source inventory only; not deployment or runtime acceptance.'
    return result


def check_docs() -> dict:
    """Read current docs, validate scope/links/gates; never mutate any file."""
    changed = sorted(set(git('diff', '--name-only', NEW).splitlines()) |
                     set(git('ls-files', '--others', '--exclude-standard').splitlines()))
    allowed = lambda p: (p.endswith('.md') or p == '.devcoveer/requirements.json'
                         or p == 'scripts/audit_rkb_source_inventory.py')
    forbidden_changes = [p for p in changed if not allowed(p)]
    broken = []
    for name in changed:
        path = ROOT / name
        if not name.endswith('.md') or not path.is_file():
            continue
        text = path.read_text(encoding='utf-8')
        for target in re.findall(r'\]\(([^\s)]+)\)', text):
            if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target) or target.startswith('#'):
                continue
            local = target.split('#', 1)[0].split('?', 1)[0]
            if local and not (path.parent / local).exists():
                broken.append(dict(path=name, target=target))
    before = json.loads(show(NEW, '.devcoveer/requirements.json'))
    after = json.loads((ROOT / '.devcoveer/requirements.json').read_text(encoding='utf-8'))
    prior = {r['id']: r for r in before['requirements']}
    current = {r['id']: r for r in after['requirements']}
    identity_ok = (set(prior) == set(current) and all(
        prior[key]['critical'] == current[key]['critical'] and
        prior[key]['statement'] == current[key]['statement'] for key in prior))
    quantitative = []
    for key, entry in prior.items():
        for rule in entry.get('acceptance', []):
            if any(term in rule for term in ('100,000', '1000 ms', '1500 ms', '2000 ms',
                                             '85%', '90%', '80%', '256-token', '512-token',
                                             '30 distinct fact')):
                if rule not in current.get(key, {}).get('acceptance', []):
                    quantitative.append(dict(id=key, missing_exact_rule=rule))
    candidates = []
    rx = re.compile(r'(?:VibePublish.{0,45}(?:sole|single|remains uploader|остаётся uploader|единствен)|'
                    r'(?:daily|ежедневн|суточн).{0,45}(?:Yandex|Яндекс)|'
                    r'(?:Yandex|Яндекс).{0,60}(?:daily|ежедневн|суточн)|'
                    r'concurrent imports|параллельных импортах|provider.digest.{0,30}fail)', re.I)
    paths = [ROOT/'AGENTS.md', *sorted((ROOT/'docs').rglob('*.md')),
             *sorted((ROOT/'.agents').rglob('*.md'))]
    for path in paths:
        name = path.relative_to(ROOT).as_posix()
        if name.startswith('docs/reports/') or not path.is_file():
            continue
        for n, line in enumerate(path.read_text(encoding='utf-8').splitlines(), 1):
            if rx.search(line):
                candidates.append(dict(path=name, line=n, text=line[:2000]))
    return dict(codec='rkb.docs-audit-check.v1', changed=changed,
                forbidden_runtime_changes=forbidden_changes, broken_local_links=broken,
                protected_ids_statements_flags_unchanged=identity_ok,
                missing_quantitative_rules=quantitative,
                remaining_policy_review_candidates=candidates,
                checked_scope='Source/docs only; no runtime or provider actions',
                passed=not forbidden_changes and not broken and identity_ok and not quantitative)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['inventory','excerpt','search','diff','files','check'])
    parser.add_argument('--ref', default=NEW)
    parser.add_argument('--path', default='')
    parser.add_argument('--pattern', default='')
    parser.add_argument('--start', type=int, default=1)
    parser.add_argument('--end', type=int, default=160)
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    if args.mode=='check':
        result=check_docs()
    elif args.mode=='inventory':
        result=inventory()
    elif args.mode=='excerpt':
        lines=show(args.ref,args.path).splitlines()
        result=dict(ref=args.ref,path=args.path,total_lines=len(lines),
                    text='\n'.join(f'{i+1}: {line}' for i,line in enumerate(lines)
                                  if args.start<=i+1<=args.end))
    elif args.mode=='diff':
        result=dict(old=OLD,new=NEW,path=args.path,
                    diff=git('diff','--unified=3',OLD,NEW,'--',args.path))
    elif args.mode=='files':
        result=dict(ref=args.ref,paths=[p for p in tree(args.ref)
                    if p.startswith(args.path) and (not args.pattern or re.search(args.pattern,p))])
    else:
        rx=re.compile(args.pattern,re.I)
        hits=[]
        for path in tree(args.ref):
            if not path.startswith(args.path) or not path.endswith(('.py','.md','.sql','.toml','.json','.yml','.yaml','.go','.rs')):
                continue
            for i,line in enumerate(show(args.ref,path).splitlines(),1):
                if rx.search(line):hits.append(dict(path=path,line=i,text=line[:1800]))
        result=dict(ref=args.ref,total=len(hits),matches=hits[:args.limit],
                    truncated=len(hits)>args.limit)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2))


if __name__=='__main__':
    main()
