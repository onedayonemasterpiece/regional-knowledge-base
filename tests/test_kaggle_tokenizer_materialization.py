"""Real tokenizer-only regression; explicitly supplied pinned assets/runtime.

Set RKB_TOKENIZER_TEST_PYTHON and RKB_TOKENIZER_TEST_MODEL_DIR to a managed
tokenizer-only environment and the original local config/tokenizer files.
No model weights, inference package, network request or dependency install runs.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


def test_real_pinned_hf_materialization_and_exact_state_rejections(tmp_path):
    python = os.getenv('RKB_TOKENIZER_TEST_PYTHON')
    model = os.getenv('RKB_TOKENIZER_TEST_MODEL_DIR')
    if not python or not model:
        pytest.skip('explicit pinned tokenizer-only runtime/assets not supplied')
    copied = tmp_path / 'model'
    copied.mkdir()
    for name in ('tokenizer.json', 'config.json', 'tokenizer_config.json', 'special_tokens_map.json'):
        shutil.copyfile(Path(model) / name, copied / name)
    source = Path(__file__).resolve().parents[1] / 'src/regional_knowledge/kaggle_bootstrap.py'
    # Isolate the exact production tokenizer functions from unrelated native PG
    # imports. The tokenizer, HF loader, config files and parser are all real.
    script = r'''
import ast,gc,hashlib,importlib.metadata,importlib.util,json,os,stat,sys
from pathlib import Path
from types import SimpleNamespace
os.environ.update(USE_TORCH='0',USE_TF='0',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',TOKENIZERS_PARALLELISM='false')
source,model=map(Path,sys.argv[1:])
pin='21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08'
assert hashlib.sha256((model/'tokenizer.json').read_bytes()).hexdigest()==pin
for name,pin_config in {
    'config.json':'26159e7ad065073448460117eb24b7a4572f6f4e78eadff65dc0a11c052449fa',
    'tokenizer_config.json':'a62b2b6784f990259fddef5f16388693a8043be4f69179e6a5257eeb3f9abac4',
    'special_tokens_map.json':'8c785abebea9ae3257b61681b4e6fd8365ceafde980c21970d001e834cf10835',
}.items():assert hashlib.sha256((model/name).read_bytes()).hexdigest()==pin_config
names={'BootstrapUnavailable','_json','_sha','_file_hash','_load_tokenizer_reference','_verify_tokenizer_reference'}
body=[n for n in ast.parse(source.read_text()).body if getattr(n,'name',None) in names]
ns=dict(Path=Path,hashlib=hashlib,importlib=importlib,json=json,os=os,stat=stat,TOKENIZER_SHA256=pin)
exec(compile(ast.Module(body=body,type_ignores=[]),'production-tokenizer-provenance','exec'),ns)
entries=tuple(SimpleNamespace(role='tokenizer' if p.name=='tokenizer.json' else 'model_config',
    requirement=SimpleNamespace(name=p.name,size=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
    for p in model.iterdir())
artifacts=SimpleNamespace(root=model,entries=entries)
load=lambda:ns['_load_tokenizer_reference'](artifacts,model)
verify=ns['_verify_tokenizer_reference']
from tokenizers import Tokenizer,normalizers,pre_tokenizers
texts=['readiness','Кёнигсберг — история\n1945.','Straße\t  Königsberg',
       'e\u0301 é\u00a0中🙂','<s> текст </s> <mask>','  a\n\n b\t ']
raw_counter=Tokenizer.from_file(str(model/'tokenizer.json'))
raw_counter.no_padding();raw_counter.no_truncation()
raw_counts=[len(raw_counter.encode(text,add_special_tokens=True).ids) for text in texts]
del raw_counter;gc.collect()
print('raw-counter-checked',file=sys.stderr,flush=True)
actual,reference=load(),load()
actual(['readiness'],padding=True,truncation=False)
assert json.loads(actual.backend_tokenizer.to_str())['normalizer']['type']=='Precompiled'
assert json.loads((model/'tokenizer.json').read_bytes())['normalizer']['type']=='Sequence'
fingerprint=verify(actual,reference)
assert len(fingerprint)==64 and fingerprint==verify(actual,reference)
print('materialized-reference-checked',file=sys.stderr,flush=True)
counts=[]
for text,raw_count in zip(texts,raw_counts):
    ids=actual.encode(text,add_special_tokens=True,padding=False,truncation=False)
    counts.append(dict(text=text,materialized=len(ids),raw=raw_count,
        ids_sha256=hashlib.sha256(json.dumps(ids,separators=(',',':')).encode()).hexdigest()))
assert any(row['materialized']!=row['raw'] for row in counts)
assert len(actual.encode('a '*510,add_special_tokens=True,padding=False,truncation=False))==512
assert len(actual.encode('a '*511,add_special_tokens=True,padding=False,truncation=False))==513
print('exact-boundaries-checked',file=sys.stderr,flush=True)
rejected=[]
changed=actual
for change in ('normalizer','pre_tokenizer','metadata','encode','truncation','added_token'):
    old_normalizer=changed.backend_tokenizer.normalizer
    old_pre_tokenizer=changed.backend_tokenizer.pre_tokenizer
    old_encode=changed.encode
    old_padding_side=changed.padding_side
    if change=='normalizer':changed.backend_tokenizer.normalizer=normalizers.Lowercase()
    if change=='pre_tokenizer':changed.backend_tokenizer.pre_tokenizer=pre_tokenizers.Whitespace()
    if change=='added_token':changed.add_tokens(['UNPROBEDTOKENMUTATION'])
    if change=='metadata':changed.padding_side='left'
    if change=='encode':changed.encode=lambda *a,**kw:[999]
    if change=='truncation':changed.backend_tokenizer.enable_truncation(max_length=12)
    try:verify(changed,reference)
    except ValueError:rejected.append(change)
    else:raise AssertionError('changed tokenizer accepted: '+change)
    print('rejected-'+change,file=sys.stderr,flush=True)
    changed.backend_tokenizer.normalizer=old_normalizer
    changed.backend_tokenizer.pre_tokenizer=old_pre_tokenizer
    changed.backend_tokenizer.no_truncation()
    changed.encode=old_encode
    changed.padding_side=old_padding_side
config=model/'tokenizer_config.json';before=config.read_bytes();config.write_bytes(before[:-1]+bytes([before[-1]^1]))
try:
    try:load()
    except ValueError:rejected.append('changed_pinned_config')
    else:raise AssertionError('changed pinned config accepted')
finally:config.write_bytes(before)
assert importlib.util.find_spec('torch') is None and 'torch' not in sys.modules
print(json.dumps(dict(fingerprint=fingerprint,counts=counts,rejected=rejected,
    exact_boundaries=[512,513],versions={name:importlib.metadata.version(name) for name in
    ('transformers','tokenizers','huggingface-hub')},torch_installed=False,torch_imported=False),ensure_ascii=False))
'''
    result = subprocess.run([python, '-c', script, str(source), str(copied)],
        capture_output=True, text=True, timeout=180, check=True)
    receipt = json.loads(result.stdout)
    assert receipt['versions'] == {'transformers': '5.16.1', 'tokenizers': '0.23.1', 'huggingface-hub': '1.29.0'}
    assert len(receipt['rejected']) == 7 and receipt['exact_boundaries'] == [512, 513]
    assert receipt['torch_installed'] is False and receipt['torch_imported'] is False
    (tmp_path / 'tokenizer-proof.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
