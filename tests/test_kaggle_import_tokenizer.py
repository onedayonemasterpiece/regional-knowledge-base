"""Bounded isolated counter protocol and opt-in real protected tokenizer proof."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

from regional_knowledge.kaggle_import_tokenizer import (
    HfImportTokenCounter, TokenizerProcessSpec, tokenizer_process_manifest,
)
from regional_knowledge.kaggle_import_tokenizer_process import FINGERPRINT, PACKAGES
from regional_knowledge.bge_contract import TOKENIZER_SHA256


class Output:
    def __init__(self, value):self.value=value
    async def read(self, maximum):
        result,self.value=self.value[:maximum],self.value[maximum:]
        return result


class Input:
    def __init__(self):self.data=bytearray()
    def write(self, value):self.data.extend(value)
    async def drain(self):pass
    def close(self):pass


class Process:
    def __init__(self, value):self.stdout=Output(value);self.stdin=Input();self.returncode=None;self.killed=False
    async def wait(self):self.returncode=0;return 0
    def kill(self):self.killed=True;self.returncode=-9


def reply(texts):
    return dict(codec='rkb.import-tokenizer-result.v1',python_version='3.13.15',packages=PACKAGES,
        tokenizer_sha256=TOKENIZER_SHA256,materialized_sha256=FINGERPRINT,
        counts=[dict(text_sha256=hashlib.sha256(text.encode()).hexdigest(),input_tokens=7) for text in texts])


def spec(**changes):
    return TokenizerProcessSpec(str(Path(sys.executable).absolute()),
        hashlib.sha256(Path(sys.executable).resolve().read_bytes()).hexdigest(),
        tokenizer_process_manifest()['helper_sha256'], **changes)


@pytest.mark.asyncio
@pytest.mark.parametrize('case',['wrong_hash','wrong_fingerprint','wrong_count','extra_field','oversized','malformed'])
async def test_process_cannot_supply_unbound_or_malformed_counts(tmp_path, monkeypatch, case):
    texts=['private-source'];value=reply(texts)
    if case=='wrong_hash':value['counts'][0]['text_sha256']='0'*64
    if case=='wrong_fingerprint':value['materialized_sha256']='0'*64
    if case=='wrong_count':value['counts'][0]['input_tokens']=True
    if case=='extra_field':value['accepted']=True
    body=json.dumps(value).encode()
    if case=='oversized':body=b' '*16385
    if case=='malformed':body=b'not-json'
    child=Process(body)
    async def create(*args,**kwargs):return child
    monkeypatch.setattr(asyncio,'create_subprocess_exec',create)
    counter=HfImportTokenCounter(tmp_path,process=spec())
    with pytest.raises((ValueError,json.JSONDecodeError)):
        await counter.counts(texts)
    assert child.returncode is not None


@pytest.mark.asyncio
async def test_one_bounded_batch_is_isolated_and_exec_pins_checked_before_spawn(tmp_path,monkeypatch):
    texts=['one exact source', 'two exact sources'];child=Process(json.dumps(reply(texts)).encode());calls=[]
    async def create(*args,**kwargs):calls.append((args,kwargs));return child
    monkeypatch.setattr(asyncio,'create_subprocess_exec',create)
    counter=HfImportTokenCounter(tmp_path,process=spec())
    assert await counter.counts(texts)==[7,7]
    assert len(calls)==1 and calls[0][0][1]=='-I'
    assert calls[0][1]['env']['USE_TORCH']=='0'
    assert 'HF_TOKEN' not in calls[0][1]['env'] and 'PYTHONPATH' not in calls[0][1]['env']
    assert json.loads(child.stdin.data)['texts']==texts
    bad=TokenizerProcessSpec(counter.process.python,'0'*64,counter.process.helper_sha256)
    with pytest.raises(ValueError,match='pin mismatch'):
        await HfImportTokenCounter(tmp_path,process=bad).counts(texts)
    assert len(calls)==1


@pytest.mark.asyncio
async def test_cancelled_process_is_killed_and_reaped_without_source_output(tmp_path,monkeypatch):
    child=Process(b'');entered=asyncio.Event()
    async def blocked(maximum):entered.set();await asyncio.Future()
    child.stdout.read=blocked
    async def create(*args,**kwargs):return child
    monkeypatch.setattr(asyncio,'create_subprocess_exec',create)
    task=asyncio.create_task(HfImportTokenCounter(tmp_path,process=spec()).counts(['private text']))
    await asyncio.wait_for(entered.wait(),2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert child.killed and child.returncode is not None


@pytest.mark.asyncio
async def test_real_isolated_hf_counts_specials_whitespace_and_512_boundary(tmp_path):
    python=os.getenv('RKB_TOKENIZER_TEST_PYTHON');directory=os.getenv('RKB_TOKENIZER_TEST_MODEL_DIR')
    if not python or not directory:
        pytest.skip('explicit protected tokenizer-only runtime and four local files required')
    process=TokenizerProcessSpec(python,hashlib.sha256(Path(python).resolve().read_bytes()).hexdigest(),
        tokenizer_process_manifest()['helper_sha256'])
    counter=HfImportTokenCounter(directory,process=process)
    texts=['<s> текст </s> <mask>','  a\n\n b\t ','a '*510,'a '*511,
        'Кёнигсберг — история\n1945.','Straße\t  Königsberg','e\u0301 é\u00a0中🙂']
    counts=await counter.counts(texts)
    assert counts[:4]==[6,4,512,513]
    copied=tmp_path/'bad-config';copied.mkdir()
    for row in tokenizer_process_manifest()['files']:
        shutil.copyfile(Path(directory)/row['name'],copied/row['name'])
    with (copied/'tokenizer_config.json').open('ab') as output:output.write(b' ')
    with pytest.raises(ValueError,match='process unavailable'):
        await HfImportTokenCounter(copied,process=process).counts(texts)


@pytest.mark.asyncio
async def test_process_deadline_reaps_child_and_oversize_batch_never_spawns(tmp_path,monkeypatch):
    child=Process(b'');calls=[]
    async def blocked(maximum):await asyncio.Future()
    child.stdout.read=blocked
    async def create(*args,**kwargs):calls.append(args);return child
    monkeypatch.setattr(asyncio,'create_subprocess_exec',create)
    counter=HfImportTokenCounter(tmp_path,process=spec(deadline_seconds=1))
    with pytest.raises(ValueError):await counter.counts(['text']*45)
    assert not calls
    with pytest.raises(TimeoutError):await counter.counts(['private text'])
    assert child.killed and child.returncode is not None
