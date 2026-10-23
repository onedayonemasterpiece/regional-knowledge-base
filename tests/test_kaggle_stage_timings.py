import json

import pytest

from regional_knowledge.kaggle_environment import bootstrap_stage


def test_bootstrap_timing_has_only_fixed_nonsecret_fields(capsys,monkeypatch):
    clock=iter((10.,12.5,20.,21.))
    monkeypatch.setattr('regional_knowledge.kaggle_environment.time.monotonic',lambda:next(clock))
    with bootstrap_stage('actual-run','stage0_download'):pass
    with pytest.raises(RuntimeError):
        with bootstrap_stage('actual-run','tokenizer_guard'):raise RuntimeError('private URL/token payload')
    rows=[json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [r['state'] for r in rows]==['start','complete','start','failed']
    assert rows[1]['duration_seconds']==2.5 and rows[3]['duration_seconds']==1.
    assert all(set(r)<={'event','run_id','stage','state','duration_seconds'} for r in rows)
    assert 'private' not in json.dumps(rows)


def test_bootstrap_timing_rejects_payload_stage_names(capsys):
    with pytest.raises(ValueError):
        with bootstrap_stage('run','https://secret.invalid'):pass
    assert capsys.readouterr().out==''
