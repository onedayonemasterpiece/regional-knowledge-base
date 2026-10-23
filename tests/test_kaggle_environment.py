import asyncio
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_environment import observe_environment, validate_environment


def test_observation_is_bounded_and_contains_no_runtime_authority():
    value = observe_environment()
    assert validate_environment(value) == value
    assert value['cache_tag'].startswith('cpython-')
    assert value['machine']
    for invalid in ({**value, 'token': 'private'}, {**value, 'python': 'x' * 1025},
                    {**value, 'libc_version': 'glibc\n2.35'}, {**value, 'os_id': None}):
        with pytest.raises(ValueError):
            validate_environment(invalid)


def test_native_identity_uses_bounded_local_accounts_without_private_fields(tmp_path,monkeypatch):
    import regional_knowledge.kaggle_environment as module
    passwd=tmp_path/'passwd'
    passwd.write_text('root:x:0:0:private root:/root:/bin/bash\n'
        'service:x:123:456:private name:/private/home:/bin/false\n'
        'alias:x:124:456:private:/hidden:/bin/false\n')
    monkeypatch.setattr(module,'_PASSWD',passwd)
    monkeypatch.setattr(module.os,'geteuid',lambda:0)
    monkeypatch.setattr(module.os,'getegid',lambda:0)
    monkeypatch.setattr(module.os,'getgroups',lambda:[0,1,1])
    monkeypatch.setattr(module.pwd,'getpwuid',lambda uid:SimpleNamespace(
        pw_name='service' if uid==123 else 'different-name',pw_gid=456))
    value=module.observe_environment()
    assert value['identity']==dict(uid=0,gid=0,groups=[0,1],accounts_source='local_passwd',
        accounts=[dict(name='service',uid=123,gid=456)])
    assert 'private' not in str(value) and 'home' not in str(value)
    legacy={key:item for key,item in value.items() if key!='identity'}
    assert module.validate_environment(legacy)==legacy
    for changes in ({'uid':True},{'groups':[1,1]}, {'groups':list(range(65))},
                    {'accounts':[dict(name='root',uid=0,gid=0)]},
                    {'accounts':[dict(name='service',uid=123,gid=456,home='/private')]}):
        with pytest.raises(ValueError):
            module.validate_environment(dict(value,identity=dict(value['identity'],**changes)))
    passwd.write_bytes(b'x'*65537)
    assert module.observe_environment()['identity']['accounts_source']=='unavailable'
    assert not module.observe_environment()['identity']['accounts']


@pytest.mark.asyncio
async def test_target_environment_sent_before_optional_runtime_load():
    from test_bge_wss_worker import CONFIG, Wire, module
    transport = module('bge_wss_transport')
    stop = asyncio.Event()
    wire = Wire(stop)

    def load():
        first = wire.sent[0]
        assert first['type'] == 'ATTACH'
        assert validate_environment(first['environment']) == observe_environment()
        assert not any(frame['type'] == 'READY' for frame in wire.sent)
        return lambda text: [1.] + [0.] * 1023

    worker = transport.BgeWssTransport({**CONFIG, 'target_runtime': {}}, load,
        lambda: {}, connector=wire.connect, heartbeat_seconds=.01,
        maximum_seconds=3, stop=stop)
    await worker.run()
    assert wire.sent[0]['environment']['python']
