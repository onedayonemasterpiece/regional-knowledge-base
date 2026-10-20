import asyncio

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
