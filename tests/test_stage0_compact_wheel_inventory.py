"""Transport deduplication must preserve exact selected wheel identities."""
from dataclasses import replace
import json

import pytest

from regional_knowledge.kaggle_stage0 import Stage0Download, Stage0Spec


def plan():
    downloads=tuple(Stage0Download(f'wheels/package{index}-1.0-py3-none-any.whl',
        f'https://fixtures.invalid/package{index}.whl',1,'a'*64) for index in range(55))
    return Stage0Spec(downloads,tuple(item.name for item in reversed(downloads)),
                      {'fixture':'1.0'},0)


def test_compact55_wheels_preserve_selection_order_pins_and_legacy_roundtrip():
    original=plan()
    compact=original.wire()
    legacy=original.wire(compact_wheels=False)
    assert compact['wheels']=={'download_indices':list(reversed(range(55)))}
    assert compact['downloads']==legacy['downloads']
    assert compact['packages']==legacy['packages']
    assert Stage0Spec.from_wire(compact)==Stage0Spec.from_wire(legacy)==original
    assert len(json.dumps(compact))<len(json.dumps(legacy))-1500
    compact['wheels']['download_indices'].clear()
    assert len(original.wheels)==55


def test_compact_never_installs_unselected_downloaded_wheel_and_keeps_cached_names():
    original=plan()
    selected=replace(original,wheels=(original.downloads[3].name,))
    assert selected.wire()['wheels']=={'download_indices':[3]}
    assert Stage0Spec.from_wire(selected.wire()).wheels==(original.downloads[3].name,)
    cached=replace(original,wheels=('cached/fixture-1.0-py3-none-any.whl',))
    assert cached.wire()['wheels']==list(cached.wheels)
    assert Stage0Spec.from_wire(cached.wire())==cached


@pytest.mark.parametrize('references',[
    {'download_indices':[True]}, {'download_indices':[-1]}, {'download_indices':[55]},
    {'download_indices':[0,0]}, {'download_indices':'0'}, {'download_indices':[0],'extra':1},
    {'other':[0]}, {'download_indices':[0]*129},
])
def test_invalid_compact_references_cannot_select_extra_or_unpinned_wheels(references):
    value=plan().wire()
    value['wheels']=references
    with pytest.raises(ValueError):
        Stage0Spec.from_wire(value)


def test_compact_references_still_validate_download_hashes_and_wheel_names():
    value=plan().wire()
    value['downloads'][0]['sha256']='invalid'
    with pytest.raises(ValueError):
        Stage0Spec.from_wire(value)
    value=plan().wire()
    value['downloads'][0]['name']='native/packages.tar'
    with pytest.raises(ValueError,match='stage0_wheel_required'):
        Stage0Spec.from_wire(value)
