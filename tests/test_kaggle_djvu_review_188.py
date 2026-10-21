"""Red-first regressions for DOT-REVIEW-188 (trust, cleanup and rotation)."""
import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

import regional_knowledge.kaggle_djvu_pixels as pixels
from regional_knowledge.kaggle_djvu_inspect import inspect_djvu
from test_kaggle_djvu_pixels import archive, bundle
from test_kaggle_djvu_inspect import fixture as djvu_fixture
from test_kaggle_native_bundle import fixture as native_fixture, prepare


def test_verified_bundle_with_unrelated_library_is_not_a_decoder_closure(native_fixture):
    executable=b"#!/bin/sh\nexit 0\n"+b"x"*1024
    library=b"some pinned but unrelated library"*75
    value=native_fixture(
        entries=[("usr/bin/ddjvu","file",executable),
                 ("usr/lib/x86_64-linux-gnu/libbogus.so","file",library)],
        files=[
            dict(path="usr/bin/ddjvu",size=len(executable),
                 sha256=__import__("hashlib").sha256(executable).hexdigest(),
                 package="fixture",member="usr/bin/ddjvu",executable=True),
            dict(path="usr/lib/x86_64-linux-gnu/libbogus.so",size=len(library),
                 sha256=__import__("hashlib").sha256(library).hexdigest(),
                 package="fixture",member="usr/lib/x86_64-linux-gnu/libbogus.so",executable=False),
        ])
    cap=prepare(value)
    assert cap.current()
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="elf|closure|library"):
        pixels._verified_decoder(cap)


@pytest.mark.asyncio
async def test_killed_oversized_decoder_closes_pipe_and_reaps_within_hard_deadline(monkeypatch,tmp_path):
    class Pipe:
        def __init__(self, event):self.event=event
        def close(self):self.event.set()
    class Transport:
        def __init__(self, event):self.pipe=Pipe(event)
        def get_pipe_transport(self,num):return self.pipe if num==1 else None
    class Reader:
        async def read(self, count):return b"x"*(pixels.MAX_PPM_BYTES+1)
    class Fake:
        def __init__(self):
            self.event=asyncio.Event()
            self._transport=Transport(self.event)
            self.stdout=Reader()
            self.returncode=None
            self.killed=False
        def kill(self):self.killed=True
        async def wait(self):
            await self.event.wait()
            self.returncode=-9
            return -9
    fake=Fake()
    async def spawn(*args,**kwargs):return fake
    monkeypatch.setattr(pixels.asyncio,"create_subprocess_exec",spawn)
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="render_output_bound"):
        await asyncio.wait_for(
            pixels._decode_single("/native/ddjvu",
                 "/native/ld-linux-x86-64.so.2","/native/lib",{"PATH":"/usr/bin"},
                 tmp_path/"verified.djvu",0,1600),timeout=2)
    assert fake.killed and fake.event.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("flag,orientation",[(6,90),(2,180),(5,270)])
async def test_info_orientation_is_explicitly_rejected_until_coordinate_transform(
        tmp_path,monkeypatch,flag,orientation):
    source=bytearray(djvu_fixture(2,dictionaries=1))
    info=source.index(b"INFO")
    source[info+8+9]=flag
    path=tmp_path/"rotated.djvu"
    path.write_bytes(source)
    inventory=inspect_djvu(path)
    assert inventory.pages[0].orientation_degrees==orientation
    monkeypatch.setattr(pixels,"PRIVATE_ROOT",tmp_path)
    with pytest.raises(pixels.DjvuPixelsUnavailable,match="orientation"):
        await pixels.render_verified_djvu_page(
            bundle=object(),original_path=path,original_bytes=len(source),
            inventory=inventory,index=0)
