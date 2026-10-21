"""DjVu multiform metadata accepts the 139-page category without OCR/decoder PASS."""
from __future__ import annotations

from pathlib import Path
import os

import pytest

from regional_knowledge.kaggle_djvu_inspect import DjvuStructureError, inspect_djvu


def _chunk(tag: bytes, data: bytes, *, pad=True):
    return tag + len(data).to_bytes(4, "big") + data + (b"\0" if len(data) % 2 and pad else b"")


def _form(tag: bytes, contents: bytes, *, pad=True):
    return _chunk(b"FORM", tag + contents, pad=pad)


def _page(index: int, *, odd=False):
    width, height = 700 + index, 900 + index
    info = _chunk(b"INFO", width.to_bytes(2, "big") + height.to_bytes(2, "big") + b"\x1a\x00\x2c\x01\x00\x00")
    body = _chunk(b"Sjbz", b"x" * (5 if odd else 6), pad=not odd)
    return _form(b"DJVU", info + body)


def fixture(page_count=2, *, dictionaries=1, odd=False):
    directory = _chunk(b"DIRM", bytes([0x81]) + (page_count + dictionaries).to_bytes(2, "big") + b"marker")
    return b"AT&T" + _form(
        b"DJVM", directory + b"".join(_form(b"DJVI", _chunk(b"INCL", b"token")) for _ in range(dictionaries))
        + b"".join(_page(i, odd=odd and i == page_count - 1) for i in range(page_count)), pad=False)


def _save(tmp_path, data):
    path = tmp_path / "input.djvu"
    path.write_bytes(data)
    return path


def test_realistic_two_page_and_dictionary_inventory_without_decoding(tmp_path):
    found = inspect_djvu(_save(tmp_path, fixture()))
    assert (found.page_count, found.shared_dictionaries, found.directory_entries) == (2, 1, 3)
    assert found.pages[0].width == 700 and found.pages[1].height == 901
    assert found.wire()["decode_ready"] is False
    assert found.wire()["format"] == "djvu"


def test_139_page_document_not_rejected_by_pilot_32_page_limit(tmp_path):
    found = inspect_djvu(_save(tmp_path, fixture(139, dictionaries=14, odd=True)))
    assert (found.page_count, found.shared_dictionaries, found.directory_entries) == (139,14,153)
    assert len(found.pages) == 139
    assert found.pages[-1].index == 138
    with pytest.raises(DjvuStructureError, match="page_count_bound"):
        inspect_djvu(_save(tmp_path, fixture(139, dictionaries=14)), max_pages=32)


@pytest.mark.parametrize("change", ["magic", "rootlength", "directory", "truncated", "noinfo", "infozero"])
def test_broken_iff_or_untrusted_page_identity_rejected(tmp_path, change):
    raw = bytearray(fixture(2))
    if change == "magic": raw[0] = ord('Q')
    elif change == "rootlength": raw[10] ^= 1
    elif change == "directory":
        at = raw.find(b"DIRM")
        raw[at+10] ^= 1
    elif change == "truncated": raw = raw[:-20]
    elif change == "noinfo":
        at = raw.find(b"INFO")
        raw[at:at+4] = b"NOPE"
    elif change == "infozero":
        at = raw.find(b"INFO")
        raw[at+8:at+10] = b"\0\0"
    with pytest.raises(DjvuStructureError):inspect_djvu(_save(tmp_path, raw))


def test_symlink_refused_and_upper_bound_enforced(tmp_path):
    file = _save(tmp_path, fixture())
    shortcut = tmp_path / "alias.djvu"
    shortcut.symlink_to(file)
    with pytest.raises(OSError):inspect_djvu(shortcut)
    with pytest.raises(DjvuStructureError, match="original_size"):
        inspect_djvu(file, max_bytes=len(file.read_bytes())-1)


def test_single_page_djvu_form(tmp_path):
    file = _save(tmp_path, b"AT&T" + _page(0))
    found = inspect_djvu(file)
    assert (found.page_count,found.directory_entries,found.shared_dictionaries) == (1,1,0)


@pytest.mark.skipif(not os.environ.get("RKB_DJVU_TEST_ORIGINAL"),
                    reason="real owner-supplied DjVu isn't committed into this public repository")
def test_actual_owner_djvu_inventory_when_explicitly_mounted():
    # Requires a private real-file fixture supplied by the authorized owner.
    found = inspect_djvu(Path(os.environ["RKB_DJVU_TEST_ORIGINAL"]))
    assert found.page_count == 139
    assert found.shared_dictionaries == 14
    assert found.directory_entries == 153
    assert found.byte_length > 2*1024*1024
    assert sum(p.text_chunks for p in found.pages) == 0
