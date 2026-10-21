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


def fixture(page_count=2, *, dictionaries=1, odd=False, order=None):
    components = ([_form(b"DJVI", _chunk(b"INCL", b"token")) for _ in range(dictionaries)]
                  + [_page(i, odd=odd and i == page_count - 1) for i in range(page_count)])
    count = len(components)
    placeholder = _chunk(b"DIRM", bytes([0x81]) + count.to_bytes(2, "big")
                         + b"\0"*(4*count) + b"bzz-placeholder")
    offset = 16 + len(placeholder)
    offsets = []
    for component in components:
        offsets.append(offset)
        offset += len(component)
    if order is not None:
        offsets = [offsets[i] for i in order]
    directory = _chunk(b"DIRM", bytes([0x81]) + count.to_bytes(2, "big")
                       + b"".join(value.to_bytes(4,"big") for value in offsets)
                       + b"bzz-placeholder")
    assert len(directory) == len(placeholder)
    return b"AT&T" + _form(b"DJVM", directory + b"".join(components), pad=False)

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


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires POSIX FIFOs")
def test_fifo_is_refused_with_nonblocking_open_and_bounded_deadline(tmp_path):
    """A hostile FIFO must not hang at open() before structural bounds run."""
    import subprocess
    import sys

    fifo = tmp_path / "blocking.djvu"
    os.mkfifo(fifo)
    program = (
        "import sys\n"
        "from regional_knowledge.kaggle_djvu_inspect import inspect_djvu, DjvuStructureError\n"
        "try:\n"
        "    inspect_djvu(sys.argv[1])\n"
        "except DjvuStructureError:\n"
        "    sys.exit(0)\n"
    )
    # Do not hang pytest if the guarded O_NONBLOCK ever regresses.
    completed = subprocess.run(
        [sys.executable, "-c", program, str(fifo)],
        capture_output=True, text=True, timeout=3)
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("change", ["missing_table", "duplicate_offset", "wrong_offset", "reversed_directory"])
def test_bundled_dirm_offsets_must_bind_exact_embedded_form_order(tmp_path, change):
    raw = bytearray(fixture(3, dictionaries=1))
    at = raw.index(b"DIRM")
    first_offset = at + 8 + 3
    if change == "missing_table":
        # A directory count without corresponding file offsets is not complete.
        raw[first_offset - 1] = 0xff
    elif change == "duplicate_offset":
        raw[first_offset + 4:first_offset + 8] = raw[first_offset:first_offset + 4]
    elif change == "wrong_offset":
        raw[first_offset + 3] ^= 2
    elif change == "reversed_directory":
        first = bytes(raw[first_offset:first_offset + 4])
        last = bytes(raw[first_offset + 12:first_offset + 16])
        raw[first_offset:first_offset + 4] = last
        raw[first_offset + 12:first_offset + 16] = first
    with pytest.raises(DjvuStructureError, match="directory|offsets|page"):
        inspect_djvu(_save(tmp_path, raw))


def test_directory_order_page_indices_are_exact_not_physical_heuristic(tmp_path):
    found=inspect_djvu(_save(tmp_path,fixture(3,dictionaries=2)))
    assert [page.index for page in found.pages] == [0,1,2]
    assert [page.width for page in found.pages] == [700,701,702]
