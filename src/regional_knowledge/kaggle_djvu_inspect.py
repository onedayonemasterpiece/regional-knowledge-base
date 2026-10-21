"""Bounded, read-only DjVu IFF inventory for an already acquired original.

This deliberately does not decode DjVu page pixels, OCR scans or prove model
review. The exact original SHA is verified separately by the existing intake;
this parser only establishes structural page identities/geometry and the need
for an independently pinned decoder if book_pages is requested.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import stat


MAX_DJVU_ORIGINAL_BYTES = 512 * 1024 * 1024
MAX_DJVU_PAGES = 2500
MAX_DJVU_SHAPES = 4096
MAX_PAGE_CHUNKS = 4096


class DjvuStructureError(ValueError):
    """A bounded-format refusal, not an OCR or historical-content judgment."""


@dataclass(frozen=True)
class DjvuPage:
    index: int
    width: int
    height: int
    compressed_bytes: int
    text_chunks: int


@dataclass(frozen=True)
class DjvuInventory:
    page_count: int
    shared_dictionaries: int
    directory_entries: int
    byte_length: int
    pages: tuple[DjvuPage, ...]

    def wire(self):
        return dict(format='djvu', page_count=self.page_count,
                    shared_dictionaries=self.shared_dictionaries,
                    directory_entries=self.directory_entries,
                    byte_length=self.byte_length,
                    pages=[vars(x) for x in self.pages],
                    decode_ready=False)


def _exact(fd, position, length):
    if position < 0 or length < 0 or length > 64:
        raise DjvuStructureError('djvu_bounded_read_invalid')
    value = os.pread(fd, length, position)
    if len(value) != length:
        raise DjvuStructureError('djvu_chunk_truncated')
    return value


def _chunks(fd, start, end, max_chunks):
    position = start
    count = 0
    while position < end:
        if end-position < 8:
            raise DjvuStructureError('djvu_trailing_chunk_bytes')
        tag = _exact(fd, position, 4)
        size = int.from_bytes(_exact(fd, position + 4, 4), 'big')
        value = position + 8
        data_end = value + size
        next_position = data_end + (size & 1)
        if data_end > end or size > MAX_DJVU_ORIGINAL_BYTES:
            raise DjvuStructureError('djvu_chunk_overflow')
        # DjVu IFF omits the pad byte after the final odd-sized child inside
        # some FORM payloads. That byte belongs to the enclosing FORM instead.
        if next_position > end:
            if data_end != end:
                raise DjvuStructureError('djvu_chunk_overflow')
            next_position = end
        count += 1
        if count > max_chunks:
            raise DjvuStructureError('djvu_chunk_count_bound')
        yield tag, value, size
        position = next_position
    if position != end:
        raise DjvuStructureError('djvu_chunk_alignment')


def _page(fd, start, size, index):
    if size < 4 or _exact(fd, start, 4) != b'DJVU':
        raise DjvuStructureError('djvu_page_form_invalid')
    info = None
    text_chunks = 0
    for tag, offset, length in _chunks(fd, start+4, start+size, MAX_PAGE_CHUNKS):
        if tag == b'INFO':
            if info is not None or length < 10:
                raise DjvuStructureError('djvu_page_info_invalid')
            width = int.from_bytes(_exact(fd, offset, 2), 'big')
            height = int.from_bytes(_exact(fd, offset+2, 2), 'big')
            if not 1 <= width <= 20000 or not 1 <= height <= 20000:
                raise DjvuStructureError('djvu_page_geometry_invalid')
            info = (width, height)
        elif tag in (b'TXTa', b'TXTz'):
            text_chunks += 1
    if info is None:
        raise DjvuStructureError('djvu_page_info_missing')
    return DjvuPage(index, *info, compressed_bytes=size, text_chunks=text_chunks)


def inspect_djvu(path: str | os.PathLike, *, max_bytes=MAX_DJVU_ORIGINAL_BYTES,
                 max_pages=MAX_DJVU_PAGES):
    """Inspect real page frames without decoding pages or trusting extension.

    The file must be a regular unlinked immutable-by-caller local original.
    Missing DjVuLib decoder does not prevent truthful page-count admission,
    but cannot be mistaken for a page-rendering capability.
    """
    if (type(max_bytes) is not int or not 16 <= max_bytes <= MAX_DJVU_ORIGINAL_BYTES
            or type(max_pages) is not int or not 1 <= max_pages <= MAX_DJVU_PAGES):
        raise DjvuStructureError('djvu_inspection_budget_invalid')
    fd = os.open(Path(path), os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0))
    try:
        info = os.fstat(fd)
        size = info.st_size
        if not stat.S_ISREG(info.st_mode) or not 16 <= size <= max_bytes:
            raise DjvuStructureError('djvu_original_size_invalid')
        if _exact(fd, 0, 8) != b'AT&TFORM':
            raise DjvuStructureError('djvu_magic_invalid')
        length = int.from_bytes(_exact(fd, 8, 4), 'big')
        kind = _exact(fd, 12, 4)
        if length + 12 != size or length < 4 or kind not in (b'DJVM', b'DJVU'):
            raise DjvuStructureError('djvu_top_form_invalid')
        if kind == b'DJVU':
            pages = [_page(fd, 12, length, 0)]
            inventory = DjvuInventory(1, 0, 1, size, tuple(pages))
        else:
            pages = []
            dictionaries = 0
            directory_entries = None
            directory_offsets = None
            component_offsets = []
            first = True
            for tag, start, section_size in _chunks(fd, 16, size, MAX_DJVU_SHAPES):
                if first and tag != b'DIRM':
                    raise DjvuStructureError('djvu_directory_first_required')
                first = False
                if tag == b'DIRM':
                    if directory_entries is not None or section_size < 3:
                        raise DjvuStructureError('djvu_directory_invalid')
                    prefix = _exact(fd, start, 3)
                    if prefix[0] != 0x81:
                        raise DjvuStructureError('djvu_bundled_directory_v1_required')
                    directory_entries = int.from_bytes(prefix[1:3], 'big')
                    if not 1 <= directory_entries <= MAX_DJVU_SHAPES:
                        raise DjvuStructureError('djvu_directory_count_invalid')
                    # DIRM v1 has exactly one uncompressed BE32 offset for
                    # each component file; remaining BZZ-encoded names/flags
                    # require a pinned DjVu decoder and are not asserted here.
                    if section_size < 3 + 4 * directory_entries + 1:
                        raise DjvuStructureError('djvu_directory_offsets_truncated')
                    directory_offsets = [int.from_bytes(_exact(fd, start + 3 + 4*i, 4), 'big')
                                         for i in range(directory_entries)]
                    # Without decoding BZZ page flags we support the strict
                    # ascending subset only: never assign a *physical* FORM
                    # index to a different *directory* page by guessing.
                    if directory_offsets != sorted(set(directory_offsets)):
                        raise DjvuStructureError('djvu_directory_order_requires_decoder')
                elif tag == b'FORM':
                    if section_size < 4:
                        raise DjvuStructureError('djvu_child_form_invalid')
                    subkind = _exact(fd, start, 4)
                    if subkind in (b'DJVU', b'DJVI'):
                        component_offsets.append(start - 8)
                    if subkind == b'DJVU':
                        if len(pages) >= max_pages:
                            raise DjvuStructureError('djvu_page_count_bound')
                        pages.append(_page(fd, start, section_size, len(pages)))
                    elif subkind == b'DJVI':
                        dictionaries += 1
                    elif subkind != b'THUM':
                        raise DjvuStructureError('djvu_child_form_unsupported')
                elif tag != b'NAVM':
                    raise DjvuStructureError('djvu_top_chunk_unsupported')
            if (directory_entries is None or not pages
                    or directory_entries != len(pages) + dictionaries
                    or directory_offsets != component_offsets):
                raise DjvuStructureError('djvu_page_directory_mismatch')
            inventory = DjvuInventory(len(pages), dictionaries,
                                      directory_entries, size, tuple(pages))
        current = os.fstat(fd)
        if (current.st_dev, current.st_ino, current.st_size,
                current.st_mtime_ns, current.st_ctime_ns) != (
                info.st_dev, info.st_ino, info.st_size,
                info.st_mtime_ns, info.st_ctime_ns):
            raise DjvuStructureError('djvu_original_changed_during_inspection')
        return inventory
    finally:
        os.close(fd)
