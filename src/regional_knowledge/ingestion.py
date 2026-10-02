from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

MAX_PAGES = 2500
MAX_NATIVE_TEXT_CHARS = 24_000
MAX_NATIVE_BLOCKS = 80
MAX_NATIVE_BLOCK_TEXT = 1_000
PAGE_MAX_EDGE = 1600


@dataclass(frozen=True, slots=True)
class PdfInfo:
    page_count: int
    title: str | None = None


@dataclass(frozen=True, slots=True)
class PdfRenderedPage:
    physical_page_index: int
    mime_type: str
    data: bytes
    width: int
    height: int
    native_text: str | None
    native_blocks: tuple[dict[str, Any], ...]


class PdfProcessor(Protocol):
    async def inspect_file(self, path: Path) -> PdfInfo: ...

    async def render_file(
        self,
        path: Path,
        *,
        start: int,
        count: int,
    ) -> tuple[int, tuple[PdfRenderedPage, ...]]: ...


class PyMuPdfProcessor:
    """Deterministic PDF inspection/rendering; semantic layout stays with the model."""

    async def inspect_file(self, path: Path) -> PdfInfo:
        return await asyncio.to_thread(self._inspect_sync, path)

    async def render_file(
        self,
        path: Path,
        *,
        start: int,
        count: int,
    ) -> tuple[int, tuple[PdfRenderedPage, ...]]:
        if start < 0 or count < 1 or count > 8:
            raise ValueError("invalid page batch")
        return await asyncio.to_thread(self._render_sync, path, start, count)

    @staticmethod
    def _fitz():
        try:
            import fitz
        except ImportError as exc:  # pragma: no cover - packaging guard
            raise RuntimeError("install the 'ingest' extra for PDF ingestion") from exc
        return fitz

    @classmethod
    def _inspect_sync(cls, path: Path) -> PdfInfo:
        fitz = cls._fitz()
        with fitz.open(path) as document:
            page_count = int(document.page_count)
            if page_count < 1 or page_count > MAX_PAGES:
                raise ValueError("PDF page count is outside the supported range")
            metadata = document.metadata or {}
            title = str(metadata.get("title") or "").strip() or None
            return PdfInfo(page_count=page_count, title=title)

    @classmethod
    def _render_sync(
        cls,
        path: Path,
        start: int,
        count: int,
    ) -> tuple[int, tuple[PdfRenderedPage, ...]]:
        fitz = cls._fitz()
        output: list[PdfRenderedPage] = []
        with fitz.open(path) as document:
            total = int(document.page_count)
            if total < 1 or total > MAX_PAGES:
                raise ValueError("PDF page count is outside the supported range")
            if start >= total:
                return total, ()

            for index in range(start, min(start + count, total)):
                page = document.load_page(index)
                rect = page.rect
                longest = max(float(rect.width), float(rect.height), 1.0)
                scale = min(2.0, PAGE_MAX_EDGE / longest)
                scale = max(0.5, scale)
                pixmap = page.get_pixmap(
                    matrix=fitz.Matrix(scale, scale),
                    alpha=False,
                )
                image = pixmap.tobytes("jpeg", jpg_quality=82)

                native_text = page.get_text("text", sort=False).strip()
                if native_text:
                    native_text = native_text[:MAX_NATIVE_TEXT_CHARS]
                else:
                    native_text = None

                blocks: list[dict[str, Any]] = []
                for raw in page.get_text("blocks", sort=False):
                    if len(blocks) >= MAX_NATIVE_BLOCKS:
                        break
                    if len(raw) < 7 or int(raw[6]) != 0:
                        continue
                    text = str(raw[4]).strip()
                    if not text:
                        continue
                    x0, y0, x1, y1 = map(float, raw[:4])
                    width = max(float(rect.width), 1.0)
                    height = max(float(rect.height), 1.0)
                    bbox = {
                        "left": max(0, min(1000, round(x0 / width * 1000))),
                        "top": max(0, min(1000, round(y0 / height * 1000))),
                        "right": max(0, min(1000, round(x1 / width * 1000))),
                        "bottom": max(0, min(1000, round(y1 / height * 1000))),
                    }
                    if bbox["left"] >= bbox["right"] or bbox["top"] >= bbox["bottom"]:
                        continue
                    blocks.append(
                        {
                            "bbox": bbox,
                            "text": text[:MAX_NATIVE_BLOCK_TEXT],
                        }
                    )

                output.append(
                    PdfRenderedPage(
                        physical_page_index=index,
                        mime_type="image/jpeg",
                        data=image,
                        width=pixmap.width,
                        height=pixmap.height,
                        native_text=native_text,
                        native_blocks=tuple(blocks),
                    )
                )

        return total, tuple(output)



class SourcePdfCache:
    """Bounded disposable disk cache. Object storage remains authoritative."""

    def __init__(
        self,
        directory: Path | None = None,
        max_bytes: int | None = None,
    ) -> None:
        self.directory = directory or Path(
            os.getenv(
                "RKB_CACHE_DIR",
                str(Path.home() / ".cache" / "regional-knowledge-base"),
            )
        )
        self.max_bytes = max_bytes or int(
            os.getenv("RKB_SOURCE_CACHE_BYTES", str(1024 * 1024 * 1024))
        )
        self._lock = asyncio.Lock()

    @property
    def incoming_dir(self) -> Path:
        return self.directory / "incoming"

    def path_for(self, sha256: str) -> Path:
        if len(sha256) != 64 or any(c not in "0123456789abcdef" for c in sha256):
            raise ValueError("invalid source digest")
        return self.directory / "pdf" / f"{sha256}.pdf"

    async def adopt(self, source: Path, sha256: str) -> Path:
        async with self._lock:
            target = self.path_for(sha256)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                source.unlink(missing_ok=True)
                if await asyncio.to_thread(self._sha256, target) != sha256:
                    target.unlink(missing_ok=True)
                else:
                    os.utime(target, None)
                    return target
            os.replace(source, target)
            await asyncio.to_thread(self._prune, target)
            return target

    async def materialize(self, store, key: str, sha256: str) -> Path:
        async with self._lock:
            target = self.path_for(sha256)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if await asyncio.to_thread(self._sha256, target) == sha256:
                    os.utime(target, None)
                    return target
                target.unlink(missing_ok=True)

            fd, name = tempfile.mkstemp(
                prefix="source-",
                suffix=".pdf",
                dir=target.parent,
            )
            os.close(fd)
            temp = Path(name)
            try:
                await store.download_file(key, str(temp))
                if await asyncio.to_thread(self._sha256, temp) != sha256:
                    raise RuntimeError("source PDF integrity check failed")
                os.replace(temp, target)
                await asyncio.to_thread(self._prune, target)
                return target
            finally:
                temp.unlink(missing_ok=True)

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _prune(self, keep: Path) -> None:
        root = self.directory / "pdf"
        if not root.exists():
            return
        files = [path for path in root.glob("*.pdf") if path.is_file()]
        total = sum(path.stat().st_size for path in files)
        if total <= self.max_bytes:
            return
        for path in sorted(files, key=lambda item: item.stat().st_mtime):
            if path == keep:
                continue
            size = path.stat().st_size
            path.unlink(missing_ok=True)
            total -= size
            if total <= self.max_bytes:
                break
