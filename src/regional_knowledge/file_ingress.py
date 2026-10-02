from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import re
import socket
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

MAX_DEFAULT_BYTES = 128 * 1024 * 1024
MAX_HARD_BYTES = 512 * 1024 * 1024
DOWNLOAD_SECONDS = 45


class FileIngressError(ValueError):
    pass


def _max_bytes() -> int:
    raw = os.getenv("RKB_MAX_PDF_BYTES", str(MAX_DEFAULT_BYTES))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError("RKB_MAX_PDF_BYTES must be an integer") from exc
    if not 1 <= value <= MAX_HARD_BYTES:
        raise RuntimeError("RKB_MAX_PDF_BYTES is outside the supported bound")
    return value


def validated_public_https_host(url: str) -> str:
    try:
        if (
            not isinstance(url, str)
            or not 1 <= len(url) <= 8192
            or any(ord(c) <= 32 or ord(c) >= 127 for c in url)
            or "\\" in url
        ):
            raise ValueError
        parts = urlsplit(url)
        host = parts.hostname or ""
        if (
            parts.scheme != "https"
            or parts.username is not None
            or parts.password is not None
            or parts.fragment
            or parts.port not in {None, 443}
            or len(host) > 253
            or not re.fullmatch(
                r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", host
            )
            or any(
                not label
                or len(label) > 63
                or label.startswith("-")
                or label.endswith("-")
                for label in host.split(".")
            )
        ):
            raise ValueError
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            # Signed file URLs should use a hostname. Reject all IP literals so
            # private/link-local/metadata destinations cannot be smuggled in.
            raise ValueError
        return host.lower()
    except ValueError:
        raise FileIngressError("file_url_invalid") from None


async def _public_addresses(host: str) -> tuple[str, ...]:
    records = await asyncio.get_running_loop().getaddrinfo(
        host, 443, type=socket.SOCK_STREAM
    )
    addresses: list[str] = []
    for record in records:
        address = ipaddress.ip_address(record[4][0].split("%", 1)[0])
        if (
            not address.is_global
            or address.is_multicast
            or isinstance(address, ipaddress.IPv6Address)
            and (
                address.ipv4_mapped
                or address.sixtofour
                or address.teredo
                or address in ipaddress.ip_network("64:ff9b::/96")
            )
        ):
            raise FileIngressError("file_nonpublic_address")
        value = str(address)
        if value not in addresses:
            addresses.append(value)
    if not addresses or len(addresses) > 16:
        raise FileIngressError("file_dns_invalid")
    return tuple(addresses)


@dataclass(frozen=True, slots=True)
class DownloadedFile:
    path: Path
    sha256: str
    size_bytes: int


class _PinnedResolver:
    def __init__(self, host: str, addresses: tuple[str, ...]) -> None:
        self.host = host
        self.addresses = addresses

    async def resolve(self, host, port=0, family=socket.AF_INET):
        if host != self.host:
            raise OSError("unexpected DNS host")
        output = []
        for value in self.addresses:
            ip = ipaddress.ip_address(value)
            output.append(
                {
                    "hostname": host,
                    "host": value,
                    "port": port,
                    "family": socket.AF_INET6 if ip.version == 6 else socket.AF_INET,
                    "proto": 0,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        return output

    async def close(self):
        return None


def sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


class ChatFileDownloader:
    """Download one ephemeral client attachment to a bounded local staging file."""

    async def download(self, url: str, directory: Path) -> DownloadedFile:
        import aiohttp
        from yarl import URL

        host = validated_public_https_host(url)
        addresses = await _public_addresses(host)
        directory.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix="incoming-", suffix=".pdf", dir=directory
        )
        os.close(fd)
        temp = Path(temp_name)
        size = 0
        digest = hashlib.sha256()
        maximum = _max_bytes()
        try:
            resolver = _PinnedResolver(host, addresses)
            connector = aiohttp.TCPConnector(
                resolver=resolver,
                use_dns_cache=False,
                limit=1,
                force_close=True,
            )
            timeout = aiohttp.ClientTimeout(total=DOWNLOAD_SECONDS)
            async with asyncio.timeout(DOWNLOAD_SECONDS):
                async with aiohttp.ClientSession(
                    connector=connector,
                    trust_env=False,
                    cookie_jar=aiohttp.DummyCookieJar(),
                    auto_decompress=False,
                    timeout=timeout,
                    headers={"Accept-Encoding": "identity"},
                ) as session:
                    async with session.get(
                        URL(url, encoded=True),
                        allow_redirects=False,
                    ) as response:
                        if response.status != 200:
                            raise FileIngressError("file_download_failed")
                        if response.headers.get("Content-Encoding", "").lower() not in {
                            "",
                            "identity",
                        }:
                            raise FileIngressError("file_encoding_unsupported")
                        if (
                            response.content_length is not None
                            and response.content_length > maximum
                        ):
                            raise FileIngressError("pdf_size_limit")
                        with temp.open("wb") as out:
                            async for chunk in response.content.iter_chunked(128 * 1024):
                                size += len(chunk)
                                if size > maximum:
                                    raise FileIngressError("pdf_size_limit")
                                digest.update(chunk)
                                out.write(chunk)
            if size < 5:
                raise FileIngressError("invalid_pdf")
            with temp.open("rb") as source:
                if source.read(5) != b"%PDF-":
                    raise FileIngressError("invalid_pdf")
            return DownloadedFile(temp, digest.hexdigest(), size)
        except FileIngressError:
            temp.unlink(missing_ok=True)
            raise
        except (TimeoutError, OSError, ValueError) as exc:
            temp.unlink(missing_ok=True)
            raise FileIngressError("file_download_failed") from exc
