from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol


class ObjectStore(Protocol):
    async def get_range(self, key: str, start: int, end: int) -> bytes: ...
    async def get_bytes(self, key: str) -> bytes: ...
    async def put_bytes(self, key: str, data: bytes, content_type: str) -> None: ...


class UnavailableObjectStore:
    async def get_range(self, key: str, start: int, end: int) -> bytes:
        raise RuntimeError("object store is not configured")

    async def get_bytes(self, key: str) -> bytes:
        raise RuntimeError("object store is not configured")

    async def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        raise RuntimeError("object store is not configured")


@dataclass(frozen=True, slots=True)
class S3Config:
    endpoint_url: str
    region_name: str
    bucket: str
    access_key_id: str
    secret_access_key: str


class S3ObjectStore:
    """Private S3-compatible corpus store with bounded server-side access."""

    def __init__(self, config: S3Config) -> None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - deployment packaging guard
            raise RuntimeError("install the 'ingest' extra for S3 support") from exc
        self.bucket = config.bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
        )

    async def get_range(self, key: str, start: int, end: int) -> bytes:
        if start < 0 or end <= start:
            raise ValueError("invalid object byte range")

        def load() -> bytes:
            result = self.client.get_object(
                Bucket=self.bucket,
                Key=key,
                Range=f"bytes={start}-{end - 1}",
            )
            return result["Body"].read()

        return await asyncio.to_thread(load)

    async def get_bytes(self, key: str) -> bytes:
        def load() -> bytes:
            result = self.client.get_object(Bucket=self.bucket, Key=key)
            return result["Body"].read()

        return await asyncio.to_thread(load)

    async def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        def put() -> None:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )

        await asyncio.to_thread(put)
