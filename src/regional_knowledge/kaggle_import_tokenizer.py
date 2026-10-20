"""Exact HF source counters; isolated tokenizer-only batches never run a model."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import threading
from types import SimpleNamespace

from .bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from .kaggle_import_tokenizer_process import FILES, PACKAGES, FINGERPRINT, MAX_BATCH, MAX_BATCH_BYTES, file_hash


@dataclass(frozen=True)
class TokenizerProcessSpec:
    python: str
    python_sha256: str
    helper_sha256: str
    python_version: str = '3.13.15'
    deadline_seconds: float = 45

    def __post_init__(self):
        if (type(self.python) is not str or not Path(self.python).is_absolute() or len(self.python) > 1024
                or any(type(value) is not str or re.fullmatch('[a-f0-9]{64}', value) is None
                    for value in (self.python_sha256, self.helper_sha256))
                or self.python_version != '3.13.15' or type(self.deadline_seconds) not in (int, float)
                or not math.isfinite(self.deadline_seconds) or not 1 <= self.deadline_seconds <= 60):
            raise ValueError('explicit pinned tokenizer process required')

    def wire(self):
        return dict(codec='rkb.import-tokenizer-process.v1', **self.__dict__)

    @classmethod
    def from_wire(cls, value):
        if type(value) is not dict or set(value) != {'codec', 'python', 'python_sha256', 'helper_sha256',
                'python_version', 'deadline_seconds'} or value['codec'] != 'rkb.import-tokenizer-process.v1':
            raise ValueError('exact tokenizer process configuration required')
        return cls(**{key: field for key, field in value.items() if key != 'codec'})


def tokenizer_process_manifest():
    helper = Path(__file__).with_name('kaggle_import_tokenizer_process.py')
    return dict(codec='rkb.import-tokenizer-requirements.v1', python_version='3.13.15', packages=dict(PACKAGES),
        files=[dict(name=name, size=size, sha256=digest) for name, size, digest in FILES],
        helper_sha256=file_hash(helper, helper.stat().st_size), materialized_sha256=FINGERPRINT,
        model_revision=REVISION, space=SPACE, tokenizer_sha256=TOKENIZER_SHA256)


class HfImportTokenCounter:
    revision, space, tokenizer_sha256 = REVISION, SPACE, TOKENIZER_SHA256

    def __init__(self, directory, *, process=None):
        root = Path(directory)
        if not root.is_absolute() or process is not None and type(process) is not TokenizerProcessSpec:
            raise ValueError('explicit pinned tokenizer configuration required')
        self.root, self.process = root, process
        self._tokenizer = None
        self._lock = threading.Lock()

    def _load(self):
        if self._tokenizer is None:
            from .kaggle_bootstrap import PreparedArtifact, _load_tokenizer_reference, _verify_tokenizer_reference
            from .kaggle_readiness import ArtifactRequirement
            if self.root.resolve(strict=True) != self.root:
                raise ValueError('immutable tokenizer directory required')
            requirements = tuple(PreparedArtifact(ArtifactRequirement(name, size, digest),
                'tokenizer' if name == 'tokenizer.json' else 'model_config') for name, size, digest in FILES)
            assets = SimpleNamespace(root=self.root, entries=requirements)
            tokenizer = _load_tokenizer_reference(assets, self.root)
            if _verify_tokenizer_reference(tokenizer, tokenizer) != FINGERPRINT:
                raise ValueError('protected tokenizer materialization required')
            self._tokenizer = tokenizer
        return self._tokenizer

    async def _process(self, texts):
        spec = TokenizerProcessSpec.from_wire(self.process.wire())
        helper = Path(__file__).with_name('kaggle_import_tokenizer_process.py')
        executable = Path(spec.python).resolve(strict=True)
        def verify():
            for path, expected in ((executable, spec.python_sha256), (helper, spec.helper_sha256)):
                size = path.stat().st_size
                if not 1 <= size <= 128 * 1024 * 1024 or file_hash(path, size) != expected:
                    raise ValueError('tokenizer executable pin mismatch')
        await asyncio.to_thread(verify)
        data = json.dumps(dict(codec='rkb.import-tokenizer.v1', directory=str(self.root),
            python_version=spec.python_version, texts=texts), ensure_ascii=False, allow_nan=False).encode()
        if len(data) > MAX_BATCH_BYTES:
            raise ValueError('tokenizer batch byte bound')
        process = await asyncio.create_subprocess_exec(spec.python, '-I', str(helper),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            env={'PATH':'/usr/bin:/bin', 'HF_HUB_OFFLINE':'1', 'TRANSFORMERS_OFFLINE':'1',
                'HF_HUB_DISABLE_TELEMETRY':'1', 'USE_TORCH':'0', 'USE_TF':'0', 'USE_FLAX':'0',
                'TOKENIZERS_PARALLELISM':'false'})
        try:
            async with asyncio.timeout(spec.deadline_seconds):
                process.stdin.write(data)
                await process.stdin.drain()
                process.stdin.close()
                result = bytearray()
                while chunk := await process.stdout.read(4096):
                    result.extend(chunk)
                    if len(result) > 16384:
                        raise ValueError('tokenizer result bound')
                if await process.wait() != 0:
                    raise ValueError('tokenizer-only process unavailable')
            value = json.loads(result)
            if (type(value) is not dict or set(value) != {'codec','python_version','packages','tokenizer_sha256',
                    'materialized_sha256','counts'} or value['codec'] != 'rkb.import-tokenizer-result.v1'
                    or value['python_version'] != spec.python_version or value['packages'] != PACKAGES
                    or value['tokenizer_sha256'] != TOKENIZER_SHA256 or value['materialized_sha256'] != FINGERPRINT
                    or type(value['counts']) is not list or len(value['counts']) != len(texts)):
                raise ValueError('tokenizer result provenance mismatch')
            counts = []
            for text, row in zip(texts, value['counts'], strict=True):
                if (type(row) is not dict or set(row) != {'text_sha256','input_tokens'}
                        or row['text_sha256'] != hashlib.sha256(text.encode()).hexdigest()
                        or type(row['input_tokens']) is not int or not 1 <= row['input_tokens'] <= 100000):
                    raise ValueError('tokenizer complete input mismatch')
                counts.append(row['input_tokens'])
            return counts
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def counts(self, texts):
        if (type(texts) is not list or not 1 <= len(texts) <= MAX_BATCH
                or any(type(text) is not str or not text or len(text.encode()) > 65536 for text in texts)
                or sum(len(text.encode()) for text in texts) > MAX_BATCH_BYTES - 2048):
            raise ValueError('bounded complete source batch required')
        if self.process is not None:
            return await self._process(texts)
        def exact():
            with self._lock:
                tokenizer = self._load()
                counts = []
                for text in texts:
                    ids = tokenizer.encode(text, add_special_tokens=True, truncation=False, padding=False)
                    if type(ids) is not list or any(type(value) is not int for value in ids):
                        raise ValueError('exact tokenizer result required')
                    counts.append(len(ids))
                return counts
        task = asyncio.create_task(asyncio.to_thread(exact))
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        if cancelled:
            if not task.cancelled():
                task.exception()
            raise asyncio.CancelledError
        return task.result()

    async def count(self, text):
        return (await self.counts([text]))[0]
