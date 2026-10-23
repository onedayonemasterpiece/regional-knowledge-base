"""Isolated tokenizer-only batch executable; no application/model imports."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import socket
import stat
import sys

PACKAGES = {'transformers': '5.16.1', 'tokenizers': '0.23.1', 'huggingface-hub': '1.29.0'}
FILES = (
    ('config.json', 687, '26159e7ad065073448460117eb24b7a4572f6f4e78eadff65dc0a11c052449fa'),
    ('tokenizer_config.json', 444, 'a62b2b6784f990259fddef5f16388693a8043be4f69179e6a5257eeb3f9abac4'),
    ('special_tokens_map.json', 964, '8c785abebea9ae3257b61681b4e6fd8365ceafde980c21970d001e834cf10835'),
    ('tokenizer.json', 17098108, '21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08'),
)
FINGERPRINT = '535ea403c62e784ccea787acbdee630d46d5f8f103267fa49a2d857d4f9b3710'
MAX_BATCH_BYTES = 128 * 1024
MAX_BATCH = 44


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(',', ':')).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def file_hash(path, size):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size != size:
            raise ValueError('tokenizer input mismatch')
        value = hashlib.sha256()
        remaining = size
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError('tokenizer input mismatch')
            value.update(chunk)
            remaining -= len(chunk)
        after = os.fstat(fd)
        if os.read(fd, 1) or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError('tokenizer input changed')
        return value.hexdigest()
    finally:
        os.close(fd)


def load(directory):
    root = Path(directory)
    if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
        raise ValueError('tokenizer directory required')
    if {path.name for path in root.iterdir()} != {row[0] for row in FILES}:
        raise ValueError('exact tokenizer-only inventory required')
    if any(importlib.metadata.version(name) != version for name, version in PACKAGES.items()):
        raise ValueError('tokenizer dependency mismatch')
    for name, size, digest in FILES:
        if file_hash(root / name, size) != digest:
            raise ValueError('tokenizer input mismatch')
    # Match the protected worker's full HF materialization, not raw Tokenizers.
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(str(root), use_fast=True,
        tokenizer_file=str(root / 'tokenizer.json'), local_files_only=True, trust_remote_code=False)
    if not tokenizer.is_fast or tokenizer.backend_tokenizer.truncation is not None:
        raise ValueError('tokenizer implementation mismatch')
    metadata = {name: getattr(tokenizer, name, None) for name in (
        'special_tokens_map', 'all_special_ids', 'all_special_tokens', 'model_input_names',
        'padding_side', 'truncation_side', 'model_max_length', 'clean_up_tokenization_spaces',
        'split_special_tokens')}
    for text in ('readiness', 'Кёнигсберг — история\n1945.', 'Straße\t  Königsberg',
            'e\u0301 é\u00a0中🙂', '<s> текст </s> <mask>', '  a\n\n b\t '):
        batch = tokenizer([text], padding=True, truncation=False)
        ids = tokenizer.encode(text, add_special_tokens=True, truncation=False, padding=False)
        if batch['input_ids'] != [ids]:
            raise ValueError('tokenizer exact encoding mismatch')
    backend = json.loads(tokenizer.backend_tokenizer.to_str())
    if (backend.get('padding') is not None or backend.get('truncation') is not None
            or sha(canonical({'backend': backend, 'configuration': metadata})) != FINGERPRINT):
        raise ValueError('tokenizer materialization mismatch')
    if any(name == 'torch' or name.startswith('torch.') for name in sys.modules):
        raise ValueError('tokenizer-only process required')
    return tokenizer


def main():
    import resource
    resource.setrlimit(resource.RLIMIT_CPU, (60, 60))
    resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    sys.dont_write_bytecode = True
    def deny(*args, **kwargs):
        raise RuntimeError('network disabled')
    socket.socket.connect = deny
    socket.socket.connect_ex = deny
    socket.create_connection = deny
    raw = sys.stdin.buffer.read(MAX_BATCH_BYTES + 1)
    if not 1 <= len(raw) <= MAX_BATCH_BYTES:
        raise ValueError('tokenizer batch bound')
    request = json.loads(raw)
    if (type(request) is not dict or set(request) != {'codec', 'directory', 'python_version', 'texts'}
            or request['codec'] != 'rkb.import-tokenizer.v1' or request['python_version'] != platform.python_version()
            or type(request['texts']) is not list or not 1 <= len(request['texts']) <= MAX_BATCH
            or any(type(text) is not str or not text or len(text.encode()) > 65536 for text in request['texts'])):
        raise ValueError('tokenizer request mismatch')
    tokenizer = load(request['directory'])
    counts = []
    for text in request['texts']:
        ids = tokenizer.encode(text, add_special_tokens=True, truncation=False, padding=False)
        if type(ids) is not list or any(type(value) is not int for value in ids):
            raise ValueError('tokenizer exact result required')
        counts.append({'text_sha256': sha(text.encode()), 'input_tokens': len(ids)})
    sys.stdout.buffer.write(canonical({'codec': 'rkb.import-tokenizer-result.v1',
        'python_version': platform.python_version(), 'packages': PACKAGES,
        'tokenizer_sha256': FILES[-1][2], 'materialized_sha256': FINGERPRINT, 'counts': counts}))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        # Tracebacks and third-party diagnostics may contain private input/path.
        sys.exit(2)
