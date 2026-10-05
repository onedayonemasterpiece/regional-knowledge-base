"""Prepare non-secret retrieval runtime assets/config for the measured BGE-first path."""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path

from regional_knowledge.bge_contract import TOKENIZER_SHA256

DEFAULT_DEST = Path("/home/dev/.local/share/regional-knowledge-base/fast-e5/bge-m3-tokenizer.json")
DEFAULT_ENV = Path("/home/dev/.local/state/regional-knowledge-base/service.env")
SETTINGS = {
    "RKB_BGE_WARM_MODE": "bge",
    "RKB_BGE_QUERY_WAIT_SECONDS": "0.8",
    "RKB_LEXICAL_BUDGET_MS": "100",
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def install_tokenizer(source: Path, destination: Path) -> None:
    if not source.is_file() or digest(source) != TOKENIZER_SHA256:
        raise ValueError("BGE tokenizer source/hash mismatch")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.exists() and digest(destination) == TOKENIZER_SHA256:
        destination.chmod(0o600)
        return
    fd, tmp_name = tempfile.mkstemp(prefix=".bge-tokenizer-", dir=destination.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as out, source.open("rb") as inp:
            shutil.copyfileobj(inp, out, length=1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        tmp.chmod(0o600)
        if digest(tmp) != TOKENIZER_SHA256:
            raise ValueError("copied BGE tokenizer hash mismatch")
        os.replace(tmp, destination)
    finally:
        if tmp.exists():
            tmp.unlink()


def update_env(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(path)
    original = path.read_text()
    lines = original.splitlines()
    found: set[str] = set()
    output: list[str] = []
    for line in lines:
        replacement = None
        for key, value in SETTINGS.items():
            if re.match(rf"^\s*(?:export\s+)?{re.escape(key)}=", line):
                replacement = f"{key}={value}"
                found.add(key)
                break
        output.append(replacement if replacement is not None else line)
    for key, value in SETTINGS.items():
        if key not in found:
            output.append(f"{key}={value}")
    rendered = "\n".join(output) + "\n"
    if rendered == original:
        return
    mode = path.stat().st_mode & 0o777
    fd, tmp_name = tempfile.mkstemp(prefix=".service-env-", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.chmod(mode)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bge-tokenizer", type=Path, required=True)
    parser.add_argument("--destination", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--service-env", type=Path, default=DEFAULT_ENV)
    args = parser.parse_args()
    install_tokenizer(args.bge_tokenizer, args.destination)
    update_env(args.service_env)
    print({
        "tokenizer_sha256": digest(args.destination),
        "updated_keys": sorted(SETTINGS),
        "destination": str(args.destination),
    })


if __name__ == "__main__":
    main()
