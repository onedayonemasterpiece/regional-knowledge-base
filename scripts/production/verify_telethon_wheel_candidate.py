#!/usr/bin/env python3
"""CI-only: verify official PyPI pins, reproducibly build pyaes, smoke API, emit lock.

Network: official PyPI JSON + hash-pinned distribution bytes during GitHub-hosted
CI only. No project credentials, provider calls, Telegram connectivity, PG or
installed model. Never run on DevCoveer/Kaggle. Emits metadata, not a release.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from packaging.specifiers import SpecifierSet
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.parse
import urllib.request

PINS = {
    "telethon": ("1.45.0", "telethon-1.45.0-py3-none-any.whl",
                 "339e24c83aedc9f12cbe389264627edc49779e4289c535aa90aafc4ce62f7972"),
    "rsa": ("4.9.1", "rsa-4.9.1-py3-none-any.whl",
            "68635866661c6836b8d39430f97a996acbd61bfa49406748ea243539fe239762"),
    "pyasn1": ("0.6.4", "pyasn1-0.6.4-py3-none-any.whl",
               "deda9277cfd454080ec40b207fb6df82206a3a2688735233cdcd8d3d565f088b"),
}
PYAES = ("1.6.1", "pyaes-1.6.1.tar.gz",
         "02c1b1405c38d3c370b085fb952dd8bea3fadcee6411ad99f312cc129c536d8f")
EXPECTED_NAMES = {
    "telethon": {"pyaes", "rsa"},
    "rsa": {"pyasn1"},
    "pyasn1": set(),
    "pyaes": set(),
}
SOURCE_DATE_EPOCH = "1700000000"
BUILD_TOOLS = {"pip": "25.2", "setuptools": "80.9.0", "wheel": "0.45.1"}
MAX_HTTP_METADATA = 2 * 1024 * 1024
MAX_WHEEL = 2 * 1024 * 1024
MAX_SDIST = 120_000


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(data: object) -> bytes:
    return (json.dumps(data, ensure_ascii=False, allow_nan=False,
                       separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def fetch(url: str, size_cap: int) -> bytes:
    # No metadata may direct downloads off official PyPI file storage.
    parts = urllib.parse.urlsplit(url)
    if (parts.scheme != "https" or parts.hostname not in
            {"pypi.org", "files.pythonhosted.org"} or parts.username
            or parts.password or parts.query or parts.fragment):
        raise RuntimeError("unapproved artifact URL host or shape")
    request = urllib.request.Request(url, headers={"User-Agent": "RKB-candidate-wheel-CI/1"})
    with urllib.request.urlopen(request, timeout=25) as response:
        if urllib.parse.urlsplit(response.geturl()).hostname not in {
                "pypi.org", "files.pythonhosted.org"}:
            raise RuntimeError("unapproved redirect")
        data = response.read(size_cap + 1)
    if len(data) > size_cap:
        raise RuntimeError("bounded metadata/artifact too large")
    return data


def load_pypi(name: str, version: str) -> dict:
    raw = fetch(f"https://pypi.org/pypi/{name}/{version}/json", MAX_HTTP_METADATA)
    metadata = json.loads(raw)
    if (metadata.get("info", {}).get("version") != version
            or metadata["info"]["name"].lower().replace("-", "_") != name.lower().replace("-", "_")):
        raise RuntimeError(f"PyPI release identity mismatch: {name}")
    return metadata


def validate_dependencies(name: str, metadata: dict) -> None:
    actual = set()
    for req in metadata["info"].get("requires_dist") or []:
        # Ignore explicitly optional extras, never implicitly install cryptg.
        if re.search(r"extra\s*==", req, flags=re.I):
            continue
        match = re.match(r"^\s*([A-Za-z0-9._-]+)", req)
        if not match:
            raise RuntimeError(f"unrecognized dependency requirement for {name}")
        actual.add(match.group(1).lower().replace("_", "-"))
    if actual != EXPECTED_NAMES[name]:
        raise RuntimeError(f"dependency closure changed for {name}: {sorted(actual)}")


def select(meta: dict, filename: str, expected_sha: str, kind: str) -> dict:
    options = [item for item in meta.get("urls", [])
               if item.get("filename") == filename and item.get("packagetype") == kind]
    if len(options) != 1:
        raise RuntimeError(f"exact PyPI release asset missing: {filename}")
    item = options[0]
    if (item.get("yanked") or item.get("digests", {}).get("sha256") != expected_sha
            or type(item.get("size")) is not int or item["size"] <= 0):
        raise RuntimeError(f"official release artifact SHA/size/yanked mismatch: {filename}")
    url = item["url"]
    parts = urllib.parse.urlsplit(url)
    if (parts.scheme != "https" or parts.hostname != "files.pythonhosted.org"
            or not parts.path.endswith("/" + filename) or parts.query or parts.fragment):
        raise RuntimeError("noncanonical official artifact URL")
    return {"filename": filename, "size": item["size"],
            "sha256": expected_sha, "url": url,
            "requires_python": item.get("requires_python")}


def acquire_verified(item: dict, destination: Path, cap: int) -> None:
    if item["size"] > cap:
        raise RuntimeError(f"approved size cap exceeded: {item['filename']}")
    downloaded = fetch(item["url"], cap)
    if len(downloaded) != item["size"] or sha256(downloaded) != item["sha256"]:
        raise RuntimeError(f"downloaded bytes differ from PyPI JSON: {item['filename']}")
    destination.write_bytes(downloaded)


def clean_unpack(sdist: Path, dest: Path) -> Path:
    dest.mkdir(parents=True)
    with tarfile.open(sdist, "r:gz") as tar:
        members = tar.getmembers()
        if len(members) > 250:
            raise RuntimeError("sdist member count exceeded")
        for member in members:
            path = Path(member.name)
            if (path.is_absolute() or ".." in path.parts or path.parts[0] != "pyaes-1.6.1"
                    or not (member.isfile() or member.isdir())
                    or member.size > MAX_SDIST):
                raise RuntimeError("sdist traversal/symlink/type/size rejected")
        tar.extractall(dest, filter="data")
    source = dest / "pyaes-1.6.1"
    if not (source / "setup.py").is_file():
        raise RuntimeError("pyaes setup.py absent")
    return source


def build_pyaes(sdist: Path, name: str, root: Path) -> tuple[Path, dict]:
    directory = root / name
    source = clean_unpack(sdist, directory / "source")
    wheels = directory / "wheels"
    wheels.mkdir()
    env = dict(os.environ, SOURCE_DATE_EPOCH=SOURCE_DATE_EPOCH,
               PYTHONHASHSEED="0", PYTHONDONTWRITEBYTECODE="1",
               TZ="UTC", LC_ALL="C.UTF-8", PIP_DISABLE_PIP_VERSION_CHECK="1",
               PIP_NO_INDEX="1", PIP_NO_INPUT="1")
    # All build tools are installed/pinned by the existing workflow step.
    result = subprocess.run(
        [sys.executable, "-m", "pip", "wheel", "--no-deps",
         "--no-build-isolation", "--no-index", "--wheel-dir", str(wheels), str(source)],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=75)
    if result.returncode:
        raise RuntimeError("pyaes wheel build failure: " + result.stdout[-2500:])
    files = list(wheels.iterdir())
    if len(files) != 1 or not files[0].name.startswith("pyaes-1.6.1-") or files[0].suffix != ".whl":
        raise RuntimeError("unexpected wheel filename/count")
    output = files[0]
    digest = sha256(output.read_bytes())
    return output, {"filename": output.name, "size": output.stat().st_size,
                    "sha256": digest}


def test_actual_api(folder: Path, out: Path) -> dict:
    # This subprocess uses only the four local --no-deps wheels, and denies
    # ALL sockets including localhost. No session/authorization happens.
    smoke = r'''
import asyncio
import inspect
import json
import os
import socket
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import telethon, pyaes, rsa, pyasn1
from telethon import TelegramClient
from telethon.sessions import StringSession
assert telethon.__version__ == "1.45.0"
assert hasattr(TelegramClient, "get_messages")
assert hasattr(TelegramClient, "iter_download")
assert hasattr(TelegramClient, "is_user_authorized")
assert "request_size" in inspect.signature(TelegramClient.iter_download).parameters
assert "chunk_size" in inspect.signature(TelegramClient.iter_download).parameters
device={"app_version":"RKB-test-only","device_model":"fake-device","lang_code":"en",
        "system_lang_code":"en","system_version":"test-os"}
def fake_session(value):
    assert value == ""
    return StringSession(value)
def fake_client(session, app_id, app_hash, **kwargs):
    assert isinstance(session, StringSession)
    assert app_id == 123456 and app_hash == "0"*32
    assert {k:kwargs[k] for k in device} == device
    assert {k:kwargs[k] for k in ("auto_reconnect","connection_retries","request_retries")} == {
        "auto_reconnect":False,"connection_retries":0,"request_retries":0}
    return type("SyntheticClient",(),{
        "get_messages":lambda *args,**kw:None,
        "iter_download":lambda *args,**kw:None,
        "is_user_authorized":lambda *args,**kw:None})()
fake=fake_client(fake_session(""),123456,"0"*32,**device,auto_reconnect=False,
                 connection_retries=0,request_retries=0)
assert all(callable(getattr(fake,k,None)) for k in ("get_messages","iter_download","is_user_authorized"))
_original_socket=socket.socket
def forbidden(*args,**kwargs):
    raise AssertionError("network socket forbidden in Telethon candidate test")
socket.socket=forbidden
try:
    c=TelegramClient(StringSession(),123456,"0"*32,**device,
                     auto_reconnect=False,connection_retries=0,request_retries=0)
    assert c.is_connected() is False
    assert callable(c.get_messages) and callable(c.iter_download)
finally:
    socket.socket=_original_socket
print(json.dumps({"python":sys.version.split()[0],"telethon":telethon.__version__,
  "factory_fake":True,"actual_client_constructed":True,"network_forbidden":True,
  "get_messages":True,"iter_download":True,"is_user_authorized":True,
  "string_session":True},sort_keys=True))
'''
    result = subprocess.run([sys.executable, "-I", "-c", smoke, str(folder)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, timeout=20, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    if result.returncode:
        raise RuntimeError("offline Telethon API smoke failed: " + result.stderr[-2000:])
    result_data = json.loads(result.stdout.strip())
    out.write_bytes(canonical(result_data))
    return result_data


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base-revision", required=True)
    args = parser.parse_args()
    if sys.version_info[:2] not in ((3, 12), (3, 13)):
        raise RuntimeError("explicit CPython 3.12/3.13 only")
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="rkb-telethon-ci-") as temp:
        root = Path(temp)
        metadata = {}
        assets = {}
        for name in (*PINS.keys(), "pyaes"):
            version, filename, expected = PINS.get(name, PYAES)
            meta = load_pypi(name, version)
            validate_dependencies(name, meta)
            required_python = meta["info"].get("requires_python")
            if required_python and f"{sys.version_info.major}.{sys.version_info.minor}" not in SpecifierSet(required_python):
                raise RuntimeError(f"candidate does not support this CPython: {name}")
            required = "sdist" if name == "pyaes" else "bdist_wheel"
            asset = select(meta, filename, expected, required)
            if name == "pyaes" and any(
                    x.get("packagetype") == "bdist_wheel" for x in meta["urls"]):
                raise RuntimeError("pyaes unexpectedly has an upstream wheel; re-review")
            # Official JSON metadata is authoritative for exact bytes and URL.
            assets[name] = asset
            metadata[name] = {"version": version,
                              "requires_python": meta["info"].get("requires_python"),
                              "requires_dist": meta["info"].get("requires_dist") or [],
                              "asset": asset}
        sdist = root / PYAES[1]
        acquire_verified(assets["pyaes"], sdist, MAX_SDIST)
        first, fa = build_pyaes(sdist, "build-a", root)
        second, fb = build_pyaes(sdist, "build-b", root)
        if fa != fb or first.read_bytes() != second.read_bytes():
            (args.output / "reproducibility-failure.json").write_bytes(canonical({
                "status":"NONREPRODUCIBLE", "a":fa, "b":fb,
                "builder":BUILD_TOOLS, "source_date_epoch":SOURCE_DATE_EPOCH}))
            raise RuntimeError("pyaes two clean builds differ")
        wheel_dir = args.output / "wheels"
        wheel_dir.mkdir()
        wheels = []
        for name in (*PINS.keys(), "pyaes"):
            if name == "pyaes":
                frompath, source_url, origin = first, None, "github_ci_verified_reproducible_build"
            else:
                frompath = root / assets[name]["filename"]
                acquire_verified(assets[name], frompath, MAX_WHEEL)
                source_url, origin = assets[name]["url"], "pypi_verified_wheel"
            dest = wheel_dir / frompath.name
            shutil.copyfile(frompath, dest)
            size = dest.stat().st_size
            digest = sha256(dest.read_bytes())
            if (name != "pyaes" and (digest != assets[name]["sha256"]
                                     or size != assets[name]["size"])):
                raise RuntimeError("official wheel hash/size changed")
            wheels.append({"package":name, "version":metadata[name]["version"],
                           "name":f"wheels-cp{sys.version_info.minor}/{dest.name}",
                           "filename":dest.name, "size":size, "sha256":digest,
                           "url":source_url, "origin":origin,
                           "requires_python":metadata[name]["requires_python"]})
        install = root / "isolated-target"
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-deps", "--no-index",
             "--target", str(install), *[str(p) for p in sorted(wheel_dir.iterdir())]],
            capture_output=True, text=True, timeout=45,
            env=dict(os.environ,PIP_NO_INPUT="1", PIP_DISABLE_PIP_VERSION_CHECK="1"))
        if result.returncode:
            raise RuntimeError("local --no-deps wheel install failed: " + result.stderr[-2000:])
        smoke = test_actual_api(install, args.output / "api-smoke.json")
        manifest = {
            "codec":"rkb.telethon_candidate_wheel_lock.v1",
            "status":"ci_verified_candidate_NOT_approved_Stage0",
            "python":f"CPython {sys.version_info.major}.{sys.version_info.minor}",
            "machine":"x86_64", "base_source_revision":args.base_revision,
            "source_date_epoch":int(SOURCE_DATE_EPOCH),
            "builder":{"tools": BUILD_TOOLS, "clean_directories":2,
                       "offline_build_no_index":True, "repeat_hash_and_bytes_equal":True},
            "pyaes_sdist":assets["pyaes"],
            "metadata":{n:{"requires_dist":metadata[n]["requires_dist"],
                          "requires_python":metadata[n]["requires_python"]}
                        for n in sorted(metadata)},
            "packages":wheels,
            "wheel_bytes":sum(w["size"] for w in wheels),
            "api":smoke,
            "artifact_placement":{"pyaes_permanent_url":None,
                                  "approved_for_runtime":False,
                                  "missing":"permanent content-addressed approved HTTPS wheel URL and receipt"},
            "policy":"no deployed code, no Stage0 approval, no Telegram connectivity",
        }
        (args.output / "candidate-lock.json").write_bytes(canonical(manifest))
        print(json.dumps({"status":"VERIFIED_CANDIDATE_NOT_STAGE0",
                          "python":manifest["python"], "pyaes":fa,
                          "wheels":len(wheels), "wheel_bytes":manifest["wheel_bytes"],
                          "api":"PASS","permanent_url":"MISSING"},sort_keys=True))


if __name__ == "__main__":
    main()
