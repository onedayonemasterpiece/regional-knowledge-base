"""Deploy only the Story Registry MCP release, preserving corpus and other workers.

The active SQLite remains the authority. Preflight takes a consistent SQLite
Online Backup before any schema migration or service change. A failed rollout
restores the prior systemd configuration, not an older copy of live user data.

Run as the existing owner user on DevCoveer, never as root. No LLM usage.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path

ROOT = Path("/home/dev/projects/regional-knowledge-base")
RUNTIME = Path("/home/dev/.local/share/regional-knowledge-base/releases")
STATE = Path("/home/dev/.local/state/regional-knowledge-base")
UNIT_DIR = Path("/home/dev/.config/systemd/user")
OVERRIDE = UNIT_DIR / "regional-knowledge-base.service.d/90-story-registry.conf"
OVERRIDE_ENV = STATE / "story-registry-release.env"
ENV = STATE / "service.env"
BACKUPS = STATE / "backups" / "story-registry"
SERVICE = "regional-knowledge-base.service"
BRANCH = "chatgpt/story-registry-mvp-20261009"


def cmd(*args, capture=True, timeout=90):
    p = subprocess.run([str(v) for v in args], check=True, capture_output=capture,
                       text=True, timeout=timeout)
    return p.stdout.strip() if capture else ""


def private_setting(name):
    for raw in ENV.read_text().splitlines():
        if raw.strip().startswith(name + "="):
            v = raw.split("=", 1)[1].strip()
            return v[1:-1] if len(v) > 1 and v[0] == v[-1] and v[0] in "'\"" else v
    raise RuntimeError(name + " missing from service environment")


def atomically(path, contents, mode):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".new-" + str(os.getpid()))
    with open(tmp, "x", encoding="utf8") as out:
        out.write(contents)
        out.flush()
        os.fsync(out.fileno())
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def offline_backup():
    db_path = Path(private_setting("RKB_SQLITE_CORPUS_PATH"))
    if not db_path.exists() or not db_path.is_file():
        raise RuntimeError("authoritative SQLite corpus is missing")
    size = db_path.stat().st_size
    available = shutil.disk_usage(STATE).free
    if available < size * 2 + 128 * 1024 * 1024:
        raise RuntimeError("insufficient free space for consistent SQLite snapshot")
    BACKUPS.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(BACKUPS, 0o700)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    target = BACKUPS / ("pre-story-" + stamp + "-" + str(os.getpid()) + ".sqlite3")
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=15) as src:
            with sqlite3.connect(target, timeout=15) as dest:
                src.backup(dest, pages=200)
        os.chmod(target, 0o600)
        with sqlite3.connect(f"file:{target}?mode=ro", uri=True) as restored:
            if restored.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise RuntimeError("online backup integrity_check failed")
            fk = restored.execute("PRAGMA foreign_key_check").fetchall()
            if fk:
                raise RuntimeError("online backup foreign_key_check failed")
        hasher = hashlib.sha256()
        with open(target, "rb") as copy:
            for part in iter(lambda: copy.read(1024 * 1024), b""):
                hasher.update(part)
        digest = hasher.hexdigest()
        receipt = {"db_backup": str(target), "backup_bytes": target.stat().st_size,
                   "sha256": digest, "integrity_check": "ok", "foreign_keys": "ok"}
        atomically(target.with_suffix(".json"), json.dumps(receipt, ensure_ascii=False), 0o600)
        return receipt
    except Exception:
        target.unlink(missing_ok=True)
        raise


def materialize(sha):
    if not re.fullmatch(r"[a-f0-9]{40}", sha):
        raise ValueError("exact full release SHA required")
    current = cmd("git", "-C", ROOT, "rev-parse", "HEAD")
    # Protect the user's dirty vector-work checkout, without switching it.
    if not current:
        raise RuntimeError("could not read the existing worktree")
    if subprocess.run(["git", "-C", str(ROOT), "cat-file", "-e", sha + "^{commit}"],
                      capture_output=True).returncode:
        cmd("git", "-C", ROOT, "fetch", "--no-tags", "origin",
            "refs/heads/" + BRANCH, timeout=120)
    if cmd("git", "-C", ROOT, "rev-parse", sha + "^{commit}") != sha:
        raise RuntimeError("requested immutable Git object unavailable")
    release = RUNTIME / sha
    source = release / "source"
    marker = source / ".rkb-release-sha"
    if marker.exists() and marker.read_text().strip() == sha:
        return source
    if source.exists():
        raise RuntimeError("release path exists with a different marker")
    release.mkdir(mode=0o700, parents=True, exist_ok=True)
    archive = subprocess.run(["git", "-C", str(ROOT), "archive", "--format=tar", sha],
                             capture_output=True, check=True, timeout=120).stdout
    temporary = release / ("source.tmp-" + str(os.getpid()))
    temporary.mkdir(mode=0o700)
    try:
        with tarfile.open(fileobj=io.BytesIO(archive)) as tf:
            tf.extractall(temporary, filter="data")
        atomically(temporary / ".rkb-release-sha", sha + "\n", 0o600)
        os.replace(temporary, source)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return source


def verify_imports(source):
    env = os.environ.copy()
    env.update(PYTHONPATH=str(source / "src"), RKB_DEV_NOAUTH="1")
    statement = '''
import asyncio
from types import SimpleNamespace
from regional_knowledge.server import build_server
from regional_knowledge.sqlite_corpus import SQLiteCorpus
async def main():
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as d:
        backend=SimpleNamespace(corpus=SQLiteCorpus(Path(d)/"story-smoke.sqlite3"))
        for profile,required in {
            "full":{"story_create","story_edit","story_transition","search","book_find"},
            "story_editor":{"story_create","story_transition","story_export"},
            "story_reader":{"story_search","story_get","story_history"},
            "live":{"knowledge_search"}}.items():
            server=build_server(backend=backend,profile=profile)
            found={x.name for x in await server.list_tools()}
            assert required <= found,(profile,required-found)
            if profile=="live":assert found=={"knowledge_search"}
asyncio.run(main())
'''
    subprocess.run([str(ROOT / ".venv/bin/python"), "-c", statement], cwd=str(source),
                   env=env, check=True, timeout=35, capture_output=True)


def configure(source, sha):
    # An isolated per-service override: changing the global service.env would
    # inadvertently alter BGE/graph/indexing workers after their next restart.
    override = """[Service]
WorkingDirectory={source}
EnvironmentFile=
EnvironmentFile={env}
EnvironmentFile={extra}
""".format(source=source, env=ENV, extra=OVERRIDE_ENV)
    metadata = "PYTHONPATH=\"{}\"\nRKB_RELEASE_SHA=\"{}\"\n".format(source / "src", sha)
    atomically(OVERRIDE_ENV, metadata, 0o600)
    atomically(OVERRIDE, override, 0o644)


def health():
    for url in ("http://127.0.0.1:8000/health",
                "https://knowledge.kenigevents.ru/health"):
        last = None
        for _ in range(35):
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if response.status == 200 and json.loads(response.read()).get("status") == "ok":
                        break
            except Exception as e:
                last = type(e).__name__
            time.sleep(0.5)
        else:
            raise RuntimeError("MCP health failed at " + url + "; " + str(last))


def systemctl(action, service=None):
    cmd("systemctl", "--user", action, *( [service] if service else [] ), timeout=40)


def verify_pid(source, sha):
    pid = int(cmd("systemctl", "--user", "show", SERVICE, "--value", "-p", "MainPID"))
    if pid < 1:
        raise RuntimeError("MCP process not alive")
    environment = Path("/proc") / str(pid) / "environ"
    # This verifies ONLY the non-secret release keys; never print environment.
    variables = dict(entry.split(b"=", 1) for entry in environment.read_bytes().split(b"\0")
                     if b"=" in entry)
    if variables.get(b"PYTHONPATH", b"").decode() != str(source / "src"):
        raise RuntimeError("production MCP PYTHONPATH not pointed at candidate")
    if variables.get(b"RKB_RELEASE_SHA", b"").decode() != sha:
        raise RuntimeError("production release marker missing")
    return pid


def run(sha, apply):
    os.environ["XDG_RUNTIME_DIR"] = "/run/user/" + str(os.getuid())
    os.environ["DBUS_SESSION_BUS_ADDRESS"] = "unix:path=" + os.environ["XDG_RUNTIME_DIR"] + "/bus"
    source = materialize(sha)
    verify_imports(source)
    existing_override = OVERRIDE.read_bytes() if OVERRIDE.exists() else None
    existing_env = OVERRIDE_ENV.read_bytes() if OVERRIDE_ENV.exists() else None
    if existing_override and b"story-registry" not in existing_override:
        raise RuntimeError("unexpected unrelated systemd override: refusing overwrite")
    print(json.dumps({"phase": "preflight", "candidate_sha": sha, "source": str(source),
                      "preflight": "passed", "sqlite_runtime": sqlite3.sqlite_version,
                      "will_restart_only": SERVICE}, ensure_ascii=False), flush=True)
    if not apply:
        return
    backup = offline_backup()
    print(json.dumps({"phase": "backup", **backup}, ensure_ascii=False), flush=True)
    try:
        configure(source, sha)
        systemctl("daemon-reload")
        systemctl("restart", SERVICE)
        health()
        pid = verify_pid(source, sha)
        print(json.dumps({"phase": "deployed", "sha": sha, "service": SERVICE,
                          "pid": pid, "public_health": "ok",
                          "backup_file": backup["db_backup"]}, ensure_ascii=False), flush=True)
    except BaseException:
        for path, previous, mode in ((OVERRIDE, existing_override, 0o644),
                                     (OVERRIDE_ENV, existing_env, 0o600)):
            if previous is None:
                path.unlink(missing_ok=True)
            else:
                atomically(path, previous.decode("utf8"), mode)
        try:
            systemctl("daemon-reload")
            systemctl("restart", SERVICE)
            health()
            print(json.dumps({"phase": "rolled_back_service_only", "health": "ok"}), flush=True)
        except Exception as rollback_error:
            print(json.dumps({"phase": "rollback_health_failed",
                              "error_type": type(rollback_error).__name__}), flush=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    run(args.sha, args.apply)
