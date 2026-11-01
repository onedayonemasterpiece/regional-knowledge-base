"""Deploy the Story MCP and optionally the indexing activation worker.

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
import logging
import os
import re
import shutil
import shlex
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
INDEXER_SERVICE = "regional-knowledge-indexing.service"
GRAPH_SERVICE = "regional-knowledge-graph-discovery.service"
GRAPH_OVERRIDE = UNIT_DIR / "regional-knowledge-graph-discovery.service.d/90-model-led-locations.conf"
INDEXER_OVERRIDE = UNIT_DIR / "regional-knowledge-indexing.service.d/90-story-ingestion.conf"
BRANCH = "chatgpt/auto-story-extract-on-book-ingest-20261009"
ARCHIVE_ENV = STATE / "archive-s2s-pythonpath.env"
LATE_PIN_NAME = "zzzzzzzzzzzzzz-local-archive-20261018.conf"
_UNSET = object()


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


def atomically(path, contents, mode, *, expected=_UNSET):
    if expected is not _UNSET and snapshot(path) != expected:
        raise RuntimeError("configuration changed before write: " + path.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".new-" + str(os.getpid()))
    with open(tmp, "x", encoding="utf8") as out:
        out.write(contents)
        out.flush()
        os.fsync(out.fileno())
    os.chmod(tmp, mode)
    if expected is not _UNSET and snapshot(path) != expected:
        tmp.unlink()
        raise RuntimeError("configuration changed during write: " + path.name)
    os.replace(tmp, path)


def offline_backup():
    db_path = Path(private_setting("RKB_SQLITE_CORPUS_PATH"))
    if not db_path.exists() or not db_path.is_file():
        raise RuntimeError("authoritative SQLite corpus is missing")
    size = db_path.stat().st_size
    available = shutil.disk_usage(STATE).free
    # Online Backup writes one new snapshot of approximately the live .db size.
    # Leave at least 128 MiB additional headroom for WAL and normal writes;
    # do not invent a second full snapshot allocation.
    if available < size + 128 * 1024 * 1024:
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
            if profile=="full":
                declared={tool.name:tool for tool in await server.list_tools()}
                arg_schema=declared["book_ingest"].input_schema
                assert "story_candidates" in arg_schema["properties"]
                assert "story_candidates_reviewed" in str(arg_schema)
                assert "story_extraction" in declared["book_ingest"].output_schema["properties"]
            if profile=="live":assert found=={"knowledge_search"}
asyncio.run(main())
'''
    subprocess.run([str(ROOT / ".venv/bin/python"), "-c", statement], cwd=str(source),
                   env=env, check=True, timeout=35, capture_output=True)


def snapshot(path):
    if path.is_symlink():
        raise RuntimeError("refusing symlink configuration: " + path.name)
    return path.read_bytes() if path.exists() else None


def release_environment(previous, source, sha):
    """Change only release keys; keep credentials and unrelated settings intact."""
    values = {"PYTHONPATH": str(source / "src"), "RKB_RELEASE_SHA": sha}
    lines = (previous or b"").decode("utf8").splitlines(keepends=True)
    seen = set()
    for index, line in enumerate(lines):
        match = re.match(r"^\s*(PYTHONPATH|RKB_RELEASE_SHA)\s*=", line)
        if match:
            key = match.group(1)
            if key in seen or line.rstrip("\r\n").endswith("\\"):
                raise RuntimeError("ambiguous release environment key: " + key)
            seen.add(key)
            lines[index] = key + '="' + values[key] + '"\n'
    if lines and not lines[-1].endswith(("\n", "\r")):
        lines[-1] += "\n"
    lines.extend(key + '="' + value + '"\n' for key, value in values.items() if key not in seen)
    return "".join(lines)


def retarget_override(previous, source, *, late=False):
    """Preserve every unrelated directive, especially M2M EnvironmentFile links."""
    lines = previous.decode("utf8").splitlines(keepends=True)
    service = False
    working = pythonpath = 0
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("["):
            service = stripped == "[Service]"
        if not service:
            continue
        if re.match(r"^\s*WorkingDirectory\s*=", line):
            working += 1
            lines[index] = "WorkingDirectory=" + str(source) + "\n"
        elif late and re.match(r"^\s*Environment\s*=", line):
            value = line.split("=", 1)[1].strip()
            tokens = shlex.split(value)
            if any(token.startswith("PYTHONPATH=") for token in tokens):
                if len(tokens) != 1:
                    raise RuntimeError("mixed late PYTHONPATH assignment: refusing rewrite")
                pythonpath += 1
                lines[index] = "Environment=PYTHONPATH=" + str(source / "src") + "\n"
    if working != 1 or (late and pythonpath != 1):
        raise RuntimeError("unexpected release pin configuration")
    return "".join(lines)


def configuration_plan(source, sha, *, with_indexer=False, with_graph_worker=False):
    """One candidate, all existing pins, exact snapshots before any service change.

    EnvironmentFile overrides Environment regardless of drop-in order. The
    existing late archive envfile and WorkingDirectory pins must follow the
    canonical release too; adding another zzz drop-in would only hide the drift.
    """
    override = ("[Service]\nWorkingDirectory={source}\nEnvironmentFile=\n"
                "EnvironmentFile={env}\nEnvironmentFile={extra}\n").format(
                    source=source, env=ENV, extra=OVERRIDE_ENV)
    plan = {}

    def add(path, rendered, default_mode):
        previous = snapshot(path)
        mode = path.stat().st_mode & 0o777 if previous is not None else default_mode
        plan[path] = (previous, rendered(previous).encode("utf8"), mode)

    add(OVERRIDE_ENV, lambda old: release_environment(old, source, sha), 0o600)
    selected = [(SERVICE, OVERRIDE)]
    if with_indexer:
        selected.append((INDEXER_SERVICE, INDEXER_OVERRIDE))
    if with_graph_worker:
        selected.append((GRAPH_SERVICE, GRAPH_OVERRIDE))
    for unit, path in selected:
        previous = snapshot(path)
        if previous is not None and b"story-registry" not in previous:
            raise RuntimeError("unexpected unrelated systemd override: refusing overwrite")
        add(path, lambda old: retarget_override(old, source) if old is not None else override, 0o644)
        late = UNIT_DIR / (unit + ".d") / LATE_PIN_NAME
        if late.exists():
            add(late, lambda old: retarget_override(old, source, late=True), 0o644)
    if with_indexer and ARCHIVE_ENV.exists():
        add(ARCHIVE_ENV, lambda old: release_environment(old, source, sha), 0o600)
    return plan


def configure(plan, changed):
    # Validate the whole snapshot first, then CAS every individual replacement.
    for path, (previous, _new, _mode) in plan.items():
        if snapshot(path) != previous:
            raise RuntimeError("configuration changed after preflight: " + path.name)
    for path, (previous, new, mode) in plan.items():
        if new != previous:
            atomically(path, new.decode("utf8"), mode, expected=previous)
            changed.append(path)


def restore_configuration(plan, changed):
    # Never overwrite another operator's subsequent change during rollback.
    conflicts = []
    for path in reversed(changed):
        previous, new, mode = plan[path]
        if snapshot(path) != new:
            conflicts.append(path.name)
            continue
        if previous is None:
            path.unlink()
        else:
            atomically(path, previous.decode("utf8"), mode, expected=new)
    if conflicts:
        raise RuntimeError("concurrent configuration change prevents rollback: " + ", ".join(conflicts))


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
    cmd("systemctl", "--user", action, *( [service] if service else [] ), timeout=120)


def verify_pid(source, sha, unit=SERVICE):
    pid = int(cmd("systemctl", "--user", "show", unit, "--value", "-p", "MainPID"))
    if pid < 1:
        raise RuntimeError("MCP process not alive")
    process = Path("/proc") / str(pid)
    configured = cmd("systemctl", "--user", "show", unit, "--value", "-p", "WorkingDirectory")
    if process.joinpath("cwd").resolve() != source.resolve() or configured != str(source):
        raise RuntimeError(unit + " working directory not pointed at candidate")
    environment = process / "environ"
    # This verifies ONLY the non-secret release keys; never print environment.
    variables = dict(entry.split(b"=", 1) for entry in environment.read_bytes().split(b"\0")
                     if b"=" in entry)
    if variables.get(b"PYTHONPATH", b"").decode() != str(source / "src"):
        raise RuntimeError("production MCP PYTHONPATH not pointed at candidate")
    if variables.get(b"RKB_RELEASE_SHA", b"").decode() != sha:
        raise RuntimeError("production release marker missing")
    return pid


def running_source(unit):
    pid = cmd("systemctl", "--user", "show", unit, "--value", "-p", "MainPID")
    return (Path("/proc") / pid / "cwd").resolve(strict=True)


def compiler_checkpoint_preflight(source):
    """A changed compiler needs the accepted prefix saved before rollout."""
    current = running_source(SERVICE)
    statement = "from regional_knowledge.kaggle_source_materializer import materializer_fingerprint; print(materializer_fingerprint())"
    pins = [cmd("env", "PYTHONPATH=" + str(path / "src"), sys.executable, "-c", statement) for path in (current, source)]
    if pins[0] == pins[1]: return
    with sqlite3.connect(f"file:{STATE / 'bge/queue.sqlite'}?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        accepted = db.execute("SELECT accepted_sequence FROM rkb_control_spool_meta WHERE id=1").fetchone()[0]
        (base, latest), = db.execute("SELECT checkpoint_json,latest_json FROM rkb_dataset_recovery_store").fetchall()
        saved = json.loads(latest or base)["base_sequence"]
    if saved < accepted:
        raise RuntimeError("compiler changed: wait for the accepted prefix checkpoint before rollout")


def refresh_main_for_rotation():
    """Advance the installed services at the existing pre-launch boundary."""
    current = Path(__file__).resolve().parents[2]
    marker = current / ".rkb-release-sha"
    if not marker.is_file(): return  # Development checkouts are not installations.
    controller = "regional-knowledge-bge-controller.service"
    api_drop = UNIT_DIR / (SERVICE + ".d/zzzzzzzzzzzzzzz-kaggle-source-acceptance.conf")
    controller_drop = UNIT_DIR / (controller + ".d/90-kaggle-bootstrap.conf")
    try:
        cmd("git", "-C", ROOT, "fetch", "--no-tags", "origin", "main", timeout=120)
        sha = cmd("git", "-C", ROOT, "rev-parse", "origin/main")
        current_sha = marker.read_text().strip()
        if sha == current_sha: return
        running = {}
        for unit in (SERVICE, controller):
            path = running_source(unit)
            previous = (path / ".rkb-release-sha").read_text().strip()
            cmd("git", "-C", ROOT, "merge-base", "--is-ancestor", previous, sha)
            running[unit] = path
        source = materialize(sha)
        compiler_checkpoint_preflight(source)
        api_raw, controller_raw = snapshot(api_drop), snapshot(controller_drop)
        old_env = Path([line.split("=", 1)[1] for line in api_raw.decode().splitlines()
                        if line.startswith("EnvironmentFile=")][-1])
        env_raw = snapshot(old_env)
        environment = release_environment(env_raw, source, sha)
        api_text = api_raw.decode().replace(str(running[SERVICE]), str(source))
        controller_text = controller_raw.decode().replace(str(current), str(source)).replace(
            "Environment=RKB_RELEASE_SHA=" + current_sha, "Environment=RKB_RELEASE_SHA=" + sha)
        if str(source) not in api_text or str(source) not in controller_text:
            raise RuntimeError("installed release pins do not match running services")
    except Exception as error:
        logging.warning("main_rotation state=deferred error_type=%s", type(error).__name__)
        return
    plan = {old_env: (env_raw, environment.encode(), 0o600),
            api_drop: (api_raw, api_text.encode(), api_drop.stat().st_mode & 0o777),
            controller_drop: (controller_raw, controller_text.encode(), controller_drop.stat().st_mode & 0o777)}
    changed = []
    try:
        configure(plan, changed)
        cmd("systemctl", "--user", "daemon-reload")
        logging.info("main_rotation state=controller_exec release=%s", sha)
        env = dict(os.environ, PYTHONPATH=str(source / "src"), RKB_RELEASE_SHA=sha)
        os.chdir(source)
        os.execve(sys.executable, [sys.executable,
                  str(source / "scripts/production/bge_kaggle_controller.py")], env)
    except Exception:
        # Reuse deployment rollback; a failed update must not leave mixed pins.
        restore_configuration(plan, changed)
        cmd("systemctl", "--user", "daemon-reload")
        os.chdir(current)
        raise


def run(sha, apply, *, with_indexer=False, with_graph_worker=False):
    os.environ["XDG_RUNTIME_DIR"] = "/run/user/" + str(os.getuid())
    os.environ["DBUS_SESSION_BUS_ADDRESS"] = "unix:path=" + os.environ["XDG_RUNTIME_DIR"] + "/bus"
    source = materialize(sha)
    verify_imports(source)
    compiler_checkpoint_preflight(source)
    units = [SERVICE] + ([INDEXER_SERVICE] if with_indexer else []) + ([GRAPH_SERVICE] if with_graph_worker else [])
    if with_indexer and not (source / "src/regional_knowledge/story_ingestion.py").is_file():
        raise RuntimeError("Candidate release has no story ingestion hook")
    plan = configuration_plan(source, sha, with_indexer=with_indexer,
                              with_graph_worker=with_graph_worker)
    print(json.dumps({"phase": "preflight", "candidate_sha": sha, "source": str(source),
                      "preflight": "passed", "sqlite_runtime": sqlite3.sqlite_version,
                      "will_restart": units},
                     ensure_ascii=False), flush=True)
    if not apply:
        return
    backup = offline_backup()
    print(json.dumps({"phase": "backup", **backup}, ensure_ascii=False), flush=True)
    changed = []
    restart_started = False
    try:
        configure(plan, changed)
        systemctl("daemon-reload")
        for unit in units:
            configured = cmd("systemctl", "--user", "show", unit, "--value", "-p", "WorkingDirectory")
            if configured != str(source):
                raise RuntimeError(unit + " effective working directory still pinned elsewhere")
        for unit in units:
            restart_started = True
            systemctl("restart", unit)
        health()
        pid = verify_pid(source, sha)
        indexer_pid = verify_pid(source, sha, INDEXER_SERVICE) if with_indexer else None
        graph_pid = verify_pid(source, sha, GRAPH_SERVICE) if with_graph_worker else None
        print(json.dumps({"phase": "deployed", "sha": sha, "service": SERVICE,
                          "pid": pid, "indexer_pid": indexer_pid, "graph_pid": graph_pid,
                          "public_health": "ok",
                          "backup_file": backup["db_backup"]}, ensure_ascii=False), flush=True)
    except BaseException:
        try:
            restore_configuration(plan, changed)
            systemctl("daemon-reload")
            if restart_started:
                for unit in units:
                    systemctl("restart", unit)
            health()
            print(json.dumps({"phase": "rolled_back_previous_services", "health": "ok"}), flush=True)
        except Exception as rollback_error:
            print(json.dumps({"phase": "rollback_health_failed",
                              "error_type": type(rollback_error).__name__}), flush=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--with-indexer", action="store_true",
                        help="Update both MCP and the accepted-revision activation worker")
    parser.add_argument("--with-graph-worker", action="store_true",
                        help="Update candidate-discovery policy in the existing graph worker")
    args = parser.parse_args()
    run(args.sha, args.apply, with_indexer=args.with_indexer, with_graph_worker=args.with_graph_worker)
