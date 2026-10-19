"""Local audited owner bootstrap for narrow expert runtime service grants."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets

from .contracts import Principal
from .expert_requests import ExpertRequestError, ExpertRequests
from .participants import ParticipantError, Participants, RequestAuthority
from .slack_runtime import private_text
from .sqlite_corpus import SQLiteCorpus


def service_to_file(requests, owner, service_id, kind, path, *, expected_epoch, enabled=True, generate=False):
    if kind not in {"executor", "delivery"} or not re.fullmatch(r"[a-z][a-z0-9_-]{4,79}", service_id):
        raise ValueError("invalid scoped service")
    with requests.corpus.connect() as db:
        requests.people._authority(db, owner, admin=True)
    path = Path(path)
    if not path.is_absolute() or not path.parent.is_dir() or path.parent.stat().st_mode & 0o077:
        raise ValueError("existing private credential directory required")
    if path.exists():
        token = private_text(path).strip()
    else:
        if not generate:
            raise ValueError("existing credential required")
        token = secrets.token_urlsafe(32)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(token + "\n"); stream.flush(); os.fsync(stream.fileno())
    if not 32 <= len(token) <= 256 or any(ord(c) < 33 or ord(c) > 126 for c in token):
        raise ValueError("invalid service credential")
    # File precedes the audited DB effect. Repeating the exact key/file/epoch
    # reconciles provision_service's immutable outcome instead of duplicating it.
    return requests.provision_service(owner, service_id, kind, token, expected_epoch=expected_epoch, enabled=enabled)


def runtime():
    corpus_path = Path(os.environ["RKB_SQLITE_CORPUS_PATH"])
    if not corpus_path.is_file():
        raise ValueError("existing corpus required")
    config = json.loads(private_text(os.environ["RKB_SLACK_CONFIG_PATH"]))
    people = Participants(SQLiteCorpus(corpus_path), os.environ["RKB_OWNER_SUBJECT"])
    owner = RequestAuthority(Principal(subject=people.owner, client_id="rkb-local-admin", issuer="rkb-local", access_token=""), "owner")
    requests = ExpertRequests(people, app_id=config["app_id"], team_id=config["team_id"], bot_user_id=config["bot_user_id"],
        allowed_channels=config.get("allowed_channels") or [config["authorized_test_channel_id"]])
    return requests, owner


def main(argv=None):
    parser = argparse.ArgumentParser(description="Provision/revoke scoped RKB expert services; never print tokens.")
    parser.add_argument("--service-id", required=True)
    parser.add_argument("--kind", choices=("executor", "delivery"), required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--expected-epoch", type=int, required=True)
    parser.add_argument("--generate", action="store_true", help="Create a missing private file; never replace an existing token.")
    parser.add_argument("--disable", action="store_true")
    args = parser.parse_args(argv)
    try:
        requests, owner = runtime()
        result = service_to_file(requests, owner, args.service_id, args.kind, args.token_file,
            expected_epoch=args.expected_epoch, enabled=not args.disable, generate=args.generate)
    except (ValueError, KeyError, OSError, ParticipantError, ExpertRequestError) as error:
        raise SystemExit("Expert service administration failed (" + type(error).__name__ + "). No token printed.") from None
    print(json.dumps(result))


if __name__ == "__main__":
    main()
