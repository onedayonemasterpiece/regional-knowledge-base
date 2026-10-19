"""Local owner administration; raw login codes go only to protected files."""
import argparse
import json
import os
from pathlib import Path
import stat
import time
from uuid import UUID

from .personal_auth import PersonalPolicy, new_login_code
from .participants import ParticipantError, Participants
from .sqlite_corpus import SQLiteCorpus


def _private_read(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as stream:
        mode = os.fstat(stream.fileno()).st_mode
        if not stat.S_ISREG(mode) or mode & 0o077:
            raise ValueError("credential file must be a private regular file")
        return json.load(stream)


def issue_to_file(provider, owner, person, path, *, days=30, reason, resume=False):
    person = str(UUID(str(person)))
    if not 1 <= days <= 90:
        raise ValueError("credential lifetime must be 1..90 days")
    # Validate current owner and target before creating a file. No missing user,
    # profile, external binding or capability is automatically provisioned.
    provider.personal_policy.require_admin(owner)
    provider._personal_person(person)
    path = Path(path)
    if not path.is_absolute() or not path.parent.is_dir() or path.parent.stat().st_mode & 0o077:
        raise ValueError("use an existing private absolute credential directory")
    if resume:
        stored = _private_read(path)
        if stored["subject"] != person or stored["reason"] != reason:
            raise ValueError("credential file identity conflict")
    else:
        ident, code = new_login_code()
        stored = {"credential_id": ident, "subject": person, "login_code": code,
            "expires_at": time.time() + days * 86400, "reason": reason}
        # Persist the original before changing OAuth state. --resume reuses
        # exactly these bytes/identity after a lost process response.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(stored, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
    if provider._personal_code_id(stored["login_code"]) != stored["credential_id"]:
        raise ValueError("credential file identity conflict")
    return provider.issue_personal_credential(owner, person, login_code=stored["login_code"],
        expires_at=stored["expires_at"], reason=stored["reason"])


def _runtime():
    from .oauth_provider import oauth_provider_from_env
    corpus_path = Path(os.environ["RKB_SQLITE_CORPUS_PATH"])
    if not corpus_path.is_file():
        raise ValueError("existing corpus required")
    # Administration must use the configured, existing authority/key pair.
    # It cannot silently create a new OAuth server beside the running one.
    for name in ("RKB_OAUTH_STATE_FILE", "RKB_OAUTH_KEY_FILE"):
        if not Path(os.environ[name]).is_file():
            raise ValueError("existing OAuth state/key required")
    provider = oauth_provider_from_env(issuer=os.environ["RKB_AUTH_ISSUER"], resource=os.environ["RKB_RESOURCE_URL"])
    people = Participants(SQLiteCorpus(corpus_path), os.environ["RKB_OWNER_SUBJECT"])
    provider.personal_policy = PersonalPolicy(people)
    return provider, provider.personal_policy._context(people.owner, owner=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Administer a real participant's RKB login; never print login codes.")
    sub = parser.add_subparsers(dest="action", required=True)
    issue = sub.add_parser("issue")
    issue.add_argument("--person", type=UUID, required=True)
    issue.add_argument("--output", type=Path, required=True)
    issue.add_argument("--days", type=int, default=30)
    issue.add_argument("--reason", required=True)
    issue.add_argument("--resume", action="store_true")
    listing = sub.add_parser("list")
    listing.add_argument("--person", type=UUID, required=True)
    revoke = sub.add_parser("revoke")
    revoke.add_argument("--credential", type=UUID, required=True)
    revoke.add_argument("--expected-epoch", type=int, required=True)
    revoke.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    try:
        provider, owner = _runtime()
        if args.action == "issue":
            result = issue_to_file(provider, owner, args.person, args.output, days=args.days, reason=args.reason, resume=args.resume)
        elif args.action == "list":
            result = provider.personal_credentials(owner, str(args.person))
        else:
            result = provider.revoke_personal_credential(owner, str(args.credential), expected_epoch=args.expected_epoch, reason=args.reason)
    except (ValueError, KeyError, OSError, ParticipantError) as error:
        # Details may contain native inputs, paths or credentials. Keep the
        # public operator error bounded; original private file stays available.
        raise SystemExit("Personal authentication administration failed (" + type(error).__name__ + "). No login code printed.") from None
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
