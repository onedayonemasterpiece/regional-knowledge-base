"""Actual tiny-file inventory boundaries; no installs, models or PG execution."""
import hashlib

import pytest

from regional_knowledge import kaggle_bootstrap as boot
from regional_knowledge import kaggle_stage0 as stage0
from regional_knowledge.kaggle_readiness import ArtifactRequirement
from test_kaggle_bootstrap import artifacts


def complete(artifacts, count):
    entries = list(artifacts.entries)
    folder = artifacts.root / 'additional-code'
    folder.mkdir()
    while len(entries) < count:
        name = f'additional-code/leaf-{len(entries)}.py'
        (artifacts.root / name).write_bytes(b'x')
        entries.append(boot.PreparedArtifact(
            ArtifactRequirement(name, 1, hashlib.sha256(b'x').hexdigest()), 'code'))
    return tuple(entries)


def prepared(artifacts, entries):
    return dict(root=str(artifacts.root), entries=[dict(name=e.requirement.name,
        size=e.requirement.size, sha256=e.requirement.sha256, role=e.role) for e in entries],
        max_total_bytes=sum(e.requirement.size for e in entries))


def test_196_actual_files_pass_both_inventory_and_hash_verification(artifacts, monkeypatch):
    entries = complete(artifacts, 196)
    monkeypatch.setattr(stage0, 'TRUSTED_ROOT', artifacts.root)
    inventory = prepared(artifacts, entries)
    assert stage0.MAX_FILES == 128
    assert len(stage0._inventory(inventory)) == 196
    verified = boot.verify_artifacts(artifacts.root, entries,
                                     max_total_bytes=inventory['max_total_bytes'])
    assert len(verified.observed) == 196


@pytest.mark.parametrize('failure', ['hash', 'bytes', 'missing', 'required_role'])
def test_195_file_inventory_keeps_integrity_byte_and_required_file_guards(artifacts, monkeypatch, failure):
    entries = complete(artifacts, 195)
    monkeypatch.setattr(stage0, 'TRUSTED_ROOT', artifacts.root)
    inventory = prepared(artifacts, entries)
    budget = inventory['max_total_bytes']
    if failure == 'hash':
        (artifacts.root / entries[-1].requirement.name).write_bytes(b'y')
    elif failure == 'bytes':
        budget -= 1
        inventory['max_total_bytes'] = budget
        with pytest.raises(ValueError, match='stage0_prepared_budget_exceeded'):
            stage0._inventory(inventory)
    elif failure == 'missing':
        (artifacts.root / entries[-1].requirement.name).unlink()
    else:
        entries = tuple(boot.PreparedArtifact(e.requirement, 'code') if e.role == 'tokenizer' else e
                        for e in entries)
    with pytest.raises(boot.BootstrapUnavailable):
        boot.verify_artifacts(artifacts.root, entries, max_total_bytes=budget)
