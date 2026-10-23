"""The schema simplification plan explicitly requires a source grep guard."""
import ast
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'src/regional_knowledge'


def test_schema_changes_have_no_journal_transition_or_restore_pair():
    for path in SOURCE.glob('*.py'):
        assert 'schema_transition' not in path.read_text(),path.name
    for name in ('kaggle_restore_startup.py','pg_snapshot_recovery.py','kaggle_pg_snapshot_native.py'):
        text=(SOURCE/name).read_text()
        for retired in ('ordered_upgrade','GEO_V2_REVISIONS','ACTIVATION_REVISIONS'):
            assert retired not in text,(name,retired)
    assert not list((ROOT/'scripts/production/manifests').glob('rkb-migration-pins-*.json'))
    assert not (SOURCE/'kaggle_geo_upgrade.py').exists()
    assert not (SOURCE/'kaggle_native_source_upgrade.py').exists()


def test_readiness_and_restore_do_not_gate_on_catalog_or_acl_hashes():
    for name in ('kaggle_readiness.py','kaggle_restore_startup.py','kaggle_snapshot_startup.py',
                 'pg_snapshot_recovery.py','kaggle_pg_snapshot_native.py',
                 'source_history_verifier.py','kaggle_source_history_native.py','source_history_wss.py'):
        tree=ast.parse((SOURCE/name).read_text())
        for node in ast.walk(tree):
            if isinstance(node,(ast.If,ast.IfExp,ast.While,ast.Assert)):
                condition=ast.unparse(node.test)
                assert 'catalog_sha256' not in condition,(name,node.lineno)
                assert 'acl_sha256' not in condition,(name,node.lineno)
