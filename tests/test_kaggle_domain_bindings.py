from dataclasses import replace
import pytest
from regional_knowledge.kaggle_domain_bindings import *
from regional_knowledge.source_import_v4 import validate_import_v4_command
from regional_knowledge.story_editorial import validate_story_edit_command
from regional_knowledge.disagreement_native import validate_case_command
from test_source_manifest import command as imported,manifest_payload
from test_story_editorial import command as edited
from test_disagreement_native import command as case_command


@pytest.mark.parametrize('factory,original_validator',[(lambda:imported(manifest_payload()),validate_import_v4_command),
    (edited,validate_story_edit_command),(case_command,validate_case_command)])
def test_current_validation_preserves_exact_original_semantics_and_never_relabels_history(factory,original_validator):
    old=factory();old_bytes=old.encode();old_value=original_validator(old)
    current=replace(old,schema_revision=DOMAIN_REVISIONS.schema,function_revision=DOMAIN_REVISIONS.function,
        executor_revision=DOMAIN_REVISIONS.executor)
    assert validate_current_command(current)==old_value
    assert old.encode()==old_bytes and current.payload==old.payload
    with pytest.raises(ValueError):original_validator(current)
    with pytest.raises(ValueError):validate_current_command(old)
    with pytest.raises(ValueError):validate_current_command(replace(current,function_revision='unreviewed-latest'))
    old_handler=current_handler(current.route,check_sha256='a'*64,apply_sha256='b'*64)
    assert old_handler.revisions==DOMAIN_REVISIONS and old_handler.route==old.route


def test_fixed_current_registry_and_wire_keep_full_current_tuple_and_payload():
    from types import SimpleNamespace
    from regional_knowledge.kaggle_source_commands import fixed_registry,decode_command
    c=resolved_current_import_v4_command(sequence=1,operation_key='current-source-1',request_id='current-request',
        person_id='60000000-0000-0000-0000-000000000002',logical_time='2026-10-20T18:00:00Z',
        source_id=manifest_payload()['source_manifest']['source_id'],payload=manifest_payload())
    db=SimpleNamespace(schema_revision=DOMAIN_REVISIONS.schema,function_revision=DOMAIN_REVISIONS.function,
        executor_revision=DOMAIN_REVISIONS.executor)
    registry=fixed_registry(db,[dict(route=c.route,check_sha256='a'*64,apply_sha256='b'*64)])
    assert registry.resolve(c).revisions==DOMAIN_REVISIONS
    assert decode_command(c.encode()).encode()==c.encode()


def test_current_graph_handler_preserves_frozen_nine_function_binding():
    from regional_knowledge.source_import_v4 import source_import_v4_handler
    current=current_handler('source.import.v4',check_sha256='a'*64,apply_sha256='b'*64)
    frozen=source_import_v4_handler(check_sha256='a'*64,apply_sha256='b'*64)
    assert current.check.name=='check_source_import_graph_v1'
    assert current.apply.name=='apply_source_import_graph_v1'
    assert frozen.check.name=='check_source_import_v4'
    assert frozen.apply.name=='apply_source_import_v4'


def test_full_ordered_closure_registry_fits_thirty_three_retained_handlers():
    """Historic schema11 + schema12 handlers, both migrations and schema14 routes (33)."""
    import pytest
    from types import SimpleNamespace
    from regional_knowledge.kaggle_source_commands import fixed_registry
    from regional_knowledge.kaggle_domain_bindings import CURRENT_ROUTES
    from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES
    from regional_knowledge.geo_manifest_commands import GEO_REVISIONS, GEO_V2_REVISIONS
    from regional_knowledge.kaggle_native_source_upgrade import NativeSourceUpgradePins, native_source_upgrade_handler
    pins = NativeSourceUpgradePins('1'*64, '2'*64, '3'*64, '4'*64)
    native = native_source_upgrade_handler(pins)
    db = SimpleNamespace(schema_revision='rkb-v4-sql001-014-v1', function_revision='f14', executor_revision='e14')
    pin = dict(check_sha256='a'*64, apply_sha256='b'*64)
    entries = ([dict(route=r, revisions=list(GEO_REVISIONS.wire()), **pin) for r in CURRENT_ROUTES]
        + [dict(route=r, revisions=list(GEO_V2_REVISIONS.wire()), **pin) for r in CURRENT_ROUTES]
        + [dict(route='geo.dependencies.upgrade.v2', revisions=list(GEO_REVISIONS.wire()), artifact_sha256='c'*64, **pin)]
        + [dict(route='source.native.upgrade.v1', revisions=list(native.revisions.wire()), artifact_sha256=pins.artifact_sha256,
                check_sha256=pins.transition_check_sha256, apply_sha256=pins.transition_apply_sha256,
                native_source_upgrade=pins.wire())]
        + [dict(route=r, revisions=['rkb-v4-sql001-014-v1', 'f14', 'e14'], **pin) for r in NATIVE_ROUTES])
    assert len(entries) == 33
    assert len(fixed_registry(db, entries).handlers) == 33
    with pytest.raises(ValueError, match='fixed source registry required'):
        fixed_registry(db, entries * 2)
