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
