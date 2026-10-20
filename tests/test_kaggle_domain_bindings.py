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
