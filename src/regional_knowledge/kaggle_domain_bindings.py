"""Explicit fresh11 bindings to frozen source/editorial/Case payload semantics.

Historical command validators and journal bytes keep their original tuples.
Only these new-command builders select the reviewed fresh11 installed tuple;
there is deliberately no API to relabel an already accepted command.
"""
from dataclasses import replace
from .kaggle_apply import Revisions,HandlerSpec,PgFunction,DependencySpec
from .recovery_journal import ExactCommand

DOMAIN_REVISIONS=Revisions('rkb-v4-sql001-011-v1','rkb-v4-source-story-cases-geo-v6','rkb-python-apply-v1')
CURRENT_ROUTES=frozenset({'source.import.v4','source.withdraw.v1','story.edit.v1','story.access.v1',
    'disagreement.edit.v1','disagreement.access.v1','disagreement.draft.v1','disagreement.publish.v1',
    'geo.manifest.install.v1'})


def _view(command,revisions):
    # An ephemeral validation view only. No serialization, storage or dispatch
    # ever sees this view; callers always retain the original exact command.
    return replace(command,schema_revision=revisions.schema,function_revision=revisions.function,
        executor_revision=revisions.executor)


def validate_current_command(command):
    if (type(command) is not ExactCommand or command.kind!='domain_command' or command.route not in CURRENT_ROUTES
            or (command.schema_revision,command.function_revision,command.executor_revision)!=DOMAIN_REVISIONS.wire()):
        raise ValueError('explicit reviewed fresh11 command binding required')
    route=command.route
    if route=='source.import.v4':
        from .source_import_v4 import validate_import_v4_payload
        if set(command.ids)!={'source_id'} or command.expected_revisions!={'absent_source':'true'}:
            raise ValueError('exact absent-source precondition required')
        return validate_import_v4_payload(command.payload,source_id=command.ids['source_id'])
    if route in ('story.edit.v1','story.access.v1'):
        from .story_editorial import EDITORIAL_REVISIONS,validate_story_edit_command,validate_story_access_command
        return (validate_story_edit_command if route=='story.edit.v1' else validate_story_access_command)(_view(command,EDITORIAL_REVISIONS))
    if route.startswith('disagreement.'):
        from .disagreement_native import CASE_REVISIONS,validate_case_command
        return validate_case_command(_view(command,CASE_REVISIONS))
    if route=='geo.manifest.install.v1':
        from .geo_manifest_commands import validate_geo_manifest_command
        return validate_geo_manifest_command(command)
    from .source_import import _uuid
    if set(command.ids)!={'source_id'} or set(command.expected_revisions)!={'eligibility_revision'} or command.payload!=b'{"withdrawn":true}':
        raise ValueError('exact withdrawal command required')
    _uuid(command.ids['source_id']);revision=command.expected_revisions['eligibility_revision']
    if (type(revision) is not str or not revision.isascii() or not revision.isdecimal()
            or str(int(revision))!=revision or not 1<=int(revision)<2**63):
        raise ValueError('canonical source eligibility revision required')
    return {'withdrawn':True}


def current_handler(route,*,check_sha256,apply_sha256):
    if route not in CURRENT_ROUTES:raise ValueError('fixed current native domain route required')
    if route=='source.import.v4':
        from .source_import_v4 import source_import_v4_handler
        original=source_import_v4_handler(check_sha256=check_sha256,apply_sha256=apply_sha256)
    elif route in ('story.edit.v1','story.access.v1'):
        from .story_editorial import editorial_handler
        original=editorial_handler(route=route,check_sha256=check_sha256,apply_sha256=apply_sha256)
    elif route.startswith('disagreement.'):
        from .disagreement_native import case_handler
        original=case_handler(route,check_sha256=check_sha256,apply_sha256=apply_sha256)
    elif route=='geo.manifest.install.v1':
        from .geo_manifest_commands import geo_manifest_handler
        original=geo_manifest_handler(revisions=DOMAIN_REVISIONS,check_sha256=check_sha256,apply_sha256=apply_sha256)
    else:
        return HandlerSpec('domain_command',route,DOMAIN_REVISIONS,PgFunction('check_source_withdraw_v1',check_sha256),
            PgFunction('apply_source_withdraw_v1',apply_sha256),frozenset({'eligibility_revision'}),
            dependency=DependencySpec(route,'public_source',frozenset({'source_id'}),'shared',True))
    return replace(original,revisions=DOMAIN_REVISIONS)


def _new_at_current(command):
    command=_view(command,DOMAIN_REVISIONS)
    validate_current_command(command)
    return command


def resolved_current_import_v4_command(*,revisions=DOMAIN_REVISIONS,**arguments):
    if revisions!=DOMAIN_REVISIONS:raise ValueError('exact installed domain revisions required')
    from .source_import_v4 import resolved_import_v4_command
    return _new_at_current(resolved_import_v4_command(**arguments))


def resolved_current_story_edit_command(**arguments):
    from .story_editorial import resolved_story_edit_command
    return _new_at_current(resolved_story_edit_command(**arguments))


def resolved_current_story_access_command(**arguments):
    from .story_editorial import resolved_story_access_command
    return _new_at_current(resolved_story_access_command(**arguments))


def resolved_current_case_command(**arguments):
    from .disagreement_native import resolved_case_command
    return _new_at_current(resolved_case_command(**arguments))
