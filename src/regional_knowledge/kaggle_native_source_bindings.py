"""Explicit schema14 bindings; historical accepted bytes are never relabelled."""
from dataclasses import replace
from .kaggle_apply import Revisions,PgFunction
from .recovery_journal import ExactCommand
from .kaggle_domain_bindings import DOMAIN_REVISIONS, CURRENT_ROUTES, current_handler, validate_current_command
from .source_stage import STAGE_REVISIONS, STAGE_ROUTE, SEAL_ROUTE, stage_handler, validate_stage_command, resolved_stage_command
from .source_activation import (ACTIVATION_REVISIONS, ACTIVATE_ROUTE, MATERIALIZE_ROUTE,
    activation_handler, materialize_handler, validate_activation_command, validate_materialize_command)

NATIVE_ROUTES=CURRENT_ROUTES|frozenset({STAGE_ROUTE,SEAL_ROUTE,ACTIVATE_ROUTE,MATERIALIZE_ROUTE})


def _view(command,revisions):
    return replace(command,schema_revision=revisions.schema,function_revision=revisions.function,
        executor_revision=revisions.executor)


def validate_native_source_command(command):
    if (type(command) is not ExactCommand or command.kind!='domain_command'
            or command.route not in NATIVE_ROUTES
            or (command.schema_revision,command.function_revision,command.executor_revision)!=ACTIVATION_REVISIONS.wire()):
        raise ValueError('exact installed native source revision required')
    if command.route in (STAGE_ROUTE,SEAL_ROUTE):
        return validate_stage_command(_view(command,Revisions(*STAGE_REVISIONS)))
    if command.route==ACTIVATE_ROUTE:return validate_activation_command(command)
    if command.route==MATERIALIZE_ROUTE:return validate_materialize_command(command)
    if command.route=='geo.manifest.install.v1':
        from .geo_manifest_commands import GEO_V2_REVISIONS,validate_geo_manifest_command
        return validate_geo_manifest_command(_view(command,GEO_V2_REVISIONS))
    return validate_current_command(_view(command,DOMAIN_REVISIONS))


def native_source_handler(route,*,check_sha256,apply_sha256):
    if route in (STAGE_ROUTE,SEAL_ROUTE):
        result=stage_handler(route=route,revisions=Revisions(*STAGE_REVISIONS),
            check_sha256=check_sha256,apply_sha256=apply_sha256)
        suffix='source_stage_v2' if route==STAGE_ROUTE else 'source_stage_seal_v2'
        result=replace(result,check=PgFunction('check_'+suffix,check_sha256),
            apply=PgFunction('apply_'+suffix,apply_sha256))
    elif route==ACTIVATE_ROUTE:
        return activation_handler(check_sha256=check_sha256,apply_sha256=apply_sha256)
    elif route==MATERIALIZE_ROUTE:
        return materialize_handler(check_sha256=check_sha256,apply_sha256=apply_sha256)
    elif route=='geo.manifest.install.v1':
        from .geo_manifest_commands import GEO_V2_REVISIONS,geo_manifest_handler
        result=geo_manifest_handler(revisions=GEO_V2_REVISIONS,
            check_sha256=check_sha256,apply_sha256=apply_sha256)
    else:result=current_handler(route,check_sha256=check_sha256,apply_sha256=apply_sha256)
    return replace(result,revisions=ACTIVATION_REVISIONS)


def resolved_native_stage_command(*,route,revisions=ACTIVATION_REVISIONS,**arguments):
    if revisions!=ACTIVATION_REVISIONS:raise ValueError('explicit native source revision required')
    original=resolved_stage_command(route=route,revisions=Revisions(*STAGE_REVISIONS),**arguments)
    command=_view(original,ACTIVATION_REVISIONS)
    validate_native_source_command(command)
    return command


def new_native_command(command):
    """Trusted new-intent builder only; never use on a stored/accepted command."""
    validate_current_command(command)
    value=_view(command,ACTIVATION_REVISIONS)
    validate_native_source_command(value)
    return value


def required_native_source_capabilities(command):
    validate_native_source_command(command)
    if command.route.startswith('disagreement.'):
        from .disagreement_native import CASE_REVISIONS,required_capabilities
        return required_capabilities(_view(command,CASE_REVISIONS))
    if command.route in ('story.edit.v1','story.access.v1'):return frozenset({'public_search'})
    return frozenset({'import'})
