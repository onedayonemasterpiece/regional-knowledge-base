"""Complete retained first-book registry from public measured function pins.

Origin catalog/ACL are synthetic. No live configuration, credentials, database
identities or corpus are inputs to this metadata-only fixture.
"""
import json

from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_domain_bindings import CURRENT_ROUTES, current_handler
from regional_knowledge.kaggle_geo_upgrade import GeoUpgradePins
from regional_knowledge.kaggle_native_source_upgrade import (
    NativeSourceUpgradePins, native_source_upgrade_handler,
)
from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES, native_source_handler


def first_book_entries(repository):
    pins = json.loads((repository / 'scripts/production/manifests/'
                      'rkb-migration-pins-39793744232.json').read_text())

    def measured(inventory, name):
        values = [value for key, value in pins[inventory].items()
                  if key.startswith(name + '(')]
        assert len(values) == 1
        return values[0]

    originals = []
    for route in sorted(CURRENT_ROUTES):
        template = current_handler(route, check_sha256='0' * 64, apply_sha256='0' * 64)
        originals.append(current_handler(route,
            check_sha256=measured('schema11_function_definitions', template.check.name),
            apply_sha256=measured('schema11_function_definitions', template.apply.name)))
    geo = GeoUpgradePins(**pins['geo_upgrade_function_pins'],
        origin_catalog_sha256='0' * 64, origin_acl_sha256='1' * 64)
    native = NativeSourceUpgradePins.from_wire(pins['native_source_upgrade'])
    handlers = list(geo.registry(HandlerRegistry(originals)).handlers.values())
    handlers.append(native_source_upgrade_handler(native))
    for route in sorted(NATIVE_ROUTES):
        template = native_source_handler(route, check_sha256='0' * 64, apply_sha256='0' * 64)
        handlers.append(native_source_handler(route,
            check_sha256=measured('schema14_function_definitions', template.check.name),
            apply_sha256=measured('schema14_function_definitions', template.apply.name)))
    assert len(handlers) == len(HandlerRegistry(handlers).handlers) == 33
    entries = []
    for handler in handlers:
        entry = dict(route=handler.route, revisions=list(handler.revisions.wire()),
            check_sha256=handler.check.definition_sha256, apply_sha256=handler.apply.definition_sha256)
        if handler.transition:
            entry['artifact_sha256'] = handler.transition.artifact_sha256
        if handler.route == 'source.native.upgrade.v1':
            entry['native_source_upgrade'] = native.wire()
        entries.append(entry)
    return entries
