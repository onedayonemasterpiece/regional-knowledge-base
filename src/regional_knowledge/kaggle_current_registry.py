"""Current executable routes, compiled from the verified application code.

SQL structure/version belongs to the migration ledger. Registry hashes bind the
route ABI (fixed function names, preconditions and dependencies), never an
observed PostgreSQL catalog or deployment-supplied function fingerprint.
"""
from hashlib import sha256

from .kaggle_apply import HandlerRegistry
from .kaggle_native_source_bindings import NATIVE_ROUTES,native_source_handler


def current_registry():
    handlers=[]
    for route in sorted(NATIVE_ROUTES):
        # The historical field name is part of the v1 handler audit shape.
        # Current values bind fixed callable signatures from code, without a
        # runtime measurement or an operator-maintained pin inventory.
        prototype=native_source_handler(route,check_sha256='0'*64,apply_sha256='0'*64)
        handlers.append(native_source_handler(route,
            check_sha256=sha256(prototype.check.signature.encode()).hexdigest(),
            apply_sha256=sha256(prototype.apply.signature.encode()).hexdigest()))
    return HandlerRegistry(handlers)


def registry_sha256(registry=None):
    from .kaggle_apply import _hash,_json
    registry=current_registry() if registry is None else registry
    return _hash(_json(sorted(handler.fingerprint for handler in registry.handlers.values())))
