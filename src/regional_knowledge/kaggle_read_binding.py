"""Runtime read identity shared with native leaves; no OAuth/server dependencies."""
from dataclasses import dataclass


class SourceGateDenied(PermissionError):
    pass


@dataclass(frozen=True)
class TargetReadBinding:
    run_id: str
    runtime_incarnation: str
    connection_generation: int
    database_generation: str
    min_data_seq: int
    provider_version: str


def binding_key(binding):
    fields = ('run_id', 'runtime_incarnation', 'connection_generation',
              'database_generation', 'min_data_seq', 'provider_version')
    values = tuple(getattr(binding, name, None) for name in fields)
    if (any(not isinstance(values[i], str) or not 1 <= len(values[i].encode()) <= 256
            for i in (0, 1, 3, 5))
            or type(values[2]) is not int or values[2] < 1
            or type(values[4]) is not int or values[4] < 0):
        raise SourceGateDenied('trusted runtime binding required')
    return values
