"""Small, payload-free facts for queue/controller exception boundaries."""
from pathlib import Path
import re
import sqlite3


_PACKAGE = Path(__file__).resolve().parent
_CONTROLLER = _PACKAGE.parent.parent / 'scripts/production/bge_kaggle_controller.py'
_SQLITE_CODES = {
    name: value for name, value in vars(sqlite3).items()
    if name.startswith('SQLITE_') and type(value) is int
}


def failure_facts(error):
    """Expose native SQLite codes and at most three trusted source locations.

    Never inspect exception text, causes, locals, SQL or provider responses.
    Non-SQLite objects cannot supply SQLite attributes. Source locations omit
    absolute paths and exclude third-party, generated and caller code.
    """
    facts = {}
    if not isinstance(error, BaseException):
        return facts
    if isinstance(error, sqlite3.Error):
        code = getattr(error, 'sqlite_errorcode', None)
        name = getattr(error, 'sqlite_errorname', None)
        if type(code) is int and type(name) is str and _SQLITE_CODES.get(name) == code:
            facts.update(sqlite_errorcode=code, sqlite_errorname=name)
    frames = []
    trace = error.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if ((path.parent == _PACKAGE or path == _CONTROLLER)
                and re.fullmatch(r'[a-z_][a-z0-9_]{0,127}\.py', path.name)
                and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_<>]{0,127}', code.co_name)):
            frames.append(dict(file=path.name, function=code.co_name, line=trace.tb_lineno))
            frames = frames[-3:]
        trace = trace.tb_next
    if frames:
        facts['error_frames'] = frames
    return facts
