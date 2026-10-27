# Isolated original parser dependency resolution

Stage0's temporary-root installation places verified wheels in `.rkb-site`
and adds it to the worker's `sys.path`. A `python -I script` child does not
inherit that addition. A fresh isolated interpreter reproduced a missing
`pymupdf` import on a synthetic PDF; adding the installed PyMuPDF 1.28.2
distribution root made the unchanged parser successfully inspect it.

The native launcher now captures the distribution root alongside the existing
deployment version check. Its isolated bootstrap prepends only that root and
checks the child distribution version/root and module origin before executing
the unchanged parser script. The root is deployment-derived, never a request
argument. `-I`, the minimal environment, parser argv/main resource limits,
output bound, timeout and cancellation/reaping behavior remain in place.
The parser script bytes and existing current/historical receipt pins do not
change. Failure logging contains only allowlisted exception class, action and
return code; no path, exception message or original content is logged.

Synthetic child tests cover inspect/render/text, a competing base installation,
cwd/PYTHONPATH shadowing, wrong root/version rejection before parser entry,
normal venv installation, historical pin selection, and DjVu behavior.
These checks establish the dependency-resolution defect and source fix, not
live incident attribution or acceptance of a restored private original.

Known failed restore reconciliation is separate: `_restore_handle` retains its
original `restore_json` args/nonce and refuses a different runtime identity.
This change does not clear, rebind or retry those rows. A controlled recovery
must reconcile the exact failed job and prior runtime before reusing that
same restore intent; the existing code has no automatic known-failure rebind.
