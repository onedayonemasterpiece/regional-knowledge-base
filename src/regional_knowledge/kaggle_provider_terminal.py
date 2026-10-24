"""Exact-version terminal status through the native Kaggle API.

No launch, cancellation, output retrieval, credentials or caller termination flags.
See docs/operations/kaggle-provider-status.md for the provider contract and limits.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib import import_module
import math
import signal
import threading
import time
from types import MappingProxyType

MAX_STATUS_BYTES = 16384
MAX_OBSERVATION_SECONDS = 12
MAX_PROOF_AGE_SECONDS = 30
CONTRACT = 'kaggle-2.2.4-sdk-0.1.37-exact-session-persisted-stream-v1'
_SNAPSHOT = ('id', 'provider_ref', 'provider_version', 'launch_state', 'status',
             'launch_nonce', 'runtime_incarnation', 'connection_generation',
             'channel_active', 'attachment_conflict', 'heartbeat', 'started')
_OBSERVED = object()


@dataclass(frozen=True)
class _NativeTerminalProof:
    snapshot: object
    started_at: float
    observed_at: float
    status: str
    contract: str = CONTRACT
    _seal: object = field(default=None, repr=False, compare=False)

    def receipt(self):
        # Preserve the existing receipt identifier and identity/CAS semantics.
        # The provider response is not retained; no source/log read is needed.
        return {'contract': self.contract, 'identity': dict(self.snapshot),
                'started_at': self.started_at, 'observed_at': self.observed_at,
                'status': self.status}


def _phase(response):
    value = getattr(response, 'status', None)
    return str(getattr(value, 'name', value)).rsplit('.', 1)[-1].upper()


def _request(name, ref, number):
    sdk = import_module('kagglesdk.kernels.types.kernels_api_service')
    request = getattr(sdk, name)()
    if not hasattr(request, 'version_label'):
        raise ValueError('version-qualified native request unavailable')
    request.user_name, request.kernel_slug = ref.split('/')
    request.version_label = 'v' + str(number)
    return request


def _body(response, maximum, deadline, monotonic):
    try:
        if response.status_code != 200:
            raise ValueError('native response not successful')
        length = response.headers.get('Content-Length')
        if length is not None and (int(length) < 0 or int(length) > maximum):
            raise ValueError('native response exceeds budget')
        parts = []; size = 0
        for chunk in response.iter_content(chunk_size=8192):
            if monotonic() >= deadline:
                raise TimeoutError('native observation deadline')
            size += len(chunk)
            if size > maximum:
                raise ValueError('native response exceeds budget')
            parts.append(chunk)
        return b''.join(parts)
    finally:
        response.close()


@contextmanager
def _total_deadline(deadline, monotonic):
    """Interrupt blocked headers/body reads, not just gaps between chunks.

    The existing standalone controller runs on the POSIX main thread. Refuse
    unsupported/occupied signal contexts before any network read rather than
    silently weakening the elapsed-time bound or leaving background readers.
    """
    if (threading.current_thread() is not threading.main_thread()
            or not hasattr(signal, 'setitimer')
            or signal.getitimer(signal.ITIMER_REAL) != (0., 0.)
            or signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, [])):
        raise RuntimeError('exclusive main-thread deadline unavailable')
    remaining = deadline - monotonic()
    if remaining <= 0: raise TimeoutError('native observation deadline')
    prior = signal.getsignal(signal.SIGALRM)
    def expired(signum, frame):
        raise TimeoutError('native observation deadline')
    signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, remaining)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prior)


@contextmanager
def _bounded_transport(client, deadline, monotonic):
    with _total_deadline(deadline, monotonic):
        with _native_send_boundary(client, deadline, monotonic) as budget:
            yield budget


@contextmanager
def _native_send_boundary(client, deadline, monotonic):
    """Bound before SDK JSON decoding; the SDK otherwise buffers JSON.

    This fresh native client is owned by this observation, never shared with
    another operation. Only its send boundary is wrapped; native request methods
    still select the provider operation and serialize version_label.
    """
    http = client._http_client
    http._init_session()
    session = http._session
    original = session.send
    verbose = getattr(http, '_verbose', None)
    if verbose is not None: http._verbose = False
    budget = [MAX_STATUS_BYTES]

    def send(request, **kwargs):
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError('native observation deadline')
        kwargs.update(timeout=(min(3, remaining), min(3, remaining)), stream=True)
        # Session.send can consume intermediate redirect bodies even with
        # allow_redirects=False (next-request preparation). Use only its selected
        # HTTPAdapter.send, which disables urllib3 redirect handling. The SDK
        # still prepared/authenticated the exact native request; no raw endpoint
        # or new credential path is introduced. Every 3xx fails before body read.
        kwargs.pop('allow_redirects', None)
        response = session.get_adapter(request.url).send(request, **kwargs)
        # Cache only bounded bytes before KaggleHttpClient._prepare_response
        # examines application/json. Response.json/content then use this cache.
        content = _body(response, budget[0], deadline, monotonic)
        response._content = content
        response._content_consumed = True
        return response

    session.send = send
    try:
        yield budget
    finally:
        session.send = original
        if verbose is not None: http._verbose = verbose


def observe_version_terminal(api, run, *, clock=time.time, monotonic=time.monotonic):
    """Observe only the stored owner/slug/version; never fall back to latest.

    Terminal phases close the compute reservation. Writer authority, accepted
    commands, publication and result settlement retain their existing fences.
    """
    if (type(run.get('provider_version')) is not int or run['provider_version'] < 1
            or type(run.get('provider_ref')) is not str
            or run['provider_ref'].count('/') != 1
            or not all(run['provider_ref'].split('/'))
            or run.get('launch_state') not in ('dispatching', 'dispatched')):
        return None
    began = clock(); deadline = monotonic() + MAX_OBSERVATION_SECONDS
    try:
        snapshot = MappingProxyType({k: run[k] for k in _SNAPSHOT})
        ref, number = run['provider_ref'], run['provider_version']
        with api.build_kaggle_client() as client:
            with _bounded_transport(client, deadline, monotonic) as budget:
                native = client.kernels.kernels_api_client
                phase = _phase(native.get_kernel_session_status(
                    _request('ApiGetKernelSessionStatusRequest', ref, number)))
                if phase not in ('ERROR', 'COMPLETE', 'CANCEL_ACKNOWLEDGED'): return None
        ended = clock()
        if (monotonic() >= deadline or not all(map(math.isfinite, (began, ended)))
                or ended < began or began <= run['heartbeat']):
            return None
        return _NativeTerminalProof(snapshot, began, ended, phase, _seal=_OBSERVED)
    except Exception:
        # Body/error text may contain secrets. Controller records only UNKNOWN;
        # failed capability/transport never retires a reservation.
        return None


def validate_proof(proof, run, now):
    if type(proof) is not _NativeTerminalProof or proof._seal is not _OBSERVED:
        raise TypeError('native observed proof required')
    return (proof.contract == CONTRACT and math.isfinite(now)
            and 0 <= now - proof.observed_at <= MAX_PROOF_AGE_SECONDS
            and proof.started_at > run['heartbeat']
            and all(proof.snapshot[k] == run[k] for k in _SNAPSHOT))
