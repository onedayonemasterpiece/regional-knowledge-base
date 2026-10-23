"""Renew explicitly pinned Stage0 GET links before composing a new launch.

Controller-side only: no environment/credential discovery, network transfer,
queue mutation or provider operation. The caller supplies its existing signer
and independent object pins. Returned URLs are private runtime metadata.
"""
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import re
from urllib.parse import parse_qsl, quote, urlsplit

from .kaggle_stage0 import MAX_FILES, Stage0Download, Stage0Spec


def _origin(value):
    try:
        if type(value) is not str:
            raise ValueError()
        parsed = urlsplit(value)
        valid = (parsed.scheme == 'https' and parsed.hostname and parsed.geturl() == value
                 and parsed.netloc == parsed.hostname and not parsed.path
                 and not parsed.query and not parsed.fragment)
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError('stage0_exact_signing_origin_required')
    return value


def _seconds(value, *, minimum=1):
    if type(value) is not int or not minimum <= value <= 604800:
        raise ValueError('stage0_bounded_signing_lifetime_required')
    return value


@dataclass(frozen=True)
class PinnedStage0Object:
    """Existing deployment object, not a source-selected signing destination."""
    name: str
    origin: str
    bucket: str
    key: str
    size: int
    sha256: str

    def __post_init__(self):
        _origin(self.origin)
        if (type(self.bucket) is not str
                or re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', self.bucket) is None
                or '..' in self.bucket
                or type(self.key) is not str or not 1 <= len(self.key.encode()) <= 1024
                or any(part in ('', '.', '..') for part in self.key.split('/'))
                or any(ord(c) < 32 or ord(c) == 127 for c in self.key)):
            raise ValueError('stage0_exact_signing_object_required')
        Stage0Download(self.name, self.url, self.size, self.sha256)

    @property
    def url(self):
        # Explicit path-style S3 binding. No guessed host-style bucket mapping.
        return self.origin+'/'+quote(self.bucket, safe='')+'/'+quote(self.key, safe='/')


def _expiry(url, pin, now):
    """Validate one exact SigV4 GET destination without exposing its query."""
    try:
        if type(url) is not str:
            raise ValueError()
        parsed = urlsplit(url)
        if (len(url) > 16384 or parsed.fragment
                or parsed._replace(query='').geturl() != pin.url):
            raise ValueError()
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True,
                          max_num_fields=8)
        fields = dict(pairs)
        required = {'X-Amz-Algorithm', 'X-Amz-Credential', 'X-Amz-Date',
                    'X-Amz-Expires', 'X-Amz-SignedHeaders', 'X-Amz-Signature'}
        if (len(fields) != len(pairs) or not required <= fields.keys()
                or fields.keys()-required-{'X-Amz-Security-Token'}
                or fields['X-Amz-Algorithm'] != 'AWS4-HMAC-SHA256'
                or not 1 <= len(fields['X-Amz-Credential']) <= 2048
                or fields['X-Amz-SignedHeaders'] != 'host'
                or re.fullmatch('[0-9a-f]{64}', fields['X-Amz-Signature']) is None
                or re.fullmatch('[0-9]{8}T[0-9]{6}Z', fields['X-Amz-Date']) is None
                or re.fullmatch('[1-9][0-9]{0,5}', fields['X-Amz-Expires']) is None):
            raise ValueError()
        started = datetime.strptime(fields['X-Amz-Date'], '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
        lifetime = _seconds(int(fields['X-Amz-Expires']))
        if started > now+timedelta(seconds=60):
            raise ValueError()
        return started+timedelta(seconds=lifetime)
    except (ValueError, TypeError, KeyError, OverflowError):
        # Never interpolate the signed URL, access-key ID or SDK exception.
        raise ValueError('stage0_signed_get_binding_invalid') from None


class S3Stage0GetSigner:
    """Use an existing service client; never construct or discover credentials."""
    def __init__(self, *, client, bucket):
        self.client, self.bucket = client, bucket
        self.origin = _origin(client.meta.endpoint_url.rstrip('/'))

    def __call__(self, pin, seconds):
        _seconds(seconds)
        if (type(pin) is not PinnedStage0Object or pin.origin != self.origin
                or pin.bucket != self.bucket):
            raise ValueError('stage0_signing_authority_differs')
        return self.client.generate_presigned_url('get_object',
            Params={'Bucket': pin.bucket, 'Key': pin.key}, ExpiresIn=seconds, HttpMethod='GET')


def renew_stage0_urls(stage0, *, objects, sign_get, now,
                      minimum_remaining_seconds=1800, lifetime_seconds=3600):
    """Return a detached plan, preserving every pin and unrelated URL.

    Call BEFORE queue runtime expectations or provider launch claims. Existing
    expectations are immutable and must not be rewritten to accommodate new
    signatures. A live/UNKNOWN operation must reconcile its original intent.
    All old bindings are checked before invoking the signer even once.
    """
    if (type(stage0) is not Stage0Spec or type(objects) is not tuple
            or not 1 <= len(objects) <= MAX_FILES
            or any(type(pin) is not PinnedStage0Object for pin in objects)
            or len({pin.name for pin in objects}) != len(objects)
            or not callable(sign_get) or type(now) is not datetime or now.tzinfo is None
            or now.utcoffset() is None):
        raise ValueError('stage0_exact_renewal_inputs_required')
    minimum = _seconds(minimum_remaining_seconds, minimum=stage0.total_seconds)
    lifetime = _seconds(lifetime_seconds, minimum=minimum+60)
    now = now.astimezone(timezone.utc)
    declared = {item.name: item for item in stage0.downloads}
    pins = {pin.name: pin for pin in objects}
    expired = set()
    for pin in objects:
        item = declared.get(pin.name)
        if item is None or (item.size, item.sha256) != (pin.size, pin.sha256):
            raise ValueError('stage0_signing_inventory_differs')
        if (_expiry(item.url, pin, now)-now).total_seconds() < minimum:
            expired.add(pin.name)
    for item in stage0.downloads:
        if item.name not in pins:
            try:
                names = {key for key, _ in parse_qsl(urlsplit(item.url).query,
                    keep_blank_values=True, max_num_fields=64)}
            except ValueError:
                raise ValueError('stage0_unmapped_query_invalid') from None
            if any(key.startswith('X-Amz-') for key in names):
                raise ValueError('stage0_signed_object_not_pinned')
    if not expired:
        return stage0
    downloads = []
    for item in stage0.downloads:
        if item.name in expired:
            pin = pins[item.name]
            try:
                url = sign_get(pin, lifetime)
            except Exception:
                raise ValueError('stage0_signing_failed') from None
            remaining = (_expiry(url, pin, now)-now).total_seconds()
            if not minimum <= remaining <= lifetime+60:
                raise ValueError('stage0_renewed_lifetime_differs')
            item = replace(item, url=url)
        downloads.append(item)
    return replace(stage0, downloads=tuple(downloads))


def renew_target_stage0_urls(target, **options):
    """Preserve the complete typed target; only Stage0 URL signatures change."""
    from .kaggle_target_runtime import TargetLaunchSpec
    if type(target) is not TargetLaunchSpec or target.stage0 is None:
        raise ValueError('stage0_typed_target_required')
    stage0 = renew_stage0_urls(target.stage0, **options)
    return target if stage0 is target.stage0 else replace(target, stage0=stage0)
