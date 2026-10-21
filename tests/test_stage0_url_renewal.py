"""Expired native-package links must not survive into a new launch intent."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from regional_knowledge.kaggle_stage0 import Stage0Download, Stage0Spec
from regional_knowledge.kaggle_stage0_urls import (
    PinnedStage0Object, S3Stage0GetSigner, renew_stage0_urls, renew_target_stage0_urls,
)

NOW = datetime(2026, 10, 21, 14, tzinfo=timezone.utc)
ORIGIN = 'https://storage.example.invalid'


def pin(name='native.tar', key='native/packages.tar', sha='a'*64):
    return PinnedStage0Object(name, ORIGIN, 'rkb-fixture', key, 128, sha)


def signed(item, when=NOW, seconds=3600, **overrides):
    fields = {'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
              'X-Amz-Credential': 'FIXTURE/20261021/fixture/s3/aws4_request',
              'X-Amz-Date': when.strftime('%Y%m%dT%H%M%SZ'),
              'X-Amz-Expires': str(seconds), 'X-Amz-SignedHeaders': 'host',
              'X-Amz-Signature': '0'*64}
    fields.update(overrides)
    return item.url + '?' + urlencode(fields)


def plan(items, *, when=NOW, seconds=3600):
    downloads = tuple(Stage0Download(p.name, signed(p, when, seconds), p.size, p.sha256)
                      for p in items)
    wheel = Stage0Download('fixture.whl', 'https://packages.example.invalid/fixture.whl',
                           64, 'b'*64)
    return Stage0Spec(downloads+(wheel,), ('fixture.whl',), {'fixture': '1'}, 256, 60)


def test_expired_links_renew_same_objects_before_new_expectation():
    objects = (pin(), pin('native.json', 'native/manifest.json', 'c'*64))
    before = plan(objects, when=NOW-timedelta(days=1))
    calls = []
    def sign(item, seconds):
        calls.append((item, seconds))
        return signed(item, seconds=seconds)
    after = renew_stage0_urls(before, objects=objects, sign_get=sign, now=NOW)
    assert calls == [(p, 3600) for p in objects]
    for old, new in zip(before.downloads, after.downloads):
        assert (old.name, old.size, old.sha256, old.executable) == (
            new.name, new.size, new.sha256, new.executable)
    assert before.downloads[0].url != after.downloads[0].url
    assert after.downloads[-1] is before.downloads[-1]
    assert (after.wheels, after.packages, after.install_bytes, after.total_seconds) == (
        before.wheels, before.packages, before.install_bytes, before.total_seconds)
    assert renew_stage0_urls(after, objects=objects, sign_get=lambda *a: pytest.fail('resigned fresh URL'),
                            now=NOW) is after


@pytest.mark.parametrize('change', [
    {'key': 'other/secret'}, {'origin': 'https://other.example.invalid'},
    {'sha256': 'f'*64}, {'size': 129}, {'name': 'other.tar'},
])
def test_whole_inventory_validated_before_any_signing(change):
    objects = (pin(), pin('native.json', 'native/manifest.json', 'c'*64))
    before = plan(objects, when=NOW-timedelta(days=1))
    with pytest.raises(ValueError):
        renew_stage0_urls(before, objects=(objects[0], replace(objects[1], **change)),
                          sign_get=lambda *a: pytest.fail('signed before checking all pins'), now=NOW)


@pytest.mark.parametrize('replacement', ['other_object', 'http', 'expired', 'future',
                                       'short', 'unsigned', 'duplicate_date', 'bad_signature'])
def test_signer_cannot_swap_destination_or_return_invalid_lifetime(replacement):
    item = pin(); before = plan((item,), when=NOW-timedelta(days=1))
    urls = {'other_object': signed(replace(item, key='other/object')),
            'http': signed(item).replace('https:', 'http:', 1),
            'expired': signed(item, when=NOW-timedelta(days=1)),
            'future': signed(item, when=NOW+timedelta(hours=1)),
            'short': signed(item, seconds=60), 'unsigned': item.url,
            'duplicate_date': signed(item)+'&X-Amz-Date=20261021T140000Z',
            'bad_signature': signed(item, **{'X-Amz-Signature': 'invalid'})}
    with pytest.raises(ValueError):
        renew_stage0_urls(before, objects=(item,), sign_get=lambda *a: urls[replacement], now=NOW)
    assert before.downloads[0].url == signed(item, when=NOW-timedelta(days=1))


def test_near_expiry_is_renewed_and_download_budget_is_covered():
    item = pin(); before = plan((item,), seconds=1800)
    after = renew_stage0_urls(before, objects=(item,), sign_get=lambda p, s: signed(p, seconds=s),
                             now=NOW+timedelta(seconds=1))
    assert after is not before
    with pytest.raises(ValueError):
        renew_stage0_urls(before, objects=(item,), sign_get=lambda *a: pytest.fail(), now=NOW,
                          minimum_remaining_seconds=30)


def test_unmapped_signed_object_is_not_silently_ignored():
    a, b = pin(), pin('native.json', 'native/manifest.json')
    with pytest.raises(ValueError):
        renew_stage0_urls(plan((a,b)), objects=(a,), sign_get=lambda *a: pytest.fail(), now=NOW)


def test_encoded_unmapped_signing_fields_fail_before_first_signer_call():
    a, b = pin(), pin('native.json', 'native/manifest.json')
    before = plan((a,b), when=NOW-timedelta(days=1))
    encoded = replace(before.downloads[1], url=before.downloads[1].url.replace('X-Amz-', 'X%2DAmz-'))
    before = replace(before, downloads=(before.downloads[0], encoded, before.downloads[2]))
    calls = []
    def sign(p, seconds):
        calls.append(p.name)
        return signed(p, seconds=seconds)
    with pytest.raises(ValueError, match='stage0_signed_object_not_pinned'):
        renew_stage0_urls(before, objects=(a,), sign_get=sign, now=NOW)
    assert calls == []


def test_existing_s3_signer_uses_get_only_and_exact_bucket_key():
    calls = []
    item = pin()
    class Client:
        meta = SimpleNamespace(endpoint_url=ORIGIN)
        def generate_presigned_url(self, operation, **kwargs):
            calls.append((operation, kwargs))
            return signed(item)
    signer = S3Stage0GetSigner(client=Client(), bucket=item.bucket)
    assert signer(item, 3600) == signed(item)
    assert calls == [('get_object', {'Params': {'Bucket': item.bucket, 'Key': item.key},
                                    'ExpiresIn': 3600, 'HttpMethod': 'GET'})]
    with pytest.raises(ValueError): signer(replace(item, bucket='other-bucket'), 3600)
    with pytest.raises(ValueError): signer(replace(item, origin='https://other.example.invalid'), 3600)
    assert len(calls) == 1


def test_target_composition_keeps_pins_and_never_rewrites_existing_intent(tmp_path):
    import ast
    from test_kaggle_target_runtime import spec, stage0, queue_run, generate
    before = spec(); preparation = stage0(before)
    artifact = preparation.downloads[-1]
    item = PinnedStage0Object(artifact.name, ORIGIN, 'rkb-fixture', 'native/fixed',
                             artifact.size, artifact.sha256)
    downloads = preparation.downloads[:-1]+(replace(artifact, url=signed(item, when=NOW-timedelta(days=1))),)
    before = replace(before, stage0=replace(preparation, downloads=downloads))
    q, run = queue_run(tmp_path/'old')
    original_source = generate(q, run, before)
    after = renew_target_stage0_urls(before, objects=(item,), now=NOW,
                                    sign_get=lambda p, s: signed(p, seconds=s))
    old_wire, new_wire = before.wire(), after.wire()
    old_wire.pop('stage0'); new_wire.pop('stage0')
    assert old_wire == new_wire
    with pytest.raises(ValueError, match='immutable'):
        generate(q, run, after)
    assert generate(q, run, before) == original_source
    q2, run2 = queue_run(tmp_path/'new')
    source = generate(q2, run2, after)
    config = next(ast.literal_eval(n.value) for n in ast.parse(source).body
                  if isinstance(n, ast.Assign) and any(isinstance(x, ast.Name) and x.id=='RUN_CONFIG'
                                                       for x in n.targets))
    assert config['stage0']['downloads'][-1]['url'] == signed(item)


@pytest.mark.parametrize('value', [None, 123, 'not a URL'])
def test_bad_signer_output_has_fixed_non_secret_error(value):
    item = pin()
    with pytest.raises(ValueError, match='^stage0_signed_get_binding_invalid$'):
        renew_stage0_urls(plan((item,), when=NOW-timedelta(days=1)), objects=(item,),
                          sign_get=lambda *a: value, now=NOW)


def test_signing_failure_does_not_expose_sdk_message():
    item = pin()
    def failing(*args):raise RuntimeError('private fixture credential')
    with pytest.raises(ValueError, match='^stage0_signing_failed$'):
        renew_stage0_urls(plan((item,), when=NOW-timedelta(days=1)), objects=(item,),
                          sign_get=failing, now=NOW)
