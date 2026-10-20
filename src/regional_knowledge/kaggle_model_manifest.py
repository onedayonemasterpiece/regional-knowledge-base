"""Public immutable metadata for the protected original BGE model revision.

No weights are downloaded or loaded by this module. LFS digests/sizes come from
the official revision API; small config bytes were also matched to Git blob IDs.
https://huggingface.co/api/models/BAAI/bge-m3/revision/5617a9f61b028005a4858fdac845db406aefb181?blobs=true
"""
from .bge_contract import REVISION, TOKENIZER_SHA256

_REVISION = '5617a9f61b028005a4858fdac845db406aefb181'
_FILES = (
    ('config.json', 687, '26159e7ad065073448460117eb24b7a4572f6f4e78eadff65dc0a11c052449fa', 'model_config'),
    ('tokenizer_config.json', 444, 'a62b2b6784f990259fddef5f16388693a8043be4f69179e6a5257eeb3f9abac4', 'model_config'),
    ('special_tokens_map.json', 964, '8c785abebea9ae3257b61681b4e6fd8365ceafde980c21970d001e834cf10835', 'model_config'),
    ('tokenizer.json', 17098108, '21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08', 'tokenizer'),
    ('pytorch_model.bin', 2271145830, 'b5e0ce3470abf5ef3831aa1bd5553b486803e83251590ab7ff35a117cf6aad38', 'weight'),
    ('sentencepiece.bpe.model', 5069051, 'cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865', 'model_config'),
)


def bge_model_manifest():
    """Return detached prepared-inventory/download rows for Kaggle Stage0 only.

    This revision publishes pytorch_model.bin, not safetensors. Preserve the
    original bytes and inference space; no conversion/latest-revision fallback.
    Native PostgreSQL and the complete wheel closure need their own exact pins.
    """
    if REVISION != _REVISION or _FILES[3][2] != TOKENIZER_SHA256:
        raise ValueError('public model metadata differs from protected BGE pins')
    folder = 'bge-m3-' + _REVISION
    entries, downloads = [], []
    for filename, size, digest, role in _FILES:
        name = folder + '/' + filename
        entries.append(dict(name=name, size=size, sha256=digest, role=role))
        downloads.append(dict(name=name, size=size, sha256=digest, executable=False,
            url=f'https://huggingface.co/BAAI/bge-m3/resolve/{_REVISION}/{filename}'))
    return tuple(entries), tuple(downloads)
