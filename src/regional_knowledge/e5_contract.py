"""Pinned fast-tier identity; pure contracts shared by client and encoder."""
import math

MODEL_REVISION = '761b726dd34fb83930e26aab4e9ac3899aa1fa78'
MODEL_SHA256 = 'f80102d3f2a1229f387d3c81909990d8945513e347b0eab049f7de3c6f98c193'
TOKENIZER_SHA256 = '0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39'
SPACE = 'e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1'
DIMENSION = 384
MAX_TOKENS = 512
TARGET_PASSAGE_TOKENS = 256

def validate_vector(vector):
    if not isinstance(vector, list) or len(vector) != DIMENSION:
        raise ValueError('E5 requires384 components')
    values = [float(v) for v in vector]
    if not all(math.isfinite(v) for v in values):
        raise ValueError('nonfinite E5 vector')
    if abs(math.sqrt(sum(v*v for v in values))-1) > .002:
        raise ValueError('E5 vector must be L2 normalized')
    return values


def prepare_text(role, text):
    if role not in ('query','passage'):
        raise ValueError('invalid E5 role')
    return ('query: ' if role=='query' else 'passage: ')+text
