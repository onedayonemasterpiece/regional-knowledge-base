"""Pinned BGE dense contract; no inference dependencies in the consuming service."""
import math

MODEL = 'BAAI/bge-m3'
REVISION = '5617a9f61b028005a4858fdac845db406aefb181'
SPACE = 'bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1'
DIMENSION = 1024
MAX_TOKENS = 512
TOKENIZER_SHA256 = '21106b6d7dab2952c1d496fb21d5dc9db75c28ed361a05f5020bbba27810dd08'

def validate_vector(values):
    if not isinstance(values, list) or len(values) != DIMENSION:
        raise ValueError('BGE dimension mismatch')
    vector = [float(value) for value in values]
    if not all(math.isfinite(value) for value in vector):
        raise ValueError('BGE nonfinite vector')
    if abs(math.sqrt(sum(value * value for value in vector)) - 1) > .002:
        raise ValueError('BGE normalization mismatch')
    return vector
