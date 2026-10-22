"""Current-route metadata fixture; function digests are synthetic, not live pins."""
from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES


def first_book_entries(repository):
    return [dict(route=route,check_sha256='a'*64,apply_sha256='b'*64)
        for route in sorted(NATIVE_ROUTES)]
