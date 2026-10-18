"""Component metadata stays immutable; shared archive state stays current."""
import json
from contextlib import contextmanager

import pytest

from regional_knowledge.catalog_components import visible


class Corpus:
    def __init__(self, row, parent, denied=False):
        self.payload = json.dumps(row)
        self.parent = parent
        self.denied = denied

    @contextmanager
    def connect(self):
        yield self

    def execute(self, query):
        assert query == 'select payload from catalog_components'
        return self

    def fetchall(self):
        return [(self.payload,)]

    def authorize(self, actor, parent_id):
        assert actor == 'owner' and parent_id == 'issue'
        if self.denied:
            raise PermissionError('not visible')
        return self.parent


@pytest.mark.parametrize('state', ['verified', 'pending', 'failed', None])
def test_component_projects_current_archive_state_for_identical_original(state):
    row = {'id': 'article', 'parent_id': 'issue', 'source_sha256': 'a' * 64,
           'source_archive_status': 'pending', 'active_revision': 1,
           'page_ids': ['page'], 'chunk_ids': ['chunk']}
    parent = {'source_sha256': 'a' * 64, 'source_archive_status': state,
              'active_revision': 2}
    corpus = Corpus(row, parent)
    assert visible(corpus, 'owner') == [{**row, 'source_archive_status': state}]
    assert json.loads(corpus.payload) == row


def test_component_does_not_inherit_replacement_original_archive_state():
    row = {'id': 'article', 'parent_id': 'issue', 'source_sha256': 'a' * 64,
           'source_archive_status': 'pending', 'active_revision': 1}
    corpus = Corpus(row, {'source_sha256': 'b' * 64,
                          'source_archive_status': 'verified', 'active_revision': 2})
    assert visible(corpus, 'owner') == [row]


def test_component_archive_projection_preserves_parent_authorization():
    row = {'id': 'article', 'parent_id': 'issue', 'source_sha256': 'a' * 64}
    assert visible(Corpus(row, {}, denied=True), 'owner') == []


@pytest.mark.parametrize('digest', [None, ''])
def test_component_without_source_hash_does_not_inherit_archive_state(digest):
    row = {'id': 'article', 'parent_id': 'issue', 'source_sha256': digest,
           'source_archive_status': 'pending'}
    assert visible(Corpus(row, {'source_sha256': digest,
                                'source_archive_status': 'verified'}), 'owner') == [row]


def test_component_missing_parent_remains_hidden():
    class MissingParent(Corpus):
        def authorize(self, actor, parent_id):
            raise LookupError('missing parent')
    assert visible(MissingParent({'parent_id': 'issue'}, {}), 'owner') == []
