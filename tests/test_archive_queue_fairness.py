"""Fairness of source archiving while story-vector outbox keeps growing."""
from regional_knowledge.indexing import IndexReconciler


def test_story_backlog_cannot_indefinitely_starve_archive():
    indexer = IndexReconciler.__new__(IndexReconciler)
    indexer.last_archive_tick = 100.0
    assert not indexer.archive_due([{"id": "pending-vector"}], True, 161.0)
    assert not indexer.archive_due([], True, 159.9)
    assert indexer.archive_due([], True, 160.0)
    indexer.last_archive_tick = 160.0
    assert not indexer.archive_due([], True, 160.01)
    assert indexer.archive_due([], True, 220.0)
    assert indexer.archive_due([], False, 160.01)
    assert not indexer.archive_due([{"id": "pending-vector"}], False, 220.0)
