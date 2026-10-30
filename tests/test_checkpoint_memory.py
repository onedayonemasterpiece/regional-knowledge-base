"""Synthetic-only regression: checkpoint metadata must not require a second full buffer."""
from contextlib import contextmanager
import hashlib
import json
import tracemalloc
from types import SimpleNamespace as NS

import pytest
from kagglesdk.datasets.types.dataset_api_service import ApiDownloadDatasetRequest  # warm SDK import before measuring buffers

from regional_knowledge.kaggle_dataset_base import DatasetFile
from regional_knowledge.kaggle_dataset_publisher import (
    DatasetPublicationError, KaggleDatasetPublisher,
)
from test_dataset_recovery_store import prepared


def test_checkpoint_control_buffers_peak_and_exact_bytes(tmp_path):
    store = prepared(tmp_path).store
    content = b"x" * 65536
    with store.queue.connect() as db:
        db.executemany(
            "INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,NULL)",
            ((store.storage_identity,store.prefix + f"outcomes/synthetic/{i:04d}",
              content,"synthetic") for i in range(128)),
        )

    tracemalloc.start()
    journal = store.export_control()
    _, export_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    # An extra full-size conversion made the old implementation exceed 1.8x.
    assert export_peak < len(journal) * 1.8
    assert journal == json.dumps(json.loads(journal),sort_keys=True,separators=(",",":")).encode()

    body = b"j" * (16 * 1024**2 + 19)
    closed = []

    class Response:
        status_code = 200

        def iter_content(self, chunk_size):
            assert chunk_size == 65536
            for offset in range(0, len(body), chunk_size):
                yield body[offset:offset + chunk_size]

        def close(self):
            closed.append(True)

    class Api:
        @contextmanager
        def build_kaggle_client(self):
            def download(request):
                assert isinstance(request,ApiDownloadDatasetRequest)
                assert request.file_name == "journal-control.json" and request.dataset_version_number == 7
                return Response()
            yield NS(datasets=NS(dataset_api_client=NS(download_dataset=download)))

    publisher = KaggleDatasetPublisher(Api(), provider_ref="owner/rkb-pg", owner="owner")
    file = DatasetFile("journal-control.json",len(body),hashlib.sha256(body).hexdigest())
    tracemalloc.start()
    readback = publisher.read_control(file,version=7)
    _, read_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert read_peak < len(readback) * 1.8
    assert type(readback) is bytes and readback == body
    assert len(readback) == file.size and hashlib.sha256(readback).hexdigest() == file.sha256
    assert closed == [True]

    with pytest.raises(DatasetPublicationError,match="dataset_control_read_bytes_changed"):
        publisher.read_control(DatasetFile(file.name,file.size,"0"*64),version=7)
    with pytest.raises(DatasetPublicationError,match="dataset_control_read_bytes_changed"):
        publisher.read_control(DatasetFile(file.name,file.size-1,file.sha256),version=7)
    assert closed == [True,True,True]
