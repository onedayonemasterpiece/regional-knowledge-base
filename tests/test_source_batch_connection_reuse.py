"""Fictional prepared vectors preserve ordered transactions without reconnecting per input."""
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

import regional_knowledge.kaggle_source_batch as batch
import regional_knowledge.kaggle_source_materializer as materializer


class Cursor:
    def __init__(self, row): self.row = row
    async def fetchone(self): return self.row


class PreparedPacket:
    def __init__(self):
        self.acquired = self.closed = self.transaction_depth = 0
        self.vectors = {}
        self.installed = []
        self.encoded = []
        self.stop_on = None
        self.current = True
        self.knowledge = dict(passages=[dict(passage_id=str(i), body=f'Fictional passage {i}') for i in range(3)],
                              semantic=dict(stories=[]))

    @asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        try: yield self
        finally: self.closed += 1

    @asynccontextmanager
    async def transaction(self):
        self.transaction_depth += 1
        try: yield
        finally: self.transaction_depth -= 1

    async def execute(self, sql, args=None):
        assert self.transaction_depth == 1
        if 'applied_ops' in sql: return Cursor(None)
        if 'SELECT knowledge' in sql: return Cursor((self.knowledge,))
        if 'SELECT embedding' in sql: return Cursor(self.vectors.get((args[1], args[2])))
        if 'install_source_batch_vector' in sql:
            _, _, _, ident, sha, *_ = args
            self.installed.append(ident)
            self.vectors[ident, sha] = ([1.] + [0.] * 1023,)
            return Cursor((sha,))
        assert sql == "SET LOCAL statement_timeout='5s'"
        return Cursor(None)

    def check_current(self):
        if not self.current: raise PermissionError('fictional writer superseded')

    async def encode(self, text):
        assert self.transaction_depth == 0, 'inference must never hold a PG transaction'
        self.encoded.append(text)
        if text == self.stop_on:
            self.current = False
        return [1.] + [0.] * 1023

    async def run(self, monkeypatch):
        monkeypatch.setattr(batch, 'validate_command', lambda command: dict(
            batch_index=4, component='text', materializer_sha256='same'))
        monkeypatch.setattr(materializer, 'materializer_fingerprint', lambda: 'same')
        command = SimpleNamespace(ids=dict(source_id='fictional-source'), operation_key='fictional-op', sequence=1, encode=lambda: b'c')
        await batch.materialize_packet(SimpleNamespace(_current=self.check_current, encode=self.encode),
            command, acquire=self.acquire, database=SimpleNamespace(database_generation='db', writer_generation='writer'))


@pytest.mark.asyncio
async def test_vector_loop_reuses_connection_with_exact_order_and_no_transaction_during_encode(monkeypatch):
    packet = PreparedPacket()
    await packet.run(monkeypatch)
    assert packet.acquired == packet.closed == 2  # prepared lookup, then vector loop
    assert packet.transaction_depth == 0
    assert packet.installed == ['0', '1', '2']
    assert packet.encoded == [f'Fictional passage {i}' for i in range(3)]
    # A modeled 10 ms connection cost becomes 20 ms instead of the old 70 ms.
    # This is a deterministic acquisition budget, not a live latency claim.
    assert packet.acquired * 10 == 20


@pytest.mark.asyncio
async def test_supersession_releases_connection_and_retry_reuses_committed_partial_vector(monkeypatch):
    packet = PreparedPacket()
    packet.stop_on = 'Fictional passage 1'
    with pytest.raises(PermissionError, match='writer superseded'):
        await packet.run(monkeypatch)
    assert packet.acquired == packet.closed == 2
    assert packet.installed == ['0']  # current() fences second install after encode
    assert packet.transaction_depth == 0
    packet.stop_on = None
    packet.current = True
    await packet.run(monkeypatch)
    assert packet.acquired == packet.closed == 4
    assert packet.installed == ['0', '0', '1', '2']
    assert packet.encoded.count('Fictional passage 0') == 1
    assert packet.transaction_depth == 0


@pytest.mark.asyncio
async def test_cancelled_encode_releases_connection_without_installing_vector(monkeypatch):
    import asyncio
    packet = PreparedPacket()
    async def cancel(text):
        assert packet.transaction_depth == 0
        raise asyncio.CancelledError
    packet.encode = cancel
    with pytest.raises(asyncio.CancelledError):
        await packet.run(monkeypatch)
    assert packet.acquired == packet.closed == 2
    assert packet.installed == []
    assert packet.transaction_depth == 0
