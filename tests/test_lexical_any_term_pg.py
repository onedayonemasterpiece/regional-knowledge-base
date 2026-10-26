"""PG17: a natural question retrieves a short fact, not just topical paragraphs."""
import os
from pathlib import Path
from uuid import UUID

import pytest
from test_kaggle_pg_driver import database

ROOT = Path(__file__).resolve().parents[1]
SOURCE = UUID("00000000-0000-0000-0000-000000000001")
FACT = UUID("10000000-0000-0000-0000-000000000099")
QUESTION = "Кто основал Кёнигсбергский университет и в каком году его открыли?"


@pytest.mark.skipif(not (os.environ.get("RKB_KAGGLE_TEST_DSN")
                        or os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET")),
                    reason="disposable PG17 fixture required")
@pytest.mark.asyncio
async def test_natural_question_recalls_short_chronology(database):
    vector = "[1," + ",".join(["0"] * 1023) + "]"
    async with database.transaction():
        await database.execute((ROOT / "sql/kaggle/003_search_scope.sql").read_text(),
                               prepare=False)
        # The PG fixture already contains one long, unrelated passage.
        await database.execute("""
            INSERT INTO rkb_v4.passage_search
                (passage_id, source_id, revision, ordinal, locator, body,
                 language, config, embedding)
            VALUES (%s, %s, 1, 1, 'page 141', %s,
                    'ru', 'pg_catalog.russian', %s::public.vector)
        """, (FACT, SOURCE, "1544 17 августа основание университета (Альбертины)",
              vector))
        rows = await (await database.execute(
            "SELECT passage_id FROM rkb_v4.lexical_candidates(%s, 100)",
            (QUESTION,))).fetchall()
        assert [row[0] for row in rows] == [FACT]
        rows = await (await database.execute(
            "SELECT passage_id FROM rkb_v4.lexical_candidates_scoped(%s, 100, %s)",
            (QUESTION, [SOURCE]))).fetchall()
        assert [row[0] for row in rows] == [FACT]
