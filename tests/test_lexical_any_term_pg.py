"""PG17: natural-language recall while preserving exclusions and simple AND."""
import os
from pathlib import Path
from uuid import UUID

import pytest
from test_kaggle_pg_driver import database

ROOT = Path(__file__).resolve().parents[1]
SOURCE = UUID("00000000-0000-0000-0000-000000000001")
FACT = UUID("10000000-0000-0000-0000-000000000099")
GERMAN = UUID("10000000-0000-0000-0000-000000000098")
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
        # The PG fixture already contains one long, unrelated simple passage.
        await database.execute("""
            INSERT INTO rkb_v4.passage_search
                (passage_id, source_id, revision, ordinal, locator, body,
                 language, config, embedding)
            VALUES (%s, %s, 1, 1, 'page 141', %s,
                    'ru', 'pg_catalog.russian', %s::public.vector)
        """, (FACT, SOURCE, "1544 17 августа основание университета (Альбертины)",
              vector))
        await database.execute("""INSERT INTO rkb_v4.passage_search
            (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            VALUES (%s,%s,1,2,'page 142','Universität Königsberg','de',
                    'pg_catalog.german',%s::public.vector)""", (GERMAN, SOURCE, vector))
        unscoped = "SELECT passage_id FROM rkb_v4.lexical_candidates(%s, 100)"
        scoped = "SELECT passage_id FROM rkb_v4.lexical_candidates_scoped(%s, 100, %s)"
        # The immutable original migrations require every word and return nothing.
        assert await (await database.execute(unscoped, (QUESTION,))).fetchall() == []
        assert await (await database.execute(scoped, (QUESTION, [SOURCE]))).fetchall() == []
        await database.execute((ROOT / "sql/kaggle/039_lexical_any_term.sql").read_text(),
                               prepare=False)
        rows = await (await database.execute(unscoped, (QUESTION,))).fetchall()
        assert [row[0] for row in rows] == [FACT]
        rows = await (await database.execute(scoped, (QUESTION, [SOURCE]))).fetchall()
        assert [row[0] for row in rows] == [FACT]
        for query, expected in (("университет -основание", []),
                                ("semantic absent", []),
                                ("Universität unbekannt", [GERMAN])):
            for sql, args in ((unscoped, (query,)), (scoped, (query, [SOURCE]))):
                rows = await (await database.execute(sql, args)).fetchall()
                assert [r[0] for r in rows] == expected
