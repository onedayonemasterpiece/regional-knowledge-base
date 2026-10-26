"""PG17 rollback: AND restores narrow short-fact matching after SQL039."""
import os
import pytest

from test_kaggle_pg_driver import database, ROOT
from test_lexical_any_term_pg import FACT, QUESTION, SOURCE


@pytest.mark.skipif(not (os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET")
                         or os.environ.get("RKB_KAGGLE_TEST_DSN")),
                    reason="disposable PG17 fixture required")
@pytest.mark.asyncio
async def test_rollback_040_restores_original_lexical_and(database):
    vector = "[1," + ",".join(["0"] * 1023) + "]"
    async with database.transaction():
        await database.execute((ROOT / "sql/kaggle/003_search_scope.sql").read_text(),
                               prepare=False)
        await database.execute("""
            INSERT INTO rkb_v4.passage_search
            (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
            VALUES (%s,%s,1,1,'page 141',%s,'ru','pg_catalog.russian',%s::public.vector)
        """, (FACT, SOURCE, "1544 17 августа основание университета (Альбертины)", vector))
        await database.execute((ROOT / "sql/kaggle/039_lexical_any_term.sql").read_text(),
                               prepare=False)
        queries = (("SELECT passage_id FROM rkb_v4.lexical_candidates(%s,100)", ()),
                   ("SELECT passage_id FROM rkb_v4.lexical_candidates_scoped(%s,100,%s)",
                    ([SOURCE],)))
        for sql, suffix in queries:
            rows = await (await database.execute(sql, (QUESTION, *suffix))).fetchall()
            assert [row[0] for row in rows] == [FACT]
        await database.execute((ROOT / "sql/kaggle/040_lexical_restore_and.sql").read_text(),
                               prepare=False)
        for sql, suffix in queries:
            rows = await (await database.execute(sql, (QUESTION, *suffix))).fetchall()
            assert rows == []
            rows = await (await database.execute(sql, ("основание университета", *suffix))).fetchall()
            assert [row[0] for row in rows] == [FACT]
