"""PG17 ranking regression: navigation-only entries remain lexical, not dense."""
import os
from pathlib import Path
from uuid import UUID

import pytest
from test_kaggle_pg_driver import database

ROOT = Path(__file__).resolve().parents[1]
SOURCE = UUID("00000000-0000-0000-0000-000000000046")


@pytest.mark.skipif(
    not (os.environ.get("RKB_KAGGLE_TEST_DSN") or os.environ.get("RKB_PG_DRIVER_FIXTURE_SOCKET")),
    reason="disposable PG17 fixture required",
)
@pytest.mark.asyncio
async def test_pure_crossreferences_do_not_displace_article_in_dense_top3(database):
    query_vector = "[1," + ",".join(["0"] * 1023) + "]"
    redirects = [
        ("20000000-0000-0000-0000-000000000001", "Obenwinkel, s. Nordwinkel."),
        ("20000000-0000-0000-0000-000000000002", "Gr. Uferwinkel s. Uferwinkel."),
        ("20000000-0000-0000-0000-000000000003", "Bildergalerie s. Bildersaal."),
    ]
    article = UUID("20000000-0000-0000-0000-000000000004")
    definition = UUID("20000000-0000-0000-0000-000000000005")
    context = UUID("20000000-0000-0000-0000-000000000006")
    entries = [
        *( (UUID(key), body, query_vector) for key, body in redirects),
        (article, "Uferwinkel. Im Jahr 1820 wurde die Dorfkirche erbaut.", "[0.8,0.6," + ",".join(["0"] * 1022) + "]"),
        (definition, "Uferwinkel ist ein Dorf.", "[0.7,0.71414284," + ",".join(["0"] * 1022) + "]"),
        (context, "Uferwinkel, s. Nordwinkel. Die Geschichte ist hier erläutert.",
         "[0.6,0.8," + ",".join(["0"] * 1022) + "]"),
    ]
    async with database.transaction():
        await database.execute((ROOT / "sql/kaggle/003_search_scope.sql").read_text(), prepare=False)
        await database.execute("INSERT INTO rkb_v4.sources VALUES (%s,'synthetic atlas',true,false)", (SOURCE,))
        await database.execute("INSERT INTO rkb_v4.revisions VALUES (%s,1)", (SOURCE,))
        await database.execute("INSERT INTO rkb_v4.active_revision VALUES (%s,1)", (SOURCE,))
        for ordinal, (key, body, vector) in enumerate(entries):
            await database.execute(
                """INSERT INTO rkb_v4.passage_search
                   (passage_id,source_id,revision,ordinal,locator,body,language,config,embedding)
                   VALUES (%s,%s,1,%s,'page 1',%s,'de','pg_catalog.german',%s::public.vector)""",
                (key, SOURCE, ordinal, body, vector))
        scoped = ("SELECT passage_id FROM rkb_v4.dense_candidates_scoped("
                  "%s::public.vector,3,%s::uuid[])")
        args = (query_vector, [SOURCE])
        baseline = await (await database.execute(scoped, args)).fetchall()
        assert {row[0] for row in baseline} == {UUID(key) for key, _ in redirects}

        await database.execute(
            (ROOT / "sql/kaggle/046_dense_redirects_lexical_only.sql").read_text(),
            prepare=False)
        after = await (await database.execute(scoped, args)).fetchall()
        assert [row[0] for row in after] == [article, definition, context]

        all_dense = await (await database.execute(
            "SELECT passage_id FROM rkb_v4.dense_candidates(%s::public.vector,100)",
            (query_vector,))).fetchall()
        assert not {row[0] for row in all_dense} & {UUID(key) for key, _ in redirects}
        lexical = await (await database.execute(
            "SELECT passage_id FROM rkb_v4.lexical_candidates_scoped(%s,100,%s::uuid[])",
            ("Obenwinkel", [SOURCE]))).fetchall()
        assert UUID(redirects[0][0]) in {row[0] for row in lexical}
