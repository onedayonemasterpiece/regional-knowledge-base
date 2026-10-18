"""Read-time evidence → illustration linkage without reingestion or guessed subjects."""
import hashlib
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder


@pytest.mark.asyncio
async def test_text_fetch_links_exact_accepted_caption_and_figure_regions(tmp_path):
    b = SQLiteBackend(
        corpus_path=tmp_path / "corpus.sqlite3",
        embedder=LexicalOnlyEmbedder(),
        object_store=UnavailableObjectStore(),
    )
    def uuid():
        return str(uuid4())
    owner, outsider, document = uuid(), uuid(), uuid()
    page, body, caption, figure, other_caption, other_figure = [uuid() for _ in range(6)]
    image, unrelated = uuid(), uuid()
    text_chunk, figure_chunk, unlinked_chunk = uuid(), uuid(), uuid()
    actor = lambda sub: Principal(
        subject=sub, client_id="test", issuer="test", access_token="test",
    )
    def put(table, **fields):
        b.corpus.put(table, [{**defaults(table), **fields}])
    def add_chunk(ident, title, source_text, region_ids, illustration_ids=()):
        sha = hashlib.sha256(source_text.encode()).hexdigest()
        put("rkb_chunks", id=ident, document_id=document, revision=1,
            title=title, page_ids=[page], region_ids=region_ids,
            footnote_region_ids=[], illustration_ids=list(illustration_ids),
            source_text=source_text, text_sha256=sha,
            search_material=source_text, search_material_sha256=sha)

    try:
        put("rkb_users", id=owner)
        put("rkb_documents", id=document, owner_user_id=owner,
            source_sha256="a"*64, title="Königsberg source",
            active_revision=1, page_count=1, source_visibility="private")
        put("rkb_pages", id=page, document_id=document, revision=1,
            physical_page_index=0)
        for region_id, kind in ((body, "body"), (caption, "caption"),
                                (figure, "figure"), (other_caption, "caption"),
                                (other_figure, "figure")):
            put("rkb_regions", id=region_id, page_id=page, kind=kind,
                reading_order=0, bbox={"left":0,"top":0,"right":1000,"bottom":1000})
        put("rkb_illustrations", id=image, document_id=document, page_id=page,
            source_region_id=figure, caption_region_ids=[caption],
            kind="drawing", visibility="private", source_crop_sha256="b"*64)
        put("rkb_illustrations", id=unrelated, document_id=document, page_id=page,
            source_region_id=other_figure, caption_region_ids=[other_caption],
            kind="drawing", visibility="private", source_crop_sha256="c"*64)
        add_chunk(text_chunk, "Text about Green Gate", "Gate and its caption.",
                  [body, caption])
        add_chunk(figure_chunk, "Caption figure", "Lithograph",
                  [caption], [image])
        add_chunk(unlinked_chunk, "Unrelated bridge text", "Unrelated bridge.",
                  [body])

        implicit = await b.fetch(text_chunk, actor(owner))
        assert [x["illustration_id"] for x in implicit.metadata["illustrations"]] == [image]
        assert implicit.metadata["illustrations"][0]["association_basis"] == "exact_shared_region"
        assert implicit.metadata["illustrations"][0]["caption_region_ids"] == [caption]
        # Explicit staging wins and is never duplicated by the same caption.
        explicit = await b.fetch(figure_chunk, actor(owner))
        assert [x["illustration_id"] for x in explicit.metadata["illustrations"]] == [image]
        # A different subject on the same page cannot steal another caption.
        unrelated_text = await b.fetch(unlinked_chunk, actor(owner))
        assert unrelated_text.metadata["illustrations"] == []
        # Cross-actor access still fails before any media metadata is queried.
        with pytest.raises(LookupError, match="evidence_not_found"):
            await b.fetch(text_chunk, actor(outsider))
        # The fix is an authorized projection; accepted chunks are immutable.
        assert b.corpus.one("rkb_chunks", text_chunk)["illustration_ids"] == []
    finally:
        await b.aclose()
