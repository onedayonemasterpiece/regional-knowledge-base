# Correcting inactive native source input

After a rejected seal, the calling model can append a corrective stage batch using the same chunk key or an explicit replacement entity with the same identity and kind. Accepted batch bytes and their hash chain remain immutable. The compiler selects the latest explicitly replaced chunk before compiling source selections, then validates graph evidence against the final corrected source before tokenization.

Whole-book candidate preparation and atomic `source.activate.v1` projection use a bounded 60-second SQL statement budget. Small input/vector queries retain their 5-second budget; other accepted commands retain 2 seconds and all writer lock waits retain 1 second. SQL025 reuses passage and region identity maps during validation; duplicate or missing identities, changed source text and invalid spans still reject. Candidate logs contain sequence, elapsed time, counts and SQLSTATE, never source text.

An unrelated chunk key cannot suppress an invalid selection. Missing evidence, changed entity identity, unknown model fields, incomplete coverage and oversized complete encoder inputs still reject. The original rejected seal receipt remains in history; the corrected head needs its own successful seal. This does not relax native SQL activation checks or Dataset durability.

Focused verification: `tests/test_native_source_materializer.py` covers invalid footnote selection followed by an explicit correction, a boundary-whitespace graph quote correction, unresolved evidence, unrelated keys and identity changes. Native deterministic materialization rejections log only route, sequence and a bounded validation code.

When native validation finds textual regions absent from chunk references, the
existing rejection reason includes `missing_chunk_coverage:physical_page_index/region_key=`
and up to the validator's existing 40 locations, followed by `fix=add_chunk_region_refs`.
For example, `123/r123_05;123/r123_18` identifies the original region keys on physical
page index 123. Add the omitted references to appropriate chunks and correct the same
packet through the existing flow. The canonical receipt and `book_ingest.status`
preserve this bounded diagnostic without source text. Other validation errors retain
the existing generic reason; accepted earlier pages and retry cursors are unchanged.
