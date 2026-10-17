"""Shared retrieval coverage policy; source preservation is independent."""

# Running headers/page numbers remain in the source graph, not mandatory
# retrieval passages. Semantic headings, captions and notes remain required.
RETRIEVAL_TEXT_KINDS = frozenset({
    "heading", "body", "caption", "footnote", "table", "marginalia",
})
