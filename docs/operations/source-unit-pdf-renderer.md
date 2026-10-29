# Source-unit PDF renderer seam

`source_unit_pdf.render_source_unit_pdf` is the existing pure A4 helper.
Merged PRs #607/#624 provide the current accepted-unit `source_unit_export`
**MCP read tool**. The first Albinus/Hoffmann article PDF opened in ChatGPT.
This v2 formatting change is source-only until reviewed, merged and installed
by Shkiper; there is no automatic Slack upload or extra client-specific path.

**MCP transport (2025-11-25):** on a warm read, `source_unit_export(source_id,
article_id, annotation_ru=None, translation_ru=None)` returns one `CallToolResult` with an `EmbeddedResource`:
`type=resource`, `resource.blob` is base64 of the actual PDF,
`resource.mimeType=application/pdf` and the descriptive URI identifies the
source and unit. The text block identifies the article; it is not the file.
The model sees real bytes only if its MCP client supports binary embedded
resources. The MCP protocol permits the content type but cannot guarantee
client UI/download integration. No ResourceLink-only result is passed off as a
working attachment. Spec: https://modelcontextprotocol.io/specification/2025-11-25/server/tools

**Bytes and authorization:** Kaggle's existing PG read gets the *complete*
`source_unit_boundaries` projection and only accepted original regions from
SQL046 (not search chunks or the 16-span public preview), then renders the PDF.
Export selects the exact ordered unique pages from the complete unit's source
spans and end anchor. It preserves accepted model reading order, including
nonmonotone physical page indexes, in both author text and bibliography.
The existing ephemeral `SOURCE_READ_RESULT` sends its base64 PDF through the
authenticated Kaggle→Dev WSS. Dev only holds bytes transiently in the active
read ticket and MCP HTTP JSON response; it does not write PDF bytes to a file,
SQLite, the control spool, Dataset, S3, or Slack. The original read authority,
source gate and final MCP issuance recheck still apply. This **transit without
storage** is the owner-approved refinement of #504 §4: Kaggle does the work;
Dev handles authenticated, short-lived MCP transport only.

**Current bounded vertical slice:** the existing SOURCE_READ wire envelope
remains 256 KiB and the shared read deadline 2 s. The complete PDF is returned
only when it fits those limits. Optional caller-supplied annotation/translation
increase both request and PDF size: the existing 16 KiB request envelope and
256 KiB result envelope remain unchanged. No content is shortened to fit.
Oversize PDF returns `pdf_transport_limit`, while oversized requests follow the
existing query-size rejection; a not-yet-complete unit returns `unit_not_ready` with the instruction to finish
`book_ingest`. Neither case returns partial PDF. Longer/heavier units require a
separately reviewed larger-file transfer, not an untested limit increase or
a second ad hoc store. No live throughput/SLA or universal client support is
claimed from synthetic CI. Shkiper's one live acceptance test after merge:
request the *same* finished Albinus unit (physical pages 61–64) via
`source_unit_export` from ChatGPT and Grok, confirm each receives actual
PDF bytes (`%PDF-`, intact title/author/RU+DE text and final page) and can
offer/open the artifact, not merely quote a URI. If either client does not
surface embedded PDF, record that exact support gap; do not silently fall back
to a Slack-only path.

The caller must supply the full completed internal source-unit projection, its
accepted revision, exact ordered `source_spans`, existing `Region` objects with
original `source_text`, verified `StartMetadataInput` / `CatalogMetadata` edition
fields and original page labels. Never use BGE/search passages, normalized text,
a public 16-span preview or model-generated replacement text. The helper checks
completeness, exact span bounds, region identity/kind, order, overlap and review
flags. Supplied interior spans must cover their full region; only the first
span may start inside its region and only the final span may end inside it.
The final span must reach the exact end anchor. A half-open end at the
next region's offset zero is supported when the preceding span reaches its region
end and the caller includes that accepted endpoint `Region` (it is not rendered).
This checks endpoint identity/page/order; a supplied subset cannot prove that an
entire middle region was omitted. The caller must establish the full accepted
projection, including all intervening regions, before calling the helper.
These checks do not prove current actor/source/revision eligibility. The
existing source/actor gate must run before retrieval and again before delivery;
withdrawn sources and stale revisions must remain fenced by that caller.

One supplied source span is one explicit author block. Heading/body regions are
supported, including literal internal newlines and long block flow. No paragraph,
list hierarchy, table cells or footnote callouts are inferred from plain text.
Footnote, table, marginalia, caption, figure and other unsupported significant
region kinds fail explicitly with `unsupported_structure:<kind>`. They are never
omitted or converted into invented endnotes. Richer structures require their
accepted relationships in a later integration; this helper does not establish a
second source model.

The fixed layout is A4 with 56 pt margins, 11.5 pt serif body type, larger
bold article and source heading styles, 1.3 line spacing, 14 pt first-line
body/translation indent, and source-title-word emphasis when a dictionary
article's first body word matches its exact accepted title. It measures actual shaped blocks at final width,
keeps short blocks together, reserves heading plus following body, and splits
long blocks with at least two visual lines on either side. It does not rely on
MuPDF's ignored CSS pagination hints. The separate first page leads with a large article title, then the verified
bibliography, original page labels and exact service caption. An optional
1–2-paragraph `annotation_ru` appears there with the label
«Краткое изложение, подготовлено сервисом». The following pages begin with
the article title and the *verbatim* accepted author text, followed (only if
supplied) by `translation_ru` in a separately labeled section
«Автоматический перевод; авторитетен оригинал выше».
The caller model supplies both strings; Kaggle only lays out, never translates
or summarizes. No supplied text means no empty sections. Every page has a
title · authors · book running header and a bare PDF page number below, without
the word «PDF». PDF metadata binds the supplied source/unit/revision.
Missing edition identifiers are never invented; ISBN, ISSN, УДК, ББК and DOI
are shown only when supplied by the verified catalog.

Author text is HTML-escaped and cannot load arbitrary resources. Inert span
boundaries preserve literal ASCII f/fi/ff/ffi sequences because MuPDF's default
ligature shaping otherwise changes selectable Unicode; original Unicode ligature
characters remain unchanged. Actual PDF text is checked in order, ignoring only
ASCII layout whitespace, and fonts must be embedded with ToUnicode mappings.
Replacement/missing glyphs, overwide text and unsupported layout fail explicitly.
This is a reflowed text edition, not a facsimile or illustration export.

Bounds are 250,000 input characters, 2,000 regions/spans, 128 PDF pages, 20 MiB and
a cooperative 20-second render budget. Checks occur between bounded native layout
operations and page validation; this is not a preemptive process deadline. Failure
returns no PDF bytes. The existing controller must schedule any eventual export
outside search permits and apply its process/resource deadlines at integration.

Synthetic real-PDF tests cover RU/DE/old letters and punctuation, literal markup,
ligatures, embedded fonts, exact extraction, A4 geometry, long paragraphs,
short-block/heading pagination, 30+ pages, sparse metadata, deterministic bytes,
revision metadata, invalid/read-incomplete inputs and unsupported structures.
These tests do not establish actual source retrieval, live Kaggle performance,
current ACL behavior or usable MCP/Slack PDF delivery.
