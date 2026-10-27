# Source-unit PDF renderer seam

`source_unit_pdf.render_source_unit_pdf` is a pure source helper intended for the
existing Kaggle process, outside the ordinary search path. It adds no MCP tool,
transport, queue, corpus store, service or inference. It is **not a deployed PDF
export feature**. Citation resolution, accepted-source retrieval, authorization,
asynchronous operation recovery and direct Kaggle-to-client delivery are not
wired by this module.

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

The fixed layout is A4 with 56 pt margins, 11.5 pt serif type, 1.3 line spacing and
14 pt first-line body indent. It measures actual shaped blocks at final width,
keeps short blocks together, reserves heading plus following body, and splits
long blocks with at least two visual lines on either side. It does not rely on
MuPDF's ignored CSS pagination hints. The separate first page contains existing
bibliography, exact service caption and original page labels; generated footers
say `Страница PDF` so new pagination is distinct. PDF metadata binds the supplied
source/unit/revision. Missing edition identifiers are never invented.

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
