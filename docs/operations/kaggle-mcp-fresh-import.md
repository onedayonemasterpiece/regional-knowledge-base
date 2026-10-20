# Bounded fresh-source MCP import

This optional composition connects the existing `book_ingest` and `book_pages`
contracts to the same owner OAuth/participants, control spool, journal, document
admission and accepted source dispatcher. It does not select the production
backend, enable a parser on an existing notebook, or establish public-profile
parity. The existing legacy profile remains the default.

The initial scope is one new PDF, at most 2 MiB and 32 physical pages, with up to
32 source passages and 12 model-authored narrative candidates. Each stage batch
uses the existing maximum of eight pages. Each preview call returns one JPEG
plus explicit native text/block continuation. Larger/multipart sources, HTML,
DjVu, reprocess/rechunk, catalog corrections and entity/POI mutations return an
explicit unsupported result. A deployed target needs its independently pinned
PDF parser wheel closure and `TargetLaunchSpec.original_intake` opt-in first.

## Existing owners and private configuration

`compose_source_service(..., source_import=SourceImportOwners(...))` takes typed
optional owners. The existing private `rkb.source-service.v1` file may contain
one `source_import` object matching `SourceImportOwners.wire()`. Its exact keys
are `codec=rkb.source-import-owners.v1`, `launch_config_sha256`, `parser_sha256`,
`allowed_origins`, `tokenizer_directory`, and `tokenizer_process`. The remaining
source-service fields are unchanged. No credentials are copied into this object.

The launch-config digest must be the independently reviewed config containing
native intake. Every import and final output checks the accepted runtime against
that exact digest. The parser digest is the SHA-256 of canonical JSON
`{packages: <exact parser package/version map>, code: <parser module SHA-256>}`,
matching `NativeOriginalIntake`. Native receipts must carry this independently
supplied digest. The same queue, spool, participant registry and existing S3
client/bucket are required; construction performs no provider or model action.

`tokenizer_process_manifest()` publishes the four original config/tokenizer file
sizes and hashes, protected HF package versions, interpreter version, helper
source digest, raw tokenizer digest, model/space constants and the independently
verified materialized fingerprint. The operator prepares a stable versioned
runtime and directory containing those four files; disposable artifacts/venvs
are not an operational configuration. Instantiate:

```python
process = TokenizerProcessSpec(
    python=stable_venv_python,
    python_sha256=independently_verified_interpreter_sha256,
    helper_sha256=tokenizer_process_manifest()['helper_sha256'],
    python_version='3.13.15',
    deadline_seconds=45,
)
counter = HfImportTokenCounter(stable_four_file_directory, process=process)
```

The isolated `-I` subprocess accepts only a bounded batch (44 texts / 128 KiB),
checks the interpreter/helper/file/version/fingerprint pins, disables model
backends and network connections, and returns only exact input SHA/counts. Its
stdout is bounded to 16 KiB; cancellation/deadline kills and reaps the child.
Protected HF materialization is required: raw Tokenizers differs for real
special-token and whitespace cases. Neither search nor fetch invokes this
counter. No BGE weights or model inference run on Dev.

## Model workflow and durable boundaries

1. Call the ordinary `book_ingest(start)` with the attached PDF, normal metadata
   and `source_import.public_rights`. The existing public rights policy must
   allow the explicitly supplied status/evidence/policy version. The server
   assigns stable source/ingestion identities and persists bounded control state.
2. Native intake downloads the original directly on Kaggle, archives it through
   the existing storage authority, and independently rereads/hashes the stored
   exact version. Dev never downloads original bytes. Pending/unknown outcomes
   remain pending; a refreshed attachment retry uses the same `file_id`, metadata
   and rights. Fixed native rejections produce actionable terminal output.
3. Read actual pages and all continuation cursors. Stage the model's source
   graph and source-backed spans through the normal inline fields or the existing
   bounded stage-JSON attachment. A region must select an unambiguous exact span
   from an observed complete native block with its actual bbox. Original receipt,
   parser and full native-page proof are checked again before finalization.
   Unseen/truncated text is never silently accepted. Unsupported visual-only/OCR
   transcription requires another explicit provenance contract.
4. Supply explicit `source_import.research` state/continuation. Pending work must
   point to this owned ingestion's reachable `book_pages` tool. Complete review
   requires the model's same-pass page-review attestations. Submit findings in
   `story_candidates`; `basic_variant` supplies a real authored body and complete
   evidence map. No body is synthesized from a summary. `facts_only` and
   `needs_context` remain distinct explicit model outcomes.
5. Validate full source coverage and exact complete 512-token input bounds. Each
   source passage and supplied narrative uses an immutable existing DOCUMENT job
   bound to original receipt, parser, stage hash, text hash and pinned BGE space.
   Vectors are produced by the resident Kaggle encoder. No query-vector roundtrip
   or Dev inference occurs.
6. Finalize freezes one `source.import.v2` command under the existing spool's
   ordering lock. The accepted journal segment precedes native activation.
   Source, immutable findings and supplied narrative/default variant activate
   atomically through the fixed v2 handler. A local apply receipt is still
   processing until its exact outcome is remotely durable. Retry retains the
   original command/job identities. V1 remains immutable and independently
   replayable.

Final output preserves the model-authored pending continuation and distinguishes
source activation from semantic/retrieval acceptance. The normal return models,
annotations and `openai/fileParams=[file]` remain. Owner OAuth, current import
permission, writer/runtime and launch pins are checked at the existing final
HTTP JSON boundary, including complete image output. Local SQLite stores only
bounded control records, not corpus rows. The first control table is capped at
32 intakes / 8 MiB; exhaustion is explicit, without deleting unknown state.

Local tests use synthetic originals/native apply/storage and real owner OAuth,
SQLite queue/admission, MCP schema/call, exact accepted journal bytes and remote
outcome publication. An opt-in regression uses the real independently pinned
17 MiB tokenizer in a tokenizer-only runtime, including 512/513 boundaries and
tampered-input rejection. These checks do not claim a deployed importer, real
provider/source effects, protected capacity, or full product parity.
