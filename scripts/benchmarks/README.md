# DevCoveer small embedding benchmark

This is an isolated CPU experiment. Production dependencies, search configuration,
DB schema and vectors are unchanged. Only anonymous public static model files are
fetched; inference is local and offline. Private corpus text never belongs in Git.

Run from the repository root on a host with user systemd/cgroup v2. Create a
managed **retained** directory using `dev-artifacts new regional-knowledge-base
small-embeddings --retain --reason 'Benchmark pending review'`, then set `lab`
to the printed absolute path. Check free space before downloading approximately
844 MiB of model assets plus the isolated runtime. Keep one cache at `$lab/models`.

```bash
python3 -m venv "$lab/venv"
"$lab/venv/bin/pip" install --no-cache-dir -r scripts/benchmarks/small_embedding_requirements.txt
"$lab/venv/bin/python" scripts/benchmarks/download_small_models.py "$lab"
.venv/bin/python scripts/benchmarks/export_small_embedding_corpus.py "$lab/corpus.jsonl"
.venv/bin/python scripts/benchmarks/small_embedding_lexical.py "$lab"
bash scripts/benchmarks/run_small_embedding.sh "$lab" e5
# Wait for this service to finish before starting the next candidate.
bash scripts/benchmarks/run_small_embedding.sh "$lab" gemma
bash scripts/benchmarks/run_small_embedding.sh "$lab" potion
# If default POTION loading OOMs, preserve its result and journal first.
bash scripts/benchmarks/run_small_embedding.sh "$lab" potion --potion-mmap
```

The returned unit name identifies the log at `$lab/<unit>.log`. Check
`systemctl --user show <unit> -p ActiveState -p Result` and preserve
`journalctl --user -u <unit> --no-pager`. A killed worker may leave status
`running` in its last checkpoint; **systemd's terminal result controls**.
Default POTION's failed checkpoint is `potion-smoke-result.json` in the original
run; later mmap runs write `potion-result.json`. Do not overwrite failed-run
evidence on reruns.

`--live-only` reuses existing local document vectors and repeats startup,
quality and query measurements, including actual read-only SQL lexical round
trips. It writes `<candidate>-live-result.json`. `--smoke` loads a model and
checks ten multilingual embeddings, writing `<candidate>-smoke-result.json`.
It does not measure corpus throughput or retrieval quality. The optional
sequence script runs later candidates serially after a named existing unit
finishes; it preserves journal and status files for each run.

Every measured service has `MemoryMax=1G`, `MemorySwapMax=0`, `CPUQuota=100%`
and affinity to one permitted CPU. ONNX intra/inter-op threads are 1 with
spinning disabled; BLAS threads are 1; tokenizer parallelism, Model2Vec
multiprocessing, GPU visibility and HF online access are disabled. Each process
records its actual `memory.max`, `memory.swap.max`, `cpu.max`, `memory.peak`,
process-tree RSS/PSS samples and process high-water RSS. Sampled per-phase peaks
can miss short spikes; cgroup peak and `ru_maxrss` provide lifetime maxima.

Questions and provenance are frozen in `small_embedding_questions.json`.
The corpus hash and all evidence IDs must match before evaluation. Do not tune
questions, fusion weights, thresholds or labels to a candidate's results.
The fixture contains source paraphrases and hashes, never long source extracts.

Lexical-only uses the actual `simple` / `websearch_to_tsquery` / `ts_rank_cd`
production SQL, with active revision and document filter, verified against the
existing RPC. Full natural questions generally require every query token to
occur. Four keyword controls are reported separately. This is the **current**
baseline; a hypothetical OR/BM25/stemmed baseline is not substituted.

Vectors are normalized within their own space and searched with brute-force
cosine. RRF uses equal weights, `k=60`, top-100 from each branch and chunk-ID
ascending ties, matching the production rank-fusion formula. We never add raw
model similarities. Local retrieval latency uses the stored lexical rankings;
full retrieval latency includes a real SQL round trip on a persistent read-only
connection plus query encoding, cosine ranking and fusion. It excludes OAuth,
MCP HTTP transport and evidence text fetch.

Recall@k is macro recall across **required evidence groups**: for each question,
count the fraction of groups with any acceptable chunk in the first k results,
then average over answerable questions. Hit@k counts questions with any evidence.
MRR uses the first relevant position across the complete available ranking.
All-evidence@10 is the fraction of multi-evidence questions with every required
group found. Negative controls are excluded from recall/MRR and report returned
results and top similarities separately. No threshold is calibrated on the two
negative controls; returned passages do not establish answerability.

This is one book and author-curated, sparse qrels, not a representative independent
multilingual retrieval evaluation. Keyword controls request a known topical chunk,
so their exact-chunk misses must not be confused with complete topical failure.
Compatibility strings cover additional languages but measure runtime consistency,
not retrieval quality in those languages. Latency is measured on a shared host;
no OS page cache eviction or exclusive CPU reservation is claimed.

After all runs succeed, use the isolated venv to run
`python scripts/benchmarks/summarize_small_embeddings.py "$lab"`. This independently
recomputes all rankings and metric slices from the saved matrices, checks MRR
against worker measurements, and writes the consolidated JSON under `$lab`.
The final report and its small public summary live in `docs/reports/`.

The winning encoder's `small_embedding_compatibility.json` contains ten fixed
multilingual strings, query/document vectors, token IDs where applicable, pinned
file hashes and comparison tolerances. Reference vectors are L2-normalized FP32;
byte hashes are little-endian FP32. Compare a client implementation against both
query and document roles using the same pinned export and preprocessing. The
hash detects exact agreement; tolerance comparison permits small CPU/backend
floating-point differences. Test the local fixture with the ordinary project
suite; no ML dependency or download is required by CI.

## Retrieval follow-up (2026-10-04, continuing PR #26)

The original benchmark is historical evidence on an **incomplete source
projection**. Read `docs/reports/gause-import-coverage-audit-20261004.md` and
`docs/reports/embedding-retrieval-validation-followup-20261004.md` before treating
its E5 recommendation as quality acceptance. Original fixtures/results stay
unchanged; `retrieval_validation_questions.v1.json`,
`retrieval_validation_original_qrels.v2.json` and
`retrieval_validation_judgments.v1.json` are separate artifacts. The latter is
explicitly partial, model-assisted and coverage-aware.

Reuse the original retained `lab` path and venv/cache. A separate retained
`output` directory stores private follow-up evidence, with `questions-freeze.json`
and the new-query read-only `lexical.json`. Run `retrieval_validation_worker.py
lab output e5` and then `gemma` under the same systemd `MemoryMax=1G`,
`MemorySwapMax=0`, `CPUQuota=100%`, `taskset` controls as the original launcher;
its own guard rejects missing hard memory/CPU limits. It encodes only new queries
and necessary batch1 documents, never overwriting historical matrices/results.
`small_embedding_lexical.py output scripts/benchmarks/retrieval_validation_questions.v1.json`
uses the unchanged production baseline and verifies NULL-vector RPC equivalence.
Then run `retrieval_validation_pool.py lab output` and
`summarize_retrieval_validation.py lab output`. Pool passages, ranking matrices
and full coverage output remain private; only aggregate statistics/locators go
into Git. Full pool absence of a judgment means unknown, never grade zero.
The source audit command and read-only object provenance are retained with the
private evidence; do not fetch/reimport source implicitly to reproduce metrics.
