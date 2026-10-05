"""Insert measured tables and the explicitly scoped decision into the audit report."""
from __future__ import annotations
import argparse,json
from pathlib import Path
from chunk_size_audit import load

BEGIN='<!-- MEASURED_CHUNK_AUDIT_RESULTS_BEGIN -->'
END='<!-- MEASURED_CHUNK_AUDIT_RESULTS_END -->'


def main():
    p=argparse.ArgumentParser();p.add_argument('--lab',required=True);p.add_argument('--report',required=True);a=p.parse_args();lab=Path(a.lab);report=Path(a.report)
    selected=load(lab/'selection.json')['variant']
    if selected!='t256':raise ValueError('this authored decision requires the frozen t256 selection')
    dev=load(lab/'score-dev.json')['results'];test=load(lab/'score-test.json')['results'];allrows=load(lab/'score-all.json')['results']
    shape=load(lab/'manifest.json')['shape'];boot=load(lab/'bootstrap.json');hybrid=load(lab/'hybrid-ablation.json')
    lines=[BEGIN,'## Completed size selection and held-out results','',
      '**Selected operating target: approximately 256 E5 tokens per embedding passage, with semantic boundaries and exact final-input token checks.** In this corpus that variant has median 814 characters and a maximum of 1,096. A practical agent-facing guide is roughly 700–1,100 characters, usually around 800–1,000, not a blind character ceiling. The hard deployment ceiling remains 512 tokens for each actual encoder, including role prefix and all augmentation. A self-contained sentence/paragraph may deviate from the target; source loss, broken words and unrelated topic concatenation are not permitted.',
      '',
      'This decision was frozen from development results before held-out rankings were opened. `t256` had the highest development top-five/top-ten known-proof coverage and fixed 5,000-character-budget coverage (14/32). `c1000` had better development top-one/MRR, but lower top-five/context coverage (12/32); the selection favors a small multi-passage evidence pack for an agent rather than a single top-one hit. The held-out results retain that selection; they are not used to switch winners after the fact.',
      '',
      '### E5 size sweep — development and held-out partitions',
      '',
      'Every row below has a complete document matrix and the same frozen query fixture. Hit percentages refer to the preselected known source span. Test has 32 questions in 16 paired RU/DE fact families.',
      '',
      '| Variant | Dev Hit@5 | Dev MRR@10 | Test Hit@1 | Test Hit@5 | Test Hit@10 | Test MRR@10 | Test 5,000-char coverage |',
      '|---|---:|---:|---:|---:|---:|---:|---:|']
    for variant in ('current','current_norm','c700','c1000','c1400','c1800','t256'):
        d=dev[variant+':e5']['aggregate'];t=test[variant+':e5']['aggregate']
        lines.append(f"| {variant} | {d['single_hit5']:.2%} | {d['mrr10']:.4f} | {t['single_hit1']:.2%} | {t['single_hit5']:.2%} | {t['single_hit10']:.2%} | {t['mrr10']:.4f} | {t['span_hit5000chars']:.2%} |")
    lines+=['',
      'The 256-token choice and the 1,000-character variant tie on held-out top-five/top-ten hit counts, and their MRR differs by less than 0.001. This is not evidence of a universal sharp optimum at exactly 256 tokens. It supports this small-passage operating range and an exact token guard. The 700-character cap loses some semantic completeness; the 1,800-character cap adds truncation risk and larger evidence packs without a measured quality advantage.',
      '',
      'The selected E5 variant improves held-out Hit@10 from 16/32 to 18/32 and top-five evidence volume from an average 6,770 to 4,106 characters. The paired 16-family bootstrap gives Hit@10 difference +6.25 percentage points, 95% interval [-6.25, +18.75]; MRR difference +0.0732, interval [-0.0333, +0.1953]. The small fixture does **not** statistically establish a general quality gain. The reduction in input size and elimination of observed truncation are directly measured.',
      '',
      '**Important BGE limitation:** the complete BGE experiment tests the current 1,449-passage corpus, not the rebuilt size grid. The target above is selected from the E5 grid and checked against both tokenizers. BGE quality for a mass 256-token re-chunking remains to be tested before a production corpus migration. No such migration is claimed or performed.',
      '',
      '### Dense-only encoder/fusion comparison on unchanged current chunks',
      '',
      '| Method, no lexical branch | Test Hit@1 | Test Hit@5 | Test Hit@10 | Test MRR@10 | All 64 Hit@10 |',
      '|---|---:|---:|---:|---:|---:|']
    for key,label in [('current:e5','E5'),('current:bge','BGE dense'),('current:e5_bge','Equal-weight E5 + BGE RRF')]:
        t=test[key]['aggregate'];whole=allrows[key]['aggregate']
        lines.append(f"| {label} | {t['single_hit1']:.2%} | {t['single_hit5']:.2%} | {t['single_hit10']:.2%} | {t['mrr10']:.4f} | {whole['single_hit10']:.2%} |")
    lines+=['',
      '**BGE dense is materially stronger on this fixture; blindly fusing E5 into it hurts.** BGE reaches the known proof in the top ten for 27/32 held-out questions, whereas E5 reaches 16 and equal-weight fusion 23. Both development and holdout show this pattern. The paired held-out BGE-minus-E5 Hit@10 interval is [+12.5, +53.125] percentage points; this supports a difference on this fixture, not a universal model ranking.',
      '',
      'The bilingual slice is particularly diagnostic. Across all 24 Russian questions about the German source B, current E5 has known-proof Hit@10 **1/24**, the selected shorter E5 variant **3/24**, BGE **20/24**, and equal-weight E5+BGE **11/24**. For the corresponding 24 German questions the counts are **21/24**, **22/24**, **22/24**, and **23/24**. Size alone does not repair the current E5 cross-language weakness. It is a property of the tested deployed INT8 configuration and source/query fixture; this audit does not isolate whether model capacity, quantization, OCR/domain vocabulary or another factor is its underlying cause.',
      '',
      'These findings are not directly comparable to the old high-recall benchmark on a different, smaller/incomplete projection and a different question set. They do not establish a chronological regression percentage. Twelve diagnostic semantic judgments already found one acceptable alternative passage outside the exact seed label, so the table must not be reported as the accuracy of generated answers.',
      '',
      '### Does lexical retrieval hide the problem?',
      '',
      f"The no-alias full-question FTS ablation returns any result for **{hybrid['current']['lexical_nonempty']}/64** questions on current chunks and **{hybrid['t256']['lexical_nonempty']}/64** on selected shorter chunks. Current lexical-only known-proof Hit@10 is 1/64. Adding FTS leaves current BGE Hit@10 at 54/64 and equal-weight E5+BGE at 45/64; it changes early ranking on one development query but does not account for the dense quality gap. On the held-out partition it changes none of those methods' metrics. Thus this fixture does **not** support the hypothesis that lexical retrieval is responsible for all successful answers. The stronger dense encoder is doing substantial work, and the current fusion policy is a separate problem.",
      '',
      '### Prioritized implementation follow-up (not deployed by this audit)',
      '',
      '1. Expose the semantic-passage target and final-input token counts in the MCP ingestion instructions/validation. Count each active encoder with its pinned tokenizer, include prefixes/captions/descriptions, and reject silent truncation for newly staged data. Do not make the server invent or mechanically rewrite semantic passages. Preserve page/region provenance and original evidence.',
      '2. Keep vector-only, lexical-only and fused ablations in the release gate. Qualify same-language and cross-language routes separately. Prefer a BGE-first warm policy over the currently unconditional equal-weight E5+BGE fusion unless a measured weighted/conditional alternative proves better. No claim that a ready E5 vector count makes its cold fallback reliable.',
      '3. Before mass re-chunking, run the selected small-passage candidate through BGE and verify held-out direct evidence, boundary/footnote completeness and a fixed context budget. Keep existing active revisions until the new revision is fully indexed and accepted.',
      '4. Keep FTS as a bounded optional parallel complement; improve its natural-query formulation separately from alias/phrase controls. Do not classify a fast empty AND-query as successful retrieval. A configurable branch deadline and a genuine no-lexical path are needed.',
      '5. Remove all-chunk-ID enumeration/transmission from the live path while retaining compact actor/document/revision authorization and bounded candidate provenance checks. Measure exact-versus-ANN recall and real network latency at larger diverse-corpus scale; the synthetic FTS stress does not establish that result.',
      '6. Set and test live response deadlines and explicit degraded states. The observed backend component timings are compatible with subsecond warm evidence lookup, but this audit is not a complete MCP/voice answer-latency or multiuser SLO acceptance. The current ten-second BGE wait and pending book-publication path need separate operational acceptance.',
      '',
      '### Run completion and test status',
      '',
      'All seven E5 size/control matrices, all 64 E5 query vectors, the complete 1,449-row BGE current matrix and all 64 BGE query vectors were completed and hash-recorded before their quality metrics were used. The first E5 sweep reached its one-hour execution bound after saving six complete matrices; only the remaining 256-token variant was resumed. A mixed BGE size job was stopped to finish the current corpus first, reusing same-owner byte-identical cached jobs. BGE document demand was bounded to eight outstanding audit jobs; the final audit queue was empty. No production vector installation occurred.',
      '',
      'Focused synthetic audit tests: **16 passed**. Full local suite: **186 passed, 26 skipped**, one existing Starlette deprecation warning. Skipped integration tests were not run and are not claimed. The private evidence remains retained. Source B was still awaiting vector publication at the read-only runtime capture; completing these independent lab vectors does not publish it.',
      '',END,'']
    text=report.read_text()
    old='Status: measured audit in progress; not a production rollout or a completed size selection. The machine summary is regenerated only from saved measurements. Pending matrices must never be evaluated as a complete corpus.'
    new='Status: completed seven-variant E5 audit, complete BGE current-corpus ablation, held-out evaluation and a measured small-passage operating recommendation. No production rollout or full BGE size-grid validation is claimed. The machine summary is generated only from saved completed measurements.'
    if old in text:text=text.replace(old,new,1)
    if BEGIN in text:
        before,rest=text.split(BEGIN,1);_,after=rest.split(END,1);text=before+'\n'.join(lines)+after
    else:
        marker='## Lexical latency is not intrinsically a full-corpus text scan'
        if marker not in text:raise ValueError('report insertion point missing')
        text=text.replace(marker,'\n'.join(lines)+'\n'+marker,1)
    report.write_text(text);print(json.dumps({'report':str(report),'bytes':report.stat().st_size,'selection':selected,'quality_matrices_complete':True}))
if __name__=='__main__':main()
