"""Synthetic-only tests of public-query E2E sample accounting.

No public MCP client, connection, PG, BGE, provider, credentials or Dev install.
Tests the existing ReadTelemetry record schema as the only stage source.
"""
from __future__ import annotations

import asyncio
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/"scripts/benchmarks/public_query_latency_leaf.py"
spec=importlib.util.spec_from_file_location("rkb_public_query_latency_leaf", PATH)
leaf=importlib.util.module_from_spec(spec)
spec.loader.exec_module(leaf)


def corpus(*, count=3):
    return dict(database_generation_ref="a"*64,
                schema_revision="rkb-v4-sql001-011-v1",
                accepted_source_count=count, visible_passage_count=128,
                applied_seq=25, projection_seq=25, visibility_epoch=7,
                corpus_manifest_sha256="b"*64, release_sha="c"*40,
                owner_handoff_ref="synthetic-owner-only")


def make_telemetry(client_rows):
    events=[]
    for sample in client_rows:
        if sample["event"] != "public_client_sample":continue
        trace_id,attempt_id,request_ref=(sample[k] for k in
            ("trace_id","attempt_id","request_ref"))
        events.append({
            "event":"read_attempt_terminal","plane":"controller","terminal":True,
            "trace_id":trace_id,"attempt_id":attempt_id,"request_ref":request_ref,
            "status":sample["status"],"reason":sample["reason"],
            "backend_elapsed_ms":4.5, "executed_branches":["lexical","dense"],
            "stages_ms":{},"timing_semantics":"overlapping_do_not_sum",
            "dispatch_state":"dispatched","diagnostic_state":"observed",
            "loss_before":{"dropped":0,"failed":0},
            "run_id":"synthetic-run","runtime_incarnation":"synthetic-incarnation",
            "connection_generation":1,"turn_generation":0,
            "controller_stages_ms":{"auth_ms":1.0,"admission_ms":1.0,
                "dispatch_to_result_ms":3.5,"finish_gate_ms":0.5}})
        fixed={"run_id":"synthetic-run","runtime_incarnation":"synthetic-incarnation",
            "connection_generation":1,"turn_generation":0,"request_ref":request_ref,
            "executed_branches":["lexical","dense"],
            "timing_semantics":"overlapping_do_not_sum",
            "diagnostic_state":"observed","loss_before":{"dropped":0,"failed":0},
            "controller_loss_before":{"dropped":0,"failed":0}}
        events.append(dict(fixed, event="kaggle_read_completed",
            plane="executor_stage",terminal=False,
            stages_ms={"tokenize_ms":1.0,"encode_ms":2.0,"lexical_ms":1.5,
                       "dense_ms":1.4,"hydrate_ms":0.7,
                       "search_ms":4.1,"serialize_ms":0.3,"total_ms":5.0}))
        events.append(dict(fixed, event="kaggle_read_adapter_terminal",
            plane="adapter",terminal=True,stages_ms={"adapter_ms":5.5},
            status=sample["status"],reason=sample["reason"]))
    return events


class WorkloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_not_ready_corpus_fails_before_public_queries(self):
        count=0
        async def query(text):
            nonlocal count
            count+=1
        async def readback():
            return corpus(count=0)
        with self.assertRaisesRegex(ValueError,"corpus_not_ready"):
            await leaf.run_public_workload({k:"Private wording"+str(n)
                        for n,k in enumerate(leaf.CASE_IDS)},query,readback,lambda row:None)
        self.assertEqual(count,0)

    async def test_callback_workload_has_reproducible_bounds_and_zero_private_fields(self):
        questions={k:"Never save private question wording %d"%i
                   for i,k in enumerate(leaf.CASE_IDS)}
        records=[]
        inflight=0
        max_concurrency=0
        counter=0
        snapshots=0
        async def query(text):
            nonlocal inflight,max_concurrency,counter
            self.assertIn(text,questions.values())
            inflight+=1
            max_concurrency=max(max_concurrency,inflight)
            try:
                await asyncio.sleep(0)
                counter+=1
                ident=sha256(str(counter).encode()).hexdigest()
                return {"status":"degraded","reason":"bounded_retrieval",
                    "mode":"hybrid","results":[{"id":"not emitted"}],
                    "read_trace":{"trace_id":ident[:32],
                                  "attempt_id":ident[32:]+"0"*32 if False else ident[:32],
                                  "request_ref":ident}}
            finally:
                inflight-=1
        async def readback():
            nonlocal snapshots
            snapshots+=1
            return corpus()
        receipt=await leaf.run_public_workload(questions,query,readback,records.append)
        self.assertEqual(receipt["calls"],128)
        self.assertEqual(receipt["warmups"],32)
        self.assertEqual(snapshots,2)
        self.assertLessEqual(max_concurrency,2)
        self.assertGreaterEqual(max_concurrency,2)
        self.assertEqual(len(records),130)
        samples=[x for x in records if x["event"]=="public_client_sample"]
        self.assertEqual(sum(x["phase"]=="serial" and not x["warmup"] for x in samples),48)
        self.assertEqual(sum(x["phase"]=="concurrent2" and not x["warmup"] for x in samples),48)
        for text in questions.values():
            self.assertNotIn(text,json.dumps(records))
        summary,joint=leaf.analyze(records,make_telemetry(samples))
        self.assertEqual(summary["conclusion"],"CANDIDATE_BASELINE_MEASURED_NOT_INTEGRATION_ACCEPTANCE")
        self.assertEqual(summary["cohorts"]["serial"]["samples"],48)
        self.assertEqual(summary["cohorts"]["serial"]["useful_hybrid"],48)
        self.assertEqual(summary["cohorts"]["concurrent2"]["observed_complete"],48)
        self.assertIsNone(summary["transport_standalone_ms"])
        self.assertIsNone(summary["kaggle_native_cpu_ms"])
        self.assertIsNone(summary["kaggle_native_rss_kib"])
        self.assertTrue(summary["overlapping_stages_not_summed"])
        self.assertTrue(all(x["diagnostic_state"]=="observed" for x in joint))

    async def test_changed_corpus_rejects_run_after_unmodified_query_records(self):
        questions={k:"irrelevant fixture %s"%k for k in leaf.CASE_IDS}
        n=0
        async def readback():
            nonlocal n
            n+=1
            return dict(corpus(),visibility_epoch=n)
        async def query(text):
            v=sha256((text+str(n)).encode()).hexdigest()
            return {"status":"degraded","reason":"bounded_retrieval","mode":"hybrid",
                "results":[{"id":"synthetic"}],
                "read_trace":{"trace_id":v[:32],"attempt_id":v[:32],"request_ref":v}}
        emitted=[]
        with self.assertRaisesRegex(RuntimeError,"corpus/version changed"):
            await leaf.run_public_workload(questions,query,readback,emitted.append)
        self.assertEqual(len([x for x in emitted if x["event"]=="public_client_sample"]),128)


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.samples=[]
        for phase,_ in leaf.COHORTS:
            for case_id in leaf.CASE_IDS:
                for seq in range(8):
                    trace=sha256((phase+case_id+str(seq)).encode()).hexdigest()
                    self.samples.append(dict(event="public_client_sample",
                        case_id=case_id,phase=phase,warmup=seq<2,
                        trace_id=trace[:32],attempt_id=trace[:32],request_ref=trace,
                        status="degraded",reason="bounded_retrieval",
                        mode="hybrid",evidence_count=1,
                        wall_ms=float(seq+1),client_cpu_ms=float(seq+1) if phase=="serial" else None,
                        client_rss_kib=2048))
        self.events=make_telemetry(self.samples)

    def test_percentiles_and_overlapping_stage_windows_are_not_summed(self):
        summary,joint=leaf.analyze(self.samples,self.events)
        first=summary["cohorts"]["serial"]
        self.assertEqual(first["p50_wall_ms"],5.0)
        self.assertEqual(first["p95_wall_ms"],8.0)
        self.assertEqual(first["native_stages"]["search_ms"]["p95_ms"],4.1)
        self.assertEqual(first["native_stages"]["encode_ms"]["p95_ms"],2.0)
        self.assertNotIn("total_additive_ms",first)
        self.assertEqual(first["useful_hybrid"],48)
        self.assertTrue(all(x["useful"] for x in joint))

    def test_loss_or_terminal_conflict_is_inconclusive_not_ignored(self):
        events=list(self.events)
        events[0]=dict(events[0],loss_before={"dropped":1,"failed":0})
        summary,_=leaf.analyze(self.samples,events)
        self.assertEqual(summary["conclusion"],"INCONCLUSIVE_OR_NONREPRESENTATIVE")
        self.assertEqual(summary["cohorts"]["serial"]["observed_complete"],47)
        events=list(self.events)
        events[0]=dict(events[0],status="denied",reason="access_denied")
        summary,_=leaf.analyze(self.samples,events)
        self.assertEqual(summary["cohorts"]["serial"]["useful_hybrid"],47)

    def test_missing_native_event_never_becomes_zero_latency(self):
        summary,joined=leaf.analyze(self.samples,self.events[1:])
        self.assertEqual(summary["conclusion"],"INCONCLUSIVE_OR_NONREPRESENTATIVE")
        self.assertEqual(sum(x["diagnostic_state"]=="inconclusive" for x in joined),1)

    def test_duplicate_terminal_and_invalid_private_payload_are_rejected(self):
        with self.assertRaisesRegex(ValueError,"duplicate controller terminal"):
            leaf.analyze(self.samples,self.events+[self.events[0]])
        injected=dict(self.events[0],secret="private credential")
        with self.assertRaisesRegex(ValueError,"potentially sensitive"):
            leaf.analyze(self.samples,[injected]+self.events[1:])

    def test_nonfinite_or_negative_stage_values_rejected(self):
        for val in (float("nan"),float("inf"),-1.0,True):
            changed=list(self.events)
            changed[1]=dict(changed[1],stages_ms=dict(changed[1]["stages_ms"],encode_ms=val))
            with self.assertRaisesRegex(ValueError,"stage"):
                leaf.analyze(self.samples,changed)

    def test_serialized_raw_jsonl_contains_only_public_fields(self):
        with tempfile.TemporaryDirectory() as t:
            client=Path(t)/"client.jsonl"
            events=Path(t)/"telemetry.jsonl"
            client.write_text("\n".join(json.dumps(x) for x in self.samples)+"\n")
            events.write_text("\n".join(json.dumps(x) for x in self.events)+"\n")
            summary,samples=leaf.analyze(leaf._read_events(client),leaf._read_events(events))
            self.assertEqual(summary["cohorts"]["serial"]["samples"],48)
            data=json.dumps(samples)
            self.assertNotIn("synthetic-run",data)
            self.assertNotIn("synthetic-incarnation",data)
            self.assertNotIn("not emitted",data)
            with client.open("ab") as handle:handle.write(b"x"*(leaf.MAX_LINE_BYTES+1)+b"\n")
            with self.assertRaisesRegex(ValueError,"bounded telemetry"):
                leaf._read_events(client)
