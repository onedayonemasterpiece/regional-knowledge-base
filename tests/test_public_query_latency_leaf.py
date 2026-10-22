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
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/"scripts/benchmarks/public_query_latency_leaf.py"
spec=importlib.util.spec_from_file_location("rkb_public_query_latency_leaf", PATH)
leaf=importlib.util.module_from_spec(spec)
spec.loader.exec_module(leaf)


def corpus(*, count=3, passages=128, profile="comparison_corpus"):
    return dict(database_generation_ref="a"*64,
                schema_revision="rkb-v4-sql001-011-v1",
                accepted_source_count=count, visible_passage_count=passages,
                applied_seq=25, projection_seq=25, visibility_epoch=7,
                corpus_manifest_sha256="b"*64, release_sha="c"*40,
                owner_handoff_ref="synthetic-owner-only", profile=profile)


def make_telemetry(client_rows):
    events=[]
    for sample in client_rows:
        if sample["event"] != "public_client_sample" or sample["trace_id"] is None:continue
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
        with self.assertRaisesRegex(ValueError,"comparison_corpus_requires"):
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
        summary,joint=leaf.analyze(records,make_telemetry(samples),profile="comparison_corpus")
        self.assertEqual(summary["conclusion"],"COMPARISON_CORPUS_BASELINE_ONLY_NOT_PRODUCTION_ACCEPTANCE")
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
        with self.assertRaisesRegex(RuntimeError,"corpus/profile/version changed"):
            await leaf.run_public_workload(questions,query,readback,emitted.append)
        self.assertEqual(len([x for x in emitted if x["event"]=="public_client_sample"]),128)


class CensorAndProfileTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _questions():
        return {k: "PRIVATE query material "+k for k in leaf.CASE_IDS}

    @staticmethod
    def _response(text, index):
        ident=sha256((text+str(index)).encode()).hexdigest()
        return {"status":"degraded","reason":"bounded_retrieval","mode":"hybrid",
                "results":[{"id":"private must never be saved"}],
                "read_trace":{"trace_id":ident[:32],"attempt_id":ident[:32],
                              "request_ref":ident}}

    @staticmethod
    def _operator_first_book():
        return corpus(count=1, passages=5, profile="limited_first_book")

    async def test_limited_first_real_book_profile_allows_one_bounded_source(self):
        before=self._operator_first_book()
        self.assertEqual(leaf.valid_corpus(before)["profile"],"limited_first_book")
        self.assertEqual(leaf.valid_corpus(corpus())["profile"],"comparison_corpus")
        with self.assertRaisesRegex(ValueError,"comparison_corpus_requires"):
            leaf.valid_corpus(dict(before,profile="comparison_corpus"))
        with self.assertRaisesRegex(ValueError,"limited_first_book_requires"):
            leaf.valid_corpus(dict(before,visible_passage_count=0))
        with self.assertRaisesRegex(ValueError,"limited_first_book_requires"):
            leaf.valid_corpus(dict(corpus(),profile="limited_first_book"))
        counter=0
        emitted=[]
        async def query(text):
            nonlocal counter
            counter+=1
            await asyncio.sleep(0)
            return self._response(text,counter)
        async def readback():
            return dict(before)
        receipt=await leaf.run_public_workload(self._questions(),query,readback,emitted.append)
        self.assertEqual(receipt["corpus"]["profile"],"limited_first_book")
        records=[x for x in emitted if x["event"]=="public_client_sample"]
        self.assertEqual(len(records),128)
        report,_=leaf.analyze(emitted,make_telemetry(records),profile="limited_first_book")
        self.assertEqual(report["conclusion"],"LIMITED_FIRST_BOOK_BASELINE_ONLY_NOT_REPRESENTATIVE")
        self.assertFalse(report["production_scale_accepted"])
        self.assertIn("NOT_REPRESENTATIVE",report["profile_label"])
        self.assertEqual(report["cohorts"]["serial"]["timeout_rate"],0)
        self.assertEqual(report["cohorts"]["concurrent2"]["public_success_count"],48)

    async def test_caller_3s_timeout_is_right_censored_and_all_timeout_rate_one(self):
        emitted=[]
        async def query(text):
            await asyncio.sleep(.05)
            raise AssertionError("must be cancelled before private late response")
        async def readback():
            return self._operator_first_book()
        with patch.object(leaf,"CALL_TIMEOUT_S",.003):
            receipt=await leaf.run_public_workload(self._questions(),query,readback,emitted.append)
        self.assertEqual(receipt["calls"],128)
        samples=[x for x in emitted if x["event"]=="public_client_sample"]
        self.assertEqual(sum(x["latency_observation"]=="right_censored" for x in samples),128)
        self.assertEqual(sum(x["warmup"] for x in samples),32)
        self.assertTrue(all(x["censor_lower_bound_ms"]==x["callback_wall_ms"] for x in samples))
        report,joint=leaf.analyze(emitted,[],profile="limited_first_book")
        for group in report["cohorts"].values():
            self.assertEqual(group["samples"],48)
            self.assertEqual(group["timeout_rate"],1.0)
            self.assertEqual(group["failure_rate"],1.0)
            self.assertEqual(group["right_censored_count"],48)
            self.assertEqual(group["caller_timeout_censored_count"],48)
            self.assertIsNone(group["success_only_p95_public_callback_ms"])
            self.assertIsNone(group["all_requests_completion_p95_public_ms"])
            self.assertGreaterEqual(group["all_requests_p95_public_lower_bound_ms"],2.5)
        self.assertEqual(report["conclusion"],"INCONCLUSIVE_OR_CENSORED")
        self.assertTrue(all(x["diagnostic_state"]=="inconclusive" for x in joint))
        self.assertNotIn("PRIVATE query material",json.dumps(joint))
        self.assertNotIn("must be cancelled",json.dumps(joint))

    async def test_slow_tail_partial_success_and_fast_remote_deadline_keep_denominator(self):
        emitted=[]
        counter=0
        async def query(text):
            nonlocal counter
            counter+=1
            if text.endswith("q08"):
                await asyncio.sleep(.05)
                return self._response(text,counter)
            if text.endswith("q07"):
                raise TimeoutError("secret remote exception text")
            await asyncio.sleep(0)
            return self._response(text,counter)
        async def readback():
            return self._operator_first_book()
        with patch.object(leaf,"CALL_TIMEOUT_S",.003):
            await leaf.run_public_workload(self._questions(),query,readback,emitted.append)
        samples=[x for x in emitted if x["event"]=="public_client_sample"]
        report,joint=leaf.analyze(emitted,make_telemetry(samples),profile="limited_first_book")
        for group in report["cohorts"].values():
            self.assertEqual(group["samples"],48)
            self.assertEqual(group["public_success_count"],36)
            self.assertEqual(group["failure_count"],12)
            self.assertEqual(group["timeout_count"],12)
            self.assertEqual(group["timeout_rate"],.25)
            self.assertEqual(group["failure_rate"],.25)
            self.assertEqual(group["caller_timeout_censored_count"],6)
            self.assertEqual(group["server_deadline_completed_count"],6)
            self.assertEqual(group["right_censored_count"],6)
            self.assertIsNone(group["all_requests_completion_p95_public_ms"])
            self.assertIsNone(group["all_requests_completion_p95_scheduled_ms"])
            self.assertIsNotNone(group["success_only_p95_public_callback_ms"])
            self.assertLess(group["success_only_p95_public_callback_ms"],
                            group["all_requests_p95_public_lower_bound_ms"])
            self.assertEqual(group["status_reason_counts"]["unavailable:deadline_exceeded"],12)
        self.assertEqual(report["conclusion"],"INCONCLUSIVE_OR_CENSORED")
        self.assertNotIn("secret remote exception",json.dumps(joint))
        self.assertNotIn("PRIVATE query material",json.dumps(joint))

    async def test_local_semaphore_wait_measured_not_hidden_or_added_to_public_callback(self):
        emitted=[]
        counter=0
        async def query(text):
            nonlocal counter
            counter+=1
            await asyncio.sleep(.004)
            return self._response(text,counter)
        async def readback():
            return self._operator_first_book()
        with patch.object(leaf,"CALL_TIMEOUT_S",.1):
            await leaf.run_public_workload(self._questions(),query,readback,emitted.append)
        report,joint=leaf.analyze(emitted,make_telemetry(emitted),profile="limited_first_book")
        rows=[x for x in joint if x["phase"]=="concurrent2" and not x["warmup"]]
        self.assertEqual(len(rows),48)
        self.assertTrue(any(x["client_semaphore_wait_ms"]>=5 for x in rows))
        self.assertTrue(all(abs(x["wall_ms"]-x["callback_wall_ms"]
                                -x["client_semaphore_wait_ms"])<.02 for x in rows))
        self.assertGreater(report["cohorts"]["concurrent2"]["success_only_p95_scheduled_ms"],
                           report["cohorts"]["concurrent2"]["success_only_p95_public_callback_ms"])
        self.assertFalse(report["semaphore_wait_in_public_callback_latency"])
        self.assertTrue(report["semaphore_wait_in_client_scheduled_latency"])

    async def test_before_after_requires_same_profile_same_corpus_and_question_hash(self):
        async def readback():
            return self._operator_first_book()
        counter=0
        async def query(text):
            nonlocal counter
            counter+=1
            return self._response(text,counter)
        receipt=await leaf.run_public_workload(self._questions(),query,readback,lambda row:None)
        self.assertTrue(leaf.assert_comparable_receipts(receipt,dict(receipt))["same_fixture"])
        other_profile=dict(receipt,corpus=dict(corpus(),profile="comparison_corpus"))
        with self.assertRaisesRegex(ValueError,"before_after_profile_changed"):
            leaf.assert_comparable_receipts(receipt,other_profile)
        changed_questions=dict(receipt,question_fixture_sha256="f"*64)
        with self.assertRaisesRegex(ValueError,"before_after_question_fixture_changed"):
            leaf.assert_comparable_receipts(receipt,changed_questions)
        changed_revision=dict(receipt,corpus=dict(receipt["corpus"],visibility_epoch=99))
        with self.assertRaisesRegex(ValueError,"before_after_corpus_or_version_changed"):
            leaf.assert_comparable_receipts(receipt,changed_revision)

    async def test_changed_profile_mid_run_rejected_without_faking_baseline(self):
        observations=0
        async def readback():
            nonlocal observations
            observations+=1
            if observations==1:
                return self._operator_first_book()
            return corpus()
        emitted=[]
        n=0
        async def query(text):
            nonlocal n
            n+=1
            return self._response(text,n)
        with self.assertRaisesRegex(RuntimeError,"corpus/profile/version changed"):
            await leaf.run_public_workload(self._questions(),query,readback,emitted.append)
        self.assertEqual(n,128)
        self.assertEqual(observations,2)

    def test_timeout_shape_and_production_deadline_distinct_from_client_guard(self):
        row=leaf._client_row(None,case_id="q01",phase="concurrent2",warmup=False,
            elapsed_ms=3600.0,callback_ms=3000.0,semaphore_wait_ms=600.0,
            cpu_ms=None,rss_kib=123,exception="caller_timeout")
        self.assertEqual(row["latency_observation"],"right_censored")
        self.assertEqual(row["censor_lower_bound_ms"],3000.0)
        self.assertEqual(row["client_semaphore_wait_ms"],600.0)
        remote=leaf._client_row(None,case_id="q01",phase="serial",warmup=False,
            elapsed_ms=1800.0,callback_ms=1800.0,semaphore_wait_ms=0.0,
            cpu_ms=1,rss_kib=123,exception="remote_timeout")
        self.assertEqual(remote["latency_observation"],"completed")
        self.assertIsNone(remote["censor_lower_bound_ms"])
        self.assertEqual(leaf.PUBLIC_SERVER_QUERY_DEADLINE_S,2.0)
        self.assertEqual(leaf.PUBLIC_MCP_P95_SLO_MS,1500)
        self.assertEqual(leaf.CALL_TIMEOUT_S,3.0)
        backend=(ROOT/"src/regional_knowledge/kaggle_public_backend.py").read_text()
        worker=(ROOT/"src/regional_knowledge/kaggle_query.py").read_text()
        self.assertIn("deadline = min(time.monotonic()+2,",backend)
        self.assertIn("deadline = started + min(query.deadline_ms, 2000) / 1000",worker)




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
                        wall_ms=float(seq+1),callback_wall_ms=float(seq+1),
                        client_semaphore_wait_ms=0.0,latency_observation="completed",
                        censor_lower_bound_ms=None,
                        client_cpu_ms=float(seq+1) if phase=="serial" else None,
                        client_rss_kib=2048))
        self.events=make_telemetry(self.samples)

    def test_percentiles_and_overlapping_stage_windows_are_not_summed(self):
        summary,joint=leaf.analyze(self.samples,self.events,profile="comparison_corpus")
        first=summary["cohorts"]["serial"]
        self.assertEqual(first["success_only_p50_public_callback_ms"],5.0)
        self.assertEqual(first["success_only_p95_public_callback_ms"],8.0)
        self.assertEqual(first["all_requests_completion_p95_public_ms"],8.0)
        self.assertEqual(first["timeout_rate"],0.0)
        self.assertEqual(first["native_stages"]["search_ms"]["p95_ms"],4.1)
        self.assertEqual(first["native_stages"]["encode_ms"]["p95_ms"],2.0)
        self.assertNotIn("total_additive_ms",first)
        self.assertEqual(first["useful_hybrid"],48)
        self.assertTrue(all(x["useful"] for x in joint))

    def test_loss_or_terminal_conflict_is_inconclusive_not_ignored(self):
        events=list(self.events)
        events[6]=dict(events[6],loss_before={"dropped":1,"failed":0})
        summary,_=leaf.analyze(self.samples,events,profile="comparison_corpus")
        self.assertEqual(summary["conclusion"],"INCONCLUSIVE_OR_CENSORED")
        self.assertEqual(summary["cohorts"]["serial"]["observed_complete"],47)
        events=list(self.events)
        events[6]=dict(events[6],status="denied",reason="access_denied")
        summary,_=leaf.analyze(self.samples,events,profile="comparison_corpus")
        self.assertEqual(summary["cohorts"]["serial"]["useful_hybrid"],47)

    def test_missing_native_event_never_becomes_zero_latency(self):
        summary,joined=leaf.analyze(self.samples,self.events[:7]+self.events[8:],profile="comparison_corpus")
        self.assertEqual(summary["conclusion"],"INCONCLUSIVE_OR_CENSORED")
        self.assertEqual(sum(x["diagnostic_state"]=="inconclusive" for x in joined),1)

    def test_duplicate_terminal_and_invalid_private_payload_are_rejected(self):
        with self.assertRaisesRegex(ValueError,"duplicate controller terminal"):
            leaf.analyze(self.samples,self.events+[self.events[0]],profile="comparison_corpus")
        injected=dict(self.events[0],secret="private credential")
        with self.assertRaisesRegex(ValueError,"potentially sensitive"):
            leaf.analyze(self.samples,[injected]+self.events[1:],profile="comparison_corpus")

    def test_nonfinite_or_negative_stage_values_rejected(self):
        for val in (float("nan"),float("inf"),-1.0,True):
            changed=list(self.events)
            changed[1]=dict(changed[1],stages_ms=dict(changed[1]["stages_ms"],encode_ms=val))
            with self.assertRaisesRegex(ValueError,"stage"):
                leaf.analyze(self.samples,changed,profile="comparison_corpus")

    def test_actual_production_telemetry_stage_vocabulary_not_diverged(self):
        import ast
        def value_of(path,name):
            tree=ast.parse(path.read_text())
            found=[node.value for node in tree.body if isinstance(node,ast.Assign)
                   and any(isinstance(target,ast.Name) and target.id==name
                           for target in node.targets)]
            self.assertEqual(len(found),1)
            # The existing source declares these with frozenset({...}).
            self.assertIsInstance(found[0],ast.Call)
            self.assertEqual(found[0].func.id,"frozenset")
            return frozenset(ast.literal_eval(found[0].args[0]))
        self.assertEqual(value_of(ROOT/"src/regional_knowledge/query_telemetry.py","_STAGES"),
                         leaf.STAGES)
        self.assertEqual(value_of(ROOT/"src/regional_knowledge/read_telemetry.py","_CONTROLLER_STAGES"),
                         leaf.CONTROLLER_STAGES)

    def test_native_terminal_mismatch_is_not_counted_as_useful_hybrid(self):
        events=list(self.events)
        events[8]=dict(events[8],status="error",reason="internal_error")
        summary,_=leaf.analyze(self.samples,events,profile="comparison_corpus")
        self.assertEqual(summary["conclusion"],"INCONCLUSIVE_OR_CENSORED")
        self.assertEqual(summary["cohorts"]["serial"]["useful_hybrid"],47)

    def test_native_runtime_reconnect_invalidates_comparison(self):
        events=list(self.events)
        # First measured request of concurrent2 is 64*3 + 2*3.
        first=(len(leaf.CASE_IDS)*8)*3+2*3
        for offset in (0,1,2):
            events[first+offset]=dict(events[first+offset],run_id="synthetic-new-run")
        summary,_=leaf.analyze(self.samples,events,profile="comparison_corpus")
        self.assertFalse(summary["native_runtime_scope_stable"])
        self.assertEqual(summary["conclusion"],"INCONCLUSIVE_OR_CENSORED")

    def test_process_cpu_and_rss_are_scoped_to_client_not_kaggle(self):
        resource=[{"event":"public_phase_resources","phase":name,
                   "client_process_cpu_ms":12.5,"client_peak_rss_kib":12345}
                  for name,_ in leaf.COHORTS]
        summary,_=leaf.analyze(self.samples+resource,self.events,profile="comparison_corpus")
        self.assertEqual(summary["cohorts"]["serial"]["client_phase_cpu_total_ms"],12.5)
        self.assertEqual(summary["cohorts"]["concurrent2"]["client_phase_peak_rss_kib"],12345)
        self.assertIsNone(summary["kaggle_native_cpu_ms"])
        with self.assertRaisesRegex(ValueError,"duplicate client process resource"):
            leaf.analyze(self.samples+resource+resource[:1],self.events,profile="comparison_corpus")

    def test_serialized_raw_jsonl_contains_only_public_fields(self):
        with tempfile.TemporaryDirectory() as t:
            client=Path(t)/"client.jsonl"
            events=Path(t)/"telemetry.jsonl"
            client.write_text("\n".join(json.dumps(x) for x in self.samples)+"\n")
            events.write_text("\n".join(json.dumps(x) for x in self.events)+"\n")
            summary,samples=leaf.analyze(leaf._read_events(client),leaf._read_events(events),profile="comparison_corpus")
            self.assertEqual(summary["cohorts"]["serial"]["samples"],48)
            data=json.dumps(samples)
            self.assertNotIn("synthetic-run",data)
            self.assertNotIn("synthetic-incarnation",data)
            self.assertNotIn("not emitted",data)
            with client.open("ab") as handle:handle.write(b"x"*(leaf.MAX_LINE_BYTES+1)+b"\n")
            with self.assertRaisesRegex(ValueError,"bounded telemetry"):
                leaf._read_events(client)
