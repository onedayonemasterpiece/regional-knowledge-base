#!/usr/bin/env python3
"""Offline numeric views of an existing PMI trace; no network, models or original gold.

Outputs are built from an explicit allowlist. Query text, native identifiers,
URLs, handles, document text and runtime paths never enter the output rows.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


ARMS = ("A0", "A1", "A2", "A3", "A4")
OPS = {"search", "fetch", "book_find", "catalog", "status", "entity_list",
       "story_calendar", "geo_lookup", "story_search", "story_get", "graph_fetch"}
PURPOSES = {"supplemental_source_diagnostic", "german_source_diagnostic",
            "boolean_diagnostic", "source_scope_diagnostic", "context_ablation",
            "typed_entity_diagnostic"}
STATES = {"ok", "ready", "success", "completed", "partial", "error", "timeout",
          "denied", "pending", "queued", "unavailable", "not_found", "not_ready"}
ERRORS = {"INVALID_ARGUMENT", "UNAVAILABLE", "INTERNAL", "NOT_FOUND",
          "PERMISSION_DENIED", "DEADLINE_EXCEEDED", "CANCELLED"}


def number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if math.isfinite(value) and value >= 0 else None
    return None


def integer(value):
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def object_value(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except (ValueError, TypeError):
            return None
    return None


def normalize_response(call):
    """Prefer structuredContent; unwrap result, including serialized objects."""
    response = object_value(call.get("response")) or {}
    payload = object_value(response.get("structuredContent"))
    if payload is None:
        for block in response.get("content", []):
            if isinstance(block, dict) and block.get("type") == "text":
                payload = object_value(block.get("text"))
                if payload is not None:
                    break
    if payload is None:
        payload = {}
    for _ in range(4):
        inner = object_value(payload.get("result"))
        if inner is None:
            break
        payload = inner
    return response, payload


def execution(response, payload):
    state = payload.get("status")
    if response.get("isError") is True or payload.get("error_code"):
        return "error"
    if state in {"error", "timeout", "denied", "unavailable", "not_ready"}:
        return state
    if state in {"pending", "queued"}:
        return "pending"
    if state in {"ok", "ready", "success", "completed", "partial", "not_found"}:
        return "completed"
    if any(key in payload for key in ("results", "chunks", "text", "books")):
        return "completed"
    return "unknown"


def timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result if result.tzinfo else result.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def state_key(payload):
    return (integer(payload.get("observed_data_seq")),
            integer(payload.get("observed_projection_seq")))


def strict_sum(values):
    return sum(values) if values and all(v is not None for v in values) else None


def distribution(values):
    present = sorted(v for v in values if v is not None)
    n = len(present)
    p95 = None
    if n:
        position = .95 * (n - 1)  # descriptive linear-interpolated quantile
        lo, hi = math.floor(position), math.ceil(position)
        p95 = present[lo] + (present[hi] - present[lo]) * (position - lo)
    return {"n": n, "missing": len(values) - n,
            "sum_complete": strict_sum(values),
            "sum_known": sum(present) if present else None,
            "median": statistics.median(present) if present else None,
            "max": max(present) if present else None,
            "empirical_p95": p95}


def rrf_components(lists, k=60):
    """Each list contains {key, url, rank, confirmed}; retain first URL per list.

    Unconfirmed entries must have occurrence-specific keys supplied by caller.
    Therefore an unconfirmed URL is never fused across lists by this function.
    """
    merged = {}
    encounter = 0
    for list_no, entries in enumerate(lists, 1):
        seen_urls = set()
        for entry in entries:
            if not entry.get("url") or entry["url"] in seen_urls:
                continue
            seen_urls.add(entry["url"])
            key = entry["key"]
            if key not in merged:
                merged[key] = {**entry, "score": 0.0, "ranks": {},
                               "encounter": encounter}
                encounter += 1
            merged[key]["score"] += 1.0 / (k + entry["rank"])
            merged[key]["ranks"][list_no] = entry["rank"]
    return sorted(merged.values(), key=lambda e: (-e["score"],
                  min(e["ranks"].values()), e["encounter"]))


class Analyzer:
    def __init__(self, trace, expected_sources=None, cutoff=None):
        self.trace = trace
        self.cutoff = timestamp(cutoff)
        selected = trace.get("plan", {}).get("selected_families", [])
        self.families = {f: (f if isinstance(f, str) and re.fullmatch(r"F[0-9]{2}", f)
                            else f"F{i:02d}") for i, f in enumerate(selected, 1)}
        self.expected_sources = {alias: (expected_sources or {}).get(family)
                                 for family, alias in self.families.items()}
        self.calls = []
        self.bindings = defaultdict(list)
        self.passage_labels = {}
        self.source_labels = {}
        self.query_labels = {}
        self.state_labels = {}
        for index, raw in enumerate(trace.get("calls", []), 1):
            response, payload = normalize_response(raw)
            op = raw.get("operation") if raw.get("operation") in OPS else "other"
            when = timestamp(raw.get("started_utc"))
            scope = (op == "search" and raw.get("family_id") in self.families
                     and raw.get("arm") in ARMS and
                     not any(key in raw.get("args", {}) for key in
                             ("document_ids", "source_ids", "source_filter")))
            cutoff_unknown = bool(scope and self.cutoff and when is None)
            core = bool(scope and not cutoff_unknown and
                        (self.cutoff is None or when <= self.cutoff))
            read_trace = object_value(payload.get("read_trace")) or {}
            c = {"raw": raw, "p": payload, "index": index, "op": op,
                 "alias": f"C{index:04d}", "core": core,
                 "cutoff_unknown": cutoff_unknown,
                 "family": self.families.get(raw.get("family_id")),
                 "arm": raw.get("arm") if raw.get("arm") in ARMS else None,
                 "state": state_key(payload), "execution": execution(response, payload),
                 "wall": number(raw.get("elapsed_ms")),
                 "native": number(read_trace.get("backend_elapsed_ms")),
                 "reported": number(payload.get("latency_ms"))}
            self.calls.append(c)
            if op == "fetch" and c["execution"] == "completed":
                self._register_fetch(c)

    def _register_fetch(self, call):
        p = call["p"]
        meta = object_value(p.get("metadata")) or {}
        provenance = object_value(meta.get("source_provenance")) or {}
        revisions = meta.get("source_revisions")
        revisions = revisions if isinstance(revisions, list) else []
        source = revision = None
        conflict = False
        if len(revisions) == 1 and isinstance(revisions[0], dict):
            source = revisions[0].get("source_id")
            revision = integer(revisions[0].get("content_revision"))
        stated_revision = integer(provenance.get("source_revision"))
        if revision is not None and stated_revision is not None:
            conflict = revision != stated_revision
        if revision is None:
            revision = stated_revision
        url = p.get("url")
        if isinstance(url, str) and url:
            self.bindings[url].append({"source": source, "revision": revision,
                "state": call["state"], "conflict": conflict})

    def binding(self, url, state):
        rows = self.bindings.get(url, [])
        sources = {r["source"] for r in rows if r["source"]}
        source = next(iter(sources)) if len(sources) == 1 else None
        matching = [r for r in rows if state[0] is not None and
                    r["state"][0] == state[0] and
                    (state[1] is None or r["state"][1] == state[1])]
        confirmed = {(r["source"], r["revision"]) for r in matching
                     if r["source"] and r["revision"] is not None and not r["conflict"]}
        bad = any(r["conflict"] for r in matching) or len(confirmed) > 1
        if len(confirmed) == 1 and not bad:
            s, rev = next(iter(confirmed))
            return {"source": s, "revision": rev, "confirmed": True,
                    "status": "confirmed"}
        return {"source": source, "revision": None, "confirmed": False,
                "status": "conflict" if bad else "unverified"}

    @staticmethod
    def label(mapping, value, prefix):
        if value is None:
            return None
        if value not in mapping:
            mapping[value] = f"{prefix}{len(mapping) + 1:03d}"
        return mapping[value]

    def candidates(self, call):
        results = call["p"].get("results")
        if not isinstance(results, list):
            return []
        entries = []
        for rank, result in enumerate(results, 1):
            if not isinstance(result, dict):
                continue
            url = result.get("url")
            if not isinstance(url, str) or not url:
                continue
            b = self.binding(url, call["state"])
            key = ("confirmed", url, b["source"], b["revision"]) if b["confirmed"] else (
                "unverified", call["index"], rank)
            entries.append({"url": url, "rank": rank, "key": key, **b,
                "passage": self.label(self.passage_labels, url, "P"),
                "source_label": self.label(self.source_labels, b["source"], "S")})
        return entries

    def analyze(self):
        call_rows, rank_rows, family_rows, rrf_rows, latency_rows, scope_rows = [], [], [], [], [], []
        groups = defaultdict(list)
        for c in self.calls:
            if c["core"]:
                groups[(c["family"], c["arm"])].append(c)
        baselines = {f: groups[(f, "A0")][0] for f in self.families.values()
                     if groups.get((f, "A0"))}
        pair_states = Counter()
        query_orders = {}
        for group in groups.values():
            for order, c in enumerate(group, 1):
                query_orders[c["index"]] = order
        for c in self.calls:
            p, raw = c["p"], c["raw"]
            state = p.get("status")
            query = raw.get("args", {}).get("query") if c["op"] == "search" else None
            entries = self.candidates(c) if c["op"] == "search" else []
            expected_source = self.expected_sources.get(c["family"])
            scopes = [e["source"] == expected_source for e in entries
                      if e["source"] and expected_source]
            call_rows.append({"call": c["alias"], "scope": "core_search" if c["core"] else
                ("other_search" if c["op"] == "search" else c["op"]),
                "family": c["family"], "arm": c["arm"], "operation": c["op"],
                "purpose_group": raw.get("purpose") if raw.get("purpose") in PURPOSES else
                    ("core_search" if c["core"] else "other_or_unspecified"),
                "query_order": query_orders.get(c["index"]),
                "query_group": self.label(self.query_labels, query, "Q") if c["core"] else None,
                "execution": c["execution"], "native_status": state if state in STATES else None,
                "native_error": p.get("error_code") if p.get("error_code") in ERRORS else None,
                "wall_ms": c["wall"], "native_ms": c["native"],
                "reported_latency_ms": c["reported"],
                "wall_native_delta_ms": c["wall"] - c["native"]
                    if c["wall"] is not None and c["native"] is not None else None,
                "state_group": self.label(self.state_labels, c["state"], "D")
                    if c["state"][0] is not None else None,
                "returned_candidates": len(p["results"]) if isinstance(p.get("results"), list) else None,
                "revision_confirmed_candidates": sum(e["confirmed"] for e in entries)
                    if c["op"] == "search" and isinstance(p.get("results"), list) else None,
                "known_candidate_sources": len(scopes) if expected_source and c["op"] == "search" else None,
                "source_scope_mismatches": sum(not s for s in scopes) if scopes else None})
            if not c["core"]:
                continue
            base = self.candidates(baselines[c["family"]]) if c["family"] in baselines else []
            by_url = {e["url"]: e for e in reversed(base)}
            top_source = base[0]["source"] if base else None
            for entry in entries:
                b = by_url.get(entry["url"])
                verified = bool(b and b["confirmed"] and entry["confirmed"] and b["key"] == entry["key"])
                rank_rows.append({"call": c["alias"], "family": c["family"], "arm": c["arm"],
                    "query_order": query_orders[c["index"]], "passage": entry["passage"],
                    "rank": entry["rank"], "identity_status": entry["status"],
                    "source": entry["source_label"],
                    "source_scope_match": entry["source"] == expected_source
                        if entry["source"] and expected_source else None,
                    "source_shift_vs_baseline_top1": entry["source"] != top_source
                        if entry["source"] and top_source else None,
                    "baseline_rank_url_only": b["rank"] if b else None,
                    "rank_delta_url_only": entry["rank"] - b["rank"] if b else None,
                    "rank_delta_confirmed": entry["rank"] - b["rank"] if verified else None})
        for family in self.families.values():
            expected_source = self.expected_sources.get(family)
            for arm in ARMS:
                group = groups.get((family, arm), [])
                entries_by_call = [self.candidates(c) for c in group]
                candidate_entries = [e for entries in entries_by_call for e in entries]
                scope_known = [e for e in candidate_entries if e["source"] and expected_source]
                base = self.candidates(baselines[family]) if family in baselines else []
                baseline_urls = {e["url"] for e in base}
                urls = {e["url"] for e in candidate_entries}
                ws, ns = distribution([c["wall"] for c in group]), distribution([c["native"] for c in group])
                all_ok = bool(group) and all(c["execution"] == "completed" and
                    isinstance(c["p"].get("results"), list) for c in group)
                pair_status = "single_query" if len(group) == 1 else "not_executed"
                selected = entries_by_call[0][:3] if len(group) == 1 and all_ok else None
                if len(group) == 2:
                    full = all_ok and all(e["confirmed"] for e in candidate_entries) and all(
                        len(entries) == len(c["p"]["results"])
                        for c, entries in zip(group, entries_by_call))
                    same_state = group[0]["state"] == group[1]["state"] and group[0]["state"][0] is not None
                    pair_status = ("complete" if full and same_state else
                        "snapshot_mismatch_or_unknown" if full else "unverified_identity" if all_ok else "incomplete_calls")
                    if all_ok:
                        components = rrf_components(entries_by_call)
                        if pair_status == "complete":
                            selected = components[:3]
                        for rank, e in enumerate(components, 1):
                            rrf_rows.append({"family": family, "arm": arm, "pair_status": pair_status,
                                "passage": e["passage"], "source": e["source_label"],
                                "identity_status": e["status"],
                                "rrf_rank": rank if pair_status == "complete" else None,
                                "rrf_score": e["score"], "rank_query_1": e["ranks"].get(1),
                                "rank_query_2": e["ranks"].get(2), "contributing_lists": len(e["ranks"]),
                                "source_scope_match": e["source"] == expected_source
                                    if e["source"] and expected_source else None})
                elif len(group) > 2:
                    pair_status = "ambiguous_call_count"
                pair_states[pair_status] += 1
                selected_known = [e for e in selected if e["source"] and expected_source] if selected is not None else []
                scope_rows.append({"family": family, "arm": arm,
                    "selector": "single_query_top3" if len(group) == 1 else "rrf60_diagnostic_top3",
                    "selection_status": "available" if selected is not None else "unavailable",
                    "selected_candidates": len(selected) if selected is not None else None,
                    "known_candidate_sources": len(selected_known) if selected is not None and expected_source else None,
                    "source_scope_mismatches": sum(e["source"] != expected_source for e in selected_known)
                        if selected_known else None})
                family_rows.append({"family": family, "arm": arm,
                    "search_calls": len(group), "completed_calls": sum(c["execution"] == "completed" for c in group),
                    "noncompleted_calls": sum(c["execution"] != "completed" for c in group),
                    "search_wall_sum_ms": ws["sum_complete"], "search_wall_known_sum_ms": ws["sum_known"],
                    "search_native_sum_ms": ns["sum_complete"], "search_native_known_sum_ms": ns["sum_known"],
                    "wall_missing": ws["missing"], "native_missing": ns["missing"],
                    "unique_returned_urls": len(urls) if all_ok else None,
                    "url_overlap_with_A0": len(urls & baseline_urls) if all_ok and base else None,
                    "new_urls_vs_A0": len(urls - baseline_urls) if all_ok and base else None,
                    "baseline_urls_not_returned": len(baseline_urls - urls) if all_ok and base else None,
                    "candidate_source_known": len(scope_known) if all_ok and expected_source else None,
                    "candidate_scope_mismatches": sum(e["source"] != expected_source for e in scope_known)
                        if scope_known else None, "rrf_pair_status": pair_status})
        for arm in ARMS:
            arm_calls = [c for c in self.calls if c["core"] and c["arm"] == arm]
            orders = sorted({query_orders[c["index"]] for c in arm_calls})
            for order in [None] + orders:
                subset = [c for c in arm_calls if order is None or query_orders[c["index"]] == order]
                row = {"arm": arm, "query_order": order, "search_calls": len(subset),
                       "completed_calls": sum(c["execution"] == "completed" for c in subset)}
                for name in ("wall", "native", "reported"):
                    row.update({f"{name}_{key}": value for key, value in distribution([c[name] for c in subset]).items()})
                latency_rows.append(row)
        paired_rows = []
        for family in self.families.values():
            a0, a1 = groups.get((family, "A0"), []), groups.get((family, "A1"), [])
            valid = len(a0) == len(a1) == 1 and all(
                c["execution"] == "completed" for c in a0 + a1)
            n0 = a0[0]["native"] if valid else None
            n1 = a1[0]["native"] if valid else None
            paired_rows.append({"family": family, "A0_native_ms": n0,
                "A1_native_ms": n1,
                "A1_minus_A0_native_ms": n1 - n0 if n0 is not None and n1 is not None else None,
                "A1_div_A0_native_ratio": n1 / n0 if n0 is not None and n0 > 0 and n1 is not None else None})
        fetch_calls = [c for c in self.calls if c["op"] == "fetch"]
        fetched_urls, confirmed_fetches, source_chunks = set(), set(), set()
        returned_chunks = 0
        for c in fetch_calls:
            if c["execution"] != "completed":
                continue
            p = c["p"]
            if isinstance(p.get("url"), str):
                fetched_urls.add(p["url"])
                b = self.binding(p["url"], c["state"])
                if b["confirmed"]:
                    confirmed_fetches.add((p["url"], b["source"], b["revision"]))
            for chunk in p.get("chunks", []) if isinstance(p.get("chunks"), list) else []:
                if isinstance(chunk, dict) and chunk.get("chunk_id"):
                    returned_chunks += 1
                    source_chunks.add((chunk.get("source_id", p.get("source_id")), chunk["chunk_id"], c["state"]))
        core = [c for c in self.calls if c["core"]]
        core_entries = [e for c in core for e in self.candidates(c)]
        repeated_queries = defaultdict(list)
        for c in core:
            query = c["raw"].get("args", {}).get("query")
            if isinstance(query, str):
                repeated_queries[query].append(tuple(e["url"] for e in self.candidates(c)))
        repeats = [v for v in repeated_queries.values() if len(v) > 1]
        source_conflicts = sum(len({r["source"] for r in rows if r["source"]}) > 1
                               for rows in self.bindings.values())
        family_wall = {arm: distribution([r["search_wall_sum_ms"] for r in family_rows if r["arm"] == arm])
                       for arm in ARMS}
        summary = {"analysis_scope": "exploratory_core_only_no_original_gold_read",
            "core_cutoff_utc": self.cutoff.isoformat() if self.cutoff else None,
            "selected_families": len(self.families), "planned_arms": len(ARMS),
            "all_trace_calls": len(self.calls), "all_calls_by_operation": dict(Counter(c["op"] for c in self.calls)),
            "all_calls_by_execution": dict(Counter(c["execution"] for c in self.calls)),
            "core_search_calls": len(core), "other_search_calls": sum(c["op"] == "search" and not c["core"] for c in self.calls),
            "core_search_by_arm": dict(Counter(c["arm"] for c in core)),
            "core_execution_counts": dict(Counter(c["execution"] for c in core)),
            "core_observed_state_count": len({c["state"] for c in core if c["state"][0] is not None}),
            "core_calls_missing_observed_state": sum(c["state"][0] is None for c in core),
            "cutoff_unclassifiable_calls": sum(c["cutoff_unknown"] for c in self.calls),
            "expected_source_mapping_available": bool(self.expected_sources) and all(self.expected_sources.values()),
            "core_unique_candidate_urls": len({e["url"] for e in core_entries}),
            "core_known_source_count": len({e["source"] for e in core_entries if e["source"]}),
            "repeated_exact_query_groups": len(repeats),
            "repeated_exact_query_calls": sum(map(len, repeats)),
            "repeated_query_groups_with_ordered_url_change": sum(len(set(v)) > 1 for v in repeats),
            "core_identity_status_counts": dict(Counter(e["status"] for e in core_entries)),
            "fetched_urls_with_source_conflicts": source_conflicts,
            "fetch_payload_revision_conflicts": sum(r["conflict"] for rows in self.bindings.values() for r in rows),
            "family_search_wall_sum_statistics_by_arm": family_wall,
            "paired_A1_A0_native_ratio_statistics": distribution([r["A1_div_A0_native_ratio"] for r in paired_rows]),
            "paired_A1_minus_A0_native_delta_statistics": distribution([r["A1_minus_A0_native_ms"] for r in paired_rows]),
            "all_fetch_wall_statistics": distribution([c["wall"] for c in fetch_calls]),
            "all_fetch_native_statistics": distribution([c["native"] for c in fetch_calls]),
            "rrf_pair_status_counts": dict(pair_states), "fetch_calls": len(fetch_calls),
            "fetched_passage_url_count": len(fetched_urls),
            "fetched_confirmed_passage_revision_count": len(confirmed_fetches),
            "returned_source_page_chunk_occurrences": returned_chunks,
            "source_page_chunk_state_unique_count": len(source_chunks),
            "unified_passage_plus_page_chunk_count": None,
            "counts_note": "Page chunks and fetched passage URLs may overlap; not summed.",
            "no_gold_quality_metrics": True,
            "timing_note": "Observed tool clocks only; no user_to_answer or causal arm speed claim.",
            "rank_note": "Absent top-3 membership is censored; no fabricated rank 4.",
            "rrf_note": "k=60; within-list URL dedup; cross-list merge only for confirmed source/revision and state binding."}
        return {"calls": call_rows, "family_arms": family_rows, "latency_by_arm_query": latency_rows,
                "rank_comparison": rank_rows, "rrf60": rrf_rows,
                "paired_native_A1_A0": paired_rows, "top3_source_scope": scope_rows,
                "summary": summary}


def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)  # None becomes empty, never numeric zero.


def quality_view(analyzer, judgments):
    """Join an independent rubric to observed candidates; never infer an answer.

    Supporting sets refer to the judge's per-family evidence keys. Unknown
    candidates remain unjudged; a fully known sufficient subset may still prove
    sufficiency. RRF is an offline candidate view, not a generated answer.
    """
    evidence = defaultdict(dict)
    for row in judgments.get("evidence_judgments", []):
        evidence[row["family_id"]][row["evidence_key"]] = row
    family_judgments = {r["family_id"]: r for r in judgments.get("family_union_judgments", [])}
    output = []

    def rating_known(row):
        grade = row.get("relevance_grade_0_2")
        return (isinstance(grade, (int, float)) and not isinstance(grade, bool)
                and grade in (0, 1, 2) and isinstance(row.get("source_scope_ok"), bool))

    for raw_family, family in analyzer.families.items():
        rubric = family_judgments.get(raw_family)
        rated = evidence.get(raw_family, {})
        by_url = {r["evidence_url"]: r for r in rated.values()
                  if isinstance(r.get("evidence_url"), str) and r["evidence_url"]}
        known_urls = {url for url, row in by_url.items() if rating_known(row)}
        supporting = []
        incomplete_supporting = False
        if rubric:
            sets = rubric.get("minimal_supporting_sets")
            incomplete_supporting = not isinstance(sets, list)
            for keys in sets if isinstance(sets, list) else []:
                if keys and all(k in rated and isinstance(rated[k].get("evidence_url"), str)
                                and rated[k]["evidence_url"] and rating_known(rated[k]) for k in keys):
                    supporting.append({rated[k]["evidence_url"] for k in keys})
                else:
                    incomplete_supporting = True

        def sufficient(urls):
            if urls is None or rubric is None:
                return None
            pool = set(urls)
            if any(required <= pool for required in supporting):
                return True
            return False if not incomplete_supporting and pool <= known_urls else None

        for arm in ARMS:
            calls = [c for c in analyzer.calls if c["core"] and c["family"] == family and c["arm"] == arm]
            lists = [analyzer.candidates(c) for c in calls]
            entries = [e for seq in lists for e in seq]
            valid = bool(calls) and all(c["execution"] == "completed" and
                isinstance(c["p"].get("results"), list) for c in calls)
            valid = valid and all(len(seq) == len(c["p"]["results"])
                                  for c, seq in zip(calls, lists))
            valid = valid and all(e["confirmed"] for e in entries)
            states = {c["state"] for c in calls}
            valid = valid and len(states) == 1 and next(iter(states))[0] is not None
            top = None
            if valid and len(lists) == 1:
                top = lists[0][:3]
            elif valid and len(lists) == 2:
                top = rrf_components(lists)[:3]
            top_urls = [e["url"] for e in top] if top is not None else None
            union = {e["url"] for e in entries} if valid else None
            partial_hit = None
            if top_urls is not None and rubric:
                matches = [u for u in top_urls if u in known_urls and
                           by_url[u]["relevance_grade_0_2"] > 0 and
                           by_url[u].get("source_scope_ok") is True]
                partial_hit = True if matches else (False if set(top_urls) <= known_urls else None)
            output.append({"family": family, "arm": arm, "search_calls": len(calls),
                "top3_view": "native" if len(calls) == 1 else "offline_rrf60" if len(calls) == 2 else "unavailable",
                "top3_candidate_count": len(top_urls) if top_urls is not None else None,
                "union_candidate_count": len(union) if union is not None else None,
                "top3_partial_or_complete_evidence_hit": partial_hit,
                "top3_complete_evidence_set": sufficient(top_urls),
                "retrieved_union_complete_evidence_set": sufficient(union),
                "unjudged_union_candidates": len(union - known_urls) if union is not None and rubric else None,
                "judge_count": judgments.get("judge_count"),
                "not_model_answer_accuracy": True})
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="pmi_private_runs_checkpoint.json")
    parser.add_argument("--queries", default="pmi_queries.json")
    parser.add_argument("--out-dir", default="pmi_numeric")
    parser.add_argument("--core-cutoff", required=True, help="UTC cutoff for frozen core search calls")
    parser.add_argument("--judgments", help="Optional independent evidence judgments; never the original gold file")
    args = parser.parse_args()
    try:
        trace = json.loads(Path(args.input).read_text(encoding="utf-8"))
        queries = json.loads(Path(args.queries).read_text(encoding="utf-8")) if args.queries else {}
        sources = queries.get("sources", {})
        expected_sources = {family["family_id"]: sources.get(family.get("source_key"), {}).get("document_id")
                            for family in queries.get("families", []) if isinstance(family, dict) and family.get("family_id")}
        if timestamp(args.core_cutoff) is None:
            raise ValueError("invalid cutoff")
        analyzer = Analyzer(trace, expected_sources=expected_sources, cutoff=args.core_cutoff)
        analyzed = analyzer.analyze()
        if args.judgments:
            judgments = json.loads(Path(args.judgments).read_text(encoding="utf-8"))
            analyzed["quality_by_family_arm"] = quality_view(analyzer, judgments)
            analyzed["summary"]["independent_judgments_joined"] = True
            analyzed["summary"]["no_gold_quality_metrics"] = False
        out = Path(args.out_dir)
        out.mkdir(parents=True, exist_ok=True)
        for name, rows in analyzed.items():
            if name == "summary":
                (out / "summary.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            else:
                write_csv(out / f"{name}.csv", rows)
        print(json.dumps({"analysis_written": True, "core_search_calls": analyzed["summary"]["core_search_calls"]}))
    except (OSError, ValueError, TypeError, KeyError) as exc:
        # Do not echo exception messages: they can contain private paths or text.
        print(json.dumps({"analysis_written": False, "error_class": type(exc).__name__}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
