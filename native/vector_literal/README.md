# RKB-Linza: isolated Rust vector-literal experiment (NOT PRODUCTION)

This directory contains one optional, in-process `cdylib` micro-kernel for formatting the Kaggle-local BGE-M3 1024-value vector as pgvector text. It does **not** implement inference, tokenizer, PostgreSQL, authentication, WSS, job scheduling, retention, provider access, or production packaging. Toolchain is pinned to Rust **1.85.1**; no Cargo dependencies. Compilation happens only in GitHub-hosted Actions.

Pinned starting main commit: `f2c6ad0ba9df283a08240ee31de5ae771c2820eb`.
Verified Python source call sites at that commit: `kaggle_query.py:163-169` validates/discards a literal, and `kaggle_search.py:58-64,189-196` validates/formats it again. `bge_contract.py:11-19` defines strict 1024 length, finite and L2 tolerance of 0.002.

The benchmark reads **unchanged Python function bodies** directly from these pinned tracked files through Python AST, avoiding unrelated PG/model imports:
* A: call original `vector_literal` twice, as the current two call sites do;
* B: call the identical original function once, estimating value of eliminating duplicate work without native code;
* C: Python list and strict type check, Python-float-to-contiguous-f64 conversion, ctypes FFI into Rust, UTF-8 output copy and decode. Output, exception class, and exact exception message must match A/B for the parity suite. Rust sees no secrets, permissions, binary model tensors, nor network.

Parity is required **before** performance. Exact floating representation (signed zero, Python exponent window, integer-as-float, round-trip decimal), invalid errors and nonfinite/norm checks have no relaxed tolerance. Time results are synthetic Linux microbenchmarks; they are not end-to-end warm MCP latency evidence. Rust is not approved for deployment unless it beats a fair one-pass Python solution with end-to-end evidence and complete lifecycle/ABI compatibility.

The isolated workflow `.github/workflows/rkb-native-vector-prototype.yml` is triggered **only** on the experiment branch. It records binary SHA-256, p50/p95 per-call wall/CPU, resident/peak RSS, and cold `ctypes.CDLL` load cost; it uploads reports/logs. No datasets, Kaggle notebook, PostgreSQL service, BGE weights, third-party crates, credentials or existing required-check changes.
