"""Verify intentional, bounded CPU scheduling for RAG services."""
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def _weight(path):
    text=(ROOT/path).read_text()
    weights=[line.split("=",1)[1] for line in text.splitlines() if line.startswith("CPUWeight=")]
    assert len(weights)==1
    value=int(weights[0])
    assert 1<=value<=10000
    return value

def test_query_can_use_available_cores_and_maintains_one_cpu_ceiling():
    text=(ROOT/"ops/regional-knowledge-bge-query.service").read_text()
    assert "CPUQuota=100%" in text
    assert "CPUAffinity=" not in text
    assert "MemoryMax=1536M" in text
    assert "MemorySwapMax=0" in text
    assert _weight("ops/regional-knowledge-bge-query.service")==300

def test_interactive_cpu_priority_above_durable_background_indexing():
    interactive=_weight("ops/regional-knowledge-base-cpu-priority.conf")
    background=_weight("ops/regional-knowledge-indexing-cpu-priority.conf")
    query=_weight("ops/regional-knowledge-bge-query.service")
    assert 50<=background<100<interactive<query<=350
