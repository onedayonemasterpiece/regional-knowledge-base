"""Check that RKB reserves CPU for MCP without hosting BGE inference."""
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def _weight(path):
    text=(ROOT/path).read_text()
    weights=[line.split("=",1)[1] for line in text.splitlines()
             if line.startswith("CPUWeight=")]
    assert len(weights)==1
    value=int(weights[0])
    assert 1<=value<=10000
    return value


def test_interactive_mcp_cpu_priority_above_background_indexing():
    interactive=_weight("ops/regional-knowledge-base-cpu-priority.conf")
    background=_weight("ops/regional-knowledge-indexing-cpu-priority.conf")
    assert 50<=background<100<interactive<=350


def test_no_local_bge_service_or_model_loader_in_deployable_source():
    assert not (ROOT/"ops/regional-knowledge-bge-query.service").exists()
    assert not (ROOT/"src/regional_knowledge/bge_query_service.py").exists()
    assert not (ROOT/"src/regional_knowledge/local_bge.py").exists()
