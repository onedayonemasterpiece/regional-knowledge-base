"""Build one stateless warm handler with an isolated updated graph implementation."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

WIRE_CLASSES=('SourceGraphReadRequest','SourceGraphReadResult')
MAX_BYTES=512*1024


def bundle(query_source,graph_source,baseline_graph_source):
    graph=ast.parse(graph_source);baseline=ast.parse(baseline_graph_source)
    def wire(tree):
        return {node.name:ast.dump(node,include_attributes=False) for node in tree.body
            if isinstance(node,ast.ClassDef) and node.name in WIRE_CLASSES}
    if set(wire(graph))!=set(WIRE_CLASSES) or wire(graph)!=wire(baseline):
        raise ValueError('warm_graph_wire_abi_changed')
    graph.body=[node for node in graph.body if not isinstance(node,ast.ClassDef) or node.name not in WIRE_CLASSES]
    query=ast.parse(query_source)
    replaced=0
    class IsolateGraph(ast.NodeTransformer):
        def visit_ImportFrom(self,node):
            nonlocal replaced
            if node.level==1 and node.module=='kaggle_source_graph_read' and any(
                    item.name=='execute_source_graph_read' for item in node.names):
                if len(node.names)!=1 or node.names[0].asname is not None:
                    raise ValueError('warm_graph_import_changed')
                replaced+=1
                return ast.copy_location(ast.parse(
                    "execute_source_graph_read = _warm_graph['execute_source_graph_read']").body[0],node)
            return node
    query=IsolateGraph().visit(query)
    if replaced!=1:raise ValueError('warm_graph_import_missing')
    query.body=[node for node in query.body if not isinstance(node,ast.ImportFrom) or node.module!='__future__']
    source=("from __future__ import annotations\n"
        "from .kaggle_source_graph_read import SourceGraphReadRequest, SourceGraphReadResult\n"
        "_warm_graph = dict(__name__=__name__, __package__=__package__, __file__=__file__, "
        "SourceGraphReadRequest=SourceGraphReadRequest, SourceGraphReadResult=SourceGraphReadResult)\n"
        f"exec(compile({ast.unparse(graph)!r}, '<warm-source-graph>', 'exec'), _warm_graph)\n"
        +ast.unparse(ast.fix_missing_locations(query))+"\n")
    if len(source.encode())>MAX_BYTES:raise ValueError('warm_handler_budget')
    compile(source,'<warm-handler>','exec')
    return source


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reviewed-commit',required=True)
    parser.add_argument('--baseline-commit',required=True)
    parser.add_argument('--output-dir',required=True,type=Path)
    args=parser.parse_args()
    def git(*parts):return subprocess.check_output(['git',*parts],text=True).strip()
    commit=git('rev-parse',args.reviewed_commit+'^{commit}')
    baseline=git('rev-parse',args.baseline_commit+'^{commit}')
    subprocess.run(['git','merge-base','--is-ancestor',commit,'origin/main'],check=True)
    root='src/regional_knowledge/'
    def read(ref,name):return git('show',ref+':'+root+name+'.py')
    source=bundle(read(commit,'kaggle_worker_query'),read(commit,'kaggle_source_graph_read'),
        read(baseline,'kaggle_source_graph_read'))
    output=args.output_dir.resolve()
    if not (output/'.artifact.json').is_file():raise ValueError('managed_artifact_directory_required')
    content=source.encode();digest=hashlib.sha256(content).hexdigest()
    target=output/('handler-'+digest+'.blob')
    with target.open('xb') as stream:stream.write(content)
    target.chmod(0o600)
    manifest=dict(reviewed_commit=commit,baseline_commit=baseline,handler_sha256=digest,
        size=len(content),file=target.name)
    (output/'warm-handler-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(manifest))


if __name__=='__main__':main()
