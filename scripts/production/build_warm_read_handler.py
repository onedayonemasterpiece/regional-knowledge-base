"""Build one stateless warm handler with isolated reviewed read implementations."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

WIRE_CLASSES=('SourceGraphReadRequest','SourceGraphReadResult')
MAX_BYTES=512*1024


def bundle(query_source,graph_source,baseline_graph_source,executor_source=None,baseline_executor_source=None,*,case_source=None):
    graph=ast.parse(graph_source);baseline=ast.parse(baseline_graph_source)
    def wire(tree):
        return {node.name:ast.dump(node,include_attributes=False) for node in tree.body
            if isinstance(node,ast.ClassDef) and node.name in WIRE_CLASSES}
    if set(wire(graph))!=set(WIRE_CLASSES) or wire(graph)!=wire(baseline):
        raise ValueError('warm_graph_wire_abi_changed')
    graph.body=[node for node in graph.body if not isinstance(node,ast.ClassDef) or node.name not in WIRE_CLASSES]
    executor_prefix=''
    if executor_source is not None:
        executor=ast.parse(executor_source)
        baseline_executor=ast.parse(baseline_executor_source)
        def tokenizer(tree):
            return [ast.dump(node,include_attributes=False) for node in tree.body
                if isinstance(node,ast.ClassDef) and node.name=='PinnedTokenizer']
        if len(tokenizer(executor))!=1 or tokenizer(executor)!=tokenizer(baseline_executor):
            raise ValueError('warm_query_tokenizer_abi_changed')
        executor.body=[node for node in executor.body
            if not isinstance(node,ast.ClassDef) or node.name!='PinnedTokenizer']
        executor_prefix=(
            'from .kaggle_query import PinnedTokenizer\n'
            '_warm_query = dict(__name__=__name__, __package__=__package__, '
            '__file__=__file__, PinnedTokenizer=PinnedTokenizer)\n'
            f"exec(compile({ast.unparse(executor)!r}, '<warm-query>', 'exec'), _warm_query)\n")
    case_prefix=(
        '_warm_case = dict(__name__=__name__, __package__=__package__, __file__=__file__)\n'
        f"exec(compile({case_source!r}, '<warm-case>', 'exec'), _warm_case)\n"
        if case_source is not None else '')
    query=ast.parse(query_source)
    replaced=0
    class IsolateGraph(ast.NodeTransformer):
        def visit_ImportFrom(self,node):
            nonlocal replaced
            if case_source is not None and node.level==1 and node.module=='kaggle_case_read':
                return ast.copy_location(ast.parse(
                    "execute_case_read = _warm_case['execute_case_read']").body[0],node)
            if executor_source is not None and node.level==1 and node.module=='kaggle_query':
                executors=[item for item in node.names if item.name in ('execute_query','execute_passage_read')]
                if not executors:return node
                node.names=[item for item in node.names if item not in executors]
                assignments=[ast.copy_location(ast.parse(
                    f"{item.asname or item.name} = _warm_query[{item.name!r}]").body[0],node) for item in executors]
                return ([node] if node.names else [])+assignments
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
        +executor_prefix+case_prefix+
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
        read(baseline,'kaggle_source_graph_read'),read(commit,'kaggle_query'),read(baseline,'kaggle_query'),
        case_source=read(commit,'kaggle_case_read'))
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
