"""Mint the per-run restore configs for the current successor run.

Operator step used for v105: with the controller stopped, take the pending
current run, give it a fresh writer generation and persist its launch intent
through the controller's own worker_source (same release bytes the controller
will push), then copy the previous verified target/source-service with only the
run-bound fields changed. No provider call, no writer mutation: the service
moves the writer in begin_restore after the new run attaches.
"""
import argparse
import importlib.util
import json
import os
import uuid
from pathlib import Path

RUN_FIELDS = ('run_id', 'writer_generation', 'launch_config_sha256', 'operation_key')


def successor_configs(target, service, *, run_id, writer_generation, launch_config_sha256, release_sha):
    """Previous verified configs with only the run-bound fields replaced."""
    target = json.loads(json.dumps(target))
    service = json.loads(json.dumps(service))
    if service.get('codec') != 'rkb.source-service.v1' or 'source_restore' not in service:
        raise ValueError('previous verified source-service restore config required')
    if target['database']['database_generation'] != service['database']['database_generation']:
        raise ValueError('previous target and source-service differ')
    target['database']['writer_generation'] = writer_generation
    service['run_id'] = run_id
    service['database']['writer_generation'] = writer_generation
    service['source_restore']['launch_config_sha256'] = launch_config_sha256
    if 'source_import' in service:
        service['source_import']['launch_config_sha256'] = launch_config_sha256
    service['source_restore']['operation_key'] = f'rkb-restore-{release_sha[:7]}-{run_id[:8]}'
    return target, service


def pending_successor(queue):
    """The current run must be a fresh pending launch and the writer settled."""
    queue.maintenance()
    with queue.connect(write=False) as db:
        run_id = db.execute('select current_run from control where id=1').fetchone()[0]
        run = db.execute('select status,launch_state from runs where id=?', (run_id,)).fetchone() if run_id else None
        used = {r[0] for r in db.execute('select writer_generation from rkb_writer_generations')}
        writer = db.execute('select active,publication_json from rkb_writer_control where id=1').fetchone()
    if run is None or tuple(run) != ('starting', 'pending'):
        raise ValueError('a pending current successor run is required (is the controller stopped?)')
    if writer is None or writer[0] or writer[1] is not None:
        raise ValueError('previous writer must be retired and settled')
    writer_generation = str(uuid.uuid4())
    if writer_generation in used:
        raise ValueError('writer generation collision')
    return run_id, writer_generation


def write_private(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))
    path.chmod(0o600)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous', required=True, type=Path, help='previous verified-restore-* directory')
    parser.add_argument('--release-sha', required=True)
    parser.add_argument('--broker', required=True)
    parser.add_argument('--docker-image', required=True)
    args = parser.parse_args(argv)
    source = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location('rkb_controller', source / 'scripts/production/bge_kaggle_controller.py')
    controller = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller)
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.kaggle_launch_config import load_target_launch
    queue = BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    run_id, writer_generation = pending_successor(queue)
    previous_service = next(args.previous.glob('source-service-*.json'))
    target = json.loads((args.previous / 'target.json').read_text())
    service = json.loads(previous_service.read_text())
    out = args.previous.parent / f'verified-restore-{args.release_sha[:12]}-{run_id[:8]}'
    out.mkdir(mode=0o700)
    target, _ = successor_configs(target, service, run_id=run_id, writer_generation=writer_generation,
                                  launch_config_sha256='0' * 64, release_sha=args.release_sha)
    write_private(out / 'target.json', target)
    token = (queue.path.parent / 'bge-worker-credentials' / run_id).read_text()
    template = (source / 'scripts/production/bge_kaggle_worker.py').read_text()
    # Same call the controller makes before push: persists nonce and runtime
    # expectation idempotently, so its later compose is byte-identical.
    controller.worker_source(template, queue, run_id, token, args.broker, transport='wss',
                             target=load_target_launch(str(out / 'target.json')), provider_image=args.docker_image)
    del token
    with queue.connect(write=False) as db:
        expected = json.loads(queue._runtime_row(db, run_id)['expected_json'])
    _, service = successor_configs(target, service, run_id=run_id, writer_generation=writer_generation,
                                   launch_config_sha256=expected['hashes']['launch_config'], release_sha=args.release_sha)
    service_path = out / f'source-service-{run_id}.json'
    write_private(service_path, service)
    print(json.dumps({'run_id': run_id, 'target': str(out / 'target.json'), 'source_service': str(service_path)}))


if __name__ == '__main__':
    main()
