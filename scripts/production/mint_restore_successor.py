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


def current_code_pins(target, service):
    """Bind code metadata to this release, preserving the verified data base."""
    from regional_knowledge.kaggle_current_registry import registry_sha256
    from regional_knowledge.kaggle_source_history_native import observer_fingerprint
    target=json.loads(json.dumps(target));service=json.loads(json.dumps(service))
    registry=registry_sha256()
    target['registry_sha256']=registry
    service['registry_sha256']=registry
    service['observer_sha256']=observer_fingerprint()
    return target,service


def successor_configs(target, service, *, run_id, writer_generation, launch_config_sha256, release_sha,
                      checkpoint=None, schema_version=None):
    """Previous verified configs with only the run-bound fields replaced.

    checkpoint is the store's verified promoted Dataset checkpoint (wire). It
    replaces the restore base consistently: target/service checkpoint, the
    target read floor and the source restore manifest reference.
    """
    target = json.loads(json.dumps(target))
    service = json.loads(json.dumps(service))
    if service.get('codec') != 'rkb.source-service.v1' or 'source_restore' not in service:
        raise ValueError('previous verified source-service restore config required')
    if target['database']['database_generation'] != service['database']['database_generation']:
        raise ValueError('previous target and source-service differ')
    if schema_version is not None:
        previous = target['database']['schema_version']
        if type(schema_version) is not int or schema_version < previous or service['database']['schema_version'] != previous:
            raise ValueError('successor schema must extend the same verified restore target')
        target['database']['schema_version'] = schema_version
        service['database']['schema_version'] = schema_version
    if checkpoint is not None and checkpoint != target.get('dataset_checkpoint'):
        restore = service['source_restore']
        if (checkpoint['database_generation'] != target['database']['database_generation']
                or target.get('dataset_checkpoint') != restore.get('dataset_checkpoint')
                or checkpoint['base_sequence'] < target['dataset_checkpoint']['base_sequence']):
            raise ValueError('promoted checkpoint must extend the same restore base')
        manifest = checkpoint['manifest']
        restore['manifest'] = dict(key=restore['manifest']['key'].rsplit('/', 1)[0] + '/' + manifest['sha256'],
                                   last_sequence=checkpoint['base_sequence'], length=manifest['size'],
                                   sha256=manifest['sha256'])
        restore['dataset_checkpoint'] = checkpoint
        target['dataset_checkpoint'] = checkpoint
        target['min_data_seq'] = checkpoint['base_sequence']
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


def verified_latest_checkpoint(queue):
    """The store's promoted checkpoint (latest_json is written only after native verify)."""
    with queue.connect(write=False) as db:
        rows = db.execute('select checkpoint_json,latest_json from rkb_dataset_recovery_store').fetchall()
    if len(rows) != 1:
        raise ValueError('exactly one Dataset recovery store required')
    return json.loads(rows[0][1] or rows[0][0])


def write_private(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')))
    path.chmod(0o600)


def previous_service_config(directory):
    """Select the run config, excluding backups that share its filename prefix."""
    candidates = []
    for path in directory.glob('source-service-*.json'):
        value = json.loads(path.read_text())
        if path.name == f"source-service-{value.get('run_id')}.json":
            candidates.append(value)
    if len(candidates) != 1:
        raise ValueError('exactly one previous run source-service config required')
    return candidates[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous', required=True, type=Path, help='previous verified-restore-* directory')
    parser.add_argument('--release-sha', required=True)
    parser.add_argument('--broker', required=True)
    parser.add_argument('--docker-image', required=True)
    parser.add_argument('--recover-not-sent-startup', action='store_true',
                        help='reopen a BLOCKED demand only if every exhausted attempt was provably not sent')
    args = parser.parse_args(argv)
    source = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location('rkb_controller', source / 'scripts/production/bge_kaggle_controller.py')
    controller = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller)
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.kaggle_launch_config import load_target_launch
    from regional_knowledge.kaggle_schema_migrate import latest_schema_version
    queue = BgeQueue(os.environ['RKB_BGE_QUEUE_PATH'])
    if args.recover_not_sent_startup:
        print(json.dumps({'recovered_startup': queue.recover_not_sent_startup()}))
    run_id, writer_generation = pending_successor(queue)
    checkpoint = verified_latest_checkpoint(queue)
    previous_target = json.loads((args.previous / 'target.json').read_text())
    service = previous_service_config(args.previous)
    previous_target,service=current_code_pins(previous_target,service)
    out = args.previous.parent / f'verified-restore-{args.release_sha[:12]}-{run_id[:8]}'
    out.mkdir(mode=0o700)
    target, _ = successor_configs(previous_target, service, run_id=run_id, writer_generation=writer_generation,
                                  launch_config_sha256='0' * 64, release_sha=args.release_sha, checkpoint=checkpoint,
                                  schema_version=latest_schema_version())
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
    _, service = successor_configs(previous_target, service, run_id=run_id, writer_generation=writer_generation,
                                   launch_config_sha256=expected['hashes']['launch_config'], release_sha=args.release_sha,
                                   checkpoint=checkpoint, schema_version=latest_schema_version())
    service_path = out / f'source-service-{run_id}.json'
    write_private(service_path, service)
    print(json.dumps({'run_id': run_id, 'target': str(out / 'target.json'), 'source_service': str(service_path)}))


if __name__ == '__main__':
    main()
