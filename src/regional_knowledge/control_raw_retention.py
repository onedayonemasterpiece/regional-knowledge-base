"""Retire redundant local bodies, preserving the existing ordered journal identity."""


def initialize_nullable_raw(db):
    if not next(r[3] for r in db.execute('PRAGMA table_info(rkb_control_operations)')
                if r[1] == 'command_bytes'):
        return
    # Supported transactional SQLite rebuild; indexes and triggers stay identical.
    schema = db.execute("SELECT sql FROM sqlite_master WHERE name='rkb_control_operations'").fetchone()[0]
    objects = list(db.execute("SELECT name,sql FROM sqlite_master WHERE tbl_name='rkb_control_operations' "
                              "AND type IN ('index','trigger') AND sql IS NOT NULL"))
    columns = ','.join(r[1] for r in db.execute('PRAGMA table_info(rkb_control_operations)'))
    db.execute(schema.replace('rkb_control_operations','rkb_control_operations_raw',1)
                     .replace('command_bytes BLOB NOT NULL','command_bytes BLOB'))
    db.execute(f'INSERT INTO rkb_control_operations_raw({columns}) SELECT {columns} FROM rkb_control_operations')
    db.execute('DROP TABLE rkb_control_operations')
    db.execute('ALTER TABLE rkb_control_operations_raw RENAME TO rkb_control_operations')
    for _, sql in objects:
        db.execute(sql)


def protected_raw(db):
    """One metadata fold per intake; current and missing-SEM stages keep originals."""
    from .source_page_progress import progress_packet, progress_columns
    protected = set()
    for state in db.execute('SELECT * FROM rkb_native_source_intake_state'):
        if state['pending_operation_key']:
            protected.add(state['pending_operation_key'])
        stages = {}; text_batches = set(); semantic_batches = set(); published = set(); semantic = set()
        latest = True
        for item in db.execute('SELECT operation_key,apply_state,'+progress_columns()+
                " FROM rkb_control_operations WHERE source_id=? AND person_id=? "
                "AND (operation_key LIKE ? OR operation_key LIKE ?) ORDER BY sequence DESC LIMIT 10000",
                (state['source_id'],state['person_id'],'source-stage:'+state['ingestion_id']+':%',
                 'source-packet:'+state['ingestion_id']+':%')):
            packet, stage_sha = progress_packet(item)
            if packet.get('ingestion_id') != state['ingestion_id']: continue
            if item['operation_key'].startswith('source-stage:'):
                pages = {p['physical_page_index'] for p in packet.get('model_payload',{}).get('pages',[])}
                if latest or packet.get('batch_index') in (state['batch_count'],state['rejected_batch_index']):
                    protected.add(item['operation_key'])
                latest = False
                stages[item['operation_key']] = pages
                if item['apply_state']=='APPLIED':
                    pair = (packet.get('batch_index'),stage_sha)
                    if pair in text_batches: published.update(pages)
                    if pair in semantic_batches: semantic.update(pages)
            elif item['apply_state']=='APPLIED':
                pair = (packet.get('batch_index'),packet.get('last_batch_sha256'))
                if packet.get('component')=='text': text_batches.add(pair)
                elif packet.get('component')=='semantic': semantic_batches.add(pair)
        protected.update(key for key,pages in stages.items() if pages & (published-semantic))
    return protected
