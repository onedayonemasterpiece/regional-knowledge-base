"""One-time idempotent RKB original migration; import from authorized owner runtime.

After reviewed native rollout, in the existing owner importer process:
    from scripts.migrate_telegram_originals import migrate
    results = await migrate(importer.originals, access.context)

Never execute against live as an unauthenticated standalone CLI.
"""
async def migrate(originals, owner_context):
    actor, _ = await originals._authorize(owner_context)
    with originals.queue.connect(write=False) as db:
        rows = db.execute(
            "SELECT ingestion_id FROM rkb_original_intakes "
            "WHERE actor=? AND receipt_json IS NOT NULL AND telegram_locator_json IS NULL "
            "ORDER BY ingestion_id", (actor,)).fetchall()
    results = []
    for row in rows:
        locator = await originals.migrate_s3_original_to_telegram(
            owner_context, row['ingestion_id'])
        results.append((row['ingestion_id'], locator.wire()))
    return results
