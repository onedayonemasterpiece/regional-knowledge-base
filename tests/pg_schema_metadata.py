"""Integer metadata for isolated fixtures that install historical SQL prefixes.

Production fresh/restore always runs the complete migration bundle. These
fixtures deliberately exercise an earlier function set without running models.
"""
import re
from regional_knowledge.kaggle_apply import initialize_database
from regional_knowledge.kaggle_schema_migrate import latest_schema_version


async def initialize_fixture_database(connection,**kwargs):
    revision=kwargs.pop('revisions',None)
    prior=re.fullmatch(r'rkb-v4-sql001-([0-9]{3})-v1',getattr(revision,'schema',''))
    version=kwargs.pop('schema_version',int(prior[1]) if prior else latest_schema_version())
    if type(version) is not int or version<1:raise ValueError('fixture integer schema version required')
    async with connection.transaction():
        await connection.execute('CREATE TABLE IF NOT EXISTS rkb_v4.schema_migrations(version integer PRIMARY KEY)')
        await connection.execute('INSERT INTO rkb_v4.schema_migrations SELECT generate_series(1,%s) ON CONFLICT DO NOTHING',(version,))
        legacy=await (await connection.execute("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='rkb_v4' AND table_name='recovery_state' AND column_name='schema_revision')")).fetchone()
        if legacy==(True,):
            await connection.execute("ALTER TABLE rkb_v4.recovery_state ALTER COLUMN schema_revision SET DEFAULT 's1',ALTER COLUMN function_revision SET DEFAULT 'f1',ALTER COLUMN executor_revision SET DEFAULT 'e1'")
    await initialize_database(connection,**kwargs)
