"""Integrated historical closure selects current schema without dropping old replay handlers."""
from types import SimpleNamespace
import pytest
from test_kaggle_native_source_importer import native_intake, target, auth, people
from regional_knowledge.kaggle_apply import HandlerRegistry
from regional_knowledge.kaggle_domain_bindings import current_handler, DOMAIN_REVISIONS, DOMAIN_V2_REVISIONS
from regional_knowledge.kaggle_importer import KaggleSourceImporter
from regional_knowledge.source_activation import ACTIVATION_REVISIONS

@pytest.mark.asyncio
@pytest.mark.parametrize("selected",[DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS,ACTIVATION_REVISIONS,None])
async def test_mixed_retained_registry_selects_exact_current_import_profile(native_intake,selected):
    s=native_intake
    retained=list(s.spool.registry.handlers.values())
    retained.extend(current_handler("source.import.v4",check_sha256="a"*64,apply_sha256="b"*64,revisions=v)
                    for v in (DOMAIN_REVISIONS,DOMAIN_V2_REVISIONS))
    s.spool.registry=HandlerRegistry(retained)
    config=SimpleNamespace(**vars(s.startup.config))
    config.history_adapter=SimpleNamespace()
    if selected is not None:
        config.history_adapter.database=SimpleNamespace(**dict(zip(
            ("schema_revision","function_revision","executor_revision"),selected.wire())))
    startup=SimpleNamespace(config=config,authority=s.startup.authority,backend=s.startup.backend)
    if selected is None:
        with pytest.raises(TypeError,match="exact future source"):
            KaggleSourceImporter(startup,originals=s.originals,tokenizer=s.importer.tokenizer)
    else:
        importer=KaggleSourceImporter(startup,originals=s.originals,tokenizer=s.importer.tokenizer)
        assert importer.import_revisions==selected
        assert (importer.native_intake is not None)==(selected==ACTIVATION_REVISIONS)
        assert len(s.spool.registry.handlers)==len(retained)
