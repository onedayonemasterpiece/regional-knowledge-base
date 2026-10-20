"""Explicit future source-import owners over the existing queue/storage authority."""
from dataclasses import dataclass
from pathlib import Path
import re

from .kaggle_import_tokenizer import HfImportTokenCounter, TokenizerProcessSpec
from .native_snapshot_transfer import origin


@dataclass(frozen=True)
class SourceImportOwners:
    launch_config_sha256: str
    parser_sha256: str
    allowed_origins: tuple[str, ...]
    tokenizer_directory: str
    tokenizer_process: TokenizerProcessSpec

    def __post_init__(self):
        if (any(type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None
                for value in (self.launch_config_sha256,self.parser_sha256))
                or type(self.allowed_origins) is not tuple or not 1<=len(self.allowed_origins)<=4
                or len(set(self.allowed_origins)) != len(self.allowed_origins)
                or any(origin(value)!=value for value in self.allowed_origins)
                or type(self.tokenizer_directory) is not str or not Path(self.tokenizer_directory).is_absolute()
                or not 1<=len(self.tokenizer_directory)<=1024 or type(self.tokenizer_process) is not TokenizerProcessSpec):
            raise ValueError('explicit pinned source import owners required')

    def wire(self):
        return dict(codec='rkb.source-import-owners.v1',launch_config_sha256=self.launch_config_sha256,
            parser_sha256=self.parser_sha256,allowed_origins=list(self.allowed_origins),
            tokenizer_directory=self.tokenizer_directory,tokenizer_process=self.tokenizer_process.wire())

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!={'codec','launch_config_sha256','parser_sha256','allowed_origins',
                'tokenizer_directory','tokenizer_process'} or value['codec']!='rkb.source-import-owners.v1':
            raise ValueError('exact source import owner config required')
        if type(value['allowed_origins']) is not list:
            raise ValueError('exact source import origins required')
        return cls(value['launch_config_sha256'],value['parser_sha256'],tuple(value['allowed_origins']),
            value['tokenizer_directory'],TokenizerProcessSpec.from_wire(value['tokenizer_process']))

    def compose(self,*,queue,spool,participants,object_store,writer):
        # No provider, download, subprocess or model action during construction.
        from .kaggle_public_backend import current_binding
        from .original_intake_controller import OriginalIntakeController
        def current(binding):
            try:
                return current_binding(queue)==binding and writer.current(
                    database_generation=binding.database_generation,writer_generation=writer.writer_generation)
            except Exception:
                return False
        originals=OriginalIntakeController(queue=queue,spool=spool,participants=participants,
            object_store=object_store,binding_current=current,current_binding=lambda:current_binding(queue),
            allowed_origins=frozenset(self.allowed_origins),max_bytes=2*1024*1024)
        return dict(import_originals=originals,
            import_tokenizer=HfImportTokenCounter(self.tokenizer_directory,process=self.tokenizer_process),
            import_parser_sha256=self.parser_sha256,import_launch_config_sha256=self.launch_config_sha256)
