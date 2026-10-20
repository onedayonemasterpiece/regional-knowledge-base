"""Bounded original intake metadata; no bytes, URLs, storage or authority."""
from __future__ import annotations

from dataclasses import dataclass,asdict
import hashlib
import json
from uuid import UUID

PHASES=frozenset({'original_acquire','original_archive','original_verify','original_pages'})

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(value).hexdigest()

class OriginalIntakeError(ValueError):pass

def text(value,maximum=256):
    if type(value) is not str or not 1<=len(value.encode())<=maximum or any(ord(c)<32 for c in value):
        raise OriginalIntakeError('original_metadata_invalid')
    return value

def hash_value(value):
    if type(value) is not str or len(value)!=64 or set(value)-set('0123456789abcdef'):
        raise OriginalIntakeError('original_hash_invalid')
    return value

def uuid_value(value):
    try:
        if type(value) is not str or str(UUID(value))!=value:raise ValueError
    except ValueError:raise OriginalIntakeError('original_identity_invalid') from None
    return value

@dataclass(frozen=True)
class OriginalIntakeSpec:
    source_id:str
    ingestion_id:str
    request_id:str
    file_id:str
    revision:int=1
    file_name:str|None=None
    mime_type:str|None=None
    max_bytes:int=128*1024*1024
    expected_sha256:str|None=None
    expected_size:int|None=None
    max_pages:int=32

    def __post_init__(self):
        for value in (self.source_id,self.ingestion_id,self.request_id):uuid_value(value)
        text(self.file_id)
        if self.file_name is not None:text(self.file_name,512)
        if self.mime_type is not None:text(self.mime_type,128)
        if type(self.revision) is not int or self.revision!=1 or type(self.max_bytes) is not int or not 1<=self.max_bytes<=512*1024*1024:
            raise OriginalIntakeError('original_bounds_invalid')
        if self.expected_sha256 is not None:hash_value(self.expected_sha256)
        if self.expected_size is not None and (type(self.expected_size) is not int or not 1<=self.expected_size<=self.max_bytes):
            raise OriginalIntakeError('original_bounds_invalid')
        if type(self.max_pages) is not int or not 1<=self.max_pages<=32:raise OriginalIntakeError('original_page_bound')

    def wire(self):return asdict(self)
    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__):raise OriginalIntakeError('original_spec_invalid')
        return cls(**value)

@dataclass(frozen=True)
class VerifiedOriginalReceipt:
    source_id:str
    ingestion_id:str
    request_id:str
    revision:int
    sha256:str
    size:int
    key:str
    native_version:str
    format:str
    page_count:int
    parser_sha256:str
    acquisition_sha256:str
    native_verification_sha256:str

    def __post_init__(self):
        for value in (self.source_id,self.ingestion_id,self.request_id):uuid_value(value)
        for value in (self.sha256,self.parser_sha256,self.acquisition_sha256,self.native_verification_sha256):hash_value(value)
        text(self.key,1024);text(self.native_version,512)
        if ('://' in self.key or any(p in ('','.','..') for p in self.key.split('/'))
                or self.format not in ('pdf','djvu') or type(self.page_count) is not int or not 1<=self.page_count<=2500
                or type(self.size) is not int or not 1<=self.size<=512*1024*1024 or self.revision!=1):
            raise OriginalIntakeError('original_receipt_invalid')

    def wire(self):return {'codec':'rkb.original-receipt.v1',**asdict(self)}
    @property
    def receipt_sha256(self):return digest(canonical(self.wire()))
    @property
    def original(self):return dict(sha256=self.sha256,size=self.size,ref='archive:'+self.key,receipt_sha256=self.receipt_sha256)
    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!={'codec',*cls.__dataclass_fields__} or value['codec']!='rkb.original-receipt.v1':
            raise OriginalIntakeError('original_receipt_invalid')
        return cls(**{k:v for k,v in value.items() if k!='codec'})

@dataclass(frozen=True)
class OriginalIntakeStatus:
    ingestion_id:str
    state:str
    receipt:VerifiedOriginalReceipt|None=None
    reason:str|None=None

    @property
    def verified(self):return self.receipt is not None and self.state=='verified'


@dataclass(frozen=True)
class NativePagesProof:
    original_receipt_sha256:str
    page_bytes:tuple[bytes,...]

    def __post_init__(self):
        hash_value(self.original_receipt_sha256)
        if (type(self.page_bytes) is not tuple or not 1<=len(self.page_bytes)<=32
                or any(type(raw) is not bytes or not 1<=len(raw)<=120*1024 for raw in self.page_bytes)):
            raise OriginalIntakeError('original_page_proof_bound')
    @property
    def pages(self):return tuple(json.loads(raw) for raw in self.page_bytes)
    @property
    def sha256(self):return digest(canonical(dict(original_receipt_sha256=self.original_receipt_sha256,
        pages=list(self.pages))))
