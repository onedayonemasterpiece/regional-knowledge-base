"""Bounded original intake metadata; no bytes, URLs, storage or authority."""
from __future__ import annotations

from dataclasses import dataclass,asdict
import base64
import hashlib
import json
from uuid import UUID
import math

PHASES=frozenset({'original_acquire','original_archive','original_verify','original_pages'})

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(value).hexdigest()

class OriginalIntakeError(ValueError):pass

TERMINAL_REJECTIONS=frozenset({'original_djvu_pinned_parser_required','original_djvu_invalid','original_format_unsupported',
    'original_expected_bytes_mismatch','original_page_count_limit','original_pdf_invalid',
    'original_page_geometry_invalid','original_render_byte_bound'})

class OriginalIntakeRejected(OriginalIntakeError):
    def __init__(self,code):
        if code not in TERMINAL_REJECTIONS:raise ValueError('fixed original rejection required')
        self.code=code;super().__init__(code)

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
    max_pages:int=2500

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
        if type(self.max_pages) is not int or not 1<=self.max_pages<=2500:raise OriginalIntakeError('original_page_bound')

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
                or any(type(raw) is not bytes or not 1<=len(raw)<=2*1024*1024 for raw in self.page_bytes)
                or sum(map(len,self.page_bytes))>8*1024*1024):
            raise OriginalIntakeError('original_page_proof_bound')
    @property
    def pages(self):return tuple(json.loads(raw) for raw in self.page_bytes)
    @property
    def sha256(self):return digest(canonical(dict(original_receipt_sha256=self.original_receipt_sha256,
        pages=list(self.pages))))


def issued_image_receipt(receipt, page):
    """Small exact image provenance; never certifies model text as native text.

    Called only after the controller verifies the authenticated page response
    and actual derivative bytes. Native-text continuation is deliberately absent.
    """
    if type(receipt) is not VerifiedOriginalReceipt or type(page) is not dict:
        raise OriginalIntakeError('original_image_receipt_invalid')
    from uuid import uuid5
    index=page.get('physical_page_index')
    if (type(index) is not int or not 0<=index<receipt.page_count
            or page.get('page_id')!=str(uuid5(UUID(receipt.source_id),f'page:1:{index}'))
            or page.get('original_sha256')!=receipt.sha256
            or page.get('parser_sha256')!=receipt.parser_sha256
            or page.get('mime_type')!='image/jpeg'):
        raise OriginalIntakeError('original_image_receipt_invalid')
    for key in ('width','height'):
        if type(page.get(key)) not in (int,float) or not math.isfinite(page[key]) or not 0<page[key]<=100000:
            raise OriginalIntakeError('original_image_receipt_invalid')
    hash_value(page.get('image_sha256'))
    render=page.get('preview_render')
    if type(render) is not dict or len(canonical(render))>4096:
        raise OriginalIntakeError('original_image_receipt_invalid')
    value=dict(codec='rkb.issued-source-image.v1',original_receipt_sha256=receipt.receipt_sha256,
        page_id=page['page_id'],physical_page_index=index,original_sha256=receipt.sha256,
        parser_sha256=receipt.parser_sha256,width=page['width'],height=page['height'],
        image_sha256=page['image_sha256'],mime_type=page['mime_type'],preview_render=render)
    return dict(value,source_image_receipt_sha256=digest(canonical(value)))


def encode_page_cursor(page,cursor):
    data=canonical(dict(page=page,**cursor))
    return 'pc1.'+base64.urlsafe_b64encode(data).decode().rstrip('=')


def decode_page_cursor(value):
    if value is None:return 0,dict(block=0,offset=0,text=0)
    if type(value) is not str or not 1<=len(value)<=512:raise OriginalIntakeError('original_page_cursor_invalid')
    if value.isascii() and value.isdecimal():return int(value),dict(block=0,offset=0,text=0)
    try:
        if not value.startswith('pc1.'):raise ValueError
        encoded=value[4:];raw=base64.urlsafe_b64decode(encoded+'='*((-len(encoded))%4));data=json.loads(raw)
        if (type(data) is not dict or set(data)!={'page','block','offset','text'} or canonical(data)!=raw
                or any(type(v) is not int or not 0<=v<2**31 for v in data.values())
                or encode_page_cursor(data['page'],{k:data[k] for k in ('block','offset','text')})!=value):raise ValueError
        return data['page'],{k:data[k] for k in ('block','offset','text')}
    except (ValueError,KeyError,TypeError):raise OriginalIntakeError('original_page_cursor_invalid') from None


def assemble_native_page(chunks):
    """Join a complete exact cursor chain; gaps, clipping and drift refuse proof."""
    if not chunks or len(chunks)>4096:raise OriginalIntakeError('original_page_proof_bound')
    first=chunks[0];info=first['native_text_info'];native=[];blocks={};cursor=dict(block=0,offset=0,text=0)
    fixed=('format','page_count','physical_page_index','width','height','page_id','original_sha256',
        'parser_sha256','image_size','image_sha256','mime_type','preview_render')
    text_size=0
    for part in chunks:
        if (part.get('native_cursor')!=cursor or any(part.get(k)!=first[k] for k in fixed)
                or part.get('page_receipt_sha256')!=digest(canonical({k:v for k,v in part.items() if k!='page_receipt_sha256'}))):
            raise OriginalIntakeError('original_page_fragment_changed')
        status=part['native_text_info']
        if any(status.get(k)!=info.get(k) for k in ('original_length','block_count','native_text_sha256','native_blocks_sha256','encoding_notice')):
            raise OriginalIntakeError('original_page_fragment_changed')
        if status['offset']!=text_size or status['end']!=text_size+len(part['native_text']):
            raise OriginalIntakeError('original_page_text_gap')
        native.append(part['native_text']);text_size=status['end']
        for segment in part['native_blocks']:
            index=segment['block_index'];old=blocks.get(index)
            if old is None:
                if blocks and index<=max(blocks):raise OriginalIntakeError('original_page_block_order')
                old=blocks[index]=dict(block_index=index,bbox=segment['bbox'],text='',original_length=segment['original_length'],
                    text_sha256=segment['text_sha256'])
            if (old['bbox']!=segment['bbox'] or old['original_length']!=segment['original_length']
                    or old['text_sha256']!=segment['text_sha256'] or segment['offset']!=len(old['text'])
                    or segment['end']!=segment['offset']+len(segment['text'])):
                raise OriginalIntakeError('original_page_block_gap')
            old['text']+=segment['text']
        cursor=part['next_native']
    if cursor is not None or not chunks[-1]['native_text_info']['complete']:
        raise OriginalIntakeError('original_page_continuation_required')
    native=''.join(native)
    if len(native)!=info['original_length'] or digest(native.encode())!=info['native_text_sha256'] or len(blocks)!=info['block_count']:
        raise OriginalIntakeError('original_page_text_gap')
    for block in blocks.values():
        if len(block['text'])!=block['original_length'] or digest(block['text'].encode())!=block['text_sha256']:
            raise OriginalIntakeError('original_page_block_gap')
    raw_blocks=[{k:b[k] for k in ('block_index','bbox','text')} for b in blocks.values()]
    if digest(canonical(raw_blocks))!=info['native_blocks_sha256']:raise OriginalIntakeError('original_page_block_gap')
    value={k:first[k] for k in fixed}
    value.update(native_text=native,native_blocks=[dict(b,truncated=False,offset=0,end=b['original_length']) for b in blocks.values()],
        native_text_info=dict(info,offset=0,end=len(native),truncated=False,blocks_truncated=False,complete=True))
    value['page_receipt_sha256']=digest(canonical(value))
    if len(canonical(value))>2*1024*1024:raise OriginalIntakeError('original_page_proof_bound')
    return value
