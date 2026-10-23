"""API owner composition for the existing RKB1 worker channel and credential."""
import json
import os
from pathlib import Path
import re
import shlex
import stat

from .telegram_original import TelegramOriginalLocator
from .telegram_runtime_binding import SLOT, TelegramBindingUnavailable, _read_ephemeral_bundle
from .telegram_runtime_channel import Rkb1ChannelSource


def existing_rkb1_credentials():
    """Read only the three existing named credentials, just when READY requests them."""
    try:
        values={}
        for line in Path('/home/dev/.env').read_text().splitlines():
            key,separator,value=line.partition('=')
            if separator and key in (SLOT,'TELEGRAM_API_ID','TELEGRAM_API_HASH'):
                parsed=shlex.split(value)
                if len(parsed)!=1:raise ValueError('credential format')
                values[key]=parsed[0]
        bundle=values[SLOT]
        _read_ephemeral_bundle(bundle)
        api_id=int(values['TELEGRAM_API_ID']);api_hash=values['TELEGRAM_API_HASH']
        if api_id<=0 or re.fullmatch('[0-9a-f]{32}',api_hash) is None:raise ValueError('credential format')
        return dict(bundle_b64=bundle,api_id=api_id,api_hash=api_hash)
    except Exception:
        raise TelegramBindingUnavailable('telegram_credential_supplier_unavailable') from None


def configured_rkb1_source(queue,locator_file):
    """Opt in with the exact private existing archive locator; no provider I/O."""
    if type(locator_file) is not str or not Path(locator_file).is_absolute():
        raise ValueError('private Telegram locator file required')
    fd=os.open(locator_file,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_mode&0o077 or not 1<=info.st_size<=4096:
            raise ValueError('private Telegram locator file required')
        raw=os.read(fd,4097)
        if len(raw)!=info.st_size or os.read(fd,1):raise ValueError('Telegram locator changed')
    finally:os.close(fd)
    locator=TelegramOriginalLocator.from_wire(json.loads(raw))
    return Rkb1ChannelSource(queue=queue,locator=locator,credentials=existing_rkb1_credentials)
