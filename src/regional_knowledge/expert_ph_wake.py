"""Service-scoped same-host PH Event wake; RKB remains the expert-request authority."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat

import httpx


def _private_text(path: str, limit: int) -> str:
    if not isinstance(path, str) or not Path(path).is_absolute():
        raise ValueError('absolute private path required')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077
                or not 1 <= info.st_size <= limit):
            raise ValueError('private regular file required')
        data = os.read(fd, limit + 1)
        if len(data) != info.st_size:
            raise ValueError('private file changed')
        return data.decode('utf-8').strip()
    finally:
        os.close(fd)


class ProjectsHubExpertWake:
    """Send only the saved request UUID, never the Slack actor/text or a user bearer."""

    URL = 'http://127.0.0.1:8196/internal/rkb/expert-wake'

    def __init__(self, control, config_path: str, *, transport=None):
        from .expert_request_control import TargetExpertRequestControl
        if type(control) is not TargetExpertRequestControl:
            raise TypeError('original RKB request control required')
        config = json.loads(_private_text(config_path, 4096))
        if (type(config) is not dict
                or set(config) != {'codec', 'project_id', 'service_secret_file'}
                or config['codec'] != 'rkb.ph-expert-wake.v1'
                or not isinstance(config['project_id'], str)
                or not re.fullmatch(r'prj_[0-9a-f]{32}', config['project_id'])):
            raise ValueError('exact PH wake scope required')
        secret = _private_text(config['service_secret_file'], 512)
        if len(secret) < 32:
            raise ValueError('PH service credential too short')
        self.control, self.project_id = control, config['project_id']
        self._secret, self._transport = secret, transport

    async def wake(self, request_id: str) -> dict:
        if not isinstance(request_id, str) or not re.fullmatch(
                r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', request_id):
            raise ValueError('original RKB request ID required')
        async with httpx.AsyncClient(timeout=1.5, trust_env=False,
                                     transport=self._transport) as client:
            response = await client.post(self.URL,
                headers={'Authorization': 'Bearer ' + self._secret},
                json={'project_id': self.project_id, 'rkb_request_id': request_id})
        if response.status_code != 202:
            raise RuntimeError('PH wake not accepted')
        receipt = response.json()
        if (not isinstance(receipt, dict)
                or receipt.get('project_id') != self.project_id
                or not isinstance(receipt.get('request_id'), str)
                or not receipt['request_id'].startswith('mcp_test_')
                or not isinstance(receipt.get('eventId'), str)
                or not receipt['eventId'].startswith('evt_')):
            raise RuntimeError('PH request receipt invalid')
        return receipt
