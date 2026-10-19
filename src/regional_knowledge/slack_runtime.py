"""Existing backend lifecycle for the installed Kaliningrad Base app.

Private installation/config files select only the bot and scoped delivery
service. They never select a human actor or grant access to the source corpus.
"""
import asyncio
import json
import logging
import os
from pathlib import Path
import random
import stat

from .expert_requests import ServiceCredentials
from .slack_expert import SlackExpertAdapter

log = logging.getLogger(__name__)


def private_text(path):
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("private configuration path must be absolute")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as stream:
        mode = os.fstat(stream.fileno()).st_mode
        if not stat.S_ISREG(mode) or mode & 0o077:
            raise ValueError("runtime credential must be a private regular file")
        text = stream.read(16385)
        if len(text) > 16384:
            raise ValueError("runtime credential too large")
        return text


def bot_tokens(path):
    values = {}
    allowed = {"SLACK_BOT_TOKEN": "xoxb-", "SLACK_APP_TOKEN": "xapp-"}
    for line in private_text(path).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key not in allowed or key in values or not separator or not value.startswith(allowed[key]) or any(ord(c) < 33 or ord(c) > 126 for c in value):
            raise ValueError("invalid bot/app credential configuration")
        values[key] = value
    if set(values) != set(allowed):
        raise ValueError("bot/app credentials required")
    return values


def configured_adapter(requests, config):
    tokens = bot_tokens(config["credentials_file"])
    delivery = ServiceCredentials(config["delivery_service_id"], private_text(config["delivery_service_token_file"]).strip())
    # Startup does not grant rights or copy an owner bearer. Existing audited
    # app/team/kind scope must match; every later operation checks it again.
    with requests.corpus.connect() as db:
        requests._service(db, delivery, "delivery")
    return SlackExpertAdapter(requests, delivery, bot_token=tokens["SLACK_BOT_TOKEN"], app_token=tokens["SLACK_APP_TOKEN"])


async def supervise(adapter):
    delay = 1
    while True:
        try:
            # run owns/closes aiohttp and the socket/outbox TaskGroup. Provider
            # failures leave durable inbox/results pending while MCP stays up.
            await adapter.run()
            delay = 1
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning(json.dumps({"event": "expert_slack_runtime_wait", "component": "kaliningrad_base",
                "error_type": type(error).__name__, "retry_after": delay}))
        await asyncio.sleep(delay + random.uniform(0, min(1, delay / 5)))
        delay = min(60, delay * 2)
