"""Configuration of the same request authority, independent of Slack install."""
import hashlib
import json
import os


def configuration(resource):
    from .slack_runtime import private_text
    path=os.getenv('RKB_REQUEST_CONFIG_PATH') or os.getenv('RKB_SLACK_CONFIG_PATH')
    if path:return json.loads(private_text(path))
    if os.getenv('RKB_SLACK_RUNTIME_ENABLED')=='1':raise RuntimeError('Slack runtime configuration required')
    if not resource:raise RuntimeError('Managed requests require the existing authenticated resource')
    return {'app_id':'rkb-native','team_id':hashlib.sha256(resource.encode()).hexdigest(),'bot_user_id':'','allowed_channels':[]}
