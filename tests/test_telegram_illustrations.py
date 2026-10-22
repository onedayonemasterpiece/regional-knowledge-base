"""Synthetic native client only: no credentials, Telegram, downloads or provider."""
import asyncio
import copy
import unittest
from types import SimpleNamespace as N

from regional_knowledge.telegram_illustrations import (
    NativeTelegramIllustrations, IllustrationArchiveUnknown, needs_resolution_document)

CHAT=-10012345
TOPIC=4


def message(kind='photo',width=800,height=600):
    media=N(id=9,sizes=[N(w=width,h=height)],attributes=[N(w=width,h=height)])
    return N(id=23,chat_id=CHAT,reply_to=N(reply_to_top_id=TOPIC),
        photo=media if kind=='photo' else None,document=media if kind=='document' else None)


class Client:
    def __init__(self):self.sent=[];self.read=[];self.message=message();self.fail_send=False;self.fail_read=False
    async def send_file(self,chat,data,**kw):
        self.sent.append((chat,data.read(),kw))
        if self.fail_send:raise ConnectionError('lost send acknowledgement')
        return self.message
    async def get_messages(self,chat,**kw):
        self.read.append((chat,kw))
        if self.fail_read:raise ConnectionError('presence unavailable')
        return self.message
    async def download_media(self,*a,**kw):raise AssertionError('no verification download')
    def iter_download(self,*a,**kw):raise AssertionError('no checksum download')


class Store:
    def __init__(self):self.state={'status':'pending'};self.before_send=[]
    async def load(self,key):return copy.deepcopy(self.state)
    async def begin(self,key):
        if self.state['status']!='pending':return False
        self.state={'status':'unknown'};return True
    async def register(self,key,locator):self.state={'status':'registered','locator':copy.deepcopy(locator)}
    async def complete(self,key,locator):self.state={'status':'ready','locator':copy.deepcopy(locator)}


class NativeIllustrationArchiveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client=Client();self.current=True;self.store=Store()
        self.adapter=NativeTelegramIllustrations(client=self.client,chat_id=CHAT,topic_id=TOPIC,current=lambda:self.current)
    async def send(self,kind='photo'):
        return await self.adapter.send_one(operation_key='illustration:synthetic',crop=b'synthetic bytes',
            mime_type='image/jpeg',kind=kind,load=self.store.load,begin=self.store.begin,
            register=self.store.register,complete=self.store.complete)
    async def test_photo_default_one_send_and_presence_no_hash_download(self):
        locator=await self.send()
        self.assertEqual(locator['kind'],'photo');self.assertEqual(self.store.state['status'],'ready')
        self.assertFalse(self.client.sent[0][2]['force_document'])
        self.assertEqual(self.client.sent[0][2]['reply_to'],TOPIC)
        self.assertEqual(len(self.client.read),1)
    async def test_repeat_ready_rechecks_exact_locator_without_resend(self):
        first=await self.send();second=await self.send()
        self.assertEqual(first,second);self.assertEqual(len(self.client.sent),1);self.assertEqual(len(self.client.read),2)
    async def test_lost_send_ack_stays_unknown_no_blind_retry(self):
        self.client.fail_send=True
        with self.assertRaises(ConnectionError):await self.send()
        self.assertEqual(self.store.state['status'],'unknown')
        self.client.fail_send=False
        with self.assertRaises(IllustrationArchiveUnknown):await self.send()
        self.assertEqual(len(self.client.sent),1)
    async def test_lost_presence_ack_keeps_locator_and_resumes_read_only(self):
        self.client.fail_read=True
        with self.assertRaises(ConnectionError):await self.send()
        self.assertEqual(self.store.state['status'],'registered')
        self.client.fail_read=False;await self.send()
        self.assertEqual(len(self.client.sent),1);self.assertEqual(self.store.state['status'],'ready')
    async def test_revoked_lease_before_send_has_no_effect(self):
        self.current=False
        with self.assertRaises(PermissionError):await self.send()
        self.assertFalse(self.client.sent);self.assertEqual(self.store.state['status'],'pending')
    async def test_revocation_during_send_prevents_ready_and_resend(self):
        original=self.client.send_file
        async def revoke(*args,**kwargs):
            result=await original(*args,**kwargs);self.current=False;return result
        self.client.send_file=revoke
        with self.assertRaises(PermissionError):await self.send()
        self.assertEqual(self.store.state['status'],'unknown')
    async def test_changed_topic_is_not_registered_success(self):
        self.client.message.reply_to.reply_to_top_id=2
        with self.assertRaises(ValueError):await self.send()
        self.assertEqual(self.store.state['status'],'unknown')
    async def test_changed_media_on_ready_recheck_fails(self):
        await self.send();self.client.message.photo.id=10
        with self.assertRaises(ValueError):await self.send()
        self.assertEqual(len(self.client.sent),1)
    async def test_document_is_explicit_not_unconditional_second_file(self):
        self.client.message=message('document',1600,1200)
        result=await self.send('document')
        self.assertEqual(result['kind'],'document');self.assertTrue(self.client.sent[0][2]['force_document'])
        self.assertEqual(len(self.client.sent),1)
    async def test_concurrent_admission_one_effect(self):
        values=await asyncio.gather(self.send(),self.send(),return_exceptions=True)
        self.assertEqual(len(self.client.sent),1)
        self.assertTrue(any(isinstance(v,dict) for v in values))
    async def test_stale_scope_rejected_before_message_lookup(self):
        with self.assertRaises(ValueError):
            await self.adapter.observe(dict(chat_id=CHAT,topic_id=2,message_id=23,media_id=9,kind='photo'))
        self.assertFalse(self.client.read)
    def test_resolution_decision_uses_actual_dimensions_not_magic_limit(self):
        photo=dict(kind='photo',width=1200,height=900)
        self.assertFalse(needs_resolution_document(source_pixels=(1200,900),observed_photo=photo))
        self.assertTrue(needs_resolution_document(source_pixels=(1800,1350),observed_photo=photo))
        self.assertFalse(needs_resolution_document(source_pixels=(600,450),observed_photo=photo))

if __name__=='__main__':unittest.main()


class ArchiveScopeTests(unittest.TestCase):
    def test_original_topic_cannot_be_used_for_public_figures(self):
        class Client:
            async def get_messages(self,*a,**kw):pass
            async def send_file(self,*a,**kw):pass
        with self.assertRaises(ValueError):
            NativeTelegramIllustrations(client=Client(),chat_id=CHAT,topic_id=2,current=lambda:True)
