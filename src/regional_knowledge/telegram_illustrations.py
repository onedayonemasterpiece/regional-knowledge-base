"""Narrow topic-scoped media adapter borrowing the existing leased native client.

No client construction, credentials, extra uploader, download-to-verify or hash
comparison. Durable effect state belongs to the existing source operation store.
An UNKNOWN send without a registered locator is NEVER blindly sent again.
"""
from __future__ import annotations

from io import BytesIO


class IllustrationArchiveUnknown(RuntimeError):
    pass


class NativeTelegramIllustrations:
    def __init__(self, *, client, chat_id, topic_id, current):
        if (type(chat_id) is not int or not -(2**63)<chat_id<0
                or type(topic_id) is not int or topic_id!=4
                or not callable(current) or not callable(getattr(client,'get_messages',None))
                or not callable(getattr(client,'send_file',None))):
            raise ValueError('existing leased client and exact illustration archive scope required')
        self.client,self.chat_id,self.topic_id,self.current=client,chat_id,topic_id,current

    def _check(self):
        if self.current() is not True:
            raise PermissionError('illustration Telegram lease changed')

    async def observe(self, locator):
        """Exact native presence/dimensions only; never download/hash media."""
        if (type(locator) is not dict or locator.get('chat_id')!=self.chat_id
                or locator.get('topic_id')!=self.topic_id
                or locator.get('kind') not in ('photo','document')
                or type(locator.get('message_id')) is not int or locator['message_id']<=0
                or type(locator.get('media_id')) is not int or not locator['media_id']):
            raise ValueError('exact registered illustration locator required')
        self._check()
        message=await self.client.get_messages(self.chat_id,ids=locator['message_id'])
        self._check()
        actual=self._locator(message,locator['kind'])
        if any(actual[k]!=locator[k] for k in ('chat_id','topic_id','message_id','media_id','kind')):
            raise ValueError('registered illustration media changed')
        return actual

    def _locator(self, message, kind):
        reply=getattr(message,'reply_to',None)
        topic=getattr(reply,'reply_to_top_id',None) or getattr(reply,'reply_to_msg_id',None)
        media=getattr(message,kind,None)
        if (getattr(message,'chat_id',None)!=self.chat_id or topic!=self.topic_id
                or type(getattr(message,'id',None)) is not int or message.id<=0
                or type(getattr(media,'id',None)) is not int or not media.id):
            raise ValueError('expected illustration archive message unavailable')
        if kind=='photo':
            dimensions=[(getattr(s,'w',0),getattr(s,'h',0)) for s in getattr(media,'sizes',())]
        else:
            dimensions=[(getattr(a,'w',0),getattr(a,'h',0)) for a in getattr(media,'attributes',())]
        dimensions=[(w,h) for w,h in dimensions if type(w) is int and type(h) is int and 0<w<=20000 and 0<h<=20000]
        if not dimensions:raise ValueError('observed illustration dimensions unavailable')
        w,h=max(dimensions,key=lambda p:p[0]*p[1])
        return dict(chat_id=self.chat_id,topic_id=self.topic_id,message_id=message.id,
            media_id=media.id,kind=kind,width=w,height=h)

    async def send_one(self, *, operation_key, crop, mime_type, kind, load, begin, register, complete):
        """One existing durable effect; callbacks use exact-key compare-and-set.

        load returns pending/unknown/registered/ready. begin durably changes only
        pending→unknown BEFORE send, returning True to its single winning caller.
        register durably stores the returned locator BEFORE presence verification.
        These callbacks belong to the native source operation store, not a new
        queue. They must not retain a PG transaction across this method's awaits.
        """
        if (type(operation_key) is not str or not 1<=len(operation_key)<=256
                or type(crop) is not bytes or not 1<=len(crop)<=8*1024*1024
                or mime_type not in ('image/jpeg','image/png','image/webp')
                or kind not in ('photo','document')):
            raise ValueError('bounded exact illustration send required')
        self._check()
        state=await load(operation_key)
        self._check()
        if state['status'] in ('registered','ready'):
            actual=await self.observe(state['locator'])
            await complete(operation_key,actual)
            self._check()
            return actual
        if state['status']!='pending':
            raise IllustrationArchiveUnknown('reconcile the same unknown send; no blind resend')
        if await begin(operation_key) is not True:
            raise IllustrationArchiveUnknown('send already admitted; reconcile same operation')
        self._check()
        # A cancellation/lost reply from here leaves durable UNKNOWN. Do not
        # infer absence from a network exception or publish under another key.
        content=BytesIO(crop)
        content.name='illustration.'+{'image/jpeg':'jpg','image/png':'png','image/webp':'webp'}[mime_type]
        message=await self.client.send_file(self.chat_id,content,reply_to=self.topic_id,
            force_document=kind=='document',caption='RKB illustration '+operation_key)
        self._check()
        locator=self._locator(message,kind)
        await register(operation_key,locator)
        self._check()
        actual=await self.observe(locator)
        await complete(operation_key,actual)
        self._check()
        return actual


def needs_resolution_document(*, source_pixels, observed_photo):
    """No universal Telegram pixel limit; use this actual upload's dimensions."""
    if (type(source_pixels) not in (tuple,list) or len(source_pixels)!=2
            or any(type(v) is not int or not 0<v<=20000 for v in source_pixels)
            or observed_photo.get('kind')!='photo'
            or any(type(observed_photo.get(k)) is not int or observed_photo[k]<=0 for k in ('width','height'))):
        raise ValueError('actual source and returned photo dimensions required')
    return source_pixels[0]>observed_photo['width'] or source_pixels[1]>observed_photo['height']
