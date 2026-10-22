"""Durable ORIGINAL Slack result/send/receipt records in the existing Dataset store.

No PG command, second journal, new actor registry, provider call or send permit.
Only already verified Dataset key+exact bytes can recover an intent/receipt.
"""
from __future__ import annotations

import hashlib
import json
from uuid import NAMESPACE_URL, uuid5

from .expert_request_recovery import (
    ExpertDatasetRequestRecords, ExpertRecordError, from_row, _canonical,
    MAX_RECORD_BYTES,
)


RESULT_CODEC = "rkb.expert-result-record.v1"
SEND_CODEC = "rkb.expert-send-record.v1"
RECEIPT_CODEC = "rkb.expert-delivery-receipt-record.v1"
RESULT_KEYS = frozenset({"codec","request_id","intake_sha256","result_json",
                          "result_hash","result_generation"})
SEND_KEYS = frozenset({"codec","request_id","intake_sha256","result_record_sha256",
                        "send_json","client_msg_id","result_hash"})
RECEIPT_KEYS = frozenset({"codec","request_id","intake_sha256",
                           "send_record_sha256","receipt_json","text_sha256"})


class ExpertDeliveryRecoveryError(ValueError):
    pass


def _fail(reason="expert_delivery_record_changed"):
    raise ExpertDeliveryRecoveryError(reason)


def _decode(raw, keys, codec):
    if type(raw) is not bytes or not 1 <= len(raw) <= MAX_RECORD_BYTES:
        _fail()
    def unique(pairs):
        result={}
        for name,value in pairs:
            if name in result:_fail()
            result[name]=value
        return result
    try:
        doc=json.loads(raw,object_pairs_hook=unique,
                       parse_constant=lambda _: _fail())
        if type(doc) is not dict or set(doc)!=keys or doc["codec"]!=codec:
            _fail()
        if _canonical(doc)!=raw:
            _fail()
        return doc
    except (UnicodeError,TypeError,ValueError,OverflowError):
        _fail()


def _json_object(text):
    if type(text) is not str or not 1 <= len(text.encode("utf-8")) <= MAX_RECORD_BYTES:
        _fail()
    try: item=json.loads(text)
    except (UnicodeError,ValueError,TypeError):_fail()
    if type(item) is not dict or _canonical(item).decode("utf-8") != text:
        _fail()
    return item


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _intake_hash(row):
    return _sha(from_row(row))


def result_bytes(row):
    """Bind original TargetExpertDeliveryControl.prepare_result bytes."""
    payload=_json_object(row["result_json"])
    gen=row["result_generation"]
    if (set(payload)!={"codec","request_id","generation","person_id",
                       "output_scope","text"} or payload["codec"]!="rkb.expert-result.v1"
            or payload["request_id"]!=row["id"] or payload["person_id"]!=row["person_id"]
            or payload["output_scope"]!=row["output_scope"]
            or type(gen) is not int or gen<1 or payload["generation"]!=gen
            or type(payload["text"]) is not str
            or not 1 <= len(payload["text"].encode())<=24000
            or row["result_hash"]!=_sha(row["result_json"].encode())):
        _fail("original_result_identity_changed")
    doc={"codec":RESULT_CODEC,"request_id":row["id"],
         "intake_sha256":_intake_hash(row),"result_json":row["result_json"],
         "result_hash":row["result_hash"],"result_generation":gen}
    return _canonical(doc)


def parse_result(row,raw):
    doc=_decode(raw,RESULT_KEYS,RESULT_CODEC)
    if doc["request_id"]!=row["id"] or doc["intake_sha256"]!=_intake_hash(row):
        _fail()
    payload=_json_object(doc["result_json"])
    if (set(payload)!={"codec","request_id","generation","person_id",
                       "output_scope","text"} or payload["codec"]!="rkb.expert-result.v1"
            or payload["request_id"]!=row["id"] or payload["person_id"]!=row["person_id"]
            or payload["output_scope"]!=row["output_scope"]
            or type(doc["result_generation"]) is not int
            or doc["result_generation"]<1
            or payload["generation"]!=doc["result_generation"]
            or not isinstance(payload["text"],str)
            or not 1<=len(payload["text"].encode())<=24000
            or doc["result_hash"]!=_sha(doc["result_json"].encode())):
        _fail("original_result_identity_changed")
    return doc,payload


def send_bytes(row,result_doc,result_raw):
    """Same original stable send marker/payload as exact_send_intent, sans PG."""
    if result_doc["request_id"]!=row["id"] or row["result_hash"]!=result_doc["result_hash"]:
        _fail()
    gen=result_doc["result_generation"]
    marker=str(uuid5(NAMESPACE_URL,"rkb:expert-send:"+row["id"]+":"+str(gen)))
    origin=json.loads(row["origin"])
    payload={"codec":"rkb.expert-slack-send.v1","request_id":row["id"],
             "generation":gen,"marker":marker,"origin":origin,
             "result_hash":result_doc["result_hash"],
             "output_scope":row["output_scope"]}
    doc={"codec":SEND_CODEC,"request_id":row["id"],
         "intake_sha256":_intake_hash(row),
         "result_record_sha256":_sha(result_raw),
         "send_json":_canonical(payload).decode(),
         "client_msg_id":marker,"result_hash":result_doc["result_hash"]}
    return _canonical(doc)


def parse_send(row,raw,result_doc,result_raw):
    doc=_decode(raw,SEND_KEYS,SEND_CODEC)
    # Recover exact original identity independently of missing local result.
    expected_row=dict(row)
    expected_row["result_hash"]=result_doc["result_hash"]
    expected=send_bytes(expected_row,result_doc,result_raw)
    if raw!=expected:
        _fail("original_send_intent_identity_changed")
    return doc,_json_object(doc["send_json"])


def receipt_bytes(row,result_doc,send_doc,send_raw,proof,*,bot_user_id):
    """Existing trusted NativeSlackReceipt only. No caller assertion boolean."""
    from .expert_delivery_control import NativeSlackReceipt, _RECEIPT
    if type(proof) is not NativeSlackReceipt or proof._trusted is not _RECEIPT:
        _fail("native_receipt_required")
    origin=json.loads(row["origin"])
    result=_json_object(result_doc["result_json"])
    if (proof.app_id!=origin["app_id"] or proof.team_id!=origin["team_id"]
            or proof.bot_user_id!=bot_user_id
            or proof.channel_id!=origin["channel_id"]
            or proof.thread_ts!=origin["thread_ts"]
            or proof.client_message_id!=send_doc["client_msg_id"]
            or proof.text!=result["text"]):
        _fail("native_receipt_mismatch")
    receipt={"app_id":proof.app_id,"team_id":proof.team_id,
             "bot_user_id":proof.bot_user_id,"channel_id":proof.channel_id,
             "thread_ts":proof.thread_ts,"message_ts":proof.message_ts,
             "client_message_id":proof.client_message_id,
             "result_hash":result_doc["result_hash"]}
    doc={"codec":RECEIPT_CODEC,"request_id":row["id"],
         "intake_sha256":_intake_hash(row),"send_record_sha256":_sha(send_raw),
         "receipt_json":_canonical(receipt).decode(),
         "text_sha256":_sha(result["text"].encode())}
    return _canonical(doc)


def parse_receipt(row,raw,result_doc,send_doc,send_raw,*,bot_user_id):
    doc=_decode(raw,RECEIPT_KEYS,RECEIPT_CODEC)
    origin=json.loads(row["origin"])
    receipt=_json_object(doc["receipt_json"])
    result=_json_object(result_doc["result_json"])
    if (doc["request_id"]!=row["id"] or doc["intake_sha256"]!=_intake_hash(row)
            or doc["send_record_sha256"]!=_sha(send_raw)
            or doc["text_sha256"]!=_sha(result["text"].encode())
            or set(receipt)!={"app_id","team_id","bot_user_id","channel_id",
                               "thread_ts","message_ts","client_message_id","result_hash"}
            or receipt["app_id"]!=origin["app_id"]
            or receipt["team_id"]!=origin["team_id"]
            or receipt["bot_user_id"]!=bot_user_id
            or receipt["channel_id"]!=origin["channel_id"]
            or receipt["thread_ts"]!=origin["thread_ts"]
            or receipt["client_message_id"]!=send_doc["client_msg_id"]
            or receipt["result_hash"]!=result_doc["result_hash"]):
        _fail("native_receipt_mismatch")
    return doc,receipt


class ExpertDatasetDeliveryRecords:
    """Three child records under the ALREADY owner-bound expert request UUID."""

    def __init__(self, parent):
        if type(parent) is not ExpertDatasetRequestRecords:
            raise TypeError("existing canonical Dataset request records required")
        self.parent=parent
        self.store=parent.store

    def key(self,request_id,stage):
        return self.parent.effect_key(request_id,stage)

    async def stage(self,request_id,stage,raw):
        # Caller is an existing current executor/delivery controller; the
        # leaf accepts only server-derived UUID+enumerated stage.
        return await self.parent._stage_immutable(self.key(request_id,stage),raw)

    def covered(self,request_id,stage,raw):
        return self.store.remote_durable(self.key(request_id,stage),raw)

    def read_covered(self,request_id,stage):
        key=self.key(request_id,stage)
        with self.store.queue.connect(write=False) as db:
            row=db.execute("SELECT data,covered_version FROM rkb_dataset_recovery_objects "
                "WHERE storage=? AND key=?",
                (self.store.storage_identity,key)).fetchone()
        if row is None or row["covered_version"] is None:return None
        raw=bytes(row["data"])
        if len(raw)>MAX_RECORD_BYTES or not self.store.remote_durable(key,raw):
            _fail("delivery_coverage_changed")
        return raw
