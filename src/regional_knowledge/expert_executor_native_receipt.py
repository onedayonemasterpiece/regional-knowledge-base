"""Original native Slack receipt -> remote-covered Dataset delivery completion.

This is an in-process callback for an ALREADY INSTALLED trusted native provider
observer. It performs NO Slack API call and constructs NO provider receipt,
executor, IAM grant, credential, queue or scheduler. No HTTP payload can select
the receipt/proof. Unknown/missing provider observations never enable resend.
"""
from __future__ import annotations

import asyncio
from uuid import UUID

from .expert_delivery_control import NativeSlackReceipt, _RECEIPT
from .expert_executor_workflow import ExpertExecutorContext
from .expert_request_control import RequestControlError


class BoundExpertNativeReceipt:
    """One exact original service and the original signed request authority."""

    def __init__(self, context: ExpertExecutorContext):
        if type(context) is not ExpertExecutorContext:
            raise TypeError("one existing provisioned executor context required")
        self.context=context
        self.delivery=context.delivery

    async def observe(self, request_id: str, proof: NativeSlackReceipt):
        """Stage/verify an ALREADY OBSERVED native response; never post/send.

        A crash or lost Dataset confirmation leaves the same original child
        pending and reusable, not a new provider permit. RequestControl must
        already have claimed the single provider send and marked an unknown or
        sending state, so a fake bool cannot manufacture provider success.
        """
        if (type(proof) is not NativeSlackReceipt or proof._trusted is not _RECEIPT):
            raise RequestControlError("native_receipt_required")
        if type(request_id) is not str or len(request_id)!=36:
            raise RequestControlError("request_unavailable")
        try:
            if str(UUID(request_id))!=request_id:
                raise RequestControlError("request_unavailable")
        except ValueError as exc:
            raise RequestControlError("request_unavailable") from exc

        credentials=self.context.credentials
        # Trust the ORIGINAL delivery controller; it verifies service epoch,
        # result+send Dataset coverage and exact app/team/bot/text/destination.
        # Never assert a provider outcome from locally staged receipt bytes.
        await self.delivery.stage_dataset_native_receipt(
            credentials,request_id,proof)
        try:
            result=await asyncio.to_thread(
                self.delivery.confirm_dataset_native_receipt,
                credentials,request_id,proof)
        except RequestControlError as exc:
            if exc.code=="dataset_delivery_receipt_not_covered":
                return dict(state="receipt_pending",remote_covered=False,
                            retry_send_allowed=False)
            raise
        if (type(result) is not dict or result.get("state")!="delivered"):
            raise RequestControlError("native_receipt_confirmation_unverified")
        return dict(state="delivered",remote_covered=True,
                    retry_send_allowed=False)
