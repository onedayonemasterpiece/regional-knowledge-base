"""Bind signed Slack ingress and delegated source read to ONE existing RKB owner.

A wired HTTP route is not proof of an installed integration or a completed
expert result. This leaf only prevents accidentally composing an ingress and
read receiver from different identity, spool, target or read authorities.
No service credentials, grants, network client, second journal or sender are
created here. The server owner must explicitly mount both existing routes.
"""
from __future__ import annotations

from .expert_evidence_receiver import ExpertEvidenceReceiver
from .expert_slack_ingress import ExpertSlackIngress
from .kaggle_service_startup import TargetServiceStartup


def compose_expert_evidence_receiver(
    *, target_startup: TargetServiceStartup, expert_slack_ingress: ExpertSlackIngress
) -> ExpertEvidenceReceiver:
    """Return the existing real read handler for exactly this signed ingress.

    Only an already-configured target, canonical Participants, writer/spool
    and ReadAuthority can form this route. Runtime startup/registration,
    credential provisioning, executor work and Slack delivery are separate
    explicit operations and MUST NOT be inferred from this construction.
    """
    if (type(target_startup) is not TargetServiceStartup
            or type(expert_slack_ingress) is not ExpertSlackIngress):
        raise TypeError("exact target startup and verified Slack ingress required")

    control = expert_slack_ingress.control
    config = target_startup.config
    backend = target_startup.backend
    if (control.spool is not config.spool
            or control.people is not config.participants
            or config.spool.queue is not config.queue
            or backend is not getattr(target_startup, "backend", None)
            or backend.worker_queue is not config.queue
            or backend.read_authority is not target_startup.authority
            or target_startup.authority.participants is not config.participants
            or getattr(backend, "startup", None) is not target_startup):
        raise ValueError("expert ingress and target read authorities must be identical")

    # The existing receiver independently verifies the source gate, actor
    # link, service epoch, request generation and buffered final issuance.
    # Neither factory nor caller may inject a synthetic allow-all source gate.
    return ExpertEvidenceReceiver(control=control, backend=backend)
