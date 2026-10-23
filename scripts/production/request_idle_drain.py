"""Request planned closure; provider terminal proof remains a separate read."""
import argparse
import json
from pathlib import Path

from regional_knowledge.bge_queue import BgeQueue


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue',required=True,type=Path)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--provider-ref',required=True)
    parser.add_argument('--provider-version',required=True,type=int)
    args=parser.parse_args()
    if not args.queue.is_file():parser.error('existing queue required')
    queue=BgeQueue(args.queue)
    accepted=queue.request_idle_drain(args.run_id,provider_ref=args.provider_ref,
                                    provider_version=args.provider_version)
    if accepted:queue.maintenance()
    print(json.dumps(dict(run_id=args.run_id,drain_requested=accepted,
                         provider_terminal_confirmed=False)))


if __name__=='__main__':main()
