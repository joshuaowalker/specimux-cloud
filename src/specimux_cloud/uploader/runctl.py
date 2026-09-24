"""Operate on one run with a host's service key:

    specimux-cloud run status RUN --run-api URL --service-key KEY
    specimux-cloud run cancel RUN --run-api URL --service-key KEY [--reason TEXT]
    specimux-cloud run retry  RUN --run-api URL --service-key KEY

cancel stops a run: a run still waiting fails at once, and an active
stage's job is stopped, so the run fails when the job reports its exit.
retry basecalls a run that failed in basecalling again, skipping the files
already done. The key only reaches its own host's runs.
"""

import argparse
import json
import logging
import os
import sys
from typing import Optional

import httpx


def run(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="specimux-cloud run", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "cancel", "retry"])
    ap.add_argument("run_id")
    ap.add_argument("--run-api", required=True)
    ap.add_argument("--service-key", default=os.environ.get("SPECIMUX_SERVICE_KEY"),
                    help="A host's service key (default: SPECIMUX_SERVICE_KEY)")
    ap.add_argument("--reason", default="", help="cancel: recorded with the run's exit")
    args = ap.parse_args(argv)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if not args.service_key:
        ap.error("--service-key or SPECIMUX_SERVICE_KEY is required")
    base = args.run_api.rstrip("/")
    hdr = {"X-Service-Key": args.service_key}
    with httpx.Client(timeout=60.0) as c:
        if args.action == "status":
            r = c.get(f"{base}/v1/runs/{args.run_id}", headers=hdr)
        elif args.action == "cancel":
            r = c.post(f"{base}/v1/runs/{args.run_id}/cancel", headers=hdr,
                       json={"reason": args.reason} if args.reason else None)
        else:
            r = c.post(f"{base}/v1/runs/{args.run_id}/retry", headers=hdr)
    try:
        body = r.json()
    except ValueError:
        body = {"error": r.text}
    if r.status_code != 200:
        print(f"{args.action} refused ({r.status_code}): {body.get('error') or body.get('detail') or body}",
              file=sys.stderr)
        return 1
    ex = body.get("exit") or {}
    line = f"run {body.get('id', args.run_id)}: {body.get('state')}"
    if body.get("cancel"):
        line += f" · cancel requested ({body['cancel'].get('reason')})"
    if ex:
        line += f" · exit {ex.get('code')}" + (f" ({ex.get('reason')})" if ex.get("reason") else "")
    print(line)
    if args.action == "status" and logging.getLogger().isEnabledFor(logging.DEBUG):
        print(json.dumps(body, indent=2, default=str))
    return 0


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
