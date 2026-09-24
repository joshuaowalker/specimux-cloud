"""specimux-cloud command line: run the run API (with the console), manage
hosts and their keys, run the engine wrapper, upload, or submit a run."""

import argparse
import logging
import os
import sys
import time
from pathlib import Path


def _store(args):
    """The control-plane store the ``hosts`` commands act on."""
    if args.backend == "aws":
        from .backends.aws import DynamoStore
        table = args.table or os.environ.get("SPECIMUX_TABLE")
        if not table:
            raise SystemExit("--table or SPECIMUX_TABLE is required for the aws backend")
        return DynamoStore(table, region=args.region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    from .backends.local import SQLiteStore
    return SQLiteStore(Path(args.data_dir) / "control-plane.sqlite")


def _host_service(args):
    """A RunService with only the store behind it, for host management."""
    from .runapi.service import RunService, ServiceConfig

    class _Nothing:
        def __getattr__(self, name):
            raise RuntimeError("only the store is available here")

    cfg = ServiceConfig(data_dir=Path(args.data_dir), base_url="http://hosts.local", session_secret="hosts-cli")
    return RunService(cfg, storage=_Nothing(), queue=_Nothing(), launcher=_Nothing(), store=_store(args))


def _print_hosts(hosts):
    now = time.time()
    for h in hosts:
        flag = " (disabled)" if h.get("disabled") else ""
        print(f"{h['id']}{flag}: {h.get('name')}  authorize: {h.get('authorize_url') or '-'}")
        for k in h.get("keys", []):
            exp = k.get("expires")
            state = "expired" if exp and exp < now else (
                f"expires {time.strftime('%Y-%m-%d %H:%M', time.localtime(exp))}" if exp else "live")
            print(f"    key {k['label']}: {state}, created {time.strftime('%Y-%m-%d', time.localtime(k.get('created') or 0))}")


# Subcommands with their own argument parsers get the rest of the line
# untouched (argparse's REMAINDER refuses options before a positional)
PASSTHROUGH = {"engine": "specimux_cloud.engine.wrapper", "dorado": "specimux_cloud.dorado.wrapper",
               "upload": "specimux_cloud.uploader.cli", "submit": "specimux_cloud.uploader.submit",
               "run": "specimux_cloud.uploader.runctl"}


def start_reconcile_loop(service, interval_s: float):
    """Reconcile every ``interval_s`` seconds on a daemon thread, so a job
    that dies without an exit report (stopped, lost host) is judged within
    minutes instead of at the next restart. 0 disables it."""
    import threading
    if interval_s <= 0:
        return None
    log = logging.getLogger(__name__)

    def loop():
        while True:
            time.sleep(interval_s)
            try:
                result = service.reconcile()
                if any(result.values()):
                    log.info(f"Reconciled: {result}")
            except Exception:
                log.exception("Periodic reconcile failed")

    t = threading.Thread(target=loop, name="reconcile", daemon=True)
    t.start()
    return t


def start_cleanup_loop(service, interval_s: float):
    """Clean up every ``interval_s`` seconds on a daemon thread of its own
    (a first pass over a large EFS takes a while, and reconcile must not
    wait for it). 0 disables it."""
    import threading
    if interval_s <= 0:
        return None
    log = logging.getLogger(__name__)

    def loop():
        while True:
            try:
                result = service.clean_up()
                if any(result.values()):
                    log.info(f"Cleaned up: {result}")
            except Exception:
                log.exception("Cleanup failed")
            time.sleep(interval_s)

    t = threading.Thread(target=loop, name="cleanup", daemon=True)
    t.start()
    return t


def main(argv=None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in PASSTHROUGH:
        import importlib
        sys.exit(importlib.import_module(PASSTHROUGH[argv[0]]).run(argv[1:]))
    ap = argparse.ArgumentParser(prog="specimux-cloud", description=__doc__)
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    sub = ap.add_subparsers(dest="command", required=True)

    def backend_args(p):
        p.add_argument("--backend", choices=["local", "aws"], default="local",
                       help="local: a data directory; aws: S3/SQS/Batch/DynamoDB from SPECIMUX_* env")
        p.add_argument("--data-dir", type=Path, default=Path("data"), help="Local root (storage, work dirs, state)")
        p.add_argument("--table", default=None, help="aws: the control-plane table (default SPECIMUX_TABLE)")
        p.add_argument("--region", default=None)

    r = sub.add_parser("runapi", help="Run the run API and the console")
    backend_args(r)
    r.add_argument("--host", default="127.0.0.1")
    r.add_argument("--port", type=int, default=8090)
    r.add_argument("--base-url", default=None, help="Public URL (default http://HOST:PORT)")
    r.add_argument("--dev-key", default="dev.dev-service-key",
                   help="local: the service key of the built-in `dev` host ('' for none)")
    r.add_argument("--no-console", action="store_true", help="Do not mount the console at /console")
    r.add_argument("--engine-arg", action="append", default=[], help="Extra argument for every engine run")

    h = sub.add_parser("hosts", help="Manage hosts and their service keys")
    backend_args(h)
    hs = h.add_subparsers(dest="hosts_command", required=True)
    a = hs.add_parser("add", help="Register a host and print its first key (shown once)")
    a.add_argument("host_id")
    a.add_argument("--name", default="")
    a.add_argument("--authorize-url", default=None,
                   help="The host's authorize route; omit for a host that uses the console")
    a.add_argument("--label", default="default", help="Label of the first key")
    a.add_argument("--console", action="store_true",
                   help="The host's authorize route is this deployment's console (needs --base-url)")
    a.add_argument("--base-url", default=None, help="With --console: the run API's public URL")
    k = hs.add_parser("key", help="Add a key under a label (shown once)")
    k.add_argument("host_id")
    k.add_argument("label")
    ro = hs.add_parser("rotate", help="Replace the keys under a label; the old ones live a day longer")
    ro.add_argument("host_id")
    ro.add_argument("label")
    ro.add_argument("--grace-hours", type=float, default=24.0)
    rv = hs.add_parser("revoke", help="Remove every key under a label at once")
    rv.add_argument("host_id")
    rv.add_argument("label")
    for name in ("disable", "enable"):
        d = hs.add_parser(name)
        d.add_argument("host_id")
    hs.add_parser("list")
    se = hs.add_parser("set", help="Change a host's name or authorize URL")
    se.add_argument("host_id")
    se.add_argument("--name", default=None)
    se.add_argument("--authorize-url", default=None)

    e = sub.add_parser("engine", help="The engine wrapper (normally launched by the run API)")
    e.add_argument("rest", nargs=argparse.REMAINDER)

    u = sub.add_parser("upload", help="Upload a run folder with a job code")
    u.add_argument("rest", nargs=argparse.REMAINDER)

    s = sub.add_parser("submit", help="Create a run with a service key, upload, and optionally wait for results")
    s.add_argument("rest", nargs=argparse.REMAINDER)

    rc = sub.add_parser("run", help="Status, cancel or retry one run with a service key")
    rc.add_argument("rest", nargs=argparse.REMAINDER)

    args = ap.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s", datefmt="%H:%M:%S")
    log = logging.getLogger(__name__)

    if args.command == "runapi":
        import uvicorn
        from .runapi.app import build_aws_service, build_local_service, create_app
        base = args.base_url or f"http://{args.host}:{args.port}"
        if args.backend == "aws":
            base = args.base_url or os.environ.get("SPECIMUX_BASE_URL") or base
            service = build_aws_service(base, engine_extra_args=args.engine_arg)
        else:
            service = build_local_service(args.data_dir, base, dev_key=args.dev_key or None,
                                          engine_extra_args=args.engine_arg)
        result = service.reconcile()
        log.info(f"Reconciled at startup: {result}")
        start_reconcile_loop(service, float(os.environ.get("SPECIMUX_RECONCILE_S", "120")))
        start_cleanup_loop(service, float(os.environ.get("SPECIMUX_CLEANUP_S", "600")))
        app = create_app(service, console=not args.no_console)
        log.info(f"Run API at {base}" + ("" if args.no_console else f", console at {base}/console/"))
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    elif args.command == "hosts":
        from .runapi.service import ServiceError
        svc = _host_service(args)
        try:
            c = args.hosts_command
            if c == "add":
                url = args.authorize_url
                if args.console:
                    if not args.base_url:
                        raise SystemExit("--console needs --base-url (the run API's public URL)")
                    url = args.base_url.rstrip("/") + "/console/authorize"
                host, key = svc.add_host(args.host_id, name=args.name, authorize_url=url, label=args.label)
                print(f"host {host['id']} registered; its key (label {args.label}), shown once:\n\n    {key}\n")
            elif c == "key":
                print(f"new key for {args.host_id} (label {args.label}), shown once:\n\n    "
                      f"{svc.add_key(args.host_id, args.label)}\n")
            elif c == "rotate":
                key = svc.rotate_key(args.host_id, args.label, grace_s=args.grace_hours * 3600)
                print(f"rotated {args.host_id}/{args.label}; the old key works for {args.grace_hours:g} more hours. "
                      f"New key, shown once:\n\n    {key}\n")
            elif c == "revoke":
                print(f"removed {svc.revoke_key(args.host_id, args.label)} key(s)")
            elif c in ("disable", "enable"):
                svc.update_host(args.host_id, disabled=(c == "disable"))
                print(f"{args.host_id} {c}d")
            elif c == "set":
                svc.update_host(args.host_id, name=args.name, authorize_url=args.authorize_url)
                _print_hosts([svc.get_host(args.host_id)])
            elif c == "list":
                _print_hosts(svc.store.list_hosts())
        except ServiceError as ex:
            raise SystemExit(f"error: {ex.message}")
