"""Submit a run from the command line, as a host would, with a service key:

    specimux-cloud submit --run-api URL --service-key KEY \\
        --primers primers.fasta --specimens Index.txt [--reference refs.fasta] \\
        [--profile NAME] [--min-reads N] [--wait [--results DIR]] <folder or FASTQ/POD5 files>

Creates the run through the job API, uploads the reads with the job code
it was given (the uploader, ``--once``), and prints the run id, the
dashboard URL and the console page. With ``--wait`` it polls status until
the run is sealed or failed and downloads results.zip. The whole path a
user takes through mycomap.org or the console, scriptable.

``--live`` creates a live run instead: the engine starts with the first
upload and processes files as they arrive, and the uploader watches the
folder (a MinKNOW run folder) until MinKNOW writes its final summary.

POD5 input (detected from the files, or ``--input pod5``) is basecalled
by the service first; ``--model``, ``--min-length``, ``--max-length`` and
``--min-qscore`` set the basecalling (defaults: the service's).

``--rerun-of RUN`` runs a finished run again as a new run, over its input,
with nothing uploaded: ``--start engine`` (the default) over its FASTQ (a
POD5 run's basecalled reads), ``--start basecall`` from its POD5 with the
basecalling options given. Files and settings not given are the source
run's; ``--no-reference`` leaves its reference out.

    specimux-cloud submit --run-api URL --rerun-of r7503b970 --primers fixed.fasta

``--drive-folder LINK`` takes the input from a Google Drive folder shared
with "Anyone with the link" instead of local files: the service copies
it (``--input`` says which files, default pod5).
"""

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import httpx

from ..packages import download_name
from .cli import Uploader, check_service

logger = logging.getLogger("specimux_cloud.submit")


def _has_pod5(path: Path) -> bool:
    if path.is_dir():
        return any(p.suffix.lower() == ".pod5" for p in path.rglob("*"))
    return path.suffix.lower() == ".pod5"


def run(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="specimux-cloud submit", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inputs", nargs="*", type=Path,
                    help="A folder of FASTQ or POD5 files, or the files (none with --rerun-of)")
    ap.add_argument("--run-api", required=True, help="The run API base URL")
    ap.add_argument("--service-key", default=os.environ.get("SPECIMUX_SERVICE_KEY"),
                    help="A host's service key (default: SPECIMUX_SERVICE_KEY)")
    ap.add_argument("--primers", type=Path, help="Required, except with --rerun-of")
    ap.add_argument("--specimens", type=Path, help="Required, except with --rerun-of")
    ap.add_argument("--reference", type=Path, default=None)
    ap.add_argument("--rerun-of", default=None, metavar="RUN",
                    help="Run a finished run again over its input (nothing is uploaded)")
    ap.add_argument("--start", choices=["engine", "basecall"], default="engine",
                    help="With --rerun-of: where the new run starts (default: the engine)")
    ap.add_argument("--drive-folder", default=None, metavar="LINK",
                    help="Copy the input from this Google Drive folder (shared with anyone with the link) "
                         "instead of uploading local files")
    ap.add_argument("--no-reference", action="store_true",
                    help="With --rerun-of: leave out the source run's reference")
    ap.add_argument("--name", default=None,
                    help="The run's name (up to 100 characters); the downloads are named after it, "
                         "e.g. Run150 -> Run150_Summary.zip")
    ap.add_argument("--profile", default=None, help="Default: default (a rerun: the source's)")
    ap.add_argument("--min-reads", type=int, default=None, help="Default: 10 (a rerun: the source's)")
    ap.add_argument("--vcpus", type=int, default=None, help="Engine job size (default: the service's)")
    ap.add_argument("--input", choices=["fastq", "pod5"], default=None,
                    help="Input kind (default: pod5 if any input file is .pod5, else fastq)")
    ap.add_argument("--model", default=None, help="POD5: dorado model complex, e.g. sup@v5.0.0")
    ap.add_argument("--min-length", type=int, default=None, help="POD5: shortest read kept")
    ap.add_argument("--max-length", type=int, default=None, help="POD5: longest read kept")
    ap.add_argument("--min-qscore", type=float, default=None, help="POD5: dorado's --min-qscore")
    ap.add_argument("--user", default=None, help="Recorded as the submitter (default: the key's label)")
    ap.add_argument("--live", action="store_true",
                    help="A live run: process files as they arrive; the uploader watches the folder "
                         "until MinKNOW's final summary")
    ap.add_argument("--settle", type=float, default=30.0, help="Uploader: seconds a file must be unchanged")
    ap.add_argument("--include-failed", action="store_true",
                    help="Uploader: also upload MinKNOW's fastq_fail / pod5_fail reads")
    ap.add_argument("--wait", action="store_true", help="Poll until the run is sealed, then download results")
    ap.add_argument("--results", type=Path, default=Path("."), help="With --wait: where results.zip goes")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)
    if not args.service_key:
        ap.error("--service-key or SPECIMUX_SERVICE_KEY is required")
    logging.basicConfig(level=getattr(logging, args.log_level.upper()),
                        format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
    if logging.getLogger().level > logging.DEBUG:
        # httpx logs every request line, and a presigned URL carries a session token
        logging.getLogger("httpx").setLevel(logging.WARNING)
    base = args.run_api.rstrip("/")
    key = {"X-Service-Key": args.service_key}

    if args.rerun_of:
        if args.inputs or args.live or args.input:
            ap.error("--rerun-of runs over the source run's input: no files, --live or --input")
        spec = {"rerun_of": args.rerun_of, "start": args.start}
        for field, value in (("profile", args.profile), ("min_reads", args.min_reads)):
            if value is not None:
                spec[field] = value
        kind = "pod5" if args.start == "basecall" else None
    elif args.drive_folder:
        if args.inputs or args.live or not args.primers or not args.specimens:
            ap.error("--drive-folder takes --primers and --specimens, and no input files or --live")
        kind = args.input or "pod5"
        spec = {"mode": "batch", "input": kind, "profile": args.profile or "default",
                "min_reads": 10 if args.min_reads is None else args.min_reads,
                "source": {"google_drive": args.drive_folder}}
    else:
        if not args.inputs or not args.primers or not args.specimens:
            ap.error("the input files, --primers and --specimens are required (except with --rerun-of)")
        if args.no_reference:
            ap.error("--no-reference applies to --rerun-of")
        kind = args.input or ("pod5" if any(_has_pod5(p) for p in args.inputs) else "fastq")
        if args.live and (kind != "fastq" or len(args.inputs) != 1 or not args.inputs[0].is_dir()):
            logger.error("--live takes one folder of FASTQ (live POD5 is not built yet)")
            return 2
        spec = {"mode": "live" if args.live else "batch", "input": kind, "profile": args.profile or "default",
                "min_reads": 10 if args.min_reads is None else args.min_reads}
    if args.name:
        spec["name"] = args.name
    if args.vcpus:
        spec["vcpus"] = args.vcpus
    basecall = {k: v for k, v in (("model", args.model), ("min_length", args.min_length),
                                  ("max_length", args.max_length), ("min_qscore", args.min_qscore)) if v is not None}
    if basecall and kind != "pod5":
        logger.error("--model/--min-length/--max-length/--min-qscore apply to POD5 input "
                     "(a rerun: with --start basecall)")
        return 2
    if kind == "pod5":
        spec["basecall"] = basecall
    check_service(base)   # a too-old uploader stops before it creates a run
    files = {role: (path.name, path.read_bytes())
             for role, path in (("primers", args.primers), ("specimens", args.specimens)) if path}
    data = {"spec": json.dumps(spec), "client_token": f"submit-{time.time()}"}
    if args.no_reference:
        data["reference_sha256"] = "none"
    if args.reference:
        # the service keeps references by content: send the file only if it lacks this one
        reference = args.reference.read_bytes()
        data["reference_sha256"] = hashlib.sha256(reference).hexdigest()
        known = httpx.get(f"{base}/v1/references/sha256/{data['reference_sha256']}", headers=key, timeout=30.0)
        if known.status_code == 200:
            logger.info(f"The service already has {args.reference.name}; not sending it")
        else:
            files["reference"] = (args.reference.name, reference)
    if args.user:
        data["user_id"] = args.user
    # the reference database can be tens of MB over a slow uplink
    with httpx.Client(timeout=httpx.Timeout(900.0, connect=30.0)) as c:
        r = c.post(f"{base}/v1/runs", headers=key, data=data, files=files)
        if r.status_code != 200:
            logger.error(f"create failed: {r.status_code} {r.text[:300]}")
            return 1
        run = r.json()
    rid = run["id"]
    print(f"run {rid} created; dashboard {run['dashboard_url']}; console {base}/console/runs/{rid}", flush=True)
    if args.rerun_of:
        print(f"run {rid} is a rerun of {args.rerun_of} from {'basecalling' if args.start == 'basecall' else 'the engine'}: "
              f"{run['state']}", flush=True)
        return wait_for_results(base, key, rid, args.results) if args.wait else 0
    if args.drive_folder:
        d = run.get("drive") or {}
        print(f"run {rid}: copying {d.get('files')} file(s), {d.get('bytes', 0):,} bytes, from Google Drive", flush=True)
        return wait_for_results(base, key, rid, args.results) if args.wait else 0
    code = run["job_code"]

    # the uploader takes a folder: stage loose files into a temp one
    folder = args.inputs[0] if len(args.inputs) == 1 and args.inputs[0].is_dir() else None
    staged = None
    if folder is None:
        staged = Path(tempfile.mkdtemp(prefix="specimux-submit-"))
        for p in args.inputs:
            if not p.is_file():
                logger.error(f"{p} is not a file")
                return 1
            shutil.copy2(p, staged / p.name)
        folder = staged
    try:
        Uploader(base, code, folder, settle_s=args.settle,
                 include_failed=args.include_failed).run(once=not args.live)
    finally:
        if staged:
            shutil.rmtree(staged, ignore_errors=True)
    if not args.wait:
        return 0
    return wait_for_results(base, key, rid, args.results)


def wait_for_results(base: str, key: dict, rid: str, results: Path) -> int:
    with httpx.Client(timeout=60.0) as c:
        last = None
        while True:
            st = c.get(f"{base}/v1/runs/{rid}", headers=key).json()
            if st.get("state") != last:
                logger.info(f"run {rid}: {st.get('state')}")
                last = st.get("state")
            if st.get("state") in ("sealed", "failed", "incomplete") and st.get("sealed"):
                break
            time.sleep(15)
        if st.get("sealed", {}).get("error") or not st["sealed"].get("results"):
            logger.error(f"run {rid} ended {st['state']} without a results package: {st.get('sealed')}")
            return 1
        r = c.get(f"{base}/v1/runs/{rid}/results.zip", headers=key, follow_redirects=True)
        r.raise_for_status()
        results.mkdir(parents=True, exist_ok=True)
        out = results / download_name(st, "results")   # as the browser saves it: Run150_Summary.zip
        out.write_bytes(r.content)
        print(f"run {rid} {st['state']} (engine exit {st.get('exit', {}).get('code')}); results in {out}")
        return 0 if st["state"] == "sealed" else 1


if __name__ == "__main__":
    sys.exit(run())
