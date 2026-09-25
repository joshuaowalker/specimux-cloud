"""The engine wrapper: what a compute job runs.

Reads its identity from the environment (``SPECIMUX_RUN_ID``,
``SPECIMUX_RUN_API``, ``SPECIMUX_JOB_SECRET``, ``SPECIMUX_GENERATION``,
``SPECIMUX_WORK_DIR``, ``SPECIMUX_SCRATCH``), fetches the job bundle
from the run API, downloads the inputs and the manifest's reads, takes
the generation lease, runs ``specimux-suite`` with the cloud plugin
loaded, packages the output, and reports the exit code and log tail.
Keeping this outside the engine process means an engine crash and a
wrapper failure are distinguishable, and the exit report is sent even
when the engine dies.

Two directories (docs/DESIGN.md "Engine storage"): the engine works in a
fresh directory on the job's local disk (``SPECIMUX_SCRATCH``), where the
demux's appends and the consensus debug files run at disk speed, and
mirrors what the dashboard reads (the event log, consensus and summary
FASTAs, photos) into the run's directory on EFS (``SPECIMUX_WORK_DIR``,
``output/``) with the suite's ``--mirror-dir``. After the engine exits
the wrapper builds the three downloads from the local output and uploads
them over presigned URLs. Nothing is resumed: a relaunch, or a retried
attempt, starts over with a fresh scratch dir and a fresh mirror (the
previous one is kept beside it as ``output.<time>``).

Live mode: the engine watches a directory on local disk; ``LiveFeed``
polls the run API for new uploads, downloads each beside the watch dir and
renames it in, and once the upload is complete and the engine has
demultiplexed every file (the run API's view of its ``specimux.completed``
events) sends it SIGINT, the suite's "finalize and exit". A relaunch
replays every uploaded file from storage into a fresh engine.

Lease: ``lease.json`` in the work dir carries the generation and a
heartbeat; a wrapper refuses to start over a live lease with a higher
generation, and a stale one (heartbeat older than ``LEASE_STALE_S``) is
taken over. On a shared filesystem this is a guard, not a proof — the run
API's generation check on ingest is the fence.
"""

import argparse
import gzip
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from specimux_suite.util import USER_AGENT, atomic_write

from ..packages import PACKAGES, build_zip, package_files
from ..stopsignal import STOPPED_EXIT, STOPPED_REPORT_PATIENCE_S, StopRequested, StopSignal

logger = logging.getLogger("specimux_cloud.engine")

LEASE_STALE_S = 120.0
LEASE_HEARTBEAT_S = 20.0
# A live engine: how often the wrapper asks for new uploads, and the
# suite's settle time for its watch dir (files arrive by atomic rename, so
# a short settle only guards against a slow directory scan)
LIVE_POLL_S = 10.0
LIVE_SETTLE_S = 5


class RunApiClient:
    def __init__(self, base_url: str, run_id: str, job_secret: str):
        self.base = base_url.rstrip("/")
        self.run_id = run_id
        self.headers = {"X-Job-Secret": job_secret, "User-Agent": USER_AGENT,
                        "Content-Type": "application/json"}

    def _call(self, path: str, body: Optional[dict] = None, timeout: float = 60.0) -> dict:
        req = urllib.request.Request(f"{self.base}{path}", headers=self.headers,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     method="POST" if body is not None else "GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read() or b"{}")

    def job_bundle(self) -> dict:
        return self._call(f"/v1/runs/{self.run_id}/job")

    def live_inputs(self, have: list[str]) -> dict:
        return self._call(f"/v1/runs/{self.run_id}/inputs", {"have": sorted(have)}, timeout=60.0)

    def package_uploads(self, generation: int) -> dict:
        return self._call(f"/v1/runs/{self.run_id}/package-uploads", {"generation": generation}, timeout=60.0)

    def report_exit(self, generation: int, exit_code: int, log_tail: str,
                    patience_s: float = 1800.0, packages: Optional[dict] = None) -> dict:
        """Deliver the exit report, retrying for up to ``patience_s`` (the
        run API may be restarting or busy); the report is idempotent on
        the run API side, and a 409 means the run has moved past this
        generation, which is also final."""
        deadline = time.monotonic() + patience_s
        delay = 2.0
        while True:
            try:
                return self._call(f"/v1/runs/{self.run_id}/exit",
                                  {"generation": generation, "exit_code": exit_code, "log_tail": log_tail,
                                   "packages": packages or {}},
                                  timeout=min(120.0, max(5.0, patience_s)))
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    logger.warning("Exit report refused as stale; the run has moved on")
                    return {}
                logger.warning(f"Exit report failed (HTTP {e.code}); retrying")
            except (urllib.error.URLError, OSError) as e:
                logger.warning(f"Exit report failed ({e}); retrying")
            if time.monotonic() >= deadline:
                raise RuntimeError("Could not deliver the exit report")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)


def download(url: str, dest: Path, expected_etag: Optional[str] = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp, open(tmp, "wb") as out:
        shutil.copyfileobj(resp, out, length=1 << 20)
    os.replace(tmp, dest)


class Lease:
    def __init__(self, work_dir: Path, generation: int):
        self.path = work_dir / "lease.json"
        self.generation = generation
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def acquire(self) -> None:
        try:
            current = json.loads(self.path.read_text())
        except (OSError, ValueError):
            current = None
        if current:
            age = time.time() - float(current.get("heartbeat", 0))
            if current.get("generation", 0) > self.generation:
                raise RuntimeError(f"A newer generation ({current['generation']}) holds the lease")
            if current.get("generation", 0) == self.generation and age < LEASE_STALE_S \
                    and current.get("pid") != os.getpid():
                raise RuntimeError(f"Generation {self.generation} is still running (heartbeat {age:.0f}s ago)")
            if age < LEASE_STALE_S and current.get("generation", 0) < self.generation:
                logger.warning(f"Taking over from generation {current.get('generation')} "
                               f"(heartbeat {age:.0f}s ago); its ingest is fenced off")
        self._write()
        self._thread = threading.Thread(target=self._beat, name="lease-heartbeat", daemon=True)
        self._thread.start()

    def _write(self) -> None:
        atomic_write(self.path, json.dumps({
            "generation": self.generation, "pid": os.getpid(), "heartbeat": time.time(),
        }).encode())

    def _beat(self) -> None:
        while not self._stop.wait(LEASE_HEARTBEAT_S):
            try:
                self._write()
            except OSError as e:
                logger.warning(f"Lease heartbeat failed: {e}")

    def release(self) -> None:
        self._stop.set()


ENGINE_EXIT_FILENAME = "engine-exit.json"


def write_engine_exit_record(work_dir: Path, generation: int, exit_code: int, log_tail: str,
                             packages: Optional[dict] = None) -> None:
    """The engine's outcome, durable in the work dir, so a later attempt of
    the same generation reports it rather than running the engine again.
    Written once the packages are uploaded: the local output they came from
    dies with the job."""
    atomic_write(work_dir / ENGINE_EXIT_FILENAME, json.dumps({
        "generation": generation, "exit_code": exit_code, "log_tail": log_tail[-4000:],
        "packages": packages or {}, "finished": time.time()}).encode())


def engine_exit_record(work_dir: Path, generation: int) -> Optional[dict]:
    try:
        rec = json.loads((work_dir / ENGINE_EXIT_FILENAME).read_text())
    except (OSError, ValueError):
        return None
    return rec if rec.get("generation") == generation else None


def redact_secret(cmd: list[str], secret: str) -> list[str]:
    """The command with the job secret masked, for logging."""
    return [c.replace(secret, "***") if secret else c for c in cmd]


def build_engine_command(bundle: dict, work_dir: Path, scratch: Path, run_id: str, run_api: str,
                         job_secret: str, generation: int, extra_args: list[str]) -> list[str]:
    spec = bundle.get("spec") or {}
    inputs = scratch / "input"
    mode = spec.get("mode", "batch")
    cmd = ["specimux-suite", mode, str(inputs / "primers"), str(inputs / "specimens")]
    if mode == "batch":
        cmd.append(str(scratch / "reads.fastq"))
    else:
        # the wrapper delivers live files there (local disk, by atomic rename)
        cmd += [str(scratch / "watch"), "--settle-time", str(LIVE_SETTLE_S)]
    cmd += ["-o", str(scratch / "output"), "--mirror-dir", str(work_dir / "output"),
            "--no-web", "--no-open", "--inat-background",
            # the dashboard is on the internet: photos come from the provider
            "--no-photo-cache"]
    if (inputs / "reference").exists():
        cmd += ["--reference-db", str(inputs / "reference")]
    if spec.get("profile"):
        cmd += ["--profile", str(spec["profile"])]
    for key, flag in (("min_reads", "--min-reads"), ("reprocess_ratio", "--reprocess-ratio")):
        if spec.get(key) is not None:
            cmd += [flag, str(spec[key])]
    # The engine owns its instance: use every vCPU of the job (the suite's
    # default of half the cores is for a shared laptop)
    workers = spec.get("workers") or os.environ.get("SPECIMUX_VCPUS") or os.cpu_count() or 1
    cmd += ["--workers", str(int(workers))]
    cmd += ["--plugin", "cloud",
            "--plugin-opt", f"run_id={run_id}", "--plugin-opt", f"run_api={run_api}",
            "--plugin-opt", f"job_secret={job_secret}", "--plugin-opt", f"generation={generation}"]
    cmd += list(extra_args)
    return cmd


def fresh_mirror(work_dir: Path) -> None:
    """Start the run's EFS mirror over: an earlier attempt's is kept beside
    it as ``output.<time>`` (its event log is that attempt's record), and
    the photo cache moves into the new one."""
    mirror = work_dir / "output"
    # only an attempt that started (it wrote an event log) is kept aside; the
    # run API creates the empty dir when the dashboard is opened early
    if (mirror / "events.jsonl").exists():
        old = work_dir / f"output.{int(time.time())}"
        os.replace(mirror, old)
        mirror.mkdir(parents=True)
        photos = old / "inat_photos"
        if photos.is_dir():
            os.replace(photos, mirror / "inat_photos")
        logger.info(f"Earlier attempt's output kept as {old.name}")
    mirror.mkdir(parents=True, exist_ok=True)


def upload(url: str, path: Path) -> None:
    size = path.stat().st_size
    with open(path, "rb") as f:
        req = urllib.request.Request(url, data=f, method="PUT",
                                     headers={"Content-Length": str(size), "User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=3600) as resp:
            resp.read()


def package_and_upload(api: "RunApiClient", generation: int, scratch: Path, mirror: Path) -> dict:
    """Build the three downloads from the local output (the event log from
    the mirror) and upload them; returns ``{name: {"size": bytes}}``."""
    out = scratch / "output"
    urls = api.package_uploads(generation)
    done = {}
    for name, include in PACKAGES.items():
        if name not in urls:
            continue
        files = package_files(out, include, extra={"events.jsonl": mirror / "events.jsonl"})
        dest = build_zip(scratch / name, files)
        upload(urls[name]["url"], dest)
        done[name] = {"size": dest.stat().st_size, "files": len(files)}
        dest.unlink(missing_ok=True)
        logger.info(f"Uploaded {name}: {done[name]['size']:,} bytes, {len(files)} files")
    return done


def stage_inputs(bundle: dict, work_dir: Path) -> None:
    """Download the run's input files and its reads into the (scratch) work dir."""
    work_dir.mkdir(parents=True, exist_ok=True)
    inputs = work_dir / "input"
    for role, url in (bundle.get("inputs") or {}).items():
        download(url, inputs / role)
    reads = bundle.get("reads") or []
    mode = (bundle.get("spec") or {}).get("mode", "batch")
    if mode == "batch":
        # one FASTQ for `specimux-suite batch`: the manifest's files
        # concatenated, gzipped ones (MinKNOW's .fastq.gz) decompressed
        target = work_dir / "reads.fastq"
        tmp = target.with_name("reads.fastq.part")
        with open(tmp, "wb") as out:
            for r in reads:
                part = work_dir / "downloads" / r["name"]
                download(r["url"], part, r.get("etag"))
                opener = gzip.open if part.name.endswith(".gz") else open
                with opener(part, "rb") as f:
                    first = f.read(1)
                    if first != b"@":
                        logger.warning(f"{r['name']} does not look like FASTQ")
                    out.write(first)
                    shutil.copyfileobj(f, out, length=1 << 20)
                part.unlink()
        os.replace(tmp, target)
        shutil.rmtree(work_dir / "downloads", ignore_errors=True)
    else:
        (work_dir / "watch").mkdir(parents=True, exist_ok=True)


class LiveFeed:
    """A live engine's input: new uploads go into the watch dir (download
    beside it, then rename, so the engine's watcher never sees a partial
    file). Once the upload is complete and the engine has demultiplexed
    every file of it (``specimux.completed`` for each, as the run API saw
    through ingest), the engine is sent SIGINT — the suite's live "finalize
    and exit" — once."""

    def __init__(self, api: "RunApiClient", scratch: Path):
        self.api = api
        self.watch = scratch / "watch"
        self.incoming = scratch / "incoming"
        self.have: set[str] = set()
        self.finalizing = False

    def step(self, proc) -> None:
        body = self.api.live_inputs(sorted(self.have))
        for f in body.get("files") or []:
            download(f["url"], self.incoming / f["name"], f.get("etag"))
            os.replace(self.incoming / f["name"], self.watch / f["name"])
            self.have.add(f["name"])
            logger.info(f"Delivered {f['name']} ({f.get('size', 0):,} bytes)")
        names = set(body.get("names") or [])
        if body.get("complete") and not self.finalizing and names <= self.have \
                and names <= set(body.get("ingested") or []):
            logger.info(f"Upload complete and all {len(names)} files demultiplexed: finalizing the engine")
            proc.send_signal(signal.SIGINT)
            self.finalizing = True

    def run(self, proc, poll_s: float = LIVE_POLL_S) -> int:
        """Feed until the engine exits; returns its exit code."""
        while True:
            try:
                self.step(proc)
            except (urllib.error.URLError, OSError, ValueError) as e:
                logger.warning(f"Live input poll failed ({e}); retrying")
            try:
                return proc.wait(timeout=poll_s)
            except subprocess.TimeoutExpired:
                continue


def run(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="specimux-cloud-engine", description=__doc__)
    ap.add_argument("--run-id", default=os.environ.get("SPECIMUX_RUN_ID"))
    ap.add_argument("--run-api", default=os.environ.get("SPECIMUX_RUN_API"))
    ap.add_argument("--job-secret", default=os.environ.get("SPECIMUX_JOB_SECRET"))
    ap.add_argument("--generation", type=int, default=int(os.environ.get("SPECIMUX_GENERATION", "0") or 0))
    ap.add_argument("--work-dir", type=Path, default=Path(os.environ.get("SPECIMUX_WORK_DIR", "") or "."))
    ap.add_argument("--scratch", type=Path,
                    default=Path(os.environ["SPECIMUX_SCRATCH"]) if os.environ.get("SPECIMUX_SCRATCH") else None,
                    help="Local disk for the engine's work (default: a temp dir)")
    ap.add_argument("--engine-arg", action="append", default=[], help="Extra argument for specimux-suite")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
    if not (args.run_id and args.run_api and args.job_secret and args.generation):
        ap.error("run id, run API, job secret and generation are required (flags or SPECIMUX_* env)")

    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    api = RunApiClient(args.run_api, args.run_id, args.job_secret)
    stop = StopSignal().install()
    lease = Lease(work_dir, args.generation)
    exit_code = 1
    tail = ""
    packages: dict = {}
    # resolved: a watch dir reached through a symlink (macOS's /var) gets
    # its files reported under two names (the suite dedupes by real path
    # only from 0.3.4)
    scratch = (args.scratch or Path(tempfile.gettempdir())).resolve() / f"specimux-{args.run_id}-g{args.generation}"
    # A previous attempt of this generation whose engine already finished
    # (its report never landed, or the host died between the two): report
    # that result instead of running the engine again on a finished dir.
    done = engine_exit_record(work_dir, args.generation)
    if done is not None:
        logger.info(f"Engine already exited {done['exit_code']} in a previous attempt; reporting it")
        try:
            api.report_exit(args.generation, int(done["exit_code"]), str(done.get("log_tail") or ""),
                            packages=done.get("packages") or {})
        except Exception:
            logger.exception("Exit report not delivered")
            return 1
        return int(done["exit_code"])
    try:
        bundle = api.job_bundle()
        if int(bundle.get("generation", 0)) != args.generation:
            raise RuntimeError(f"Run is at generation {bundle.get('generation')}, this job is {args.generation}")
        lease.acquire()
        shutil.rmtree(scratch, ignore_errors=True)
        scratch.mkdir(parents=True)
        fresh_mirror(work_dir)
        stage_inputs(bundle, scratch)
        live = (bundle.get("spec") or {}).get("mode", "batch") == "live"
        cmd = build_engine_command(bundle, work_dir, scratch, args.run_id, args.run_api, args.job_secret,
                                   args.generation, args.engine_arg)
        # (never the job secret: the log goes to CloudWatch)
        logger.info("Starting engine: " + " ".join(redact_secret(cmd, args.job_secret)))
        engine_log = work_dir / "engine.log"
        with open(engine_log, "ab") as log:
            proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                    cwd=str(work_dir))
            stop.child = proc
            exit_code = LiveFeed(api, scratch).run(proc) if live else proc.wait()
        tail = engine_log.read_text(errors="replace")[-4000:]
        if stop.stopped:
            # not a finished engine: no exit record, so a retry of this
            # generation runs it again instead of reporting this
            exit_code = STOPPED_EXIT
            tail = (tail + "\nwrapper: stopped (SIGTERM)")[-4000:]
            logger.warning("Stopped by SIGTERM; engine terminated")
        else:
            logger.info(f"Engine exited {exit_code}")
            try:
                packages = package_and_upload(api, args.generation, scratch, work_dir / "output")
            except StopRequested:
                raise
            except Exception as e:
                # the run API seals from the mirror instead
                logger.exception("Packaging failed")
                tail = (tail + f"\nwrapper: packaging failed: {e}")[-4000:]
            write_engine_exit_record(work_dir, args.generation, exit_code, tail, packages)
    except StopRequested as e:
        logger.warning("Stopped by SIGTERM before the engine started")
        tail = (tail + f"\nwrapper: {e}")[-4000:]
        exit_code = STOPPED_EXIT
    except Exception as e:
        logger.exception("Wrapper failed")
        tail = (tail + f"\nwrapper: {e}")[-4000:]
        exit_code = exit_code or 1
    finally:
        lease.release()
        if not os.environ.get("SPECIMUX_KEEP_SCRATCH"):
            shutil.rmtree(scratch, ignore_errors=True)   # the instance runs other jobs next
    stop.shield()
    try:
        if stop.stopped:
            api.report_exit(args.generation, exit_code, tail, patience_s=STOPPED_REPORT_PATIENCE_S)
        else:
            api.report_exit(args.generation, exit_code, tail, packages=packages)
    except Exception:
        logger.exception("Exit report not delivered; the run API will reconcile from the job state")
        return exit_code or 1
    return exit_code


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
