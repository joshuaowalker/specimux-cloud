"""The dorado wrapper: what the basecalling job runs (docs/DESIGN.md
"Basecalling stage").

Reads its identity from the environment (``SPECIMUX_RUN_ID``,
``SPECIMUX_RUN_API``, ``SPECIMUX_JOB_SECRET``, ``SPECIMUX_GENERATION``),
fetches the job bundle from the run API, and for each POD5 file of the
manifest that no earlier attempt delivered: downloads it, runs ``dorado
basecaller`` with the run's model (``--no-trim``, FASTQ out, an optional
qscore floor), keeps the reads inside the run's length window, PUTs the
FASTQ to the presigned URL the bundle gave for it, and reports the file
to the run API. Files are processed one at a time so scratch space stays
at one POD5 plus its FASTQ. The exit report closes the job; the run API
judges success by the storage listing, not by this report alone.

Standalone on purpose: only the standard library, so the dorado image is
the dorado tarball, its models, and this module.

Environment: ``SPECIMUX_DORADO_BIN`` (default ``dorado``),
``SPECIMUX_DORADO_DEVICE`` (default ``cuda:all``; ``cpu`` for a test),
``SPECIMUX_SCRATCH`` (default a temp dir), ``DORADO_MODELS_DIRECTORY``
(set by the image; dorado resolves the model complex there without a
download).
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from specimux_cloud.stopsignal import STOPPED_EXIT, STOPPED_REPORT_PATIENCE_S, StopRequested, StopSignal

logger = logging.getLogger("specimux_cloud.dorado")

USER_AGENT = "specimux-cloud-dorado"
# Progress while a file is basecalled: reads called so far (counted in
# dorado's FASTQ output) and an estimate of the file's reads from its size,
# reported this often. POD5 bytes per read until a file of this job gives
# the real figure: 9.5k to 10.4k per file on full-ITS runs.
PROGRESS_S = 30.0
DEFAULT_BYTES_PER_READ = 10_000


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

    def report_basecalled(self, generation: int, name: str, key: str, reads_in: int, reads_out: int) -> dict:
        return self._retrying(f"/v1/runs/{self.run_id}/basecalled",
                              {"generation": generation, "name": name, "key": key,
                               "reads_in": reads_in, "reads_out": reads_out}, patience_s=600.0)

    def report_progress(self, generation: int, name: str, reads: int, estimate: int) -> None:
        """Best effort, for the run page: never retried, never fatal."""
        try:
            self._call(f"/v1/runs/{self.run_id}/basecall-progress",
                       {"generation": generation, "file": name, "reads": reads, "estimate": estimate},
                       timeout=10.0)
        except Exception as e:
            logger.debug(f"Progress report not delivered: {e}")

    def report_exit(self, generation: int, exit_code: int, log_tail: str, patience_s: float = 1800.0) -> dict:
        return self._retrying(f"/v1/runs/{self.run_id}/exit",
                              {"generation": generation, "exit_code": exit_code, "log_tail": log_tail},
                              patience_s=patience_s)

    def _retrying(self, path: str, body: dict, patience_s: float) -> dict:
        """POST, retrying for up to ``patience_s`` (the run API may be
        restarting or busy). A 4xx other than 408/429 is final: the run
        has moved on, or the report is wrong, and repeating it changes
        nothing."""
        deadline = time.monotonic() + patience_s
        delay = 2.0
        while True:
            try:
                return self._call(path, body, timeout=min(120.0, max(5.0, patience_s)))
            except urllib.error.HTTPError as e:
                if 400 <= e.code < 500 and e.code not in (408, 429):
                    detail = e.read().decode(errors="replace")[:300]
                    raise RuntimeError(f"{path} refused ({e.code}): {detail}")
                logger.warning(f"{path} failed (HTTP {e.code}); retrying")
            except (urllib.error.URLError, OSError) as e:
                logger.warning(f"{path} failed ({e}); retrying")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Could not deliver {path}")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=600) as resp, open(tmp, "wb") as out:
        shutil.copyfileobj(resp, out, length=1 << 20)
    os.replace(tmp, dest)


def upload(url: str, path: Path) -> None:
    """PUT a file to a presigned URL, streamed."""
    size = path.stat().st_size
    with open(path, "rb") as f:
        req = urllib.request.Request(url, data=f, method="PUT",
                                     headers={"User-Agent": USER_AGENT, "Content-Length": str(size),
                                              "Content-Type": "application/octet-stream"})
        with urllib.request.urlopen(req, timeout=3600) as resp:
            if resp.status not in (200, 201, 204):
                raise RuntimeError(f"PUT {path.name} returned {resp.status}")


def filter_fastq(src: Path, dest: Path, min_length: int = 0, max_length: int = 0) -> tuple[int, int]:
    """Copy the reads whose sequence length lies within [min, max] (a max
    of 0 means no upper bound). Returns (reads in, reads out)."""
    reads_in = reads_out = 0
    tmp = dest.with_name(dest.name + ".part")
    with open(src, "rb") as f, open(tmp, "wb") as out:
        while True:
            header = f.readline()
            if not header:
                break
            seq = f.readline()
            plus = f.readline()
            qual = f.readline()
            if not qual:
                raise ValueError(f"{src.name}: truncated FASTQ record at read {reads_in + 1}")
            reads_in += 1
            n = len(seq.rstrip(b"\r\n"))
            if n < min_length or (max_length and n > max_length):
                continue
            out.write(header)
            out.write(seq)
            out.write(plus)
            out.write(qual)
            reads_out += 1
    os.replace(tmp, dest)
    return reads_in, reads_out


def build_dorado_command(basecall: dict, pod5: Path, device: str, binary: str = "dorado") -> list[str]:
    cmd = [binary, "basecaller", str(basecall.get("model") or "sup"), str(pod5), "--emit-fastq", "--no-trim",
           "--device", device]
    if basecall.get("min_qscore") is not None:
        cmd += ["--min-qscore", str(basecall["min_qscore"])]
    return cmd


class FastqCounter:
    """Reads in a FASTQ being written, counted incrementally (four lines a
    record; only the bytes added since the last count are read)."""

    def __init__(self, path: Path):
        self.path = path
        self.offset = 0
        self.lines = 0

    def reads(self) -> int:
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                while chunk := f.read(1 << 20):
                    self.lines += chunk.count(b"\n")
                    self.offset += len(chunk)
        except OSError:
            pass
        return self.lines // 4


def basecall_file(entry: dict, bundle: dict, scratch: Path, device: str, binary: str, log,
                  stop: Optional[StopSignal] = None, progress=None) -> tuple[int, int]:
    """One POD5 file: download, basecall, filter, upload. Returns the read
    counts (before and after the length filter). ``progress(reads)`` is
    called every PROGRESS_S while dorado runs."""
    name = entry["name"]
    out_name = (name[:-5] if name.lower().endswith(".pod5") else name) + ".fastq"
    target = bundle["fastq_uploads"][out_name]
    basecall = bundle.get("basecall") or {}
    pod5 = scratch / name
    raw = scratch / (out_name + ".raw")
    filtered = scratch / out_name
    try:
        logger.info(f"{name}: downloading ({entry.get('size', 0):,} bytes)")
        download(entry["url"], pod5)
        cmd = build_dorado_command(basecall, pod5, device, binary)
        logger.info(f"{name}: " + " ".join(cmd))
        log.write(f"\n== {name}: {' '.join(cmd)}\n".encode())
        log.flush()
        started = time.monotonic()
        with open(raw, "wb") as out:
            proc = subprocess.Popen(cmd, stdout=out, stderr=log, stdin=subprocess.DEVNULL)
            if stop is not None:
                stop.child = proc
            try:
                counter = FastqCounter(raw)
                while True:
                    try:
                        rc = proc.wait(timeout=PROGRESS_S)
                        break
                    except subprocess.TimeoutExpired:
                        if progress is not None:
                            progress(counter.reads())
            finally:
                if stop is not None:
                    stop.child = None
        if stop is not None and stop.stopped:
            raise StopRequested(f"stopped (SIGTERM) while basecalling {name}")
        if rc != 0:
            raise RuntimeError(f"dorado exited {rc} on {name}")
        reads_in, reads_out = filter_fastq(raw, filtered, int(basecall.get("min_length") or 0),
                                           int(basecall.get("max_length") or 0))
        logger.info(f"{name}: {reads_in:,} reads called in {time.monotonic() - started:.0f}s, "
                    f"{reads_out:,} within {basecall.get('min_length')}-{basecall.get('max_length')}; uploading")
        upload(target["url"], filtered)
        return reads_in, reads_out
    finally:
        for p in (pod5, raw, filtered):
            p.unlink(missing_ok=True)


def run(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="specimux-cloud dorado", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-id", default=os.environ.get("SPECIMUX_RUN_ID"))
    ap.add_argument("--run-api", default=os.environ.get("SPECIMUX_RUN_API"))
    ap.add_argument("--job-secret", default=os.environ.get("SPECIMUX_JOB_SECRET"))
    ap.add_argument("--generation", type=int, default=int(os.environ.get("SPECIMUX_GENERATION", "0") or 0))
    ap.add_argument("--scratch", type=Path, default=Path(os.environ["SPECIMUX_SCRATCH"]) if os.environ.get("SPECIMUX_SCRATCH") else None)
    ap.add_argument("--dorado", default=os.environ.get("SPECIMUX_DORADO_BIN", "dorado"))
    ap.add_argument("--device", default=os.environ.get("SPECIMUX_DORADO_DEVICE", "cuda:all"))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
    if not (args.run_id and args.run_api and args.job_secret and args.generation):
        ap.error("run id, run API, job secret and generation are required (flags or SPECIMUX_* env)")

    api = RunApiClient(args.run_api, args.run_id, args.job_secret)
    # its own directory under a scratch root other jobs may share (the
    # local stack runs several on one disk): files are named by POD5, and
    # two runs can hold the same POD5
    scratch = (args.scratch or Path(tempfile.gettempdir())).resolve() / f"specimux-dorado-{args.run_id}-g{args.generation}"
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    log_path = scratch / "dorado.log"
    stop = StopSignal().install()
    exit_code = 0
    tail = ""
    try:
        bundle = api.job_bundle()
        if int(bundle.get("generation", 0)) != args.generation:
            raise RuntimeError(f"Run is at generation {bundle.get('generation')}, this job is {args.generation}")
        if "pod5" not in bundle:
            raise RuntimeError("This run has no POD5 input to basecall")
        done = set(bundle.get("basecalled") or [])
        todo = [e for e in sorted(bundle["pod5"], key=lambda e: e["name"])
                if (e["name"][:-5] if e["name"].lower().endswith(".pod5") else e["name"]) + ".fastq" not in done]
        logger.info(f"{len(bundle['pod5'])} POD5 files, {len(done)} already basecalled, {len(todo)} to do; "
                    f"model {bundle.get('basecall', {}).get('model')} on {args.device}")
        bytes_per_read = DEFAULT_BYTES_PER_READ
        with open(log_path, "ab") as log:
            for i, entry in enumerate(todo, 1):
                estimate = max(1, int((entry.get("size") or 0) / bytes_per_read))
                report = (lambda reads, name=entry["name"], est=estimate:
                          api.report_progress(args.generation, name, reads, est))
                reads_in, reads_out = basecall_file(entry, bundle, scratch, args.device, args.dorado, log, stop,
                                                    progress=report)
                if reads_in and entry.get("size"):
                    bytes_per_read = entry["size"] / reads_in   # this run's own reads, for the next estimate
                out_name = (entry["name"][:-5] if entry["name"].lower().endswith(".pod5") else entry["name"]) + ".fastq"
                api.report_basecalled(args.generation, out_name, bundle["fastq_uploads"][out_name]["key"],
                                      reads_in, reads_out)
                logger.info(f"{i}/{len(todo)} delivered: {out_name}")
    except StopRequested as e:
        # delivered files stay; a relaunch skips them
        logger.warning(f"Basecalling {e}")
        exit_code = STOPPED_EXIT
        tail = f"wrapper: {e}"
    except Exception as e:
        logger.exception("Basecalling failed")
        exit_code = 1
        tail = f"wrapper: {e}"
    stop.shield()
    try:
        tail = (log_path.read_text(errors="replace")[-3500:] + "\n" + tail) if log_path.exists() else tail
    except OSError:
        pass
    try:
        api.report_exit(args.generation, exit_code, tail[-4000:],
                        **({"patience_s": STOPPED_REPORT_PATIENCE_S} if stop.stopped else {}))
    except Exception:
        logger.exception("Exit report not delivered; the run API will reconcile from the job state")
        return exit_code or 1
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return exit_code


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
