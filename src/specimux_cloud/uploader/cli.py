"""The uploader: move a run folder to the cloud with a job code.

    specimux-cloud upload --job-code <run>.<secret> --run-api URL <folder>

Watches the folder (or takes it as is with ``--once``), uploads each
FASTQ or POD5 file once it has stopped growing (not those under MinKNOW's
``fastq_fail``/``pod5_fail`` folders unless ``--include-failed``), straight to storage over a
presigned URL from the run API, resumes on restart (a file whose size and
checksum match what storage holds is skipped), forwards MinKNOW's
``final_summary_*.txt`` when it appears, and then calls ``complete`` with
the manifest of everything it uploaded (key, size, ETag) so the run API
can verify the input is exactly what was sent. The uploader talks only to
the run API; it never needs a login.

It names itself and its version in every request's User-Agent, and checks
``/v1/version`` first: a service that no longer serves this version stops
it with the upgrade command before anything is sent (versioning.py).
Installed uploaders are old for a long time, so the run API keeps the
routes they call backward compatible (runapi/app.py).
"""

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import httpx

from ..versioning import UPGRADE_COMMAND, older, uploader_user_agent

logger = logging.getLogger("specimux_cloud.uploader")

INPUT_SUFFIXES = (".fastq", ".fq", ".fastq.gz", ".fq.gz", ".pod5")


FAILED_DIRS = ("fastq_fail", "pod5_fail")   # MinKNOW's reads that failed its quality filter
# Progress while a file is sent: a log line this often, and a report to the
# run API (its run page shows it) this often
PROGRESS_LOG_S = 10.0
PROGRESS_REPORT_S = 15.0
CHUNK = 1 << 20


def _size(n) -> str:
    """Bytes for people: 2.1 GB, 950 MB."""
    n = float(n or 0)
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if n < 1000 or unit == "TB":
            return f"{n:,.0f} {unit}" if unit == "bytes" else f"{n:,.1f} {unit}"
        n /= 1000


def _duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{seconds / 3600:.1f} h"


def _is_input(path: Path) -> bool:
    return path.is_file() and any(path.name.endswith(s) for s in INPUT_SUFFIXES) and not path.name.startswith(".")


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_service(base: str, client: Optional[httpx.Client] = None) -> None:
    """Before anything is sent: stop if the service no longer serves this
    uploader's version (it names a minimum), and say so once if a newer
    one is out. A service that doesn't answer is no reason to stop; the
    upload itself will say what is wrong."""
    from .. import __version__
    try:
        r = (client or httpx).get(f"{base.rstrip('/')}/v1/version", timeout=15.0,
                                  headers={"User-Agent": uploader_user_agent()})
        info = (r.json().get("uploader") or {}) if r.status_code == 200 else {}
    except (httpx.HTTPError, ValueError):
        return
    minimum, latest = info.get("minimum"), info.get("latest")
    if minimum and older(__version__, minimum):
        raise SystemExit(f"This uploader ({__version__}) is too old for this service, which needs {minimum} "
                         f"or later. Upgrade with: {UPGRADE_COMMAND}")
    if latest and older(__version__, latest):
        logger.info(f"specimux-cloud {latest} is available (this is {__version__}): {UPGRADE_COMMAND}")


class Uploader:
    def __init__(self, run_api: str, job_code: str, folder: Path, settle_s: float = 30.0,
                 state_path: Optional[Path] = None, include_failed: bool = False):
        self.base = run_api.rstrip("/")
        self.run_id, _, self.secret = job_code.partition(".")
        if not self.run_id or not self.secret:
            raise SystemExit("A job code looks like <run id>.<secret>")
        self.folder = Path(folder)
        self.settle_s = settle_s
        self.include_failed = include_failed
        self.state_path = state_path or (self.folder / ".specimux-upload.json")
        self.headers = {"Authorization": f"JobCode {job_code}"}
        self.reports = True   # False once the run API turns out not to take progress reports

        self.done: dict[str, dict] = self._load_state()
        # every request names this uploader and its version: the run API
        # refuses one older than its minimum (versioning.py)
        agent = {"User-Agent": uploader_user_agent()}
        self.client = httpx.Client(timeout=httpx.Timeout(600.0, connect=30.0), headers=agent)
        # reports go out while the file's PUT is in flight on self.client
        self.report_client = httpx.Client(timeout=httpx.Timeout(10.0), headers=agent)

    # --- state (resume) ---

    def _load_state(self) -> dict:
        try:
            data = json.loads(self.state_path.read_text())
            if data.get("run_id") == self.run_id:
                return data.get("uploaded", {})
        except (OSError, ValueError):
            pass
        return {}

    def _save_state(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"run_id": self.run_id, "uploaded": self.done}, indent=1))
        tmp.replace(self.state_path)

    # --- one file ---

    def stable(self, path: Path) -> bool:
        """True once the file has not grown for settle_s (MinKNOW closes files
        in one go, but a copy in progress must not be uploaded half-way)."""
        size = path.stat().st_size
        age = time.time() - path.stat().st_mtime
        return size > 0 and age >= self.settle_s

    def upload(self, path: Path) -> dict:
        name = path.name
        size = path.stat().st_size
        md5 = _md5(path)
        prev = self.done.get(name)
        if prev and prev.get("size") == size and prev.get("md5") == md5:
            return prev
        r = self.client.post(f"{self.base}/v1/runs/{self.run_id}/uploads",
                             json={"files": [name]}, headers=self.headers)
        r.raise_for_status()
        target = r.json()["uploads"][name]
        started = time.monotonic()
        put = self.client.put(target["url"], content=self._stream(path, size, started),
                              headers={"Content-Length": str(size)})   # storage takes no chunked body
        put.raise_for_status()
        etag = (put.headers.get("etag") or "").strip('"')
        record = {"key": target["key"], "size": size, "md5": md5, "etag": etag, "uploaded": time.time()}
        self.done[name] = record
        self._save_state()
        took = time.monotonic() - started
        total = sum(r.get("size", 0) for r in self.done.values())
        logger.info(f"Uploaded {name} ({_size(size)} in {_duration(took)}"
                    + (f", {_size(size / took)}/s" if took >= 1 else "")
                    + f"); {len(self.done)} file(s), {_size(total)} uploaded so far")
        return record

    def _stream(self, path: Path, size: int, started: float):
        """The file in chunks, logging progress every PROGRESS_LOG_S and
        reporting it to the run API every PROGRESS_REPORT_S."""
        sent = 0
        logged = reported = started
        with open(path, "rb") as f:
            while chunk := f.read(CHUNK):
                sent += len(chunk)
                yield chunk
                now = time.monotonic()
                rate = sent / (now - started) if now > started else 0.0
                if now - logged >= PROGRESS_LOG_S and sent < size:
                    logged = now
                    left = f", about {_duration((size - sent) / rate)} left" if rate else ""
                    logger.info(f"  {path.name}: {100 * sent / size:.0f}% of {_size(size)}"
                                f" at {_size(rate)}/s{left}")
                if now - reported >= PROGRESS_REPORT_S and sent < size:
                    reported = now
                    self.report_progress(path.name, sent, size, rate)

    def report_progress(self, name: str, sent: int, size: int, rate: float) -> None:
        """Best effort: the run page shows it; an upload never waits on it or
        fails for it, and a run API without the route is asked no more."""
        if not self.reports:
            return
        body = {"file": name, "sent": sent, "size": size, "rate": round(rate, 1),
                "files_done": len(self.done), "bytes_done": sum(r.get("size", 0) for r in self.done.values())}
        try:
            r = self.report_client.post(f"{self.base}/v1/runs/{self.run_id}/upload/progress", json=body,
                                        headers=self.headers)
            if r.status_code in (404, 405):
                self.reports = False
        except httpx.HTTPError:
            pass

    # --- the folder ---

    def pending(self) -> list[Path]:
        files = sorted(p for p in self.folder.rglob("*") if _is_input(p)
                       and (self.include_failed or not any(d in FAILED_DIRS for d in p.relative_to(self.folder).parts[:-1])))
        out = []
        for p in files:
            rec = self.done.get(p.name)
            if rec and rec.get("size") == p.stat().st_size:
                continue
            out.append(p)
        return out

    def final_summary(self) -> Optional[Path]:
        hits = sorted(self.folder.rglob("final_summary_*.txt"))
        return hits[0] if hits else None

    def complete(self) -> dict:
        manifest = [{"key": r["key"], "size": r["size"], "etag": r["etag"]} for r in self.done.values()]
        summary = self.final_summary()
        body = {"manifest": manifest}
        if summary:
            body["final_summary"] = summary.read_text(errors="replace")[-20000:]
        r = self.client.post(f"{self.base}/v1/runs/{self.run_id}/complete", json=body, headers=self.headers)
        r.raise_for_status()
        logger.info(f"Run {self.run_id} complete: {len(manifest)} file(s); state {r.json().get('state')}")
        return r.json()

    def upload_status(self) -> dict:
        r = self.client.get(f"{self.base}/v1/runs/{self.run_id}/upload", headers=self.headers)
        r.raise_for_status()
        return r.json()

    def run(self, once: bool = False, poll_s: float = 5.0, status_s: float = 60.0,
            hint_after_s: float = 600.0) -> dict:
        """Upload until the folder is done: with ``once``, everything there
        now; otherwise until MinKNOW's final summary appears and every file
        is uploaded, or the run is completed another way (the run page's
        button), which the uploader notices within ``status_s``. After
        ``hint_after_s`` with every file up and nothing new (MinKNOW writes
        a file every few minutes while sequencing), it says how to finish."""
        check_service(self.base, self.client)
        told_waiting = False
        idle_since = time.monotonic()
        todo = self.pending()
        if todo:
            logger.info(f"{len(todo)} file(s) to upload ({_size(sum(p.stat().st_size for p in todo))})"
                        + (f"; {len(self.done)} already uploaded" if self.done else ""))
        last_status = time.monotonic()
        while True:
            for p in self.pending():
                if once or self.stable(p):
                    try:
                        self.upload(p)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 409:
                            status = self.upload_status()
                            if not status.get("open"):
                                logger.warning(f"Run {self.run_id} stopped taking uploads (state "
                                               f"{status.get('state')}) before {p.name} was uploaded")
                                return status
                        if 400 <= e.response.status_code < 500:
                            raise  # a bad job code or a closed run: no retry will fix it
                        logger.warning(f"Upload of {p.name} failed ({e}); will retry")
                    except httpx.HTTPError as e:
                        logger.warning(f"Upload of {p.name} failed ({e}); will retry")
            if once or (self.final_summary() is not None and not self.pending()):
                if not self.done:
                    raise SystemExit("Nothing to upload: no FASTQ or POD5 files in the folder")
                return self.complete()
            if self.done and not self.pending():
                if not told_waiting and time.monotonic() - idle_since >= hint_after_s:
                    logger.info(
                        f"All {len(self.done)} file(s) uploaded; waiting for MinKNOW's final_summary. "
                        "If sequencing is finished, press Ctrl+C and run again with --once "
                        "(nothing is uploaded twice), or click 'Upload is complete' on the run page.")
                    told_waiting = True
            else:
                told_waiting = False
                idle_since = time.monotonic()
            if time.monotonic() - last_status >= status_s:
                last_status = time.monotonic()
                try:
                    status = self.upload_status()
                except httpx.HTTPError as e:
                    logger.warning(f"Could not check the run's state ({e}); will retry")
                else:
                    if not status.get("open"):
                        logger.info(f"Run {self.run_id} is no longer taking uploads "
                                    f"(state {status.get('state')}); nothing left to do")
                        return status
            time.sleep(poll_s)


def run(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="specimux-cloud upload", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folder", type=Path, help="The run folder (MinKNOW output, or a folder of FASTQ)")
    ap.add_argument("--job-code", required=True, help="From the job page: <run id>.<secret>")
    ap.add_argument("--run-api", required=True, help="The run API base URL")
    ap.add_argument("--once", action="store_true",
                    help="Upload what is there now and complete, instead of watching for the final summary")
    ap.add_argument("--settle", type=float, default=30.0, help="Seconds a file must be unchanged (default 30)")
    ap.add_argument("--include-failed", action="store_true",
                    help="Also upload reads under MinKNOW's fastq_fail / pod5_fail folders")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()),
                        format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
    if logging.getLogger().level > logging.DEBUG:
        # httpx logs every request line, and a presigned URL carries a session token
        logging.getLogger("httpx").setLevel(logging.WARNING)
    if not args.folder.is_dir():
        ap.error(f"{args.folder} is not a directory")
    up = Uploader(args.run_api, args.job_code, args.folder, settle_s=args.settle,
                  include_failed=args.include_failed)
    try:
        up.run(once=args.once)
    except httpx.HTTPStatusError as e:
        logger.error(f"{e.request.method} {e.request.url}: {e.response.status_code} {e.response.text[:300]}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(run())
