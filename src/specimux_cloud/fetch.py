"""The fetch job: copies a run's input from a public Google Drive folder
to its archive (docs/DESIGN.md "Input from Google Drive").

It is an uploader whose folder is in Drive. It reads its identity from
the environment (``SPECIMUX_RUN_ID``, ``SPECIMUX_RUN_API``,
``SPECIMUX_JOB_SECRET``, ``SPECIMUX_GENERATION``, and the run's job code
``SPECIMUX_JOB_CODE``), takes the folder's files from the job bundle (the
listing made when the run was created, each with a download URL), and for
each one the archive doesn't already hold streams it from Drive straight
into a presigned PUT, through the uploader's own routes, checking the MD5
Drive reports. Nothing touches the local disk. Then it completes the
upload as an uploader does, which starts basecalling or the engine, and
reports its exit.
"""

import argparse
import collections
import hashlib
import logging
import os
import sys
import time
from typing import Optional

import httpx

from .drive import KEY_HEADER, error_reason
from .stopsignal import STOPPED_EXIT, STOPPED_REPORT_PATIENCE_S, StopRequested, StopSignal
from .uploader.cli import CHUNK, PROGRESS_REPORT_S, _duration, _size
from .versioning import uploader_user_agent

logger = logging.getLogger("specimux_cloud.fetch")

# Attempts per file, and the pause before each retry: a dropped download
# or a busy Drive usually clears within minutes
RETRY_PAUSES_S = (10, 30, 60, 120, 300)
LOG_S = 30.0


class FetchError(Exception):
    """A file can't be copied; ``final`` when retrying won't help."""

    def __init__(self, message: str, final: bool = False):
        super().__init__(message)
        self.final = final


class Fetcher:
    def __init__(self, run_api: str, run_id: str, job_code: str, job_secret: str, generation: int,
                 client: Optional[httpx.Client] = None, pauses=RETRY_PAUSES_S):
        self.base = run_api.rstrip("/")
        self.run_id = run_id
        self.generation = generation
        self.code_headers = {"Authorization": f"JobCode {job_code}"}
        self.job_headers = {"X-Job-Secret": job_secret}
        agent = {"User-Agent": uploader_user_agent()}
        self.client = client or httpx.Client(timeout=httpx.Timeout(600.0, connect=30.0), headers=agent)
        self.client.headers.update(agent)
        self.pauses = pauses
        self.done: dict[str, dict] = {}
        self.files: list[dict] = []
        self.api_key = ""

    # --- the run API ---

    def bundle(self) -> dict:
        r = self.client.get(f"{self.base}/v1/runs/{self.run_id}/job", headers=self.job_headers)
        r.raise_for_status()
        return r.json()

    def report_progress(self, name: str, sent: int, size: int, rate: float) -> None:
        """Best effort, as the uploader's: the run page and the dashboard
        banner show the whole copy."""
        body = {"file": name, "sent": sent, "size": size, "rate": round(rate, 1),
                "files_done": len(self.done), "bytes_done": sum(r["size"] for r in self.done.values()),
                "files_total": len(self.files), "bytes_total": sum(f["size"] for f in self.files)}
        try:
            self.client.post(f"{self.base}/v1/runs/{self.run_id}/upload/progress", json=body,
                             headers=self.code_headers, timeout=10.0)
        except httpx.HTTPError:
            pass

    def complete(self) -> dict:
        manifest = [{"key": r["key"], "size": r["size"], "etag": r["etag"]} for r in self.done.values()]
        r = self.client.post(f"{self.base}/v1/runs/{self.run_id}/complete", json={"manifest": manifest},
                             headers=self.code_headers)
        r.raise_for_status()
        return r.json()

    def report_exit(self, code: int, log_tail: str, patience_s: float = 600.0) -> None:
        deadline = time.monotonic() + patience_s
        while True:
            try:
                r = self.client.post(f"{self.base}/v1/runs/{self.run_id}/exit",
                                     json={"generation": self.generation, "exit_code": code, "log_tail": log_tail},
                                     headers=self.job_headers, timeout=30.0)
                if r.status_code < 500:
                    return
            except httpx.HTTPError as e:
                logger.warning(f"Exit report failed ({e})")
            if time.monotonic() > deadline:
                logger.error("Gave up reporting the exit; the run API's reconcile will judge the job")
                return
            time.sleep(5)

    # --- one file ---

    def copy(self, f: dict) -> dict:
        """Stream one Drive file into the archive; the archive's record of it."""
        r = self.client.post(f"{self.base}/v1/runs/{self.run_id}/uploads", json={"files": [f["name"]]},
                             headers=self.code_headers)
        if r.status_code == 409:
            raise FetchError(f"The run stopped taking uploads ({r.json().get('error')})", final=True)
        r.raise_for_status()
        target = r.json()["uploads"][f["name"]]
        md5 = hashlib.md5()
        started = time.monotonic()
        with self.client.stream("GET", f["url"], headers={KEY_HEADER: self.api_key}) as src:
            if src.status_code != 200:
                src.read()
                reason = error_reason(src)
                if reason in ("downloadQuotaExceeded", "cannotDownloadFile"):
                    raise FetchError(f"Google Drive won't let {f['path']} be downloaded now ({reason}: "
                                     "a widely shared file hit Drive's download limit; it resets within a day)",
                                     final=True)
                if src.status_code in (403, 404):
                    raise FetchError(f"{f['path']} is no longer shared publicly or was removed "
                                     f"({src.status_code} {reason})", final=True)
                raise FetchError(f"Google Drive answered {src.status_code} {reason} for {f['path']}")

            def body():
                sent, logged, reported = 0, started, started
                for chunk in src.iter_bytes(CHUNK):
                    md5.update(chunk)
                    sent += len(chunk)
                    yield chunk
                    now = time.monotonic()
                    rate = sent / (now - started) if now > started else 0.0
                    if now - logged >= LOG_S and sent < f["size"]:
                        logged = now
                        logger.info(f"  {f['name']}: {100 * sent / max(1, f['size']):.0f}% of {_size(f['size'])} "
                                    f"at {_size(rate)}/s")
                    if now - reported >= PROGRESS_REPORT_S and sent < f["size"]:
                        reported = now
                        self.report_progress(f["name"], sent, f["size"], rate)
                if sent != f["size"]:
                    raise FetchError(f"{f['path']}: got {sent:,} bytes of {f['size']:,}")

            # storage takes no chunked body: the length is Drive's
            put = self.client.put(target["url"], content=body(), headers={"Content-Length": str(f["size"])})
        put.raise_for_status()
        digest = md5.hexdigest()
        if f.get("md5") and digest != f["md5"]:
            raise FetchError(f"{f['path']}: MD5 {digest} differs from Drive's {f['md5']}")
        etag = (put.headers.get("etag") or "").strip('"')
        took = time.monotonic() - started
        logger.info(f"Copied {f['path']} ({_size(f['size'])} in {_duration(took)}"
                    + (f", {_size(f['size'] / took)}/s" if took >= 1 else "") + ")")
        return {"key": target["key"], "size": f["size"], "etag": etag}

    def copy_with_retries(self, f: dict) -> dict:
        for attempt in range(len(self.pauses) + 1):
            try:
                return self.copy(f)
            except FetchError as e:
                if e.final or attempt == len(self.pauses):
                    raise
                why = str(e)
            except httpx.HTTPStatusError as e:
                if 400 <= e.response.status_code < 500 and e.response.status_code not in (408, 429):
                    raise FetchError(f"{e.request.method} {e.request.url.path}: {e.response.status_code} "
                                     f"{e.response.text[:200]}", final=True)
                why = f"{e.response.status_code}"
                if attempt == len(self.pauses):
                    raise FetchError(f"{f['path']}: {why}")
            except httpx.HTTPError as e:
                why = str(e) or type(e).__name__
                if attempt == len(self.pauses):
                    raise FetchError(f"{f['path']}: {why}")
            pause = self.pauses[attempt]
            logger.warning(f"Copy of {f['path']} failed ({why}); retrying in {pause} s")
            time.sleep(pause)
        raise AssertionError("unreachable")

    # --- the folder ---

    def run(self) -> dict:
        bundle = self.bundle()
        self.files = bundle["drive"]["files"]
        self.api_key = bundle["drive"].get("api_key") or ""
        archived = bundle["drive"].get("archived") or {}
        for f in self.files:
            have = archived.get(f["name"])
            # a single-part PUT's ETag is the file's MD5: an earlier attempt's copy
            if have and have["size"] == f["size"] and f.get("md5") and have["etag"] == f["md5"]:
                self.done[f["name"]] = have
        todo = [f for f in self.files if f["name"] not in self.done]
        total = sum(f["size"] for f in todo)
        logger.info(f"{len(todo)} file(s) to copy from Google Drive ({_size(total)})"
                    + (f"; {len(self.done)} already copied" if self.done else ""))
        started = time.monotonic()
        for f in todo:
            self.done[f["name"]] = self.copy_with_retries(f)
        took = time.monotonic() - started
        if todo:
            logger.info(f"Copied {len(todo)} file(s), {_size(total)}, in {_duration(took)}"
                        + (f" ({_size(total / took)}/s)" if took >= 1 else ""))
        st = self.complete()
        logger.info(f"Run {self.run_id} upload complete: {len(self.done)} file(s); state {st.get('state')}")
        return st


class _Tail(logging.Handler):
    def __init__(self, n: int = 60):
        super().__init__()
        self.lines = collections.deque(maxlen=n)

    def emit(self, record):
        self.lines.append(self.format(record))


def run(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="specimux-cloud fetch", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)
    fmt = "%(asctime)s %(levelname)-8s %(message)s"
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format=fmt, datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)   # request lines carry presigned URLs
    tail = _Tail()
    tail.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(tail)
    env = os.environ
    fetcher = Fetcher(env["SPECIMUX_RUN_API"], env["SPECIMUX_RUN_ID"], env["SPECIMUX_JOB_CODE"],
                      env["SPECIMUX_JOB_SECRET"], int(env["SPECIMUX_GENERATION"]))
    stop = StopSignal().install()
    patience = 600.0
    try:
        fetcher.run()
        code = 0
    except StopRequested:
        logger.error("Stopped (SIGTERM)")
        code, patience = STOPPED_EXIT, STOPPED_REPORT_PATIENCE_S
    except FetchError as e:
        logger.error(str(e))
        code = 1
    except Exception as e:
        logger.exception(f"The copy failed: {e}")
        code = 1
    stop.shield()
    fetcher.report_exit(code, "\n".join(tail.lines), patience_s=patience)
    return code


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
