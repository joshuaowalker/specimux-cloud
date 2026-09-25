"""Whole-job progress for a run's upload and basecalling, from its run
record (as ``GET /v1/runs/{id}`` returns it): how far along, how long
left, and the line the run page shows. Standard library only; the console
formats its run page with it.

Basecalling progress is measured in POD5 bytes: the files delivered, plus
the file being called scaled by its reads so far over the dorado job's
estimate of its reads. Time left comes from this attempt's own rate (a
retry skips the files an earlier attempt delivered), once it has run a
few minutes. Upload progress is measured in bytes of the files the
uploader has found so far (0.1.2 reports them; an older uploader reports
only the file in flight).
"""

import time
from typing import Optional

# reports older than these are from a job that stopped reporting
UPLOAD_REPORT_FRESH_S = 60
BASECALL_REPORT_FRESH_S = 120
# no time-left estimate before this much of an attempt has run
ETA_AFTER_S = 180


def size_text(n) -> str:
    """Bytes for people: 2.1 GB, 950 MB."""
    n = float(n or 0)
    for unit in ("bytes", "KB", "MB", "GB", "TB"):
        if n < 1000 or unit == "TB":
            return f"{n:,.0f} {unit}" if unit == "bytes" else f"{n:,.1f} {unit}"
        n /= 1000


def time_left_text(seconds: float) -> str:
    """About how long: "about 25 min left", "about 1 h 40 min left"."""
    minutes = max(1, round(seconds / 60))
    if minutes < 60:
        return f"about {minutes} min left"
    return f"about {minutes // 60} h {minutes % 60} min left"


def ordinal(n: int) -> str:
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def fastq_name(pod5_name: str) -> str:
    """The name of the FASTQ basecalled from a POD5 file (the run API's
    ``basecalled_key``)."""
    stem = pod5_name[:-5] if pod5_name.lower().endswith(".pod5") else pod5_name
    return f"{stem}.fastq"


def basecall_estimate(run: dict, now: Optional[float] = None) -> Optional[dict]:
    """How far a POD5 run's basecalling is: ``{"fraction", "reads",
    "files_done", "files_total", "current", "seconds_left"}`` (the last two
    may be None, and are while the run is not basecalling), or None when
    the run has nothing to basecall."""
    now = time.time() if now is None else now
    sizes = {m["key"].rsplit("/", 1)[-1]: m.get("size") or 0 for m in run.get("manifest") or []}
    total = sum(sizes.values())
    if not total:
        return None
    by_fastq = {fastq_name(n): n for n in sizes}
    bc = run.get("basecalling") or {}
    files = bc.get("files") or []
    done_names = {by_fastq.get(f["name"]) for f in files}
    done_bytes = sum(sizes.get(n, 0) for n in done_names if n)
    reads = sum(f.get("reads_in", 0) for f in files)

    cur = run.get("basecall_current") or {}
    current, current_bytes = None, 0.0
    active = run.get("state") == "basecalling"
    if active and cur.get("file") in sizes and cur["file"] not in done_names \
            and now - float(cur.get("at") or 0) <= BASECALL_REPORT_FRESH_S:
        current = cur["file"]
        estimate = cur.get("estimate") or 0
        if estimate:
            current_bytes = sizes[current] * min(0.99, (cur.get("reads") or 0) / estimate)
        reads += cur.get("reads") or 0

    seconds_left = None
    attempt = run.get("basecall_attempt") or {}
    elapsed = now - float(attempt.get("started") or now)
    if active and elapsed >= ETA_AFTER_S:
        mine = sum(sizes.get(by_fastq.get(f["name"]), 0) for f in files
                   if f.get("generation") == attempt.get("generation"))
        rate = (mine + current_bytes) / elapsed
        if rate > 0:
            seconds_left = (total - done_bytes - current_bytes) / rate
    return {"fraction": (done_bytes + current_bytes) / total, "reads": reads,
            "files_done": len(done_names - {None}), "files_total": len(sizes),
            "current": current, "seconds_left": seconds_left}


def basecall_text(run: dict, now: Optional[float] = None) -> str:
    """The run page's basecalling line."""
    bc = run.get("basecalling") or {}
    if bc.get("finished"):
        text = f"{bc.get('done', 0)} of {bc.get('total', 0)} file(s) done"
        if bc.get("reads_in"):
            text += f" · {bc['reads_in']:,} reads called, {bc.get('reads_out', 0):,} within the length window"
        return text
    est = basecall_estimate(run, now)
    if est is None:
        return ""
    if run.get("state") != "basecalling":   # stopped part-way (failed, cancelled)
        text = f"{est['files_done']} of {est['files_total']} file(s) done"
        if bc.get("reads_in"):
            text += f" · {bc['reads_in']:,} reads called, {bc.get('reads_out', 0):,} within the length window"
        return text
    head = f"about {min(99, round(100 * est['fraction']))}%"
    if est["seconds_left"] is not None:
        head += f", {time_left_text(est['seconds_left'])}"
    files = f"{est['files_done']} of {est['files_total']} file(s) done"
    if est["current"]:
        files += f", {ordinal(est['files_done'] + 1)} in progress"
    if est["reads"]:
        return f"{head} · {est['reads']:,} reads called so far ({files})"
    return f"{head} · {files}"


def upload_estimate(up: dict, now: Optional[float] = None) -> dict:
    """How far an upload is, from the run's ``upload`` block: the files
    received whole, and the uploader's last report while it is recent.
    ``{"files", "bytes", "current", "fraction", "total_bytes",
    "files_total", "rate", "seconds_left"}``; the last five are None when
    the uploader did not report them (the whole-upload ones from 0.1.2)."""
    now = time.time() if now is None else now
    out = {"files": up.get("files", 0), "bytes": up.get("bytes") or 0, "current": None, "fraction": None,
           "total_bytes": None, "files_total": None, "rate": None, "seconds_left": None}
    pr = up.get("progress") or {}
    size, sent = pr.get("size") or 0, pr.get("sent") or 0
    if not (pr.get("file") and size and sent < size and now - float(pr.get("at") or 0) < UPLOAD_REPORT_FRESH_S):
        return out
    rate = pr.get("rate") or 0
    out.update(current={"file": pr["file"], "sent": sent, "size": size}, rate=rate or None)
    total = pr.get("bytes_total") or 0
    if total:
        sent_total = (pr.get("bytes_done") or 0) + sent
        out.update(fraction=min(1.0, sent_total / total), total_bytes=total, files_total=pr.get("files_total"))
        if rate > 0:
            out["seconds_left"] = max(0, total - sent_total) / rate
    elif rate > 0:
        out["seconds_left"] = (size - sent) / rate
    return out


def upload_text(up: dict, now: Optional[float] = None) -> str:
    """The run page's upload line: the whole upload when the uploader
    reports it (0.1.2), else the files received and the file in flight."""
    est = upload_estimate(up, now)
    cur = est["current"]
    if cur and est["fraction"] is not None:
        text = f"about {min(99, round(100 * est['fraction']))}% of {size_text(est['total_bytes'])}"
        if est["seconds_left"] is not None:
            text += f", {time_left_text(est['seconds_left'])}"
        if est["rate"]:
            text += f" at {size_text(est['rate'])}/s"
        text += f" · {est['files']}" + (f" of {est['files_total']}" if est["files_total"] else "")
        return text + f" file(s) received, sending {cur['file']}"
    text = f"{est['files']} file(s) received ({size_text(est['bytes'])})"
    if cur:
        text += f" · sending {cur['file']}: {100 * cur['sent'] / cur['size']:.0f}% of {size_text(cur['size'])}"
        if est["rate"]:
            text += f" at {size_text(est['rate'])}/s, {time_left_text(est['seconds_left'])}"
    return text
