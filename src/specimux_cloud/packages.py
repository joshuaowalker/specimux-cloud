"""The three downloads a sealed run offers, built from an output dir
(standard library only: the engine wrapper builds them on the job's local
disk; the run API builds them from the run's EFS mirror when a job died
before it could).

- results.zip: the MycoMap summary package: summary/ as
  speconsense-summarize wrote it, at the root of the zip, nothing added or
  left out (speconsense-summarize owns that format; the suite writes
  nothing of its own there). Downloaded as <run name>_Summary.zip.
- output.zip: the rest worth keeping: the consensus FASTAs, the
  identification tables, the iNat ID audit and the event log (the name is
  kept for the /v1 route; downloaded as <run name>_Extras.zip)
- reads.zip: the demultiplexed reads on their own (large, rarely wanted)
"""

import os
import re
import shutil
import struct
import tempfile
import time
import zipfile
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

# Scratch and bulk that are not served or packaged per file: snapshots and
# staging are transient, specimux/ is the demultiplexed reads (reads.zip),
# cluster_debug holds speconsense's per-cluster reads (thousands of files)
SEAL_SKIP_DIRS = {"snapshots", ".staging", "specimux", "cluster_debug"}

EXTRA_FILES = {"events.jsonl", "inat_id_suggestions.tsv", "inat_id_corrections.tsv"}


def _summary(rel: Path):
    return "/".join(rel.parts[1:]) if rel.parts[0] == "summary" and len(rel.parts) > 1 else None


def _extras(rel: Path) -> bool:
    if rel.parts[0] in ("consensus", "identification"):
        return "cluster_debug" not in rel.parts
    return len(rel.parts) == 1 and rel.name in EXTRA_FILES


# name -> select(path relative to the output dir): False/None leaves the
# file out, True packs it under that path, a string under that name
PACKAGES: dict[str, Callable[[Path], object]] = {
    "results.zip": _summary,
    "output.zip": _extras,
    "reads.zip": lambda rel: rel.parts[0] == "specimux",
}
# what a package is called when downloaded, after the run's name
DOWNLOAD_SUFFIX = {"results": "Summary", "output": "Extras", "reads": "Reads"}
DOWNLOAD_LABEL = {"results": "MycoMap summary", "output": "extras", "reads": "demultiplexed reads"}


def download_stem(run: dict) -> str:
    """What a run's downloads are named after: its name (spec.name) reduced
    to filename-safe characters ("Run 150" -> "Run_150"), else its id."""
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", str((run.get("spec") or {}).get("name") or "")).strip("._-")
    return name or run["id"]


def download_name(run: dict, package: str) -> str:
    """"Run150_Summary.zip" for the run's results.zip, and so on."""
    return f"{download_stem(run)}_{DOWNLOAD_SUFFIX[package]}.zip"


def package_files(out: Path, include: Callable[[Path], object], extra: dict[str, Path] = None) -> list:
    """(arcname, path) for the files under ``out`` that ``include`` takes
    (it returns True, or the name to pack the file under).
    Symlinked directories are not followed (the photo cache is a link into
    the mirror; photos are served from there, not packaged). ``extra`` adds
    files from elsewhere under the given names (the event log, which lives
    in the mirror)."""
    files = []
    out = Path(out)
    if out.exists():
        for f in sorted(_walk_files(out)):
            rel = f.relative_to(out)
            took = include(rel) if not rel.name.endswith(".tmp") else None
            if took:
                files.append((took if isinstance(took, str) else rel.as_posix(), f))
    for name, path in (extra or {}).items():
        took = include(Path(name))
        arcname = took if isinstance(took, str) else name
        if Path(path).exists() and took and arcname not in {a for a, _ in files}:
            files.append((arcname, Path(path)))
    return files


def _walk_files(root: Path):
    """The files under ``root``, from directory listings alone (a file's type
    comes with its entry; on EFS a stat per file is a round trip). Symlinked
    directories are not entered; a symlinked file counts if it resolves."""
    with os.scandir(root) as entries:
        for e in entries:
            if e.is_dir(follow_symlinks=False):
                yield from _walk_files(Path(e.path))
            elif e.is_file():
                yield Path(e.path)


# Members are deflated in parallel, each on its own thread (zlib releases
# the GIL), and written in order by one writer: zipfile cannot take a
# member compressed elsewhere, so the container is written here. On a 2.6M
# read run, reads.zip (1.9 GB of deflated FASTQ) took six minutes on one
# core. The format is plain PKZIP with ZIP64 records where sizes, offsets
# or the member count need them; tests read every archive back with zipfile.
CHUNK = 1 << 20
LEVEL = 6                       # zlib's default, what ZIP_DEFLATED used
# when ZIP64 records are needed (module constants so tests can lower them)
ZIP64_LIMIT = 0xFFFFFFFF
MAX_MEMBERS = 0xFFFF
FULL32, FULL16 = 0xFFFFFFFF, 0xFFFF     # "see the ZIP64 record"
SPOOL_BYTES = 64 << 20          # a deflated member larger than this spills to disk


def compress_workers() -> int:
    """Threads for deflating: the CPUs this process may use."""
    try:
        n = len(os.sched_getaffinity(0))
    except AttributeError:      # macOS
        n = os.cpu_count() or 1
    return max(1, min(32, n))


def _deflate(path: Path, spool_dir: Optional[Path]):
    """(stat, crc, raw size, deflated size, spool holding the deflated
    bytes); the stat here too, since on a network filesystem every call is
    a round trip."""
    st = path.stat()
    comp = zlib.compressobj(LEVEL, zlib.DEFLATED, -15)
    spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_BYTES, dir=spool_dir)
    crc = size = 0
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            crc = zlib.crc32(chunk, crc)
            size += len(chunk)
            spool.write(comp.compress(chunk))
    spool.write(comp.flush())
    csize = spool.tell()
    spool.seek(0)
    return st, crc, size, csize, spool


def _dos_time(ts: float) -> tuple[int, int]:
    t = time.localtime(max(ts, 315532800))      # zip dates start in 1980
    return ((t.tm_hour << 11) | (t.tm_min << 5) | (t.tm_sec // 2),
            ((t.tm_year - 1980) << 9) | (t.tm_mon << 5) | t.tm_mday)


def build_zip(dest: Path, files: list, workers: Optional[int] = None) -> Path:
    """A deflated zip of ``files`` ((arcname, path) pairs), in order.
    Members are read and compressed on ``workers`` threads (default:
    every CPU; more pays when reading is the slow part, as over EFS) with
    a bounded number in flight, so a large archive never sits in
    memory; each spills to a temp file beside ``dest`` past SPOOL_BYTES."""
    workers = workers or compress_workers()
    dest = Path(dest)
    central, offset = [], 0
    with open(dest, "wb") as out, ThreadPoolExecutor(workers) as pool:
        pending = deque()
        items = iter(files)

        def fill():
            while len(pending) < 2 * workers:
                item = next(items, None)
                if item is None:
                    return
                arcname, path = item
                pending.append((arcname, Path(path), pool.submit(_deflate, Path(path), dest.parent)))

        fill()
        while pending:
            arcname, path, fut = pending.popleft()
            fill()
            st, crc, size, csize, spool = fut.result()
            with spool:
                name = arcname.encode("utf-8")
                dtime, ddate = _dos_time(st.st_mtime)
                big = size >= ZIP64_LIMIT or csize >= ZIP64_LIMIT
                extra = struct.pack("<HHQQ", 1, 16, size, csize) if big else b""
                out.write(struct.pack("<IHHHHHIIIHH", 0x04034B50, 45 if big else 20, 0x800, 8, dtime, ddate,
                                      crc, FULL32 if big else csize, FULL32 if big else size,
                                      len(name), len(extra)) + name + extra)
                shutil.copyfileobj(spool, out, CHUNK)
            mode = (st.st_mode & 0xFFFF) << 16
            central.append((name, dtime, ddate, crc, size, csize, offset, mode))
            offset += 30 + len(name) + len(extra) + csize

        cd_start = offset
        for name, dtime, ddate, crc, size, csize, at, mode in central:
            z64 = b""
            if size >= ZIP64_LIMIT:
                z64 += struct.pack("<Q", size)
            if csize >= ZIP64_LIMIT:
                z64 += struct.pack("<Q", csize)
            if at >= ZIP64_LIMIT:
                z64 += struct.pack("<Q", at)
            extra = struct.pack("<HH", 1, len(z64)) + z64 if z64 else b""
            version = 45 if z64 else 20
            entry = struct.pack("<IHHHHHHIIIHHHHHII", 0x02014B50, (3 << 8) | version, version, 0x800, 8,
                                dtime, ddate, crc, FULL32 if csize >= ZIP64_LIMIT else csize,
                                FULL32 if size >= ZIP64_LIMIT else size,
                                len(name), len(extra), 0, 0, 0, mode, FULL32 if at >= ZIP64_LIMIT else at)
            out.write(entry + name + extra)
            offset += len(entry) + len(name) + len(extra)
        cd_size, count = offset - cd_start, len(central)
        if count >= MAX_MEMBERS or cd_start >= ZIP64_LIMIT or cd_size >= ZIP64_LIMIT:
            out.write(struct.pack("<IQHHIIQQQQ", 0x06064B50, 44, (3 << 8) | 45, 45, 0, 0,
                                  count, count, cd_size, cd_start))
            out.write(struct.pack("<IIQI", 0x07064B50, 0, offset, 1))
            count16, cd_size32, cd_start32 = FULL16, FULL32, FULL32
        else:
            count16, cd_size32, cd_start32 = count, cd_size, cd_start
        out.write(struct.pack("<IHHHHIIH", 0x06054B50, 0, 0, count16, count16, cd_size32, cd_start32, 0))
    return dest
