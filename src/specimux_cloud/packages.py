"""The three downloads a sealed run offers, built from an output dir
(standard library only: the engine wrapper builds them on the job's local
disk; the run API builds them from the run's EFS mirror when a job died
before it could).

- results.zip: the summary package MycoMap accepts today, plus the log
- output.zip: the whole output minus scratch and debug
- reads.zip: the demultiplexed reads on their own (large, rarely wanted)
"""

import zipfile
from pathlib import Path
from typing import Callable

# Scratch and bulk that are not served or packaged per file: snapshots and
# staging are transient, specimux/ is the demultiplexed reads (reads.zip),
# cluster_debug holds speconsense's per-cluster reads (thousands of files)
SEAL_SKIP_DIRS = {"snapshots", ".staging", "specimux", "cluster_debug"}

PACKAGES: dict[str, Callable[[Path], bool]] = {
    "results.zip": lambda rel: rel.parts[0] == "summary" or rel.as_posix() == "events.jsonl",
    "output.zip": lambda rel: rel.parts[0] not in SEAL_SKIP_DIRS and "cluster_debug" not in rel.parts,
    "reads.zip": lambda rel: rel.parts[0] == "specimux",
}


def package_files(out: Path, include: Callable[[Path], bool], extra: dict[str, Path] = None) -> list:
    """(arcname, path) for the files under ``out`` that ``include`` takes.
    Symlinked directories are not followed (the photo cache is a link into
    the mirror; photos are served from there, not packaged). ``extra`` adds
    files from elsewhere under the given names (the event log, which lives
    in the mirror)."""
    files = []
    out = Path(out)
    if out.exists():
        for f in sorted(p for p in out.rglob("*") if p.is_file()):
            rel = f.relative_to(out)
            if not rel.name.endswith(".tmp") and include(rel):
                files.append((rel.as_posix(), f))
    for arcname, path in (extra or {}).items():
        if Path(path).exists() and include(Path(arcname)) and arcname not in {a for a, _ in files}:
            files.append((arcname, Path(path)))
    return files


def build_zip(dest: Path, files: list) -> Path:
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        for arcname, path in files:
            z.write(path, arcname)
    return dest
