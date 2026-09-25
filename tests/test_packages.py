"""The package zip writer (packages.build_zip): members deflated in
parallel, written in order, readable by zipfile and by Info-ZIP."""

import os
import shutil
import subprocess
import zipfile

import pytest

from specimux_cloud import packages


def _tree(root):
    files = {
        "summary/S1-1.v1-RiC3.fasta": b">S1\nACGT\n" * 50,
        "specimux/full/ITS/S1.fastq": b"@r\nACGTACGTAC\n+\nIIIIIIIIII\n" * 20000,   # several chunks
        "empty.txt": b"",
        "noise.bin": os.urandom(300_000),                                            # does not compress
        "names/Amanita müscaria.fasta": b">x\nA\n",
    }
    out = []
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        out.append((rel, p))
    return files, out


def _check(dest, files, order):
    with zipfile.ZipFile(dest) as z:
        assert z.testzip() is None
        assert z.namelist() == order
        for info in z.infolist():
            assert info.compress_type == zipfile.ZIP_DEFLATED
            assert z.read(info) == files[info.filename]
    if shutil.which("unzip"):
        r = subprocess.run(["unzip", "-tq", str(dest)], capture_output=True, text=True)
        assert r.returncode == 0, r.stdout + r.stderr


def test_members_round_trip_in_order(tmp_path):
    files, pairs = _tree(tmp_path / "src")
    dest = packages.build_zip(tmp_path / "a.zip", pairs, workers=4)
    _check(dest, files, [a for a, _ in pairs])
    # the thread count changes nothing in the archive
    one = packages.build_zip(tmp_path / "b.zip", pairs, workers=1)
    assert one.read_bytes() == dest.read_bytes()


def test_zip64_records_when_sizes_offsets_or_counts_need_them(tmp_path, monkeypatch):
    monkeypatch.setattr(packages, "ZIP64_LIMIT", 1000)      # most sizes and offsets past the limit
    monkeypatch.setattr(packages, "MAX_MEMBERS", 3)         # more members than the plain record holds
    files, pairs = _tree(tmp_path / "src")
    dest = packages.build_zip(tmp_path / "a.zip", pairs, workers=3)
    _check(dest, files, [a for a, _ in pairs])


def test_large_members_spill_to_disk(tmp_path, monkeypatch):
    monkeypatch.setattr(packages, "SPOOL_BYTES", 1000)
    files, pairs = _tree(tmp_path / "src")
    dest = packages.build_zip(tmp_path / "a.zip", pairs, workers=2)
    _check(dest, files, [a for a, _ in pairs])
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.zip", "src"]    # spills are removed


def test_a_missing_file_fails_the_build(tmp_path):
    files, pairs = _tree(tmp_path / "src")
    pairs.append(("gone.txt", tmp_path / "gone.txt"))
    with pytest.raises(FileNotFoundError):
        packages.build_zip(tmp_path / "a.zip", pairs, workers=2)
