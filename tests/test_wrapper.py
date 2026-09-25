"""The engine wrapper's attempt semantics: an attempt that finds the
engine's exit record for its generation reports it and does not run the
engine again."""

import json
from pathlib import Path

from specimux_cloud.engine import wrapper


class FakeApi:
    def __init__(self):
        self.reports = []
        self.bundles = 0

    def job_bundle(self):
        self.bundles += 1
        return {"generation": 1, "spec": {"mode": "batch"}, "inputs": {}, "reads": []}

    def report_exit(self, generation, exit_code, log_tail, patience_s=0, packages=None):
        self.reports.append((generation, exit_code))
        self.packages = packages
        return {}


def test_second_attempt_reports_the_recorded_exit(tmp_path, monkeypatch):
    api = FakeApi()
    monkeypatch.setattr(wrapper, "RunApiClient", lambda *a, **k: api)
    wrapper.write_engine_exit_record(tmp_path, 1, 0, "done", {"results.zip": {"size": 10}})
    assert wrapper.engine_exit_record(tmp_path, 1)["exit_code"] == 0
    assert wrapper.engine_exit_record(tmp_path, 2) is None       # another generation: no
    rc = wrapper.run(["--run-id", "r1", "--run-api", "http://x", "--job-secret", "s",
                      "--generation", "1", "--work-dir", str(tmp_path)])
    assert rc == 0 and api.reports == [(1, 0)] and api.bundles == 0
    assert api.packages == {"results.zip": {"size": 10}}    # the uploaded packages are re-reported


def test_engine_exit_record_roundtrip(tmp_path):
    wrapper.write_engine_exit_record(tmp_path, 3, 2, "x" * 5000)
    rec = json.loads((tmp_path / wrapper.ENGINE_EXIT_FILENAME).read_text())
    assert rec["generation"] == 3 and rec["exit_code"] == 2 and len(rec["log_tail"]) == 4000


def test_a_fresh_mirror_keeps_the_earlier_attempt_and_the_photos(tmp_path):
    mirror = tmp_path / "output"
    (mirror / "inat_photos").mkdir(parents=True)
    (mirror / "inat_photos" / "1_medium.jpg").write_bytes(b"jpg")
    (mirror / "events.jsonl").write_text('{"version": 1}\n')
    wrapper.fresh_mirror(tmp_path)
    kept = [p for p in tmp_path.iterdir() if p.name.startswith("output.")]
    assert len(kept) == 1 and (kept[0] / "events.jsonl").exists()
    assert sorted(p.name for p in mirror.iterdir()) == ["inat_photos"]
    assert (mirror / "inat_photos" / "1_medium.jpg").read_bytes() == b"jpg"
    wrapper.fresh_mirror(tmp_path / "new")           # nothing there yet: just made
    assert (tmp_path / "new" / "output").is_dir()
    (tmp_path / "early" / "output" / "inat_photos").mkdir(parents=True)   # opened before the engine ran
    wrapper.fresh_mirror(tmp_path / "early")
    assert [p.name for p in (tmp_path / "early").iterdir()] == ["output"]


def test_packages_come_from_the_local_output_and_the_mirrored_log(tmp_path):
    from specimux_cloud.packages import PACKAGES, package_files
    out, mirror = tmp_path / "out", tmp_path / "mirror"
    for rel in ("summary/S1-1.v1-RiC3.fasta", "summary/variants/S1-1.v1-RiC3.fasta",
                "consensus/S1/S1-all.fasta", "consensus/S1/cluster_debug/x.fastq",
                "specimux/full/ITS/S1.fastq", "snapshots/S1.fastq"):
        (out / rel).parent.mkdir(parents=True, exist_ok=True)
        (out / rel).write_text("x")
    (mirror / "inat_photos").mkdir(parents=True)
    (mirror / "inat_photos" / "1.jpg").write_text("x")
    (out / "inat_photos").symlink_to(mirror / "inat_photos", target_is_directory=True)
    (mirror / "events.jsonl").write_text("{}\n")
    names = {name: sorted(a for a, _ in package_files(out, inc, extra={"events.jsonl": mirror / "events.jsonl"}))
             for name, inc in PACKAGES.items()}
    assert names["results.zip"] == ["events.jsonl", "summary/S1-1.v1-RiC3.fasta", "summary/variants/S1-1.v1-RiC3.fasta"]
    assert names["output.zip"] == ["consensus/S1/S1-all.fasta", "events.jsonl", "summary/S1-1.v1-RiC3.fasta",
                                   "summary/variants/S1-1.v1-RiC3.fasta"]
    assert names["reads.zip"] == ["specimux/full/ITS/S1.fastq"]



def test_batch_staging_decompresses_gzipped_reads(tmp_path, monkeypatch):
    """MinKNOW writes .fastq.gz: batch staging concatenates plain and
    gzipped files into one plain FASTQ."""
    import gzip
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.fastq").write_bytes(b"@a\nACGT\n+\nIIII\n")
    (src / "b.fastq.gz").write_bytes(gzip.compress(b"@b\nTTTT\n+\nIIII\n"))
    monkeypatch.setattr(wrapper, "download", lambda url, dest, etag=None: (
        dest.parent.mkdir(parents=True, exist_ok=True), dest.write_bytes(Path(url).read_bytes())))
    bundle = {"spec": {"mode": "batch"}, "inputs": {},
              "reads": [{"name": "a.fastq", "url": str(src / "a.fastq")},
                        {"name": "b.fastq.gz", "url": str(src / "b.fastq.gz")}]}
    work = tmp_path / "work"
    wrapper.stage_inputs(bundle, work)
    assert (work / "reads.fastq").read_bytes() == b"@a\nACGT\n+\nIIII\n@b\nTTTT\n+\nIIII\n"
    assert not (work / "downloads").exists()


def test_the_logged_engine_command_carries_no_job_secret():
    from specimux_cloud.engine.wrapper import redact_secret
    cmd = ["specimux-suite", "batch", "--plugin-opt", "job_secret=s3cr3t-value", "--plugin-opt", "generation=3"]
    shown = " ".join(redact_secret(cmd, "s3cr3t-value"))
    assert "s3cr3t-value" not in shown and "job_secret=***" in shown and "generation=3" in shown
    assert redact_secret(cmd, "") == cmd
