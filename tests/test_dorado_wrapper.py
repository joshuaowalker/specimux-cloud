"""The dorado wrapper's pure parts: the length filter and the command it
builds. The job itself runs in test_local_stack (stand-in dorado) and
test_runapi (its reports, by hand)."""

from pathlib import Path

from specimux_cloud.dorado.wrapper import build_dorado_command, filter_fastq


def _fastq(*lengths):
    return "".join(f"@r{i} ch=1\n{'A' * n}\n+\n{'I' * n}\n" for i, n in enumerate(lengths)).encode()


def test_filter_keeps_the_length_window(tmp_path):
    src = tmp_path / "raw.fastq"
    src.write_bytes(_fastq(399, 400, 1000, 2000, 2001))
    dest = tmp_path / "out.fastq"
    assert filter_fastq(src, dest, 400, 2000) == (5, 3)
    kept = [line for line in dest.read_text().splitlines() if line.startswith("@")]
    assert kept == ["@r1 ch=1", "@r2 ch=1", "@r3 ch=1"]
    # bounds of zero mean no bound
    assert filter_fastq(src, dest, 0, 0) == (5, 5)
    assert filter_fastq(src, dest, 2001, 0) == (5, 1)
    assert not (tmp_path / "out.fastq.part").exists()


def test_filter_refuses_a_truncated_record(tmp_path):
    src = tmp_path / "raw.fastq"
    src.write_bytes(_fastq(500) + b"@r9\nACGT\n+\n")
    try:
        filter_fastq(src, tmp_path / "out.fastq", 0, 0)
    except ValueError as e:
        assert "truncated" in str(e)
    else:
        raise AssertionError("a truncated record passed")


def test_command_follows_the_protocol():
    cmd = build_dorado_command({"model": "sup@v5.0.0", "min_length": 400, "max_length": 2000, "min_qscore": None},
                               Path("/scratch/a.pod5"), "cuda:all")
    assert cmd == ["dorado", "basecaller", "sup@v5.0.0", "/scratch/a.pod5", "--emit-fastq", "--no-trim",
                   "--device", "cuda:all"]
    cmd = build_dorado_command({"model": "hac@v6.0.0", "min_qscore": 9}, Path("a.pod5"), "cpu", binary="/opt/dorado")
    assert cmd[0] == "/opt/dorado" and cmd[-2:] == ["--min-qscore", "9"] and "--no-trim" in cmd


def test_reads_are_counted_as_dorado_writes_them(tmp_path):
    from specimux_cloud.dorado.wrapper import FastqCounter
    out = tmp_path / "raw.fastq"
    counter = FastqCounter(out)
    assert counter.reads() == 0                        # not there yet
    out.write_bytes(_fastq(500, 600))
    assert counter.reads() == 2
    with open(out, "ab") as f:
        f.write(_fastq(700)[:-5])                      # a record half written
    assert counter.reads() == 2
    with open(out, "ab") as f:
        f.write(_fastq(700)[-5:] + _fastq(800))
    assert counter.reads() == 4


def test_basecalling_reports_progress_while_dorado_runs(tmp_path, monkeypatch):
    """While dorado runs the wrapper reports the reads called so far every
    PROGRESS_S; the file is then filtered and delivered as before."""
    import os
    from specimux_cloud.dorado import wrapper
    fake = Path(__file__).parent / "fake_tools" / "dorado"
    reads = _fastq(500, 600, 700)
    monkeypatch.setattr(wrapper, "PROGRESS_S", 0.05)
    monkeypatch.setattr(wrapper, "download", lambda url, dest, *a: dest.write_bytes(reads))
    delivered = {}
    monkeypatch.setattr(wrapper, "upload", lambda url, path: delivered.setdefault(url, path.read_bytes()))
    monkeypatch.setenv("FAKE_DORADO_SLEEP", "0.4")
    seen = []
    entry = {"name": "a.pod5", "url": "u", "size": 30000}
    bundle = {"fastq_uploads": {"a.fastq": {"url": "put-a", "key": "k"}},
              "basecall": {"model": "sup@v5.0.0", "min_length": 100, "max_length": 3000}}
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    with open(tmp_path / "dorado.log", "ab") as log:
        got = wrapper.basecall_file(entry, bundle, scratch, "cpu", str(fake), log, progress=seen.append)
    assert got == (3, 3) and delivered["put-a"] == reads
    assert len(seen) >= 3 and all(n == 0 for n in seen)   # the stand-in writes only at the end
    assert not os.listdir(scratch)                         # scratch cleaned
