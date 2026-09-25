"""Whole-job progress lines (progress.py), from run records shaped like the
run API's."""

from specimux_cloud.progress import basecall_estimate, basecall_text, upload_text

GB = 1_000_000_000
NOW = 1_000_000.0


def _run(done=(), current=None, attempt=(2, NOW - 1200), state="basecalling", finished=False):
    """Four 1 GB POD5 files; ``done`` = (index, generation, reads) delivered;
    ``current`` = (index, reads, estimate) in flight."""
    manifest = [{"key": f"archives/x/pod5/f{i}.pod5", "size": GB} for i in range(4)]
    files = [{"name": f"f{i}.fastq", "reads_in": reads, "reads_out": reads - 10, "generation": gen}
             for i, gen, reads in done]
    run = {"state": state, "manifest": manifest,
           "basecalling": {"files": files, "done": len(files), "total": 4,
                           "reads_in": sum(f["reads_in"] for f in files),
                           "reads_out": sum(f["reads_out"] for f in files)},
           "basecall_attempt": {"generation": attempt[0], "started": attempt[1]}}
    if finished:
        run["basecalling"]["finished"] = NOW
    if current:
        i, reads, est = current
        run["basecall_current"] = {"file": f"f{i}.pod5", "reads": reads, "estimate": est, "at": NOW - 10}
    return run


def test_whole_job_basecalling_counts_the_file_in_flight():
    # two files delivered by this attempt in 20 min, the third half done
    run = _run(done=[(0, 2, 100_000), (1, 2, 100_000)], current=(2, 50_000, 100_000))
    est = basecall_estimate(run, NOW)
    assert est["fraction"] == 2.5 / 4 and est["reads"] == 250_000
    assert (est["files_done"], est["files_total"], est["current"]) == (2, 4, "f2.pod5")
    assert round(est["seconds_left"]) == 720                       # 1.5 GB left at 2.5 GB / 20 min
    assert basecall_text(run, NOW) == ("about 62%, about 12 min left · 250,000 reads called so far "
                                       "(2 of 4 file(s) done, 3rd in progress)")


def test_a_retry_is_timed_by_its_own_files():
    # the first attempt delivered files 0-1; this one, 10 min in, is half way through file 2
    run = _run(done=[(0, 1, 100_000), (1, 1, 100_000)], current=(2, 50_000, 100_000), attempt=(2, NOW - 600))
    est = basecall_estimate(run, NOW)
    assert est["fraction"] == 2.5 / 4
    assert round(est["seconds_left"]) == 1800                      # 1.5 GB left at 0.5 GB / 10 min


def test_no_time_left_early_and_estimates_capped():
    run = _run(current=(0, 150_000, 100_000), attempt=(2, NOW - 60))   # more reads than estimated
    est = basecall_estimate(run, NOW)
    assert est["seconds_left"] is None and est["fraction"] == 0.99 / 4
    assert basecall_text(run, NOW) == "about 25% · 150,000 reads called so far (0 of 4 file(s) done, 1st in progress)"


def test_stale_report_and_stopped_runs():
    run = _run(done=[(0, 2, 100_000)], current=(1, 50_000, 100_000))
    run["basecall_current"]["at"] = NOW - 600                     # the job stopped reporting
    assert basecall_estimate(run, NOW)["current"] is None
    failed = _run(done=[(0, 2, 100_000)], current=(1, 50_000, 100_000), state="failed")
    assert basecall_text(failed, NOW) == "1 of 4 file(s) done · 100,000 reads called, 99,990 within the length window"
    finished = _run(done=[(i, 2, 100_000) for i in range(4)], state="running", finished=True)
    assert basecall_text(finished, NOW) == "4 of 4 file(s) done · 400,000 reads called, 399,960 within the length window"
    assert basecall_text({"state": "basecalling"}, NOW) == ""        # nothing to basecall


def test_whole_upload_line():
    up = {"files": 3, "bytes": 3 * GB,
          "progress": {"file": "f3.pod5", "sent": GB // 2, "size": GB, "rate": 500_000,
                       "files_done": 3, "bytes_done": 3 * GB, "files_total": 6, "bytes_total": 6 * GB,
                       "at": NOW - 5}}
    assert upload_text(up, NOW) == ("about 58% of 6.0 GB, about 1 h 23 min left at 500.0 KB/s"
                                    " · 3 of 6 file(s) received, sending f3.pod5")
