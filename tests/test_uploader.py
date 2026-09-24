"""The uploader against the run API (fake launcher): stable files only,
resume without re-upload, the final summary triggers complete with a
manifest the run API verifies."""

import json
import os
import threading
import time
from pathlib import Path

from fastapi.testclient import TestClient

from specimux_cloud.uploader.cli import Uploader, run as uploader_main
from test_runapi import KEY, FakeLauncher, _create, api  # noqa: F401  (fixture)


def _patch_client(uploader, test_client):
    """Route the uploader's HTTP through the TestClient."""
    uploader.client = test_client


def test_uploader_watches_resumes_and_completes(api, tmp_path):
    client, service, launcher = api
    run = _create(client)
    folder = tmp_path / "minknow"
    folder.mkdir()
    (folder / "a.fastq").write_text("@r1\nACGT\n+\nIIII\n")
    fresh = folder / "b.fastq"
    fresh.write_text("@r2\nACGT\n+\nIIII\n")
    up = Uploader("http://testserver", run["job_code"], folder, settle_s=2.0)
    _patch_client(up, client)
    old = time.time() - 10
    os.utime(folder / "a.fastq", (old, old))          # a is settled, b is fresh

    assert [p.name for p in up.pending()] == ["a.fastq", "b.fastq"]
    assert up.stable(folder / "a.fastq") and not up.stable(fresh)
    rec = up.upload(folder / "a.fastq")
    assert rec["key"].endswith("/fastq/a.fastq") and rec["etag"]
    assert service.storage.head(rec["key"]).etag == rec["etag"]
    assert [p.name for p in up.pending()] == ["b.fastq"]
    # resume: a new uploader over the same folder knows a.fastq is done
    up2 = Uploader("http://testserver", run["job_code"], folder, settle_s=0)
    _patch_client(up2, client)
    assert [p.name for p in up2.pending()] == ["b.fastq"]
    assert up2.upload(folder / "a.fastq") == rec         # unchanged: no PUT

    # the run's end: everything uploaded and the final summary present
    (folder / "final_summary_ABC.txt").write_text("protocol=x\n")
    result = up2.run(once=False, poll_s=0.1)
    assert result["state"] == "running"                  # batch: engine launched
    status = client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).json()
    assert sorted(m["key"].rsplit("/", 1)[-1] for m in status["manifest"]) == ["a.fastq", "b.fastq"]
    assert launcher.specs[0].run_id == run["id"]
    state = json.loads((folder / ".specimux-upload.json").read_text())
    assert set(state["uploaded"]) == {"a.fastq", "b.fastq"}


def test_uploader_once_and_bad_code(api, tmp_path):
    client, service, _ = api
    run = _create(client)
    folder = tmp_path / "f"
    folder.mkdir()
    (folder / "x.fastq").write_text("@r\nA\n+\nI\n")
    bad = Uploader("http://testserver", f"{run['id']}.nope", folder)
    _patch_client(bad, client)
    import httpx, pytest
    with pytest.raises(httpx.HTTPStatusError):
        bad.run(once=True)
    good = Uploader("http://testserver", run["job_code"], folder)
    _patch_client(good, client)
    assert good.run(once=True)["state"] == "running"
    empty = tmp_path / "empty"
    empty.mkdir()
    other = _create(client)
    none = Uploader("http://testserver", other["job_code"], empty)
    _patch_client(none, client)
    with pytest.raises(SystemExit):
        none.run(once=True)


def test_a_watching_uploader_stops_when_the_run_is_completed_elsewhere(api, tmp_path, caplog):
    """A forgotten --once: every file is up, no final summary is coming;
    the uploader says how to finish, and the run page's button (complete
    with the service key, manifest from the listing) ends its wait."""
    import logging
    client, service, _ = api
    run = _create(client)
    folder = tmp_path / "done"
    folder.mkdir()
    (folder / "a.fastq").write_text("@r\nA\n+\nI\n")
    up = Uploader("http://testserver", run["job_code"], folder, settle_s=0)
    _patch_client(up, client)
    assert client.get(f"/v1/runs/{run['id']}/upload",
                      headers={"Authorization": f"JobCode {run['job_code']}"}).json()["open"] is True

    done = threading.Event()
    result = {}

    def watch():
        result["status"] = up.run(once=False, poll_s=0.05, status_s=0.1, hint_after_s=0.2)
        done.set()

    with caplog.at_level(logging.INFO, logger="specimux_cloud.uploader"):
        threading.Thread(target=watch, daemon=True).start()
        deadline = time.time() + 5
        while "waiting for MinKNOW's final_summary" not in caplog.text and time.time() < deadline:
            time.sleep(0.05)
        assert "run again with --once" in caplog.text
        assert not done.is_set()
        r = client.post(f"/v1/runs/{run['id']}/complete", headers={"X-Service-Key": KEY})
        assert r.status_code == 200 and r.json()["state"] == "running"
        assert done.wait(5)
    assert result["status"] == {"run_id": run["id"], "state": "running", "open": False}
    # the status route checks the code even for a closed run
    assert client.get(f"/v1/runs/{run['id']}/upload",
                      headers={"Authorization": f"JobCode {run['id']}.nope"}).status_code == 403


def test_a_file_that_arrives_after_the_run_closed_ends_the_uploader(api, tmp_path):
    client, service, _ = api
    run = _create(client)
    folder = tmp_path / "late"
    folder.mkdir()
    (folder / "a.fastq").write_text("@r\nA\n+\nI\n")
    up = Uploader("http://testserver", run["job_code"], folder, settle_s=0)
    _patch_client(up, client)
    up.upload(folder / "a.fastq")
    assert client.post(f"/v1/runs/{run['id']}/complete", headers={"X-Service-Key": KEY}).status_code == 200
    (folder / "b.fastq").write_text("@r\nC\n+\nI\n")
    assert up.run(once=False, poll_s=0.05)["open"] is False


def test_minknow_failed_reads_are_left_out(api, tmp_path):
    client, service, _ = api
    run = _create(client)
    folder = tmp_path / "minknow"
    for sub in ("fastq_pass/barcode01", "fastq_fail/barcode01"):
        (folder / sub).mkdir(parents=True)
    (folder / "fastq_pass/barcode01/p_0.fastq.gz").write_bytes(b"pass")
    (folder / "fastq_fail/barcode01/f_0.fastq.gz").write_bytes(b"fail")
    up = Uploader("http://testserver", run["job_code"], folder, settle_s=0)
    assert [p.name for p in up.pending()] == ["p_0.fastq.gz"]
    everything = Uploader("http://testserver", run["job_code"], folder, settle_s=0, include_failed=True)
    assert sorted(p.name for p in everything.pending()) == ["f_0.fastq.gz", "p_0.fastq.gz"]
