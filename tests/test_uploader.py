"""The uploader against the run API (fake launcher): stable files only,
resume without re-upload, the final summary triggers complete with a
manifest the run API verifies."""

import json
import os
import threading
import time

import httpx
import pytest
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
    # uploads closed; complete launches the run, so the uploader's poll may
    # land just before the launch (input_complete) or after it (running)
    assert result["status"]["open"] is False and result["status"]["run_id"] == run["id"]
    assert result["status"]["state"] in ("input_complete", "running")
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


def test_upload_progress_is_logged_and_shown_on_the_run_page(api, tmp_path, caplog, monkeypatch):
    """A file in flight: the uploader logs its progress and reports it to the
    run API, whose run record carries what has arrived plus that report, and
    the console turns it into the run page's upload line."""
    import logging
    from specimux_cloud.progress import upload_text
    from specimux_cloud.uploader import cli as up_cli
    monkeypatch.setattr(up_cli, "PROGRESS_LOG_S", 0.0)
    monkeypatch.setattr(up_cli, "PROGRESS_REPORT_S", 0.0)
    monkeypatch.setattr(up_cli, "CHUNK", 1000)
    client, service, _ = api
    run = _create(client)
    folder = tmp_path / "reads"
    folder.mkdir()
    data = b"@r\n" + b"A" * 5000 + b"\n+\n" + b"I" * 5000 + b"\n"
    (folder / "a.fastq").write_bytes(data)
    up = Uploader("http://testserver", run["job_code"], folder, settle_s=0)
    _patch_client(up, client)
    up.report_client = client
    with caplog.at_level(logging.INFO, logger="specimux_cloud.uploader"):
        up.upload(folder / "a.fastq")
    assert "a.fastq: " in caplog.text and "% of" in caplog.text
    assert "1 file(s), 10.0 KB uploaded so far" in caplog.text
    rec = client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).json()
    assert rec["upload"]["files"] == 1 and rec["upload"]["bytes"] == len(data)
    pr = rec["upload"]["progress"]
    assert pr["file"] == "a.fastq" and 0 < pr["sent"] < pr["size"] == len(data)
    assert (pr["files_total"], pr["bytes_total"]) == (1, len(data))   # the whole upload so far (0.1.2)
    text = upload_text({**rec["upload"], "progress": {**pr, "sent": 2000, "rate": 10.0, "at": time.time()}})
    assert text == "about 20% of 10.0 KB, about 13 min left at 10 bytes/s · 1 of 1 file(s) received, sending a.fastq"
    # an uploader before 0.1.2 reports only the file in flight
    old = {k: v for k, v in pr.items() if k not in ("files_total", "bytes_total")}
    text = upload_text({**rec["upload"], "progress": {**old, "sent": 2000, "rate": 10.0, "at": time.time()}})
    assert text == "1 file(s) received (10.0 KB) · sending a.fastq: 20% of 10.0 KB at 10 bytes/s, about 13 min left"
    stale = upload_text({**rec["upload"], "progress": {**pr, "at": time.time() - 120}})
    assert stale == "1 file(s) received (10.0 KB)"                  # an old report is not shown
    hdr = {"Authorization": f"JobCode {run['job_code']}"}
    assert client.post(f"/v1/runs/{run['id']}/upload/progress", json={"sent": -1}, headers=hdr).status_code == 400
    assert client.post(f"/v1/runs/{run['id']}/upload/progress", json={"sent": 1}).status_code in (401, 403)
    client.post(f"/v1/runs/{run['id']}/complete", headers={"X-Service-Key": KEY})
    assert client.post(f"/v1/runs/{run['id']}/upload/progress", json={"sent": 1}, headers=hdr).status_code == 409
    assert "upload" not in client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).json()


def test_an_older_run_api_is_not_sent_progress_again(tmp_path):
    folder = tmp_path / "reads"
    folder.mkdir()
    up = Uploader("http://testserver", "r1.secret", folder, settle_s=0)
    calls = []

    class Old:
        def post(self, url, **kw):
            calls.append(url)
            return httpx.Response(404)
    up.report_client = Old()
    up.report_progress("a.pod5", 10, 100, 1.0)
    up.report_progress("a.pod5", 20, 100, 1.0)
    assert len(calls) == 1 and up.reports is False


def test_version_parsing():
    from specimux_cloud.versioning import older, parse_version, uploader_version
    assert parse_version("0.1.10") == (0, 1, 10) and parse_version("0.2.0rc1") == (0, 2, 0)
    assert older("0.1.9", "0.1.10") and not older("0.1.10", "0.1.9") and not older("0.1.1", "0.1.1")
    assert uploader_version("specimux-cloud-uploader/0.1.1") == "0.1.1"
    assert uploader_version("python-httpx/0.27.0") == "0.1.0"          # 0.1.0 named nothing
    assert uploader_version(None) == "0.1.0"


def test_an_uploader_below_the_minimum_is_refused_with_the_upgrade_command(api, tmp_path, caplog):
    """The operator's minimum turns an old uploader away with 426 and the
    command that fixes it, on every job-code route; the uploader itself
    checks /v1/version first and stops before sending anything; a host
    completing from the run page is not an uploader and is never refused."""
    import logging
    from specimux_cloud import __version__
    from specimux_cloud.uploader.cli import check_service
    client, service, _ = api
    run = _create(client)
    hdr = {"Authorization": f"JobCode {run['job_code']}"}
    info = client.get("/v1/version").json()["uploader"]
    assert info == {"minimum": None, "latest": __version__}
    old = {**hdr, "User-Agent": "python-httpx/0.27.0"}                  # what 0.1.0 sends
    assert client.post(f"/v1/runs/{run['id']}/uploads", json={"files": ["a.fastq"]}, headers=old).status_code == 200

    service.config.min_uploader = "9.0"
    for method, path, body in (("post", "uploads", {"files": ["a.fastq"]}), ("get", "upload", None),
                               ("post", "upload/progress", {"sent": 1}), ("post", "complete", None)):
        r = client.request(method.upper(), f"/v1/runs/{run['id']}/{path}", json=body, headers=old)
        assert r.status_code == 426, (path, r.status_code)
        assert "pip install -U specimux-cloud" in r.json()["error"] and "0.1.0" in r.json()["error"]
    new = {**hdr, "User-Agent": "specimux-cloud-uploader/9.0.0"}
    assert client.get(f"/v1/runs/{run['id']}/upload", headers=new).status_code == 200
    assert client.get("/v1/version").json()["uploader"]["minimum"] == "9.0"
    with pytest.raises(SystemExit, match="too old for this service"):
        check_service("http://testserver", client)
    folder = tmp_path / "reads"
    folder.mkdir()
    (folder / "a.fastq").write_text("@r\nA\n+\nI\n")
    up = Uploader("http://testserver", run["job_code"], folder, settle_s=0)
    _patch_client(up, client)
    with pytest.raises(SystemExit, match="Upgrade with: pip install -U specimux-cloud"):
        up.run(once=True)
    assert not up.done                                                  # nothing was sent
    assert client.post(f"/v1/runs/{run['id']}/complete", headers={"X-Service-Key": KEY}).status_code in (200, 409)

    # a newer release out: said once, nothing stops
    service.config.min_uploader = None
    import specimux_cloud.runapi.app as app_mod
    with caplog.at_level(logging.INFO, logger="specimux_cloud.uploader"):
        orig = client.get

        def newer(url, **kw):
            r = orig(url, **kw)
            if url.endswith("/v1/version"):
                return httpx.Response(200, json={**r.json(), "uploader": {"minimum": None, "latest": "99.0"}})
            return r
        client.get = newer
        try:
            check_service("http://testserver", client)
        finally:
            client.get = orig
    assert "specimux-cloud 99.0 is available" in caplog.text
    # a service that doesn't answer is no reason to stop
    check_service("http://127.0.0.1:9")
