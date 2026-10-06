"""Runs whose input is a public Google Drive folder: the listing at
creation, the copy job (fetch.py, run in process here against a fake
Drive), and what happens when Drive or the job fails."""

import json

import pytest
from fastapi.testclient import TestClient

from specimux_cloud import drive
from specimux_cloud.backends.base import JobStatus
from specimux_cloud.backends.local import DirectoryStorage, MemoryQueue, SQLiteStore
from specimux_cloud.fetch import FetchError, Fetcher
from specimux_cloud.runapi.app import create_app, install_dev_host
from specimux_cloud.runapi.service import RunService, ServiceConfig

from fake_drive import FakeDrive, HostRouter
from test_runapi import KEY, POD5_FILES, SECRET, FakeLauncher

DRIVE_URL = "http://drive.test/drive/v3"
LINK = "https://drive.google.com/drive/folders/RUNFOLDER0001?usp=sharing"


@pytest.fixture
def stack(tmp_path):
    base = "http://testserver"
    fake = FakeDrive()
    config = ServiceConfig(data_dir=tmp_path / "data", base_url=base, session_secret=SECRET,
                           stage_slots={"engine": 1, "dorado": 1, "fetch": 1},
                           drive_api_key=fake.key, drive_api_url=DRIVE_URL)
    launcher = FakeLauncher()
    service = RunService(config, storage=DirectoryStorage(tmp_path / "data" / "storage", base_url=base, secret=SECRET),
                         queue=MemoryQueue(), launcher=launcher, store=SQLiteStore(tmp_path / "cp.sqlite"))
    install_dev_host(service, KEY)
    client = TestClient(HostRouter(create_app(service), fake.app), base_url=base)
    service.drive_client = client
    # a MinKNOW run folder: passing reads in a barcode folder, failed ones apart
    fake.folder("RUNFOLDER0001", "run155")
    fake.folder("PASSFOLDER01", "pod5_pass", "RUNFOLDER0001")
    fake.folder("BARCODEDIR01", "barcode01", "PASSFOLDER01")
    fake.file("FILEA0000001", "a.pod5", b"POD5:a" * 100, "BARCODEDIR01")
    fake.file("FILEB0000001", "b.pod5", b"POD5:b" * 50, "BARCODEDIR01")
    fake.file("FILEC0000001", "c.pod5", b"POD5:c", "PASSFOLDER01")
    fake.file("NOTES0000001", "notes.txt", b"hello", "RUNFOLDER0001")
    fake.folder("FAILFOLDER01", "pod5_fail", "RUNFOLDER0001")
    fake.file("FILEX0000001", "x.pod5", b"POD5:x", "FAILFOLDER01")
    return client, service, launcher, fake


def _create(client, link=LINK, spec=None, token=None):
    data = {"spec": json.dumps({"mode": "batch", "input": "pod5", "source": {"google_drive": link}, **(spec or {})}),
            "user_id": "u42"}
    if token:
        data["client_token"] = token
    return client.post("/v1/runs", data=data, files=POD5_FILES, headers={"X-Service-Key": KEY})


def _fetcher(client, launcher, pauses=()):
    env = launcher.specs[-1].env
    return Fetcher("http://testserver", env["SPECIMUX_RUN_ID"], env["SPECIMUX_JOB_CODE"], env["SPECIMUX_JOB_SECRET"],
                   int(env["SPECIMUX_GENERATION"]), client=client, pauses=pauses)


def test_folder_links():
    for link in ("https://drive.google.com/drive/folders/1AbC_def-GHIjkl",
                 "https://drive.google.com/drive/u/0/folders/1AbC_def-GHIjkl?usp=sharing",
                 "https://drive.google.com/open?id=1AbC_def-GHIjkl", "1AbC_def-GHIjkl"):
        assert drive.folder_id(link) == "1AbC_def-GHIjkl", link
    for bad in ("https://example.com/drive/folders/1AbC_def-GHIjkl", "https://drive.google.com/drive/my-drive",
                "../../etc", ""):
        with pytest.raises(drive.DriveError):
            drive.folder_id(bad)


def test_a_run_from_a_drive_folder(stack):
    client, service, launcher, fake = stack
    r = _create(client)
    assert r.status_code == 200, r.text
    run = r.json()
    rid = run["id"]
    # listed at creation: the POD5 files outside pod5_fail, from every level, pages followed
    assert run["drive"] == {"folder": "RUNFOLDER0001", "files": 3, "bytes": 600 + 300 + 6}
    assert run["spec"]["source"] == {"google_drive": "RUNFOLDER0001"}
    assert "job_code" not in run                     # nobody uploads
    # the copy job launched at once, with a job code of its own
    assert run["state"] == "uploading" and launcher.specs[-1].kind == "fetch"
    assert launcher.specs[-1].name == f"{rid}-fetch-1" and launcher.specs[-1].env["SPECIMUX_JOB_CODE"].startswith(rid + ".")
    service._status_cache.clear()
    assert service.dashboard_status(rid)["text"].startswith("Copying from Google Drive")

    st = _fetcher(client, launcher).run()
    assert st["state"] == "basecalling"              # complete started the next stage
    archive = f"archives/u42/{run['archive_id']}/pod5"
    assert service.storage.get(f"{archive}/a.pod5") == b"POD5:a" * 100
    assert sorted(o.key.rsplit("/", 1)[-1] for o in service.storage.list(archive + "/")) == ["a.pod5", "b.pod5", "c.pod5"]
    assert sorted(fake.downloads) == ["FILEA0000001", "FILEB0000001", "FILEC0000001"]
    # its exit report ends the stage and frees its slot
    fetch_secret = {"X-Job-Secret": launcher.specs[0].env["SPECIMUX_JOB_SECRET"]}
    client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 0}, headers=fetch_secret)
    run = service.get_run(rid)
    assert run["state"] == "basecalling" and not run["stages"]["fetch"]["active"]
    assert service.store.stage_holders("fetch") == [] and launcher.specs[-1].kind == "dorado"
    dor = client.get(f"/v1/runs/{rid}/job", headers={"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}).json()
    assert [p["name"] for p in dor["pod5"]] == ["a.pod5", "b.pod5", "c.pod5"]


def test_what_a_drive_run_refuses(stack):
    client, service, launcher, fake = stack
    fake.folder("EMPTYFOLDER1", "empty")
    fake.folder("DUPFOLDER001", "dups")
    fake.file("DUP000000001", "a.pod5", b"1", "DUPFOLDER001")
    fake.folder("DUPSUBFOLDR1", "more", "DUPFOLDER001")
    fake.file("DUP000000002", "a.pod5", b"2", "DUPSUBFOLDR1")
    fake.private.add("PRIVATEFOLD1")
    fake.folder("PRIVATEFOLD1", "secret")

    def refused(link=LINK, spec=None, status=400):
        r = _create(client, link, spec)
        assert r.status_code == status, r.text
        return r.json()["error"]
    assert "Not a Google Drive folder link" in refused("https://example.com/folders/RUNFOLDER0001")
    assert "not shared with" in refused("https://drive.google.com/drive/folders/PRIVATEFOLD1")
    assert "not shared with" in refused("https://drive.google.com/drive/folders/NOSUCHFOLDR1")
    assert "a file, not a folder" in refused("https://drive.google.com/drive/folders/FILEA0000001")
    assert "No POD5 files" in refused("https://drive.google.com/drive/folders/EMPTYFOLDER1")
    assert "No FASTQ files" in refused(spec={"input": "fastq"})
    assert "Two files are named a.pod5" in refused("https://drive.google.com/drive/folders/DUPFOLDER001")
    assert "batch run" in refused(spec={"mode": "live", "input": "fastq"})
    assert "source must be" in refused(spec={"source": {"dropbox": "x"}})
    # the failed reads only on request
    r = _create(client, spec={"source": {"google_drive": LINK, "include_failed": True}})
    assert r.status_code == 200 and r.json()["drive"]["files"] == 4
    # a service without a key, or with a bad one: the service's problem
    service.config.drive_api_key = None
    assert "not set up" in refused(status=500)
    service.config.drive_api_key = "wrong"
    assert "misconfigured" in refused(status=500)
    assert len(service.store.list_runs()) == 1


def test_a_failed_copy_fails_the_run_and_a_retry_copies_the_rest(stack):
    client, service, launcher, fake = stack
    fake.quota.add("FILEB0000001")
    rid = _create(client).json()["id"]
    f = _fetcher(client, launcher)
    with pytest.raises(FetchError) as e:
        f.run()
    assert e.value.final and "download limit" in str(e.value)
    # what the job reports: its last error line becomes the run's reason
    secret = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    tail = f"10:00:00 INFO     Copied a.pod5\n10:00:01 ERROR    {e.value}"
    st = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 1, "log_tail": tail},
                     headers=secret).json()
    assert st["state"] == "failed" and st["exit"]["stage"] == "fetch" and "download limit" in st["exit"]["reason"]
    assert service.store.stage_holders("fetch") == []
    assert client.post(f"/v1/runs/{rid}/retry", headers={"X-Service-Key": KEY}).json()["state"] == "uploading"
    assert launcher.specs[-1].name == f"{rid}-fetch-2"
    fake.quota.clear()
    fake.downloads.clear()
    assert _fetcher(client, launcher).run()["state"] == "basecalling"
    assert sorted(fake.downloads) == ["FILEB0000001", "FILEC0000001"]       # a.pod5 was already there


def test_a_corrupt_download_is_retried_then_fails(stack):
    client, service, launcher, fake = stack
    fake.corrupt.add("FILEA0000001")
    _create(client)
    with pytest.raises(FetchError) as e:
        _fetcher(client, launcher, pauses=(0,)).run()
    assert "differs from Drive's" in str(e.value)
    assert fake.downloads.count("FILEA0000001") == 2


def test_cancel_stops_the_copy_job_and_reconcile_judges_a_dead_one(stack):
    client, service, launcher, fake = stack
    rid = _create(client).json()["id"]
    job_id = service.get_run(rid)["jobs"][f"{rid}-fetch-1"]["id"]
    st = client.post(f"/v1/runs/{rid}/cancel", json={}, headers={"X-Service-Key": KEY}).json()
    assert st["state"] == "failed" and launcher.states[job_id].state == "failed"    # stopped
    assert client.delete(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).status_code == 409  # its job still holds the stage
    # the stopped job never reports: reconcile ends the stage, the run stays cancelled
    service.reconcile(intent_grace_s=0)
    run = service.get_run(rid)
    assert not run["stages"]["fetch"]["active"] and run["cancel"] and service.store.stage_holders("fetch") == []
    assert client.delete(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).status_code == 200

    # a copy job that dies: the run fails with the stage named
    other = _create(client).json()["id"]
    job_id = service.get_run(other)["jobs"][f"{other}-fetch-1"]["id"]
    launcher.states[job_id] = JobStatus("failed", exit_code=137, reason="Host EC2 terminated")
    service.reconcile(intent_grace_s=0)
    run = service.get_run(other)
    assert run["state"] == "failed" and run["exit"]["stage"] == "fetch" and "Host EC2" in run["exit"]["reason"]


def test_drive_runs_queue_for_the_copy_stage(stack):
    client, service, launcher, fake = stack
    first = _create(client).json()
    second = _create(client).json()
    assert first["state"] == "uploading" and second["state"] == "created"   # one fetch slot here
    service._status_cache.clear()
    assert service.dashboard_status(second["id"])["text"] == "Waiting to copy the input from Google Drive"
    _fetcher(client, launcher).run()
    client.post(f"/v1/runs/{first['id']}/exit", json={"generation": 1, "exit_code": 0},
                headers={"X-Job-Secret": launcher.specs[0].env["SPECIMUX_JOB_SECRET"]})
    assert service.get_run(second["id"])["state"] == "uploading"


def test_a_drive_run_from_the_console(stack):
    client, service, launcher, fake = stack
    assert client.post("/console/login", data={"key": KEY}, follow_redirects=False).status_code == 303
    form = client.get("/console/new").text
    assert 'name="drive_folder"' in form and "Anyone with the link" in form
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "input": "pod5", "client_token": "ct-d",
                                          "model": "sup@v5.0.0", "min_length": "100", "max_length": "3000",
                                          "drive_folder": LINK}, files=POD5_FILES, follow_redirects=False)
    assert r.status_code == 303, r.text
    rid = r.headers["location"].rsplit("/", 1)[-1]
    page = client.get(f"/console/runs/{rid}").text
    assert "copying from Google Drive" in page and "3 file(s), 906 bytes" in page
    assert "drive.google.com/drive/folders/RUNFOLDER0001" in page
    # a bad link goes back to the form with the reason
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "input": "pod5", "client_token": "ct-e",
                                          "drive_folder": "https://drive.google.com/drive/folders/NOSUCHFOLDR1"},
                    files=POD5_FILES, follow_redirects=False)
    assert r.status_code == 303 and "not+shared" in r.headers["location"]
    # a failed copy offers a retry
    client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 1, "log_tail": "x ERROR    boom"},
                headers={"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]})
    page = client.get(f"/console/runs/{rid}").text
    assert "Retry the copy" in page and "Copy from Google Drive" in page and "boom" in page
