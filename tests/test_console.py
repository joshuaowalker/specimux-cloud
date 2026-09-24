"""The console: a host made of a login, a job page and an authorize route,
talking to the run API over HTTP with a service key (in-process here)."""

import json
import re
import time
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

from specimux_cloud.backends.local import DirectoryStorage, MemoryQueue, SQLiteStore
from specimux_cloud.runapi.app import create_app, install_dev_host
from specimux_cloud.runapi.service import RunService, ServiceConfig

from test_runapi import KEY, SECRET, FakeLauncher

BASE = "http://testserver"
FILES = {"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
         "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")}


@pytest.fixture
def stack(tmp_path):
    config = ServiceConfig(data_dir=tmp_path / "data", base_url=BASE, session_secret=SECRET)
    service = RunService(config, storage=DirectoryStorage(tmp_path / "data" / "storage", base_url=BASE, secret=SECRET),
                         queue=MemoryQueue(), launcher=FakeLauncher(), store=SQLiteStore(tmp_path / "cp.sqlite"))
    install_dev_host(service, KEY)
    app = create_app(service, console=True)
    return TestClient(app, base_url=BASE, follow_redirects=False), service


def _login(client, key=KEY):
    r = client.post("/console/login", data={"key": key})
    assert r.status_code == 303, r.text
    return r


def _create(client) -> str:
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "user_id": "user-1",
                                          "client_token": "ct-console-1"}, files=FILES)
    assert r.status_code == 200, r.text
    m = re.search(r"<pre>(r[0-9a-f]{8}\.[^<]+)</pre>", r.text)
    assert m, r.text
    return m.group(1)


def test_login_logout_and_the_runs_list(stack):
    client, service = stack
    r = client.get("/console/")
    assert r.status_code == 303 and r.headers["location"] == "/console/login"
    assert "Service key" in client.get("/console/login").text
    r = client.post("/console/login", data={"key": "dev.wrong"})
    assert r.status_code == 303 and "error=" in r.headers["location"]
    assert client.cookies.get("specimux_console") is None
    r = _login(client)
    assert r.headers["location"] == "/console/"
    cookie = r.headers["set-cookie"]
    assert "HttpOnly" in cookie and "Path=/console/" in cookie and KEY not in cookie   # encrypted
    page = client.get("/console/").text
    assert "No runs yet" in page and "Local development" in page and "<code>dev</code>" in page
    r = client.post("/console/logout")
    assert r.status_code == 303
    assert client.get("/console/").status_code == 303


def test_create_a_run_and_walk_its_page(stack):
    client, service = stack
    _login(client)
    assert "New run" in client.get("/console/new").text
    code = _create(client)
    rid = code.split(".")[0]
    run = service.get_run(rid)
    assert run["host"] == "dev" and run["user_id"] == "user-1" and run["spec"]["min_reads"] == 7
    # the list and the run page
    listing = client.get("/console/").text
    assert rid in listing and "created" in listing and f"/v1/runs/{rid}/" in listing
    assert "Service load (all users)" in listing and "Pipeline: 0 of 2 workers busy" in listing
    page = client.get(f"/console/runs/{rid}").text
    assert "waiting for the upload" in page and "New job code" in page and "Delete run" in page
    assert 'http-equiv="refresh"' in page
    # a new job code invalidates the old one
    r = client.post(f"/console/runs/{rid}/job-code")
    assert r.status_code == 200
    new = re.search(r"<pre>(r[0-9a-f]{8}\.[^<]+)</pre>", r.text).group(1)
    assert new != code and new.startswith(rid + ".")
    assert f"--job-code {new} --once &lt;folder&gt;" in r.text and "Upload is complete" in r.text
    assert client.post(f"/v1/runs/{rid}/uploads", json={"files": ["a.fastq"]},
                       headers={"Authorization": f"JobCode {code}"}).status_code == 403
    # upload with the new code, then "upload is complete" from the page
    r = client.post(f"/v1/runs/{rid}/uploads", json={"files": ["a.fastq"]}, headers={"Authorization": f"JobCode {new}"})
    client.put(r.json()["uploads"]["a.fastq"]["url"], content=b"@r\nA\n+\nI\n")
    r = client.post(f"/console/runs/{rid}/complete")
    assert r.status_code == 303 and "error" not in r.headers["location"]
    assert service.get_run(rid)["state"] == "running"
    page = client.get(f"/console/runs/{rid}").text
    assert "engine running" in page and "Delete run" not in page
    # an error from the API lands on the page
    r = client.post(f"/console/runs/{rid}/complete")
    assert r.status_code == 303 and "error=" in r.headers["location"]
    q = parse_qs(urlsplit(r.headers["location"]).query)
    assert "Run is running" in q["error"][0]
    assert "Run is running" in client.get(r.headers["location"]).text
    # cancel stops the engine job; the button goes once the stop is requested
    assert "Cancel run" in page
    r = client.post(f"/console/runs/{rid}/cancel")
    assert r.status_code == 303 and "error" not in r.headers["location"]
    assert service.get_run(rid)["cancel"]["reason"].startswith("cancelled by")
    assert "Cancel run" not in client.get(f"/console/runs/{rid}").text


def test_authorize_hands_the_page_a_token(stack):
    """Same-origin: JSON. Navigated to with a return URL: a redirect back
    with the token in the fragment, only to the run's own dashboard."""
    client, service = stack
    _login(client)
    rid = _create(client).split(".")[0]
    dashboard = f"{BASE}/v1/runs/{rid}/"
    # JSON form (the page fetched it)
    r = client.get(f"/console/authorize?run={rid}")
    assert r.status_code == 200
    tok = r.json()
    assert tok["scope"] == "admin" and tok["expires_in"] == 60
    r = client.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"})
    assert r.status_code == 200 and r.json()["user"] == "dev"
    assert client.get(f"/v1/runs/{rid}/api/state").status_code == 200
    assert client.post(f"/v1/runs/{rid}/commands", json={"command": "finalize"}).status_code == 409  # admin, not running
    # redirect form (the page navigated here)
    r = client.get(f"/console/authorize", params={"run": rid, "return": dashboard})
    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith(dashboard + "#token=")
    frag = loc.split("#token=", 1)[1]
    assert client.post("/v1/session", headers={"Authorization": f"Bearer {frag}"}).status_code == 200
    # a return URL elsewhere is refused
    r = client.get("/console/authorize", params={"run": rid, "return": "https://evil.example/"})
    assert r.status_code == 400
    r = client.get("/console/authorize", params={"run": rid, "return": f"{BASE}/v1/runs/other/"})
    assert r.status_code == 400
    # a run of another host: nothing
    assert client.get("/console/authorize?run=nope").status_code == 404
    assert client.get("/console/authorize").status_code == 400
    # the page's runtime points at this route
    page = client.get(dashboard).text
    assert f'"tokenEndpoint": "{BASE}/console/authorize?run={rid}"' in page


def test_authorize_without_a_console_login(stack):
    client, service = stack
    _login(client)
    rid = _create(client).split(".")[0]
    anon = TestClient(client.app, base_url=BASE, follow_redirects=False)
    assert anon.get(f"/console/authorize?run={rid}").status_code == 401
    r = anon.get("/console/authorize", params={"run": rid, "return": f"{BASE}/v1/runs/{rid}/"})
    assert r.status_code == 303
    nxt = parse_qs(urlsplit(r.headers["location"]).query)["next"][0]
    assert nxt.startswith("/console/authorize?") and rid in nxt
    # logging in continues to the authorize route, which bounces to the page
    r = anon.post("/console/login", data={"key": KEY, "next": nxt})
    assert r.status_code == 303 and r.headers["location"] == nxt
    r = anon.get(nxt)
    assert r.status_code == 302 and r.headers["location"].startswith(f"{BASE}/v1/runs/{rid}/#token=")


def test_downloads_forward_the_redirect_and_delete_removes_the_run(stack):
    client, service = stack
    _login(client)
    rid = _create(client).split(".")[0]
    # not sealed yet: back to the page with the reason
    r = client.get(f"/console/runs/{rid}/results.zip")
    assert r.status_code == 303 and "error=" in r.headers["location"]
    # pretend the seal happened
    run = service.get_run(rid)
    key = f"{service.run_prefix(run)}/results.zip"
    service.storage.put(key, b"PK\x05\x06" + b"\0" * 18)
    service.store.update_run(rid, {"state": "sealed", "sealed": {"results": key, "results_bytes": 22}})
    r = client.get(f"/console/runs/{rid}/results.zip")
    assert r.status_code == 302 and "/v1/storage/" in r.headers["location"] and "sig=" in r.headers["location"]
    assert client.get(r.headers["location"]).status_code == 200
    page = client.get(f"/console/runs/{rid}").text
    assert "results.zip" in page and "22 bytes" in page and 'http-equiv="refresh"' not in page
    assert "results.zip" in client.get("/console/").text
    # delete
    r = client.post(f"/console/runs/{rid}/delete")
    assert r.status_code == 303 and r.headers["location"] == "/console/"
    assert client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).status_code == 404
    assert "No runs yet" in client.get("/console/").text


def test_the_console_only_sees_its_own_host(stack):
    client, service = stack
    _login(client)
    rid = _create(client).split(".")[0]
    _, fundis_key = service.add_host("fundis", name="FUNDIS", label="lab-staff")
    other = TestClient(client.app, base_url=BASE, follow_redirects=False)
    _login(other, fundis_key)
    page = other.get("/console/").text
    assert "FUNDIS" in page and "lab-staff" in page and rid not in page and "No runs yet" in page
    assert "No such run" in other.get(f"/console/runs/{rid}").text
    assert other.get(f"/console/authorize?run={rid}").status_code == 404


def test_a_pod5_run_from_the_form(stack):
    client, service = stack
    _login(client)
    form = client.get("/console/new").text
    assert 'name="input"' in form and "sup@v5.0.0" in form and 'name="min_length"' in form
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "user_id": "user-1",
                                          "client_token": "ct-pod5", "input": "pod5", "model": "hac@v6.0.0",
                                          "min_length": "100", "max_length": "700", "min_qscore": ""}, files=FILES)
    assert r.status_code == 200, r.text
    assert "POD5 files" in r.text and "Basecalling, then the engine" in r.text
    rid = re.search(r"<pre>(r[0-9a-f]{8})\.", r.text).group(1)
    run = service.get_run(rid)
    assert run["spec"]["input"] == "pod5"
    assert run["spec"]["basecall"] == {"model": "hac@v6.0.0", "min_length": 100, "max_length": 700, "min_qscore": None}
    # a bad number goes back to the form
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "user_id": "user-1",
                                          "client_token": "ct-pod5-2", "input": "pod5", "model": "hac@v6.0.0",
                                          "min_length": "lots", "max_length": "700"}, files=FILES)
    assert r.status_code == 303 and "error=" in r.headers["location"]
    # the run page shows basecalling progress once the job is on
    service.store.update_run(rid, {"state": "basecalling", "generation": 1,
                                   "basecalling": {"done": 1, "total": 3, "reads_in": 4000, "reads_out": 3500}})
    page = client.get(f"/console/runs/{rid}").text
    assert "basecalling on the GPU" in page and "1 of 3 file(s)" in page and "4,000 reads called, 3,500" in page
    assert 'http-equiv="refresh"' in page and "Delete run" not in page


def test_a_reference_used_before_is_offered_and_not_sent_again(stack):
    client, service = stack
    _login(client)
    ref = b'>REF1 name="Amanita muscaria"\nACGTACGTAC\n'
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "client_token": "ct-ref-1"},
                    files={**FILES, "reference": ("mycomap-2026.fasta", ref)})
    assert r.status_code == 200, r.text
    form = client.get("/console/new").text
    m = re.search(r'<option value="([0-9a-f]{64})">mycomap-2026.fasta \(', form)
    assert m, form
    r = client.post("/console/new", data={"profile": "default", "min_reads": "7", "client_token": "ct-ref-2",
                                          "reference_sha256": m.group(1)}, files=FILES)
    assert r.status_code == 200, r.text
    rid = re.search(r"<pre>(r[0-9a-f]{8})\.", r.text).group(1)
    assert service.get_run(rid)["spec"]["reference_sha256"] == m.group(1)
    assert "reference" in service.job_bundle(rid)["inputs"]


def test_an_abandoned_upload_shows_why(stack):
    client, service = stack
    _login(client)
    rid = _create(client).split(".")[0]
    service.expire_idle_uploads(now=time.time() + 8 * 24 * 3600)
    page = client.get(f"/console/runs/{rid}").text
    assert "incomplete" in page and "code None" not in page
    assert "<tr><th>Upload</th><td>upload abandoned: nothing uploaded within 7 days of creation</td></tr>" in page


def test_sharing_a_run_publicly_from_the_run_page(stack):
    client, service = stack
    _login(client)
    rid = _create(client).split(".")[0]
    page = client.get(f"/console/runs/{rid}").text
    assert "Share publicly" in page
    assert client.post(f"/console/runs/{rid}/public", data={"action": "enable"}).status_code == 303
    page = client.get(f"/console/runs/{rid}").text
    url = service.public_url(service.get_run(rid))
    assert url and f"<pre>{url}</pre>" in page and "may star specimens" in page and "Stop sharing" in page
    client.post(f"/console/runs/{rid}/public", data={"action": "toggle_starring"})
    assert service.get_run(rid)["public"]["allow_starring"] is False
    client.post(f"/console/runs/{rid}/public", data={"action": "disable"})
    assert service.get_run(rid)["public"]["enabled"] is False and "Share publicly" in client.get(f"/console/runs/{rid}").text
