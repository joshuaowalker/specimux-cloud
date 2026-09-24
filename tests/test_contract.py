"""C4, the job API, as a host sees it over plain HTTP (DESIGN.md "Contracts
and versioning"): the routes mycomap.org calls with its service key and
the token/session handoff its authorize route relies on.

By default this runs against a run API started in this process (the local
backends, the ``dev`` host). Pointed at a deployment it runs the same
requests there, which is how staging is checked before go-live and how
console drift is caught:

    SPECIMUX_CONTRACT_RUN_API=https://runs.example.org \\
    SPECIMUX_CONTRACT_KEY=<a host's service key> pytest tests/test_contract.py

No engine is launched: the run is created, uploaded to, inspected and
deleted, never completed.
"""

import json
import os
import socket
import time

import httpx
import pytest

PRIMERS = b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"
SPECIMENS = b"SampleID\tPrimerPool\nS1\tITS\n"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def target(tmp_path_factory):
    """(base URL, service key) for the run API under test."""
    base = os.environ.get("SPECIMUX_CONTRACT_RUN_API")
    key = os.environ.get("SPECIMUX_CONTRACT_KEY")
    if base and key:
        yield base.rstrip("/"), key
        return
    from specimux_suite.web.viewer import serve_in_thread
    from specimux_cloud.runapi.app import build_local_service, create_app
    key = "dev.contract-key"
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    service = build_local_service(tmp_path_factory.mktemp("data"), base, dev_key=key)
    serve_in_thread(create_app(service), "127.0.0.1", port)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            httpx.get(base + "/v1/version", timeout=1)
            break
        except httpx.HTTPError:
            time.sleep(0.1)
    yield base, key


@pytest.fixture
def host(target):
    base, key = target
    with httpx.Client(base_url=base, headers={"X-Service-Key": key}, timeout=30.0, follow_redirects=False) as c:
        yield c


@pytest.fixture
def browser(target):
    base, _ = target
    with httpx.Client(base_url=base, timeout=30.0, follow_redirects=False) as c:
        yield c


def test_version_and_identity(host):
    v = host.get("/v1/version").json()
    assert v["suite"] and v["cloud"]
    me = host.get("/v1/hosts/me").json()
    assert me["host"] and me["label"]
    assert host.get("/v1/hosts/me", headers={"X-Service-Key": "nope.nope"}).status_code == 403
    opts = host.get("/v1/options").json()
    assert "default" in opts["profiles"] and opts["modes"] == ["batch", "live"]


def test_a_run_from_creation_to_deletion(host, browser, target):
    base, _ = target
    token = f"contract-{time.time()}"
    data = {"spec": json.dumps({"mode": "batch", "input": "fastq", "profile": "default", "min_reads": 10}),
            "user_id": "contract-user", "client_token": token}
    files = {"primers": ("primers.fasta", PRIMERS), "specimens": ("Index.txt", SPECIMENS)}
    r = host.post("/v1/runs", data=data, files=files)
    assert r.status_code == 200, r.text
    run = r.json()
    rid = run["id"]
    try:
        assert run["state"] == "created" and run["job_code"].startswith(rid + ".")
        assert run["dashboard_url"] == f"{base}/v1/runs/{rid}/"
        assert "secret_hash" not in run and "job_secret" not in run
        # idempotent under the client token, secret shown once
        again = host.post("/v1/runs", data=data, files=files).json()
        assert again["id"] == rid and "job_code" not in again
        # status and listing
        st = host.get(f"/v1/runs/{rid}").json()
        assert st["state"] == "created" and st["versions"]["suite"] and st["pending_commands"] == []
        assert rid in [x["id"] for x in host.get("/v1/runs").json()["runs"]]
        # the uploader's side, with the job code
        jc = {"Authorization": f"JobCode {run['job_code']}"}
        r = browser.post(f"/v1/runs/{rid}/uploads", json={"files": ["a.fastq"]}, headers=jc)
        assert r.status_code == 200, r.text
        up = r.json()["uploads"]["a.fastq"]
        put = httpx.put(up["url"], content=b"@r\nACGT\n+\nIIII\n", timeout=60)
        assert put.status_code == 200 and put.headers.get("etag")
        assert host.get(f"/v1/runs/{rid}").json()["state"] == "uploading"
        # a new job code retires the old one
        new = host.post(f"/v1/runs/{rid}/job-code").json()
        assert new["job_code"] != run["job_code"]
        assert browser.post(f"/v1/runs/{rid}/uploads", json={"files": ["b.fastq"]}, headers=jc).status_code == 403
        # results are not there yet
        assert host.get(f"/v1/runs/{rid}/results.zip").status_code == 409
        # the token/session handoff the authorize route performs
        assert host.post(f"/v1/runs/{rid}/tokens", json={"scope": "root"}).status_code == 400
        tok = host.post(f"/v1/runs/{rid}/tokens", json={"user": "contract-user", "scope": "view"}).json()
        assert tok["token"] and tok["expires_in"] > 0 and tok["dashboard_url"] == run["dashboard_url"]
        assert browser.get(f"/v1/runs/{rid}/api/state").status_code == 401
        page = browser.get(f"/v1/runs/{rid}/")
        assert page.status_code == 200 and 'id="specimux-runtime"' in page.text and '"sessionEndpoint"' in page.text
        r = browser.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"})
        assert r.status_code == 200 and r.json()["run"] == rid and r.json()["scope"] == "view"
        assert browser.get(f"/v1/runs/{rid}/api/state").status_code == 200
        assert browser.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}x"}).status_code == 401
    finally:
        assert host.delete(f"/v1/runs/{rid}").status_code == 200
    assert host.get(f"/v1/runs/{rid}").status_code == 404
    assert host.get("/v1/runs/rffffffff").status_code == 404
