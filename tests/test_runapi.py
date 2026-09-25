"""The run API over the local backends, with a fake launcher in place of
the engine: the whole batch-FASTQ life of a run through HTTP.

What the fake engine does by hand here — fetch the job bundle, ingest
events with the generation, poll and acknowledge commands, report the
exit — is exactly what the wrapper and the cloud plugin do.
"""

import io
import json
import time
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from specimux_cloud.backends.base import JobHandle, JobSpec, JobStatus
from specimux_cloud.backends.local import DirectoryStorage, MemoryQueue, SQLiteStore
from specimux_cloud.runapi.app import SESSION_COOKIE, create_app, install_dev_host
from specimux_cloud.runapi.service import RunService, ServiceConfig

KEY = "dev.test-service-key"        # the `dev` host's key
SECRET = "test-session-secret"


class FakeLauncher:
    def __init__(self):
        self.specs: list[JobSpec] = []
        self.states: dict[str, JobStatus] = {}

    def submit(self, spec):
        self.specs.append(spec)
        h = JobHandle(f"job-{len(self.specs)}", spec.name)
        self.states[h.id] = JobStatus("running")
        return h

    def describe(self, job_id):
        return self.states.get(job_id, JobStatus("unknown"))

    def find_by_name(self, name):
        for i, s in enumerate(self.specs, 1):
            if s.name == name:
                return JobHandle(f"job-{i}", name)
        return None

    def cancel(self, job_id, reason=""):
        self.states[job_id] = JobStatus("failed", exit_code=137, reason="cancelled")


@pytest.fixture
def api(tmp_path):
    base = "http://testserver"
    # one run per stage, so the queueing below is exercised; two slots
    # have their own test
    config = ServiceConfig(data_dir=tmp_path / "data", base_url=base, session_secret=SECRET,
                           stage_slots={"engine": 1, "dorado": 1})
    launcher = FakeLauncher()
    service = RunService(config, storage=DirectoryStorage(tmp_path / "data" / "storage", base_url=base, secret=SECRET),
                         queue=MemoryQueue(visibility_s=0.5), launcher=launcher,
                         store=SQLiteStore(tmp_path / "data" / "cp.sqlite"))
    install_dev_host(service, KEY)
    client = TestClient(create_app(service), base_url=base)
    return client, service, launcher


def _session(client, rid, scope="admin", user="u42", key=KEY):
    """What a host's authorize route and the page do: mint a token, exchange
    it; the client keeps the cookie."""
    r = client.post(f"/v1/runs/{rid}/tokens", json={"user": user, "scope": scope}, headers={"X-Service-Key": key})
    assert r.status_code == 200, r.text
    tok = r.json()
    r = client.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"})
    assert r.status_code == 200, r.text
    assert r.json()["scope"] == scope and r.json()["run"] == rid
    return tok


def _wait_state(service, rid, states, timeout=10.0):
    """The seal runs in a background thread after the exit report."""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = service.get_run(rid)
        if run["state"] in states:
            return run
        time.sleep(0.05)
    raise AssertionError(f"run {rid} stayed {run['state']}")


def _create(client, mode="batch", token=None):
    data = {"spec": json.dumps({"mode": mode, "input": "fastq", "profile": "default", "min_reads": 10}),
            "user_id": "u42"}
    if token:
        data["client_token"] = token
    files = {"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
             "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")}
    r = client.post("/v1/runs", data=data, files=files, headers={"X-Service-Key": KEY})
    assert r.status_code == 200, r.text
    return r.json()


def test_create_requires_service_key_and_is_idempotent(api):
    client, service, _ = api
    r = client.post("/v1/runs", data={"spec": "{}", "user_id": "u"}, headers={"X-Service-Key": "nope"})
    assert r.status_code == 403
    run = _create(client, token="ct-1")
    assert run["state"] == "created" and run["job_code"].startswith(run["id"] + ".")
    assert "secret_hash" not in run
    again = _create(client, token="ct-1")
    assert again["id"] == run["id"] and "job_code" not in again    # secret shown once
    other = _create(client)
    assert other["id"] != run["id"]
    st = client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).json()
    assert st["state"] == "created" and st["versions"]["suite"] and st["host"] == "dev"
    assert st["dashboard_url"] == f"http://testserver/v1/runs/{run['id']}/"
    assert service.storage.head(f"runs/u42/{run['id']}/input/primers")
    assert client.get("/v1/version").json()["suite"]
    assert "profiles" in client.get("/v1/options", headers={"X-Service-Key": KEY}).json()
    me = client.get("/v1/hosts/me", headers={"X-Service-Key": KEY}).json()
    assert me["host"] == "dev" and me["label"] == "dev" and me["authorize_url"].endswith("/console/authorize")


def _upload(client, run, name, data):
    hdr = {"Authorization": f"JobCode {run['job_code']}"}
    r = client.post(f"/v1/runs/{run['id']}/uploads", json={"files": [name]}, headers=hdr)
    assert r.status_code == 200, r.text
    url = r.json()["uploads"][name]["url"]
    put = client.put(url, content=data)
    assert put.status_code == 200, put.text
    return r.json()["uploads"][name]["key"], put.headers["etag"].strip('"')


def test_uploads_need_the_job_code_and_signed_urls(api):
    client, service, _ = api
    run = _create(client)
    r = client.post(f"/v1/runs/{run['id']}/uploads", json={"files": ["a.fastq"]},
                    headers={"Authorization": f"JobCode {run['id']}.wrong"})
    assert r.status_code == 403
    key, etag = _upload(client, run, "a.fastq", b"@r1\nACGT\n+\nIIII\n")
    assert key == f"archives/u42/{run['archive_id']}/fastq/a.fastq"
    assert service.storage.head(key).etag == etag
    # tampered signature
    r = client.put(f"/v1/storage/{key}?exp=9999999999&sig=bad", content=b"x")
    assert r.status_code == 403
    assert client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).json()["state"] == "uploading"


def test_full_batch_run_through_the_api(api, captured_events):
    client, service, launcher = api
    run = _create(client)
    rid = run["id"]
    key, etag = _upload(client, run, "chunk1.fastq", b"@r1\nACGT\n+\nIIII\n")
    hdr = {"Authorization": f"JobCode {run['job_code']}"}

    # complete with a manifest that must match what was uploaded
    r = client.post(f"/v1/runs/{rid}/complete", json={"manifest": [{"key": key, "etag": "stale"}]}, headers=hdr)
    assert r.status_code == 409
    r = client.post(f"/v1/runs/{rid}/complete", json={"manifest": [{"key": key, "etag": etag}]}, headers=hdr)
    assert r.status_code == 200, r.text
    status = r.json()
    assert status["state"] == "running" and status["generation"] == 1
    assert launcher.specs[0].name == f"{rid}-engine-1"
    env = launcher.specs[0].env
    assert env["SPECIMUX_RUN_ID"] == rid and env["SPECIMUX_GENERATION"] == "1"
    assert env["SPECIMUX_VCPUS"] == "16" and launcher.specs[0].vcpus == 16
    job_secret = env["SPECIMUX_JOB_SECRET"]
    assert service.store.stage_holders("engine") == [rid]
    # the job code is dead now
    assert client.post(f"/v1/runs/{rid}/uploads", json={"files": ["late.fastq"]}, headers=hdr).status_code == 409
    # a second run cannot take the engine stage: it waits in input_complete
    other = _create(client)
    _upload(client, other, "x.fastq", b"@r\nA\n+\nI\n")
    r = client.post(f"/v1/runs/{other['id']}/complete", headers={"Authorization": f"JobCode {other['job_code']}"})
    assert r.status_code == 200 and r.json()["state"] == "input_complete"
    assert len(launcher.specs) == 1

    # --- the engine's side ---
    eng = {"X-Job-Secret": job_secret}
    assert client.get(f"/v1/runs/{rid}/job", headers={"X-Job-Secret": "bad"}).status_code == 403
    bundle = client.get(f"/v1/runs/{rid}/job", headers=eng).json()
    assert bundle["generation"] == 1 and bundle["spec"]["mode"] == "batch"
    assert set(bundle["inputs"]) == {"primers", "specimens"}
    assert bundle["reads"][0]["name"] == "chunk1.fastq" and bundle["reads"][0]["etag"] == etag
    assert client.get(bundle["reads"][0]["url"]).content == b"@r1\nACGT\n+\nIIII\n"
    assert client.get(bundle["inputs"]["specimens"]).text.startswith("SampleID")

    # ingest in batches, a retried batch, a stale generation
    events = captured_events
    r = client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": events[:100]}, headers=eng)
    assert r.json() == {"accepted": 100, "version": 100}
    r = client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": events[:100]}, headers=eng)
    assert r.json()["accepted"] == 0
    r = client.post(f"/v1/runs/{rid}/ingest", json={"generation": 0, "events": events[100:101]}, headers=eng)
    assert r.status_code == 409
    r = client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": events[100:]}, headers=eng)
    assert r.json()["version"] == len(events)

    # --- the dashboard over the ingested log (with a session) ---
    assert client.get(f"/v1/runs/{rid}/api/state").status_code == 401
    _session(client, rid, scope="admin", user="u42")
    snap = client.get(f"/v1/runs/{rid}/api/state").json()
    assert snap["version"] == len(events) and snap["specimens"]
    assert snap["config_summary"]["min_reads"] == 10          # from pipeline.started
    page = client.get(f"/v1/runs/{rid}/").text
    assert f'"apiBase": "/v1/runs/{rid}"' in page and f'src="/v1/runs/{rid}/static/derived.js"' in page
    assert f'"tokenEndpoint": "http://testserver/console/authorize?run={rid}"' in page
    assert '"sessionEndpoint": "/v1/session"' in page
    assert client.get(f"/v1/runs/{rid}/static/derived.js").status_code == 200
    assert client.get(f"/v1/runs/{rid}/api/specimens").status_code == 200
    assert client.get("/v1/runs/nope/api/state").status_code == 401     # no session: nothing is revealed
    st = client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()
    assert st["effective_config"]["min_reads"] == 10
    assert st["ingested_files"]                                 # specimux.completed seen

    # --- a command from the browser, applied by the engine ---
    sid = next(iter(snap["specimens"]))
    r = client.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": sid, "actor": "root"})
    assert r.status_code == 200
    cid = r.json()["command_id"]
    st = client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()
    assert st["pending_commands"] == [cid]
    polled = client.get(f"/v1/runs/{rid}/commands/next?wait=0", headers=eng).json()["commands"]
    assert polled[0]["id"] == cid and polled[0]["command"] == "watch"
    assert polled[0]["args"] == {"specimen_id": sid} and polled[0]["actor"] == "dev:u42"
    client.post(f"/v1/runs/{rid}/commands/{polled[0]['message_id']}/ack", headers=eng)
    # the engine applies it and the outcome comes back through ingest
    v = len(events)
    outcome = [
        {"v": 1, "version": v + 1, "type": "specimen.watched", "ts": "t",
         "data": {"specimen_id": sid, "watched": True, "actor": "dev:u42", "command_id": cid}},
        {"v": 1, "version": v + 2, "type": "command.outcome", "ts": "t",
         "data": {"command_id": cid, "command": "watch", "actor": "dev:u42", "outcome": "applied"}},
    ]
    client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": outcome}, headers=eng)
    assert client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()["pending_commands"] == []
    assert service.store.get_command(rid, cid)["outcome"] == "applied"
    assert client.get(f"/v1/runs/{rid}/api/state").json()["specimens"][sid]["watched"] is True
    assert client.post(f"/v1/runs/{rid}/commands", json={"command": "reboot"}).status_code == 400
    # a view-scope session may watch but not abort; an admin one may (queued, not applied here)
    viewer = TestClient(client.app, base_url="http://testserver")
    _session(viewer, rid, scope="view", user="v1")
    assert viewer.post(f"/v1/runs/{rid}/commands", json={"command": "abort"}).status_code == 403
    assert viewer.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": sid}).status_code == 200
    polled = client.get(f"/v1/runs/{rid}/commands/next?wait=0", headers=eng).json()["commands"]
    assert polled[0]["actor"] == "dev:v1"
    client.post(f"/v1/runs/{rid}/commands/{polled[0]['message_id']}/ack", headers=eng)
    client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": [
        {"v": 1, "version": v + 3, "type": "command.outcome", "ts": "t",
         "data": {"command_id": polled[0]["id"], "command": "watch", "actor": "dev:v1", "outcome": "noop"}}]},
        headers=eng)
    outcome = outcome + [{"v": 1, "version": v + 3, "type": "command.outcome", "ts": "t",
                          "data": {"command_id": polled[0]["id"], "command": "watch", "actor": "dev:v1",
                                   "outcome": "noop"}}]

    # --- exit and seal ---
    out = service.output_dir(rid)
    (out / "summary").mkdir(parents=True)
    (out / "summary" / "summary.fasta").write_text(">S1\nACGT\n")
    (out / "consensus" / "S1" / "cluster_debug").mkdir(parents=True)
    (out / "consensus" / "S1" / "S1-all.fasta").write_text(">S1-c0\nACGT\n")
    (out / "consensus" / "S1" / "cluster_debug" / "S1-c0-RiC3-final.fastq").write_text("@r\nA\n+\nI\n")
    (out / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events + outcome))
    assert client.get(f"/v1/runs/{rid}/results.zip", headers={"X-Service-Key": KEY}).status_code == 409
    r = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 0, "log_tail": "done"}, headers=eng)
    assert r.status_code == 200 and r.json()["state"] == "sealing"
    assert rid not in service.store.stage_holders("engine")               # released at once (and handed on)
    # a retried exit report is acknowledged, not re-applied
    assert client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 0}, headers=eng).status_code == 200
    assert _wait_state(service, rid, {"sealed"})["sealed"]["view_files"] >= 1
    assert client.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": sid}).status_code == 409
    r = client.get(f"/v1/runs/{rid}/results.zip", headers={"X-Service-Key": KEY}, follow_redirects=True)
    assert r.status_code == 200
    names = zipfile.ZipFile(io.BytesIO(r.content)).namelist()
    assert names == ["events.jsonl", "summary/summary.fasta"]
    # the same download from the dashboard, with the session cookie only
    r = client.get(f"/v1/runs/{rid}/results.zip", follow_redirects=False)
    assert r.status_code == 302 and "sig=" in r.headers["location"]
    assert client.get(f"/v1/runs/{rid}/events.jsonl", follow_redirects=True).status_code == 200
    assert TestClient(client.app, base_url="http://testserver").get(f"/v1/runs/{rid}/results.zip").status_code == 401
    view = zipfile.ZipFile(io.BytesIO(service.storage.get(f"runs/u42/{rid}/view.zip"))).namelist()
    assert {"events.jsonl", "summary/summary.fasta", "consensus/S1/S1-all.fasta"} <= set(view)
    r = client.get(f"/v1/runs/{rid}/output.zip", headers={"X-Service-Key": KEY}, follow_redirects=True)
    names = zipfile.ZipFile(io.BytesIO(r.content)).namelist()
    assert "consensus/S1/S1-all.fasta" in names and not any("cluster_debug" in n for n in names)
    assert client.get(f"/v1/runs/{rid}/reads.zip", headers={"X-Service-Key": KEY}, follow_redirects=True).status_code == 200
    assert client.get(f"/v1/runs/{rid}/other.zip", headers={"X-Service-Key": KEY}).status_code == 404
    sealed = service.get_run(rid)["sealed"]
    assert sealed["output_bytes"] > 0 and sealed["results"].endswith("results.zip")
    # a finished job's secret is good only for re-sending its exit report
    assert client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": []}, headers=eng).status_code == 403
    # the seal packed no per-cluster debug files, and no per-file tree
    assert not [n for n in view if "cluster_debug" in n]
    assert not service.storage.list(f"runs/u42/{rid}/output/")
    # sealing released the stage: the waiting run was launched
    st = client.get(f"/v1/runs/{other['id']}", headers={"X-Service-Key": KEY}).json()
    assert st["state"] == "running" and st["generation"] == 1
    assert launcher.specs[1].name == f"{other['id']}-engine-1"
    assert service.store.stage_holders("engine") == [other["id"]]


def test_reconcile_adopts_a_lost_launch_and_fails_a_dead_job(api):
    client, service, launcher = api
    run = _create(client)
    _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
    client.post(f"/v1/runs/{run['id']}/complete", headers={"Authorization": f"JobCode {run['job_code']}"})
    rid = run["id"]
    # Simulate a crash after Batch accepted the job but before the handle was stored:
    # reopen the intent and forget the job record
    iid = service.store.open_intent(rid, "launch-engine", {"generation": 1, "name": f"{rid}-engine-1"})
    service.store.update_run(rid, {"jobs": {}})
    # a young intent may be a launch still in flight: left alone
    assert service.reconcile()["adopted"] == 0
    assert len(service.store.list_open_intents()) == 1
    result = service.reconcile(intent_grace_s=0)
    assert result["adopted"] == 1
    assert service.store.list_open_intents() == []
    assert service.get_run(rid)["jobs"][f"{rid}-engine-1"]["id"] == "job-1"
    assert len(launcher.specs) == 1                       # adopted, not resubmitted
    # The job dies without an exit report
    launcher.states["job-1"] = JobStatus("failed", exit_code=137, reason="OOM")
    assert service.reconcile()["failed"] == 1
    st = _wait_state(service, rid, {"failed"})
    assert st["exit"]["reason"] == "OOM" and st["sealed"]["results"]
    assert service.store.stage_holders("engine") == []


def test_an_exit_report_and_reconcile_apply_once(api):
    """The periodic reconcile runs beside live traffic: whichever of the
    job's own exit report and a reconcile judgement lands first wins, and
    the other changes nothing."""
    client, service, launcher = api
    # reconcile first: the job is gone, then its late report arrives
    run = _create(client)
    rid = run["id"]
    _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
    client.post(f"/v1/runs/{rid}/complete", headers={"Authorization": f"JobCode {run['job_code']}"})
    eng = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    launcher.states[service.get_run(rid)["jobs"][f"{rid}-engine-1"]["id"]] = \
        JobStatus("failed", exit_code=143, reason="Stopped by operator")
    assert service.reconcile()["failed"] == 1
    r = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 0}, headers=eng)
    assert r.status_code == 200
    st = _wait_state(service, rid, {"failed"})
    assert st["exit"]["code"] == 143 and st["exit"]["reason"] == "Stopped by operator"

    # report first: a reconcile pass that read the run as running changes nothing
    run = _create(client, token="two")
    rid = run["id"]
    _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
    client.post(f"/v1/runs/{rid}/complete", headers={"Authorization": f"JobCode {run['job_code']}"})
    eng = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    stale = service.get_run(rid)
    client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 0, "log_tail": "done"}, headers=eng)
    st = _wait_state(service, rid, {"sealed"})
    launcher.states[stale["jobs"][f"{rid}-engine-1"]["id"]] = JobStatus("succeeded", exit_code=0)
    real_list = service.store.list_runs
    service.store.list_runs = lambda **kw: [stale] if kw.get("states") and "running" in kw["states"] else real_list(**kw)
    try:
        assert service.reconcile()["failed"] == 0
    finally:
        service.store.list_runs = real_list
    st = service.get_run(rid)
    assert st["state"] == "sealed" and st["exit"]["log_tail"] == "done"


def test_a_dorado_exit_applies_once(api):
    client, service, launcher = api
    run = _create_pod5(client).json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["a.pod5"])
    client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr)
    dor = {"X-Job-Secret": launcher.specs[0].env["SPECIMUX_JOB_SECRET"]}
    stale = service.get_run(rid)
    r = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 143,
                                                   "log_tail": "wrapper: stopped (SIGTERM)"}, headers=dor)
    assert r.json()["state"] == "failed" and service.store.stage_holders("dorado") == []
    # a second judgement of the same job (a reconcile that read it earlier) is a no-op
    service._dorado_exited(stale, 1, 1, reason="no exit report")
    st = service.get_run(rid)
    assert st["exit"]["code"] == 143 and "reason" not in st["exit"]


def test_cancel_and_retry(api):
    client, service, launcher = api
    hdr = {"X-Service-Key": KEY}
    # a run still waiting for input fails at once
    run = _create(client)
    r = client.post(f"/v1/runs/{run['id']}/cancel", json={"reason": "wrong index"}, headers=hdr)
    assert r.status_code == 200
    st = r.json()
    assert st["state"] == "failed" and st["exit"]["reason"] == "cancelled by dev: wrong index"
    assert client.post(f"/v1/runs/{run['id']}/cancel", headers=hdr).status_code == 409
    assert client.post(f"/v1/runs/{run['id']}/retry", headers=hdr).status_code == 409   # nothing uploaded

    # an engine failure: the retry relaunches the engine at the next generation
    run = _create(client, token="e")
    rid = run["id"]
    _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
    client.post(f"/v1/runs/{rid}/complete", headers={"Authorization": f"JobCode {run['job_code']}"})
    eng = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 1, "log_tail": "boom"}, headers=eng)
    _wait_state(service, rid, {"failed"})
    st = client.post(f"/v1/runs/{rid}/retry", headers=hdr).json()
    assert st["state"] == "running" and st["generation"] == 2 and launcher.specs[-1].name == f"{rid}-engine-2"
    assert not st.get("exit") and not st.get("sealed")
    eng2 = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    ups = client.post(f"/v1/runs/{rid}/package-uploads", json={"generation": 2}, headers=eng2).json()
    assert set(ups) == {"results.zip", "output.zip", "reads.zip"} and ups["reads.zip"]["key"].endswith(f"{rid}/reads.zip")
    assert client.post(f"/v1/runs/{rid}/package-uploads", json={"generation": 1}, headers=eng).status_code in (403, 409)

    # basecalling: the job is stopped, the wrapper's report fails the run with the reason
    run = _create_pod5(client, token="p").json()
    rid = run["id"]
    manifest, up = _upload_pod5(client, run, ["a.pod5", "b.pod5"])
    client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=up)
    dor = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    job_id = service.get_run(rid)["jobs"][f"{rid}-dorado-1"]["id"]
    # one file got done before the stop
    bundle = client.get(f"/v1/runs/{rid}/job", headers=dor).json()
    a = bundle["fastq_uploads"]["a.fastq"]
    client.put(a["url"], content=b"@r1\nACGT\n+\nIIII\n")
    client.post(f"/v1/runs/{rid}/basecalled", json={"generation": 1, "name": "a.fastq", "key": a["key"],
                                                    "reads_in": 1, "reads_out": 1}, headers=dor)
    r = client.post(f"/v1/runs/{rid}/cancel", headers=hdr)
    assert r.status_code == 200 and r.json()["state"] == "basecalling" and r.json()["cancel"]
    assert launcher.states[job_id].state == "failed"                 # Batch was told to stop it
    assert client.delete(f"/v1/runs/{rid}", headers=hdr).status_code == 409
    st = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 143,
                                                   "log_tail": "wrapper: stopped (SIGTERM)"}, headers=dor).json()
    assert st["state"] == "failed" and st["exit"]["stage"] == "dorado" and st["exit"]["reason"] == "cancelled by dev"
    # retry: basecalling again at the next generation, skipping the file already done
    st = client.post(f"/v1/runs/{rid}/retry", headers=hdr).json()
    assert st["state"] == "basecalling" and st["generation"] == 2 and not st.get("cancel") and not st.get("exit")
    dor2 = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    assert client.get(f"/v1/runs/{rid}/job", headers=dor2).json()["basecalled"] == ["a.fastq"]
    assert client.post(f"/v1/runs/{rid}/retry", headers=hdr).status_code == 409   # not failed


def test_sessions_gate_the_dashboard(api):
    """No session: the page and its assets load (so the page can go and get
    one), the data does not. A token is exchanged once, is scoped to its run
    and host, and dies with the host."""
    client, service, _ = api
    run = _create(client)
    rid = run["id"]
    assert client.get(f"/v1/runs/{rid}/").status_code == 200
    assert client.get(f"/v1/runs/{rid}/static/runtime.js").status_code == 200
    assert client.get(f"/v1/runs/{rid}/api/state").status_code == 401
    assert client.get(f"/v1/runs/{rid}/events").status_code == 401
    assert client.post(f"/v1/runs/{rid}/commands", json={"command": "watch"}).status_code == 401
    assert client.post("/v1/session", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.post("/v1/session").status_code == 401
    # scope and ttl are validated at minting
    hdr = {"X-Service-Key": KEY}
    assert client.post(f"/v1/runs/{rid}/tokens", json={"scope": "root"}, headers=hdr).status_code == 400
    assert client.post(f"/v1/runs/{rid}/tokens", json={"ttl_seconds": 99999}, headers=hdr).status_code == 400
    tok = client.post(f"/v1/runs/{rid}/tokens", headers=hdr).json()      # defaults: the key's label, view
    assert tok["expires_in"] == 60 and tok["scope"] == "view" and tok["dashboard_url"].endswith(f"/v1/runs/{rid}/")
    r = client.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"})
    assert r.status_code == 200 and r.json()["user"] == "dev" and r.json()["host"] == "dev"
    cookie = r.headers["set-cookie"]
    assert f"Path=/v1/runs/{rid}/" in cookie and "HttpOnly" in cookie and "SameSite=lax" in cookie.replace("Lax", "lax")
    assert "Secure" not in cookie                    # http base URL: local development
    assert client.get(f"/v1/runs/{rid}/api/state").status_code == 200
    # the cookie is for that run only
    other = _create(client)
    assert client.get(f"/v1/runs/{other['id']}/api/state").status_code == 401
    # a session is refused once its host is disabled, and a token from a
    # disabled host cannot be exchanged
    service.update_host("dev", disabled=True)
    assert client.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"}).status_code == 401
    assert client.post(f"/v1/runs/{rid}/tokens", headers=hdr).status_code == 403
    service.update_host("dev", disabled=False)
    # an expired token
    from specimux_cloud.runapi import auth
    old, _ = auth.mint(auth.TOKEN, SECRET, host="dev", user="u", run=rid, scope="view", ttl_s=-1)
    assert client.post("/v1/session", headers={"Authorization": f"Bearer {old}"}).status_code == 401
    # a token signed with another secret
    forged, _ = auth.mint(auth.TOKEN, "other-secret", host="dev", user="u", run=rid, scope="admin", ttl_s=60)
    assert client.post("/v1/session", headers={"Authorization": f"Bearer {forged}"}).status_code == 401


def test_hosts_keys_and_scoping(api):
    """Each host sees only its own runs; keys are labelled, rotated with a
    grace period, revoked, and a disabled host's keys stop working."""
    client, service, _ = api
    other_host, other_key = service.add_host("fundis", name="FUNDIS", label="lab-staff")
    assert other_key.startswith("fundis.") and other_host["keys"][0]["label"] == "lab-staff"
    mine = _create(client)
    theirs = client.post("/v1/runs", data={"spec": json.dumps({"mode": "batch"}), "user_id": "h"},
                         files={"primers": ("p", b">a\nA\n"), "specimens": ("s", b"SampleID\tPrimerPool\nS1\tP\n")},
                         headers={"X-Service-Key": other_key}).json()
    assert theirs["host"] == "fundis"
    them, me = {"X-Service-Key": other_key}, {"X-Service-Key": KEY}
    assert [r["id"] for r in client.get("/v1/runs", headers=me).json()["runs"]] == [mine["id"]]
    assert [r["id"] for r in client.get("/v1/runs", headers=them).json()["runs"]] == [theirs["id"]]
    for method, path in (("GET", f"/v1/runs/{mine['id']}"), ("DELETE", f"/v1/runs/{mine['id']}"),
                         ("POST", f"/v1/runs/{mine['id']}/tokens"), ("POST", f"/v1/runs/{mine['id']}/job-code"),
                         ("GET", f"/v1/runs/{mine['id']}/results.zip"), ("POST", f"/v1/runs/{mine['id']}/complete")):
        r = client.request(method, path, headers=them)
        assert r.status_code == 404, (method, path, r.status_code)
    assert client.get(f"/v1/runs/{mine['id']}", headers=me).status_code == 200
    # the client token is per host too
    again = client.post("/v1/runs", data={"spec": "{}", "user_id": "h", "client_token": "ct"},
                        files={"primers": ("p", b">a\nA\n"), "specimens": ("s", b"x")}, headers=them).json()
    other_again = client.post("/v1/runs", data={"spec": "{}", "user_id": "u", "client_token": "ct"},
                              files={"primers": ("p", b">a\nA\n"), "specimens": ("s", b"x")}, headers=me).json()
    assert again["id"] != other_again["id"]
    # keys: bad shapes, a second label, rotation with grace, revocation
    for bad in ("", "fundis", "fundis.wrong", "nope.x", KEY + "x"):
        assert client.get("/v1/hosts/me", headers={"X-Service-Key": bad}).status_code == 403
    server_key = service.add_key("fundis", "server")
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": server_key}).json()["label"] == "server"
    rotated = service.rotate_key("fundis", "lab-staff", grace_s=3600)
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": other_key}).status_code == 200   # grace
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": rotated}).json()["label"] == "lab-staff"
    expired = service.rotate_key("fundis", "lab-staff", grace_s=-1)
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": rotated}).status_code == 403
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": expired}).status_code == 200
    assert service.revoke_key("fundis", "lab-staff") == 3
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": expired}).status_code == 403
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": server_key}).status_code == 200
    service.update_host("fundis", disabled=True)
    assert client.get("/v1/hosts/me", headers={"X-Service-Key": server_key}).status_code == 403
    listed = {h["id"]: h for h in service.list_hosts()}
    assert "hash" not in listed["fundis"]["keys"][0] and listed["fundis"]["disabled"]
    with pytest.raises(Exception):
        service.add_host("Bad Id")
    with pytest.raises(Exception):
        service.add_host("dev")


def test_job_code_regeneration(api):
    client, service, _ = api
    run = _create(client)
    rid = run["id"]
    new = client.post(f"/v1/runs/{rid}/job-code", headers={"X-Service-Key": KEY}).json()
    assert new["job_code"].startswith(rid + ".") and new["job_code"] != run["job_code"]
    old_hdr = {"Authorization": f"JobCode {run['job_code']}"}
    assert client.post(f"/v1/runs/{rid}/uploads", json={"files": ["a.fastq"]}, headers=old_hdr).status_code == 403
    _upload(client, new, "a.fastq", b"@r\nA\n+\nI\n")
    r = client.post(f"/v1/runs/{rid}/complete", headers={"X-Service-Key": KEY})    # the job page may complete
    assert r.status_code == 200 and r.json()["state"] == "running"
    assert client.post(f"/v1/runs/{rid}/job-code", headers={"X-Service-Key": KEY}).status_code == 409


def test_ui_assets_are_served_raw_for_a_proxying_host(api):
    client, _, _ = api
    from specimux_suite import __version__
    r = client.get(f"/v1/ui/{__version__}/index.html")
    assert r.status_code == 200 and "{{asset_base}}" in r.text
    assert client.get(f"/v1/ui/{__version__}/../pages.py").status_code in (404, 400)
    assert client.get("/v1/ui/0.0.0/index.html").status_code == 404


def test_a_refused_launch_leaves_the_run_relaunchable(api):
    client, service, launcher = api
    run = _create(client)
    _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
    launcher.submit = lambda spec: (_ for _ in ()).throw(RuntimeError("AccessDenied"))
    r = client.post(f"/v1/runs/{run['id']}/complete", headers={"Authorization": f"JobCode {run['job_code']}"})
    assert r.status_code == 502 and "AccessDenied" in r.json()["error"]
    st = service.get_run(run["id"])
    assert st["state"] == "input_complete" and st["generation"] == 0 and not st.get("job_secret")
    assert service.store.stage_holders("engine") == []
    assert service.store.list_open_intents() == []
    # once the launcher works again, the run launches
    launcher.submit = FakeLauncher.submit.__get__(launcher)
    assert service.launch_next_queued()["state"] == "running"


def test_an_abandoned_upload_can_be_deleted(api):
    client, service, _ = api
    run = _create(client)
    _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
    assert client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).json()["state"] == "uploading"
    assert client.delete(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).status_code == 200
    assert client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": KEY}).status_code == 404
    # nothing was processed: the upload archive the run made goes with it
    assert not service.storage.list(f"archives/u42/{run['archive_id']}/")
    assert service.store.get_archive(run["archive_id"])["deleted"]


# --- POD5 runs: the dorado stage in front of the engine ---

POD5_FILES = {"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
              "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")}


def _create_pod5(client, basecall=None, token=None):
    spec = {"mode": "batch", "input": "pod5", "profile": "default", "min_reads": 10}
    if basecall is not None:
        spec["basecall"] = basecall
    data = {"spec": json.dumps(spec), "user_id": "u42"}
    if token:
        data["client_token"] = token
    return client.post("/v1/runs", data=data, files=POD5_FILES, headers={"X-Service-Key": KEY})


def test_pod5_run_settings_are_validated_and_recorded(api):
    client, service, launcher = api
    r = _create_pod5(client)
    assert r.status_code == 200, r.text
    # defaults filled in: the protocol's sup, --no-trim, a wide 100-3000, no qscore floor
    assert r.json()["spec"]["basecall"] == {"model": "sup@v5.0.0", "min_length": 100, "max_length": 3000,
                                            "min_qscore": None}
    r = _create_pod5(client, {"model": "hac@v6.0.0", "min_length": 100, "max_length": 700, "min_qscore": 9})
    assert r.json()["spec"]["basecall"] == {"model": "hac@v6.0.0", "min_length": 100, "max_length": 700,
                                            "min_qscore": 9.0}
    assert _create_pod5(client, {"model": "sup@v1.0.0"}).status_code == 400          # not baked in the image
    assert _create_pod5(client, {"max_length": 100, "min_length": 400}).status_code == 400
    assert _create_pod5(client, {"min_length": "many"}).status_code == 400
    assert _create_pod5(client, {"trim": True}).status_code == 400
    # basecalling settings on a FASTQ run make no sense
    data = {"spec": json.dumps({"mode": "batch", "input": "fastq", "basecall": {"model": "sup@v5.0.0"}}), "user_id": "u42"}
    assert client.post("/v1/runs", data=data, files=POD5_FILES, headers={"X-Service-Key": KEY}).status_code == 400
    opts = client.get("/v1/options", headers={"X-Service-Key": KEY}).json()
    assert opts["dorado_models"] == ["sup@v5.0.0", "sup@v5.2.0", "hac@v6.0.0"]
    assert (opts["basecall_defaults"]["min_length"], opts["basecall_defaults"]["max_length"]) == (100, 3000)
    assert "references" not in opts


def _upload_pod5(client, run, names):
    hdr = {"Authorization": f"JobCode {run['job_code']}"}
    r = client.post(f"/v1/runs/{run['id']}/uploads", json={"files": names}, headers=hdr)
    assert r.status_code == 200, r.text
    manifest = []
    for name, up in r.json()["uploads"].items():
        assert up["key"] == f"archives/u42/{run['archive_id']}/pod5/{name}"
        put = client.put(up["url"], content=f"POD5:{name}".encode())
        manifest.append({"key": up["key"], "etag": put.headers["etag"].strip('"')})
    return manifest, hdr


def test_pod5_run_is_basecalled_then_run(api):
    client, service, launcher = api
    run = _create_pod5(client).json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["b.pod5", "a.pod5"])

    # complete launches the dorado job, not the engine
    r = client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr)
    assert r.status_code == 200, r.text
    st = r.json()
    assert st["state"] == "basecalling" and st["generation"] == 1
    assert st["basecalling"] == {"files": [], "done": 0, "total": 2, "reads_in": 0, "reads_out": 0}
    spec = launcher.specs[0]
    assert spec.kind == "dorado" and spec.name == f"{rid}-dorado-1" and spec.vcpus is None
    assert spec.env["SPECIMUX_GENERATION"] == "1" and "SPECIMUX_WORK_DIR" not in spec.env
    assert service.store.stage_holders("dorado") == [rid] and service.store.stage_holders("engine") == []
    # a second POD5 run waits for the dorado stage; a FASTQ run takes the engine at once
    other = _create_pod5(client, token="second").json()
    m2, h2 = _upload_pod5(client, other, ["c.pod5"])
    assert client.post(f"/v1/runs/{other['id']}/complete", json={"manifest": m2}, headers=h2).json()["state"] == "input_complete"
    fastq_run = _create(client)
    _upload(client, fastq_run, "x.fastq", b"@r\nA\n+\nI\n")
    r = client.post(f"/v1/runs/{fastq_run['id']}/complete", headers={"Authorization": f"JobCode {fastq_run['job_code']}"})
    assert r.json()["state"] == "running" and launcher.specs[1].kind == "engine"
    # dashboard sessions and commands: not during basecalling
    _session(client, rid)
    assert client.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": "S1"}).status_code == 409
    assert client.delete(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).status_code == 409

    # --- the dorado job's side ---
    dor = {"X-Job-Secret": spec.env["SPECIMUX_JOB_SECRET"]}
    bundle = client.get(f"/v1/runs/{rid}/job", headers=dor).json()
    assert [e["name"] for e in bundle["pod5"]] == ["b.pod5", "a.pod5"]      # manifest order; the wrapper sorts
    assert bundle["basecall"]["model"] == "sup@v5.0.0" and bundle["basecalled"] == [] and bundle["reads"] == []
    assert set(bundle["fastq_uploads"]) == {"a.fastq", "b.fastq"}
    a = bundle["fastq_uploads"]["a.fastq"]
    assert a["key"] == f"runs/u42/{rid}/fastq/a.fastq"
    assert client.get(bundle["pod5"][1]["url"]).content == b"POD5:a.pod5"
    # progress on the file in flight, for the run page; cleared once delivered
    r = client.post(f"/v1/runs/{rid}/basecall-progress",
                    json={"generation": 1, "file": "a.pod5", "reads": 1200, "estimate": 4000}, headers=dor)
    assert r.status_code == 200
    cur = client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()["basecall_current"]
    assert (cur["file"], cur["reads"], cur["estimate"]) == ("a.pod5", 1200, 4000)
    rec = client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()
    assert rec["basecall_attempt"]["generation"] == 1                  # the attempt's clock, from its first report
    from specimux_cloud.progress import basecall_text
    assert basecall_text(rec).endswith("· 1,200 reads called so far (0 of 2 file(s) done, 1st in progress)")
    assert client.post(f"/v1/runs/{rid}/basecall-progress", json={"generation": 1, "reads": -1, "estimate": 1},
                       headers=dor).status_code == 400
    assert client.post(f"/v1/runs/{rid}/basecall-progress", json={"generation": 2, "reads": 1, "estimate": 1},
                       headers=dor).status_code == 409                                                # another generation
    # a report before the upload, a report for a stranger's file
    r = client.post(f"/v1/runs/{rid}/basecalled", json={"generation": 1, "name": "a.fastq", "key": a["key"]}, headers=dor)
    assert r.status_code == 409
    r = client.post(f"/v1/runs/{rid}/basecalled", json={"generation": 1, "name": "z.fastq", "key": a["key"]}, headers=dor)
    assert r.status_code == 400
    assert client.put(a["url"], content=b"@r1\n" + b"A" * 500 + b"\n+\n" + b"I" * 500 + b"\n").status_code == 200
    r = client.post(f"/v1/runs/{rid}/basecalled", json={"generation": 1, "name": "a.fastq", "key": a["key"],
                                                       "reads_in": 10, "reads_out": 1}, headers=dor)
    assert r.status_code == 200 and r.json() == {"done": 1, "total": 2}
    st = client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()
    assert st["basecalling"]["done"] == 1 and st["basecalling"]["reads_in"] == 10 and st["basecalling"]["reads_out"] == 1
    assert st["basecall_current"] is None
    # a second attempt of the job sees what the first delivered
    assert client.get(f"/v1/runs/{rid}/job", headers=dor).json()["basecalled"] == ["a.fastq"]
    # exit 0 with a file missing is a failure: the listing is the truth
    r = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 0, "log_tail": "bye"}, headers=dor)
    assert r.status_code == 200
    st = r.json()
    assert st["state"] == "failed" and st["exit"]["stage"] == "dorado" and "1 of 2" in st["exit"]["reason"]
    assert st["sealed"]["error"] == "basecalling failed"
    assert service.store.stage_holders("dorado") == [other["id"]]                  # released and handed on
    assert launcher.specs[2].name == f"{other['id']}-dorado-1"
    assert client.get(f"/v1/runs/{rid}/results.zip", headers={"X-Service-Key": KEY}).status_code in (302, 404)  # no package

    # --- the waiting run goes all the way ---
    oid = other["id"]
    dor2 = {"X-Job-Secret": launcher.specs[2].env["SPECIMUX_JOB_SECRET"]}
    b2 = client.get(f"/v1/runs/{oid}/job", headers=dor2).json()
    c = b2["fastq_uploads"]["c.fastq"]
    client.put(c["url"], content=b"@r1\nACGT\n+\nIIII\n")
    client.post(f"/v1/runs/{oid}/basecalled", json={"generation": 1, "name": "c.fastq", "key": c["key"],
                                                   "reads_in": 3, "reads_out": 1}, headers=dor2)
    # the engine stage is still held by the FASTQ run: the run waits, basecalled
    r = client.post(f"/v1/runs/{oid}/exit", json={"generation": 1, "exit_code": 0}, headers=dor2)
    st = r.json()
    assert st["state"] == "input_complete" and st["basecalled"][0]["key"] == c["key"] and st["exit"] is None
    assert st["basecalling"]["finished"] and service.store.stage_holders("dorado") == []
    # a retried exit report from the dorado job is stale now (generation 1 is over for that job... still 1)
    # the FASTQ run's engine exits: the stage is released and the POD5 run's engine launches
    eng = {"X-Job-Secret": launcher.specs[1].env["SPECIMUX_JOB_SECRET"]}
    client.post(f"/v1/runs/{fastq_run['id']}/exit", json={"generation": 1, "exit_code": 0}, headers=eng)
    st = client.get(f"/v1/runs/{oid}", headers={"X-Service-Key": KEY}).json()
    assert st["state"] == "running" and st["generation"] == 2
    espec = launcher.specs[3]
    assert espec.kind == "engine" and espec.name == f"{oid}-engine-2"
    # the engine's bundle: the basecalled FASTQ is its reads
    ebundle = client.get(f"/v1/runs/{oid}/job", headers={"X-Job-Secret": espec.env["SPECIMUX_JOB_SECRET"]}).json()
    assert ebundle["generation"] == 2
    assert [r["name"] for r in ebundle["reads"]] == ["c.fastq"] and ebundle["reads"][0]["key"] == c["key"]
    assert client.get(ebundle["reads"][0]["url"]).content == b"@r1\nACGT\n+\nIIII\n"
    # the old dorado secret is dead
    assert client.get(f"/v1/runs/{oid}/job", headers=dor2).status_code == 403


def test_pod5_failure_paths(api):
    client, service, launcher = api
    run = _create_pod5(client).json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["a.pod5"])
    client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr)
    dor = {"X-Job-Secret": launcher.specs[0].env["SPECIMUX_JOB_SECRET"]}
    # the job fails outright
    r = client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 1, "log_tail": "no CUDA device"}, headers=dor)
    st = r.json()
    assert st["state"] == "failed" and st["exit"]["code"] == 1 and st["exit"]["log_tail"] == "no CUDA device"
    assert service.store.stage_holders("dorado") == []
    # a retried report is acknowledged
    assert client.post(f"/v1/runs/{rid}/exit", json={"generation": 1, "exit_code": 1}, headers=dor).status_code == 200
    # a failed run can be deleted; its archive stays
    assert client.delete(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).status_code == 200
    assert service.storage.list(f"archives/u42/{run['archive_id']}/pod5/")

    # the job dies without a report: reconcile judges it by what it delivered
    run = _create_pod5(client, token="two").json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["a.pod5"])
    client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr)
    spec = launcher.specs[1]
    launcher.states["job-2"] = JobStatus("failed", exit_code=137, reason="Host EC2 terminated")
    assert service.reconcile()["failed"] == 1
    st = service.get_run(rid)
    assert st["state"] == "failed" and st["exit"]["reason"] == "Host EC2 terminated" and st["exit"]["code"] == 137
    # ...and by what it delivered: a job that died after the last upload still counts as done
    run = _create_pod5(client, token="three").json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["a.pod5"])
    client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr)
    spec = launcher.specs[2]
    dor = {"X-Job-Secret": spec.env["SPECIMUX_JOB_SECRET"]}
    b = client.get(f"/v1/runs/{rid}/job", headers=dor).json()
    client.put(b["fastq_uploads"]["a.fastq"]["url"], content=b"@r\nA\n+\nI\n")
    launcher.states["job-3"] = JobStatus("succeeded", exit_code=0)
    assert service.reconcile()["failed"] == 0
    st = service.get_run(rid)
    assert st["state"] == "running" and st["generation"] == 2 and st["basecalled"][0]["key"].endswith("/fastq/a.fastq")

    # a refused launch leaves the run relaunchable in input_complete
    run = _create_pod5(client, token="four").json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["a.pod5"])
    launcher.submit = lambda spec: (_ for _ in ()).throw(RuntimeError("no GPU quota"))
    r = client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr)
    assert r.status_code == 502 and "no GPU quota" in r.text
    st = service.get_run(rid)
    assert st["state"] == "input_complete" and st["generation"] == 0 and st["basecalling"]["done"] == 0
    assert service.store.stage_holders("dorado") == []


def test_references_are_kept_once_by_content(api):
    """A reference is stored by its SHA-256: a later run of the same host
    names it by hash instead of sending it again, and the engine gets it
    either way."""
    import hashlib
    client, service, _ = api
    ref = b'>REF1 name="Amanita muscaria"\nACGTACGTAC\n'
    sha = hashlib.sha256(ref).hexdigest()
    svc = {"X-Service-Key": KEY}
    base = {"spec": json.dumps({"mode": "batch", "input": "fastq"}), "user_id": "u42"}
    inputs = {"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
              "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")}
    assert client.get(f"/v1/references/sha256/{sha}", headers=svc).status_code == 404
    # unknown by hash alone: the host must send the file
    r = client.post("/v1/runs", data={**base, "reference_sha256": sha}, files=inputs, headers=svc)
    assert r.status_code == 409 and "send the reference" in r.text
    # a wrong hash for the file sent, or not a hash at all
    r = client.post("/v1/runs", data={**base, "reference_sha256": "0" * 64},
                    files={**inputs, "reference": ("refs.fasta", ref)}, headers=svc)
    assert r.status_code == 400 and sha in r.text
    assert client.post("/v1/runs", data={**base, "reference_sha256": "nope"}, files=inputs,
                       headers=svc).status_code == 400

    first = client.post("/v1/runs", data=base, files={**inputs, "reference": ("refs.fasta", ref)}, headers=svc).json()
    assert first["spec"]["reference_sha256"] == sha and first["spec"]["reference_name"] == "refs.fasta"
    assert client.get(f"/v1/references/sha256/{sha.upper()}", headers=svc).json() == {"sha256": sha, "size": len(ref)}
    assert not [o for o in service.storage.list("runs/") if o.key.endswith("/input/reference")]

    second = client.post("/v1/runs", data={**base, "reference_sha256": sha}, files=inputs, headers=svc).json()
    assert second["spec"]["reference_sha256"] == sha
    for run in (first, second):
        bundle = service.job_bundle(run["id"])
        assert set(bundle["inputs"]) == {"primers", "specimens", "reference"}
    # deleting a run leaves the shared reference
    assert client.delete(f"/v1/runs/{first['id']}", headers=svc).status_code == 200
    assert service.storage.get(service.reference_key(sha)) == ref
    # and the check needs a key like every host route
    assert client.get(f"/v1/references/sha256/{sha}").status_code in (401, 403)


def test_two_slots_run_two_runs_and_queue_the_third(api):
    """With two slots per stage two runs run at once, a third waits in
    input_complete and takes the slot the first frees; the load view counts
    all of it, across hosts, without naming a run."""
    client, service, launcher = api
    service.config.stage_slots = {"engine": 2, "dorado": 2}
    svc = {"X-Service-Key": KEY}
    assert client.get("/v1/load").status_code in (401, 403)
    runs = []
    for i in range(3):
        run = _create(client, token=f"slots-{i}")
        _upload(client, run, "x.fastq", b"@r\nA\n+\nI\n")
        r = client.post(f"/v1/runs/{run['id']}/complete", headers={"Authorization": f"JobCode {run['job_code']}"})
        runs.append(r.json())
    assert [r["state"] for r in runs] == ["running", "running", "input_complete"]
    assert sorted(service.store.stage_holders("engine")) == sorted(r["id"] for r in runs[:2])
    # the second engine job is still waiting for a machine
    launcher.states["job-2"] = JobStatus("pending")
    load = client.get("/v1/load", headers=svc).json()
    assert load["stages"]["engine"] == {"slots": 2, "busy": 2, "waiting_for_machine": 1, "queued": 1}
    assert load["stages"]["dorado"] == {"slots": 2, "busy": 0, "waiting_for_machine": 0, "queued": 0}
    assert load["uploading"] == 0 and load["sealing"] == 0
    assert runs[0]["id"] not in json.dumps(load)                    # counts only
    # the first run's engine exits: its slot goes to the third at once
    first = service.get_run(runs[0]["id"])
    service.report_exit(first["id"], first["generation"], 0, "done")
    third = service.get_run(runs[2]["id"])
    assert third["state"] == "running" and launcher.specs[-1].name == f"{third['id']}-engine-1"
    assert sorted(service.store.stage_holders("engine")) == sorted([runs[1]["id"], third["id"]])
    # the view is cached briefly, then current
    assert client.get("/v1/load", headers=svc).json()["stages"]["engine"]["queued"] == 1
    assert service.load(max_age_s=0)["stages"]["engine"]["queued"] == 0


def test_abandoned_uploads_end_incomplete(api):
    """An upload with no request for a day, or a run never uploaded to in a
    week, becomes incomplete at reconcile: its job code stops working, the
    reason is recorded, it leaves the load view, and it can be deleted.
    Upload requests keep a run open; nothing else does."""
    client, service, _ = api
    svc = {"X-Service-Key": KEY}
    t0 = time.time()
    stale = _create(client, token="idle-stale")
    _upload(client, stale, "a.fastq", b"@r\nA\n+\nI\n")
    busy = _create(client, token="idle-busy")
    _upload(client, busy, "a.fastq", b"@r\nA\n+\nI\n")
    fresh = _create(client, token="idle-fresh")          # created, never uploaded
    assert service.get_run(stale["id"])["last_upload"] >= t0

    # 23 hours on nothing expires; the busy run uploads again at hour 23
    assert service.expire_idle_uploads(now=t0 + 23 * 3600) == []
    service.store.update_run(busy["id"], {"last_upload": t0 + 23 * 3600})
    # the watching uploader's status checks are not activity
    for _ in range(3):
        assert client.get(f"/v1/runs/{stale['id']}/upload",
                          headers={"Authorization": f"JobCode {stale['job_code']}"}).json()["open"] is True
    assert service.expire_idle_uploads(now=t0 + 25 * 3600) == [stale["id"]]
    st = service.get_run(stale["id"])
    assert st["state"] == "incomplete" and st["exit"]["reason"] == "upload abandoned: no upload for 24 h"
    assert st["exit"]["stage"] == "upload" and st["exit"]["code"] is None
    hdr = {"Authorization": f"JobCode {stale['job_code']}"}
    assert client.post(f"/v1/runs/{stale['id']}/uploads", json={"files": ["b.fastq"]}, headers=hdr).status_code == 409
    assert client.get(f"/v1/runs/{stale['id']}/upload", headers=hdr).json() == \
        {"run_id": stale["id"], "state": "incomplete", "open": False}
    # the busy run's day runs from its last upload; a never-uploaded run gets a week
    assert service.expire_idle_uploads(now=t0 + 6 * 24 * 3600) == [busy["id"]]
    assert service.expire_idle_uploads(now=t0 + 8 * 24 * 3600) == [fresh["id"]]
    assert service.get_run(fresh["id"])["exit"]["reason"] == "upload abandoned: nothing uploaded within 7 days of creation"
    assert service.load(max_age_s=0)["uploading"] == 0
    assert client.delete(f"/v1/runs/{stale['id']}", headers=svc).status_code == 200


def test_each_stage_has_its_own_job_identity(api):
    """Two stages' jobs can be live at once (the shape a live POD5 run
    needs): launching the engine does not fence off the basecalling job,
    each job's secret works only for its own stage's routes, generation
    numbers stay unique, and a cancel stops both."""
    client, service, launcher = api
    run = _create_pod5(client, token="identity").json()
    rid = run["id"]
    manifest, hdr = _upload_pod5(client, run, ["a.pod5", "b.pod5"])
    assert client.post(f"/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=hdr).json()["state"] == "basecalling"
    dorado = launcher.specs[-1]
    dor = {"X-Job-Secret": dorado.env["SPECIMUX_JOB_SECRET"]}
    # the engine starts while basecalling is still going
    service.launch_engine(rid)
    engine = launcher.specs[-1]
    eng = {"X-Job-Secret": engine.env["SPECIMUX_JOB_SECRET"]}
    assert (dorado.generation, engine.generation) == (1, 2)
    st = service.get_run(rid)
    assert {k: (v["generation"], v["active"]) for k, v in st["stages"].items()} == {"dorado": (1, True), "engine": (2, True)}
    public = client.get(f"/v1/runs/{rid}", headers={"X-Service-Key": KEY}).json()
    assert "job_secret" not in json.dumps(public) and public["stages"]["engine"]["generation"] == 2
    # each job sees its own generation and may use only its own stage's routes
    assert client.get(f"/v1/runs/{rid}/job", headers=dor).json()["generation"] == 1
    assert client.get(f"/v1/runs/{rid}/job", headers=eng).json()["generation"] == 2
    assert client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": []}, headers=dor).status_code == 403
    assert client.get(f"/v1/runs/{rid}/commands/next?wait=0", headers=dor).status_code == 403
    assert client.post(f"/v1/runs/{rid}/basecalled", json={"generation": 2, "name": "a.fastq", "key": "k"},
                       headers=eng).status_code == 403
    assert client.post(f"/v1/runs/{rid}/ingest", json={"generation": 2, "events": []}, headers=eng).status_code == 200
    # the basecalling job is not fenced off by the engine's launch
    key = service.basecalled_key(service.get_run(rid), manifest[0]["key"])
    service.storage.put(key, b"@r\nA\n+\nI\n")
    r = client.post(f"/v1/runs/{rid}/basecalled", json={"generation": 1, "name": "a.fastq", "key": key,
                                                         "reads_in": 1, "reads_out": 1}, headers=dor)
    assert r.status_code == 200 and r.json() == {"done": 1, "total": 2}
    # a stale generation for a stage is refused
    assert client.post(f"/v1/runs/{rid}/ingest", json={"generation": 1, "events": []}, headers=eng).status_code == 409
    # cancel stops every active stage's job
    client.post(f"/v1/runs/{rid}/cancel", json={"reason": "test"}, headers={"X-Service-Key": KEY})
    assert launcher.states["job-1"].state == "failed" and launcher.states["job-2"].state == "failed"


def _create_live(client, token):
    data = {"spec": json.dumps({"mode": "live", "input": "fastq", "min_reads": 5}), "user_id": "u42",
            "client_token": token}
    files = {"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
             "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")}
    r = client.post("/v1/runs", data=data, files=files, headers={"X-Service-Key": KEY})
    assert r.status_code == 200, r.text
    return r.json()


def test_a_live_run_takes_uploads_while_its_engine_runs(api):
    """Live FASTQ: the engine starts with the first upload request, uploads
    stay open while it runs, the engine's feed lists what arrived and what
    it has demultiplexed, and complete fixes the manifest without stopping
    the engine. Live POD5 is refused for now."""
    client, service, launcher = api
    svc = {"X-Service-Key": KEY}
    r = client.post("/v1/runs", data={"spec": json.dumps({"mode": "live", "input": "pod5"}), "user_id": "u"},
                    files={"primers": ("p", b">p\nA\n"), "specimens": ("s", b"x")}, headers=svc)
    assert r.status_code == 400 and "live POD5" in r.text
    run = _create_live(client, "live-1")
    rid, jc = run["id"], {"Authorization": f"JobCode {run['job_code']}"}
    key_a, etag_a = _upload(client, run, "a.fastq.gz", b"gz-a")
    st = service.get_run(rid)
    assert st["state"] == "running" and launcher.specs[-1].name == f"{rid}-engine-1"
    assert launcher.specs[-1].timeout_s == 96 * 3600                 # a live engine outlasts the batch timeout
    assert client.get(f"/v1/runs/{rid}/upload", headers=jc).json()["open"] is True
    _upload(client, run, "b.fastq.gz", b"gz-b")                      # still open while running
    assert len(launcher.specs) == 1                                  # one engine
    eng = {"X-Job-Secret": launcher.specs[-1].env["SPECIMUX_JOB_SECRET"]}
    feed = client.post(f"/v1/runs/{rid}/inputs", json={"have": []}, headers=eng).json()
    assert [f["name"] for f in feed["files"]] == ["a.fastq.gz", "b.fastq.gz"] and feed["complete"] is False
    assert client.get(feed["files"][0]["url"]).content == b"gz-a"
    feed = client.post(f"/v1/runs/{rid}/inputs", json={"have": ["a.fastq.gz"]}, headers=eng).json()
    assert [f["name"] for f in feed["files"]] == ["b.fastq.gz"]
    # the engine demuxes a file: the feed says so
    client.post(f"/v1/runs/{rid}/ingest", headers=eng, json={"generation": 1, "events": [
        {"v": 1, "type": "specimux.completed", "ts": 1.0, "data": {"file_path": "/scratch/watch/a.fastq.gz"}}]})
    assert client.post(f"/v1/runs/{rid}/inputs", json={}, headers=eng).json()["ingested"] == ["a.fastq.gz"]
    # the uploader completes: the manifest is fixed, the engine keeps running
    r = client.post(f"/v1/runs/{rid}/complete", headers=jc)
    assert r.status_code == 200 and r.json()["state"] == "running" and r.json()["uploads_open"] is False
    assert client.post(f"/v1/runs/{rid}/uploads", json={"files": ["c.fastq.gz"]}, headers=jc).status_code == 409
    feed = client.post(f"/v1/runs/{rid}/inputs", json={"have": ["a.fastq.gz", "b.fastq.gz"]}, headers=eng).json()
    assert feed["complete"] is True and feed["names"] == ["a.fastq.gz", "b.fastq.gz"] and feed["files"] == []
    # only the engine's secret reads the feed
    assert client.post(f"/v1/runs/{rid}/inputs", json={}, headers={"X-Job-Secret": "nope"}).status_code == 403


def test_a_live_run_waits_for_a_slot_and_idle_uploads_complete(api):
    """A live run with no free engine slot waits in uploading and takes the
    next freed slot; a live upload idle for three hours is completed with
    what arrived (the engine finalizes; a waiting run goes to the queue)."""
    client, service, launcher = api
    busy = _create(client, token="busy-batch")
    _upload(client, busy, "x.fastq", b"@r\nA\n+\nI\n")
    client.post(f"/v1/runs/{busy['id']}/complete", headers={"Authorization": f"JobCode {busy['job_code']}"})
    assert service.store.stage_holders("engine") == [busy["id"]]     # the one slot is taken
    live = _create_live(client, "live-wait")
    _upload(client, live, "a.fastq", b"@r\nA\n+\nI\n")
    assert service.get_run(live["id"])["state"] == "uploading"       # no slot: waits, still uploading
    first = service.get_run(busy["id"])
    service.report_exit(busy["id"], first["generation"], 0, "done")
    st = service.get_run(live["id"])
    assert st["state"] == "running" and st["stages"]["engine"]["active"] and st["manifest"] is None
    # three idle hours: the upload is completed with what arrived
    t = float(st["last_upload"])
    assert service.expire_idle_uploads(now=t + 2 * 3600) == []
    assert service.expire_idle_uploads(now=t + 3 * 3600 + 1) == [live["id"]]
    st = service.get_run(live["id"])
    assert st["state"] == "running" and [m["key"].rsplit("/", 1)[-1] for m in st["manifest"]] == ["a.fastq"]
    assert st["auto_completed"]["reason"] == "no upload for 3 h"


def test_public_viewing_with_an_owners_link(api):
    """The owner shares a run: the link opens a public session for anyone
    (no host login), the dashboard's QR code carries the link, public
    viewers may star (not other commands; downloads only if allowed), and
    a new link or stopping sharing ends every public session."""
    client, service, _ = api
    svc = {"X-Service-Key": KEY}
    run = _create(client, token="public-1")
    rid = run["id"]
    assert client.get(f"/v1/runs/{rid}", headers=svc).json()["public"] == \
        {"enabled": False, "allow_starring": True, "allow_downloads": False, "url": None}
    other_host, other_key = service.add_host("other", label="x")
    assert client.post(f"/v1/runs/{rid}/public", json={"enabled": True},
                       headers={"X-Service-Key": other_key}).status_code == 404
    pub = client.post(f"/v1/runs/{rid}/public", json={"enabled": True}, headers=svc).json()
    assert pub["enabled"] and pub["url"].startswith(f"http://testserver/v1/runs/{rid}/#token=s1.{rid}.")
    assert "secret" not in json.dumps(client.get(f"/v1/runs/{rid}", headers=svc).json()["public"])
    token = pub["url"].split("#token=")[1]

    viewer = TestClient(client.app, base_url="http://testserver")       # a stranger's browser
    assert viewer.get(f"/v1/runs/{rid}/api/state").status_code == 401
    r = viewer.post("/v1/session", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200 and r.json()["scope"] == "public" and r.json()["expires_in"] == 7 * 24 * 3600
    state = viewer.get(f"/v1/runs/{rid}/api/state").json()
    assert state["share"]["url"] == pub["url"]                            # the QR code shows the public link
    # public viewers may star; not admin commands; no downloads unless allowed
    service.store.update_run(rid, {"state": "running"})
    assert viewer.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": "S1"}).status_code == 200
    assert viewer.post(f"/v1/runs/{rid}/commands", json={"command": "finalize"}).status_code == 403
    assert viewer.get(f"/v1/runs/{rid}/results.zip").status_code == 403
    client.post(f"/v1/runs/{rid}/public", json={"allow_starring": False, "allow_downloads": True}, headers=svc)
    assert viewer.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": "S1"}).status_code == 403
    assert viewer.get(f"/v1/runs/{rid}/results.zip").status_code == 409   # allowed; just not sealed yet
    # a new link ends the old sessions and the old link
    new = client.post(f"/v1/runs/{rid}/public", json={"new_link": True}, headers=svc).json()
    assert new["url"] != pub["url"]
    assert viewer.get(f"/v1/runs/{rid}/api/state").status_code == 401
    assert viewer.post("/v1/session", headers={"Authorization": f"Bearer {token}"}).status_code == 401
    assert viewer.post("/v1/session", headers={"Authorization": f"Bearer {new['url'].split('#token=')[1]}"}).status_code == 200
    assert viewer.get(f"/v1/runs/{rid}/api/state").status_code == 200
    # stopping sharing ends them too, and the QR code goes
    client.post(f"/v1/runs/{rid}/public", json={"enabled": False}, headers=svc)
    assert viewer.get(f"/v1/runs/{rid}/api/state").status_code == 401
    _session(client, rid)
    assert "share" not in client.get(f"/v1/runs/{rid}/api/state").json()
    # a host cannot mint a public-scope token for its authorize route
    assert client.post(f"/v1/runs/{rid}/tokens", json={"user": "u", "scope": "public"}, headers=svc).status_code == 400


def test_a_run_over_an_earlier_archive_is_the_same_host_and_user(api):
    """A spec may name an archive to run again over what was uploaded; an
    archive id is not a secret, so only the host and user it belongs to may
    name it. User ids name storage prefixes and must be one plain segment."""
    client, service, _ = api
    first = _create(client)
    other_host, other_key = service.add_host("elsewhere", name="Elsewhere", label="server")

    def create(key, user, archive_id, input_="fastq"):
        data = {"spec": json.dumps({"mode": "batch", "input": input_, "archive_id": archive_id}),
                "user_id": user}
        files = {"primers": ("primers.fasta", b">p\nACGT\n"), "specimens": ("Index.txt", b"SampleID\n")}
        return client.post("/v1/runs", data=data, files=files, headers={"X-Service-Key": key})

    # only once the run that uploaded it finished (and wasn't cancelled)
    assert create(KEY, "u42", first["archive_id"]).status_code == 409
    service.store.update_run(first["id"], {"state": "sealed"})
    again = create(KEY, "u42", first["archive_id"])
    assert again.status_code == 200 and again.json()["archive_id"] == first["archive_id"]
    cancelled = _create(client)
    client.post(f"/v1/runs/{cancelled['id']}/cancel", json={}, headers={"X-Service-Key": KEY})
    assert service.get_run(cancelled["id"])["cancel"]
    assert create(KEY, "u42", cancelled["archive_id"]).status_code == 409
    assert create(other_key, "u42", first["archive_id"]).status_code == 404     # another host
    assert create(KEY, "u43", first["archive_id"]).status_code == 404           # another user
    assert create(KEY, "u42", "a00000000").status_code == 404                   # no such archive
    assert create(KEY, "u42", first["archive_id"], "pod5").status_code == 400   # holds FASTQ
    for bad in ("../u42", "u42/x", ".hidden", "a" * 129):
        r = client.post("/v1/runs", data={"spec": "{}", "user_id": bad},
                        files={"primers": ("p", b">p\nA\n"), "specimens": ("i", b"x\n")},
                        headers={"X-Service-Key": KEY})
        assert r.status_code == 400, bad


def _finished_mirror(service, rid, photos=True):
    """A run's EFS mirror as a finished engine leaves it."""
    out = service.output_dir(rid)
    (out / "consensus" / "S1").mkdir(parents=True)
    (out / "consensus" / "S1" / "S1-all.fasta").write_text(">S1-c0\nACGT\n")
    if photos:
        (out / "inat_photos").mkdir()
        (out / "inat_photos" / "1_large.jpg").write_bytes(b"jpg")
    events = [
        {"type": "pipeline.started", "data": {"mode": "batch"}},
        {"type": "specimux.completed", "data": {"job_id": "d", "exit_code": 0, "specimens": {"S1": 12}}},
        {"type": "consensus.completed", "data": {"specimen_id": "S1", "job_id": "c", "clusters": [
            {"name": "S1-c0", "size": 12, "ric": 12}]}},
    ]
    (out / "events.jsonl").write_text("".join(
        json.dumps({"v": 1, "version": i + 1, "ts": 1.0, **e}) + "\n" for i, e in enumerate(events)))
    return out


def test_a_finished_run_moves_off_efs_to_its_view_zip(api):
    """Seal packs the mirror (no photos) into view.zip; clean_up removes the
    EFS directory an hour after the run ended; the dashboard is then served
    from a local copy of the view.zip, dropped again when idle."""
    from specimux_cloud.runapi.service import EFS_GRACE_S, VIEW_IDLE_S
    client, service, _ = api
    rid = _create(client)["id"]
    out = _finished_mirror(service, rid)
    sealed = service.seal(service.get_run(rid))
    service.store.update_run(rid, {"state": "sealed", "sealed": sealed})
    names = zipfile.ZipFile(io.BytesIO(service.storage.get(sealed["view"]))).namelist()
    assert "consensus/S1/S1-all.fasta" in names and "events.jsonl" in names
    assert not [n for n in names if "inat_photos" in n]
    _session(client, rid)
    assert client.get(f"/v1/runs/{rid}/api/state").json()["specimens"]["S1"]["total_reads"] == 12
    assert service.clean_up(now=sealed["at"] + 60)["efs"] == 0 and out.exists()   # within the grace
    assert service.clean_up(now=sealed["at"] + EFS_GRACE_S + 1)["efs"] == 1
    assert not service.work_dir(rid).exists()
    # the same dashboard, now from the view.zip
    assert client.get(f"/v1/runs/{rid}/api/state").json()["specimens"]["S1"]["total_reads"] == 12
    r = client.get(f"/v1/runs/{rid}/api/sequence/S1/S1-c0")
    assert r.status_code == 200 and "ACGT" in r.text
    cached = service.view(rid).output_dir
    assert cached.exists() and not service.work_dir(rid).exists()
    assert service.clean_up(now=time.time() + VIEW_IDLE_S + 1)["views"] == 1
    assert not cached.exists()
    assert client.get(f"/v1/runs/{rid}/api/state").json()["specimens"]["S1"]["total_reads"] == 12


def test_cleanup_keeps_what_is_still_needed(api):
    """A run in progress, a run whose seal failed, and a finished run inside
    its grace keep their EFS directories; a run sealed before view.zip
    gets one built before its directory goes; a directory with no run goes."""
    from specimux_cloud.runapi.service import EFS_GRACE_S
    client, service, _ = api
    now = time.time() + EFS_GRACE_S + 60
    running = _create(client)["id"]
    _finished_mirror(service, running)
    service.store.update_run(running, {"state": "running"})
    broken = _create(client)["id"]
    _finished_mirror(service, broken)
    service.store.update_run(broken, {"state": "failed", "sealed": {"error": "S3 was down"}})
    legacy = _create(client)["id"]
    _finished_mirror(service, legacy)
    service.store.update_run(legacy, {"state": "sealed", "sealed": {"output_files": 3}})
    orphan = service.work_dir("r0000abcd")
    orphan.mkdir(parents=True)
    (service.config.work_root / "not-a-run").mkdir()
    assert service.clean_up(now=now)["efs"] == 2                                # legacy + orphan
    assert service.work_dir(running).exists() and service.work_dir(broken).exists()
    assert not orphan.exists() and (service.config.work_root / "not-a-run").exists()
    sealed = service.get_run(legacy)["sealed"]
    assert sealed["output_files"] == 3 and sealed["view_files"] >= 2
    assert not service.work_dir(legacy).exists()
    names = zipfile.ZipFile(io.BytesIO(service.storage.get(sealed["view"]))).namelist()
    assert "consensus/S1/S1-all.fasta" in names


def test_cancelled_and_abandoned_uploads_lose_their_archive_after_a_week(api):
    """A cancelled or incomplete run's upload archive goes ARCHIVE_GRACE_S
    after it ended; until then a cancelled run may be retried, after it
    retry says the input is gone. A failed run's archive stays."""
    from specimux_cloud.runapi.service import ARCHIVE_GRACE_S
    client, service, launcher = api
    # (the abandoned run ends when expire_idle_uploads runs, a day from now)
    week = ARCHIVE_GRACE_S + service.config.upload_idle_s + 120

    def uploaded():
        run = _create(client)
        _upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")
        return run

    cancelled = uploaded()
    client.post(f"/v1/runs/{cancelled['id']}/cancel", json={"reason": "wrong sheet"}, headers={"X-Service-Key": KEY})
    abandoned = uploaded()
    service.expire_idle_uploads(now=time.time() + service.config.upload_idle_s + 60)
    assert service.get_run(abandoned["id"])["state"] == "incomplete"
    failed = uploaded()
    service.store.update_run(failed["id"], {"state": "failed", "exit": {"code": 1, "reported": time.time()}})
    for run in (cancelled, abandoned, failed):
        assert service.storage.list(f"archives/u42/{run['archive_id']}/")
    assert service.clean_up(now=time.time() + 60)["archives"] == 0             # within the week
    assert service.clean_up(now=time.time() + week)["archives"] == 2
    assert not service.storage.list(f"archives/u42/{cancelled['archive_id']}/")
    assert not service.storage.list(f"archives/u42/{abandoned['archive_id']}/")
    assert service.storage.list(f"archives/u42/{failed['archive_id']}/")
    assert service.store.get_archive(cancelled["archive_id"])["deleted"]
    assert service.get_run(cancelled["id"])["archive_deleted"]
    assert service.get_run(cancelled["id"])["state"] == "failed"               # the record itself stays
    assert service.clean_up(now=time.time() + week)["archives"] == 0          # once


def test_a_cancelled_run_cannot_be_retried_once_its_archive_is_gone(api):
    client, service, launcher = api
    from specimux_cloud.runapi.service import ARCHIVE_GRACE_S
    run = _create(client)
    rid = run["id"]
    manifest = [_upload(client, run, "a.fastq", b"@r\nA\n+\nI\n")]
    service.store.update_run(rid, {"state": "failed", "manifest": manifest,
                                   "exit": {"code": 143, "reported": time.time()},
                                   "cancel": {"requested": time.time(), "reason": "cancelled"}})
    service.clean_up(now=time.time() + ARCHIVE_GRACE_S + 60)
    r = client.post(f"/v1/runs/{rid}/retry", headers={"X-Service-Key": KEY})
    assert r.status_code == 409 and "deleted" in r.text


def test_a_reference_is_usable_by_hash_only_by_hosts_that_sent_it(api):
    """Stored once per content, but naming a reference by hash alone needs
    the host to have sent the file itself (a reference may be private or
    licensed); the existence check answers the same for another host's
    reference as for a missing one. Sending the file grants the host
    without storing a second copy."""
    import hashlib
    client, service, _ = api
    ref = b'>REF1 name="Private reference"\nACGTACGTAC\n'
    sha = hashlib.sha256(ref).hexdigest()
    _, other_key = service.add_host("elsewhere", name="Elsewhere", label="server")
    mine, theirs = {"X-Service-Key": KEY}, {"X-Service-Key": other_key}
    base = {"spec": json.dumps({"mode": "batch", "input": "fastq"}), "user_id": "u1"}
    inputs = {"primers": ("primers.fasta", b">p\nACGT\n"), "specimens": ("Index.txt", b"SampleID\n")}
    assert client.post("/v1/runs", data=base, files={**inputs, "reference": ("r.fasta", ref)},
                       headers=mine).status_code == 200
    assert client.get(f"/v1/references/sha256/{sha}", headers=mine).status_code == 200
    assert client.get(f"/v1/references/sha256/{sha}", headers=theirs).status_code == 404
    r = client.post("/v1/runs", data={**base, "reference_sha256": sha}, files=inputs, headers=theirs)
    assert r.status_code == 409 and "send the reference" in r.text
    # sending the file: granted, one stored copy
    assert client.post("/v1/runs", data=base, files={**inputs, "reference": ("r.fasta", ref)},
                       headers=theirs).status_code == 200
    assert client.post("/v1/runs", data={**base, "reference_sha256": sha}, files=inputs,
                       headers=theirs).status_code == 200
    assert [o.key for o in service.storage.list("references/sha256/")] == [service.reference_key(sha)]


def test_a_reference_from_before_grants_is_usable_by_the_host_whose_runs_used_it(api):
    import hashlib
    client, service, _ = api
    ref = b'>REF1 name="Old reference"\nACGT\n'
    sha = hashlib.sha256(ref).hexdigest()
    _, other_key = service.add_host("elsewhere", name="Elsewhere", label="server")
    run = _create(client)
    service.storage.put(service.reference_key(sha), ref)          # as stored before grants
    service.store.update_run(run["id"], lambda cur: {"spec": {**cur["spec"], "reference_sha256": sha}})
    assert client.get(f"/v1/references/sha256/{sha}", headers={"X-Service-Key": KEY}).status_code == 200
    assert service.storage.head(f"references/grants/{sha}/dev")
    assert client.get(f"/v1/references/sha256/{sha}", headers={"X-Service-Key": other_key}).status_code == 404


def test_every_refusal_carries_error_and_the_older_detail(api):
    client, service, _ = api
    run = _create(client)
    r = client.post(f"/v1/runs/{run['id']}/uploads", json={"files": ["a"]})       # no job code
    assert r.status_code == 403 and r.json()["error"] == r.json()["detail"] == "Job code required"
    r = client.get(f"/v1/runs/{run['id']}", headers={"X-Service-Key": "nope"})
    assert r.status_code in (401, 403) and r.json()["error"]
