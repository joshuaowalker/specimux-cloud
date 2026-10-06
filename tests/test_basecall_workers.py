"""Basecalling on several GPUs: a POD5 run's files are claimed one at a
time, largest first, by up to two worker jobs; the stage ends with its last
worker and is judged by what was delivered. The jobs are played by hand
here, as the dorado wrapper does them."""

import json

import pytest
from fastapi.testclient import TestClient

from specimux_cloud.backends.base import JobStatus
from specimux_cloud.backends.local import DirectoryStorage, MemoryQueue, SQLiteStore
from specimux_cloud.progress import basecall_text
from specimux_cloud.runapi.app import create_app, install_dev_host
from specimux_cloud.runapi.service import RunService, ServiceConfig

from test_runapi import KEY, POD5_FILES, SECRET, FakeLauncher

SVC = {"X-Service-Key": KEY}


@pytest.fixture
def api(tmp_path):
    base = "http://testserver"
    # two GPUs; a second worker for any spare work (the real floor is 1 GB)
    config = ServiceConfig(data_dir=tmp_path / "data", base_url=base, session_secret=SECRET,
                           stage_slots={"engine": 1, "dorado": 2, "fetch": 1}, extra_worker_min_bytes=1)
    launcher = FakeLauncher()
    service = RunService(config, storage=DirectoryStorage(tmp_path / "data" / "storage", base_url=base, secret=SECRET),
                         queue=MemoryQueue(), launcher=launcher, store=SQLiteStore(tmp_path / "cp.sqlite"))
    install_dev_host(service, KEY)
    return TestClient(create_app(service), base_url=base), service, launcher


def _pod5_run(client, sizes: dict, complete=True, token=None):
    """A POD5 run with files of these sizes (name -> bytes), uploaded."""
    data = {"spec": json.dumps({"mode": "batch", "input": "pod5"}), "user_id": "u42"}
    if token:
        data["client_token"] = token
    run = client.post("/v1/runs", data=data, files=POD5_FILES, headers=SVC).json()
    hdr = {"Authorization": f"JobCode {run['job_code']}"}
    ups = client.post(f"/v1/runs/{run['id']}/uploads", json={"files": list(sizes)}, headers=hdr).json()["uploads"]
    for name, n in sizes.items():
        client.put(ups[name]["url"], content=b"x" * n)
    if complete:
        client.post(f"/v1/runs/{run['id']}/complete", headers=hdr)
    return run["id"]


def _worker(launcher, name):
    spec = next(s for s in launcher.specs if s.name == name)
    return {"X-Job-Secret": spec.env["SPECIMUX_JOB_SECRET"]}, int(spec.env["SPECIMUX_DORADO_WORKER"]), spec.generation


def _claim(client, rid, name, launcher):
    hdr, w, gen = _worker(launcher, name)
    r = client.post(f"/v1/runs/{rid}/basecall-claim", json={"generation": gen, "worker": w}, headers=hdr)
    assert r.status_code == 200, r.text
    return r.json()


def _deliver(client, rid, name, launcher, claim, reads=10):
    hdr, w, gen = _worker(launcher, name)
    assert client.put(claim["fastq"]["url"], content=f"@{claim['fastq']['name']}\nACGT\n+\nIIII\n".encode()).status_code == 200
    r = client.post(f"/v1/runs/{rid}/basecalled", json={"generation": gen, "name": claim["fastq"]["name"],
                                                       "key": claim["fastq"]["key"], "reads_in": reads,
                                                       "reads_out": reads}, headers=hdr)
    assert r.status_code == 200, r.text


def _exit(client, rid, name, launcher, code=0, tail=""):
    hdr, w, gen = _worker(launcher, name)
    return client.post(f"/v1/runs/{rid}/exit", json={"generation": gen, "exit_code": code, "worker": w,
                                                    "log_tail": tail}, headers=hdr).json()


def test_two_workers_claim_the_largest_files_first(api):
    client, service, launcher = api
    rid = _pod5_run(client, {"a.pod5": 300, "b.pod5": 200, "c.pod5": 100})
    w0, w1 = f"{rid}-dorado-1-w0", f"{rid}-dorado-1-w1"
    assert [s.name for s in launcher.specs] == [w0, w1]
    assert sorted(service.store.stage_holders("dorado")) == [f"{rid}/0", f"{rid}/1"]
    run = service.get_run(rid)
    assert set(run["stages"]["dorado"]["workers"]) == {"0", "1"} and run["state"] == "basecalling"

    c0 = _claim(client, rid, w0, launcher)
    assert c0["pod5"]["name"] == "a.pod5" and c0["fastq"]["key"] == f"runs/u42/{rid}/fastq/a.fastq"
    assert client.get(c0["pod5"]["url"]).content == b"x" * 300
    assert _claim(client, rid, w0, launcher)["pod5"]["name"] == "a.pod5"     # held: the same again
    c1 = _claim(client, rid, w1, launcher)
    assert c1["pod5"]["name"] == "b.pod5"
    # both in flight on the run page
    for name, claim in ((w0, c0), (w1, c1)):
        hdr, w, gen = _worker(launcher, name)
        client.post(f"/v1/runs/{rid}/basecall-progress", json={"generation": 1, "worker": w,
                    "file": claim["pod5"]["name"], "reads": 5, "estimate": 10}, headers=hdr)
    assert basecall_text(service.get_run(rid)).endswith("(0 of 3 file(s) done, 2 in progress on 2 GPUs)")

    _deliver(client, rid, w0, launcher, c0)
    assert _claim(client, rid, w0, launcher)["pod5"]["name"] == "c.pod5"
    _deliver(client, rid, w1, launcher, c1)
    assert _claim(client, rid, w1, launcher) == {"done": True}
    st = _exit(client, rid, w1, launcher)
    # one worker is over: its GPU is free, the stage goes on
    assert st["state"] == "basecalling" and service.store.stage_holders("dorado") == [f"{rid}/0"]
    _deliver(client, rid, w0, launcher, _claim(client, rid, w0, launcher))
    assert _claim(client, rid, w0, launcher) == {"done": True}
    st = _exit(client, rid, w0, launcher)
    # the last: the stage is judged, the engine runs over the three FASTQs
    assert st["state"] == "running" and launcher.specs[-1].kind == "engine"
    assert service.store.stage_holders("dorado") == []
    assert [b["key"].rsplit("/", 1)[-1] for b in service.get_run(rid)["basecalled"]] == ["a.fastq", "b.fastq", "c.fastq"]
    assert service.get_run(rid)["basecalling"]["reads_in"] == 30


def test_one_file_one_worker_and_small_work_no_second(api):
    client, service, launcher = api
    rid = _pod5_run(client, {"only.pod5": 500})
    assert [s.name for s in launcher.specs] == [f"{rid}-dorado-1-w0"]
    service.config.extra_worker_min_bytes = 1000
    other = _pod5_run(client, {"a.pod5": 400, "b.pod5": 400}, token="small")   # 400 spare < 1000
    assert [s.name for s in launcher.specs][-1] == f"{other}-dorado-1-w0"
    assert len(service.store.stage_holders("dorado")) == 2


def test_a_queued_run_gets_a_freed_gpu_before_a_second_worker(api):
    client, service, launcher = api
    a = _pod5_run(client, {"a1.pod5": 300, "a2.pod5": 300, "a3.pod5": 300})       # both GPUs
    b = _pod5_run(client, {"b1.pod5": 300, "b2.pod5": 300}, token="b")
    assert service.get_run(b)["state"] == "input_complete"                         # queued
    aw0, aw1 = f"{a}-dorado-1-w0", f"{a}-dorado-1-w1"
    c = _claim(client, a, aw1, launcher)
    _deliver(client, a, aw1, launcher, c)
    # a's second worker gives its GPU up while a still has unclaimed work:
    # the queued run b takes it, not another worker for a
    _exit(client, a, aw1, launcher, code=0)
    assert service.get_run(b)["state"] == "basecalling"
    assert sorted(service.store.stage_holders("dorado")) == sorted([f"{a}/0", f"{b}/0"])

    # b finishes; its GPU goes to a, which still has two files nobody claimed
    bw0 = f"{b}-dorado-1-w0"
    for _ in range(2):
        _deliver(client, b, bw0, launcher, _claim(client, b, bw0, launcher))
    _exit(client, b, bw0, launcher)
    assert launcher.specs[-1].name == f"{a}-dorado-1-w2"
    assert sorted(service.store.stage_holders("dorado")) == [f"{a}/0", f"{a}/2"]


def test_a_failed_worker_leaves_its_file_to_the_other(api):
    client, service, launcher = api
    rid = _pod5_run(client, {"a.pod5": 300, "b.pod5": 200})
    w0, w1 = f"{rid}-dorado-1-w0", f"{rid}-dorado-1-w1"
    c0 = _claim(client, rid, w0, launcher)
    c1 = _claim(client, rid, w1, launcher)
    st = _exit(client, rid, w1, launcher, code=1, tail="dorado: CUDA error")       # its claim is free again
    assert st["state"] == "basecalling" and "b.fastq" not in (service.get_run(rid).get("basecall_claims") or {})
    _deliver(client, rid, w0, launcher, c0)
    c = _claim(client, rid, w0, launcher)
    assert c["pod5"]["name"] == c1["pod5"]["name"] == "b.pod5"
    _deliver(client, rid, w0, launcher, c)
    assert _claim(client, rid, w0, launcher) == {"done": True}
    st = _exit(client, rid, w0, launcher)
    assert st["state"] == "running" and not st.get("exit")                   # every file delivered: a success


def test_files_left_when_the_last_worker_ends_fail_the_run(api):
    client, service, launcher = api
    rid = _pod5_run(client, {"a.pod5": 300, "b.pod5": 200})
    w0, w1 = f"{rid}-dorado-1-w0", f"{rid}-dorado-1-w1"
    _deliver(client, rid, w0, launcher, _claim(client, rid, w0, launcher))
    _claim(client, rid, w1, launcher)
    _exit(client, rid, w0, launcher, code=0)
    st = _exit(client, rid, w1, launcher, code=1, tail="dorado: out of memory")
    assert st["state"] == "failed" and st["exit"]["stage"] == "dorado" and st["exit"]["code"] == 1
    assert "out of memory" in st["exit"]["log_tail"]
    # retry: one worker for the one file left (no spare work for a second)
    st = client.post(f"/v1/runs/{rid}/retry", headers=SVC).json()
    assert st["state"] == "basecalling" and launcher.specs[-1].name == f"{rid}-dorado-2-w0"
    c = _claim(client, rid, f"{rid}-dorado-2-w0", launcher)
    assert c["pod5"]["name"] == "b.pod5"


def test_a_worker_still_waiting_for_a_gpu_is_stopped_when_all_is_delivered(api):
    client, service, launcher = api
    rid = _pod5_run(client, {"a.pod5": 300, "b.pod5": 200})
    w0, w1 = f"{rid}-dorado-1-w0", f"{rid}-dorado-1-w1"
    job1 = service.get_run(rid)["jobs"][w1]["id"]
    launcher.states[job1] = JobStatus("pending")                 # no G capacity for it
    for _ in range(2):
        _deliver(client, rid, w0, launcher, _claim(client, rid, w0, launcher))
    st = _exit(client, rid, w0, launcher)
    assert launcher.states[job1].state == "failed"               # cancelled
    assert st["state"] == "running" and service.store.stage_holders("dorado") == []
    w = service.get_run(rid)["stages"]["dorado"]["workers"]
    assert not w["1"]["active"] and "not needed" in w["1"]["exit"]["reason"]


def test_reconcile_and_cancel_handle_each_worker(api):
    client, service, launcher = api
    rid = _pod5_run(client, {"a.pod5": 300, "b.pod5": 200, "c.pod5": 100})
    w0, w1 = f"{rid}-dorado-1-w0", f"{rid}-dorado-1-w1"
    _claim(client, rid, w0, launcher)
    _claim(client, rid, w1, launcher)
    # worker 1's host is lost without an exit report
    job1 = service.get_run(rid)["jobs"][w1]["id"]
    launcher.states[job1] = JobStatus("failed", exit_code=137, reason="Host EC2 (instance i-1) terminated")
    service.reconcile(intent_grace_s=0)
    run = service.get_run(rid)
    assert run["state"] == "basecalling" and not run["stages"]["dorado"]["workers"]["1"]["active"]
    assert set(run["basecall_claims"]) == {"a.fastq"}                     # its claim is free again
    # the freed GPU: no queued run, so another worker joins with the unclaimed work
    assert launcher.specs[-1].name == f"{rid}-dorado-1-w2"
    # cancel stops every worker still running
    r = client.post(f"/v1/runs/{rid}/cancel", json={}, headers=SVC).json()
    jobs = service.get_run(rid)["jobs"]
    assert launcher.states[jobs[w0]["id"]].state == "failed" and launcher.states[jobs[f"{rid}-dorado-1-w2"]["id"]].state == "failed"
    _exit(client, rid, w0, launcher, code=143)
    st = _exit(client, rid, f"{rid}-dorado-1-w2", launcher, code=143)
    assert st["state"] == "failed" and st["exit"]["reason"] == "cancelled by dev" and service.store.stage_holders("dorado") == []


def test_the_load_view_counts_gpus(api):
    client, service, launcher = api
    _pod5_run(client, {"a.pod5": 300, "b.pod5": 200})
    load = client.get("/v1/load", headers=SVC).json()
    assert load["stages"]["dorado"]["busy"] == 2 and load["stages"]["dorado"]["slots"] == 2
