"""The whole local stack end to end: run API under uvicorn, the engine
launched as a subprocess by SubprocessLauncher, the wrapper running a
real ``specimux-suite batch`` (with stand-in bioinformatics tools) with
the cloud plugin forwarding events and applying a command posted from the
dashboard side, then the exit report, the seal and the results package.

This is milestone 2's local half in one test; the POD5 test adds the
dorado stage (a stand-in dorado) in front of it.
"""

import io
import json
import os
import socket
import time
import zipfile
from pathlib import Path

import httpx
import pytest

from specimux_cloud.runapi.app import build_local_service, create_app

FAKE_TOOLS = Path(__file__).resolve().parent / "fake_tools"
KEY = "dev.e2e-key"


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait(pred, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture
def stack(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", f"{FAKE_TOOLS}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_SPECONSENSE_SLEEP", "2.5")
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    service = build_local_service(tmp_path / "data", base, dev_key=KEY)
    from specimux_suite.web.viewer import serve_in_thread
    serve_in_thread(create_app(service), "127.0.0.1", port)
    _wait(lambda: httpx.get(base + "/v1/version").status_code == 200 if _up(base) else False, 10, "run API")
    yield base, service
    service.launcher.wait_all(timeout=30)


def _up(base):
    try:
        httpx.get(base + "/v1/version", timeout=1)
        return True
    except httpx.HTTPError:
        return False


def test_batch_fastq_end_to_end(stack, tmp_path):
    base, service = stack
    svc = {"X-Service-Key": KEY}
    reads = "".join(f"@r{i}\nACGTACGTAC\n+\nIIIIIIIIII\n" for i in range(40))

    # mycomap.org creates the run
    r = httpx.post(f"{base}/v1/runs", headers=svc,
                   data={"spec": json.dumps({"mode": "batch", "input": "fastq", "min_reads": 5, "workers": 1}),
                         "user_id": "u1", "client_token": "ct-e2e"},
                   files={"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
                          "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\nS2\tITS\n"),
                          "reference": ("refs.fasta", b'>REF1 name="Amanita muscaria"\nACGTACGTAC\n')})
    assert r.status_code == 200, r.text
    run = r.json()
    rid, code = run["id"], run["job_code"]
    jc = {"Authorization": f"JobCode {code}"}

    # the uploader
    r = httpx.post(f"{base}/v1/runs/{rid}/uploads", json={"files": ["chunk1.fastq"]}, headers=jc)
    up = r.json()["uploads"]["chunk1.fastq"]
    put = httpx.put(up["url"], content=reads.encode())
    assert put.status_code == 200
    etag = put.headers["etag"].strip('"')
    r = httpx.post(f"{base}/v1/runs/{rid}/complete", json={"manifest": [{"key": up["key"], "etag": etag}]}, headers=jc)
    assert r.status_code == 200 and r.json()["state"] == "running", r.text

    # the browser side: a session from a token the host minted (admin scope)
    browser = httpx.Client(base_url=base)
    tok = httpx.post(f"{base}/v1/runs/{rid}/tokens", json={"user": "u1", "scope": "admin"}, headers=svc).json()
    assert browser.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"}).status_code == 200

    # the engine comes up: events flow into the dashboard
    def specimens():
        s = browser.get(f"/v1/runs/{rid}/api/state")
        return s.json().get("specimens") if s.status_code == 200 else None
    snap = _wait(lambda: (lambda sp: sp if sp and sp.get("S1", {}).get("total_reads") else None)(specimens()),
                 60, "demuxed reads in the dashboard")
    assert snap["S1"]["total_reads"] == 40

    # a command from the browser side, applied by the engine mid-run
    assert httpx.post(f"{base}/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": "S2"}).status_code == 401
    r = browser.post(f"/v1/runs/{rid}/commands", json={"command": "watch", "specimen_id": "S2"})
    assert r.status_code == 200, r.text
    cid = r.json()["command_id"]
    _wait(lambda: service.store.get_command(rid, cid)["outcome"] == "applied", 30, "command applied")
    assert browser.get(f"/v1/runs/{rid}/api/state").json()["specimens"]["S2"]["watched"] is True

    # the engine finishes, the wrapper reports, the run is sealed
    status = _wait(lambda: (lambda s: s if s["state"] in ("sealed", "failed") and s.get("sealed") else None)(
        httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 120, "run to seal")
    assert status["state"] == "sealed", status
    assert status["exit"]["code"] == 0
    # the job built the downloads from its local output; the seal only recorded them
    assert set(status["exit"]["packages"]) == {"results.zip", "output.zip", "reads.zip"}
    assert "rebuilt" not in status["sealed"] and status["sealed"]["reads_bytes"] > 0
    # the shared run dir holds the mirror only: no demux output, no debug reads, no scratch
    mirror = service.output_dir(rid)
    served = {p.relative_to(mirror).as_posix() for p in mirror.rglob("*") if p.is_file()}
    assert "events.jsonl" in served and "consensus/S1/S1-all.fasta" in served
    assert not [f for f in served if f.startswith(("specimux/", "snapshots/", ".staging/")) or "cluster_debug" in f]
    assert status["ingested_files"] == ["reads.fastq"]
    assert status["effective_config"]["min_reads"] == 5
    assert status["pending_commands"] == []
    assert service.store.stage_holders("engine") == []

    # the results package and the sealed log
    r = httpx.get(f"{base}/v1/runs/{rid}/results.zip", headers=svc, follow_redirects=True)
    assert r.status_code == 200
    names = zipfile.ZipFile(io.BytesIO(r.content)).namelist()
    assert "summary.fasta" in names and not any(n.startswith("summary/") for n in names)   # summary/, flat
    assert "events.jsonl" not in names
    assert "Summary.zip" in r.headers.get("content-disposition", "")
    r = httpx.get(f"{base}/v1/runs/{rid}/reads.zip", headers=svc, follow_redirects=True)
    assert any(n.startswith("specimux/") for n in zipfile.ZipFile(io.BytesIO(r.content)).namelist())
    r = httpx.get(f"{base}/v1/runs/{rid}/events.jsonl", headers=svc, follow_redirects=True)
    log = [json.loads(l) for l in r.text.splitlines() if l.strip()]
    types = [e["type"] for e in log]
    assert "pipeline.started" in types and "summarize.aggregate_completed" in types
    outcome = next(e for e in log if e["type"] == "command.outcome")
    assert outcome["data"]["command_id"] == cid and outcome["data"]["actor"] == "dev:u1"
    watched = next(e for e in log if e["type"] == "specimen.watched")
    assert watched["data"]["command_id"] == cid
    # the dashboard still serves the sealed run from its work dir
    snap = browser.get(f"/v1/runs/{rid}/api/state").json()
    assert snap["version"] == len(log) and snap["specimens"]["S1"]["status"] == "summarized"
    assert snap["specimens"]["S1"]["identification"][0]["top_hits"][0]["name"] == "Amanita muscaria"
    seq = browser.get(f"/v1/runs/{rid}/api/sequence/S1/S1-1.v1").json()
    # results from the dashboard side too, with the cookie
    assert browser.get(f"/v1/runs/{rid}/results.zip", follow_redirects=True).status_code == 200
    assert seq == {"sequence": "ACGTACGTAC"}
    # the lease was released and the wrapper's log exists
    lease = json.loads((service.work_dir(rid) / "lease.json").read_text())
    assert lease["generation"] == 1
    assert (tmp_path / "data" / "logs" / f"{rid}-engine-1.log").exists()


def test_batch_pod5_end_to_end(stack, tmp_path, monkeypatch):
    """A POD5 run: the dorado job (the wrapper around a stand-in dorado,
    CPU device) basecalls each file, filters by length, delivers the
    FASTQ, and the engine runs over those FASTQs."""
    monkeypatch.setenv("SPECIMUX_DORADO_DEVICE", "cpu")
    base, service = stack
    svc = {"X-Service-Key": KEY}
    good = "".join(f"@r{i}\n{'ACGTACGTAC' * 50}\n+\n{'I' * 500}\n" for i in range(30))
    short = "".join(f"@s{i}\nACGT\n+\nIIII\n" for i in range(5))
    r = httpx.post(f"{base}/v1/runs", headers=svc,
                   data={"spec": json.dumps({"mode": "batch", "input": "pod5", "min_reads": 5, "workers": 1,
                                             "basecall": {"model": "sup@v5.0.0", "min_length": 400, "max_length": 2000}}),
                         "user_id": "u1", "client_token": "ct-pod5"},
                   files={"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
                          "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")})
    assert r.status_code == 200, r.text
    run = r.json()
    rid, code = run["id"], run["job_code"]
    jc = {"Authorization": f"JobCode {code}"}
    r = httpx.post(f"{base}/v1/runs/{rid}/uploads", json={"files": ["one.pod5", "two.pod5"]}, headers=jc)
    manifest = []
    for name, up in r.json()["uploads"].items():
        put = httpx.put(up["url"], content=(good + short).encode())
        manifest.append({"key": up["key"], "etag": put.headers["etag"].strip('"')})
    r = httpx.post(f"{base}/v1/runs/{rid}/complete", json={"manifest": manifest}, headers=jc)
    assert r.status_code == 200 and r.json()["state"] == "basecalling", r.text

    # the dorado job delivers both files, then the engine takes over
    st = _wait(lambda: (lambda s: s if s["state"] in ("running", "finalizing", "sealing", "sealed", "failed") else None)(
        httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 90, "basecalling to finish")
    assert st["state"] != "failed", st.get("exit")
    assert st["basecalling"]["done"] == 2 and st["basecalling"]["reads_in"] == 70 and st["basecalling"]["reads_out"] == 60
    assert sorted(b["key"].rsplit("/", 1)[-1] for b in st["basecalled"]) == ["one.fastq", "two.fastq"]
    assert st["generation"] == 2
    status = _wait(lambda: (lambda s: s if s["state"] in ("sealed", "failed") and s.get("sealed") else None)(
        httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 120, "run to seal")
    assert status["state"] == "sealed", status
    assert status["ingested_files"] == ["reads.fastq"]
    # the dashboard saw the demuxed reads: 60 kept reads across the two files
    browser = httpx.Client(base_url=base)
    tok = httpx.post(f"{base}/v1/runs/{rid}/tokens", json={"user": "u1", "scope": "view"}, headers=svc).json()
    browser.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"})
    snap = browser.get(f"/v1/runs/{rid}/api/state").json()
    assert snap["specimens"]["S1"]["total_reads"] == 60
    # both jobs left their logs
    assert (tmp_path / "data" / "logs" / f"{rid}-dorado-1.log").exists()
    assert (tmp_path / "data" / "logs" / f"{rid}-engine-2.log").exists()
    assert "--emit-fastq --no-trim --device cpu" in (tmp_path / "data" / "logs" / f"{rid}-dorado-1.log").read_text()


def test_pod5_run_fails_when_dorado_does(stack, tmp_path, monkeypatch):
    monkeypatch.setenv("SPECIMUX_DORADO_DEVICE", "cpu")
    monkeypatch.setenv("FAKE_DORADO_FAIL", "1")
    base, service = stack
    svc = {"X-Service-Key": KEY}
    r = httpx.post(f"{base}/v1/runs", headers=svc,
                   data={"spec": json.dumps({"mode": "batch", "input": "pod5", "min_reads": 5}), "user_id": "u1"},
                   files={"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
                          "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")})
    run = r.json()
    rid, jc = run["id"], {"Authorization": f"JobCode {run['job_code']}"}
    r = httpx.post(f"{base}/v1/runs/{rid}/uploads", json={"files": ["one.pod5"]}, headers=jc)
    httpx.put(r.json()["uploads"]["one.pod5"]["url"], content=b"@r\nACGT\n+\nIIII\n")
    httpx.post(f"{base}/v1/runs/{rid}/complete", headers=jc)
    st = _wait(lambda: (lambda s: s if s["state"] in ("failed", "running", "sealed") else None)(
        httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 60, "the dorado job to report")
    assert st["state"] == "failed" and st["exit"]["stage"] == "dorado" and st["exit"]["code"] == 1
    assert "no CUDA device" in st["exit"]["log_tail"]
    assert service.store.stage_holders("dorado") == [] and service.store.stage_holders("engine") == []


def test_a_stopped_dorado_job_reports_its_exit(stack, tmp_path, monkeypatch):
    """Batch stops a job with SIGTERM (terminate-job, Spot, a timeout). The
    wrapper stops dorado and reports exit 143 at once, so the run fails
    without waiting for a reconcile pass."""
    monkeypatch.setenv("SPECIMUX_DORADO_DEVICE", "cpu")
    monkeypatch.setenv("FAKE_DORADO_SLEEP", "60")
    base, service = stack
    svc = {"X-Service-Key": KEY}
    r = httpx.post(f"{base}/v1/runs", headers=svc,
                   data={"spec": json.dumps({"mode": "batch", "input": "pod5", "min_reads": 5}), "user_id": "u1"},
                   files={"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
                          "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")})
    run = r.json()
    rid, jc = run["id"], {"Authorization": f"JobCode {run['job_code']}"}
    r = httpx.post(f"{base}/v1/runs/{rid}/uploads", json={"files": ["one.pod5"]}, headers=jc)
    httpx.put(r.json()["uploads"]["one.pod5"]["url"], content=b"@r\nACGT\n+\nIIII\n")
    httpx.post(f"{base}/v1/runs/{rid}/complete", headers=jc)
    _wait(lambda: service.get_run(rid)["state"] == "basecalling" and service.get_run(rid).get("jobs"),
          30, "the dorado job to launch")
    time.sleep(3)  # let the wrapper get dorado going
    job_id = service.get_run(rid)["jobs"][f"{rid}-dorado-1"]["id"]
    started = time.monotonic()
    service.launcher.cancel(job_id, "test stop")
    st = _wait(lambda: (lambda s: s if s["state"] == "failed" else None)(
        httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 20, "the stopped job's exit report")
    assert time.monotonic() - started < 20
    assert st["exit"]["code"] == 143 and "stopped (SIGTERM)" in st["exit"]["log_tail"]
    assert service.store.stage_holders("dorado") == []



def test_submit_sends_a_reference_only_once(stack, tmp_path, caplog):
    """The CLI asks whether the service holds the reference before sending
    it; the second run names it by hash and still gets it."""
    import logging
    from specimux_cloud.uploader.submit import run as submit
    base, service = stack
    (tmp_path / "primers.fasta").write_bytes(b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n")
    (tmp_path / "Index.txt").write_bytes(b"SampleID\tPrimerPool\nS1\tITS\n")
    (tmp_path / "refs.fasta").write_bytes(b'>REF1 name="Amanita muscaria"\nACGTACGTAC\n')
    (tmp_path / "reads.fastq").write_text("".join(f"@r{i}\nACGTACGTAC\n+\nIIIIIIIIII\n" for i in range(10)))
    argv = ["--run-api", base, "--service-key", KEY, "--primers", str(tmp_path / "primers.fasta"),
            "--specimens", str(tmp_path / "Index.txt"), "--reference", str(tmp_path / "refs.fasta"),
            str(tmp_path / "reads.fastq")]
    with caplog.at_level(logging.INFO):
        assert submit(argv) == 0
        assert "already has" not in caplog.text
        assert submit(argv + ["--name", "Run150"]) == 0
        assert "already has refs.fasta; not sending it" in caplog.text
    runs = sorted(service.store.list_runs(host="dev"), key=lambda r: r["created"])
    assert [r["spec"].get("name") for r in runs[-2:]] == [None, "Run150"]          # submit --name
    shas = {r["spec"].get("reference_sha256") for r in runs[-2:]}
    assert len(shas) == 1 and None not in shas
    assert all("reference" in service.job_bundle(r["id"])["inputs"] for r in runs[-2:])


def test_two_pod5_runs_at_once_share_nothing(stack, tmp_path, monkeypatch):
    """Two slots per stage: two POD5 runs basecall at the same time on one
    shared scratch root, each holding a file of the same name with
    different reads, then both engines run; each run gets its own reads."""
    monkeypatch.setenv("SPECIMUX_DORADO_DEVICE", "cpu")
    monkeypatch.setenv("SPECIMUX_SCRATCH", str(tmp_path / "scratch"))
    monkeypatch.setenv("FAKE_DORADO_SLEEP", "2")
    base, service = stack
    svc = {"X-Service-Key": KEY}
    rids = {}
    for n in (30, 20):
        reads = "".join(f"@r{i}\n{'ACGTACGTAC' * 50}\n+\n{'I' * 500}\n" for i in range(n))
        r = httpx.post(f"{base}/v1/runs", headers=svc,
                       data={"spec": json.dumps({"mode": "batch", "input": "pod5", "min_reads": 5, "workers": 1}),
                             "user_id": "u1", "client_token": f"ct-two-{n}"},
                       files={"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
                              "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\n")})
        run = r.json()
        jc = {"Authorization": f"JobCode {run['job_code']}"}
        up = httpx.post(f"{base}/v1/runs/{run['id']}/uploads", json={"files": ["same.pod5"]},
                        headers=jc).json()["uploads"]["same.pod5"]
        put = httpx.put(up["url"], content=reads.encode())
        r = httpx.post(f"{base}/v1/runs/{run['id']}/complete", headers=jc,
                       json={"manifest": [{"key": up["key"], "etag": put.headers["etag"].strip('"')}]})
        assert r.json()["state"] == "basecalling", r.text
        rids[run["id"]] = n
    assert sorted(service.store.stage_holders("dorado")) == sorted(rids)      # both at once
    for rid, n in rids.items():
        st = _wait(lambda: (lambda s: s if s["state"] in ("sealed", "failed") and s.get("sealed") else None)(
            httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 180, f"run {rid} to seal")
        assert st["state"] == "sealed", st.get("exit")
        assert st["basecalling"]["reads_in"] == n and st["basecalling"]["reads_out"] == n
    # each job cleaned up only its own scratch
    assert list((tmp_path / "scratch").glob("specimux-dorado-*")) == []


def test_live_fastq_end_to_end(stack, tmp_path):
    """A live run: the engine starts with the first upload and demuxes files
    as they arrive (gzipped, as MinKNOW writes them) while uploads are still
    open, so the dashboard fills during sequencing; completing the upload
    lets the engine take the last file, finalize and exit, and the run is
    sealed with every file's reads."""
    import gzip as gz
    base, service = stack
    svc = {"X-Service-Key": KEY}
    r = httpx.post(f"{base}/v1/runs", headers=svc,
                   data={"spec": json.dumps({"mode": "live", "input": "fastq", "min_reads": 5, "workers": 1}),
                         "user_id": "u1", "client_token": "ct-live"},
                   files={"primers": ("primers.fasta", b">ITS1F\nCTTGGTCATTTAGAGGAAGTAA\n"),
                          "specimens": ("Index.txt", b"SampleID\tPrimerPool\nS1\tITS\nS2\tITS\n")})
    run = r.json()
    rid, jc = run["id"], {"Authorization": f"JobCode {run['job_code']}"}

    def upload(name, n):
        data = gz.compress("".join(f"@{name}{i}\nACGTACGTAC\n+\nIIIIIIIIII\n" for i in range(n)).encode())
        up = httpx.post(f"{base}/v1/runs/{rid}/uploads", json={"files": [name]}, headers=jc).json()["uploads"][name]
        assert httpx.put(up["url"], content=data).status_code == 200

    upload("one.fastq.gz", 20)
    assert httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()["state"] == "running"
    browser = httpx.Client(base_url=base)
    tok = httpx.post(f"{base}/v1/runs/{rid}/tokens", json={"user": "u1", "scope": "view"}, headers=svc).json()
    browser.post("/v1/session", headers={"Authorization": f"Bearer {tok['token']}"})

    def s1_reads():
        s = browser.get(f"/v1/runs/{rid}/api/state")
        return (s.json().get("specimens") or {}).get("S1", {}).get("total_reads", 0) if s.status_code == 200 else 0
    # the first file is demultiplexed while the upload is still open
    _wait(lambda: s1_reads() == 20, 90, "the first file in the dashboard")
    assert httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()["uploads_open"] is True
    upload("two.fastq.gz", 10)
    _wait(lambda: s1_reads() == 30, 90, "the second file in the dashboard")
    # the uploader completes; the engine finalizes and the run is sealed
    assert httpx.post(f"{base}/v1/runs/{rid}/complete", headers=jc).json()["state"] == "running"
    status = _wait(lambda: (lambda s: s if s["state"] in ("sealed", "failed") and s.get("sealed") else None)(
        httpx.get(f"{base}/v1/runs/{rid}", headers=svc).json()), 180, "the live run to seal")
    assert status["state"] == "sealed", status.get("exit")
    assert sorted(status["ingested_files"]) == ["one.fastq.gz", "two.fastq.gz"]
    assert status["exit"]["code"] == 0 and set(status["exit"]["packages"]) == {"results.zip", "output.zip", "reads.zip"}
    log = (tmp_path / "data" / "logs" / f"{rid}-engine-1.log").read_text()
    assert "finalizing the engine" in log
