"""The AWS backends under moto, with the same expectations as the local
ones (tests/test_backends_local.py). Batch's launcher is tested against a
stub client, since moto's Batch tries to run jobs in Docker."""

import os
import time

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from specimux_cloud.backends.aws import BatchLauncher, DynamoStore, S3Storage, SqsQueue
from specimux_cloud.backends.base import ConflictError, JobSpec

REGION = "us-west-2"


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with moto.mock_aws():
        yield


def test_s3_storage(tmp_path):
    boto3.client("s3", region_name=REGION).create_bucket(
        Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": REGION})
    s = S3Storage("test-bucket", region=REGION)
    info = s.put("runs/u/r/fastq/a.fastq", b"@r\nA\n+\nI\n")
    assert info.size == 9 and len(info.etag) == 32
    assert s.get(info.key) == b"@r\nA\n+\nI\n"
    assert s.head(info.key) == info and s.head("nope") is None
    src = tmp_path / "b.fastq"
    src.write_bytes(b"x" * 10)
    s.put_file("runs/u/r/fastq/b.fastq", src)
    assert [o.key for o in s.list("runs/u/r/fastq/")] == ["runs/u/r/fastq/a.fastq", "runs/u/r/fastq/b.fastq"]
    dest = tmp_path / "dl" / "a.fastq"
    s.download(info.key, dest)
    assert dest.read_bytes() == b"@r\nA\n+\nI\n"
    url = s.presign_put("runs/u/r/fastq/c.fastq", expires_s=60)
    assert "Signature=" in url and "c.fastq" in url and "Expires" in url   # moto signs v2; real S3 v4
    assert "Signature=" in s.presign_get(info.key)
    assert s.delete_prefix("runs/u/r") == 2
    assert s.list("runs") == []


def test_sqs_queue_redelivers_until_acked():
    q = SqsQueue(prefix="t", region=REGION, visibility_s=1)
    q.send("r1", {"command": "watch"})
    q.send("r1", {"command": "finalize"})
    assert q.receive("r2") == []                 # no queue for r2: nothing, none created
    got = q.receive("r1", max_messages=10)
    assert sorted(m.body["command"] for m in got) == ["finalize", "watch"]
    assert q.receive("r1") == []                 # in flight
    time.sleep(1.2)
    back = q.receive("r1", max_messages=10)      # not acked: redelivered
    assert sorted(m.body["command"] for m in back) == ["finalize", "watch"]
    for m in back:
        q.ack("r1", m.id)
    time.sleep(1.2)
    assert q.receive("r1") == []
    q.purge("r1")
    assert q.receive("r1") == []


class StubBatch:
    def __init__(self):
        self.jobs = {}
        self.submitted = []

    def submit_job(self, **kw):
        jid = f"id-{len(self.jobs) + 1}"
        self.jobs[jid] = {"jobId": jid, "jobName": kw["jobName"], "status": "RUNNABLE", "createdAt": len(self.jobs),
                          "queue": kw["jobQueue"], "container": {}}
        self.submitted.append(kw)
        return {"jobId": jid, "jobName": kw["jobName"]}

    def describe_jobs(self, jobs):
        return {"jobs": [self.jobs[j] for j in jobs if j in self.jobs]}

    def list_jobs(self, jobQueue, filters):
        name = filters[0]["values"][0]
        return {"jobSummaryList": [j for j in self.jobs.values() if j["jobName"] == name and j["queue"] == jobQueue]}

    def terminate_job(self, jobId, reason):
        self.jobs[jobId]["status"] = "FAILED"
        self.jobs[jobId]["statusReason"] = reason


def test_batch_launcher_maps_kinds_states_and_names():
    stub = StubBatch()
    launcher = BatchLauncher({"engine": "q-cpu", "dorado": "q-gpu"}, {"engine": "def-engine", "dorado": "def-dorado"},
                             client=stub)
    h = launcher.submit(JobSpec(name="r1-engine-1", kind="engine", run_id="r1", generation=1,
                                env={"SPECIMUX_RUN_ID": "r1"}, args=["--x"]))
    sub = stub.submitted[0]
    assert sub["jobQueue"] == "q-cpu" and sub["jobDefinition"] == "def-engine"
    assert sub["containerOverrides"]["environment"] == [{"name": "SPECIMUX_RUN_ID", "value": "r1"}]
    assert sub["containerOverrides"]["command"] == ["--x"] and sub["tags"]["generation"] == "1"
    assert "resourceRequirements" not in sub["containerOverrides"]
    launcher.submit(JobSpec(name="r1-engine-2", kind="engine", run_id="r1", generation=2, vcpus=16))
    assert stub.submitted[1]["containerOverrides"]["resourceRequirements"] == [
        {"type": "VCPU", "value": "16"}, {"type": "MEMORY", "value": "30400"}]
    assert launcher.describe(h.id).state == "pending"
    stub.jobs[h.id]["status"] = "RUNNING"
    assert launcher.describe(h.id).state == "running"
    assert launcher.find_by_name("r1-engine-1") == h
    assert launcher.find_by_name("nope") is None
    stub.jobs[h.id]["status"] = "SUCCEEDED"
    stub.jobs[h.id]["container"] = {"exitCode": 0}
    st = launcher.describe(h.id)
    assert st.state == "succeeded" and st.exit_code == 0 and st.terminal
    h2 = launcher.submit(JobSpec(name="r1-dorado-1", kind="dorado", run_id="r1", generation=1))
    launcher.cancel(h2.id, "stale")
    st = launcher.describe(h2.id)
    assert st.state == "failed" and st.reason == "stale"
    assert launcher.describe("missing").state == "unknown"


@pytest.fixture
def store():
    DynamoStore.create_table("cp", region=REGION)
    return DynamoStore("cp", region=REGION)


def test_dynamo_runs_are_conditional_and_idempotent(store):
    a = store.create_run({"id": "r1", "user_id": "u1", "state": "created", "spec": {"min_reads": 10, "ratio": 0.5}},
                         client_token="t1")
    again = store.create_run({"id": "r-other", "user_id": "u1", "state": "created"}, client_token="t1")
    assert again["id"] == "r1"
    got = store.get_run("r1")
    assert got["spec"] == {"min_reads": 10, "ratio": 0.5}      # ints and floats survive Decimal
    store.update_run("r1", {"state": "uploading"}, expected_state=["created"])
    with pytest.raises(ConflictError):
        store.update_run("r1", {"state": "uploading"}, expected_state=["created"])
    with pytest.raises(ConflictError):
        store.update_run("nope", {"state": "x"})
    assert store.get_run("r1")["state"] == "uploading"
    store.create_run({"id": "r2", "user_id": "u2", "state": "created"})
    assert [r["id"] for r in store.list_runs(user_id="u1")] == ["r1"]
    assert [r["id"] for r in store.list_runs(states=["created"])] == ["r2"]
    store.delete_run("r2")
    assert store.get_run("r2") is None
    assert store.get_run("r1")["id"] == "r1"


def test_dynamo_intents_commands_reservations(store):
    store.create_run({"id": "r1", "state": "created"})
    iid = store.open_intent("r1", "launch", {"generation": 1})
    assert [i["kind"] for i in store.list_open_intents()] == ["launch"]
    assert store.list_open_intents("r2") == []
    store.resolve_intent(iid, {"job_id": "j1"})
    assert store.list_open_intents() == []

    store.put_command("r1", {"id": "c1", "command": "watch", "actor": "u1"})
    store.put_command("r1", {"id": "c1", "command": "watch", "actor": "u1"})
    assert len(store.list_commands("r1")) == 1
    assert store.get_command("r1", "c1")["outcome"] == "pending"
    store.mark_command("r1", "c1", "applied")
    assert store.list_commands("r1", pending_only=True) == []
    assert store.get_command("r1", "c1")["outcome"] == "applied"
    store.mark_command("r1", "ghost", "applied")                 # unknown id: ignored

    assert store.reserve_stage("engine", "r1")
    assert store.reserve_stage("engine", "r1")           # re-entrant for the holder
    assert not store.reserve_stage("engine", "r2")       # one slot by default
    assert store.stage_holders("engine") == ["r1"]
    store.release_stage("engine", "r2")                  # not the holder: no effect
    assert store.stage_holders("engine") == ["r1"]
    # two slots: a second run joins, a third waits, a freed slot is reused
    assert store.reserve_stage("engine", "r2", slots=2)
    assert store.reserve_stage("engine", "r2", slots=2)  # still one slot per run
    assert not store.reserve_stage("engine", "r3", slots=2)
    assert store.stage_holders("engine") == ["r1", "r2"]
    assert store.stage_holders("dorado") == []           # stages are independent
    store.release_stage("engine", "r1")
    assert store.reserve_stage("engine", "r3", slots=2)
    assert store.stage_holders("engine") == ["r3", "r2"]  # slot order: r3 took slot 0
    store.release_stage("engine", "r3")
    store.release_stage("engine", "r2")
    assert store.reserve_stage("engine", "r2")
    # a run's rows go together
    store.delete_run("r1")
    assert store.list_commands("r1") == []
    assert store.get_run("r1") is None


def test_dynamo_hosts_and_run_scoping(store):
    assert store.get_host("mycomap") is None and store.list_hosts() == []
    store.put_host({"id": "mycomap", "name": "MycoMap", "keys": [{"label": "server", "hash": "h"}], "created": 1.5})
    store.put_host({"id": "fundis", "name": "FUNDIS", "keys": []})
    assert [h["id"] for h in store.list_hosts()] == ["fundis", "mycomap"]
    assert store.get_host("mycomap")["keys"][0]["label"] == "server" and store.get_host("mycomap")["created"] == 1.5
    store.create_run({"id": "r1", "host": "mycomap", "user_id": "u", "state": "created"})
    store.create_run({"id": "r2", "host": "fundis", "user_id": "u", "state": "created"})
    assert [r["id"] for r in store.list_runs(host="mycomap")] == ["r1"]
    store.update_run("r2", {"state": "uploading"})
    assert [r["id"] for r in store.list_runs(host="fundis", states=["uploading"])] == ["r2"]
