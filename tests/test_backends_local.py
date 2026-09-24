"""The local backends behave like the AWS ones they stand in for."""

import subprocess
import sys
import time

import pytest

from specimux_cloud.backends.base import ConflictError, JobSpec
from specimux_cloud.backends.local import DirectoryStorage, MemoryQueue, SQLiteStore, SubprocessLauncher


def test_directory_storage_objects_and_listing(tmp_path):
    s = DirectoryStorage(tmp_path / "bucket", base_url="http://api.local")
    info = s.put("runs/u1/r1/fastq/a.fastq", b"@r\nA\n+\nI\n")
    assert info.key == "runs/u1/r1/fastq/a.fastq" and info.size == 9 and len(info.etag) == 32
    assert s.get(info.key) == b"@r\nA\n+\nI\n"
    assert s.head(info.key) == info
    assert s.head("nope") is None
    src = tmp_path / "b.fastq"
    src.write_bytes(b"x" * 10)
    s.put_file("runs/u1/r1/fastq/b.fastq", src)
    keys = [o.key for o in s.list("runs/u1/r1/fastq")]
    assert keys == ["runs/u1/r1/fastq/a.fastq", "runs/u1/r1/fastq/b.fastq"]
    assert s.list("runs/u1/r1/pod5") == []
    dest = tmp_path / "dl" / "a.fastq"
    s.download(info.key, dest)
    assert dest.read_bytes() == b"@r\nA\n+\nI\n"
    assert s.delete_prefix("runs/u1/r1") == 2
    assert s.list("runs") == []
    with pytest.raises(ValueError):
        s.put("../escape", b"")


def test_directory_storage_presigning(tmp_path):
    s = DirectoryStorage(tmp_path, base_url="http://api.local", secret="k")
    url = s.presign_put("runs/u/r/fastq/x.fastq", expires_s=60)
    assert url.startswith("http://api.local/v1/storage/runs/u/r/fastq/x.fastq?exp=")
    from urllib.parse import parse_qs, urlsplit
    q = parse_qs(urlsplit(url).query)
    assert s.verify("PUT", "runs/u/r/fastq/x.fastq", q["exp"][0], q["sig"][0])
    assert not s.verify("GET", "runs/u/r/fastq/x.fastq", q["exp"][0], q["sig"][0])   # method bound
    assert not s.verify("PUT", "runs/u/r/fastq/y.fastq", q["exp"][0], q["sig"][0])   # key bound
    assert not s.verify("PUT", "runs/u/r/fastq/x.fastq", q["exp"][0], "bad")
    expired = DirectoryStorage(tmp_path, base_url="http://api.local", secret="k").presign_get("a", expires_s=-5)
    q = parse_qs(urlsplit(expired).query)
    assert not s.verify("GET", "a", q["exp"][0], q["sig"][0])


def test_memory_queue_redelivers_until_acked():
    q = MemoryQueue(visibility_s=0.2)
    q.send("r1", {"command": "watch"})
    q.send("r1", {"command": "finalize"})
    assert q.receive("r2") == []
    first = q.receive("r1", max_messages=1)
    assert [m.body["command"] for m in first] == ["watch"]
    second = q.receive("r1", max_messages=5)
    assert [m.body["command"] for m in second] == ["finalize"]
    assert q.receive("r1") == []                      # both in flight
    time.sleep(0.25)
    back = q.receive("r1", max_messages=5)           # neither acked: both return
    assert sorted(m.body["command"] for m in back) == ["finalize", "watch"]
    for m in back:
        q.ack("r1", m.id)
    time.sleep(0.25)
    assert q.receive("r1") == []
    t0 = time.monotonic()
    assert q.receive("r1", wait_s=0.3) == []
    assert time.monotonic() - t0 >= 0.25


def test_subprocess_launcher(tmp_path):
    launcher = SubprocessLauncher(tmp_path / "logs", command=[sys.executable, "-c",
                                  "import os,sys,time; print('env', os.environ['RUN_ID'], sys.argv[1:]); "
                                  "time.sleep(0.3); sys.exit(int(os.environ.get('EXIT','0')))"])
    ok = launcher.submit(JobSpec(name="r1-engine-1", kind="engine", run_id="r1", generation=1,
                                 env={"RUN_ID": "r1"}, args=["--x"]))
    bad = launcher.submit(JobSpec(name="r2-engine-1", kind="engine", run_id="r2", generation=1,
                                  env={"RUN_ID": "r2", "EXIT": "3"}))
    assert launcher.describe(ok.id).state == "running"
    assert launcher.find_by_name("r1-engine-1") == ok
    assert launcher.find_by_name("nope") is None
    launcher.wait_all()
    assert launcher.describe(ok.id).state == "succeeded"
    st = launcher.describe(bad.id)
    assert st.state == "failed" and st.exit_code == 3 and st.terminal
    assert launcher.describe("999999").state == "unknown"
    assert "env r1 ['--x']" in (tmp_path / "logs" / "r1-engine-1.log").read_text()


def test_sqlite_store_runs_are_conditional_and_idempotent(tmp_path):
    st = SQLiteStore(tmp_path / "cp.sqlite")
    a = st.create_run({"id": "r1", "user_id": "u1", "state": "created", "spec": {"mode": "batch"}}, client_token="t1")
    again = st.create_run({"id": "r-other", "user_id": "u1", "state": "created"}, client_token="t1")
    assert again["id"] == "r1"                        # same client token → same run
    assert st.get_run("r1")["spec"] == {"mode": "batch"}
    st.update_run("r1", {"state": "uploading"}, expected_state=["created"])
    with pytest.raises(ConflictError):
        st.update_run("r1", {"state": "uploading"}, expected_state=["created"])
    with pytest.raises(ConflictError):
        st.update_run("nope", {"state": "x"})
    assert st.get_run("r1")["state"] == "uploading"
    st.create_run({"id": "r2", "user_id": "u2", "state": "created"})
    assert [r["id"] for r in st.list_runs(user_id="u1")] == ["r1"]
    assert [r["id"] for r in st.list_runs(states=["created"])] == ["r2"]
    st.delete_run("r2")
    assert st.get_run("r2") is None


def test_sqlite_store_intents_commands_reservations(tmp_path):
    st = SQLiteStore(tmp_path / "cp.sqlite")
    st.create_run({"id": "r1", "state": "created"})
    iid = st.open_intent("r1", "launch", {"generation": 1})
    assert [i["kind"] for i in st.list_open_intents()] == ["launch"]
    st.resolve_intent(iid, {"job_id": "j1"})
    assert st.list_open_intents("r1") == []

    st.put_command("r1", {"id": "c1", "command": "watch", "actor": "u1"})
    st.put_command("r1", {"id": "c1", "command": "watch", "actor": "u1"})  # idempotent
    assert len(st.list_commands("r1")) == 1
    assert st.get_command("r1", "c1")["outcome"] == "pending"
    st.mark_command("r1", "c1", "applied")
    assert st.list_commands("r1", pending_only=True) == []
    assert st.get_command("r1", "c1")["outcome"] == "applied"

    assert st.reserve_stage("engine", "r1")
    assert st.reserve_stage("engine", "r1")           # re-entrant for the holder
    assert not st.reserve_stage("engine", "r2")       # one slot by default
    assert st.stage_holders("engine") == ["r1"]
    st.release_stage("engine", "r2")                  # not the holder: no effect
    assert st.stage_holders("engine") == ["r1"]
    # two slots: a second run joins, a third waits, a freed slot is reused
    assert st.reserve_stage("engine", "r2", slots=2)
    assert st.reserve_stage("engine", "r2", slots=2)  # still one slot per run
    assert not st.reserve_stage("engine", "r3", slots=2)
    assert st.stage_holders("engine") == ["r1", "r2"]
    assert st.stage_holders("dorado") == []           # stages are independent
    st.release_stage("engine", "r1")
    assert st.reserve_stage("engine", "r3", slots=2)
    assert st.stage_holders("engine") == ["r3", "r2"]  # slot order: r3 took slot 0
    st.release_stage("engine", "r3")
    st.release_stage("engine", "r2")
    assert st.reserve_stage("engine", "r2")
    # reopening the file keeps everything
    st2 = SQLiteStore(tmp_path / "cp.sqlite")
    assert st2.stage_holders("engine") == ["r2"]


def test_sqlite_store_hosts_and_run_scoping(tmp_path):
    s = SQLiteStore(tmp_path / "cp.sqlite")
    assert s.get_host("mycomap") is None and s.list_hosts() == []
    s.put_host({"id": "mycomap", "name": "MycoMap", "keys": [{"label": "server", "hash": "h1"}]})
    s.put_host({"id": "fundis", "name": "FUNDIS", "keys": []})
    assert [h["id"] for h in s.list_hosts()] == ["fundis", "mycomap"]
    s.put_host({"id": "mycomap", "name": "MycoMap!", "keys": []})       # replace
    assert s.get_host("mycomap")["name"] == "MycoMap!"
    s.create_run({"id": "r1", "host": "mycomap", "user_id": "u", "state": "created"})
    s.create_run({"id": "r2", "host": "fundis", "user_id": "u", "state": "created"})
    assert [r["id"] for r in s.list_runs(host="mycomap")] == ["r1"]
    assert [r["id"] for r in s.list_runs(host="fundis", user_id="u")] == ["r2"]
    assert [r["id"] for r in s.list_runs()] == ["r1", "r2"]
