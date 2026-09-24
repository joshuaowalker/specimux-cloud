"""Local backends: a directory, an in-memory queue, subprocesses, SQLite.

Everything the run API needs on one laptop with no AWS account. These are
also what CI runs, so they implement the semantics the AWS backends have
rather than shortcuts: presigned URLs are real signed URLs (served by the
run API's storage route), queue messages are redelivered unless
acknowledged, conditional writes raise on conflict.
"""

import hashlib
import hmac
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import quote, urlencode

from .base import (ConflictError, JobHandle, JobSpec, JobStatus, Message,
                   ObjectInfo)


def _etag(path: Path) -> str:
    """What S3 reports for a single-part object: the hex MD5."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_key(key: str) -> str:
    parts = [p for p in key.split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        raise ValueError(f"Invalid storage key: {key!r}")
    return "/".join(parts)


# --- Storage ---

class DirectoryStorage:
    """Objects as files under ``root``. Presigned URLs point at
    ``{base_url}/v1/storage/{key}?exp=..&sig=..`` which the run API serves
    (see ``runapi.storage_routes``); the signature is an HMAC over method,
    key and expiry with ``secret``."""

    def __init__(self, root: Path, base_url: str = "", secret: Optional[str] = None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.base_url = base_url.rstrip("/")
        self.secret = (secret or "local-dev-secret").encode()

    def _path(self, key: str) -> Path:
        return self.root / _safe_key(key)

    def put(self, key: str, data: bytes) -> ObjectInfo:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + f".{uuid.uuid4().hex[:8]}.tmp")
        tmp.write_bytes(data)
        os.replace(tmp, p)
        return ObjectInfo(_safe_key(key), len(data), _etag(p))

    def put_file(self, key: str, path: Path) -> ObjectInfo:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + f".{uuid.uuid4().hex[:8]}.tmp")
        shutil.copyfile(path, tmp)
        os.replace(tmp, p)
        return ObjectInfo(_safe_key(key), p.stat().st_size, _etag(p))

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def download(self, key: str, dest: Path) -> ObjectInfo:
        p = self._path(key)
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        shutil.copyfile(p, tmp)
        os.replace(tmp, dest)
        return ObjectInfo(_safe_key(key), p.stat().st_size, _etag(p))

    def head(self, key: str) -> Optional[ObjectInfo]:
        p = self._path(key)
        if not p.is_file():
            return None
        return ObjectInfo(_safe_key(key), p.stat().st_size, _etag(p))

    def list(self, prefix: str) -> list[ObjectInfo]:
        base = self.root / prefix.strip("/") if prefix.strip("/") else self.root
        if not base.exists():
            return []
        out = []
        for p in sorted(base.rglob("*")):
            if p.is_file() and not p.name.endswith(".tmp") and not p.name.endswith(".part"):
                key = str(p.relative_to(self.root))
                out.append(ObjectInfo(key, p.stat().st_size, _etag(p)))
        return out

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def delete_prefix(self, prefix: str) -> int:
        objs = self.list(prefix)
        for o in objs:
            self.delete(o.key)
        return len(objs)

    # presigning
    def _sign(self, method: str, key: str, exp: int) -> str:
        msg = f"{method}\n{key}\n{exp}".encode()
        return hmac.new(self.secret, msg, hashlib.sha256).hexdigest()

    def _presign(self, method: str, key: str, expires_s: int) -> str:
        key = _safe_key(key)
        exp = int(time.time()) + expires_s
        q = urlencode({"exp": exp, "sig": self._sign(method, key, exp)})
        return f"{self.base_url}/v1/storage/{quote(key)}?{q}"

    def presign_put(self, key: str, expires_s: int = 3600) -> str:
        return self._presign("PUT", key, expires_s)

    def presign_get(self, key: str, expires_s: int = 3600) -> str:
        return self._presign("GET", key, expires_s)

    def verify(self, method: str, key: str, exp: str, sig: str) -> bool:
        try:
            exp_i = int(exp)
        except (TypeError, ValueError):
            return False
        if exp_i < time.time():
            return False
        return hmac.compare_digest(self._sign(method, _safe_key(key), exp_i), sig or "")


# --- Command queue ---

class MemoryQueue:
    """Per-run FIFO with SQS-like visibility: a received message is hidden
    for ``visibility_s`` and comes back unless acknowledged."""

    def __init__(self, visibility_s: float = 30.0):
        self.visibility_s = visibility_s
        self._lock = threading.Condition()
        self._queues: dict[str, deque] = {}
        self._inflight: dict[str, dict[str, tuple[float, Message]]] = {}

    def send(self, run_id: str, body: dict) -> str:
        msg = Message(uuid.uuid4().hex, dict(body))
        with self._lock:
            self._queues.setdefault(run_id, deque()).append(msg)
            self._lock.notify_all()
        return msg.id

    def _requeue_expired(self, run_id: str) -> None:
        now = time.monotonic()
        inflight = self._inflight.get(run_id, {})
        for mid, (deadline, msg) in list(inflight.items()):
            if deadline <= now:
                del inflight[mid]
                self._queues.setdefault(run_id, deque()).appendleft(msg)

    def receive(self, run_id: str, wait_s: float = 0.0, max_messages: int = 10) -> list[Message]:
        deadline = time.monotonic() + wait_s
        with self._lock:
            while True:
                self._requeue_expired(run_id)
                q = self._queues.get(run_id)
                if q:
                    out = []
                    while q and len(out) < max_messages:
                        msg = q.popleft()
                        self._inflight.setdefault(run_id, {})[msg.id] = (
                            time.monotonic() + self.visibility_s, msg)
                        out.append(msg)
                    return out
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                self._lock.wait(timeout=min(remaining, 1.0))

    def ack(self, run_id: str, message_id: str) -> None:
        with self._lock:
            self._inflight.get(run_id, {}).pop(message_id, None)

    def purge(self, run_id: str) -> None:
        with self._lock:
            self._queues.pop(run_id, None)
            self._inflight.pop(run_id, None)


# --- Launcher ---

class SubprocessLauncher:
    """Runs each job as a local subprocess of the command for its kind
    (default: the engine wrapper module for ``engine``, the dorado wrapper
    for ``dorado``) with the spec's env and args; job ids are pids. Logs
    go to ``log_dir/<name>.log``."""

    DEFAULT_COMMANDS = {"engine": [sys.executable, "-m", "specimux_cloud.engine.wrapper"],
                        "dorado": [sys.executable, "-m", "specimux_cloud.dorado.wrapper"]}

    def __init__(self, log_dir: Path, command: Optional[list[str]] = None,
                 commands: Optional[dict[str, list[str]]] = None):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.commands = {**self.DEFAULT_COMMANDS, **(commands or {})}
        if command:
            self.commands["engine"] = list(command)
        self._lock = threading.Lock()
        self._jobs: dict[str, tuple[JobSpec, subprocess.Popen]] = {}

    def submit(self, spec: JobSpec) -> JobHandle:
        log = open(self.log_dir / f"{spec.name}.log", "ab")
        env = {**os.environ, **{k: str(v) for k, v in spec.env.items()}}
        proc = subprocess.Popen(self.commands[spec.kind] + list(spec.args), env=env,
                                stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        handle = JobHandle(str(proc.pid), spec.name)
        with self._lock:
            self._jobs[handle.id] = (spec, proc)
        return handle

    def describe(self, job_id: str) -> JobStatus:
        with self._lock:
            entry = self._jobs.get(job_id)
        if entry is None:
            return JobStatus("unknown", reason="no such job")
        _, proc = entry
        rc = proc.poll()
        if rc is None:
            return JobStatus("running")
        return JobStatus("succeeded" if rc == 0 else "failed", exit_code=rc,
                         reason="" if rc == 0 else f"exit {rc}")

    def find_by_name(self, name: str) -> Optional[JobHandle]:
        with self._lock:
            for jid, (spec, _) in self._jobs.items():
                if spec.name == name:
                    return JobHandle(jid, name)
        return None

    def cancel(self, job_id: str, reason: str = "") -> None:
        with self._lock:
            entry = self._jobs.get(job_id)
        if entry and entry[1].poll() is None:
            entry[1].terminate()

    def wait_all(self, timeout: float = 30.0) -> None:
        with self._lock:
            procs = [p for _, p in self._jobs.values()]
        for p in procs:
            try:
                p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                p.kill()


# --- Store ---

class SQLiteStore:
    """Control-plane state in one SQLite file. Records are JSON documents;
    conditional updates check the run's ``state`` inside one transaction,
    the same guarantee DynamoDB's condition expressions give."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, client_token TEXT UNIQUE,
                user_id TEXT, state TEXT, created REAL, updated REAL, doc TEXT);
            CREATE TABLE IF NOT EXISTS archives (id TEXT PRIMARY KEY, user_id TEXT, doc TEXT);
            CREATE TABLE IF NOT EXISTS intents (id TEXT PRIMARY KEY, run_id TEXT, kind TEXT,
                payload TEXT, result TEXT, opened REAL, resolved REAL);
            CREATE TABLE IF NOT EXISTS commands (run_id TEXT, id TEXT, doc TEXT, outcome TEXT,
                reason TEXT, created REAL, updated REAL, PRIMARY KEY (run_id, id));
            CREATE TABLE IF NOT EXISTS stage_slots (stage TEXT, slot INTEGER, run_id TEXT, since REAL,
                PRIMARY KEY (stage, slot));
            CREATE TABLE IF NOT EXISTS hosts (id TEXT PRIMARY KEY, doc TEXT);
        """)

    # runs
    def create_run(self, run: dict, client_token: Optional[str] = None) -> dict:
        with self._lock:
            if client_token:
                row = self._conn.execute("SELECT doc FROM runs WHERE client_token=?", (client_token,)).fetchone()
                if row:
                    return json.loads(row[0])
            now = time.time()
            run = {**run, "created": run.get("created", now), "updated": now}
            self._conn.execute(
                "INSERT INTO runs (id, client_token, user_id, state, created, updated, doc) VALUES (?,?,?,?,?,?,?)",
                (run["id"], client_token, run.get("user_id"), run.get("state"), run["created"], now, json.dumps(run)))
            return run

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT doc FROM runs WHERE id=?", (run_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def update_run(self, run_id: str, updates: dict, expected_state: Optional[Iterable[str]] = None) -> dict:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT doc FROM runs WHERE id=?", (run_id,)).fetchone()
                if not row:
                    raise ConflictError(f"no run {run_id}")
                run = json.loads(row[0])
                if expected_state is not None and run.get("state") not in set(expected_state):
                    raise ConflictError(f"run {run_id} is {run.get('state')}, expected {list(expected_state)}")
                run.update(updates(dict(run)) if callable(updates) else updates)
                run["updated"] = time.time()
                self._conn.execute("UPDATE runs SET state=?, updated=?, doc=? WHERE id=?",
                                   (run.get("state"), run["updated"], json.dumps(run), run_id))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return run

    def list_runs(self, user_id: Optional[str] = None, states: Optional[Iterable[str]] = None,
                  host: Optional[str] = None) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT doc FROM runs ORDER BY created").fetchall()
        runs = [json.loads(r[0]) for r in rows]
        if host is not None:
            runs = [r for r in runs if r.get("host") == host]
        if user_id is not None:
            runs = [r for r in runs if r.get("user_id") == user_id]
        if states is not None:
            wanted = set(states)
            runs = [r for r in runs if r.get("state") in wanted]
        return runs

    def delete_run(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM runs WHERE id=?", (run_id,))
            self._conn.execute("DELETE FROM commands WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM intents WHERE run_id=?", (run_id,))

    # hosts
    def put_host(self, host: dict) -> dict:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO hosts (id, doc) VALUES (?,?)",
                               (host["id"], json.dumps(host)))
        return host

    def get_host(self, host_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT doc FROM hosts WHERE id=?", (host_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def list_hosts(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT doc FROM hosts ORDER BY id").fetchall()
        return [json.loads(r[0]) for r in rows]

    # archives
    def put_archive(self, archive: dict) -> dict:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO archives (id, user_id, doc) VALUES (?,?,?)",
                               (archive["id"], archive.get("user_id"), json.dumps(archive)))
        return archive

    def get_archive(self, archive_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT doc FROM archives WHERE id=?", (archive_id,)).fetchone()
        return json.loads(row[0]) if row else None

    # intents
    def open_intent(self, run_id: str, kind: str, payload: dict) -> str:
        iid = uuid.uuid4().hex
        with self._lock:
            self._conn.execute("INSERT INTO intents (id, run_id, kind, payload, opened) VALUES (?,?,?,?,?)",
                               (iid, run_id, kind, json.dumps(payload), time.time()))
        return iid

    def resolve_intent(self, intent_id: str, result: dict) -> None:
        with self._lock:
            self._conn.execute("UPDATE intents SET result=?, resolved=? WHERE id=?",
                               (json.dumps(result), time.time(), intent_id))

    def list_open_intents(self, run_id: Optional[str] = None) -> list[dict]:
        with self._lock:
            if run_id is None:
                rows = self._conn.execute(
                    "SELECT id, run_id, kind, payload, opened FROM intents WHERE resolved IS NULL ORDER BY opened").fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id, run_id, kind, payload, opened FROM intents WHERE resolved IS NULL AND run_id=? ORDER BY opened",
                    (run_id,)).fetchall()
        return [{"id": r[0], "run_id": r[1], "kind": r[2], "payload": json.loads(r[3]), "opened": r[4]} for r in rows]

    # commands
    def put_command(self, run_id: str, command: dict) -> dict:
        now = time.time()
        doc = {**command, "run_id": run_id, "outcome": "pending", "created": now}
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO commands (run_id, id, doc, outcome, reason, created, updated) VALUES (?,?,?,?,?,?,?)",
                (run_id, command["id"], json.dumps(doc), "pending", None, now, now))
        return doc

    def get_command(self, run_id: str, command_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute("SELECT doc, outcome, reason FROM commands WHERE run_id=? AND id=?",
                                     (run_id, command_id)).fetchone()
        if not row:
            return None
        return {**json.loads(row[0]), "outcome": row[1], "reason": row[2]}

    def mark_command(self, run_id: str, command_id: str, outcome: str, reason: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute("UPDATE commands SET outcome=?, reason=?, updated=? WHERE run_id=? AND id=?",
                               (outcome, reason, time.time(), run_id, command_id))

    def list_commands(self, run_id: str, pending_only: bool = False) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT doc, outcome, reason FROM commands WHERE run_id=? ORDER BY created", (run_id,)).fetchall()
        cmds = [{**json.loads(r[0]), "outcome": r[1], "reason": r[2]} for r in rows]
        if pending_only:
            cmds = [c for c in cmds if c["outcome"] == "pending"]
        return cmds

    # reservations
    def reserve_stage(self, stage: str, run_id: str, slots: int = 1) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute("SELECT slot, run_id FROM stage_slots WHERE stage=?", (stage,)).fetchall()
                if any(r[1] == run_id for r in rows):
                    self._conn.execute("COMMIT")
                    return True
                taken = {r[0] for r in rows}
                free = [i for i in range(slots) if i not in taken]
                if free:
                    self._conn.execute("INSERT INTO stage_slots (stage, slot, run_id, since) VALUES (?,?,?,?)",
                                       (stage, free[0], run_id, time.time()))
                self._conn.execute("COMMIT")
                return bool(free)
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def release_stage(self, stage: str, run_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM stage_slots WHERE stage=? AND run_id=?", (stage, run_id))

    def stage_holders(self, stage: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT run_id FROM stage_slots WHERE stage=? ORDER BY slot", (stage,)).fetchall()
        return [r[0] for r in rows]
