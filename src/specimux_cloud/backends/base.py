"""The backend interfaces. The run API is written against these and
nothing else, so AWS is a backend swap rather than a second code path.

Every method is synchronous; the run API calls them from request handlers
and from its reconciliation loop. Implementations must be safe to call
from several threads.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol


# --- Storage ---

@dataclass
class ObjectInfo:
    key: str
    size: int
    etag: str


class Storage(Protocol):
    """Object storage under one bucket: S3, or a directory locally.

    Keys are ``/``-separated paths (``runs/<user>/<run>/fastq/x.fastq``).
    ``presign_put``/``presign_get`` return URLs a client can PUT to or GET
    from without credentials, for a limited time; the uploader and the
    browser never see the storage credentials.
    """

    def put(self, key: str, data: bytes) -> ObjectInfo: ...

    def put_file(self, key: str, path: Path) -> ObjectInfo: ...

    def get(self, key: str) -> bytes: ...

    def download(self, key: str, dest: Path) -> ObjectInfo: ...

    def head(self, key: str) -> Optional[ObjectInfo]: ...

    def list(self, prefix: str) -> list[ObjectInfo]: ...

    def delete(self, key: str) -> None: ...

    def delete_prefix(self, prefix: str) -> int: ...

    def presign_put(self, key: str, expires_s: int = 3600) -> str: ...

    def presign_get(self, key: str, expires_s: int = 3600, filename: Optional[str] = None) -> str:
        """A GET URL; with ``filename``, the download is saved under that name."""
        ...


# --- Command queue ---

@dataclass
class Message:
    id: str
    body: dict


class CommandQueue(Protocol):
    """Per-run command delivery from the run API to the engine, at least
    once: a received message is redelivered unless acknowledged.
    """

    def send(self, run_id: str, body: dict) -> str: ...

    def receive(self, run_id: str, wait_s: float = 0.0, max_messages: int = 10) -> list[Message]: ...

    def ack(self, run_id: str, message_id: str) -> None: ...

    def purge(self, run_id: str) -> None: ...


# --- Job launcher ---

@dataclass
class JobSpec:
    """One compute job. ``name`` is deterministic (run id + kind +
    generation) so a submission whose response was lost can be found
    again by name."""
    name: str
    kind: str                     # "engine" | "dorado"
    run_id: str
    generation: int
    env: dict = field(default_factory=dict)
    args: list[str] = field(default_factory=list)
    vcpus: Optional[int] = None          # override the definition's size
    memory_mib: Optional[int] = None
    timeout_s: Optional[int] = None      # override the definition's attempt timeout


@dataclass
class JobHandle:
    id: str
    name: str


@dataclass
class JobStatus:
    state: str                    # pending | running | succeeded | failed | unknown
    exit_code: Optional[int] = None
    reason: str = ""

    @property
    def terminal(self) -> bool:
        return self.state in ("succeeded", "failed")


class Launcher(Protocol):
    def submit(self, spec: JobSpec) -> JobHandle: ...

    def describe(self, job_id: str) -> JobStatus: ...

    def find_by_name(self, name: str) -> Optional[JobHandle]: ...

    def cancel(self, job_id: str, reason: str = "") -> None: ...


# --- Control-plane store ---

class ConflictError(Exception):
    """A conditional write found different state than expected."""


class Store(Protocol):
    """Durable control-plane state with conditional writes: run records,
    hosts and their keys, open intents (a side effect about to happen),
    commands and their outcomes, and the per-stage slot reservations that
    cap how many runs a stage runs at once. DynamoDB in AWS, SQLite locally.

    Records are plain dicts; ``state`` on a run is the field conditional
    updates are checked against.
    """

    # runs
    def create_run(self, run: dict, client_token: Optional[str] = None) -> dict:
        """Insert a run; with the same client token, return the existing one."""
        ...

    def get_run(self, run_id: str) -> Optional[dict]: ...

    def update_run(self, run_id: str, updates: "dict | Callable[[dict], dict]",
                   expected_state: Optional[Iterable[str]] = None) -> dict:
        """Merge ``updates`` into the run; raise ConflictError if the run's
        state is not one of ``expected_state``. ``updates`` may be a
        function of the current record returning the updates, called on the
        record as the write sees it (again on a retry), so a
        read-modify-write of a nested field (``stages``, ``jobs``) cannot
        lose a concurrent change to it."""
        ...

    def list_runs(self, user_id: Optional[str] = None, states: Optional[Iterable[str]] = None,
                  host: Optional[str] = None) -> list[dict]: ...

    def delete_run(self, run_id: str) -> None: ...

    # hosts: the systems that create runs and vouch for their users, with
    # their (hashed) service keys; see runapi.service "hosts"
    def put_host(self, host: dict) -> dict:
        """Insert or replace a host record by ``id``."""
        ...

    def get_host(self, host_id: str) -> Optional[dict]: ...

    def list_hosts(self) -> list[dict]: ...

    # archives
    def put_archive(self, archive: dict) -> dict: ...

    def get_archive(self, archive_id: str) -> Optional[dict]: ...

    # intents: written before a side effect, resolved after
    def open_intent(self, run_id: str, kind: str, payload: dict) -> str: ...

    def resolve_intent(self, intent_id: str, result: dict) -> None: ...

    def list_open_intents(self, run_id: Optional[str] = None) -> list[dict]: ...

    # commands
    def put_command(self, run_id: str, command: dict) -> dict: ...

    def get_command(self, run_id: str, command_id: str) -> Optional[dict]: ...

    def mark_command(self, run_id: str, command_id: str, outcome: str, reason: Optional[str] = None) -> None: ...

    def list_commands(self, run_id: str, pending_only: bool = False) -> list[dict]: ...

    # stage reservations (the concurrency cap): ``slots`` numbered slots
    # per stage, a run holds at most one; claiming a free slot is a
    # conditional write on that slot, so the cap holds under races
    def reserve_stage(self, stage: str, run_id: str, slots: int = 1) -> bool: ...

    def release_stage(self, stage: str, run_id: str) -> None: ...

    def stage_holders(self, stage: str) -> list[str]: ...
