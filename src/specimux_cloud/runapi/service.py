"""RunService: the run API's logic, independent of HTTP and of the backends'
implementations. ``app.py`` puts routes in front of it.

A run's life (batch, FASTQ): created (spec and input files stored, run id
and upload secret issued) → uploading (the uploader asked for presigned
URLs; archive objects arriving) → input_complete (``complete`` fixed the
manifest) → running (engine job launched, generation 1) → sealed or
failed (the wrapper reported the engine's exit; output and log copied to
storage; results package built). A POD5 run passes through basecalling
first: a dorado job (generation 1, the ``dorado`` stage) writes one FASTQ
per POD5 file under the run and reports each; when its exit report
arrives with every file accounted for, the engine launches (generation
2) over those FASTQs exactly as it would over uploaded ones. Every side
effect is an intent first and a job launch has a deterministic name, so
a crash between the two is reconciled at startup rather than duplicated.
"""

import hashlib
import io
import json
import logging
import os
import re
import secrets
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from specimux_suite import __version__ as suite_version
from specimux_suite.state import PipelineState
from specimux_suite.web.viewer import create_viewer_app

from .. import __version__ as cloud_version
from ..packages import DOWNLOAD_SUFFIX, PACKAGES, SEAL_SKIP_DIRS, build_zip, download_name, package_files
from ..progress import basecall_estimate, basecall_text, upload_estimate, upload_text
from ..backends.base import CommandQueue, ConflictError, JobSpec, Launcher, Storage, Store
from . import auth
from .events import IngestLog

logger = logging.getLogger(__name__)

# Run states (see docs/DESIGN.md "Run states")
CREATED = "created"
UPLOADING = "uploading"
INPUT_COMPLETE = "input_complete"
BASECALLING = "basecalling"
RUNNING = "running"
FINALIZING = "finalizing"
SEALING = "sealing"
SEALED = "sealed"
FAILED = "failed"
INCOMPLETE = "incomplete"

ACTIVE_STATES = {UPLOADING, INPUT_COMPLETE, BASECALLING, RUNNING, FINALIZING, SEALING}

# The seal packs the mirror the engine job wrote (the event log and what
# the dashboard serves) into one view.zip, the run's dashboard once its EFS
# directory is gone. packages.SEAL_SKIP_DIRS never goes, nor a photo cache
# (a run from before --no-photo-cache has one; the pages fall back to the
# provider's host).
VIEW_ZIP = "view.zip"
VIEW_SKIP_DIRS = SEAL_SKIP_DIRS | {"inat_photos"}
# Zips of a mirror read thousands of small files over EFS, each a round
# trip (a 7,933-file view.zip took a minute read one at a time)
MIRROR_ZIP_WORKERS = 16
# The dashboard banner before the engine starts (dashboard_status)
STATUS_CACHE_S = 5
RUN_ID = re.compile(r"r[0-9a-f]{8}")
# A run's optional name (spec.name: the host's name for it, e.g. "Run150"),
# which names its downloads
RUN_NAME_MAX = 100
# Cleanup (clean_up, its own loop): a finished run's EFS directory outlives
# it this long (a dashboard open at the end keeps working), and a cancelled
# or abandoned run's upload archive this long (a cancelled run may be
# retried until then). A finished run's view unused this long is dropped
# from memory and its local copy removed.
EFS_GRACE_S = 3600
ARCHIVE_GRACE_S = 7 * 24 * 3600
VIEW_IDLE_S = 1800
# Default engine job size; a run's spec may set "vcpus" (workers follow it)
DEFAULT_VCPUS = 16
MEMORY_MIB_PER_VCPU = 1900   # c6i/m6i have 2 or 4 GiB per vCPU; leave headroom
# A live engine runs as long as sequencing (a MinION run is up to 72 h);
# the job definition's 12 h attempt timeout is for batch
LIVE_ENGINE_TIMEOUT_S = 96 * 3600
ENGINE_STAGE = "engine"
DORADO_STAGE = "dorado"
DEFAULT_STAGE_SLOTS = {ENGINE_STAGE: 2, DORADO_STAGE: 2}


def stage_of(run: dict, stage: str) -> dict:
    """A run's record for one stage: ``{"generation", "job_secret",
    "active"}``, or ``{}`` before its first launch. Each stage has its own
    job identity, so two stages' jobs can be live at once (a live POD5 run's
    basecalling beside its engine) without fencing each other off; the
    generation numbers come from one counter per run, so a job name
    (``<run>-<stage>-<generation>``) is never reused."""
    return dict((run.get("stages") or {}).get(stage) or {})


def engine_generation(run: dict) -> int:
    """The engine generation the dashboard shows (records from before
    per-stage identity carry only the run's generation)."""
    return int(stage_of(run, ENGINE_STAGE).get("generation") or run.get("generation") or 0)


def parse_stage_slots(text: str) -> dict:
    """``engine=2,dorado=2`` → ``{"engine": 2, "dorado": 2}``."""
    out = {}
    for part in (text or "").split(","):
        if part.strip():
            stage, _, n = part.partition("=")
            out[stage.strip()] = int(n)
    return out
# A launch intent younger than this may still be in flight; reconcile leaves it
INTENT_GRACE_S = 120.0
LOAD_CACHE_S = 15
# While a run takes uploads its record carries what has arrived (a listing
# of its archive, cached this long) and the uploader's last progress report
UPLOAD_LIST_CACHE_S = 10.0
# How long a public session's check of the run's sharing may be cached
PUBLIC_CHECK_S = 5.0
# At most this many live event streams per run (public links can travel)
MAX_VIEWERS_PER_RUN = 200
REFERENCE_SHA256 = re.compile(r"[0-9a-f]{64}")
# A host's user id names storage prefixes (runs/<user>/, archives/<user>/),
# so it is one plain path segment
USER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}")

# Basecalling defaults (docs/DESIGN.md "Basecalling stage"): the published
# protocol's `dorado basecaller sup --no-trim` followed by a length window,
# no qscore filter. The window is deliberately wide, so a default never
# loses good reads: 3000 holds any ITS amplicon with its primers and
# indexes, 100 barely holds a pair of primers and indexes. A lab narrows
# it per run (the protocol's 400-2000 for full ITS, 100-700 for ITS2). The model is a dorado model complex; the images bake the
# listed ones so no download happens at run time.
DEFAULT_DORADO_MODELS = ["sup@v5.0.0", "sup@v5.2.0", "hac@v6.0.0"]
DEFAULT_BASECALL = {"model": "sup@v5.0.0", "min_length": 100, "max_length": 3000, "min_qscore": None}

COMMANDS = ("watch", "unwatch", "correct", "dismiss", "rescan", "finalize", "abort")


class ServiceError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class ServiceConfig:
    data_dir: Path                       # local root: work dirs, caches
    base_url: str = "http://127.0.0.1:8090"
    # signs run tokens and session cookies (and, locally, presigned URLs);
    # every replica of the run API holds the same one
    session_secret: str = ""
    # engine work dirs (locally shared with the engine subprocess; EFS in AWS)
    work_root: Optional[Path] = None
    # what the engine job sees as the run API (in a container this differs
    # from base_url, e.g. host.docker.internal)
    engine_api_url: Optional[str] = None
    engine_extra_args: list = field(default_factory=list)
    session_ttl_s: float = auth.SESSION_TTL_S
    # an abandoned upload ends as incomplete: a run that has started
    # uploading and asked for no upload URL in this long, or one that never
    # uploaded at all (a lab may create the run days before sequencing ends)
    upload_idle_s: float = 24 * 3600
    created_idle_s: float = 7 * 24 * 3600
    # a live run whose uploads stop for this long is completed with what
    # arrived, so its engine finalizes instead of idling on an instance
    live_idle_s: float = 3 * 3600
    # the dorado model complexes a POD5 run may ask for (the dorado image
    # bakes them; docker/dorado.Dockerfile); the first is the default
    dorado_models: list = field(default_factory=lambda: list(DEFAULT_DORADO_MODELS))
    # how many runs each stage runs at once (SPECIMUX_STAGE_SLOTS): a cost
    # cap, and for dorado the G-instance quota (8 vCPUs = two xlarge)
    stage_slots: dict = field(default_factory=lambda: dict(DEFAULT_STAGE_SLOTS))
    # The oldest uploader still served (SPECIMUX_MIN_UPLOADER); None serves
    # every one. Raise it only when an old uploader truly cannot work
    # (specimux_cloud/versioning.py).
    min_uploader: Optional[str] = None

    def slots(self, stage: str) -> int:
        return max(1, int(self.stage_slots.get(stage, 1)))

    def __post_init__(self):
        # Absolute: the engine runs with its work dir as cwd and gets these
        # paths in its environment and command line
        self.data_dir = Path(self.data_dir).resolve()
        self.work_root = Path(self.work_root).resolve() if self.work_root else self.data_dir / "work"
        self.engine_api_url = self.engine_api_url or self.base_url
        if not self.session_secret:
            raise ValueError("session_secret is required")

    @property
    def secure_cookies(self) -> bool:
        return self.base_url.lower().startswith("https://")


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _cancelled(run: dict) -> bool:
    return bool(run.get("cancel"))


def _new_run_id() -> str:
    return "r" + secrets.token_hex(4)


class RunView:
    """Everything the viewer needs for one run: its ingest log, the state
    kept current from it, and the mounted viewer app."""

    def __init__(self, run: dict, output_dir: Path, events_path: Path, runtime: dict,
                 public_url: Optional[str] = None, status=None):
        self.run_id = run["id"]
        self.host_id = run.get("host")
        # each engine generation starts a fresh event log (the wrapper moves
        # the previous attempt's mirror aside), so a view is per generation
        self.generation = engine_generation(run)
        self.log = IngestLog(events_path)
        self.state = PipelineState()
        # A live run replays faithfully; a finished one heals as a restart would
        self.state.rebuild(self.log, heal=run.get("state") not in ACTIVE_STATES)
        self.log.add_listener(self.state.apply)
        # The effective configuration arrives with pipeline.started; the
        # viewer holds this dict by reference, so it is filled in place
        self.config_summary: dict = dict(run.get("effective_config") or {})
        # the demux in flight (its last specimux.progress), for the run page
        self.demux: Optional[dict] = None
        for ev in self.log.replay():
            self._track_config(ev)
            self._track_demux(ev)
        self.log.add_listener(self._track_config)
        self.log.add_listener(self._track_demux)
        # The dashboard draws a QR code from /api/state's "share": the
        # owner's public link while the run is shared (held by reference)
        self.share: dict = {}
        self.set_share(public_url)
        self.output_dir = output_dir
        self.last_used = time.time()
        self.app = create_viewer_app(
            self.log, self.state, output_dir,
            config_summary=self.config_summary, runtime=runtime,
            share=self.share, status=status, max_clients=MAX_VIEWERS_PER_RUN,
            title=f"specimux-suite run {self.run_id}",
        )

    def set_share(self, url: Optional[str]) -> None:
        self.share.clear()
        if url:
            self.share.update({"url": url, "max_clients": MAX_VIEWERS_PER_RUN})

    def _track_config(self, event) -> None:
        if event.type == "pipeline.started" and event.data.get("config_summary"):
            self.config_summary.clear()
            self.config_summary.update(event.data["config_summary"])

    def _track_demux(self, event) -> None:
        if event.type == "specimux.started":
            self.demux = {"processed": 0, "matched": 0, "total_est": 0}
        elif event.type == "specimux.progress":
            self.demux = {k: event.data.get(k) or 0 for k in ("processed", "matched", "total_est")}
        elif event.type == "specimux.completed":
            self.demux = None

    def engine_progress(self) -> dict:
        """Where the engine is, from the events ingested so far: the demux
        in flight, and how many specimens with enough reads for consensus
        (the run's min_reads) have been through consensus and summarized."""
        min_reads = self.config_summary.get("min_reads") or 0
        specimens = list(self.state.specimens.values())   # a copy: ingest applies events meanwhile
        eligible = [s for s in specimens if s.total_reads and s.total_reads >= min_reads]
        past = {"consensus_done", "identified", "no_match", "summarized", "error"}
        return {"demux": dict(self.demux) if self.demux else None,
                "demux_finished": self.state.demux_finished,
                "input_reads": self.state.total_input_reads,
                "matched_reads": self.state.total_matched_reads,
                "specimens": len(eligible),
                "consensus_done": sum(1 for s in eligible if s.status.value in past),
                "summarized": sum(1 for s in eligible if s.status.value == "summarized")}


class RunService:
    def __init__(self, config: ServiceConfig, storage: Storage, queue: CommandQueue,
                 launcher: Launcher, store: Store):
        self.config = config
        self.storage = storage
        self.queue = queue
        self.launcher = launcher
        self.store = store
        self._views: dict[str, RunView] = {}
        self._views_lock = threading.Lock()
        self._load_lock = threading.Lock()
        self._load_cache: Optional[dict] = None
        self._sharing_cache: dict[str, tuple[float, dict]] = {}
        self._upload_cache: dict[str, tuple[float, dict]] = {}
        self._status_cache: dict[str, tuple[float, Optional[dict]]] = {}
        self.config.work_root.mkdir(parents=True, exist_ok=True)

    # --- paths and keys ---

    def run_prefix(self, run: dict) -> str:
        return f"runs/{run['user_id']}/{run['id']}"

    def archive_prefix(self, run: dict) -> str:
        return f"archives/{run['user_id']}/{run['archive_id']}"

    def work_dir(self, run_id: str) -> Path:
        return self.config.work_root / run_id

    def output_dir(self, run_id: str) -> Path:
        return self.work_dir(run_id) / "output"

    # --- job control (C4) ---

    def create_run(self, spec: dict, user_id: str, files: dict[str, bytes],
                   client_token: Optional[str] = None, host_id: str = "",
                   reference_sha256: Optional[str] = None) -> dict:
        """Create a run from a job spec for a host. ``files`` holds the
        input files by role (``primers``, ``specimens``, optional
        ``reference``). A reference is stored once by its SHA-256
        (``reference_key``); a host that knows the service already has it
        sends only ``reference_sha256``. Returns the run record plus
        ``upload_secret`` (shown once) and ``job_code``.
        """
        if not host_id:
            raise ServiceError(400, "a run belongs to a host")
        if not USER_ID.fullmatch(user_id or ""):
            raise ServiceError(400, "user_id must be 1-128 letters, digits and . _ @ + -, "
                                    "starting with a letter or digit")
        if spec.get("archive_id"):
            # running again over an earlier upload: only the same host's
            # archive for the same user (an archive id is not a secret)
            archive = self.store.get_archive(str(spec["archive_id"]))
            if (not archive or archive.get("deleted") or archive.get("host") != host_id
                    or archive.get("user_id") != user_id):
                raise ServiceError(404, "No such archive for this user")
            # only a finished upload: the archive of a cancelled or abandoned
            # run is deleted (clean_up), so no other run may come to need it
            origin = self.store.get_run(archive.get("run") or "")
            if not origin or origin["state"] not in (SEALED, FAILED) or _cancelled(origin):
                raise ServiceError(409, "That archive's run did not finish; it cannot be run again")
            if archive.get("input", "fastq") != spec.get("input", "fastq"):
                raise ServiceError(400, f"archive holds {archive.get('input', 'fastq')} input")
        files = dict(files)
        reference = files.pop("reference", None)
        sha = (reference_sha256 or "").strip().lower() or None
        if sha is not None and not REFERENCE_SHA256.fullmatch(sha):
            raise ServiceError(400, "reference_sha256 must be 64 hex digits")
        if reference is not None:
            actual = hashlib.sha256(reference).hexdigest()
            if sha is not None and sha != actual:
                raise ServiceError(400, f"reference_sha256 does not match the reference file ({actual})")
            sha = actual
        elif sha is not None and not self.host_has_reference(host_id, sha):
            raise ServiceError(409, "Unknown reference: send the reference file with this run")
        if spec.get("name") is not None:
            name = spec["name"]
            if not isinstance(name, str) or not name.strip() or len(name) > RUN_NAME_MAX:
                raise ServiceError(400, f"name must be 1-{RUN_NAME_MAX} characters")
        if spec.get("mode", "batch") not in ("batch", "live"):
            raise ServiceError(400, "mode must be batch or live")
        if spec.get("input", "fastq") not in ("fastq", "pod5"):
            raise ServiceError(400, "input must be fastq or pod5")
        if spec.get("mode") == "live" and spec.get("input") == "pod5":
            raise ServiceError(400, "Live runs take FASTQ; live POD5 (basecalling while sequencing) is not built yet")
        spec = dict(spec)
        if spec.get("name") is not None:
            spec["name"] = spec["name"].strip()
        spec.pop("reference_sha256", None)
        if sha is not None:
            spec["reference_sha256"] = sha
        if spec.get("input", "fastq") == "pod5":
            spec["basecall"] = self._basecall_settings(spec.get("basecall") or {})
        elif spec.get("basecall"):
            raise ServiceError(400, "basecall settings apply to POD5 input only")
        for role in ("primers", "specimens"):
            if role not in files:
                raise ServiceError(400, f"missing input file: {role}")
        run_id = _new_run_id()
        secret = secrets.token_urlsafe(24)
        run = {
            "id": run_id,
            "host": host_id,
            "user_id": user_id,
            "state": CREATED,
            "spec": spec,
            "archive_id": spec.get("archive_id") or "a" + secrets.token_hex(4),
            "secret_hash": _hash_secret(secret),
            "generation": 0,
            "manifest": None,
            "jobs": {},
            "versions": {"suite": suite_version, "cloud": cloud_version},
            "effective_config": None,
            "ingested_files": [],
            "exit": None,
        }
        stored = self.store.create_run(run, client_token=f"{host_id}:{client_token}" if client_token else None)
        if stored["id"] != run_id:
            # a retried create: the existing run, without a secret (shown once)
            return self._public(stored)
        for role, data in files.items():
            self.storage.put(f"{self.run_prefix(run)}/input/{role}", data)
        if reference is not None:
            if self.storage.head(self.reference_key(sha)) is None:
                self.storage.put(self.reference_key(sha), reference)
            self._grant_reference(host_id, sha)
        if not spec.get("archive_id"):
            self.store.put_archive({"id": run["archive_id"], "run": run_id, "host": host_id, "user_id": user_id,
                                    "input": spec.get("input", "fastq"), "created": time.time()})
        out = self._public(stored)
        out["upload_secret"] = secret
        out["job_code"] = f"{run_id}.{secret}"
        return out

    @staticmethod
    def reference_key(sha256: str) -> str:
        """Where a reference database lives: once per content, shared by
        every run that uses it, kept when a run is deleted."""
        return f"references/sha256/{sha256}"

    def reference_info(self, sha256: str, host_id: str) -> dict:
        """Whether this host may name the reference by hash alone: it has
        sent the file before. Another host's reference is a 404 like a
        missing one, so the answer reveals nothing about other hosts."""
        sha = sha256.lower()
        if not REFERENCE_SHA256.fullmatch(sha) or not self.host_has_reference(host_id, sha):
            raise ServiceError(404, "No such reference")
        info = self.storage.head(self.reference_key(sha))
        return {"sha256": sha, "size": info.size}

    # A reference is stored once per content, but a host may use it by
    # hash only after sending the file itself (a reference may be private
    # or licensed): one marker object per host that has sent it.
    @staticmethod
    def _reference_grant_key(host_id: str, sha: str) -> str:
        return f"references/grants/{sha}/{host_id}"

    def _grant_reference(self, host_id: str, sha: str) -> None:
        self.storage.put(self._reference_grant_key(host_id, sha), b"")

    def host_has_reference(self, host_id: str, sha: str) -> bool:
        if self.storage.head(self.reference_key(sha)) is None:
            return False
        if self.storage.head(self._reference_grant_key(host_id, sha)) is not None:
            return True
        # references stored before grants existed: the host's own runs used it
        if any(r["spec"].get("reference_sha256") == sha for r in self.store.list_runs(host=host_id)):
            self._grant_reference(host_id, sha)
            return True
        return False

    def _basecall_settings(self, given: dict) -> dict:
        """The basecalling settings a POD5 run will use, defaults filled in
        so the run record says exactly what ran."""
        if not isinstance(given, dict):
            raise ServiceError(400, "basecall must be an object")
        unknown = set(given) - set(DEFAULT_BASECALL)
        if unknown:
            raise ServiceError(400, f"unknown basecall settings: {', '.join(sorted(unknown))}")
        out = {**DEFAULT_BASECALL, **{k: v for k, v in given.items() if v is not None}}
        out["model"] = str(out["model"] or self.config.dorado_models[0])
        if out["model"] not in self.config.dorado_models:
            raise ServiceError(400, f"basecall.model must be one of {', '.join(self.config.dorado_models)}")
        for key in ("min_length", "max_length"):
            try:
                out[key] = int(out[key])
            except (TypeError, ValueError):
                raise ServiceError(400, f"basecall.{key} must be an integer")
            if out[key] < 0:
                raise ServiceError(400, f"basecall.{key} must not be negative")
        if out["max_length"] and out["max_length"] < out["min_length"]:
            raise ServiceError(400, "basecall.max_length is below min_length")
        if out["min_qscore"] is not None:
            try:
                out["min_qscore"] = float(out["min_qscore"])
            except (TypeError, ValueError):
                raise ServiceError(400, "basecall.min_qscore must be a number")
        return out

    @staticmethod
    def uploads_open(run: dict) -> bool:
        """Whether the run takes uploads: until ``complete`` for a batch run;
        a live run keeps taking them while its engine runs, until its
        upload is completed (the manifest is fixed then)."""
        if run["state"] in (CREATED, UPLOADING):
            return True
        return (run["spec"].get("mode") == "live" and run["state"] in (RUNNING, FINALIZING)
                and run.get("manifest") is None)

    def _public(self, run: dict) -> dict:
        d = {k: v for k, v in run.items() if k not in ("secret_hash", "job_secret", "stages")}
        d["stages"] = {name: {k: v for k, v in rec.items() if k != "job_secret"}
                       for name, rec in (run.get("stages") or {}).items()}
        d["pending_commands"] = [c["id"] for c in self.store.list_commands(run["id"], pending_only=True)]
        d["uploads_open"] = self.uploads_open(run)
        if d["uploads_open"]:
            d["upload"] = self._upload_received(run)
        pub = run.get("public") or {}
        d.pop("public", None)
        d["public"] = {"enabled": bool(pub.get("enabled")), "allow_starring": pub.get("allow_starring", True),
                       "allow_downloads": bool(pub.get("allow_downloads")), "url": self.public_url(run)}
        d["dashboard_url"] = self.dashboard_url(run["id"])
        if run["state"] in (RUNNING, FINALIZING):
            # from the view ingest keeps (never built here just for this)
            with self._views_lock:
                view = self._views.get(run["id"])
            if view is not None and view.generation == engine_generation(run):
                d["engine_progress"] = view.engine_progress()
        return d

    def dashboard_url(self, run_id: str) -> str:
        return f"{self.config.base_url}/v1/runs/{run_id}/"

    def get_run(self, run_id: str, host_id: Optional[str] = None) -> dict:
        """The run record; with ``host_id``, only if that host owns it (a
        run another host created does not exist as far as this one can
        tell)."""
        run = self.store.get_run(run_id)
        if run is None or (host_id is not None and run.get("host") != host_id):
            raise ServiceError(404, "No such run")
        return run

    def status(self, run_id: str, host_id: Optional[str] = None) -> dict:
        return self._public(self.get_run(run_id, host_id))

    def load(self, max_age_s: float = LOAD_CACHE_S) -> dict:
        """How busy the service is, across every host, in counts only (no
        run ids, hosts or users): per stage, its slots, the runs holding
        them, how many of those are still waiting for a machine, and the
        runs queued for it. Cached briefly: it scans every run and asks
        Batch about each active job."""
        with self._load_lock:
            if self._load_cache and time.time() - self._load_cache["as_of"] < max_age_s:
                return self._load_cache
            active = self.store.list_runs(states=[UPLOADING, INPUT_COMPLETE, BASECALLING, RUNNING,
                                                  FINALIZING, SEALING])
            stages = {}
            for stage in (DORADO_STAGE, ENGINE_STAGE):
                busy = [r for r in active if stage_of(r, stage).get("active")]
                waiting_for_machine = 0
                for run in busy:
                    job = next((j for st, _, j in self.active_jobs(run) if st == stage), None)
                    if job:
                        try:
                            if self.launcher.describe(job["id"]).state == "pending":
                                waiting_for_machine += 1
                        except Exception:
                            logger.warning(f"Could not describe {job['id']} for the load view", exc_info=True)
                stages[stage] = {
                    "slots": self.config.slots(stage),
                    "busy": len(busy),
                    "waiting_for_machine": waiting_for_machine,
                    "queued": sum(1 for r in active if r["state"] == INPUT_COMPLETE and self.next_stage(r) == stage),
                }
            self._load_cache = {
                "as_of": time.time(),
                "uploading": sum(1 for r in active if r["state"] == UPLOADING),
                "stages": stages,
                "sealing": sum(1 for r in active if r["state"] == SEALING),
            }
            return self._load_cache

    def list_runs(self, host_id: str, user_id: Optional[str] = None) -> list[dict]:
        return [self._public(r) for r in self.store.list_runs(user_id=user_id, host=host_id)]

    def regenerate_job_code(self, run_id: str, host_id: str) -> dict:
        """A new upload secret; the old one stops working at once. Only
        while uploads are open."""
        run = self.get_run(run_id, host_id)
        if not self.uploads_open(run):
            raise ServiceError(409, f"Uploads are closed: run is {run['state']}")
        secret = secrets.token_urlsafe(24)
        run = self.store.update_run(run_id, {"secret_hash": _hash_secret(secret)},
                                    expected_state=[CREATED, UPLOADING, RUNNING, FINALIZING])
        out = self._public(run)
        out["upload_secret"] = secret
        out["job_code"] = f"{run_id}.{secret}"
        return out

    def delete_run(self, run_id: str, host_id: Optional[str] = None) -> None:
        run = self.get_run(run_id, host_id)
        if run["state"] in (BASECALLING, RUNNING, FINALIZING, SEALING):
            raise ServiceError(409, "Run is active; cancel it first")
        # nothing is running: the run's own objects go, and so does the
        # upload archive it made if it never processed it (not started,
        # abandoned, or cancelled: clean_up would delete that archive
        # anyway); a finished upload's archive stays, as archives do
        self.storage.delete_prefix(self.run_prefix(run))
        unprocessed = (run["state"] in (CREATED, UPLOADING, INPUT_COMPLETE, INCOMPLETE)
                       or (run["state"] == FAILED and _cancelled(run)))
        if unprocessed and not run["spec"].get("archive_id"):
            self._delete_archive(run)
        shutil.rmtree(self.work_dir(run_id), ignore_errors=True)
        with self._views_lock:
            self._views.pop(run_id, None)
        self.store.delete_run(run_id)

    def cancel_run(self, run_id: str, host_id: Optional[str] = None, reason: str = "",
                   actor: str = "") -> dict:
        """Stop a run. A run waiting for input or for a stage fails at once;
        an active stage's job is stopped (Batch sends SIGTERM) and its
        wrapper reports the exit, which fails the run through the usual
        path (reconcile covers a report that never comes)."""
        run = self.get_run(run_id, host_id)
        why = f"cancelled{f' by {actor}' if actor else ''}{f': {reason}' if reason else ''}"
        state = run["state"]
        if state in (CREATED, UPLOADING, INPUT_COMPLETE):
            try:
                run = self.store.update_run(run_id, {
                    "state": FAILED, "exit": {"code": None, "reason": why, "reported": time.time()},
                    "cancel": {"requested": time.time(), "reason": why},
                    "sealed": {"error": "cancelled"}},
                    expected_state=[CREATED, UPLOADING, INPUT_COMPLETE])
            except ConflictError:
                return self.cancel_run(run_id, host_id, reason, actor)  # it moved on: judge again
            logger.info(f"Run {run_id} {why} ({state})")
            return self._public(run)
        if state in (BASECALLING, RUNNING, FINALIZING):
            jobs = self.active_jobs(run)
            if not jobs:
                raise ServiceError(409, "The run's job is still being launched; try again in a minute")
            run = self.store.update_run(run_id, {"cancel": {"requested": time.time(), "reason": why}})
            for _, _, job in jobs:
                self.launcher.cancel(job["id"], why)
            logger.info(f"Run {run_id} {why}: stopping {', '.join(j['id'] for _, _, j in jobs)}")
            return self._public(run)
        raise ServiceError(409, f"Run is {state}; nothing to cancel")

    def retry_run(self, run_id: str, host_id: Optional[str] = None) -> dict:
        """Run a failed run's failed stage again. Basecalling keeps the
        files already delivered and skips them; the engine starts over at
        the next generation (it keeps nothing between attempts: its work
        is on the job's local disk)."""
        run = self.get_run(run_id, host_id)
        if run["state"] != FAILED:
            raise ServiceError(409, f"Run is {run['state']}; only a failed run can be retried")
        if not run.get("manifest"):
            raise ServiceError(409, "The run's upload was never completed; there is no fixed input to run again")
        stage = (run.get("exit") or {}).get("stage") or ENGINE_STAGE

        def retried(cur: dict) -> dict:
            # judged inside the write: clean_up may be deleting the archive
            if cur.get("archive_deleted"):
                raise ServiceError(409, "The run's uploaded input has been deleted; start a new run")
            return {"state": INPUT_COMPLETE, "exit": None, "sealed": None, "cancel": None}
        run = self.store.update_run(run_id, retried, expected_state=[FAILED])
        logger.info(f"Run {run_id} retried: {'basecalling' if stage == DORADO_STAGE else 'the engine'} again")
        return self._public(self._try_launch(run_id) or run)

    def options(self) -> dict:
        from specimux_suite.profiles import list_profiles
        return {"profiles": list_profiles(),
                "modes": ["batch", "live"], "inputs": ["fastq", "pod5"],
                "dorado_models": list(self.config.dorado_models), "basecall_defaults": dict(DEFAULT_BASECALL),
                "versions": {"suite": suite_version, "cloud": cloud_version}}

    # --- authorization helpers ---

    def check_job_code(self, run_id: str, secret: str, open_only: bool = True) -> dict:
        run = self.get_run(run_id)
        if not secret or _hash_secret(secret) != run["secret_hash"]:
            raise ServiceError(403, "Bad job code")
        if open_only and not self.uploads_open(run):
            raise ServiceError(409, f"Uploads are closed: run is {run['state']}")
        return run

    def job_identity(self, run_id: str, secret: str) -> tuple[dict, str]:
        """The run and the stage whose current job holds this secret. A
        job's secret names its stage and generation; a superseded job's
        secret matches nothing and is refused."""
        run = self.get_run(run_id)
        for stage, rec in (run.get("stages") or {}).items():
            held = rec.get("job_secret")
            if secret and held and secrets.compare_digest(secret, held):
                return run, stage
        raise ServiceError(403, "Bad job secret")

    def check_stage_generation(self, run_id: str, stage: str, generation: int) -> dict:
        """The run, if ``generation`` is still that stage's current job; a
        report from an older job is stale (409), which is final for it."""
        run = self.get_run(run_id)
        current = stage_of(run, stage).get("generation")
        if current is None or int(generation) != int(current):
            raise ServiceError(409, f"Stale {stage} generation {generation}; current is {current}")
        return run

    # --- hosts and their keys ---

    def add_host(self, host_id: str, name: str = "", authorize_url: Optional[str] = None,
                 theme: Optional[dict] = None, label: str = "default") -> tuple[dict, str]:
        """Register a host and its first key. Returns the record and the
        key, which is shown once and never stored."""
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,31}", host_id):
            raise ServiceError(400, "host id: lowercase letters, digits and dashes, 2-32 characters")
        if self.store.get_host(host_id):
            raise ServiceError(409, f"host {host_id} exists")
        host = {"id": host_id, "name": name or host_id, "authorize_url": authorize_url,
                "theme": theme or {}, "disabled": False, "keys": [], "created": time.time(),
                "token_ttl_max_s": auth.MAX_TOKEN_TTL_S}
        self.store.put_host(host)
        key = self.add_key(host_id, label)
        return self.store.get_host(host_id), key

    def update_host(self, host_id: str, **fields) -> dict:
        host = self.get_host(host_id)
        for k, v in fields.items():
            if k in ("name", "authorize_url", "theme", "disabled", "token_ttl_max_s") and v is not None:
                host[k] = v
        self.store.put_host(host)
        return host

    def get_host(self, host_id: str) -> dict:
        host = self.store.get_host(host_id)
        if host is None:
            raise ServiceError(404, f"No such host: {host_id}")
        return host

    def list_hosts(self) -> list[dict]:
        return [self._public_host(h) for h in self.store.list_hosts()]

    @staticmethod
    def _public_host(host: dict) -> dict:
        h = {k: v for k, v in host.items() if k != "keys"}
        h["keys"] = [{k: v for k, v in key.items() if k != "hash"} for key in host.get("keys", [])]
        return h

    def add_key(self, host_id: str, label: str) -> str:
        """A new key under a label (a host may hold several: its server,
        a person at the console). Shown once."""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", label):
            raise ServiceError(400, "key label: letters, digits, dot, dash, underscore")
        host = self.get_host(host_id)
        key = auth.new_service_key(host_id)
        host["keys"].append({"label": label, "hash": auth.hash_key(key), "created": time.time(),
                             "expires": None})
        self.store.put_host(host)
        return key

    def rotate_key(self, host_id: str, label: str, grace_s: float = 24 * 3600) -> str:
        """Replace the keys under a label: a new one now, the old ones
        valid for ``grace_s`` more so the host can roll its config."""
        host = self.get_host(host_id)
        found = False
        for key in host["keys"]:
            if key["label"] == label and not key.get("expires"):
                key["expires"] = time.time() + grace_s
                found = True
        if not found:
            raise ServiceError(404, f"host {host_id} has no live key labelled {label}")
        self.store.put_host(host)
        return self.add_key(host_id, label)

    def revoke_key(self, host_id: str, label: str) -> int:
        host = self.get_host(host_id)
        before = len(host["keys"])
        host["keys"] = [k for k in host["keys"] if k["label"] != label]
        self.store.put_host(host)
        return before - len(host["keys"])

    def authenticate_key(self, presented: str) -> dict:
        """The host record and the label of the key in use, for a presented
        service key; 403 for anything else."""
        host_id, secret = auth.split_key(presented)
        host = self.store.get_host(host_id) if host_id else None
        if host is None:
            raise ServiceError(403, "Service key required")
        digest = auth.hash_key(presented)
        now = time.time()
        for key in host.get("keys", []):
            if secrets.compare_digest(key["hash"], digest) and (not key.get("expires") or key["expires"] > now):
                if host.get("disabled"):
                    raise ServiceError(403, "Host is disabled")
                return {"host": host, "label": key["label"]}
        raise ServiceError(403, "Service key required")

    # --- run tokens and sessions (the browser) ---

    def mint_token(self, run_id: str, host_id: str, user: str, scope: str = "view",
                   ttl_s: Optional[float] = None, label: Optional[str] = None) -> dict:
        """A run token for a host's authorize route to hand to a browser."""
        run = self.get_run(run_id, host_id)
        host = self.get_host(host_id)
        if scope not in auth.SCOPES:
            raise ServiceError(400, f"scope must be one of {', '.join(auth.SCOPES)}")
        cap = float(host.get("token_ttl_max_s") or auth.MAX_TOKEN_TTL_S)
        ttl = float(ttl_s) if ttl_s else auth.DEFAULT_TOKEN_TTL_S
        if ttl <= 0 or ttl > cap:
            raise ServiceError(400, f"ttl_seconds must be between 1 and {int(cap)}")
        if not user:
            raise ServiceError(400, "user required")
        token, payload = auth.mint(auth.TOKEN, self.config.session_secret, host=host_id, user=str(user),
                                   run=run["id"], scope=scope, ttl_s=ttl, label=label)
        return {"token": token, "expires_in": int(ttl), "scope": scope,
                "dashboard_url": self.dashboard_url(run["id"])}

    def open_session(self, token: str) -> dict:
        """Exchange a run token for a session (the cookie's value and its
        life); the run and its host must still be there and enabled. A
        share token (the owner's public link) opens a public session."""
        if (token or "").startswith(auth.SHARE_TOKEN_PREFIX):
            return self._open_public_session(token)
        payload = auth.verify(token or "", self.config.session_secret, auth.TOKEN)
        if payload is None:
            raise ServiceError(401, "Bad or expired run token")
        run = self.store.get_run(payload["run"])
        if run is None or run.get("host") != payload["host"]:
            raise ServiceError(401, "The run is gone")
        host = self.store.get_host(payload["host"])
        if host is None or host.get("disabled"):
            raise ServiceError(401, "The host is disabled")
        ttl = self.config.session_ttl_s
        session, _ = auth.mint(auth.SESSION, self.config.session_secret, host=payload["host"],
                               user=payload["user"], run=payload["run"], scope=payload["scope"],
                               ttl_s=ttl, label=payload.get("label"))
        return {"session": session, "run": payload["run"], "scope": payload["scope"],
                "user": payload["user"], "host": payload["host"], "expires_in": int(ttl)}

    def check_session(self, run_id: str, cookie: Optional[str]) -> dict:
        """The session payload for a run, or 401. A public session is good
        only while the run is still shared under the same link."""
        payload = auth.verify(cookie or "", self.config.session_secret, auth.SESSION)
        if payload is None or payload.get("run") != run_id:
            raise ServiceError(401, "A run session is required")
        if payload.get("scope") == auth.PUBLIC_SCOPE:
            sharing = self.sharing(run_id)
            if not sharing.get("enabled") or sharing.get("generation") != payload.get("share"):
                raise ServiceError(401, "This run is no longer shared publicly")
        return payload

    # --- public viewing (an owner's "anyone with the link" share) ---

    def sharing(self, run_id: str) -> dict:
        """The run's sharing settings, cached for ``PUBLIC_CHECK_S`` (every
        public viewer's request checks them)."""
        now = time.time()
        with self._views_lock:
            hit = self._sharing_cache.get(run_id)
            if hit and now - hit[0] < PUBLIC_CHECK_S:
                return hit[1]
        run = self.store.get_run(run_id)
        value = dict((run or {}).get("public") or {})
        with self._views_lock:
            self._sharing_cache[run_id] = (now, value)
        return value

    def public_url(self, run: dict) -> Optional[str]:
        pub = run.get("public") or {}
        if not pub.get("enabled"):
            return None
        token = f"{auth.SHARE_TOKEN_PREFIX}{run['id']}.{pub['secret']}"
        return f"{self.dashboard_url(run['id'])}#token={token}"

    def set_public(self, run_id: str, host_id: str, enabled: Optional[bool] = None,
                   allow_starring: Optional[bool] = None, allow_downloads: Optional[bool] = None,
                   new_link: bool = False) -> dict:
        """The owner's switch for public viewing: an unguessable link that
        opens a read-only dashboard session for anyone who has it (viewers
        may star specimens unless ``allow_starring`` is off; downloads only
        if ``allow_downloads``). Turning it off, or a new link, ends every
        public session at its next request."""
        self.get_run(run_id, host_id)

        def change(cur: dict) -> dict:
            pub = {"enabled": False, "allow_starring": True, "allow_downloads": False, "generation": 0,
                   **(cur.get("public") or {})}
            if enabled is not None:
                pub["enabled"] = bool(enabled)
            if allow_starring is not None:
                pub["allow_starring"] = bool(allow_starring)
            if allow_downloads is not None:
                pub["allow_downloads"] = bool(allow_downloads)
            if pub["enabled"] and (new_link or not pub.get("secret")):
                pub["secret"] = secrets.token_urlsafe(18)
                pub["generation"] = int(pub.get("generation") or 0) + 1
            pub["updated"] = time.time()
            return {"public": pub}
        run = self.store.update_run(run_id, change)
        with self._views_lock:
            self._sharing_cache.pop(run_id, None)
            view = self._views.get(run_id)
        if view is not None:
            view.set_share(self.public_url(run))
        # (never the link itself: it carries the share token)
        logger.info(f"Run {run_id} public viewing: {'on' if (run.get('public') or {}).get('enabled') else 'off'}")
        return self._public(run)["public"]

    def _open_public_session(self, token: str) -> dict:
        _, _, rest = token.partition(auth.SHARE_TOKEN_PREFIX)
        run_id, _, secret = rest.partition(".")
        run = self.store.get_run(run_id) if run_id else None
        pub = (run or {}).get("public") or {}
        if not pub.get("enabled") or not secret or not secrets.compare_digest(secret, pub.get("secret") or ""):
            raise ServiceError(401, "This link does not open a public view (sharing is off, or a new link replaced it)")
        host = self.store.get_host(run.get("host") or "")
        if host is None or host.get("disabled"):
            raise ServiceError(401, "The host is disabled")
        ttl = auth.PUBLIC_SESSION_TTL_S
        session, _ = auth.mint(auth.SESSION, self.config.session_secret, host=run["host"], user="public",
                               run=run_id, scope=auth.PUBLIC_SCOPE, ttl_s=ttl,
                               extra={"share": pub["generation"]})
        return {"session": session, "run": run_id, "scope": auth.PUBLIC_SCOPE, "user": "public",
                "host": run["host"], "expires_in": int(ttl)}

    @staticmethod
    def actor_of(session: dict) -> str:
        return f"{session['host']}:{session['user']}"

    # --- uploads (C5) ---

    def presign_uploads(self, run_id: str, names: list[str]) -> dict:
        run = self.get_run(run_id)
        kind = run["spec"].get("input", "fastq")
        urls = {}
        for name in names:
            if "/" in name or name.startswith(".") or not name:
                raise ServiceError(400, f"Invalid file name: {name!r}")
            key = f"{self.archive_prefix(run)}/{kind}/{name}"
            urls[name] = {"key": key, "url": self.storage.presign_put(key)}
        # the idle clock (expire_idle_uploads) runs from the last request
        def touch(cur: dict) -> dict:
            if not self.uploads_open(cur):
                raise ConflictError(f"run {run_id} is {cur['state']}")
            return {"last_upload": time.time(), **({"state": UPLOADING} if cur["state"] == CREATED else {})}
        try:
            run = self.store.update_run(run_id, touch)
        except ConflictError:
            raise ServiceError(409, f"Uploads are closed: run is {self.get_run(run_id)['state']}")
        self._forget_upload_listing(run_id)
        # A live run's engine starts with its first upload request (or when
        # an engine slot frees); a batch run's on complete
        if run["spec"].get("mode") == "live" and run["state"] == UPLOADING \
                and not stage_of(run, ENGINE_STAGE).get("active"):
            self._try_launch(run_id)
        return {"run_id": run_id, "uploads": urls}

    def _upload_received(self, run: dict) -> dict:
        """What has arrived so far: whole files in the archive (a file shows
        once its upload finishes), and the uploader's last report on the
        file in flight (uploaders from 0.1.1 send one)."""
        now = time.time()
        with self._views_lock:
            hit = self._upload_cache.get(run["id"])
        if hit and now - hit[0] < UPLOAD_LIST_CACHE_S:
            received = hit[1]
        else:
            prefix = f"{self.archive_prefix(run)}/{run['spec'].get('input', 'fastq')}/"
            objs = self.storage.list(prefix)
            received = {"files": len(objs), "bytes": sum(o.size for o in objs)}
            with self._views_lock:
                self._upload_cache[run["id"]] = (now, received)
        out = dict(received)
        if run.get("upload_progress"):
            out["progress"] = dict(run["upload_progress"])
        return out

    def record_upload_progress(self, run_id: str, report: dict) -> dict:
        """The uploader's progress on the file in flight, shown on the run
        page. A report is upload activity (the idle clock restarts), since a
        large file can take a while to send."""
        def num(key):
            v = report.get(key)
            if v is None:
                return None
            if not isinstance(v, (int, float)) or v < 0:
                raise ServiceError(400, f"{key} must be a non-negative number")
            return v
        name = str(report.get("file") or "")[:255]
        progress = {"file": name, "sent": num("sent"), "size": num("size"), "rate": num("rate"),
                    "files_done": num("files_done"), "bytes_done": num("bytes_done"),
                    "files_total": num("files_total"), "bytes_total": num("bytes_total"), "at": time.time()}

        def touch(cur: dict) -> dict:
            if not self.uploads_open(cur):
                raise ConflictError(f"run {run_id} is {cur['state']}")
            return {"upload_progress": progress, "last_upload": time.time()}
        try:
            self.store.update_run(run_id, touch)
        except ConflictError:
            raise ServiceError(409, f"Uploads are closed: run is {self.get_run(run_id)['state']}")
        self._forget_upload_listing(run_id)
        return {"ok": True}

    def _forget_upload_listing(self, run_id: str) -> None:
        # the uploader moved on (a file may have finished): list again
        with self._views_lock:
            self._upload_cache.pop(run_id, None)

    def complete(self, run_id: str, manifest: Optional[list[dict]] = None) -> dict:
        """No more input. The manifest (key, size, etag per object) fixes
        what belongs to the run; missing, it is built from the listing."""
        run = self.get_run(run_id)
        if not self.uploads_open(run):
            raise ServiceError(409, f"Run is {run['state']}")
        kind = run["spec"].get("input", "fastq")
        prefix = f"{self.archive_prefix(run)}/{kind}/"
        listed = {o.key: o for o in self.storage.list(prefix)}
        if manifest is None:
            entries = [{"key": o.key, "size": o.size, "etag": o.etag} for o in listed.values()]
        else:
            entries = []
            for m in manifest:
                o = listed.get(m.get("key"))
                if o is None:
                    raise ServiceError(409, f"Manifest lists a missing object: {m.get('key')}")
                if m.get("etag") and m["etag"] != o.etag:
                    raise ServiceError(409, f"Object changed since upload: {m.get('key')}")
                entries.append({"key": o.key, "size": o.size, "etag": o.etag})
        if not entries:
            raise ServiceError(409, "No input files were uploaded")
        if run["state"] in (RUNNING, FINALIZING):
            # a live run whose engine is running: the manifest closes the
            # upload; the engine takes the last files, then finalizes
            run = self.store.update_run(run_id, {"manifest": entries, "input_completed": time.time()},
                                        expected_state=[RUNNING, FINALIZING])
            return self._public(run)
        run = self.store.update_run(run_id, {"state": INPUT_COMPLETE, "manifest": entries,
                                             "input_completed": time.time()},
                                    expected_state=[CREATED, UPLOADING])
        # The next stage starts now (dorado for POD5, the engine for FASTQ;
        # a live run whose engine was waiting for a slot runs it over the
        # whole upload), or as soon as that stage is free — the run waits
        # in input_complete, and a freed slot goes to the oldest waiting run.
        run = self._try_launch(run_id) or run
        return self._public(run)

    @staticmethod
    def next_stage(run: dict) -> str:
        """The stage an input_complete run is waiting for: dorado until its
        POD5 files are basecalled, then the engine."""
        if run["spec"].get("input", "fastq") == "pod5" and run.get("basecalled") is None:
            return DORADO_STAGE
        return ENGINE_STAGE

    def _try_launch(self, run_id: str) -> Optional[dict]:
        run = self.get_run(run_id)
        try:
            if self.next_stage(run) == DORADO_STAGE:
                return self.launch_dorado(run_id)
            return self.launch_engine(run_id)
        except ServiceError as e:
            if e.status == 409:
                logger.info(f"Run {run_id} queued: {e.message}")
                return None
            raise

    def launch_next_queued(self) -> Optional[dict]:
        """After a stage is released: launch the oldest run waiting for it
        (each stage on its own)."""
        launched = None
        for stage in (DORADO_STAGE, ENGINE_STAGE):
            free = self.config.slots(stage) - len(self.store.stage_holders(stage))
            if free <= 0:
                continue
            waiting = [r for r in self.store.list_runs(states=[INPUT_COMPLETE]) if self.next_stage(r) == stage]
            if stage == ENGINE_STAGE:
                # a live run that has started uploading and is waiting for its engine
                waiting += [r for r in self.store.list_runs(states=[UPLOADING])
                            if r["spec"].get("mode") == "live" and r.get("last_upload")
                            and not stage_of(r, ENGINE_STAGE).get("active")]
            waiting.sort(key=lambda r: r.get("created") or 0)
            for run in waiting:
                if free <= 0:
                    break
                got = self._try_launch(run["id"])
                if got:
                    launched = launched or got
                    free -= 1
        return launched

    # --- launching ---

    def launch_engine(self, run_id: str) -> dict:
        """Launch the engine job for the run's next generation, in a free
        engine slot, as an intent first."""
        run = self.get_run(run_id)
        vcpus = int(run["spec"].get("vcpus") or DEFAULT_VCPUS)
        live = run["spec"].get("mode") == "live"
        return self._launch(run, ENGINE_STAGE, RUNNING,
                            expected=[INPUT_COMPLETE, RUNNING, FAILED, INCOMPLETE, BASECALLING, UPLOADING],
                            env={"SPECIMUX_WORK_DIR": str(self.work_dir(run_id)), "SPECIMUX_VCPUS": str(vcpus)},
                            args=list(self.config.engine_extra_args), vcpus=vcpus,
                            memory_mib=vcpus * MEMORY_MIB_PER_VCPU,
                            timeout_s=LIVE_ENGINE_TIMEOUT_S if live else None)

    def launch_dorado(self, run_id: str) -> dict:
        """Launch the basecalling job for a POD5 run's next generation, in a
        free dorado slot, as an intent first."""
        run = self.get_run(run_id)
        if run["spec"].get("input", "fastq") != "pod5":
            raise ServiceError(409, "Only a POD5 run is basecalled")
        # a relaunch keeps what earlier attempts delivered; the job skips those files
        return self._launch(run, DORADO_STAGE, BASECALLING,
                            expected=[INPUT_COMPLETE, BASECALLING, FAILED, INCOMPLETE],
                            updates={"basecalling": self._carried_progress(run)})

    def _launch(self, run: dict, stage: str, state: str, expected: list, env: Optional[dict] = None,
                updates: Optional[dict] = None, args: Optional[list] = None, vcpus: Optional[int] = None,
                memory_mib: Optional[int] = None, timeout_s: Optional[int] = None) -> dict:
        """Claim a slot of ``stage``, give the stage a new job identity (the
        run's next generation and a fresh secret), and submit the job under
        its deterministic name, as an intent first. A refused submission
        puts the run and the stage back as they were."""
        run_id = run["id"]
        if not self.store.reserve_stage(stage, run_id, self.config.slots(stage)):
            holders = ", ".join(self.store.stage_holders(stage))
            raise ServiceError(409, f"{stage.capitalize()} stage is busy with runs {holders}")
        generation = int(run.get("generation") or 0) + 1
        name = f"{run_id}-{stage}-{generation}"
        job_secret = secrets.token_urlsafe(24)
        prior_state, prior_record = run["state"], stage_of(run, stage)
        record = {"generation": generation, "job_secret": job_secret, "active": True, "launched": time.time()}
        intent = self.store.open_intent(run_id, f"launch-{stage}", {"generation": generation, "name": name})
        run = self.store.update_run(run_id, lambda cur: {
            "generation": generation, "state": state, **(updates or {}),
            "stages": {**(cur.get("stages") or {}), stage: record}}, expected_state=expected)
        spec = JobSpec(name=name, kind=stage, run_id=run_id, generation=generation, env={
            "SPECIMUX_RUN_ID": run_id,
            "SPECIMUX_RUN_API": self.config.engine_api_url,
            "SPECIMUX_JOB_SECRET": job_secret,
            "SPECIMUX_GENERATION": str(generation),
            **(env or {}),
        }, args=list(args or []), vcpus=vcpus, memory_mib=memory_mib, timeout_s=timeout_s)
        try:
            handle = self.launcher.find_by_name(name) or self.launcher.submit(spec)
        except Exception as e:
            # The launcher refused (permissions, quota, a bad definition):
            # put the run back where it was so a later attempt can proceed
            logger.exception(f"Launch of {name} failed")
            self.store.resolve_intent(intent, {"error": str(e)})
            self.store.update_run(run_id, lambda cur: {
                "generation": generation - 1,
                "state": INPUT_COMPLETE if prior_state in (RUNNING, BASECALLING) else prior_state,
                "stages": {**(cur.get("stages") or {}), stage: prior_record}})
            self.store.release_stage(stage, run_id)
            raise ServiceError(502, f"Could not launch the {stage} job: {e}")
        self.store.resolve_intent(intent, {"job_id": handle.id})
        job = {"id": handle.id, "kind": stage, "generation": generation, "submitted": time.time()}
        run = self.store.update_run(run_id, lambda cur: {"jobs": {**(cur.get("jobs") or {}), name: job}})
        logger.info(f"Launched {name} as job {handle.id}")
        return run

    def _end_stage(self, run_id: str, stage: str, generation: int) -> None:
        """The stage's job is over: it holds no slot and is no longer
        active. Its secret stays, so a retried exit report is recognised."""
        def ended(cur: dict) -> dict:
            rec = stage_of(cur, stage)
            if rec.get("generation") != generation:
                return {}
            return {"stages": {**(cur.get("stages") or {}), stage: {**rec, "active": False}}}
        self.store.update_run(run_id, ended)
        self.store.release_stage(stage, run_id)

    def _carried_progress(self, run: dict) -> dict:
        files = [f for f in (run.get("basecalling") or {}).get("files", [])
                 if self.storage.head(f["key"]) is not None]
        return {"files": files, "done": len(files), "total": len(run.get("manifest") or []),
                "reads_in": sum(f.get("reads_in", 0) for f in files),
                "reads_out": sum(f.get("reads_out", 0) for f in files)}

    def basecalled_key(self, run: dict, pod5_key: str) -> str:
        """Where the FASTQ basecalled from an archive's POD5 object lives:
        under the run (regenerable), named after the POD5 file."""
        name = pod5_key.rsplit("/", 1)[-1]
        stem = name[:-5] if name.lower().endswith(".pod5") else name
        return f"{self.run_prefix(run)}/fastq/{stem}.fastq"

    # --- what the engine asks for (C2) ---

    def job_bundle(self, run_id: str, stage: Optional[str] = None) -> dict:
        """Everything the wrapper needs to run: the spec, presigned inputs,
        the archive objects of the manifest, and where to report. The
        generation is the calling job's stage's."""
        run = self.get_run(run_id)
        prefix = self.run_prefix(run)
        inputs = {}
        for o in self.storage.list(f"{prefix}/input/"):
            role = o.key.rsplit("/", 1)[-1]
            inputs[role] = self.storage.presign_get(o.key)
        if run["spec"].get("reference_sha256"):
            inputs["reference"] = self.storage.presign_get(self.reference_key(run["spec"]["reference_sha256"]))
        def entries(items):
            return [{"key": e["key"], "name": e["key"].rsplit("/", 1)[-1], "etag": e["etag"], "size": e["size"],
                     "url": self.storage.presign_get(e["key"])} for e in items]
        generation = stage_of(run, stage).get("generation") if stage else run.get("generation")
        bundle = {"run_id": run_id, "generation": generation, "stage": stage, "spec": run["spec"], "inputs": inputs,
                  "ingest_url": f"{self.config.engine_api_url}/v1/runs/{run_id}/ingest",
                  "commands_url": f"{self.config.engine_api_url}/v1/runs/{run_id}/commands",
                  "exit_url": f"{self.config.engine_api_url}/v1/runs/{run_id}/exit"}
        if run["spec"].get("input", "fastq") == "pod5":
            # The dorado job's side: the POD5 files, where each FASTQ goes
            # (a presigned PUT; long enough for a slow file), and what an
            # earlier attempt already delivered. The engine's side: the
            # basecalled FASTQs as its reads, once they all exist.
            done = {f["name"] for f in (run.get("basecalling") or {}).get("files", [])}
            done |= {b["key"].rsplit("/", 1)[-1] for b in run.get("basecalled") or []}
            bundle["pod5"] = entries(run.get("manifest") or [])
            bundle["basecall"] = run["spec"].get("basecall") or {}
            targets = [self.basecalled_key(run, e["key"]) for e in (run.get("manifest") or [])]
            bundle["fastq_uploads"] = {
                key.rsplit("/", 1)[-1]: {"key": key, "url": self.storage.presign_put(key, expires_s=8 * 3600)}
                for key in targets}
            bundle["basecalled"] = sorted(done)
            bundle["basecalled_url"] = f"{self.config.engine_api_url}/v1/runs/{run_id}/basecalled"
            bundle["reads"] = entries(run.get("basecalled") or [])
        else:
            bundle["reads"] = entries(run.get("manifest") or [])
        return bundle

    def live_inputs(self, run_id: str, have: list[str]) -> dict:
        """What a live engine job should deliver to its watch dir: the
        uploaded files it does not have yet (presigned), whether the upload
        is complete (then only the manifest's files count), every file of
        the run so far, and the files the engine has already demultiplexed
        (its ``specimux.completed`` events, through ingest) — the job
        finalizes once the upload is complete and every file is among them.
        The storage listing is the truth, as for batch."""
        run = self.get_run(run_id)
        prefix = f"{self.archive_prefix(run)}/{run['spec'].get('input', 'fastq')}/"
        objects = {o.key.rsplit("/", 1)[-1]: o for o in self.storage.list(prefix)}
        manifest = run.get("manifest")
        if manifest is not None:
            listed = {m["key"].rsplit("/", 1)[-1]: m for m in manifest}
            objects = {n: o for n, o in objects.items() if n in listed and o.etag == listed[n]["etag"]}
        have = set(have or [])
        new = [{"name": n, "key": o.key, "size": o.size, "etag": o.etag, "url": self.storage.presign_get(o.key)}
               for n, o in sorted(objects.items()) if n not in have]
        return {"files": new, "names": sorted(objects), "complete": manifest is not None,
                "ingested": list(run.get("ingested_files") or [])}

    def record_basecalled(self, run_id: str, generation: int, name: str, key: str, reads_in: int = 0,
                          reads_out: int = 0) -> dict:
        """The dorado job delivered one FASTQ (already PUT to its key):
        progress on the run record, and the file's identity for the engine.
        A repeated report for the same file replaces the earlier one."""
        run = self.check_stage_generation(run_id, DORADO_STAGE, generation)
        if not stage_of(run, DORADO_STAGE).get("active"):
            raise ServiceError(409, f"Run is {run['state']}; not basecalling")
        expected = {self.basecalled_key(run, e["key"]).rsplit("/", 1)[-1]: e for e in run.get("manifest") or []}
        if name not in expected or key != self.basecalled_key(run, expected[name]["key"]):
            raise ServiceError(400, f"{name} is not a basecalled file of this run")
        info = self.storage.head(key)
        if info is None:
            raise ServiceError(409, f"{key} has not been uploaded")
        progress = dict(run.get("basecalling") or {})
        files = [f for f in progress.get("files", []) if f["name"] != name]
        files.append({"name": name, "key": key, "size": info.size, "etag": info.etag,
                      "reads_in": int(reads_in), "reads_out": int(reads_out), "done": time.time(),
                      "generation": int(generation)})
        progress.update({"files": files, "done": len(files), "total": len(expected),
                         "reads_in": sum(f["reads_in"] for f in files),
                         "reads_out": sum(f["reads_out"] for f in files)})
        self.store.update_run(run_id, {"basecalling": progress, "basecall_current": None,
                                       **self._basecall_attempt(run, generation)})
        return {"done": progress["done"], "total": progress["total"]}

    def record_basecall_progress(self, run_id: str, generation: int, name: str, reads: int,
                                 estimate: int) -> dict:
        """The dorado job's progress on the file it is basecalling (reads
        called so far, and its estimate of the file's reads), for the run
        page."""
        run = self.check_stage_generation(run_id, DORADO_STAGE, generation)
        if not stage_of(run, DORADO_STAGE).get("active"):
            raise ServiceError(409, f"Run is {run['state']}; not basecalling")
        if not isinstance(reads, int) or not isinstance(estimate, int) or reads < 0 or estimate < 0:
            raise ServiceError(400, "reads and estimate must be non-negative integers")
        self.store.update_run(run_id, {"basecall_current": {"file": str(name)[:255], "reads": reads,
                                                            "estimate": estimate, "at": time.time()},
                                       **self._basecall_attempt(run, generation)})
        return {"ok": True}

    @staticmethod
    def _basecall_attempt(run: dict, generation: int) -> dict:
        """When this dorado attempt first reported (the clock for its rate,
        progress.py): set on its first report, so a retry's clock starts
        with the retry and not while its machine was starting."""
        if (run.get("basecall_attempt") or {}).get("generation") == int(generation):
            return {}
        return {"basecall_attempt": {"generation": int(generation), "started": time.time()}}

    def ingest(self, run_id: str, generation: int, events: list[dict]) -> dict:
        run = self.check_stage_generation(run_id, ENGINE_STAGE, generation)
        if run["state"] not in (RUNNING, FINALIZING):
            raise ServiceError(409, f"Run is {run['state']}; ingest is closed")
        view = self.view(run_id)
        if view.generation != int(generation):
            with self._views_lock:
                if self._views.get(run_id) is view:
                    self._views.pop(run_id, None)
            view = self.view(run_id)
        appended = view.log.ingest(events)
        if view.log.gap is not None:
            view.log.reconcile_from_file()
        self._apply_ingested(run, events)
        return {"accepted": appended, "version": view.log.version}

    def package_uploads(self, run_id: str, generation: int) -> dict:
        """Presigned PUT URLs for the three downloads, which the engine job
        builds from its local output after the engine exits."""
        run = self.check_stage_generation(run_id, ENGINE_STAGE, generation)
        if run["state"] not in (RUNNING, FINALIZING):
            raise ServiceError(409, f"Run is {run['state']}")
        prefix = self.run_prefix(run)
        return {name: {"key": f"{prefix}/{name}",
                       "url": self.storage.presign_put(f"{prefix}/{name}", expires_s=8 * 3600)}
                for name in PACKAGES}

    @staticmethod
    def active_jobs(run: dict) -> list[tuple[str, int, dict]]:
        """``(stage, generation, job)`` for each stage whose job is running
        (or launching: a job not yet recorded is left out)."""
        out = []
        for stage, rec in (run.get("stages") or {}).items():
            if rec.get("active"):
                job = (run.get("jobs") or {}).get(f"{run['id']}-{stage}-{rec['generation']}")
                if job:
                    out.append((stage, int(rec["generation"]), job))
        return out

    @staticmethod
    def stage_for_generation(run: dict, generation: int) -> Optional[str]:
        for stage, rec in (run.get("stages") or {}).items():
            if rec.get("generation") == int(generation):
                return stage
        return None

    def _apply_ingested(self, run: dict, events: list[dict]) -> None:
        """Control-plane bookkeeping from the event stream: command
        outcomes, ingested files, the effective configuration."""
        updates = {}
        ingested = list(run.get("ingested_files") or [])
        for d in events:
            t, data = d.get("type"), d.get("data") or {}
            if t == "command.outcome" and data.get("command_id"):
                self.store.mark_command(run["id"], data["command_id"], data.get("outcome", "applied"),
                                        data.get("reason"))
            elif t == "specimux.completed" and data.get("file_path"):
                name = str(data["file_path"]).rsplit("/", 1)[-1]
                if name not in ingested:
                    ingested.append(name)
                    updates["ingested_files"] = ingested
            elif t == "pipeline.started" and data.get("config_summary"):
                updates["effective_config"] = data["config_summary"]
            elif t == "finalization.started" and run.get("state") == RUNNING:
                updates["state"] = FINALIZING
        if updates:
            try:
                self.store.update_run(run["id"], updates)
            except ConflictError:
                pass

    def report_exit(self, run_id: str, generation: int, exit_code: int, log_tail: str = "",
                    packages: Optional[dict] = None, stage: Optional[str] = None) -> dict:
        """The wrapper's exit report. Records the exit, releases the stage
        and hands the run to the next waiting one at once; the seal (copying
        the output dir and log to storage, building the results package)
        runs in a background thread with the run in ``sealing``. A repeated
        report for the same generation is acknowledged, not re-applied."""
        run = self.get_run(run_id)
        exit_info = run.get("exit")
        if exit_info and exit_info.get("generation") == int(generation):
            return self._public(run)  # a retried report: already recorded
        stage = stage or self.stage_for_generation(run, generation) or ENGINE_STAGE
        run = self.check_stage_generation(run_id, stage, generation)
        if not stage_of(run, stage).get("active"):
            return self._public(run)  # already judged (a reconcile pass, or a retried report)
        if stage == DORADO_STAGE:
            return self._dorado_exited(run, int(generation), exit_code, log_tail)
        exit_info = {"code": exit_code, "generation": generation, "log_tail": log_tail[-4000:],
                     "reported": time.time()}
        if (run.get("cancel") or {}).get("reason"):
            exit_info["reason"] = run["cancel"]["reason"]
        if packages:
            exit_info["packages"] = {k: v for k, v in packages.items() if k in PACKAGES}
        with self._views_lock:
            view = self._views.get(run_id)
        if view is not None:
            view.log.reconcile_from_file()
            # errors the engine reported are part of the outcome the job page shows
            exit_info["errors"] = len(view.state.errors)
        # The job secret stays valid so a retried exit report is accepted;
        # ingest is refused once the run is no longer running (below)
        try:
            # conditional: a reconcile pass may have judged this job already
            run = self.store.update_run(run_id, {"state": SEALING, "exit": exit_info},
                                        expected_state=[RUNNING, FINALIZING])
        except ConflictError:
            return self._public(self.get_run(run_id))
        self._end_stage(run_id, ENGINE_STAGE, int(generation))
        logger.info(f"Run {run_id} generation {generation} exited {exit_code}: sealing")
        threading.Thread(target=self._seal_and_finish, args=(run_id, exit_code),
                         name=f"seal-{run_id}", daemon=True).start()
        try:
            self.launch_next_queued()
        except Exception:
            logger.exception("Launching the next queued run failed")
        return self._public(run)

    def _dorado_exited(self, run: dict, generation: int, exit_code: int, log_tail: str = "",
                       reason: str = "") -> dict:
        """The basecalling job is over. Success means every POD5 file of the
        manifest has its FASTQ in storage (the listing is the truth, the
        job's reports the bookkeeping): those become the engine's reads
        and the engine is launched. Anything else fails the run; the FASTQs
        delivered so far stay, and a relaunch picks up where it stopped."""
        run_id = run["id"]
        exit_info = {"code": exit_code, "generation": generation, "log_tail": log_tail[-4000:],
                     "reported": time.time(), "stage": DORADO_STAGE}
        if reason:
            exit_info["reason"] = reason
        if (run.get("cancel") or {}).get("reason"):
            exit_info["reason"] = run["cancel"]["reason"]
        expected = {self.basecalled_key(run, e["key"]): e for e in run.get("manifest") or []}
        present = {o.key: o for o in self.storage.list(f"{self.run_prefix(run)}/fastq/")}
        missing = sorted(k.rsplit("/", 1)[-1] for k in expected if k not in present)
        if exit_code == 0 and missing:
            exit_code = 1
            exit_info["reason"] = f"{len(missing)} of {len(expected)} files were not basecalled: " \
                                  + ", ".join(missing[:5])
        # The state change is conditional, so an exit report and a reconcile
        # pass judging the same job apply once; the stage is released after it.
        if exit_code != 0:
            try:
                run = self.store.update_run(run_id, {"state": FAILED, "exit": exit_info,
                                                     "sealed": {"error": "basecalling failed", "output_files": 0}},
                                            expected_state=[BASECALLING])
            except ConflictError:
                return self._public(self.get_run(run_id))
            self._end_stage(run_id, DORADO_STAGE, generation)
            logger.warning(f"Run {run_id} basecalling (generation {generation}) failed: "
                           f"{exit_info.get('reason') or exit_code}")
            self._launch_next_queued_quietly()
            return self._public(run)
        basecalled = [{"key": k, "size": present[k].size, "etag": present[k].etag} for k in sorted(expected)]
        try:
            run = self.store.update_run(run_id, {"state": INPUT_COMPLETE, "basecalled": basecalled,
                                                 "basecalling": {**(run.get("basecalling") or {}),
                                                                 "finished": time.time()},
                                                 "exit": None},
                                        expected_state=[BASECALLING])
        except ConflictError:
            return self._public(self.get_run(run_id))
        self._end_stage(run_id, DORADO_STAGE, generation)
        logger.info(f"Run {run_id} basecalled: {len(basecalled)} files; launching the engine")
        run = self._try_launch(run_id) or run
        self._launch_next_queued_quietly()
        return self._public(run)

    def _launch_next_queued_quietly(self) -> None:
        try:
            self.launch_next_queued()
        except Exception:
            logger.exception("Launching the next queued run failed")

    def _seal_and_finish(self, run_id: str, exit_code: int) -> None:
        final = SEALED if exit_code == 0 else FAILED
        try:
            sealed = self.seal(self.get_run(run_id))
            self.store.update_run(run_id, {"state": final, "sealed": sealed})
            logger.info(f"Run {run_id} sealed: {sealed}")
        except Exception as e:
            logger.exception(f"Seal of {run_id} failed")
            self.store.update_run(run_id, {"state": final, "sealed": {"error": str(e)}})

    def seal(self, run: dict) -> dict:
        """Store the mirror (the event log and what the dashboard serves) as
        the run's view.zip, and the event log on its own, and record the
        three downloads: the engine job uploaded them from its local output
        (the usual case); for a job that died before it could, they are
        built from the mirror, which holds the served artifacts but not the
        reads. The EFS directory itself goes later (clean_up)."""
        prefix = self.run_prefix(run)
        out = self.output_dir(run["id"])
        sealed = {"at": time.time(), "events": f"{prefix}/events.jsonl", **self._put_view(run, out)}
        if (out / "events.jsonl").exists():
            self.storage.put_file(f"{prefix}/events.jsonl", out / "events.jsonl")
        uploaded = ((run.get("exit") or {}).get("packages") or {})
        for name, include in PACKAGES.items():
            key = f"{prefix}/{name}"
            short = name.split(".")[0]
            info = self.storage.head(key) if name in uploaded else None
            if info is not None:
                sealed[short], sealed[short + "_bytes"] = key, info.size
                continue
            with self._zip_tree(out, include) as tmp:
                sealed[short], sealed[short + "_bytes"] = key, tmp.stat().st_size
                self.storage.put_file(key, tmp)
            if uploaded:
                sealed.setdefault("rebuilt", []).append(name)
        return sealed

    def _put_view(self, run: dict, out: Path) -> dict:
        """Pack a mirror into the run's view.zip; what to record on
        ``sealed`` (nothing when the mirror is empty or missing)."""
        include = (lambda rel: not any(part in VIEW_SKIP_DIRS for part in rel.parts[:-1]))
        if not out.exists() or not package_files(out, include):
            return {}
        key = f"{self.run_prefix(run)}/{VIEW_ZIP}"
        with self._zip_tree(out, include) as tmp:
            files = len(zipfile.ZipFile(tmp).namelist())
            self.storage.put_file(key, tmp)
            return {"view": key, "view_files": files, "view_bytes": tmp.stat().st_size}

    def _zip_tree(self, out: Path, include):
        """A zip of the output dir's files that ``include(rel)`` accepts,
        streamed to a temp file (never held in memory); a context manager
        that removes the file."""
        import contextlib
        import tempfile

        @contextlib.contextmanager
        def build():
            fd, name = tempfile.mkstemp(suffix=".zip", dir=self.config.data_dir)
            os.close(fd)
            tmp = Path(name)
            try:
                build_zip(tmp, package_files(out, include), workers=MIRROR_ZIP_WORKERS)
                yield tmp
            finally:
                tmp.unlink(missing_ok=True)
        return build()

    def results_url(self, run_id: str, package: str = "results", host_id: Optional[str] = None) -> str:
        run = self.get_run(run_id, host_id)
        if run["state"] not in (SEALED, FAILED, INCOMPLETE) or not run.get("sealed"):
            raise ServiceError(409, f"Run is {run['state']}; results are available once it is sealed")
        if package not in DOWNLOAD_SUFFIX:
            raise ServiceError(404, "No such package")
        return self.storage.presign_get(f"{self.run_prefix(run)}/{package}.zip",
                                        filename=download_name(run, package))

    def log_url(self, run_id: str, host_id: Optional[str] = None) -> str:
        run = self.get_run(run_id, host_id)
        if not run.get("sealed"):
            raise ServiceError(409, f"Run is {run['state']}; the log is available once it is sealed")
        return self.storage.presign_get(f"{self.run_prefix(run)}/events.jsonl")

    # --- commands ---

    def post_command(self, run_id: str, command: str, args: dict, actor: str) -> dict:
        run = self.get_run(run_id)
        if command not in COMMANDS:
            raise ServiceError(400, f"Unknown command: {command}")
        if run["state"] not in (RUNNING, FINALIZING):
            raise ServiceError(409, f"Run is {run['state']}; commands apply to a running engine")
        cid = uuid.uuid4().hex[:12]
        record = {"id": cid, "command": command, "args": dict(args), "actor": actor}
        self.store.put_command(run_id, record)          # pending before it is sent
        self.queue.send(run_id, record)
        return {"command_id": cid, "outcome": "pending"}

    def next_commands(self, run_id: str, wait_s: float = 20.0) -> list[dict]:
        msgs = self.queue.receive(run_id, wait_s=min(wait_s, 25.0))
        return [{"message_id": m.id, **m.body} for m in msgs]

    def ack_command(self, run_id: str, message_id: str) -> None:
        self.queue.ack(run_id, message_id)

    # --- the viewer ---

    def view(self, run_id: str) -> RunView:
        """The run's viewer: over its EFS mirror while it has one (a run in
        progress, or finished less than ``EFS_GRACE_S`` ago), else over a
        local copy of its view.zip. Built outside the lock (a view.zip
        download takes a moment); a concurrent build of the same view
        loses to the first one in."""
        with self._views_lock:
            view = self._views.get(run_id)
        if view is None:
            run = self.get_run(run_id)
            out = self._view_dir(run)
            base = f"/v1/runs/{run_id}"
            runtime = {"apiBase": base, "assetBase": base, "pageBase": base,
                       "sessionEndpoint": "/v1/session",
                       "tokenEndpoint": self.authorize_url(run)}
            built = RunView(run, out, out / "events.jsonl", runtime=runtime, public_url=self.public_url(run),
                            status=lambda: self.dashboard_status(run_id))
            with self._views_lock:
                view = self._views.setdefault(run_id, built)
        view.last_used = time.time()
        return view

    def dashboard_status(self, run_id: str) -> Optional[dict]:
        """The dashboard's banner while the engine has not started (the
        suite's viewer ``status``): the upload, the wait, basecalling, as
        on the run page; None once the engine's events arrive (the page
        then reloads onto them) and for a finished run. Cached briefly:
        every open dashboard asks every ten seconds."""
        now = time.time()
        with self._views_lock:
            hit = self._status_cache.get(run_id)
            view = self._views.get(run_id)
        if hit and now - hit[0] < STATUS_CACHE_S:
            return hit[1]
        run = self.get_run(run_id)
        state, status = run["state"], None
        if state in (CREATED, UPLOADING):
            up = self._upload_received(run)
            status = {"text": "Uploading: " + upload_text(up, now), "progress": upload_estimate(up, now)["fraction"]}
        elif state == INPUT_COMPLETE:
            status = {"text": "Upload complete; waiting for a machine", "progress": None}
        elif state == BASECALLING:
            est = basecall_estimate(run, now)
            status = {"text": "Basecalling: " + (basecall_text(run, now) or "starting"),
                      "progress": est["fraction"] if est else None}
        elif state in (RUNNING, FINALIZING) and (view is None or view.log.version == 0):
            status = {"text": "Starting the pipeline", "progress": None}
        with self._views_lock:
            self._status_cache[run_id] = (now, status)
        return status

    def _view_dir(self, run: dict) -> Path:
        out = self.output_dir(run["id"])
        if out.exists() or run["state"] not in (SEALED, FAILED, INCOMPLETE):
            out.mkdir(parents=True, exist_ok=True)
            return out
        # finished, its EFS directory gone: the view.zip, unpacked on local disk
        cached = self.config.data_dir / "views" / f"{run['id']}-g{engine_generation(run)}"
        if cached.exists():
            return cached
        key = (run.get("sealed") or {}).get("view")
        tmp = cached.with_name(cached.name + f".{secrets.token_hex(4)}.part")
        tmp.mkdir(parents=True)
        if key:
            with tempfile.TemporaryDirectory(dir=self.config.data_dir) as d:
                self.storage.download(key, Path(d) / VIEW_ZIP)
                with zipfile.ZipFile(Path(d) / VIEW_ZIP) as z:
                    z.extractall(tmp)
        try:
            tmp.rename(cached)
        except OSError:
            shutil.rmtree(tmp, ignore_errors=True)   # another request unpacked it first
        return cached

    def _drop_view(self, run_id: str) -> None:
        """Forget a run's view and remove its local copy, if it has one."""
        with self._views_lock:
            view = self._views.pop(run_id, None)
            self._status_cache.pop(run_id, None)
        cache = self.config.data_dir / "views"
        if view is not None and Path(view.output_dir).parent == cache:
            shutil.rmtree(view.output_dir, ignore_errors=True)

    def authorize_url(self, run: dict) -> Optional[str]:
        """Where the page sends a browser without a session: the run's
        host's authorize route, with the run id. None when the host has
        none (the page then can only show a 401)."""
        host = self.store.get_host(run.get("host") or "")
        url = (host or {}).get("authorize_url")
        if not url:
            return None
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}run={run['id']}"

    # --- reconciliation ---

    def expire_idle_uploads(self, now: Optional[float] = None) -> list[str]:
        """End abandoned uploads: a run uploading with no upload request for
        ``upload_idle_s``, or created and never uploaded to for
        ``created_idle_s``, becomes incomplete. Its job code stops working;
        what was uploaded stays in the archive. The watching uploader's
        status checks are not activity, or a forgotten one would keep its
        run open for ever."""
        now = time.time() if now is None else now
        expired = []
        # a live run whose uploads stopped: complete it with what arrived,
        # so its engine finalizes (or, waiting for a slot, runs over them)
        for run in self.store.list_runs(states=[UPLOADING, RUNNING, FINALIZING]):
            if run["spec"].get("mode") != "live" or not self.uploads_open(run) or not run.get("last_upload"):
                continue
            idle = now - float(run["last_upload"])
            if idle < self.config.live_idle_s:
                continue
            try:
                self.complete(run["id"])
            except ServiceError as e:
                logger.warning(f"Live run {run['id']} idle but not completed: {e.message}")
                continue
            self.store.update_run(run["id"], {"auto_completed": {
                "at": now, "reason": f"no upload for {int(self.config.live_idle_s // 3600)} h"}})
            logger.info(f"Live run {run['id']}: no upload for {idle / 3600:.1f} h; upload completed with what arrived")
            expired.append(run["id"])
        for run in self.store.list_runs(states=[CREATED, UPLOADING]):
            if run["spec"].get("mode") == "live" and run.get("last_upload"):
                continue  # handled above
            limit = self.config.upload_idle_s if run["state"] == UPLOADING else self.config.created_idle_s
            since = float(run.get("last_upload") or run.get("created") or now)
            if now - since < limit:
                continue
            hours = int(limit // 3600)
            why = (f"no upload for {hours} h" if run["state"] == UPLOADING
                   else f"nothing uploaded within {hours // 24} days of creation")
            try:
                self.store.update_run(run["id"], {
                    "state": INCOMPLETE, "exit": {"stage": "upload", "code": None,
                                                  "reason": f"upload abandoned: {why}", "reported": now},
                }, expected_state=[run["state"]])
            except ConflictError:
                continue  # an upload or complete landed first
            logger.info(f"Run {run['id']} incomplete: upload abandoned ({why})")
            expired.append(run["id"])
        return expired

    # --- cleanup: what was only ever transient ---

    def clean_up(self, now: Optional[float] = None) -> dict:
        """Remove what a finished run no longer needs; run by its own loop
        (``cli runapi``), since a first pass over a large EFS takes a while.

        - A finished run's EFS directory, ``EFS_GRACE_S`` after it ended,
          once its view.zip is in storage (one is built from the mirror
          first if missing: runs sealed before view.zip, or whose seal had
          nothing to pack yet), with its command queue. A directory whose run
          no longer exists goes too. A run whose seal failed keeps its
          directory for debugging.
        - A cancelled or abandoned (incomplete) run's upload archive,
          ``ARCHIVE_GRACE_S`` after it ended, when the run made it. The run
          record is marked first, inside a conditional write, so a retry
          either lands before (and the archive stays) or is refused.
        - Views of finished runs nobody has opened for ``VIEW_IDLE_S``.
        """
        now = time.time() if now is None else now
        done = {"efs": 0, "archives": 0, "views": 0}
        root = self.config.work_root
        for d in sorted(root.iterdir()) if root.exists() else []:
            if not d.is_dir() or not RUN_ID.fullmatch(d.name):
                continue
            try:
                if self._clean_run_dir(d, now):
                    done["efs"] += 1
            except Exception:
                logger.exception(f"Cleanup of {d} failed")
        for run in self.store.list_runs(states=[FAILED, INCOMPLETE]):
            try:
                if self._clean_archive(run, now):
                    done["archives"] += 1
            except Exception:
                logger.exception(f"Cleanup of {run['id']}'s archive failed")
        with self._views_lock:
            idle = [rid for rid, v in self._views.items()
                    if now - v.last_used > VIEW_IDLE_S and not self.output_dir(rid).exists()]
        for rid in idle:
            self._drop_view(rid)
            done["views"] += 1
        return done

    def _clean_run_dir(self, d: Path, now: float) -> bool:
        run = self.store.get_run(d.name)
        if run is None:
            shutil.rmtree(d, ignore_errors=True)
            logger.info(f"Cleanup: removed {d} (no such run)")
            return True
        if run["state"] not in (SEALED, FAILED, INCOMPLETE):
            return False
        sealed = run.get("sealed") or {}
        if sealed.get("error") and sealed["error"] != "cancelled":
            return False
        ended = float(sealed.get("at") or (run.get("exit") or {}).get("reported") or 0)
        if now - ended < EFS_GRACE_S:
            return False
        if not sealed.get("view") or self.storage.head(sealed["view"]) is None:
            view = self._put_view(run, self.output_dir(run["id"]))
            if view:
                try:
                    self.store.update_run(run["id"], lambda cur: {"sealed": {**(cur.get("sealed") or {}), **view}},
                                          expected_state=[run["state"]])
                except ConflictError:
                    return False  # retried meanwhile: its directory is in use again
        self._drop_view(run["id"])
        shutil.rmtree(d, ignore_errors=True)
        self.queue.purge(run["id"])
        logger.info(f"Cleanup: removed {run['id']}'s EFS directory ({run['state']})")
        return True

    def _clean_archive(self, run: dict, now: float) -> bool:
        if run.get("archive_deleted") or run["spec"].get("archive_id"):
            return False
        if run["state"] == FAILED and not _cancelled(run):
            return False
        ended = float((run.get("exit") or {}).get("reported") or 0)
        if not ended or now - ended < ARCHIVE_GRACE_S:
            return False

        def mark(cur: dict) -> dict:
            if cur["state"] == FAILED and not _cancelled(cur):
                raise ConflictError("no longer cancelled")
            return {"archive_deleted": now}
        try:
            self.store.update_run(run["id"], mark, expected_state=[FAILED, INCOMPLETE])
        except ConflictError:
            return False
        n = self._delete_archive(run)
        logger.info(f"Cleanup: deleted {run['id']}'s upload archive ({n} objects; "
                    f"{'cancelled' if _cancelled(run) else 'upload abandoned'})")
        return True

    def _delete_archive(self, run: dict) -> int:
        n = self.storage.delete_prefix(self.archive_prefix(run) + "/")
        archive = self.store.get_archive(run["archive_id"])
        if archive:
            self.store.put_archive({**archive, "deleted": time.time()})
        return n

    def reconcile(self, intent_grace_s: float = INTENT_GRACE_S) -> dict:
        """Resolve open intents and check every job believed active; run at
        startup and every ``reconcile_interval_s`` after (``cli runapi``).
        A launch whose response was lost is adopted by name; a job that
        died without an exit report fails or marks the run incomplete.

        Safe to run beside live traffic: an intent younger than
        ``intent_grace_s`` may be a launch in flight (in this task, or in
        the other task of a rolling deploy) and is left alone, and every
        judgement is a conditional state change, so a job's own exit report
        and this pass never both apply."""
        adopted, failed = 0, 0
        now = time.time()
        self.expire_idle_uploads(now)
        for intent in self.store.list_open_intents():
            if now - float(intent.get("opened") or 0) < intent_grace_s:
                continue
            payload = intent["payload"]
            handle = self.launcher.find_by_name(payload.get("name", ""))
            if handle is not None:
                self.store.resolve_intent(intent["id"], {"job_id": handle.id, "adopted": True})
                run = self.store.get_run(intent["run_id"])
                kind = "dorado" if intent["kind"] == "launch-dorado" else "engine"
                if run:
                    jobs = dict(run.get("jobs", {}))
                    jobs.setdefault(handle.name, {"id": handle.id, "kind": kind,
                                                  "generation": payload.get("generation")})
                    self.store.update_run(run["id"], {"jobs": jobs})
                adopted += 1
            else:
                # nothing was submitted: reopen the launch
                self.store.resolve_intent(intent["id"], {"resubmit": True})
                run = self.store.get_run(intent["run_id"])
                stage = intent["kind"].removeprefix("launch-")
                rec = stage_of(run, stage) if run else {}
                if run and rec.get("active") and rec.get("generation") == payload.get("generation"):
                    try:
                        self.store.update_run(run["id"], lambda cur: {
                            "state": INPUT_COMPLETE if cur["state"] in (RUNNING, BASECALLING) else cur["state"],
                            "generation": int(cur["generation"]) - 1,
                            "stages": {**(cur.get("stages") or {}), stage: {**stage_of(cur, stage), "active": False}},
                        }, expected_state=[run["state"]])
                    except ConflictError:
                        continue
                    self.store.release_stage(stage, run["id"])
                    try:
                        self._try_launch(run["id"])
                    except ServiceError as e:
                        logger.warning(f"Relaunch of {run['id']} refused: {e.message}")
        # every stage's job believed active: one that ended without an exit
        # report is judged here, through the same conditional paths
        for run in self.store.list_runs(states=[UPLOADING, INPUT_COMPLETE, BASECALLING, RUNNING, FINALIZING]):
            for stage, generation, job in self.active_jobs(run):
                status = self.launcher.describe(job["id"])
                if not status.terminal:
                    continue
                code = status.exit_code if status.exit_code is not None else 1
                if stage == DORADO_STAGE:
                    # judged by what it delivered
                    self._dorado_exited(run, generation, code, reason=status.reason or "no exit report")
                    failed += 1 if code else 0
                    continue
                try:
                    self.store.update_run(run["id"], {
                        "state": SEALING,
                        "exit": {"code": code, "reason": status.reason or "no exit report",
                                 "generation": generation},
                    }, expected_state=[RUNNING, FINALIZING])
                except ConflictError:
                    continue  # the exit report landed first
                self._end_stage(run["id"], ENGINE_STAGE, generation)
                # whatever the engine wrote is kept; success is never assumed
                threading.Thread(target=self._seal_and_finish, args=(run["id"], code if code else 1),
                                 name=f"seal-{run['id']}", daemon=True).start()
                failed += 1
        # a seal interrupted by a restart: finish it
        for run in self.store.list_runs(states=[SEALING]):
            if not any(t.name == f"seal-{run['id']}" for t in threading.enumerate()):
                code = (run.get("exit") or {}).get("code", 1)
                threading.Thread(target=self._seal_and_finish, args=(run["id"], code),
                                 name=f"seal-{run['id']}", daemon=True).start()
        self.launch_next_queued()
        return {"adopted": adopted, "failed": failed}
