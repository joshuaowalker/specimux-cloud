"""The run API's HTTP surface, all under /v1 (DESIGN.md "The run API").

Callers and their credentials:

- a host (mycomap.org, the console): ``X-Service-Key`` on job control,
  options, results and run-token minting; every such route sees only the
  calling host's runs.
- the uploader: the job code, ``Authorization: JobCode <run>.<secret>``,
  on uploads and complete.
- the engine (wrapper and cloud plugin): ``X-Job-Secret`` on the job
  bundle, ingest, command polling and the exit report; every ingest
  batch and the exit report carry the generation.
- the browser: the run session cookie, obtained at ``POST /v1/session``
  with a run token the run's host minted. The dashboard pages and their
  assets are served without it (the page then obtains a session, see the
  suite's ``static/runtime.js``); the data, the stream, photos, commands
  and downloads need it.

The per-run viewer (``api/state``, ``events``, ``api/sequence``,
``photos``, the pages) is the suite's viewer app mounted at
``/v1/runs/{id}/`` by ``RunViewerDispatcher``.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile
from starlette.requests import HTTPConnection
from starlette.types import Receive, Scope, Send

from specimux_suite import __version__ as suite_version
from specimux_suite.commands import VIEWER_COMMANDS
from specimux_suite.web.pages import STATIC_DIR

from ..backends.local import DirectoryStorage
from .auth import PUBLIC_SCOPE
from .service import COMMANDS, DORADO_STAGE, ENGINE_STAGE, RunService, ServiceError

logger = logging.getLogger(__name__)

SESSION_COOKIE = "specimux_session"

# Viewer paths a browser may fetch before it has a session: the pages and
# their static assets. Everything else the viewer serves is run data.
OPEN_VIEWER_PATHS = ("/", "/present", "/admin")


def _open_path(rest: str) -> bool:
    return rest in OPEN_VIEWER_PATHS or rest.startswith("/static/")


class RunViewerDispatcher:
    """ASGI app mounted at /v1/runs: routes /{run_id}/<rest> to that run's
    viewer app, which sees the request as /<rest>."""

    def __init__(self, service: RunService, allow):
        self.service = service
        self.allow = allow  # (run_id, rest, scope) -> None or raises

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await Response(status_code=404)(scope, receive, send)
            return
        # Starlette keeps the full path in scope["path"] and the mount
        # prefix in scope["root_path"]; the remainder is ours to route.
        from starlette.routing import get_route_path
        remainder = get_route_path(scope)
        parts = remainder.lstrip("/").split("/", 1)
        run_id = parts[0]
        rest = "/" + (parts[1] if len(parts) > 1 else "")
        try:
            self.allow(run_id, rest, scope)
            view = self.service.view(run_id)
        except HTTPException as e:
            await JSONResponse(status_code=e.status_code, content={"error": e.detail})(scope, receive, send)
            return
        except ServiceError as e:
            await JSONResponse(status_code=e.status, content={"error": e.message})(scope, receive, send)
            return
        child = dict(scope)
        child["root_path"] = scope.get("root_path", "") + "/" + run_id
        child["path"] = child["root_path"] + rest
        await view.app(child, receive, send)


def create_app(service: RunService, console: bool = True) -> FastAPI:
    app = FastAPI(title="specimux-cloud run API", version=suite_version)
    app.state.service = service
    cfg = service.config

    @app.exception_handler(ServiceError)
    async def _service_error(request: Request, exc: ServiceError):
        return JSONResponse(status_code=exc.status, content={"error": exc.message})

    # --- credentials ---

    def service_key(x_service_key: Optional[str] = Header(default=None)) -> dict:
        """The calling host: ``{"host": record, "label": key label}``."""
        return service.authenticate_key(x_service_key or "")

    def check_job_code(run_id: str, authorization: Optional[str], open_only: bool = True) -> dict:
        scheme, _, code = (authorization or "").partition(" ")
        rid, _, secret = code.partition(".")
        if scheme.lower() != "jobcode" or rid != run_id:
            raise HTTPException(403, "Job code required")
        return service.check_job_code(run_id, secret, open_only=open_only)

    def job_code(run_id: str, authorization: Optional[str] = Header(default=None)) -> dict:
        return check_job_code(run_id, authorization)

    def any_job(run_id: str, x_job_secret: Optional[str] = Header(default=None)) -> dict:
        """The calling job: its run, its stage and that stage's generation
        (a job's secret names its stage), whether or not its job is still
        running. Only the exit report accepts a finished job."""
        run, stage = service.job_identity(run_id, x_job_secret or "")
        rec = run["stages"][stage]
        return {"run": run, "stage": stage, "generation": int(rec["generation"]), "active": bool(rec.get("active"))}

    def job_secret(job: dict = Depends(any_job)) -> dict:
        if not job["active"]:
            raise HTTPException(403, "This job has finished")
        return job

    def engine_job(job: dict = Depends(job_secret)) -> dict:
        if job["stage"] != ENGINE_STAGE:
            raise HTTPException(403, "Only the engine job may do this")
        return job

    def dorado_job(job: dict = Depends(job_secret)) -> dict:
        if job["stage"] != DORADO_STAGE:
            raise HTTPException(403, "Only the basecalling job may do this")
        return job

    def session_of(run_id: str, conn: HTTPConnection) -> dict:
        return service.check_session(run_id, conn.cookies.get(SESSION_COOKIE))

    def viewer_allowed(run_id: str, rest: str, scope: Scope) -> None:
        if _open_path(rest):
            return
        session_of(run_id, HTTPConnection(scope))

    def host_or_session(run_id: str, request: Request) -> None:
        """Downloads: the owning host with its key, or a browser with a
        session for the run."""
        key = request.headers.get("x-service-key")
        if key:
            service.get_run(run_id, service.authenticate_key(key)["host"]["id"])
        else:
            session = session_of(run_id, request)
            if session.get("scope") == PUBLIC_SCOPE and not service.sharing(run_id).get("allow_downloads"):
                raise HTTPException(403, "Downloads are not shared publicly for this run")

    # --- job control (a host) ---

    @app.post("/v1/runs")
    async def create_run(request: Request, caller: dict = Depends(service_key)):
        """Multipart: ``spec`` (JSON), ``user_id``, optional
        ``client_token``, files ``primers``, ``specimens``, ``reference``;
        ``reference_sha256`` instead of the file for a reference the
        service already holds."""
        form = await request.form()
        try:
            spec = json.loads(form.get("spec") or "{}")
        except json.JSONDecodeError:
            raise ServiceError(400, "spec must be JSON")
        user_id = str(form.get("user_id") or "") or caller["label"]
        files = {}
        for role in ("primers", "specimens", "reference"):
            f = form.get(role)
            if isinstance(f, UploadFile):
                files[role] = await f.read()
                if role == "reference" and f.filename and not spec.get("reference_name"):
                    spec["reference_name"] = f.filename
        return await run_in_threadpool(service.create_run, spec, user_id, files,
                                       client_token=form.get("client_token"), host_id=caller["host"]["id"],
                                       reference_sha256=form.get("reference_sha256") or None)

    @app.get("/v1/references/sha256/{sha256}")
    def reference_info(sha256: str, caller: dict = Depends(service_key)):
        """Whether the service holds this reference database (200) or needs
        the file with the next run (404)."""
        return service.reference_info(sha256, caller["host"]["id"])

    # Routes that call the service are plain functions: FastAPI runs them
    # in the threadpool, so a slow backend call never blocks the event loop.
    @app.get("/v1/runs")
    def list_runs(user_id: Optional[str] = None, caller: dict = Depends(service_key)):
        return {"runs": service.list_runs(caller["host"]["id"], user_id)}

    @app.get("/v1/runs/{run_id}")
    def run_status(run_id: str, caller: dict = Depends(service_key)):
        return service.status(run_id, caller["host"]["id"])

    @app.delete("/v1/runs/{run_id}")
    def delete_run(run_id: str, caller: dict = Depends(service_key)):
        service.delete_run(run_id, caller["host"]["id"])
        return {"deleted": run_id}

    @app.post("/v1/runs/{run_id}/cancel")
    async def cancel_run(run_id: str, request: Request, caller: dict = Depends(service_key)):
        """Stop a run: ``{"reason"?}``. A waiting run fails at once; an
        active stage's job is stopped and the run fails when it exits."""
        body = await request.json() if int(request.headers.get("content-length") or 0) > 0 else {}
        return await run_in_threadpool(service.cancel_run, run_id, caller["host"]["id"],
                                       str((body or {}).get("reason") or ""), caller["label"])

    @app.post("/v1/runs/{run_id}/retry")
    def retry_run(run_id: str, caller: dict = Depends(service_key)):
        """Basecall a failed POD5 run again, skipping the files already done."""
        return service.retry_run(run_id, caller["host"]["id"])

    @app.post("/v1/runs/{run_id}/public")
    async def set_public(run_id: str, request: Request, caller: dict = Depends(service_key)):
        """The owner's public-viewing switch: ``{"enabled"?, "allow_starring"?,
        "allow_downloads"?, "new_link"?}`` → the settings and, while
        enabled, the link (``url``) that opens a read-only dashboard for
        anyone who has it."""
        body = await request.json() if int(request.headers.get("content-length") or 0) > 0 else {}
        body = body or {}
        flags = {}
        for k in ("enabled", "allow_starring", "allow_downloads", "new_link"):
            if k in body and not isinstance(body[k], bool):
                raise ServiceError(400, f"{k} must be true or false")
            if k in body:
                flags[k] = body[k]
        return await run_in_threadpool(service.set_public, run_id, caller["host"]["id"], **flags)

    @app.post("/v1/runs/{run_id}/job-code")
    def regenerate_job_code(run_id: str, caller: dict = Depends(service_key)):
        return service.regenerate_job_code(run_id, caller["host"]["id"])

    @app.post("/v1/runs/{run_id}/tokens")
    async def mint_token(run_id: str, request: Request, caller: dict = Depends(service_key)):
        """A run token for the host's authorize route to hand to a browser:
        ``{"user", "scope": "view"|"admin", "ttl_seconds"?}``."""
        body = await request.json() if int(request.headers.get("content-length") or 0) > 0 else {}
        body = body or {}
        return await run_in_threadpool(
            service.mint_token, run_id, caller["host"]["id"], str(body.get("user") or caller["label"]),
            str(body.get("scope") or "view"), body.get("ttl_seconds"), caller["label"])

    @app.get("/v1/hosts/me")
    def whoami(caller: dict = Depends(service_key)):
        host = caller["host"]
        return {"host": host["id"], "name": host.get("name"), "label": caller["label"],
                "authorize_url": host.get("authorize_url"), "theme": host.get("theme") or {}}

    @app.get("/v1/load")
    def load(caller: dict = Depends(service_key)):
        """The whole service's load in counts (every host's runs, none
        named): per stage its slots, busy runs, those still waiting for a
        machine, and the queue; runs uploading and sealing."""
        return service.load()

    @app.get("/v1/options")
    def options(caller: dict = Depends(service_key)):
        return service.options()

    @app.get("/v1/version")
    async def version():
        from .. import __version__
        return {"suite": suite_version, "cloud": __version__}

    @app.get("/v1/runs/{run_id}/{package}.zip")
    def results(run_id: str, package: str, request: Request):
        """results.zip (the summary package plus the log), output.zip (the
        whole output minus scratch and debug) or reads.zip; the owning
        host with its key, or a browser with a session."""
        host_or_session(run_id, request)
        return RedirectResponse(service.results_url(run_id, package), status_code=302)

    @app.get("/v1/runs/{run_id}/events.jsonl")
    def sealed_log(run_id: str, request: Request):
        host_or_session(run_id, request)
        return RedirectResponse(service.log_url(run_id), status_code=302)

    # --- uploads (the uploader, the job page) ---

    @app.post("/v1/runs/{run_id}/uploads")
    async def presign(run_id: str, request: Request, run: dict = Depends(job_code)):
        body = await request.json()
        names = body.get("files") or []
        if not isinstance(names, list) or not names:
            raise ServiceError(400, "files: a list of file names")
        return await run_in_threadpool(service.presign_uploads, run_id, [str(n) for n in names])

    @app.get("/v1/runs/{run_id}/upload")
    def upload_status(run_id: str, authorization: Optional[str] = Header(default=None)):
        """Whether the run still takes uploads: a watching uploader stops
        once the run was completed another way (the run page's button)."""
        run = check_job_code(run_id, authorization, open_only=False)
        return {"run_id": run_id, "state": run["state"], "open": service.uploads_open(run)}

    @app.post("/v1/runs/{run_id}/complete")
    async def complete(run_id: str, request: Request, authorization: Optional[str] = Header(default=None),
                       x_service_key: Optional[str] = Header(default=None)):
        # the uploader (job code) or the job page (the owning host's key)
        if x_service_key:
            service.get_run(run_id, service.authenticate_key(x_service_key)["host"]["id"])
        else:
            job_code(run_id, authorization)
        body = await request.json() if int(request.headers.get("content-length") or 0) > 0 else {}
        return await run_in_threadpool(service.complete, run_id, (body or {}).get("manifest"))

    # --- the engine ---

    @app.get("/v1/runs/{run_id}/job")
    def job_bundle(run_id: str, job: dict = Depends(job_secret)):
        return service.job_bundle(run_id, job["stage"])

    @app.post("/v1/runs/{run_id}/ingest")
    async def ingest(run_id: str, request: Request, job: dict = Depends(engine_job)):
        body = await request.json()
        generation = body.get("generation")
        if generation is None:
            raise ServiceError(400, "generation required")
        return await run_in_threadpool(service.ingest, run_id, int(generation), body.get("events") or [])

    @app.post("/v1/runs/{run_id}/inputs")
    async def live_inputs(run_id: str, request: Request, job: dict = Depends(engine_job)):
        """A live engine job's feed: ``{"have": [names]}`` → the uploaded
        files it lacks (presigned), whether the upload is complete, and
        which files the engine has demultiplexed."""
        body = await request.json() if int(request.headers.get("content-length") or 0) > 0 else {}
        have = (body or {}).get("have") or []
        if not isinstance(have, list):
            raise ServiceError(400, "have: a list of file names")
        return await run_in_threadpool(service.live_inputs, run_id, [str(n) for n in have])

    @app.get("/v1/runs/{run_id}/commands/next")
    def next_commands(run_id: str, wait: float = 20.0, job: dict = Depends(engine_job)):
        # A long poll: a plain def runs in the threadpool so the wait never
        # blocks the event loop (and every other request with it)
        return {"commands": service.next_commands(run_id, wait_s=wait)}

    @app.post("/v1/runs/{run_id}/commands/{message_id}/ack")
    def ack_command(run_id: str, message_id: str, job: dict = Depends(engine_job)):
        service.ack_command(run_id, message_id)
        return {"acked": message_id}

    @app.post("/v1/runs/{run_id}/basecalled")
    async def basecalled(run_id: str, request: Request, job: dict = Depends(dorado_job)):
        """The dorado job delivered one FASTQ (PUT to the key the bundle gave it)."""
        body = await request.json()
        for field in ("generation", "name", "key"):
            if body.get(field) in (None, ""):
                raise ServiceError(400, f"{field} required")
        return await run_in_threadpool(service.record_basecalled, run_id, int(body["generation"]),
                                       str(body["name"]), str(body["key"]),
                                       int(body.get("reads_in") or 0), int(body.get("reads_out") or 0))

    @app.post("/v1/runs/{run_id}/exit")
    async def report_exit(run_id: str, request: Request, job: dict = Depends(any_job)):
        body = await request.json()
        return await run_in_threadpool(service.report_exit, run_id, int(body.get("generation", job["generation"])),
                                       int(body.get("exit_code", 1)), str(body.get("log_tail") or ""),
                                       body.get("packages") if isinstance(body.get("packages"), dict) else None,
                                       job["stage"])

    @app.post("/v1/runs/{run_id}/package-uploads")
    async def package_uploads(run_id: str, request: Request, job: dict = Depends(engine_job)):
        """Presigned PUT URLs for results.zip, output.zip and reads.zip,
        which the engine job builds from its local output."""
        body = await request.json()
        return await run_in_threadpool(service.package_uploads, run_id,
                                       int(body.get("generation", job["generation"])))

    # --- the browser ---

    @app.post("/v1/session")
    async def open_session(request: Request, response: Response,
                           authorization: Optional[str] = Header(default=None)):
        """Exchange a run token (``Authorization: Bearer <token>``) for the
        session cookie, path-scoped to the run."""
        scheme, _, token = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise ServiceError(401, "Bearer run token required")
        session = await run_in_threadpool(service.open_session, token)
        response.set_cookie(
            SESSION_COOKIE, session["session"], max_age=session["expires_in"],
            path=f"/v1/runs/{session['run']}/", httponly=True, samesite="lax",
            secure=cfg.secure_cookies,
        )
        return {"run": session["run"], "scope": session["scope"], "user": session["user"],
                "host": session["host"], "expires_in": session["expires_in"]}

    @app.post("/v1/runs/{run_id}/commands")
    async def post_command(run_id: str, request: Request):
        session = session_of(run_id, request)
        body = await request.json()
        command = str(body.pop("command", "") or "")
        if command not in COMMANDS:
            raise ServiceError(400, f"Unknown command: {command}")
        if session.get("scope") == PUBLIC_SCOPE:
            if command not in VIEWER_COMMANDS or not service.sharing(run_id).get("allow_starring", True):
                raise HTTPException(403, "Public viewers may not do this on this run")
        elif command not in VIEWER_COMMANDS and session.get("scope") != "admin":
            raise HTTPException(403, "This command needs a session with admin scope")
        body.pop("actor", None)
        return await run_in_threadpool(service.post_command, run_id, command, body,
                                       actor=service.actor_of(session))

    # --- pages for a proxying host ---

    @app.get("/v1/ui/{version}/{path:path}")
    async def ui_asset(version: str, path: str):
        """The suite's pages and assets as templates (tokens and the
        runtime tag unfilled) for a host that serves them on its origin."""
        if version != suite_version:
            raise HTTPException(404, f"Only suite {suite_version} is served")
        target = (STATIC_DIR / path).resolve()
        if not str(target).startswith(str(STATIC_DIR.resolve())) or not target.is_file():
            raise HTTPException(404, "No such asset")
        return FileResponse(target)

    # --- local storage route (DirectoryStorage presigned URLs) ---

    if isinstance(service.storage, DirectoryStorage):
        storage: DirectoryStorage = service.storage

        @app.put("/v1/storage/{key:path}")
        async def storage_put(key: str, request: Request, exp: str = "", sig: str = ""):
            if not storage.verify("PUT", key, exp, sig):
                raise HTTPException(403, "Bad or expired signature")
            data = await request.body()
            info = storage.put(key, data)
            return Response(status_code=200, headers={"ETag": f'"{info.etag}"'})

        @app.get("/v1/storage/{key:path}")
        async def storage_get(key: str, exp: str = "", sig: str = ""):
            if not storage.verify("GET", key, exp, sig):
                raise HTTPException(403, "Bad or expired signature")
            path = storage._path(key)
            if not path.is_file():
                raise HTTPException(404, "No such object")
            return FileResponse(path)

    app.mount("/v1/runs", RunViewerDispatcher(service, viewer_allowed))

    if console:
        from ..console.app import create_console
        app.mount("/console", create_console(cfg.base_url, secret=cfg.session_secret,
                                             secure_cookies=cfg.secure_cookies, in_process=app))
    return app


def _local_secret(data_dir: Path) -> str:
    """A session secret for a local run API, kept in the data dir so
    sessions survive a restart."""
    path = Path(data_dir) / "session-secret"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        from .auth import new_secret
        path.write_text(new_secret())
        try:
            path.chmod(0o600)
        except OSError:
            pass
    return path.read_text().strip()


def build_local_service(data_dir: Path, base_url: str, dev_key: Optional[str] = "dev.dev-service-key",
                        engine_api_url: Optional[str] = None,
                        engine_extra_args: Optional[list] = None,
                        session_secret: Optional[str] = None) -> RunService:
    """A RunService over the local backends under one data directory. With
    ``dev_key``, a host ``dev`` exists whose authorize route is the console
    and whose first key is exactly that string (development only)."""
    from ..backends.local import DirectoryStorage, MemoryQueue, SQLiteStore, SubprocessLauncher
    from .service import ServiceConfig
    data_dir = Path(data_dir)
    secret = session_secret or _local_secret(data_dir)
    config = ServiceConfig(data_dir=data_dir, base_url=base_url, session_secret=secret,
                           engine_api_url=engine_api_url, engine_extra_args=list(engine_extra_args or []))
    service = RunService(
        config,
        storage=DirectoryStorage(data_dir / "storage", base_url=base_url, secret=secret),
        queue=MemoryQueue(),
        launcher=SubprocessLauncher(data_dir / "logs"),
        store=SQLiteStore(data_dir / "control-plane.sqlite"),
    )
    if dev_key:
        install_dev_host(service, dev_key)
    return service


def install_dev_host(service: RunService, dev_key: str, host_id: str = "dev") -> None:
    """A host whose key is a known string, for the local stack and tests.
    Its authorize route is the console mounted beside the run API."""
    from .auth import hash_key
    if service.store.get_host(host_id) is None:
        service.add_host(host_id, name="Local development", label="dev",
                         authorize_url=f"{service.config.base_url}/console/authorize")
    host = service.get_host(host_id)
    if not any(k["hash"] == hash_key(dev_key) for k in host["keys"]):
        host["keys"].insert(0, {"label": "dev", "hash": hash_key(dev_key), "created": 0, "expires": None})
        service.store.put_host(host)


def build_aws_service(base_url: str, engine_extra_args: Optional[list] = None) -> RunService:
    """A RunService over the AWS backends, configured from the environment
    the infrastructure sets (see infra/stack.py):

    SPECIMUX_BUCKET, SPECIMUX_TABLE, SPECIMUX_QUEUE_PREFIX,
    SPECIMUX_BATCH_QUEUE_ENGINE, SPECIMUX_BATCH_JOBDEF_ENGINE,
    SPECIMUX_SESSION_SECRET, SPECIMUX_WORK_ROOT (the EFS mount),
    SPECIMUX_DATA_DIR, SPECIMUX_ENGINE_API_URL (how a job reaches this
    service), AWS_REGION; for the dorado stage SPECIMUX_BATCH_QUEUE_DORADO,
    SPECIMUX_BATCH_JOBDEF_DORADO and SPECIMUX_DORADO_MODELS (the model
    complexes the dorado image bakes, comma separated); SPECIMUX_STAGE_SLOTS
    (``engine=2,dorado=2``) for how many runs each stage runs at once. Hosts and their
    keys live in the table (``specimux-cloud hosts``).
    """
    from ..backends.aws import BatchLauncher, DynamoStore, S3Storage, SqsQueue
    from .service import DEFAULT_STAGE_SLOTS, ServiceConfig, parse_stage_slots
    env = os.environ
    region = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION")
    config = ServiceConfig(
        data_dir=Path(env.get("SPECIMUX_DATA_DIR", "/data")),
        base_url=base_url,
        session_secret=env["SPECIMUX_SESSION_SECRET"],
        work_root=Path(env.get("SPECIMUX_WORK_ROOT", "/mnt/runs")),
        engine_api_url=env.get("SPECIMUX_ENGINE_API_URL") or base_url,
        engine_extra_args=list(engine_extra_args or []),
        **({"dorado_models": [m.strip() for m in env["SPECIMUX_DORADO_MODELS"].split(",") if m.strip()]}
           if env.get("SPECIMUX_DORADO_MODELS") else {}),
        **({"stage_slots": {**DEFAULT_STAGE_SLOTS, **parse_stage_slots(env["SPECIMUX_STAGE_SLOTS"])}}
           if env.get("SPECIMUX_STAGE_SLOTS") else {}),
    )
    queues = {"engine": env["SPECIMUX_BATCH_QUEUE_ENGINE"]}
    jobdefs = {"engine": env["SPECIMUX_BATCH_JOBDEF_ENGINE"]}
    if env.get("SPECIMUX_BATCH_QUEUE_DORADO"):
        queues["dorado"] = env["SPECIMUX_BATCH_QUEUE_DORADO"]
        jobdefs["dorado"] = env["SPECIMUX_BATCH_JOBDEF_DORADO"]
    return RunService(
        config,
        storage=S3Storage(env["SPECIMUX_BUCKET"], region=region),
        queue=SqsQueue(prefix=env.get("SPECIMUX_QUEUE_PREFIX", "specimux-cloud"), region=region),
        launcher=BatchLauncher(queues, jobdefs, region=region),
        store=DynamoStore(env["SPECIMUX_TABLE"], region=region),
    )
