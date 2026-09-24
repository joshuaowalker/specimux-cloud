"""The console: a built-in host (DESIGN.md "The console and the local stack").

A small web app that plays a host's part in the job API and nothing more:
log in with a service key, see the host's runs, create a run and get its
job code, open the dashboard, download results, delete. It is the
executable form of what mycomap.org builds, so it talks to the run API
only over HTTP with the service key, exactly as mycomap.org does, never
through the service's internals; mounted beside the run API it uses an
in-process transport, standalone it uses a URL.

Its own session is a cookie holding the service key, encrypted with the
deployment's session secret (the console never stores keys server-side,
so a replica or a restart needs nothing). The authorize route mints a run
token through the run API and hands it to the dashboard page: as JSON
when the page fetches it (same origin), or as a redirect with the token
in the URL fragment when the page navigated here (see the suite's
``static/runtime.js``).
"""

import base64
import hashlib
import html
import json
import logging
import time
from typing import Optional
from urllib.parse import quote, urlencode

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.datastructures import UploadFile

logger = logging.getLogger(__name__)

COOKIE = "specimux_console"
SESSION_TTL_S = 12 * 3600


def _fernet(secret: str):
    from cryptography.fernet import Fernet
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(f"{secret}:console".encode()).digest()))


def create_console(run_api: str, secret: str, secure_cookies: bool = False, in_process=None,
                   mount: str = "/console") -> FastAPI:
    app = FastAPI(title="specimux-cloud console")
    run_api = run_api.rstrip("/")
    fernet = _fernet(secret)

    def client(key: Optional[str] = None) -> httpx.AsyncClient:
        headers = {"X-Service-Key": key} if key else {}
        if in_process is not None:
            return httpx.AsyncClient(base_url=run_api, headers=headers,
                                     transport=httpx.ASGITransport(app=in_process))
        return httpx.AsyncClient(base_url=run_api, headers=headers, timeout=30.0)

    # --- the console session ---

    def whoami(request: Request) -> Optional[dict]:
        raw = request.cookies.get(COOKIE)
        if not raw:
            return None
        try:
            data = json.loads(fernet.decrypt(raw.encode(), ttl=SESSION_TTL_S))
        except Exception:
            return None
        return data

    def set_session(response: Response, data: dict) -> None:
        response.set_cookie(COOKIE, fernet.encrypt(json.dumps(data).encode()).decode(),
                            max_age=SESSION_TTL_S, path=mount + "/", httponly=True,
                            samesite="lax", secure=secure_cookies)

    def login_redirect(next_url: Optional[str] = None) -> RedirectResponse:
        q = f"?{urlencode({'next': next_url})}" if next_url else ""
        return RedirectResponse(f"{mount}/login{q}", status_code=303)

    # --- pages ---

    def page(title: str, body: str, user: Optional[dict] = None, refresh: Optional[int] = None) -> HTMLResponse:
        who = ""
        if user:
            who = (f'<span class="who">{esc(user["name"])} · key <code>{esc(user["label"])}</code></span>'
                   f'<form method="post" action="{mount}/logout" class="inline"><button>Log out</button></form>')
        meta = f'<meta http-equiv="refresh" content="{int(refresh)}">' if refresh else ""
        return HTMLResponse(f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>{esc(title)} · specimux console</title>{meta}
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {{ --fg: #1d1d1f; --muted: #6b6b70; --line: #e2e2e6; --accent: #2b6cb0; --bg: #fff; }}
@media (prefers-color-scheme: dark) {{ :root {{ --fg: #ececf0; --muted: #a0a0a8; --line: #333; --accent: #7fb0e8; --bg: #141416; }} }}
body {{ margin: 0; padding: 0 16px 48px; font: 15px/1.5 system-ui, sans-serif; color: var(--fg); background: var(--bg); max-width: 960px; margin: 0 auto; }}
header {{ display: flex; align-items: center; gap: 16px; padding: 16px 0; border-bottom: 1px solid var(--line); margin-bottom: 24px; flex-wrap: wrap; }}
header h1 {{ font-size: 18px; margin: 0; flex: 1; }} header h1 a {{ color: inherit; text-decoration: none; }}
.who {{ color: var(--muted); }} .inline {{ display: inline; }}
a {{ color: var(--accent); }} code {{ font-size: 13px; }}
table {{ border-collapse: collapse; width: 100%; }} th, td {{ text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); vertical-align: top; }}
th {{ color: var(--muted); font-weight: 500; font-size: 13px; }}
.state {{ font-variant: small-caps; }} .muted {{ color: var(--muted); }}
form.card, .card {{ border: 1px solid var(--line); border-radius: 8px; padding: 16px; margin: 16px 0; }}
label {{ display: block; margin: 10px 0 4px; font-size: 13px; color: var(--muted); }}
input[type=text], input[type=number], input[type=password], select {{ width: 100%; max-width: 480px; padding: 6px 8px; font: inherit; color: inherit; background: transparent; border: 1px solid var(--line); border-radius: 4px; }}
button {{ font: inherit; padding: 6px 12px; border-radius: 4px; border: 1px solid var(--line); background: transparent; color: inherit; cursor: pointer; }}
button.primary {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
.actions form {{ display: inline; margin-right: 8px; }}
pre {{ background: rgba(127,127,127,.12); padding: 12px; border-radius: 6px; overflow-x: auto; }}
.error {{ color: #c0392b; }}
</style></head>
<body><header><h1><a href="{mount}/">specimux console</a></h1>{who}</header>
{body}
</body></html>""")

    def esc(v) -> str:
        return html.escape(str(v if v is not None else ""), quote=True)

    async def api_error(resp: httpx.Response) -> str:
        try:
            return resp.json().get("error") or resp.text
        except Exception:
            return resp.text or f"HTTP {resp.status_code}"

    # --- login ---

    @app.get("/login")
    async def login_form(request: Request, next: Optional[str] = None, error: Optional[str] = None):
        err = f'<p class="error">{esc(error)}</p>' if error else ""
        nxt = f'<input type="hidden" name="next" value="{esc(next)}">' if next else ""
        return page("Log in", f"""
<form method="post" action="{mount}/login" class="card">
<h2>Log in with a service key</h2>{err}{nxt}
<label for="key">Service key</label>
<input type="password" id="key" name="key" autocomplete="off" autofocus>
<p><button class="primary">Log in</button></p>
<p class="muted">A key looks like <code>host.…</code> and was issued by the run API's operator. It is kept in a cookie in this browser only.</p>
</form>""")

    @app.post("/login")
    async def login(request: Request):
        form = await request.form()
        key = str(form.get("key") or "").strip()
        nxt = str(form.get("next") or "")
        async with client(key) as c:
            r = await c.get("/v1/hosts/me")
        if r.status_code != 200:
            return RedirectResponse(f"{mount}/login?{urlencode({'error': 'That key was not accepted', **({'next': nxt} if nxt else {})})}",
                                    status_code=303)
        me = r.json()
        target = nxt if nxt.startswith(mount + "/") or nxt.startswith(run_api + mount + "/") else f"{mount}/"
        resp = RedirectResponse(target, status_code=303)
        set_session(resp, {"key": key, "host": me["host"], "label": me["label"], "name": me.get("name") or me["host"],
                           "at": time.time()})
        return resp

    @app.post("/logout")
    async def logout():
        resp = RedirectResponse(f"{mount}/login", status_code=303)
        resp.delete_cookie(COOKIE, path=mount + "/")
        return resp

    # --- runs ---

    STATE_HELP = {
        "created": "waiting for the upload", "uploading": "receiving files",
        "input_complete": "queued for the next stage", "basecalling": "basecalling on the GPU",
        "running": "engine running",
        "finalizing": "engine finalizing", "sealing": "copying results to storage",
        "sealed": "done", "failed": "failed", "incomplete": "incomplete",
    }

    def run_row(run: dict) -> str:
        rid = run["id"]
        when = time.strftime("%Y-%m-%d %H:%M", time.localtime(run.get("created", 0)))
        state = run.get("state", "")
        spec = run.get("spec") or {}
        results = (f'<a href="{mount}/runs/{esc(rid)}/results.zip">results.zip</a>'
                   if run.get("sealed") and not run["sealed"].get("error") else '<span class="muted">—</span>')
        return (f'<tr><td><a href="{mount}/runs/{esc(rid)}"><code>{esc(rid)}</code></a></td>'
                f'<td class="state">{esc(state)}<br><span class="muted">{esc(STATE_HELP.get(state, ""))}</span></td>'
                f'<td>{esc(spec.get("mode", "batch"))} · {esc(spec.get("input", "fastq"))}</td>'
                f'<td>{esc(run.get("user_id"))}</td><td>{esc(when)}</td>'
                f'<td><a href="{esc(run.get("dashboard_url") or "")}">dashboard</a></td><td>{results}</td></tr>')

    def load_line(load: Optional[dict]) -> str:
        """The service's load across every host, one line: what a user
        needs to know to expect a wait."""
        if not load:
            return ""
        parts = []
        for stage, label, unit in (("dorado", "Basecalling", "GPU"), ("engine", "Pipeline", "worker")):
            st = (load.get("stages") or {}).get(stage)
            if not st:
                continue
            text = f"{label}: {st['busy']} of {st['slots']} {unit}{'s' if st['slots'] != 1 else ''} busy"
            if st.get("waiting_for_machine"):
                text += f" ({st['waiting_for_machine']} waiting for a machine)"
            if st.get("queued"):
                text += f", {st['queued']} queued"
            parts.append(text)
        for key, label in (("uploading", "uploading"), ("sealing", "packaging results")):
            if load.get(key):
                parts.append(f"{load[key]} {label}")
        return f'<p class="muted" id="load">Service load (all users) · {esc(" · ".join(parts))}</p>'

    async def fetch_load(c) -> Optional[dict]:
        try:
            r = await c.get("/v1/load")
            return r.json() if r.status_code == 200 else None
        except httpx.HTTPError:
            return None

    @app.get("/")
    async def index(request: Request):
        user = whoami(request)
        if not user:
            return login_redirect()
        async with client(user["key"]) as c:
            r = await c.get("/v1/runs")
            load = await fetch_load(c)
        if r.status_code == 403:
            return login_redirect()
        runs = sorted(r.json().get("runs", []), key=lambda x: -x.get("created", 0))
        rows = "".join(run_row(x) for x in runs) or '<tr><td colspan="7" class="muted">No runs yet.</td></tr>'
        return page("Runs", f"""
<p><a href="{mount}/new"><button class="primary">New run</button></a></p>
{load_line(load)}
<table><thead><tr><th>Run</th><th>State</th><th>Mode</th><th>User</th><th>Created</th><th></th><th></th></tr></thead>
<tbody>{rows}</tbody></table>""", user)

    @app.get("/new")
    async def new_form(request: Request, error: Optional[str] = None):
        user = whoami(request)
        if not user:
            return login_redirect(f"{mount}/new")
        async with client(user["key"]) as c:
            r = await c.get("/v1/options")
            listed = await c.get("/v1/runs")
            load = await fetch_load(c)
        opts = r.json() if r.status_code == 200 else {}
        # references this host's runs used, newest first: the service keeps
        # them by content, so a later run names one instead of sending it again
        used: dict[str, str] = {}
        for run in sorted(listed.json().get("runs", []) if listed.status_code == 200 else [],
                          key=lambda x: -(x.get("created") or 0)):
            sha = (run.get("spec") or {}).get("reference_sha256")
            if sha and not used.get(sha):
                used[sha] = (run.get("spec") or {}).get("reference_name") or ""
        profiles = "".join(f'<option value="{esc(p)}"{" selected" if p == "default" else ""}>{esc(p)}</option>'
                           for p in (opts.get("profiles") or ["default"]))
        refs = "".join(f'<option value="{esc(sha)}">{esc(name or "reference")} ({esc(sha[:12])})</option>' for sha, name in used.items())
        bc = opts.get("basecall_defaults") or {}
        models = "".join(f'<option value="{esc(m)}"{" selected" if m == bc.get("model") else ""}>{esc(m)}</option>'
                         for m in (opts.get("dorado_models") or []))
        err = f'<p class="error">{esc(error)}</p>' if error else ""
        return page("New run", f"""
<form method="post" action="{mount}/new" enctype="multipart/form-data" class="card">
<h2>New run</h2>{err}{load_line(load)}
<label for="primers">Primers (FASTA)</label><input type="file" id="primers" name="primers" required>
<label for="specimens">Specimens (Index.txt)</label><input type="file" id="specimens" name="specimens" required>
<label for="reference">Reference database (FASTA, optional; <code>name="…"</code> headers)</label><input type="file" id="reference" name="reference">
<label for="reference_sha256">…or one used before (not sent again)</label>
<select id="reference_sha256" name="reference_sha256"><option value="">none</option>{refs}</select>
<label for="profile">Profile</label><select id="profile" name="profile">{profiles}</select>
<label for="min_reads">Minimum reads per specimen</label><input type="number" id="min_reads" name="min_reads" value="10" min="1">
<label for="mode">Mode</label><select id="mode" name="mode">
<option value="batch">Batch: upload a finished run, then process it</option>
<option value="live">Live: process FASTQ as it arrives, while sequencing</option></select>
<span class="muted">Live mode takes FASTQ that MinKNOW basecalls on the sequencing machine; the dashboard fills while sequencing continues.</span>
<label for="input">Input</label><select id="input" name="input">
<option value="fastq">FASTQ, basecalled on your own machine</option><option value="pod5">POD5, basecalled here (dorado on a GPU)</option></select>
<fieldset><legend>Basecalling (POD5 input only)</legend>
<label for="model">Dorado model</label><select id="model" name="model">{models}</select>
<label for="min_length">Read length window</label>
<span class="inline"><input type="number" id="min_length" name="min_length" value="{esc(bc.get("min_length", 400))}" min="0" style="width:7em"> to
<input type="number" id="max_length" name="max_length" value="{esc(bc.get("max_length", 2000))}" min="0" style="width:7em"> bases</span>
<span class="muted">400 to 2000 for the full ITS amplicon; 100 to 700 for ITS2 alone.</span>
<label for="min_qscore">Minimum read qscore (optional)</label><input type="number" id="min_qscore" name="min_qscore" step="0.1" min="0" style="width:7em">
</fieldset>
<label for="user_id">Submitted by</label><input type="text" id="user_id" name="user_id" value="{esc(user["label"])}">
<input type="hidden" name="client_token" value="{esc(hashlib.sha1(f"{time.time()}{user['label']}".encode()).hexdigest())}">
<p><button class="primary">Create run</button> <span class="muted">The upload comes next, with the job code.</span></p>
</form>""", user)

    @app.post("/new")
    async def new_run(request: Request):
        user = whoami(request)
        if not user:
            return login_redirect()
        form = await request.form()
        kind = str(form.get("input") or "fastq")
        mode = "live" if form.get("mode") == "live" else "batch"
        spec = {"mode": mode, "input": kind, "profile": str(form.get("profile") or "default")}
        try:
            spec["min_reads"] = int(form.get("min_reads") or 10)
            if kind == "pod5":
                spec["basecall"] = {"model": str(form.get("model") or "") or None,
                                    "min_length": int(form.get("min_length") or 0),
                                    "max_length": int(form.get("max_length") or 0),
                                    "min_qscore": float(form.get("min_qscore")) if form.get("min_qscore") else None}
        except ValueError:
            return RedirectResponse(f"{mount}/new?{urlencode({'error': 'Minimum reads and the basecalling numbers must be numbers'})}",
                                    status_code=303)
        files = {}
        for role in ("primers", "specimens", "reference"):
            f = form.get(role)
            if isinstance(f, UploadFile) and f.filename:
                files[role] = (f.filename, await f.read())
        data = {"spec": json.dumps(spec), "user_id": str(form.get("user_id") or user["label"]),
                "client_token": str(form.get("client_token") or "")}
        if form.get("reference_sha256") and "reference" not in files:
            data["reference_sha256"] = str(form.get("reference_sha256"))
        async with client(user["key"]) as c:
            r = await c.post("/v1/runs", data=data, files=files)
        if r.status_code != 200:
            return RedirectResponse(f"{mount}/new?{urlencode({'error': await api_error(r)})}", status_code=303)
        run = r.json()
        return page("Run created", created_body(run), user)

    def created_body(run: dict) -> str:
        rid = run["id"]
        code = run.get("job_code")
        pod5 = (run.get("spec") or {}).get("input") == "pod5"
        command = f"specimux-cloud upload --run-api {esc(run_api)} --job-code {esc(code)}"
        if (run.get("spec") or {}).get("mode") == "live":
            return f"""
<div class="card"><h2>Run <code>{esc(rid)}</code> created · live</h2>
<p>This job code is shown once. Give it to the uploader on the sequencing machine:</p>
<pre>{esc(code)}</pre>
<p>While MinKNOW is sequencing, point the uploader at its run folder:</p>
<pre>{command} &lt;MinKNOW run folder&gt;</pre>
<p>It uploads each FASTQ as MinKNOW writes it (not the <code>fastq_fail</code> reads). Processing starts with the first file and the dashboard fills while sequencing continues. When MinKNOW writes its final summary the uploader completes the upload and the run finishes processing and packages its results.</p>
<p>Stopped sequencing early, or the uploader is gone? The run page's <em>Upload is complete</em> button finishes with the files already received; a live upload that stops for three hours is completed the same way by itself. <a href="{mount}/runs/{esc(rid)}">Run page</a> · <a href="{esc(run.get("dashboard_url") or "")}">Dashboard</a></p>
</div>"""
        return f"""
<div class="card"><h2>Run <code>{esc(rid)}</code> created</h2>
<p>This job code is shown once. Give it to the uploader on the sequencing machine:</p>
<pre>{esc(code)}</pre>
<p>Upload a folder of {"POD5" if pod5 else "FASTQ"} files that is already complete with:</p>
<pre>{command} --once &lt;folder&gt;</pre>
<p>or, while MinKNOW is still sequencing, leave out <code>--once</code> and point it at the run folder: it uploads each file as it is written and finishes when MinKNOW writes its final summary.</p>
<p>{"Basecalling, then the engine, start" if pod5 else "The engine starts"} when the uploader reports the upload complete. If an uploader is still waiting after the files are all up, the run page's <em>Upload is complete</em> button finishes the upload with the files already received. <a href="{mount}/runs/{esc(rid)}">Run page</a> · <a href="{esc(run.get("dashboard_url") or "")}">Dashboard</a></p>
</div>"""

    @app.get("/runs/{run_id}")
    async def run_page(request: Request, run_id: str, error: Optional[str] = None):
        user = whoami(request)
        if not user:
            return login_redirect(f"{mount}/runs/{quote(run_id)}")
        async with client(user["key"]) as c:
            r = await c.get(f"/v1/runs/{run_id}")
        if r.status_code != 200:
            return page("Run", f'<p class="error">{esc(await api_error(r))}</p>', user)
        run = r.json()
        state = run.get("state")
        active = state in ("uploading", "input_complete", "basecalling", "running", "finalizing", "sealing", "created")
        err = f'<p class="error">{esc(error)}</p>' if error else ""
        help_text = STATE_HELP.get(state, "")
        if run.get("uploads_open") and state in ("running", "finalizing"):
            help_text += ", receiving files (live)"
        rows = [("State", f'<span class="state">{esc(state)}</span> <span class="muted">{esc(help_text)}</span>'),
                ("Created", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(run.get("created", 0)))),
                ("Submitted by", esc(run.get("user_id"))),
                ("Spec", f"<code>{esc(json.dumps(run.get('spec') or {}))}</code>"),
                ("Versions", esc(json.dumps(run.get("versions") or {})))]
        if run.get("manifest"):
            rows.append(("Input", f"{len(run['manifest'])} file(s), {sum(m.get('size', 0) for m in run['manifest']):,} bytes"))
        if run.get("basecalling"):
            bc = run["basecalling"]
            done, total = bc.get("done", 0), bc.get("total", 0)
            text = f"{done} of {total} file(s)" + (" done" if bc.get("finished") else "")
            if bc.get("reads_in"):
                text += f" · {bc['reads_in']:,} reads called, {bc.get('reads_out', 0):,} within the length window"
            rows.append(("Basecalling", esc(text)))
        if run.get("effective_config"):
            rows.append(("Effective configuration", f"<code>{esc(json.dumps(run['effective_config']))}</code>"))
        if run.get("exit"):
            ex = run["exit"]
            label = {"dorado": "Dorado exit", "upload": "Upload"}.get(ex.get("stage"), "Engine exit")
            detail = [f"code {esc(ex.get('code'))}"] if ex.get("code") is not None else []
            if ex.get("errors") is not None:
                detail.append(f"{esc(ex.get('errors'))} error(s) reported")
            text = ", ".join(detail) + (f" · {esc(ex.get('reason'))}" if ex.get("reason") and detail
                                        else esc(ex.get("reason") or ""))
            rows.append((label, text))
            if ex.get("log_tail"):
                rows.append(("Log tail", f"<pre>{esc(ex['log_tail'][-1500:])}</pre>"))
        if run.get("sealed"):
            se = run["sealed"]
            if se.get("error"):
                rows.append(("Seal", f'<span class="error">failed: {esc(se["error"])}</span>'))
            else:
                links = " · ".join(f'<a href="{mount}/runs/{esc(run_id)}/{n}.zip">{n}.zip</a> ({se.get(n + "_bytes", 0):,} bytes)'
                                   for n in ("results", "output", "reads") if se.get(n))
                rows.append(("Downloads", links + f' · <a href="{mount}/runs/{esc(run_id)}/events.jsonl">events.jsonl</a>'))
        if run.get("pending_commands"):
            rows.append(("Pending commands", esc(len(run["pending_commands"]))))
        rows.append(("Public viewing", public_controls(run_id, run.get("public") or {})))
        table = "".join(f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in rows)
        actions = [f'<a href="{esc(run.get("dashboard_url") or "")}"><button class="primary">Open dashboard</button></a>']
        if run.get("uploads_open"):
            actions.append(f'<form method="post" action="{mount}/runs/{esc(run_id)}/job-code"><button>New job code</button></form>')
            actions.append(f'<form method="post" action="{mount}/runs/{esc(run_id)}/complete"><button>Upload is complete</button></form>')
        if state in ("created", "uploading", "input_complete", "basecalling", "running", "finalizing") \
                and not run.get("cancel"):
            actions.append(f'<form method="post" action="{mount}/runs/{esc(run_id)}/cancel" onsubmit="return confirm(\'Cancel this run? A running stage is stopped and the run fails.\')"><button>Cancel run</button></form>')
        if state == "failed" and (run.get("exit") or {}).get("stage") == "dorado":
            actions.append(f'<form method="post" action="{mount}/runs/{esc(run_id)}/retry"><button>Retry basecalling</button></form>')
        if state not in ("running", "finalizing", "sealing", "basecalling"):
            actions.append(f'<form method="post" action="{mount}/runs/{esc(run_id)}/delete" onsubmit="return confirm(\'Delete this run and its results? The uploaded archive is kept.\')"><button>Delete run</button></form>')
        return page(f"Run {run_id}", f"""
<h2>Run <code>{esc(run_id)}</code></h2>{err}
<table>{table}</table>
<p class="actions">{"".join(actions)}</p>
{'<p class="muted">This page refreshes every 10 seconds while the run is active.</p>' if active else ''}""",
                    user, refresh=10 if active else None)

    @app.post("/runs/{run_id}/job-code")
    async def new_job_code(request: Request, run_id: str):
        user = whoami(request)
        if not user:
            return login_redirect()
        async with client(user["key"]) as c:
            r = await c.post(f"/v1/runs/{run_id}/job-code")
        if r.status_code != 200:
            return RedirectResponse(f"{mount}/runs/{quote(run_id)}?{urlencode({'error': await api_error(r)})}", status_code=303)
        return page("New job code", created_body(r.json()).replace("created</h2>", "· new job code</h2>"), user)

    @app.post("/runs/{run_id}/complete")
    async def complete(request: Request, run_id: str):
        user = whoami(request)
        if not user:
            return login_redirect()
        async with client(user["key"]) as c:
            r = await c.post(f"/v1/runs/{run_id}/complete")
        q = f"?{urlencode({'error': await api_error(r)})}" if r.status_code != 200 else ""
        return RedirectResponse(f"{mount}/runs/{quote(run_id)}{q}", status_code=303)

    def public_controls(run_id: str, pub: dict) -> str:
        """The owner's public-viewing switch and link, on the run page."""
        def button(action: str, label: str, confirm: str = "") -> str:
            onsubmit = f' onsubmit="return confirm(\'{confirm}\')"' if confirm else ""
            return (f'<form method="post" action="{mount}/runs/{esc(run_id)}/public" style="display:inline"{onsubmit}>'
                    f'<input type="hidden" name="action" value="{action}"><button>{label}</button></form>')
        if not pub.get("enabled"):
            return ('<span class="muted">Off. A public link lets anyone who has it watch the dashboard, '
                    'no login needed; the dashboard shows it as a QR code.</span> ' + button("enable", "Share publicly"))
        star = "may star specimens" if pub.get("allow_starring", True) else "may not star specimens"
        dl = "may download results" if pub.get("allow_downloads") else "may not download results"
        return (f'<span class="state">on</span> · anyone with the link can watch; viewers {star} and {dl}.'
                f'<pre>{esc(pub.get("url") or "")}</pre>'
                + button("disable", "Stop sharing", "Stop sharing? Everyone viewing through the link loses access.")
                + " " + button("new_link", "New link", "Make a new link? The current one stops working.")
                + " " + button("toggle_starring", "Block starring" if pub.get("allow_starring", True) else "Allow starring")
                + " " + button("toggle_downloads", "Allow downloads" if not pub.get("allow_downloads") else "Block downloads"))

    @app.post("/runs/{run_id}/public")
    async def public(request: Request, run_id: str):
        user = whoami(request)
        if not user:
            return login_redirect()
        action = str((await request.form()).get("action") or "")
        async with client(user["key"]) as c:
            current = ((await c.get(f"/v1/runs/{run_id}")).json() or {}).get("public") or {}
            body = {"enable": {"enabled": True}, "disable": {"enabled": False},
                    "new_link": {"enabled": True, "new_link": True},
                    "toggle_starring": {"allow_starring": not current.get("allow_starring", True)},
                    "toggle_downloads": {"allow_downloads": not current.get("allow_downloads", False)},
                    }.get(action)
            if body is None:
                return RedirectResponse(f"{mount}/runs/{quote(run_id)}", status_code=303)
            r = await c.post(f"/v1/runs/{run_id}/public", json=body)
        q = f"?{urlencode({'error': await api_error(r)})}" if r.status_code != 200 else ""
        return RedirectResponse(f"{mount}/runs/{quote(run_id)}{q}", status_code=303)

    @app.post("/runs/{run_id}/cancel")
    async def cancel(request: Request, run_id: str):
        user = whoami(request)
        if not user:
            return login_redirect()
        async with client(user["key"]) as c:
            r = await c.post(f"/v1/runs/{run_id}/cancel")
        q = f"?{urlencode({'error': await api_error(r)})}" if r.status_code != 200 else ""
        return RedirectResponse(f"{mount}/runs/{quote(run_id)}{q}", status_code=303)

    @app.post("/runs/{run_id}/retry")
    async def retry(request: Request, run_id: str):
        user = whoami(request)
        if not user:
            return login_redirect()
        async with client(user["key"]) as c:
            r = await c.post(f"/v1/runs/{run_id}/retry")
        q = f"?{urlencode({'error': await api_error(r)})}" if r.status_code != 200 else ""
        return RedirectResponse(f"{mount}/runs/{quote(run_id)}{q}", status_code=303)

    @app.post("/runs/{run_id}/delete")
    async def delete(request: Request, run_id: str):
        user = whoami(request)
        if not user:
            return login_redirect()
        async with client(user["key"]) as c:
            r = await c.delete(f"/v1/runs/{run_id}")
        if r.status_code != 200:
            return RedirectResponse(f"{mount}/runs/{quote(run_id)}?{urlencode({'error': await api_error(r)})}", status_code=303)
        return RedirectResponse(f"{mount}/", status_code=303)

    @app.get("/runs/{run_id}/{package}.zip")
    async def download(request: Request, run_id: str, package: str):
        """Forward the run API's redirect to the presigned URL: the bytes
        never pass through the console."""
        return await forward(request, run_id, f"/v1/runs/{run_id}/{package}.zip")

    @app.get("/runs/{run_id}/events.jsonl")
    async def download_log(request: Request, run_id: str):
        return await forward(request, run_id, f"/v1/runs/{run_id}/events.jsonl")

    async def forward(request: Request, run_id: str, path: str):
        user = whoami(request)
        if not user:
            return login_redirect(f"{mount}{path[len('/v1'):]}")
        async with client(user["key"]) as c:
            r = await c.get(path)
        if r.status_code in (301, 302, 303, 307):
            return RedirectResponse(r.headers["location"], status_code=302)
        return RedirectResponse(f"{mount}/runs/{quote(run_id)}?{urlencode({'error': await api_error(r)})}", status_code=303)

    # --- the authorize route: the host's part of the dashboard session ---

    @app.get("/authorize")
    async def authorize(request: Request, run: str = "", scope: str = "admin"):
        """Mint a run token for the logged-in console user. With a
        ``return`` URL (the page navigated here): redirect back with the
        token in the fragment. Without (the page fetched this): JSON."""
        back = request.query_params.get("return")
        user = whoami(request)
        if not user:
            if back:
                here = f"{mount}/authorize?{urlencode({'run': run, 'return': back})}"
                return login_redirect(here)
            return JSONResponse(status_code=401, content={"error": "Log in to the console first"})
        if not run:
            return JSONResponse(status_code=400, content={"error": "run required"})
        scope = scope if scope in ("view", "admin") else "admin"
        async with client(user["key"]) as c:
            r = await c.post(f"/v1/runs/{run}/tokens", json={"user": user["label"], "scope": scope})
        if r.status_code != 200:
            return JSONResponse(status_code=r.status_code, content={"error": await api_error(r)})
        tok = r.json()
        if back:
            # only back to that run's own dashboard, on the run API
            if not back.split("#")[0].startswith(tok["dashboard_url"]):
                return JSONResponse(status_code=400, content={"error": "return must be the run's dashboard"})
            return RedirectResponse(back.split("#")[0] + "#token=" + quote(tok["token"], safe=""), status_code=302)
        return {"token": tok["token"], "expires_in": tok["expires_in"], "scope": tok["scope"]}

    return app
