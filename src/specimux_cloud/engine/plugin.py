"""The cloud plugin: what the engine loads inside a compute job.

Two threads over the suite's extension interface (docs/DESIGN.md, C1):

- **Events out.** The suite's ``EventForwarder`` tails the log and POSTs
  batches to the run API's ingest endpoint with the job secret and the
  run's generation as a fencing token. The log is the buffer; ingest is
  for latency and fan-out.
- **Commands in.** A poller long-polls the run API for commands (the
  run API fronts the queue backend, SQS or memory), applies each through
  the commands facade with the command id the run API assigned, and
  acknowledges it. The facade dedupes by id from the log, so a
  redelivered command is a noop; the outcome event travels back through
  ingest, which is how the run API marks the command applied.

Loaded by ``specimux-suite ... --plugin cloud`` with options
``run_id``, ``run_api``, ``job_secret`` and ``generation``, which the
wrapper passes from its environment.
"""

import json
import logging
import threading
import urllib.error
import urllib.request
from typing import Optional

from specimux_suite.forward import EventForwarder
from specimux_suite.util import USER_AGENT

logger = logging.getLogger(__name__)


class CommandPoller:
    def __init__(self, base_url: str, run_id: str, job_secret: str, wait_s: float = 20.0):
        self.url = f"{base_url.rstrip('/')}/v1/runs/{run_id}/commands"
        self.headers = {"X-Job-Secret": job_secret, "User-Agent": USER_AGENT}
        self.wait_s = wait_s
        self.commands = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.applied = 0

    def start(self, commands) -> None:
        self.commands = commands
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="command-poller", daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _request(self, url: str, method: str = "GET", timeout: float = 30.0):
        req = urllib.request.Request(url, method=method, headers=self.headers,
                                     data=b"{}" if method == "POST" else None)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read() or b"{}")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                body = self._request(f"{self.url}/next?wait={self.wait_s}", timeout=self.wait_s + 15)
            except (urllib.error.URLError, OSError, ValueError) as e:
                logger.warning(f"Command poll failed: {e}")
                self._stop.wait(5.0)
                continue
            for msg in body.get("commands") or []:
                self._apply(msg)

    def _apply(self, msg: dict) -> None:
        cid = msg.get("id")
        result = self.commands.dispatch(
            str(msg.get("command") or ""), msg.get("args") or {},
            actor=str(msg.get("actor") or "remote"), command_id=cid,
        )
        logger.info(f"Command {result.command} ({cid}) from {msg.get('actor')}: {result.outcome}"
                    + (f" — {result.reason}" if result.reason else ""))
        self.applied += 1
        mid = msg.get("message_id")
        if mid:
            try:
                self._request(f"{self.url}/{mid}/ack", method="POST", timeout=15)
            except (urllib.error.URLError, OSError, ValueError) as e:
                logger.warning(f"Command ack failed (it will be redelivered and deduped): {e}")


class CloudPlugin:
    def __init__(self, run_id: str, run_api: str, job_secret: str, generation: int):
        self.run_id = run_id
        self.run_api = run_api.rstrip("/")
        self.forwarder = EventForwarder(
            f"{self.run_api}/v1/runs/{run_id}/ingest",
            headers={"X-Job-Secret": job_secret},
            extra={"run_id": run_id, "generation": int(generation)},
        )
        self.poller = CommandPoller(self.run_api, run_id, job_secret)

    def start(self, context) -> None:
        self.forwarder.start(context)
        self.poller.start(context.commands)
        logger.info(f"Cloud plugin attached: run {self.run_id}, generation "
                    f"{self.forwarder.extra['generation']}, run API {self.run_api}")

    def shutdown(self) -> None:
        self.poller.shutdown()
        # Give the last events (finalization.completed, command outcomes)
        # a fair chance to land before the wrapper reports the exit
        self.forwarder.shutdown(flush_timeout=15.0)


def cloud_plugin(options: dict) -> CloudPlugin:
    """Entry point ``cloud``."""
    missing = [k for k in ("run_id", "run_api", "job_secret", "generation") if not options.get(k)]
    if missing:
        raise ValueError(f"cloud plugin needs options: {', '.join(missing)}")
    return CloudPlugin(options["run_id"], options["run_api"], options["job_secret"],
                       int(options["generation"]))
