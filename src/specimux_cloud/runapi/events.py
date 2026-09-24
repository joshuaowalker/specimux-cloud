"""The run API's per-run event store: fed by plugin ingest, backed by the
engine's own log.

The engine's ``events.jsonl`` in the run directory is the persisted record
(one writer, monotonic versions). ``IngestLog`` keeps the run API's copy
current from ingest batches for latency and fan-out, dedupes by version,
and can reconcile from the file when it starts or when ingest left a gap.
It offers the viewer factory's ``EventSource`` contract (``tail``,
``version``) plus ``add_listener``, so ``create_viewer_app`` and
``PipelineState.apply`` work over it exactly as over ``EventLog``.
"""

import json
import logging
import threading
import time
from pathlib import Path
from typing import Callable, Iterable, Optional

from specimux_suite.events import Event, _dict_to_event

logger = logging.getLogger(__name__)


class IngestLog:
    def __init__(self, file_path: Optional[Path] = None):
        self.file_path = Path(file_path) if file_path else None
        self._events: list[Event] = []          # index i holds version i+1
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._listeners: list[Callable[[Event], None]] = []
        self._pending: dict[int, Event] = {}    # arrived out of order, waiting for the gap
        if self.file_path:
            self.reconcile_from_file()

    @property
    def version(self) -> int:
        with self._lock:
            return len(self._events)

    def add_listener(self, fn: Callable[[Event], None]) -> None:
        with self._lock:
            self._listeners.append(fn)

    # --- feeding ---

    def ingest(self, events: Iterable[dict]) -> int:
        """Append event dicts in version order; duplicates are ignored, an
        event beyond the next version waits for the gap to fill. Returns
        how many were appended."""
        appended = 0
        with self._condition:
            for d in events:
                try:
                    ev = _dict_to_event(d)
                except (KeyError, TypeError):
                    logger.warning(f"Ingest skipped a malformed event: {d!r}")
                    continue
                if ev.version <= len(self._events):
                    continue  # already have it (a retried batch)
                self._pending[ev.version] = ev
            while (len(self._events) + 1) in self._pending:
                ev = self._pending.pop(len(self._events) + 1)
                self._events.append(ev)
                appended += 1
                for fn in self._listeners:
                    try:
                        fn(ev)
                    except Exception:
                        logger.exception(f"Ingest listener failed for {ev.type}")
            if appended:
                self._condition.notify_all()
        return appended

    @property
    def gap(self) -> Optional[int]:
        """The lowest version held back by a missing predecessor, if any."""
        with self._lock:
            return min(self._pending) if self._pending else None

    def reconcile_from_file(self) -> int:
        """Fill from the engine's log on disk (startup, or a gap)."""
        if not self.file_path or not self.file_path.exists():
            return 0
        appended = 0
        with open(self.file_path, encoding="utf-8") as f:
            batch = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    batch.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
                if len(batch) >= 1000:
                    appended += self.ingest(batch)
                    batch = []
            if batch:
                appended += self.ingest(batch)
        return appended

    # --- reading (the viewer's contract) ---

    def replay(self) -> Iterable[Event]:
        with self._lock:
            snapshot = list(self._events)
        yield from snapshot

    def tail(self, after_version: int = 0, timeout: float = 30.0) -> Iterable[Event]:
        with self._condition:
            if len(self._events) <= after_version and timeout > 0:
                deadline = time.monotonic() + timeout
                while len(self._events) <= after_version:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(timeout=min(remaining, 1.0))
            events = list(self._events[after_version:])
        yield from events
