"""SIGTERM handling for the job wrappers (standard library only).

Batch stops a job (``terminate-job``, a Spot interruption, a timeout) by
sending SIGTERM and, about 30 seconds later, SIGKILL. Python's default
SIGTERM ends the process at once, so a wrapper never got to report its
exit and the run waited for the run API's reconcile. With this handler
the wrapper stops its child and reports a stopped exit (143) inside that
window.
"""

import signal
import subprocess
from typing import Optional

STOPPED_EXIT = 143  # 128 + SIGTERM
# The exit report's retry window once stopped: SIGKILL follows in ~30 s
STOPPED_REPORT_PATIENCE_S = 20.0


class StopRequested(Exception):
    """SIGTERM arrived while no child was running (staging, uploading)."""


class StopSignal:
    def __init__(self) -> None:
        self.stopped = False
        self.shielded = False
        self.child: Optional[subprocess.Popen] = None

    def install(self) -> "StopSignal":
        signal.signal(signal.SIGTERM, self._handle)
        return self

    def shield(self) -> None:
        """From here on a SIGTERM is only recorded: the wrapper is
        reporting its exit and must not be interrupted."""
        self.shielded = True

    def _handle(self, signum, frame) -> None:
        if self.stopped:
            return
        self.stopped = True
        if self.shielded:
            return
        child = self.child
        if child is not None and child.poll() is None:
            child.terminate()  # its wait() returns and the wrapper reports
        else:
            raise StopRequested("stopped (SIGTERM)")
