"""The wrappers' SIGTERM handling (stopsignal.StopSignal)."""
import os
import signal
import subprocess
import sys

import pytest

from specimux_cloud.stopsignal import StopRequested, StopSignal


@pytest.fixture
def restore_sigterm():
    old = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, old)


def test_sigterm_stops_the_running_child(restore_sigterm):
    stop = StopSignal().install()
    stop.child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    os.kill(os.getpid(), signal.SIGTERM)
    assert stop.child.wait(timeout=10) != 0
    assert stop.stopped


def test_sigterm_without_a_child_interrupts_the_wrapper(restore_sigterm):
    stop = StopSignal().install()
    with pytest.raises(StopRequested):
        os.kill(os.getpid(), signal.SIGTERM)
        for _ in range(1000):  # the handler runs between bytecodes
            pass
    assert stop.stopped


def test_once_shielded_a_sigterm_is_only_recorded(restore_sigterm):
    stop = StopSignal().install()
    stop.shield()
    os.kill(os.getpid(), signal.SIGTERM)
    for _ in range(1000):
        pass
    assert stop.stopped
