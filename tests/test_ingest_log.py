import json
import threading
import time

from specimux_suite.state import PipelineState

from specimux_cloud.runapi.events import IngestLog


def _ev(v, type_="specimen.updated", **data):
    return {"v": 1, "version": v, "type": type_, "ts": "2026-01-01T00:00:00+00:00",
            "data": {"specimen_id": "S1", "total_reads": v, **data}}


def test_ingest_dedupes_orders_and_fills_gaps():
    log = IngestLog()
    state = PipelineState()
    log.add_listener(state.apply)
    assert log.ingest([_ev(1), _ev(2)]) == 2
    assert log.ingest([_ev(1), _ev(2)]) == 0            # a retried batch
    assert log.ingest([_ev(4), _ev(5)]) == 0            # ahead of a gap: held
    assert log.version == 2 and log.gap == 4
    assert log.ingest([_ev(3)]) == 3                    # gap filled: 3, 4, 5 land
    assert log.version == 5 and log.gap is None
    assert state.version == 5 and state.specimens["S1"].total_reads == 5
    assert [e.version for e in log.tail(after_version=3, timeout=0)] == [4, 5]
    assert list(log.tail(after_version=5, timeout=0.05)) == []


def test_tail_wakes_on_ingest():
    log = IngestLog()
    got = []

    def waiter():
        got.extend(e.version for e in log.tail(after_version=0, timeout=5))

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.1)
    log.ingest([_ev(1)])
    t.join(timeout=2)
    assert got == [1]


def test_reconcile_from_file(tmp_path):
    p = tmp_path / "events.jsonl"
    p.write_text("".join(json.dumps(_ev(i)) + "\n" for i in range(1, 8)))
    log = IngestLog(p)
    assert log.version == 7
    # ingest that skipped ahead is reconciled from the file
    p.write_text(p.read_text() + json.dumps(_ev(8)) + "\n" + json.dumps(_ev(9)) + "\n")
    assert log.ingest([_ev(10)]) == 0 and log.gap == 10
    assert log.reconcile_from_file() == 3
    assert log.version == 10 and log.gap is None
