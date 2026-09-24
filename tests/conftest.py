import json
from pathlib import Path

import pytest

SUITE_FIXTURE = Path(__file__).resolve().parent.parent.parent / "specimux-suite" / "tests" / "fixtures" / "parity" / "ont98-corrections-mini.events.jsonl"


@pytest.fixture
def captured_events() -> list[dict]:
    """A real run's event log (from the suite's parity fixtures) as dicts."""
    if not SUITE_FIXTURE.exists():
        pytest.skip("suite fixture not found beside this checkout")
    events = [json.loads(l) for l in SUITE_FIXTURE.read_text().splitlines() if l.strip()]
    # the parity fixture is a subsample with the original version numbers;
    # an engine's own log is contiguous, so renumber
    for i, e in enumerate(events, 1):
        e["version"] = i
    return events
