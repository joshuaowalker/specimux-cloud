"""Uploader versions, for the run API's compatibility check (DESIGN.md
"Contracts and versioning").

Uploaders run on lab machines and are upgraded whenever someone gets
around to it, so the run API must keep working with old ones: changes to
the routes they call are additive, and a breaking change gets new routes
beside the old ones. When an old uploader truly cannot be served any
more, the operator sets a minimum (``SPECIMUX_MIN_UPLOADER``) and the run
API answers that uploader with 426 and the command to upgrade, before it
sends anything. The uploader names itself in its User-Agent
(``specimux-cloud-uploader/<version>``) from 0.1.1; one that names
nothing is 0.1.0, the only release before.
"""

import re
from typing import Optional

UPLOADER_AGENT = "specimux-cloud-uploader"
UNNAMED_UPLOADER = "0.1.0"
UPGRADE_COMMAND = "pip install -U specimux-cloud"


def parse_version(text: str) -> tuple:
    """(0, 1, 1) from "0.1.1"; a pre-release or local suffix is ignored."""
    parts = []
    for piece in str(text).split("."):
        m = re.match(r"\d+", piece)
        if not m:
            break
        parts.append(int(m.group()))
    return tuple(parts)


def uploader_version(user_agent: Optional[str]) -> str:
    """The uploader version a User-Agent names, or UNNAMED_UPLOADER."""
    m = re.search(re.escape(UPLOADER_AGENT) + r"/([0-9][0-9A-Za-z.+-]*)", user_agent or "")
    return m.group(1) if m else UNNAMED_UPLOADER


def older(version: str, than: str) -> bool:
    return parse_version(version) < parse_version(than)


def uploader_user_agent() -> str:
    from . import __version__
    return f"{UPLOADER_AGENT}/{__version__}"
