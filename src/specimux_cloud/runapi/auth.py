"""Credentials the run API issues and checks (DESIGN.md "Identity and
authorization").

- **Service keys** name their host: ``<host id>.<random>``. The run API
  stores a hash per key, with a label, under the host record.
- **Run tokens** are minted by the run API for a host's authorize route
  and exchanged once by the browser; **sessions** are what the exchange
  returns, as a cookie. Both are the same compact signed document (a
  base64url JSON payload and an HMAC over it, keyed with the deployment's
  session secret) with a ``kind`` and an expiry, so nothing is stored per
  token and a run API replica with the same secret can verify them.
"""

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Optional

TOKEN = "token"
SESSION = "session"
SCOPES = ("view", "admin")

DEFAULT_TOKEN_TTL_S = 60
MAX_TOKEN_TTL_S = 300
SESSION_TTL_S = 12 * 3600
# A public viewer has no host to come back to for a new token, so its
# session outlasts a live run on a projector; revocation is checked on
# every request instead (RunService.check_session)
PUBLIC_SCOPE = "public"
PUBLIC_SESSION_TTL_S = 7 * 24 * 3600
SHARE_TOKEN_PREFIX = "s1."


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def sign(payload: dict, secret: str) -> str:
    body = _b64(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    mac = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
    return f"{body}.{_b64(mac)}"


def verify(document: str, secret: str, kind: str, now: Optional[float] = None) -> Optional[dict]:
    """The payload if the signature holds, the kind matches and it has not
    expired; None otherwise (never raises on malformed input)."""
    try:
        body, mac = document.split(".", 1)
        expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(_unb64(mac), expected):
            return None
        payload = json.loads(_unb64(body))
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("kind") != kind:
        return None
    if float(payload.get("exp", 0)) <= (now if now is not None else time.time()):
        return None
    return payload


def mint(kind: str, secret: str, *, host: str, user: str, run: str, scope: str, ttl_s: float,
         label: Optional[str] = None, extra: Optional[dict] = None) -> tuple[str, dict]:
    payload = {"kind": kind, "host": host, "user": user, "run": run, "scope": scope,
               "exp": time.time() + ttl_s, "iat": time.time(), **(extra or {})}
    if label:
        payload["label"] = label
    return sign(payload, secret), payload


# --- service keys ---

def new_service_key(host_id: str) -> str:
    return f"{host_id}.{secrets.token_urlsafe(32)}"


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def split_key(presented: str) -> tuple[str, str]:
    """``(host id, secret)`` from a presented key; empty strings if malformed."""
    host_id, _, secret = (presented or "").partition(".")
    return (host_id, secret) if host_id and secret else ("", "")


def new_secret() -> str:
    return secrets.token_urlsafe(32)
