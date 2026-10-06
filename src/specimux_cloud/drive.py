"""A public Google Drive folder as a run's input (docs/DESIGN.md "Input
from Google Drive").

The run API lists the folder when the run is created (``list_files``):
every POD5 or FASTQ file in it and its subfolders, as the uploader would
pick them from a run folder (not MinKNOW's ``*_fail`` folders). The fetch
job then copies each file from Drive to the run's archive (``fetch.py``).
Both go through the Drive API with an API key, which reads only what is
shared with anyone who has the link; nobody signs in. A folder link is
only ever reduced to its id, never fetched itself.
"""

import re
import time
from typing import Optional
from urllib.parse import parse_qs, urlsplit

import httpx

DRIVE_API = "https://www.googleapis.com/drive/v3"
# The API key goes in this header, never in a URL, so request logs and
# error messages can't carry it
KEY_HEADER = "X-Goog-Api-Key"
FOLDER_MIME = "application/vnd.google-apps.folder"
FAILED_DIRS = ("fastq_fail", "pod5_fail")
SUFFIXES = {"pod5": (".pod5",), "fastq": (".fastq", ".fq", ".fastq.gz", ".fq.gz")}
# A presigned PUT is one request, which S3 caps at 5 GB
MAX_FILE_BYTES = 5 * 1000 ** 3
# Bounds on a listing: a folder tree past these is not a run folder
MAX_FOLDERS = 2000
MAX_FILES = 20000
DRIVE_ID = re.compile(r"[A-Za-z0-9_-]{10,}")


class DriveError(Exception):
    """The folder can't be used: the message says why, for the user. A
    ``status`` of 500 is the service's problem (no or a bad API key)."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


def folder_id(link: str) -> str:
    """The folder id in a Drive folder link (``…/drive/folders/<id>``,
    ``…/drive/u/0/folders/<id>?usp=sharing``, ``…?id=<id>``) or a bare id."""
    link = (link or "").strip()
    parts = urlsplit(link)
    if parts.scheme in ("http", "https"):
        if not (parts.hostname or "").endswith("google.com"):
            raise DriveError("Not a Google Drive folder link")
        m = re.search(r"/folders/([A-Za-z0-9_-]+)", parts.path)
        found = m.group(1) if m else (parse_qs(parts.query).get("id") or [""])[0]
        if DRIVE_ID.fullmatch(found):
            return found
        raise DriveError("Not a Google Drive folder link: expected https://drive.google.com/drive/folders/<id>")
    if DRIVE_ID.fullmatch(link):
        return link
    raise DriveError("Not a Google Drive folder link: expected https://drive.google.com/drive/folders/<id>")


def download_url(file_id: str, base: str = DRIVE_API) -> str:
    """A file's download URL; the request needs the key in KEY_HEADER."""
    return f"{base}/files/{file_id}?alt=media&supportsAllDrives=true"


def error_reason(resp: httpx.Response) -> str:
    """Drive's machine-readable reason for an error response
    (``notFound``, ``downloadQuotaExceeded``, ``keyInvalid``, ...)."""
    try:
        err = resp.json().get("error") or {}
    except ValueError:
        return ""
    for e in err.get("errors") or []:
        if e.get("reason"):
            return e["reason"]
    for d in err.get("details") or []:
        if d.get("reason"):
            return d["reason"]
    return err.get("status") or ""


class DriveLister:
    def __init__(self, api_key: str, base: str = DRIVE_API, client: Optional[httpx.Client] = None,
                 retries: int = 4):
        if not api_key:
            raise DriveError("Google Drive input is not set up on this service", status=500)
        self.key = api_key
        self.base = base.rstrip("/")
        self.client = client or httpx.Client(timeout=30.0)
        self.retries = retries

    def _get(self, path: str, params: dict) -> dict:
        params = {**params, "supportsAllDrives": "true"}
        for attempt in range(self.retries + 1):
            try:
                r = self.client.get(f"{self.base}{path}", params=params, headers={KEY_HEADER: self.key})
            except httpx.HTTPError as e:
                if attempt == self.retries:
                    raise DriveError(f"Google Drive did not answer ({e}); try again", status=502)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                return r.json()
            reason = error_reason(r)
            if r.status_code in (429, 500, 502, 503, 504) or reason in ("rateLimitExceeded", "userRateLimitExceeded"):
                if attempt < self.retries:
                    time.sleep(2 ** attempt)
                    continue
                raise DriveError("Google Drive is busy; try again in a minute", status=503)
            # a bad key or a malformed request (400), the Drive API not
            # enabled for the key's project: ours to fix, not the user's
            if r.status_code == 400 or reason in ("accessNotConfigured", "SERVICE_DISABLED"):
                raise DriveError(f"The service's Google Drive access is misconfigured ({reason})", status=500)
            if r.status_code in (403, 404):
                raise DriveError("That Google Drive folder was not found, or is not shared with "
                                 "\"Anyone with the link\"")
            raise DriveError(f"Google Drive refused the request ({r.status_code} {reason})", status=502)
        raise AssertionError("unreachable")

    def list_files(self, root: str, kind: str, include_failed: bool = False) -> list[dict]:
        """Every ``kind`` file under the folder ``root``, breadth first:
        ``{id, name, path, size, md5}``, sorted by path. Refuses a folder
        that is not one, has none, holds two files of one name (the
        archive is flat), or a file too large to copy."""
        meta = self._get(f"/files/{root}", {"fields": "id,name,mimeType"})
        if meta.get("mimeType") != FOLDER_MIME:
            raise DriveError("That Google Drive link is a file, not a folder")
        suffixes = SUFFIXES[kind]
        folders, found, seen = [(root, "")], [], 0
        while folders:
            fid, prefix = folders.pop(0)
            seen += 1
            if seen > MAX_FOLDERS:
                raise DriveError(f"That folder has more than {MAX_FOLDERS} subfolders; link the run folder itself")
            token = None
            while True:
                params = {"q": f"'{fid}' in parents and trashed = false", "pageSize": "1000",
                          "includeItemsFromAllDrives": "true",
                          "fields": "nextPageToken, files(id, name, mimeType, size, md5Checksum)"}
                if token:
                    params["pageToken"] = token
                page = self._get("/files", params)
                for f in page.get("files") or []:
                    path = f"{prefix}{f['name']}"
                    if f.get("mimeType") == FOLDER_MIME:
                        if include_failed or f["name"] not in FAILED_DIRS:
                            folders.append((f["id"], path + "/"))
                    elif f["name"].endswith(suffixes) and not f["name"].startswith("."):
                        found.append({"id": f["id"], "name": f["name"], "path": path,
                                      "size": int(f.get("size") or 0), "md5": f.get("md5Checksum")})
                        if len(found) > MAX_FILES:
                            raise DriveError(f"That folder has more than {MAX_FILES} {kind.upper()} files")
                token = page.get("nextPageToken")
                if not token:
                    break
        if not found:
            raise DriveError(f"No {kind.upper()} files in that Google Drive folder or its subfolders"
                             + ("" if include_failed else " (MinKNOW's *_fail folders are left out)"))
        names: dict[str, str] = {}
        for f in found:
            if f["name"] in names:
                raise DriveError(f"Two files are named {f['name']} ({names[f['name']]} and {f['path']}); "
                                 "file names must be unique")
            names[f["name"]] = f["path"]
        big = [f for f in found if f["size"] > MAX_FILE_BYTES]
        if big:
            raise DriveError(f"{big[0]['path']} is over 5 GB, which can't be copied yet")
        return sorted(found, key=lambda f: f["path"])
