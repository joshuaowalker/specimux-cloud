"""A stand-in for the Google Drive API v3, as much of it as drive.py and
fetch.py use: file metadata, folder listings (paged), and downloads
(``alt=media``), behind an API key. Folders and files are added with
``folder()`` and ``file()``; ``private`` ids answer 404 as an unshared
item does, ``quota`` ids refuse downloads as Drive does for a file that
hit its download limit, and ``corrupt`` ids serve the wrong bytes."""

import hashlib
import re

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

FOLDER = "application/vnd.google-apps.folder"


class FakeDrive:
    def __init__(self, key: str = "test-drive-key", page_size: int = 2):
        self.key = key
        self.page_size = page_size
        self.items: dict[str, dict] = {}
        self.private: set[str] = set()
        self.quota: set[str] = set()
        self.corrupt: set[str] = set()
        self.downloads: list[str] = []
        self.app = self._build()

    def folder(self, fid: str, name: str, parent: str = "") -> str:
        self.items[fid] = {"id": fid, "name": name, "mimeType": FOLDER, "parent": parent}
        return fid

    def file(self, fid: str, name: str, content: bytes, parent: str) -> str:
        self.items[fid] = {"id": fid, "name": name, "mimeType": "application/octet-stream", "parent": parent,
                           "content": content}
        return fid

    @staticmethod
    def _error(status: int, reason: str, message: str = "") -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": {
            "code": status, "message": message or reason, "errors": [{"reason": reason, "message": message}]}})

    def _meta(self, item: dict) -> dict:
        out = {"id": item["id"], "name": item["name"], "mimeType": item["mimeType"]}
        if "content" in item:
            out["size"] = str(len(item["content"]))
            out["md5Checksum"] = hashlib.md5(item["content"]).hexdigest()
        return out

    def _build(self) -> FastAPI:
        app = FastAPI()

        @app.get("/drive/v3/files")
        def list_files(request: Request):
            q = request.query_params
            if request.headers.get("x-goog-api-key") != self.key:
                return self._error(400, "badRequest", "API key not valid. Please pass a valid API key.")
            parent = re.match(r"'([^']+)' in parents", q.get("q", "")).group(1)
            if parent in self.private or parent not in self.items:
                return self._error(404, "notFound", f"File not found: {parent}.")
            children = sorted((i for i in self.items.values() if i["parent"] == parent and i["id"] not in self.private),
                              key=lambda i: i["id"])
            start = int(q.get("pageToken") or 0)
            page = children[start:start + self.page_size]
            body = {"files": [self._meta(i) for i in page]}
            if start + self.page_size < len(children):
                body["nextPageToken"] = str(start + self.page_size)
            return body

        @app.get("/drive/v3/files/{fid}")
        def get_file(fid: str, request: Request):
            q = request.query_params
            if request.headers.get("x-goog-api-key") != self.key:
                return self._error(400, "badRequest", "API key not valid. Please pass a valid API key.")
            item = self.items.get(fid)
            if item is None or fid in self.private:
                return self._error(404, "notFound", f"File not found: {fid}.")
            if q.get("alt") != "media":
                return self._meta(item)
            if fid in self.quota:
                return self._error(403, "downloadQuotaExceeded", "The download quota for this file has been exceeded.")
            self.downloads.append(fid)
            content = item["content"]
            if fid in self.corrupt:
                content = content[:-1] + b"!"
            return Response(content, media_type="application/octet-stream")

        return app


class HostRouter:
    """One ASGI app for a TestClient that talks to two hosts: requests for
    ``drive_host`` go to the fake Drive, the rest to the run API."""

    def __init__(self, run_api, drive_app, drive_host: str = "drive.test"):
        self.run_api, self.drive_app, self.drive_host = run_api, drive_app, drive_host

    async def __call__(self, scope, receive, send):
        host = dict(scope.get("headers") or []).get(b"host", b"").decode().split(":")[0]
        app = self.drive_app if scope["type"] == "http" and host == self.drive_host else self.run_api
        await app(scope, receive, send)
