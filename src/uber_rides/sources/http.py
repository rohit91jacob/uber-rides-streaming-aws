"""HTTP helpers: retrying session, conditional downloads with an on-disk manifest."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from uber_rides.config import Settings
from uber_rides.logs import get_logger

log = get_logger(__name__)
CHUNK = 1 << 20


def session(settings: Settings) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=settings.http_retries,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = settings.user_agent
    return s


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class LandedFile:
    name: str
    path: str
    source: str
    sha256: str
    bytes: int
    etag: str | None
    last_modified: str | None
    landed_at: str
    changed: bool

    def to_dict(self) -> dict:
        return asdict(self)


class Manifest:
    """``_manifest.json`` next to the landed files: one entry per file name (latest version)."""

    def __init__(self, directory: Path) -> None:
        self.path = directory / "_manifest.json"
        self.entries: dict[str, dict] = json.loads(self.path.read_text("utf-8")) if self.path.exists() else {}

    def get(self, name: str) -> dict | None:
        return self.entries.get(name)

    def put(self, landed: LandedFile) -> None:
        self.entries[landed.name] = landed.to_dict()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.entries, indent=2, sort_keys=True), "utf-8")
        tmp.replace(self.path)


def land_file(
    settings: Settings,
    url: str,
    target: Path,
    *,
    local_source: Path | None = None,
    force: bool = False,
) -> LandedFile:
    """Land ``url`` at ``target``, idempotently.

    * ``local_source`` (if it exists) is copied instead of downloading. Used for fixtures and
      for a pre-downloaded cache.
    * Otherwise a conditional GET (If-None-Match / If-Modified-Since) is made; HTTP 304 keeps
      the existing file. Downloads stream to ``<target>.part`` and are renamed atomically.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(target.parent)
    previous = manifest.get(target.name)
    now = datetime.now(UTC).isoformat(timespec="seconds")

    if local_source is not None and local_source.exists():
        src_sha = sha256_file(local_source)
        if previous and previous["sha256"] == src_sha and target.exists() and not force:
            return LandedFile(**{**previous, "changed": False})
        tmp = target.with_suffix(target.suffix + ".part")
        shutil.copyfile(local_source, tmp)
        tmp.replace(target)
        landed = LandedFile(
            name=target.name,
            path=str(target),
            source=str(local_source),
            sha256=src_sha,
            bytes=target.stat().st_size,
            etag=None,
            last_modified=None,
            landed_at=now,
            changed=previous is None or previous["sha256"] != src_sha,
        )
        manifest.put(landed)
        return landed

    headers: dict[str, str] = {}
    if previous and target.exists() and not force:
        if previous.get("etag"):
            headers["If-None-Match"] = previous["etag"]
        if previous.get("last_modified"):
            headers["If-Modified-Since"] = previous["last_modified"]
    with session(settings).get(url, headers=headers, stream=True, timeout=settings.http_timeout) as r:
        if r.status_code == 304 and previous:
            log.info("not modified", extra={"url": url})
            return LandedFile(**{**previous, "changed": False})
        r.raise_for_status()
        tmp = target.with_suffix(target.suffix + ".part")
        digest = hashlib.sha256()
        size = 0
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        # Content-Length counts the encoded bytes; only compare when nothing was decoded.
        expected = r.headers.get("Content-Length")
        encoded = r.headers.get("Content-Encoding", "identity").lower() not in {"", "identity"}
        if expected is not None and not encoded and int(expected) != size:
            tmp.unlink(missing_ok=True)
            raise OSError(f"truncated download of {url}: {size} of {expected} bytes")
        tmp.replace(target)
        sha = digest.hexdigest()
        landed = LandedFile(
            name=target.name,
            path=str(target),
            source=url,
            sha256=sha,
            bytes=size,
            etag=r.headers.get("ETag"),
            last_modified=r.headers.get("Last-Modified"),
            landed_at=now,
            changed=previous is None or previous["sha256"] != sha,
        )
    manifest.put(landed)
    log.info("landed", extra={"url": url, "bytes": size, "sha256": sha})
    return landed
