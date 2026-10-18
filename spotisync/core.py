"""Durable, serialized synchronization with conservative filesystem reconciliation."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, nullcontext
import ctypes
from datetime import datetime, timedelta
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import shutil
import sqlite3
import stat
import struct
import sys
import tempfile
import time
from urllib.parse import quote, urlsplit
import uuid
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from mutagen import MutagenError
from mutagen.ogg import OggPage
from mutagen.oggvorbis import OggVorbis

from .worker import normalize_uri, WorkerFailure


SOURCE_QUALITIES = {"auto", "normal", "high", "very_high"}
SOURCE_BITRATES = {"normal": 96000, "high": 160000, "very_high": 320000}


def inspect_native_bitrate(path):
    """Read the actual nominal header, not Mutagen's fallback/estimated bitrate."""
    try:
        audio = OggVorbis(path)
        with Path(path).open("rb") as stream:
            page = OggPage(stream)
        packet = page.packets[0]
        if not page.first or page.serial != audio.info.serial or not packet.startswith(b"\x01vorbis"):
            raise ValueError("Missing native Vorbis identification header")
        nominal = struct.unpack("<i", packet[20:24])[0]
        if nominal not in SOURCE_BITRATES.values() or audio.info.bitrate != nominal:
            raise ValueError("Unknown or inconsistent nominal bitrate")
        return nominal
    except (MutagenError, ValueError, IndexError, struct.error) as exc:
        raise OSError("Native original has an unknown or corrupt nominal bitrate; file kept.") from exc


def native_path(uri, quality):
    if quality not in SOURCE_QUALITIES or not re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", uri):
        raise OSError("Native original requires a resolved Spotify track URI and valid source quality.")
    suffix = "" if quality == "auto" else "-" + quality
    return f".spotisync-originals/{uri.split(':')[-1]}{suffix}.ogg"


def validate_thumbnail_url(value: object) -> str | None:
    """Accept only HTTPS artwork on Spotify's exact public image hosts."""
    if (not isinstance(value, str) or not value or len(value) > 2048
            or re.search(r"[\x00-\x20\x7f-\x9f\\]", value) or "#" in value):
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme != "https" or parsed.netloc not in {
        "i.scdn.co", "mosaic.scdn.co",
        "image-cdn-ak.spotifycdn.com", "image-cdn-fa.spotifycdn.com",
    }:
        return None
    return value


def safe_name(value: str, limit: int = 110) -> str:
    value = re.sub(r'[\x00-\x1f<>:"/\\|?*]', "_", value).strip(" .")
    return (value.encode("utf-8")[:limit].decode("utf-8", errors="ignore").rstrip(" .") or "Untitled")


async def protected(awaitable):
    """Finish cleanup even if its caller is canceled repeatedly, then propagate cancellation."""
    task = asyncio.ensure_future(awaitable)
    canceled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            canceled = True
    result = task.result()
    if canceled:
        raise asyncio.CancelledError
    return result


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def rename_noreplace(source, destination, source_fd, destination_fd):
    rename = ctypes.CDLL(None, use_errno=True).renameat2
    if rename(source_fd, ctypes.c_char_p(os.fsencode(source)), destination_fd,
              ctypes.c_char_p(os.fsencode(destination)), 1):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), source)


def in_window(now: datetime) -> bool:
    return 3 <= now.hour < 8


def local_now() -> datetime:
    if os.environ.get("TZ"):
        try:
            return datetime.now(ZoneInfo(os.environ["TZ"]))
        except (ValueError, ZoneInfoNotFoundError):
            pass
    try:
        with Path("/etc/localtime").open("rb") as source:
            return datetime.now(ZoneInfo.from_file(source, key="Computer local time"))
    except (OSError, ValueError):
        return datetime.now().astimezone()


class WorkerRunner:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.process = None
        self._running = False
        self._stopping = None

    async def stop(self):
        process = self.process
        if process is None:
            return

        async def terminate():
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                pass
            finally:
                # The leader may exit before descendants that ignore SIGTERM.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await process.wait()
                if self.process is process:
                    self.process = None

        if self._stopping is None or self._stopping.done():
            self._stopping = asyncio.create_task(terminate())
        await protected(self._stopping)

    async def run(self, command: str, *, on_event=None, timeout: float = 300, **values) -> dict:
        if self._running or self.process is not None:
            raise RuntimeError("Another Spotify worker is already running.")
        self._running = True

        async def launch():
            self.process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "spotisync.worker", stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True, limit=8 * 1024 * 1024,
            )

        async def receive():
            process = self.process
            process.stdin.write(json.dumps({"command": command, "state_dir": str(self.state_dir), **values}).encode())
            await process.stdin.drain()
            process.stdin.close()
            result = None
            async for line in process.stdout:
                event = json.loads(line)
                if event.get("event") == "result":
                    result = event
                elif on_event:
                    on_event(event)
            await process.wait()
            return result or {"status": "error", "message": "Spotify worker exited without a result."}

        try:
            await protected(launch())
            return await asyncio.wait_for(receive(), timeout)
        except TimeoutError:
            return {"status": "error", "message": "Spotify worker timed out; retry later."}
        except (ValueError, OSError):
            return {"status": "error", "message": "Spotify worker returned an invalid response."}
        finally:
            try:
                await self.stop()
            finally:
                self._running = False


class SyncService:
    def __init__(self, state_dir: Path, music_dir: Path, *, runner=None, now=None, clock=None, sleeper=None):
        self.state_dir = state_dir.resolve()
        self.music_dir = music_dir.resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.state_dir.chmod(0o700)
        self.music_dir.mkdir(parents=True, exist_ok=True)
        self.music_fd = os.open(self.music_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.db = sqlite3.connect(self.state_dir / "spotisync.sqlite3", isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY, uri TEXT UNIQUE NOT NULL, kind TEXT NOT NULL,
                name TEXT NOT NULL, artist TEXT NOT NULL DEFAULT '', folder TEXT,
                status TEXT NOT NULL DEFAULT 'pending', last_sync TEXT,
                complete INTEGER NOT NULL DEFAULT 0, revision TEXT,
                message TEXT, next_metadata REAL NOT NULL DEFAULT 0, refreshed_job TEXT,
                thumbnail_url TEXT, thumbnail_checked REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS tracks (
                item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                key TEXT NOT NULL, uri TEXT NOT NULL, name TEXT NOT NULL, artist TEXT NOT NULL,
                album TEXT NOT NULL DEFAULT '', duration_ms INTEGER NOT NULL DEFAULT 0,
                availability TEXT NOT NULL DEFAULT 'unknown', position INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', path TEXT, hash TEXT, format TEXT,
                message TEXT, next_attempt REAL NOT NULL DEFAULT 0, attempt_job TEXT,
                thumbnail_url TEXT, thumbnail_checked REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(item_id, key)
            );
            CREATE TABLE IF NOT EXISTS publications (
                id TEXT PRIMARY KEY, item_id INTEGER NOT NULL, track_key TEXT NOT NULL,
                path TEXT NOT NULL, hash TEXT NOT NULL, format TEXT NOT NULL,
                job_id TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sources (
                uri TEXT PRIMARY KEY, path TEXT UNIQUE NOT NULL, hash TEXT NOT NULL,
                format TEXT NOT NULL DEFAULT 'ogg'
            );
            CREATE TABLE IF NOT EXISTS source_versions (
                uri TEXT NOT NULL, quality TEXT NOT NULL, path TEXT UNIQUE NOT NULL,
                hash TEXT NOT NULL, format TEXT NOT NULL DEFAULT 'ogg',
                PRIMARY KEY(uri,quality)
            );
        """)
        migrate_membership_job = "membership_job" not in {
            row[1] for row in self.db.execute("PRAGMA table_info(tracks)")}
        # Concrete migrations also cover databases created before native originals existed.
        for table, fields in {
            "items": {"thumbnail_url": "TEXT", "thumbnail_checked": "REAL NOT NULL DEFAULT 0"},
            "publications": {"kind": "TEXT NOT NULL DEFAULT 'output'", "source_uri": "TEXT",
                             "old_path": "TEXT", "old_hash": "TEXT", "quality": "TEXT NOT NULL DEFAULT 'auto'",
                             "bitrate": "INTEGER", "source_hash": "TEXT", "source_bitrate": "INTEGER"},
            "sources": {"quality": "TEXT NOT NULL DEFAULT 'auto'", "bitrate": "INTEGER"},
            "source_versions": {"bitrate": "INTEGER"},
            "tracks": {"source_message": "TEXT", "source_next_attempt": "REAL NOT NULL DEFAULT 0",
                        "source_attempt_job": "TEXT", "thumbnail_url": "TEXT",
                          "thumbnail_checked": "REAL NOT NULL DEFAULT 0", "source_quality": "TEXT NOT NULL DEFAULT 'auto'",
                          "source_hash": "TEXT", "source_bitrate": "INTEGER", "membership_job": "TEXT"},
        }.items():
            columns = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})")}
            for name, declaration in fields.items():
                if name not in columns:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
        self.db.execute("""INSERT INTO source_versions(uri,quality,path,hash,format)
            SELECT uri,quality,path,hash,format FROM sources WHERE 1 ON CONFLICT(uri,quality) DO NOTHING""")
        if migrate_membership_job:
            # A complete legacy snapshot proves successful membership; a failed refresh does not.
            self.db.execute("""UPDATE tracks SET membership_job=(SELECT refreshed_job FROM items
                WHERE id=tracks.item_id AND complete=1)""")
        self.runner = runner or WorkerRunner(self.state_dir)
        self.now = now or local_now
        self.clock = clock or time.time
        self.sleep = sleeper or asyncio.sleep
        self.task = None
        self.scheduler = None
        self.probe = None
        self._disconnecting = None
        self._auth_epoch = 0
        self._closing = False
        self._closed = False
        self._close_task = None
        self.ready = False
        self.account = {"connected": False, "username": None, "connecting": False,
                        "auth_url": None, "message": None}
        self.sync = {"running": False, "phase": None, "current_item": None,
                      "current_track": None, "message": None, "completed": 0, "total": 0,
                      "progress_ready": False, "progress_from_resume": False, "total_provisional": False}
        self.conversion = {"running": False, "total": 0, "completed": 0, "failed": 0,
                            "skipped": 0, "skips": [], "format": None, "quality": None,
                            "current_track": None, "message": None, "errors": []}
        saved_conversion = self.setting("conversion_job")
        if saved_conversion:
            self.conversion.update({key: saved_conversion[key] for key in
                                    ("total", "completed", "failed", "format", "errors")})
            self.conversion["quality"] = saved_conversion.get("quality", "auto")
            self.conversion.update(skipped=saved_conversion.get("skipped", 0), skips=saved_conversion.get("skips", []))
            self.conversion["message"] = "Offline conversion paused; queued work will resume."
        self.recover_publications()
        self.restore_sync_progress()

    def setting(self, key, default=None):
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_setting(self, key, value):
        self.db.execute("INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                         (key, json.dumps(value)))

    def restore_sync_progress(self):
        progress = self.setting("sync_progress")
        self.sync.update(completed=len(progress["completed"]) if progress else 0,
                         total=len(progress["targets"]) if progress else 0,
                         progress_ready=bool(progress and progress["ready"] and isinstance(progress.get("baseline"), list)
                                             and not progress.get("legacy")),
                         progress_from_resume=bool(progress and progress.get("from_resume")),
                         total_provisional=bool(progress and progress.get("provisional")))

    def begin_sync_progress(self, job, *, legacy=False):
        progress = self.setting("sync_progress")
        same_job = bool(progress and progress["job_id"] == job["id"])
        if same_job and not progress.get("legacy") and isinstance(progress.get("baseline"), list):
            self.restore_sync_progress()
            return
        baseline = {row[0] for row in self.db.execute("SELECT DISTINCT uri FROM tracks WHERE status='downloaded'")
                    if re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", row[0])}
        for row in list(self.db.execute("SELECT * FROM source_versions")):
            try:
                self.inspect_version(dict(row))
            except OSError:
                continue
            baseline.add(row["uri"])
        self.set_setting("sync_progress", {"job_id": job["id"], "baseline": sorted(baseline),
            "targets": [], "completed": [], "ready": False, "legacy": False,
            "from_resume": legacy or same_job, "provisional": False})
        self.restore_sync_progress()

    def update_sync_targets(self, job_id, *, ready=None):
        progress, job = self.setting("sync_progress"), self.setting("job")
        if not progress or not job or progress["job_id"] != job_id or job["id"] != job_id:
            return
        targets, baseline = set(progress["targets"]), set(progress["baseline"])
        provisional = False
        for item_id in job["item_ids"]:
            item = self.db.execute("SELECT complete FROM items WHERE id=?", (item_id,)).fetchone()
            if not item:
                continue
            provisional |= not bool(item["complete"])
            for row in self.db.execute("""SELECT uri FROM tracks WHERE item_id=? AND membership_job=? AND status NOT IN
                ('downloaded','unavailable') AND availability IN ('available','unknown')""", (item_id, job_id)):
                if re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", row["uri"]) and row["uri"] not in baseline:
                    targets.add(row["uri"])
        progress.update(targets=sorted(targets), provisional=provisional)
        if ready is not None:
            progress["ready"] = ready
        self.set_setting("sync_progress", progress)
        self.restore_sync_progress()

    def item(self, item_id):
        row = self.db.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise ValueError("Tracked item not found.")
        return dict(row)

    async def thumbnail(self, item_id):
        item = self.item(item_id)
        if not re.fullmatch(r"spotify:(track|album|playlist):([A-Za-z0-9]{22})", item["uri"]):
            raise ValueError("Tracked item has an invalid Spotify URI.")
        return await self.media_thumbnail(item["uri"])

    async def media_thumbnail(self, uri):
        try:
            normalized = normalize_uri(uri)
        except WorkerFailure as exc:
            if (isinstance(uri, str) and uri.startswith("spotify:local:")
                    and self.db.execute("SELECT 1 FROM tracks WHERE uri=?", (uri,)).fetchone()):
                return None
            raise ValueError(exc.message) from None
        uri = normalized
        kind, ident = uri.split(":")[1:]
        item = self.db.execute("SELECT * FROM items WHERE uri=?", (uri,)).fetchone()
        tracks = list(self.db.execute("SELECT * FROM tracks WHERE uri=?", (uri,))) if kind == "track" else []
        candidates = ([item] if item else []) + tracks
        if not candidates:
            raise ValueError("Tracked media not found.")
        cached = max(candidates, key=lambda row: (
            validate_thumbnail_url(row["thumbnail_url"]) is not None, row["thumbnail_checked"]))
        thumbnail = validate_thumbnail_url(cached["thumbnail_url"])
        checked = cached["thumbnail_checked"]
        ttl = 24 * 60 * 60 if thumbnail else 60 * 60
        url = "https://open.spotify.com/oembed?url=" + quote(
            f"https://open.spotify.com/{kind}/{ident}", safe="")

        def fetch():
            try:
                with requests.get(url, timeout=(3, 8), allow_redirects=False) as response:
                    if response.status_code != 200:
                        return None
                    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                    if content_type != "application/json":
                        return None
                    data = response.json()
                    return validate_thumbnail_url(data.get("thumbnail_url")) if isinstance(data, dict) else None
            except (requests.RequestException, ValueError):
                return None

        if not checked or self.clock() >= checked + ttl:
            fetched = await asyncio.to_thread(fetch)
            if self._closing or self._closed:
                return None
            # Fetching can overlap removal, ID reuse, or authenticated metadata refresh.
            item = self.db.execute("SELECT * FROM items WHERE id=? AND uri=?",
                                   (item["id"], uri)).fetchone() if item else None
            tracks = list(self.db.execute("SELECT * FROM tracks WHERE uri=?", (uri,))) if kind == "track" else []
            candidates = ([item] if item else []) + tracks
            if not candidates:
                return None
            thumbnail = fetched or next(filter(None, (
                validate_thumbnail_url(row["thumbnail_url"]) for row in candidates)), None)
            checked = self.clock()
        if item:
            self.db.execute("UPDATE items SET thumbnail_url=?,thumbnail_checked=? WHERE id=? AND uri=?",
                            (thumbnail, checked, item["id"], uri))
        if tracks:
            self.db.execute("UPDATE tracks SET thumbnail_url=?,thumbnail_checked=? WHERE uri=?",
                            (thumbnail, checked, uri))
        return thumbnail

    def add(self, value):
        try:
            uri = normalize_uri(value)
        except WorkerFailure as exc:
            raise ValueError(exc.message) from None
        kind, ident = uri.split(":")[1:]
        try:
            result = self.db.execute("INSERT INTO items(uri,kind,name) VALUES (?,?,?)",
                                     (uri, kind, f"{kind.title()} {ident}"))
        except sqlite3.IntegrityError:
            raise ValueError("This media item is already tracked.") from None
        return self.item(result.lastrowid)

    def remove(self, item_id):
        self.item(item_id)
        if self.busy:
            raise RuntimeError("Wait for current job to finish before removing tracking.")
        self.recover_publications()
        if self.db.execute("SELECT 1 FROM publications WHERE item_id=? AND kind='output'", (item_id,)).fetchone():
            raise RuntimeError("Pending publication needs managed path recovery before removing tracking.")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            job = self.setting("job")
            if job:
                job["item_ids"] = [ident for ident in job["item_ids"] if ident != item_id]
                self.set_setting("job", job if job["item_ids"] else None)
            self.db.execute("DELETE FROM items WHERE id=?", (item_id,))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    @property
    def busy(self):
        return (not self.ready or self._closing
                or (self._disconnecting is not None and not self._disconnecting.done())
                or (self.task is not None and not self.task.done()))

    @contextmanager
    def directory(self, relative="", *, create=False):
        part = Path(relative)
        if part.is_absolute() or ".." in part.parts:
            raise OSError("Invalid managed directory path.")
        fd = os.dup(self.music_fd)
        try:
            for bit in part.parts:
                if create:
                    try:
                        os.mkdir(bit, mode=0o755, dir_fd=fd)
                        os.fsync(fd)
                    except FileExistsError:
                        pass
                try:
                    child = os.open(bit, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                except OSError as exc:
                    if exc.errno in {errno.ENOTDIR, errno.ELOOP}:
                        raise OSError("Refusing symlink or non-directory in managed output.") from exc
                    raise
                os.close(fd)
                fd = child
            yield fd
        finally:
            os.close(fd)

    def path(self, relative: str) -> Path:
        part = Path(relative)
        if part.is_absolute() or ".." in part.parts or not part.parts:
            raise OSError("Invalid managed file path.")
        current = self.music_dir
        for bit in part.parts:
            current = current / bit
            if current.is_symlink():
                raise OSError("Refusing to follow symlink in managed output.")
        return current

    @contextmanager
    def verified_file(self, relative, checksum):
        part = Path(relative)
        if not checksum or part.is_absolute() or ".." in part.parts or not part.name:
            raise OSError("Managed audio has no verified ownership hash.")
        with self.directory(str(part.parent)) as parent:
            fd = os.open(part.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                before = os.fstat(fd)
                if (not stat.S_ISREG(before.st_mode) or not before.st_size
                        or digest(Path(f"/proc/self/fd/{fd}")) != checksum):
                    raise OSError("Managed audio was modified outside SpotiSync; file kept.")
                yield fd
                after = os.fstat(fd)
                entry = os.stat(part.name, dir_fd=parent, follow_symlinks=False)
                with self.directory(str(part.parent)) as current:
                    parent_info, current_info = os.fstat(parent), os.fstat(current)
                if ((before.st_dev, before.st_ino) != (entry.st_dev, entry.st_ino)
                        or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                           (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                        or (parent_info.st_dev, parent_info.st_ino) != (current_info.st_dev, current_info.st_ino)
                        or digest(Path(f"/proc/self/fd/{fd}")) != checksum):
                    raise OSError("Managed audio changed during verification; file kept.")
            finally:
                os.close(fd)

    def inspect_version(self, version):
        if version["path"] != native_path(version["uri"], version["quality"]) or version["format"] != "ogg":
            raise OSError("Native original version is unsafe; conversion blocked.")
        active = self.db.execute("SELECT * FROM sources WHERE uri=?", (version["uri"],)).fetchone()
        if active and active["quality"] == version["quality"] and any(
                active[field] != version[field] for field in ("path", "hash", "format")):
            raise OSError("Native original record mismatches immutable version; conversion blocked.")
        with self.verified_file(version["path"], version["hash"]) as fd:
            bitrate = version.get("bitrate")
            if bitrate is None:
                bitrate = inspect_native_bitrate(Path(f"/proc/self/fd/{fd}"))
            if bitrate not in SOURCE_BITRATES.values():
                raise OSError("Native original has an unknown nominal bitrate; file kept.")
        self.db.execute("UPDATE source_versions SET bitrate=? WHERE uri=? AND quality=? AND hash=?",
                        (bitrate, version["uri"], version["quality"], version["hash"]))
        self.db.execute("UPDATE sources SET bitrate=? WHERE uri=? AND path=? AND hash=?",
                        (bitrate, version["uri"], version["path"], version["hash"]))
        return dict(version, bitrate=bitrate)

    def resolve_source(self, track, quality, *, activate=True):
        """Resolve verified physical originals by actual bitrate, without alias files or rows."""
        if quality not in SOURCE_QUALITIES:
            raise OSError("Invalid source quality.")
        versions = [self.inspect_version(dict(row)) for row in list(self.db.execute(
            "SELECT * FROM source_versions WHERE uri=? ORDER BY quality", (track["uri"],)))]
        candidates = [version for version in versions if quality == "auto" or version["bitrate"] == SOURCE_BITRATES[quality]]
        if not candidates:
            return None
        source = max(candidates, key=lambda version: (version["bitrate"],
            version["hash"] == track.get("source_hash"), version["quality"] == track.get("source_quality"),
            version["quality"] == quality))
        if activate:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute("""INSERT INTO sources(uri,path,hash,format,quality,bitrate) VALUES (?,?,?,?,?,?)
                    ON CONFLICT(uri) DO UPDATE SET path=excluded.path,hash=excluded.hash,
                    format=excluded.format,quality=excluded.quality,bitrate=excluded.bitrate""",
                    (source["uri"], source["path"], source["hash"], "ogg", source["quality"], source["bitrate"]))
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
        return source

    def output_matches_source(self, track, source):
        try:
            with self.verified_file(track["path"], track["hash"]):
                pass
        except FileNotFoundError:
            return False
        if track.get("source_hash"):
            return track["source_hash"] == source["hash"] and track.get("source_bitrate") in (None, source["bitrate"])
        if track["format"] == "ogg" and track["hash"] == source["hash"]:
            return True
        return False

    def mark_output_source(self, track, source, quality, job_id):
        self.db.execute("""UPDATE tracks SET source_quality=?,source_hash=?,source_bitrate=?,
            source_message=NULL,source_next_attempt=0,source_attempt_job=? WHERE item_id=? AND key=?
            AND status='downloaded' AND path=? AND hash=?""", (quality, source["hash"], source["bitrate"],
            job_id, track["item_id"], track["key"], track["path"], track["hash"]))

    def quality_pending(self, track, quality):
        versions = list(self.db.execute("SELECT quality,hash,bitrate FROM source_versions WHERE uri=?", (track["uri"],)))
        source_hash = track["source_hash"] or (track["hash"] if track["format"] == "ogg" else None)
        if not source_hash:
            return True
        bitrate = track["source_bitrate"]
        if bitrate is None:
            legacy = next((version for version in versions if version["hash"] == source_hash), None)
            bitrate = legacy["bitrate"] if legacy else None
        if not any(version["hash"] == source_hash for version in versions):
            return True
        if bitrate not in SOURCE_BITRATES.values():
            return True
        if quality != "auto":
            return bitrate != SOURCE_BITRATES[quality]
        return any(version["bitrate"] is None or version["bitrate"] > bitrate for version in versions)

    def assign_folder(self, item, metadata):
        if item["folder"]:
            with self.directory(item["folder"], create=True):
                pass
            return item["folder"]
        category = {"playlist": "Playlists", "album": "Albums", "track": "Songs"}[item["kind"]]
        title = metadata.get("name") or item["name"]
        if item["kind"] != "playlist" and metadata.get("artist"):
            title = metadata["artist"] + " - " + title
        with self.directory(category, create=True) as parent:
            suffix = 0
            while True:
                tail = f" [{item['uri'].split(':')[-1]}]" + (f" - {suffix}" if suffix else "")
                name = safe_name(title, min(110, 255 - len(tail.encode("utf-8")))) + tail
                try:
                    os.mkdir(name, mode=0o755, dir_fd=parent)
                    os.fsync(parent)
                    return str(Path(category) / name)
                except FileExistsError:
                    suffix += 1

    def _remove_managed_file(self, track, *, directory_fd=None):
        if not track["path"]:
            return
        part = Path(track["path"])
        if not part.parts:
            raise OSError("Invalid managed file path.")
        if part.parts[0] == ".spotisync-originals" and track.get("kind") != "source":
            raise OSError("Native originals are permanent; refusing playback cleanup in original store.")
        with self.directory(str(part.parent)) if directory_fd is None else nullcontext(directory_fd) as parent:
            try:
                info = os.stat(part.name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return
            if stat.S_ISLNK(info.st_mode):
                raise OSError("Refusing symlink in managed output; file kept.")
            if not stat.S_ISREG(info.st_mode) or not track["hash"]:
                raise OSError("Removed playlist song was modified outside SpotiSync; file kept.")
            quarantine = ".spotisync-quarantine-" + uuid.uuid4().hex
            os.mkdir(quarantine, mode=0o700, dir_fd=parent)
            qfd = os.open(quarantine, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            try:
                manifest = os.open("intent.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=qfd)
                with os.fdopen(manifest, "w") as stream:
                    json.dump({"original": track["path"], "hash": track["hash"]}, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.fsync(qfd)
                os.fsync(parent)
                # Capture first. An external replacement at the original name is never unlinked.
                rename_noreplace(part.name, "captured", parent, qfd)
                os.fsync(qfd)
                os.fsync(parent)
                verified = False
                try:
                    fd = os.open("captured", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=qfd)
                    try:
                        before = os.fstat(fd)
                        verified = (stat.S_ISREG(before.st_mode)
                                    and (track.get("inode") is None or
                                         (before.st_dev, before.st_ino) == (track["device"], track["inode"]))
                                    and digest(Path(f"/proc/self/fd/{fd}")) == track["hash"])
                        if verified:
                            after = os.fstat(fd)
                            entry = os.stat("captured", dir_fd=qfd, follow_symlinks=False)
                            verified = ((before.st_dev, before.st_ino) == (entry.st_dev, entry.st_ino)
                                        and (before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                                            (after.st_size, after.st_mtime_ns, after.st_ctime_ns))
                    finally:
                        os.close(fd)
                except OSError:
                    pass
                if verified:
                    os.unlink("captured", dir_fd=qfd)
                else:
                    try:
                        os.link("captured", part.name, src_dir_fd=qfd, dst_dir_fd=parent, follow_symlinks=False)
                        os.fsync(parent)
                    except OSError:
                        raise OSError(f"Modified playlist file kept in {part.parent / quarantine}; see intent.json.") from None
                    os.unlink("captured", dir_fd=qfd)
                os.unlink("intent.json", dir_fd=qfd)
                os.rmdir(quarantine, dir_fd=parent)
                os.fsync(parent)
                if not verified:
                    raise OSError("Removed playlist song was modified outside SpotiSync; file kept.")
            finally:
                os.close(qfd)

    def finish_publication(self, operation):
        quality = operation.get("quality", "auto")
        if quality not in SOURCE_QUALITIES:
            raise OSError("Unsafe source quality in publication record.")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if operation.get("kind", "output") == "source":
                if (not re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", operation["source_uri"] or "")
                        or operation["path"] != native_path(operation["source_uri"], quality)
                        or operation["format"] != "ogg"):
                    raise OSError("Unsafe native original publication record.")
                self.db.execute("""INSERT INTO source_versions(uri,quality,path,hash,format,bitrate) VALUES (?,?,?,?,'ogg',?)
                    ON CONFLICT(uri,quality) DO NOTHING""",
                    (operation["source_uri"], quality, operation["path"], operation["hash"], operation.get("bitrate")))
                version = self.db.execute("SELECT * FROM source_versions WHERE uri=? AND quality=?",
                                          (operation["source_uri"], quality)).fetchone()
                if version["path"] != operation["path"] or version["hash"] != operation["hash"] or version["format"] != "ogg":
                    raise OSError("Native version conflicts with immutable original; file kept.")
                self.db.execute("""INSERT INTO sources(uri,path,hash,format,quality,bitrate) VALUES (?,?,?,'ogg',?,?)
                    ON CONFLICT(uri) DO UPDATE SET path=excluded.path,hash=excluded.hash,
                    format=excluded.format,quality=excluded.quality,bitrate=excluded.bitrate""",
                    (operation["source_uri"], operation["path"], operation["hash"], quality, version["bitrate"]))
            else:
                track = self.db.execute("SELECT uri FROM tracks WHERE item_id=? AND key=?",
                                        (operation["item_id"], operation["track_key"])).fetchone()
                legacy = (self.db.execute("SELECT hash,bitrate FROM source_versions WHERE uri=? AND quality=?",
                    (track["uri"], quality)).fetchone() if track and not operation.get("source_hash") else None)
                self.db.execute("""UPDATE tracks SET status='downloaded',path=?,hash=?,format=?,message=NULL,
                    source_quality=?,source_hash=?,source_bitrate=?,source_message=NULL,source_next_attempt=0,source_attempt_job=?,
                    next_attempt=0,attempt_job=? WHERE item_id=? AND key=?""",
                    (operation["path"], operation["hash"], operation["format"], quality,
                     operation.get("source_hash") or (legacy["hash"] if legacy else None),
                     operation.get("source_bitrate") or (legacy["bitrate"] if legacy else None), operation["job_id"], operation["job_id"],
                     operation["item_id"], operation["track_key"]))
                progress = self.setting("sync_progress")
                if (track and progress and progress["job_id"] == operation["job_id"]
                        and track["uri"] in progress["targets"] and track["uri"] not in progress["completed"]):
                    progress["completed"].append(track["uri"])
                    self.set_setting("sync_progress", progress)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.restore_sync_progress()
        # Keep the replacement intent until old-file cleanup completes, including across restart.
        if operation.get("old_path") and operation["old_path"] != operation["path"]:
            try:
                self._recover_replacement_capture(operation)
            except FileNotFoundError:
                pass
            try:
                self._remove_managed_file({"path": operation["old_path"], "hash": operation["old_hash"]})
            except OSError as exc:
                if "file kept" not in str(exc):
                    raise  # Retry filesystem failures using the durable old path and ownership hash.
                self.sync["message"] = str(exc)
        self.db.execute("DELETE FROM publications WHERE id=?", (operation["id"],))

    def _recover_replacement_capture(self, operation):
        """Finish only hash-verified captures belonging to this replacement intent."""
        part = Path(operation["old_path"])
        with self.directory(str(part.parent)) as parent:
            for name in os.listdir(parent):
                if not name.startswith(".spotisync-quarantine-"):
                    continue
                with self.directory(str(part.parent / name)) as qfd:
                    try:
                        fd = os.open("intent.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=qfd)
                        with os.fdopen(fd) as stream:
                            intent = json.load(stream)
                        if intent != {"original": operation["old_path"], "hash": operation["old_hash"]}:
                            continue
                        fd = os.open("captured", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=qfd)
                        try:
                            info = os.fstat(fd)
                            if not stat.S_ISREG(info.st_mode) or digest(Path(f"/proc/self/fd/{fd}")) != intent["hash"]:
                                continue
                            entry = os.stat("captured", dir_fd=qfd, follow_symlinks=False)
                            after = os.fstat(fd)
                            if ((info.st_dev, info.st_ino) != (entry.st_dev, entry.st_ino)
                                    or (info.st_size, info.st_mtime_ns, info.st_ctime_ns) !=
                                       (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                                continue
                            os.unlink("captured", dir_fd=qfd)
                        finally:
                            os.close(fd)
                        os.unlink("intent.json", dir_fd=qfd)
                        os.fsync(qfd)
                        os.rmdir(name, dir_fd=parent)
                        os.fsync(parent)
                    except (FileNotFoundError, ValueError):
                        continue

    def recover_publications(self):
        for row in list(self.db.execute("SELECT * FROM publications")):
            operation = dict(row)
            part = Path(operation["path"])
            try:
                with self.directory(str(part.parent)) as parent:
                    fd = os.open(part.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                    try:
                        info = os.fstat(fd)
                        owned = (stat.S_ISREG(info.st_mode) and info.st_size
                                 and (info.st_dev, info.st_ino) == (operation["device"], operation["inode"])
                                 and digest(Path(f"/proc/self/fd/{fd}")) == operation["hash"])
                        if owned:
                            entry = os.stat(part.name, dir_fd=parent, follow_symlinks=False)
                            after = os.fstat(fd)
                            owned = ((entry.st_dev, entry.st_ino) == (info.st_dev, info.st_ino)
                                     and (info.st_size, info.st_mtime_ns, info.st_ctime_ns) ==
                                         (after.st_size, after.st_mtime_ns, after.st_ctime_ns))
                            with self.directory(str(part.parent)) as current:
                                parent_info, current_info = os.fstat(parent), os.fstat(current)
                                if (parent_info.st_dev, parent_info.st_ino) != (current_info.st_dev, current_info.st_ino):
                                    raise OSError("Managed directory changed during publication recovery.")
                    finally:
                        os.close(fd)
            except FileNotFoundError:
                owned = False
            except OSError:
                self.sync["message"] = "Publication recovery paused; managed path is unsafe or inaccessible."
                continue
            if owned:
                try:
                    self.finish_publication(operation)
                except OSError:
                    self.sync["message"] = "Publication cleanup paused; managed path is unsafe or inaccessible."
            else:
                self.db.execute("DELETE FROM publications WHERE id=?", (operation["id"],))

    def reconcile(self, item_id, metadata, job_id):
        item = self.item(item_id)
        tracks = metadata.get("tracks")
        if not isinstance(tracks, list):
            raise ValueError("Metadata worker returned no membership list.")
        unique = {}
        for position, track in enumerate(tracks):
            if not isinstance(track, dict) or not track.get("id") or not track.get("uri"):
                raise ValueError("Metadata worker returned an invalid track.")
            if track["id"] not in unique:
                unique[track["id"]] = (position, track)
        folder = self.assign_folder(item, metadata)
        old = {row["key"]: dict(row) for row in self.db.execute("SELECT * FROM tracks WHERE item_id=?", (item_id,))}
        deletion_messages = []
        # Only complete raw membership may remove files. Download/availability failures never do.
        if metadata.get("complete") and item["kind"] == "playlist":
            for key in old.keys() - unique.keys():
                try:
                    self._remove_managed_file(old[key])
                except OSError as exc:
                    deletion_messages.append(str(exc))
                self.db.execute("DELETE FROM tracks WHERE item_id=? AND key=?", (item_id, key))
        self.db.execute("BEGIN IMMEDIATE")
        try:
            for key, (position, track) in unique.items():
                previous = old.get(key)
                status, message = "pending", None
                if previous and previous["status"] == "downloaded":
                    try:
                        part = Path(previous["path"])
                        with self.directory(str(part.parent)) as parent:
                            fd = os.open(part.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                            try:
                                info = os.fstat(fd)
                                if stat.S_ISREG(info.st_mode) and info.st_size:
                                    status = "downloaded"
                            finally:
                                os.close(fd)
                    except (OSError, TypeError):
                        pass
                if status != "downloaded" and track.get("availability") in {"local", "unavailable"}:
                    status = "unavailable"
                    message = "Local Spotify file." if track["availability"] == "local" else "Unavailable for this account."
                # Preserve pending retry timing when refreshing an incomplete snapshot within a job.
                next_attempt = previous["next_attempt"] if previous and item["refreshed_job"] == job_id else 0
                self.db.execute("""
                    INSERT INTO tracks(item_id,key,uri,name,artist,album,duration_ms,availability,position,status,message,next_attempt,thumbnail_url,membership_job)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(item_id,key) DO UPDATE SET
                    uri=excluded.uri,name=excluded.name,artist=excluded.artist,album=excluded.album,
                    duration_ms=excluded.duration_ms,availability=excluded.availability,position=excluded.position,
                    status=excluded.status,message=excluded.message,next_attempt=excluded.next_attempt,
                    thumbnail_url=COALESCE(excluded.thumbnail_url,tracks.thumbnail_url),membership_job=excluded.membership_job
                """, (item_id, key, track["uri"], track.get("name") or "Unresolved track", track.get("artist") or "",
                      track.get("album") or "", track.get("duration_ms") or 0, track.get("availability") or "unknown",
                      position, status, message, next_attempt, validate_thumbnail_url(track.get("thumbnail_url")), job_id))
            message = metadata.get("message") or (None if metadata.get("complete") else "Playlist membership is incomplete; deletion disabled.")
            if deletion_messages:
                message = " ".join(filter(None, [message, *deletion_messages]))
            self.db.execute("""UPDATE items SET name=?,artist=?,folder=?,complete=?,revision=?,message=?,
                refreshed_job=?,next_metadata=?,thumbnail_url=COALESCE(?,thumbnail_url) WHERE id=?""",
                (metadata.get("name") or item["name"], metadata.get("artist") or "", folder,
                 int(bool(metadata.get("complete"))), metadata.get("revision"), message, job_id,
                 self.clock() + 60, validate_thumbnail_url(metadata.get("thumbnail_url")), item_id))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.update_sync_targets(job_id)

    def snapshot(self):
        now = self.now()
        quality = self.setting("quality", "auto")
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        # A bind-mounted localtime may still have the image's misleading Etc/UTC symlink.
        zone = os.environ.get("TZ") or getattr(now.tzinfo, "key", None)
        if zone == "Computer local time" or not zone:
            zone = now.tzname() or "Computer local time"
        items = []
        for row in self.db.execute("SELECT * FROM items ORDER BY id"):
            item = dict(row)
            tracks = list(self.db.execute("SELECT * FROM tracks WHERE item_id=? ORDER BY position", (item["id"],)))
            item["track_count"] = len(tracks)
            item["downloaded_count"] = sum(t["status"] == "downloaded" for t in tracks)
            item["failed_count"] = sum(t["status"] in {"failed", "blocked", "auth_required"} for t in tracks)
            item["unavailable_count"] = sum(t["status"] == "unavailable" for t in tracks)
            item["pending_quality_count"] = sum(t["status"] == "downloaded" and
                self.quality_pending(t, quality) for t in tracks)
            item["incomplete"] = not bool(item.pop("complete"))
            item["errors"] = [{"name": t["name"], "status": t["status"], "message": t["message"],
                               "uri": t["uri"], "thumbnail_url": t["thumbnail_url"]}
                              for t in tracks if t["message"]]
            item["errors"].extend({"name": t["name"], "status": "source", "message": t["source_message"],
                                   "uri": t["uri"], "thumbnail_url": t["thumbnail_url"]}
                                  for t in tracks if t["source_message"])
            if item["message"]:
                item["errors"].insert(0, {"name": item["name"], "status": "metadata", "message": item["message"],
                                          "uri": item["uri"], "thumbnail_url": item["thumbnail_url"]})
            items.append(item)
        return {"account": dict(self.account), "settings": {"format": self.setting("format", "ogg"),
                                                           "quality": quality},
                "schedule": {"time": "03:00", "retry_until": "08:00", "timezone": zone or str(now.tzinfo),
                             "next_run": next_run.isoformat()}, "sync": dict(self.sync), "items": items,
                "conversion": dict(self.conversion, errors=list(self.conversion["errors"]), skips=list(self.conversion["skips"]))}

    def auth_failed(self, result):
        if result.get("status") == "auth_required":
            self.account.update(connected=False, message=result.get("message") or "Reconnect Spotify to continue.")
            self.set_setting("auth_valid", False)
            return True
        return False

    @staticmethod
    def delay(result):
        value = result.get("retry_after", 0)
        try:
            value = float(value)
            return max(60, value) if math.isfinite(value) else 60
        except (TypeError, ValueError):
            return 60

    async def worker_call(self, command, *, retry_job=None, **values):
        while self.setting("server_cooldown", 0) > self.clock():
            if retry_job is not None and not self.retry_allowed(retry_job):
                return None
            self.sync["message"] = "Waiting for Spotify's server rate-limit cooldown."
            await self.sleep(min(60, self.setting("server_cooldown") - self.clock()))
        if retry_job is not None and not self.retry_allowed(retry_job):
            return None
        result = await self.runner.run(command, **values)
        # This is the worker's HTTP 429 classification, not generic 403/blocked failures.
        if result.get("status") == "blocked" and result.get("message") == "Spotify rate limit reached.":
            self.set_setting("server_cooldown", max(self.setting("server_cooldown", 0),
                                                   self.clock() + self.delay(result)))
        return result

    async def refresh(self, item_id, job_id, *, retry_job=None):
        item = self.item(item_id)
        self.sync.update(current_item=item["name"], current_track=None)
        result = await self.worker_call("metadata", uri=item["uri"], retry_job=retry_job)
        if result is None:
            return True
        if result.get("status") == "ok":
            self.reconcile(item_id, result, job_id)
        else:
            self.db.execute("""UPDATE items SET complete=0,message=?,refreshed_job=?,next_metadata=? WHERE id=?""",
                             (result.get("message") or "Metadata refresh failed.", None if result.get("status") == "auth_required" else job_id,
                              self.clock() + self.delay(result), item_id))
        self.update_sync_targets(job_id)
        return not self.auth_failed(result)

    async def download(self, row, job_id, *, retry_job=None, quality=None):
        track = dict(row)
        quality = self.setting("quality", "auto") if quality is None else quality
        item = self.item(track["item_id"])
        self.sync.update(current_item=item["name"], current_track=track["name"])
        if track["availability"] in {"local", "unavailable"} and track["status"] != "downloaded":
            return True
        if not re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", track["uri"]):
            if track["status"] == "downloaded":
                return True
            result = {"status": "error", "message": "Unresolved Spotify track; awaiting metadata refresh."}
        elif not item["folder"]:
            result = {"status": "error", "message": "Output folder awaits successful metadata refresh."}
        else:
            self.recover_publications()
            if self.db.execute("SELECT 1 FROM publications WHERE item_id=? AND track_key=?",
                                (track["item_id"], track["key"])).fetchone():
                raise OSError("Pending publication needs managed path recovery.")
            with self.directory(item["folder"], create=True) as folder:
                # The child accesses our descriptor through /proc, not a swappable music pathname.
                staging_parent = f"/proc/{os.getpid()}/fd/{folder}"
                with tempfile.TemporaryDirectory(prefix=".spotisync-", dir=staging_parent) as staging:
                    try:
                        return await self._download_staged(track, item, job_id, retry_job, folder, staging, quality)
                    except OSError as exc:
                        if track["status"] != "downloaded":
                            raise
                        self.source_failed(track, job_id, {"message": str(exc)})
                        return True
        return self.download_failed(track, job_id, result)

    async def _download_staged(self, track, item, job_id, retry_job, folder, staging, quality):
        replacing = track["status"] == "downloaded"
        fmt = track["format"] if replacing else self.setting("format", "ogg")
        if replacing:
            try:
                with self.verified_file(track["path"], track["hash"]):
                    pass
            except FileNotFoundError:
                pass
        result = await self.ensure_source(track, job_id, retry_job=retry_job, quality=quality)
        if result is None:
            return True
        if result.get("status") != "ok":
            if track["status"] == "downloaded":
                self.source_failed(track, job_id, result)
                return not self.auth_failed(result)
            return self.download_failed(track, job_id, result)
        if replacing and self.output_matches_source(track, result["source"]):
            self.mark_output_source(track, result["source"], quality, job_id)
            return True
        result = await self.convert_source(track, staging, fmt, quality=quality)
        if result.get("status") != "ok":
            if replacing:
                self.source_failed(track, job_id, result)
                return True
            return self.download_failed(track, job_id, result)
        if replacing:
            try:
                with self.verified_file(track["path"], track["hash"]):
                    pass
            except FileNotFoundError:
                pass
        return self.publish_staged(track, item, job_id, folder, staging, result, fmt,
                                   quality=quality, replacing=replacing)

    async def ensure_source(self, track, job_id, *, retry_job=None, quality="auto", offline=False):
        uri = track["uri"]
        relative = native_path(uri, quality)
        source = self.resolve_source(track, quality)
        if source:
            return {"status": "ok", "source": source}
        # Preserve genuinely uncached legacy native playback, not extra copies just to alias a quality.
        candidates = list(self.db.execute("""SELECT * FROM tracks WHERE uri=? AND format='ogg'
            AND status='downloaded' AND path IS NOT NULL AND hash IS NOT NULL AND NOT EXISTS
            (SELECT 1 FROM source_versions WHERE uri=tracks.uri AND hash=tracks.hash) ORDER BY item_id""", (uri,)))
        adopt = candidates and not self.db.execute(
            "SELECT 1 FROM source_versions WHERE uri=? AND quality='auto'", (uri,)).fetchone()
        if offline and not adopt:
            bitrate = SOURCE_BITRATES.get(quality)
            detail = f" at {bitrate // 1000} kbps" if bitrate else ""
            return {"status": "sync_required", "message": f"Sync required: no verified native original{detail} is cached; existing playback is kept."}
        physical_quality = "auto" if adopt else quality
        relative = native_path(uri, physical_quality)
        with self.directory(".spotisync-originals", create=True) as folder:
            # Unjournaled collisions must never be adopted, overwritten, or renamed away.
            try:
                os.stat(Path(relative).name, dir_fd=folder, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise OSError("Unknown file occupies native original path; file kept.")
            with tempfile.TemporaryDirectory(prefix=".spotisync-", dir=f"/proc/{os.getpid()}/fd/{folder}") as staging:
                result = None
                for candidate in candidates if adopt else []:
                    try:
                        part = Path(candidate["path"])
                        with self.directory(str(part.parent)) as parent, self.verified_file(candidate["path"], candidate["hash"]) as fd:
                            entry, info = os.stat(part.name, dir_fd=parent, follow_symlinks=False), os.fstat(fd)
                            if (entry.st_dev, entry.st_ino) != (info.st_dev, info.st_ino):
                                raise OSError("Legacy Ogg directory changed; adoption blocked.")
                            # A legacy hash proves ownership, not codec, decode, tags, artwork, or Spotify identity.
                            result = await self.runner.run("convert", uri=uri, format="ogg",
                                source_path=f"/proc/{os.getpid()}/fd/{parent}/{part.name}", output_dir=staging,
                                timeout=min(1800, max(180, track["duration_ms"] / 1000 * 3 + 120)))
                        if result.get("status") != "ok":
                            return result
                        filename = result.get("filename")
                        if not isinstance(filename, str) or Path(filename).name != filename or filename in {"", ".", ".."}:
                            raise OSError("Worker returned an invalid output filename.")
                        stage_fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                        try:
                            copied = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=stage_fd)
                            try:
                                info = os.fstat(copied)
                                if (not stat.S_ISREG(info.st_mode) or not info.st_size
                                        or (info.st_dev, info.st_ino) == (entry.st_dev, entry.st_ino)
                                        or digest(Path(f"/proc/self/fd/{copied}")) != candidate["hash"]):
                                    raise OSError("Native original copy failed verification.")
                            finally:
                                os.close(copied)
                        finally:
                            os.close(stage_fd)
                        break
                    except FileNotFoundError:
                        if result is not None:
                            raise
                        continue
                if result is None:
                    if offline:
                        return {"status": "sync_required", "message": "Sync required: native original is missing; existing playback is kept."}
                    if not self.account["connected"]:
                        return {"status": "auth_required", "message": "Connect Spotify to backfill missing native originals; existing playback is kept."}
                    result = await self.worker_call("download", uri=uri, output_dir=staging, format="ogg", quality=quality,
                        retry_job=retry_job, timeout=min(1800, max(180, track["duration_ms"] / 1000 * 3 + 120)))
                    if result is None or result.get("status") != "ok":
                        return result
                self.publish_staged(track, {"folder": ".spotisync-originals"}, job_id, folder, staging,
                                    result, "ogg", kind="source", quality=physical_quality)
        if adopt:
            return await self.ensure_source(track, job_id, retry_job=retry_job, quality=quality, offline=offline)
        source = self.resolve_source(track, quality)
        return ({"status": "ok", "source": source} if source else
                {"status": "blocked", "message": "Downloaded native original does not match the requested actual bitrate; playback is kept."})

    async def convert_source(self, track, staging, fmt, *, quality="auto", verify_output=False):
        if fmt not in {"ogg", "mp3", "aac"}:
            raise ValueError("Choose Ogg, MP3, or AAC output format.")
        source = self.resolve_source(track, quality)
        if not source:
            raise OSError("Verified native original is missing; conversion blocked.")
        part = Path(source["path"])
        with self.directory(str(part.parent)) as parent, self.verified_file(source["path"], source["hash"]) as fd:
            entry, info = os.stat(part.name, dir_fd=parent, follow_symlinks=False), os.fstat(fd)
            if (entry.st_dev, entry.st_ino) != (info.st_dev, info.st_ino):
                raise OSError("Native original directory changed; conversion blocked.")
            if fmt == "ogg" and not verify_output:
                with os.fdopen(os.dup(fd), "rb") as original, (Path(staging) / "native.ogg").open("xb") as copy:
                    shutil.copyfileobj(original, copy)
                if digest(Path(staging) / "native.ogg") != source["hash"]:
                    raise OSError("Ogg output copy failed verification.")
                result = {"status": "ok", "filename": "native.ogg"}
            else:
                # Offline conversion must not wait for Spotify's global server cooldown.
                result = await self.runner.run("convert", uri=track["uri"],
                    source_path=f"/proc/{os.getpid()}/fd/{parent}/{part.name}", output_dir=staging, format=fmt,
                    timeout=min(1800, max(180, track["duration_ms"] / 1000 * 3 + 120)))
        return result

    def publish_staged(self, track, item, job_id, folder, staging, result, fmt, *, kind="output", replacing=False, quality="auto"):
        filename = result.get("filename")
        if not isinstance(filename, str) or Path(filename).name != filename or filename in {"", ".", ".."}:
            raise OSError("Worker returned an invalid output filename.")
        metadata = result.get("track") or track
        ident = track["uri"].split(":")[-1]
        title = f"{metadata.get('artist') or track['artist']} - {metadata.get('name') or track['name']}"
        suffix = {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[fmt]
        native = self.resolve_source(track, quality) if kind == "output" else None
        if kind == "output" and not native:
            raise OSError("Verified native original is missing; publication blocked.")
        stage_fd = os.open(staging, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            source = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=stage_fd)
            try:
                info = os.fstat(source)
                if not stat.S_ISREG(info.st_mode) or not info.st_size:
                    raise OSError("Worker returned no output file.")
                checksum = digest(Path(f"/proc/self/fd/{source}"))
                bitrate = inspect_native_bitrate(Path(f"/proc/self/fd/{source}")) if kind == "source" else None
                if kind == "source":
                    os.fchmod(source, 0o444)
                os.fsync(source)
                number = 0
                while True:
                    tail = f" [{ident}]" + (f" - {number}" if number else "") + suffix
                    name = (Path(native_path(track["uri"], quality)).name if kind == "source" else
                            safe_name(title, min(110, 255 - len(tail.encode("utf-8")))) + tail)
                    relative = str(Path(item["folder"]) / name)
                    operation = {"id": uuid.uuid4().hex, "item_id": track["item_id"],
                                 "track_key": track["key"], "path": relative, "hash": checksum,
                                 "format": fmt, "job_id": job_id, "device": info.st_dev, "inode": info.st_ino,
                                 "kind": kind, "source_uri": track["uri"] if kind == "source" else None,
                                 "quality": quality,
                                 "bitrate": bitrate, "source_hash": native["hash"] if native else None,
                                 "source_bitrate": native["bitrate"] if native else None,
                                 "old_path": track["path"] if replacing else None,
                                 "old_hash": track["hash"] if replacing else None}
                    self.db.execute("""INSERT INTO publications
                        (id,item_id,track_key,path,hash,format,job_id,device,inode,kind,source_uri,old_path,old_hash,quality,
                         bitrate,source_hash,source_bitrate)
                        VALUES (:id,:item_id,:track_key,:path,:hash,:format,:job_id,:device,:inode,
                                :kind,:source_uri,:old_path,:old_hash,:quality,:bitrate,:source_hash,:source_bitrate)""", operation)
                    try:
                        os.link(filename, name, src_dir_fd=stage_fd, dst_dir_fd=folder, follow_symlinks=False)
                    except FileExistsError:
                        self.db.execute("DELETE FROM publications WHERE id=?", (operation["id"],))
                        if kind == "source":
                            raise OSError("Unknown file occupies native original path; file kept.") from None
                        number += 1
                        continue
                    try:
                        published = os.stat(name, dir_fd=folder, follow_symlinks=False)
                        if (published.st_dev, published.st_ino) != (info.st_dev, info.st_ino):
                            raise OSError("Publication was replaced outside SpotiSync.")
                        if digest(Path(f"/proc/self/fd/{source}")) != checksum:
                            raise OSError("Publication changed during verification.")
                        os.fsync(folder)
                        with self.directory(item["folder"]) as current:
                            current_info, folder_info = os.fstat(current), os.fstat(folder)
                            if (current_info.st_dev, current_info.st_ino) != (folder_info.st_dev, folder_info.st_ino):
                                raise OSError("Managed directory changed during download.")
                        self.finish_publication(operation)
                    except (sqlite3.Error, OSError):
                        committed = self.db.execute("SELECT 1 FROM source_versions WHERE uri=? AND quality=? AND path=? AND hash=?",
                            (track["uri"], quality, relative, checksum)).fetchone() if kind == "source" else self.db.execute(
                            "SELECT 1 FROM tracks WHERE item_id=? AND key=? AND path=? AND hash=?",
                            (track["item_id"], track["key"], relative, checksum)).fetchone()
                        if committed:
                            raise  # Keep the journal and active output while cleanup is pending.
                        try:
                            self._remove_managed_file(operation, directory_fd=folder)
                            self.db.execute("DELETE FROM publications WHERE id=?", (operation["id"],))
                        except (sqlite3.Error, OSError):
                            pass  # The durable intent remains available for conservative recovery.
                        raise
                    break
            finally:
                os.close(source)
        finally:
            os.close(stage_fd)
        return True

    def download_failed(self, track, job_id, result):
        status = result.get("status")
        status = status if status in {"unavailable", "auth_required", "blocked"} else "failed"
        self.db.execute("""UPDATE tracks SET status=?,message=?,next_attempt=?,attempt_job=? WHERE item_id=? AND key=?""",
                        (status, result.get("message") or "Download failed.", self.clock() + self.delay(result),
                         job_id, track["item_id"], track["key"]))
        return not self.auth_failed(result)

    def source_failed(self, track, job_id, result):
        self.db.execute("""UPDATE tracks SET source_message=?,source_next_attempt=?,source_attempt_job=?
            WHERE item_id=? AND key=?""", (result.get("message") or "Native original backfill failed.",
            self.clock() + self.delay(result), None if result.get("status") == "auth_required" else job_id,
            track["item_id"], track["key"]))

    def start_conversion(self):
        if self.busy:
            raise RuntimeError("Another job is already running.")
        job = self.setting("conversion_job")
        if not job:
            fmt = self.setting("format", "ogg")
            if fmt not in {"ogg", "mp3", "aac"}:
                raise ValueError("Choose Ogg, MP3, or AAC output format.")
            quality = self.setting("quality", "auto")
            if quality not in SOURCE_QUALITIES:
                raise ValueError("Choose auto, normal, high, or very_high source quality.")
            queue = [{"item_id": row["item_id"], "key": row["key"], "uri": row["uri"]} for row in
                     self.db.execute("SELECT * FROM tracks WHERE status='downloaded' ORDER BY item_id,position")]
            job = {"id": uuid.uuid4().hex, "format": fmt, "quality": quality, "queue": queue, "next": 0,
                    "total": len(queue), "completed": 0, "failed": 0, "errors": [], "skipped": 0, "skips": []}
            self.set_setting("conversion_job", job)
        job.pop("waiting_for_auth", None)
        job.setdefault("quality", "auto")
        job.setdefault("skipped", 0)
        job.setdefault("skips", [])
        self.set_setting("conversion_job", job)
        self.conversion.update({key: job[key] for key in ("total", "completed", "failed", "skipped", "skips", "format", "quality", "errors")})
        self.conversion.update(running=True, current_track=None, message=None)
        self.sync.update(running=True, phase="conversion", current_item=None, current_track=None, message=None)
        self.task = asyncio.create_task(self.run_conversion(job))

    async def run_conversion(self, job):
        quality = job.get("quality", "auto")
        try:
            self.recover_publications()
            while job["next"] < job["total"]:
                entry = job["queue"][job["next"]]
                row = self.db.execute("SELECT * FROM tracks WHERE item_id=? AND key=? AND uri=?",
                                      (entry["item_id"], entry["key"], entry["uri"])).fetchone()
                track = dict(row) if row else None
                name = track["name"] if track else entry["uri"]
                self.conversion["current_track"] = name
                self.sync["current_track"] = name
                try:
                    if not track or track["status"] != "downloaded":
                        job["skipped"] += 1
                        job["skips"].append({"name": name, "message": "Sync required: track is no longer an app-managed download; files kept."})
                        job["next"] += 1
                        self.set_setting("conversion_job", job)
                        self.conversion.update(skipped=job["skipped"], skips=job["skips"])
                        continue
                    if (not track["path"] or not Path(track["path"]).parts
                            or Path(track["path"]).parts[0] == ".spotisync-originals"):
                        raise OSError("Native originals cannot be active playback outputs.")
                    if self.db.execute("SELECT 1 FROM publications WHERE source_uri=? OR (item_id=? AND track_key=?)",
                                       (track["uri"], track["item_id"], track["key"])).fetchone():
                        raise OSError("Pending publication needs managed path recovery.")
                    # Validate current ownership before backfill or conversion, and again before publication.
                    try:
                        with self.verified_file(track["path"], track["hash"]):
                            pass
                        present = True
                    except FileNotFoundError:
                        present = False
                    result = await self.ensure_source(track, job["id"], quality=quality, offline=True)
                    if result.get("status") == "sync_required":
                        job["skipped"] += 1
                        job["skips"].append({"name": name, "message": result["message"]})
                        job["next"] += 1
                        self.set_setting("conversion_job", job)
                        self.conversion.update(skipped=job["skipped"], skips=job["skips"])
                        continue
                    if result.get("status") != "ok":
                        raise OSError(result.get("message") or "Offline native validation failed.")
                    if not present or track["format"] != job["format"] or not self.output_matches_source(track, result["source"]):
                        item = self.item(track["item_id"])
                        self.sync["current_item"] = item["name"]
                        with self.directory(item["folder"]) as folder:
                            with tempfile.TemporaryDirectory(prefix=".spotisync-", dir=f"/proc/{os.getpid()}/fd/{folder}") as staging:
                                result = await self.convert_source(track, staging, job["format"], quality=quality, verify_output=True)
                                if result.get("status") != "ok":
                                    raise OSError(result.get("message") or "Local conversion failed.")
                                try:
                                    with self.verified_file(track["path"], track["hash"]):
                                        pass
                                except FileNotFoundError:
                                    pass
                                self.publish_staged(track, item, job["id"], folder, staging, result,
                                                    job["format"], replacing=True, quality=quality)
                    else:
                        self.mark_output_source(track, result["source"], quality, job["id"])
                    job["completed"] += 1
                except (OSError, ValueError, sqlite3.Error, RuntimeError) as exc:
                    job["failed"] += 1
                    job["errors"].append({"name": name, "message": str(exc)})
                job["next"] += 1
                self.set_setting("conversion_job", job)
                self.conversion.update({key: job[key] for key in ("completed", "failed", "errors")})
            self.set_setting("conversion_job", None)
            self.conversion["message"] = ("Conversion finished with errors; existing playback is kept." if job["failed"] else
                "Offline conversion finished; skipped tracks require Sync." if job["skipped"] else "Conversion finished.")
        except asyncio.CancelledError:
            self.conversion["message"] = "Conversion paused; queued work will resume."
            raise
        finally:
            self.conversion.update(running=False, current_track=None)
            self.sync.update(running=False, phase=None, current_item=None, current_track=None)

    def start_job(self, item_id=None, *, nightly=False, existing=None):
        if self.busy:
            raise RuntimeError("Another job is already running.")
        if not self.account["connected"]:
            raise RuntimeError("Connect Spotify before syncing.")
        self.recover_publications()
        now = self.now()
        if item_id is not None:
            self.item(item_id)
        saved = self.setting("job")
        if in_window(now):
            if self.setting("last_nightly") != now.date().isoformat():
                # A manual request cannot monopolize the nightly window before the full pass.
                nightly, item_id, existing = True, None, None
            elif not existing and saved and saved["date"] == now.date().isoformat():
                existing = saved
        else:
            if not existing and saved and saved["phase"] == "first_pass":
                existing = saved
            if existing and existing["phase"] == "first_pass":
                existing = dict(existing, retry=False)
        available = [r[0] for r in self.db.execute("SELECT id FROM items ORDER BY id")]
        ids = [item_id] if item_id is not None else available
        if existing:
            existing = dict(existing, item_ids=[ident for ident in existing["item_ids"] if ident in available])
        if not ids and not existing:
            raise ValueError("Add a media item before syncing.")
        quality = existing.get("quality", "auto") if existing else self.setting("quality", "auto")
        if quality not in SOURCE_QUALITIES:
            raise ValueError("Choose auto, normal, high, or very_high source quality.")
        job = existing or {"id": uuid.uuid4().hex, "date": now.date().isoformat(), "phase": "first_pass",
                           "item_ids": ids, "retry": nightly or in_window(now), "quality": quality}
        job.setdefault("quality", "auto")
        self.set_setting("job", job)
        self.begin_sync_progress(job, legacy=bool(existing))
        if nightly:
            self.set_setting("last_nightly", now.date().isoformat())
        self.sync.update(running=True, phase=job["phase"], message=None)
        self.task = asyncio.create_task(self.run_job(job))

    def summarize(self, item_id, *, quality=None):
        item = self.item(item_id)
        quality = self.setting("quality", "auto") if quality is None else quality
        tracks = list(self.db.execute("SELECT * FROM tracks WHERE item_id=?", (item_id,)))
        statuses = [row["status"] for row in tracks]
        if not item["complete"]:
            status = "incomplete"
        elif (any(status in {"failed", "blocked", "auth_required", "pending"} for status in statuses)
               or any(row["source_message"] or (row["status"] == "downloaded" and self.quality_pending(row, quality)) for row in tracks)):
            status = "failed"
        elif "unavailable" in statuses:
            status = "unavailable"
        else:
            status = "synced"
        self.db.execute("UPDATE items SET status=?,last_sync=? WHERE id=?",
                        (status, self.now().isoformat(), item_id))

    def retry_allowed(self, job):
        return bool(job["retry"] and job["date"] == self.now().date().isoformat() and in_window(self.now()))

    async def run_job(self, job):
        quality = job.get("quality", "auto")
        try:
            # Recover ownership before a fresh membership snapshot can remove track rows.
            self.recover_publications()
            if self.db.execute("SELECT 1 FROM publications LIMIT 1").fetchone():
                raise OSError("Pending publication needs managed path recovery.")
            ids = job["item_ids"]
            self.update_sync_targets(job["id"], ready=True if job["phase"] == "retry" else None)
            if job["phase"] == "first_pass":
                self.sync["message"] = "Preparing download progress: refreshing tracked membership."
                for item_id in ids:
                    item = self.item(item_id)
                    self.db.execute("UPDATE items SET status='syncing' WHERE id=?", (item_id,))
                    if item["refreshed_job"] != job["id"] and not await self.refresh(item_id, job["id"]):
                        return
                self.update_sync_targets(job["id"], ready=True)
                self.sync["message"] = None
                for item_id in ids:
                    for row in list(self.db.execute("""SELECT * FROM tracks WHERE item_id=? AND (
                        (status NOT IN ('downloaded','unavailable') AND
                            (attempt_job IS NULL OR attempt_job != ? OR status='auth_required'))
                        OR (status='downloaded' AND (source_attempt_job IS NULL OR source_attempt_job != ?))) ORDER BY position""",
                                                    (item_id, job["id"], job["id"]))):
                        if not await self.download(row, job["id"], quality=quality):
                            return
                    self.summarize(item_id, quality=quality)
                job["phase"] = "retry"
                self.set_setting("job", job)
            self.sync.update(phase="retry", current_track=None)
            while self.retry_allowed(job):
                pending = False
                for item_id in ids:
                    item = self.item(item_id)
                    if not item["complete"]:
                        pending = True
                        if item["next_metadata"] <= self.clock() and self.retry_allowed(job):
                            if not await self.refresh(item_id, job["id"], retry_job=job):
                                return
                    for row in list(self.db.execute("""SELECT * FROM tracks WHERE item_id=? AND (
                        status NOT IN ('downloaded','unavailable') OR (status='downloaded'
                        AND availability NOT IN ('local','unavailable') AND uri LIKE 'spotify:track:%'
                        AND (source_message IS NOT NULL OR source_hash IS NULL OR source_bitrate IS NULL
                            OR (? != 'auto' AND source_bitrate != ?)
                            OR (? = 'auto' AND source_bitrate < (SELECT MAX(bitrate) FROM source_versions WHERE uri=tracks.uri))
                            OR NOT EXISTS (SELECT 1 FROM source_versions WHERE uri=tracks.uri
                                AND hash=tracks.source_hash AND bitrate=tracks.source_bitrate)))) ORDER BY position""",
                                                    (item_id, quality, SOURCE_BITRATES.get(quality), quality))):
                        pending = True
                        if not self.retry_allowed(job):
                            break
                        due = row["source_next_attempt"] if row["status"] == "downloaded" else row["next_attempt"]
                        if due <= self.clock():
                            if not await self.download(row, job["id"], retry_job=job, quality=quality):
                                return
                    self.summarize(item_id, quality=quality)
                if not pending:
                    break
                if not self.retry_allowed(job):
                    break
                self.sync.update(current_track=None, message="Retrying failed work after 60 seconds; no new retries after 8am.")
                await self.sleep(60)
            self.set_setting("job", None)
            self.sync["message"] = "Sync pass finished. Pending failures remain for the next nightly run."
        except asyncio.CancelledError:
            raise
        except (OSError, ValueError, sqlite3.Error, RuntimeError):
            self.sync["message"] = "Sync paused: check music folder permissions, free disk space, and managed paths."
            # Do not tight-loop on filesystem/configuration failures.
            self.set_setting("job", None)
        finally:
            for item_id in job["item_ids"]:
                try:
                    self.summarize(item_id, quality=quality)
                except ValueError:
                    pass
            self.sync.update(running=False, phase=None, current_item=None, current_track=None)

    async def connect(self):
        def event(value):
            url = value.get("url")
            if value.get("event") == "auth_url" and isinstance(url, str):
                parsed = urlsplit(url)
                if parsed.scheme == "https" and parsed.netloc == "accounts.spotify.com":
                    self.account["auth_url"] = url
        try:
            result = await self.worker_call("auth", on_event=event, timeout=300)
            valid = result.get("status") == "ok"
            self.account.update(connected=valid, username=result.get("username") if valid else None,
                                message=None if valid else result.get("message") or "Spotify connection failed.")
            self.set_setting("auth_valid", valid)
            self.set_setting("username", self.account["username"])
        finally:
            self.account.update(connecting=False, auth_url=None)

    def start_connect(self):
        if self.busy:
            raise RuntimeError("Wait for current job before connecting Spotify.")
        self.account.update(connecting=True, auth_url=None, message=None)
        self.task = asyncio.create_task(self.connect())

    async def disconnect(self):
        async def cleanup():
            self.account["connected"] = False
            self.set_setting("auth_valid", False)
            for task in (self.probe, self.task):
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            await self.runner.stop()
            for name in ("credentials.json", "api_credentials.json"):
                (self.state_dir / name).unlink(missing_ok=True)
            self.account.update(connected=False, username=None, connecting=False, auth_url=None, message=None)
            self.set_setting("auth_valid", False)
            self.set_setting("username", None)

        if self._disconnecting is None or self._disconnecting.done():
            self._auth_epoch += 1
            self._disconnecting = asyncio.create_task(cleanup())
        await protected(self._disconnecting)

    async def tick(self):
        now = self.now()
        if self.busy:
            return
        conversion_job = self.setting("conversion_job")
        if conversion_job:
            self.start_conversion()
            return
        if not self.account["connected"] or not in_window(now):
            return
        existing = self.setting("job")
        if existing and existing["date"] == now.date().isoformat():
            self.start_job(existing=existing)
        elif self.setting("last_nightly") != now.date().isoformat() and self.db.execute("SELECT 1 FROM items LIMIT 1").fetchone():
            self.start_job(nightly=True)

    async def startup(self):
        job = self.setting("conversion_job")
        if job:
            # Saved conversions, including old auth-paused jobs, are always strictly offline.
            self.ready = True
            self.start_conversion()
            try:
                await self.task
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    raise
            self.ready = False
        if (self.state_dir / "credentials.json").exists() and self.setting("auth_valid", True):
            epoch = self._auth_epoch
            self.probe = asyncio.create_task(self.worker_call("probe", timeout=90))
            try:
                result = await self.probe
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling() or epoch == self._auth_epoch:
                    raise
            else:
                if epoch == self._auth_epoch:
                    valid = result.get("status") == "ok"
                    self.account.update(connected=valid, username=result.get("username"),
                                        message=None if valid else result.get("message"))
                    if result.get("status") == "auth_required":
                        self.set_setting("auth_valid", False)
        self.ready = True
        while True:
            await self.tick()
            await self.sleep(10)

    def start(self):
        self.scheduler = asyncio.create_task(self.startup())

    async def close(self):
        if self._closed:
            return
        self._closing = True

        async def cleanup():
            if self._disconnecting is not None and not self._disconnecting.done():
                await self._disconnecting
            for task in (self.scheduler, self.task):
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            await self.runner.stop()
            self.db.close()
            os.close(self.music_fd)
            self._closed = True

        if self._close_task is None:
            self._close_task = asyncio.create_task(cleanup())
        await protected(self._close_task)
