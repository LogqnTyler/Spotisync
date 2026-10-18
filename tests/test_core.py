"""Offline contract tests for durable scheduling and conservative file ownership."""

import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import threading
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from spotisync.core import SyncService, WorkerRunner, digest, in_window
import spotisync.core as core


REAL_INSPECT_NATIVE_BITRATE = core.inspect_native_bitrate


@pytest.fixture(autouse=True)
def native_bitrates(monkeypatch):
    """Nominal-header fixture only; codec/decode/identity validation remains at FakeRunner's worker boundary."""
    overrides = {}

    def inspect(path):
        data = Path(path).read_bytes()
        if data in overrides:
            bitrate = overrides[data]
        elif data.startswith((b"mock worker-verified audio: ", b"verified legacy ogg:")):
            quality = data.split(b";quality=", 1)[1].decode() if b";quality=" in data else "normal"
            bitrate = core.SOURCE_BITRATES.get(quality)
        else:
            bitrate = None
        if bitrate not in core.SOURCE_BITRATES.values():
            raise OSError("Native original has an unknown or corrupt nominal bitrate; file kept.")
        return bitrate

    monkeypatch.setattr(core, "inspect_native_bitrate", inspect)
    return overrides


@pytest.fixture(autouse=True)
def public_get(monkeypatch):
    get = Mock(side_effect=AssertionError("Tests must never request live Spotify oEmbed"))
    monkeypatch.setattr(core.requests, "get", get)
    return get


def oembed_response(data=None, *, status=200, content_type="application/json", body=None):
    response = core.requests.Response()
    response.status_code = status
    response.headers["Content-Type"] = content_type
    response._content = body if body is not None else json.dumps(data).encode()
    response._content_consumed = True
    return response


def uri(kind, number):
    return f"spotify:{kind}:{number:022d}"


def track(number, *, availability="available", **values):
    return {"id": f"{number:022d}", "uri": uri("track", number),
            "name": f"Song {number}", "artist": "Test Artist", "album": "Test Album",
            "duration_ms": 1000, "availability": availability, **values}


def metadata(*tracks, complete=True):
    return {"status": "ok", "name": "Test Playlist", "artist": "Test Artist",
            "complete": complete, "revision": "fake-revision", "tracks": list(tracks)}


class FakeClock:
    def __init__(self, hour=3, minute=0, second=0):
        self.value = datetime(2026, 10, 8, hour, minute, second, tzinfo=timezone.utc)
        self.sleeps = []

    def now(self):
        return self.value

    def time(self):
        return self.value.timestamp()

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        assert len(self.sleeps) < 400, "Unexpected unbounded retry loop"
        self.advance(seconds)
        await asyncio.sleep(0)


class FakeRunner:
    """Mock verified native fixtures and worker validation, without codecs or live Spotify."""

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.responses = defaultdict(deque)
        self.metadata = {}
        self.on_call = None
        self.output_kind = "file"
        self.stop_count = 0

    def queue(self, command, value, *responses):
        self.responses[command, value].extend(responses)

    async def run(self, command, *, on_event=None, timeout=300, **values):
        call = {"command": command, "at": self.clock.time(), "timeout": timeout, **values}
        self.calls.append(call)
        if self.on_call:
            await self.on_call(call)
        scripted = self.responses[command, values.get("uri")]
        if scripted:
            result = dict(scripted.popleft())
        elif command == "metadata":
            assert values["uri"] in self.metadata, "Unscripted metadata request"
            result = dict(self.metadata[values["uri"]])
        elif command in {"download", "convert"}:
            result = {"status": "ok"}
        elif command in {"auth", "probe"}:
            result = {"status": "ok", "username": "fake-user"}
        else:
            raise AssertionError(f"Unexpected command: {command}")
        if command == "convert" and result["status"] == "ok":
            native = Path(values["source_path"])
            assert native.is_absolute() and not native.is_symlink()
            assert not list(Path(values["output_dir"]).iterdir())
            data = native.read_bytes()
            # Fixture markers stand in for the worker's codec, full-decode, metadata, artwork, and URI checks.
            valid = {b"mock worker-verified audio: " + values["uri"].encode() + suffix for suffix in
                     (b"", b";quality=normal", b";quality=high", b";quality=very_high")}
            valid.add(b"verified legacy ogg:" + values["uri"].encode())
            if data not in valid:
                return {"status": "error", "message": "Fake native source validation failed."}
        if command in {"download", "convert"} and result["status"] == "ok":
            filename = "verified" + {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[values["format"]]
            source = Path(values["output_dir"]) / filename
            if self.output_kind == "file":
                data = (Path(values["source_path"]).read_bytes() if command == "convert" else
                        b"mock worker-verified audio: " + values["uri"].encode() +
                        (b"" if values.get("quality", "auto") == "auto" else b";quality=" + values["quality"].encode()))
                source.write_bytes(data if values["format"] == "ogg" else values["format"].encode() + b":" + data)
            elif self.output_kind == "empty":
                source.touch()
            elif self.output_kind == "symlink":
                source.symlink_to(self.outside_file)
            result.setdefault("filename", filename)
        return result

    async def stop(self):
        self.stop_count += 1


@pytest_asyncio.fixture
async def service(tmp_path, monkeypatch):
    async def forbidden_worker(*args, **kwargs):
        pytest.fail("Tests must never invoke a live Spotify worker")

    monkeypatch.setattr(WorkerRunner, "run", forbidden_worker)
    clock = FakeClock()
    runner = FakeRunner(clock)
    value = SyncService(tmp_path / "state", tmp_path / "music", runner=runner,
                        now=clock.now, clock=clock.time, sleeper=clock.sleep)
    value.ready = True
    value.account.update(connected=True, username="fake-user")
    yield value, runner, clock
    await value.close()


def add_item(service, runner, number=100, *tracks, kind="playlist", complete=True):
    item = service.add(uri(kind, number))
    runner.metadata[item["uri"]] = metadata(*tracks, complete=complete)
    return item["id"]


def rows(service, item_id):
    return [dict(row) for row in service.db.execute(
        "SELECT * FROM tracks WHERE item_id=? ORDER BY position", (item_id,))]


def visible_oggs(service):
    return [path for path in service.music_dir.rglob("*.ogg")
            if ".spotisync-originals" not in path.relative_to(service.music_dir).parts]


async def run_pass(service, item_id=None, **kwargs):
    service.start_job(item_id, **kwargs)
    await asyncio.wait_for(service.task, timeout=3)


@pytest.mark.parametrize("host", ["i.scdn.co", "mosaic.scdn.co",
                                 "image-cdn-ak.spotifycdn.com", "image-cdn-fa.spotifycdn.com"])
def test_thumbnail_url_accepts_exact_spotify_image_hosts(host):
    value = f"https://{host}/300/cover"
    assert core.validate_thumbnail_url(value) == value


@pytest.mark.parametrize("value", [None, 42, "", "//i.scdn.co/cover", "http://i.scdn.co/cover",
    "https://evil.example/cover", "https://open.spotify.com/cover",
    "https://i.scdn.co.evil.example/cover", "https://sub.i.scdn.co/cover",
    "https://i.scdn.co@evil.example/cover", "https://user@i.scdn.co/cover",
    "https://user:password@i.scdn.co/cover", "https://i.scdn.co:443/cover",
    "https://i.scdn.co:/cover", "https://i.scdn.co/cover#fragment", "https://i.scdn.co/cover#",
    " https://i.scdn.co/cover", "https://i.scdn.co/cover\n", "https://i.scdn.co/\x00cover",
    "https://i.scdn.co/\x7fcover", "https://i.scdn.co/\x85cover", "https://i.scdn.co\\evil/cover",
    "https://[invalid/cover", "https://i.scdn.co/" + "x" * 2048])
def test_thumbnail_url_rejects_unsafe_urls(value):
    assert core.validate_thumbnail_url(value) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["playlist", "album", "track"])
async def test_public_thumbnail_fetch_is_disconnected_metadata_only_and_cached(service, public_get, kind):
    sync, runner, clock = service
    item_id = add_item(sync, runner, kind=kind)
    sync.account["connected"] = False
    before = sync.snapshot()
    cover = "https://mosaic.scdn.co/300/" + "a" * 160
    public_get.side_effect = None
    public_get.return_value = oembed_response({"thumbnail_url": cover, "title": "Ignored title",
                                              "html": '<iframe src="https://evil.example"></iframe>'})
    assert await sync.thumbnail(item_id) == cover
    public_get.assert_called_once_with(
        "https://open.spotify.com/oembed?url=https%3A%2F%2Fopen.spotify.com%2F" + kind + "%2F" + f"{100:022d}",
        timeout=(3, 8), allow_redirects=False)
    item = sync.item(item_id)
    assert item["thumbnail_checked"] == clock.time()
    assert item["name"] == before["items"][0]["name"]
    assert item["status"] == "pending" and item["folder"] is None
    assert sync.snapshot()["items"][0]["thumbnail_url"] == cover
    assert sync.account == before["account"] and sync.sync == before["sync"]
    assert rows(sync, item_id) == [] and not runner.calls
    assert list(sync.music_dir.iterdir()) == []
    assert sync.setting("auth_valid") is None and sync.setting("job") is None
    assert await sync.media_thumbnail(f"https://open.spotify.com/{kind}/{100:022d}?si=ignored") == cover
    assert public_get.call_count == 1
    clock.advance(24 * 60 * 60 - 1)
    assert await sync.thumbnail(item_id) == cover and public_get.call_count == 1
    clock.advance(1)
    public_get.return_value = oembed_response({"thumbnail_url": "https://i.scdn.co/new"})
    assert await sync.thumbnail(item_id) == "https://i.scdn.co/new"
    assert public_get.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "connection", "redirect", "forbidden", "rate_limit",
                                    "server", "json", "html", "list", "missing", "unsafe"])
async def test_public_thumbnail_failures_are_negative_cached_without_affecting_audio(service, public_get, failure, caplog):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, metadata(track(1)), "seed")
    before_item, before_tracks = sync.item(item_id), rows(sync, item_id)
    before_account, before_sync = dict(sync.account), dict(sync.sync)
    public_get.side_effect = None
    if failure in {"timeout", "connection"}:
        exception = core.requests.Timeout if failure == "timeout" else core.requests.ConnectionError
        public_get.side_effect = exception("private-token https://private.example")
    elif failure in {"redirect", "forbidden", "rate_limit", "server"}:
        public_get.return_value = oembed_response({"thumbnail_url": "https://i.scdn.co/ignored"},
            status={"redirect": 302, "forbidden": 403, "rate_limit": 429, "server": 500}[failure])
    elif failure == "json":
        public_get.return_value = oembed_response(body=b"not json private-token")
    elif failure == "html":
        public_get.return_value = oembed_response({"thumbnail_url": "https://i.scdn.co/ignored"}, content_type="text/html")
    else:
        data = [] if failure == "list" else ({"html": '<img src="https://i.scdn.co/ignored">'}
            if failure == "missing" else {"thumbnail_url": "https://evil.example/cover"})
        public_get.return_value = oembed_response(data)
    assert await sync.thumbnail(item_id) is None
    clock.advance(60 * 60 - 1)
    assert await sync.thumbnail(item_id) is None and public_get.call_count == 1
    clock.advance(1)
    assert await sync.thumbnail(item_id) is None and public_get.call_count == 2
    after = sync.item(item_id)
    assert after.pop("thumbnail_checked") == clock.time()
    before_item.pop("thumbnail_checked")
    assert after == before_item and rows(sync, item_id) == before_tracks
    assert sync.account == before_account and sync.sync == before_sync
    assert sync.setting("auth_valid") is None and sync.setting("server_cooldown") is None
    assert not runner.calls
    assert "private-token" not in caplog.text and "https://private.example" not in caplog.text


@pytest.mark.asyncio
async def test_reconcile_optional_thumbnail_retains_cache_and_public_failure_falls_back(service, public_get):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1), track(2))
    cover = "https://i.scdn.co/image/raw-cover"
    sync.reconcile(item_id, {**metadata(track(1), track(2)), "thumbnail_url": cover}, "seed")
    assert sync.item(item_id)["thumbnail_url"] == cover
    assert sync.item(item_id)["thumbnail_checked"] == 0
    for optional in ({}, {"thumbnail_url": None}, {"thumbnail_url": "https://evil.example/cover"}):
        sync.reconcile(item_id, {**metadata(track(1), complete=False), **optional}, "partial")
        assert len(rows(sync, item_id)) == 2 and sync.item(item_id)["thumbnail_url"] == cover
    public_get.side_effect = core.requests.Timeout("private-token")
    assert await sync.thumbnail(item_id) == cover
    assert sync.item(item_id)["thumbnail_url"] == cover
    sync.reconcile(item_id, metadata(track(1)), "complete")
    assert len(rows(sync, item_id)) == 1 and sync.item(item_id)["thumbnail_url"] == cover
    assert sync.item(item_id)["message"] is None and sync.account["connected"]
    assert not runner.calls


@pytest.mark.asyncio
async def test_thumbnail_migrates_old_item_schema_and_persists_public_cache_on_restart(service, public_get):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, metadata(track(1)), "seed")
    before_item, before_tracks = sync.item(item_id), rows(sync, item_id)
    for name in ("thumbnail_url", "thumbnail_checked"):
        sync.db.execute(f"ALTER TABLE items DROP COLUMN {name}")
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner, now=clock.now, clock=clock.time)
    cover = "https://mosaic.scdn.co/300/public-cover"
    try:
        assert restarted.item(item_id) == before_item
        assert rows(restarted, item_id) == before_tracks
        public_get.side_effect = None
        public_get.return_value = oembed_response({"thumbnail_url": cover})
        assert await restarted.thumbnail(item_id) == cover
        restarted.reconcile(item_id, metadata(track(1)), "raw-without-cover")
        assert restarted.item(item_id)["thumbnail_url"] == cover
        before_tracks[0]["membership_job"] = "raw-without-cover"
    finally:
        await restarted.close()
    again = SyncService(sync.state_dir, sync.music_dir, runner=runner, now=clock.now, clock=clock.time)
    try:
        assert await again.thumbnail(item_id) == cover and public_get.call_count == 1
        assert again.item(item_id)["thumbnail_checked"] == clock.time()
        assert rows(again, item_id) == before_tracks
    finally:
        await again.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["https://evil.example/playlist/" + "1" * 22,
                                   "spotify:playlist:short", "spotify:artist:" + "1" * 22])
async def test_thumbnail_revalidates_stored_uri_and_unknown_item_never_fetches(service, public_get, value):
    sync, runner, _ = service
    with pytest.raises(ValueError, match="not found"):
        await sync.thumbnail(999)
    item_id = add_item(sync, runner)
    sync.db.execute("UPDATE items SET uri=? WHERE id=?", (value, item_id))
    with pytest.raises(ValueError, match="invalid Spotify URI"):
        await sync.thumbnail(item_id)
    public_get.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["remove", "replace", "close", "raw_refresh"])
async def test_thumbnail_fetch_yields_event_loop_and_updates_only_same_item_on_main_thread(service, public_get, action):
    sync, runner, _ = service
    item_id = add_item(sync, runner)
    entered, release = threading.Event(), threading.Event()
    main_thread = threading.get_ident()
    db_threads = []
    sync.db.set_trace_callback(lambda statement: db_threads.append(threading.get_ident()))

    def fetch(*args, **kwargs):
        assert threading.get_ident() != main_thread
        entered.set()
        assert release.wait(3)
        return oembed_response({})

    public_get.side_effect = fetch
    task = asyncio.create_task(sync.thumbnail(item_id))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        if action in {"remove", "replace"}:
            sync.remove(item_id)
            if action == "replace":
                replacement = sync.add(uri("album", 200))
                assert replacement["id"] == item_id
        elif action == "close":
            await sync.close()
        else:
            sync.reconcile(item_id, {**metadata(), "thumbnail_url": "https://i.scdn.co/raw"}, "seed")
    finally:
        release.set()
        result = await asyncio.wait_for(task, 3)
    assert result == ("https://i.scdn.co/raw" if action == "raw_refresh" else None)
    assert set(db_threads) == {main_thread}
    if action == "replace":
        assert sync.item(item_id)["thumbnail_url"] is None
        assert sync.item(item_id)["thumbnail_checked"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cover", ["https://i.scdn.co/image/song-cover", None])
async def test_media_thumbnail_caches_duplicate_failed_song_uris_without_audio_changes(service, public_get, cover):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1), track(2))
    second = add_item(sync, runner, 200, track(1, availability="unavailable"))
    for item_id in (first, second):
        sync.reconcile(item_id, runner.metadata[sync.item(item_id)["uri"]], "seed")
    sync.download_failed(rows(sync, first)[0], "seed", {"status": "error", "message": "Audio-key failed"})
    sync.source_failed(rows(sync, first)[0], "seed", {"message": "Native original failed"})
    sync.account["connected"] = False
    before = rows(sync, first) + rows(sync, second)
    before_account, before_sync = dict(sync.account), dict(sync.sync)
    before_paths = list(sync.music_dir.rglob("*"))
    public_get.side_effect = None if cover else core.requests.Timeout("private-token")
    public_get.return_value = oembed_response({"thumbnail_url": cover})
    assert await sync.media_thumbnail(uri("track", 1)) == cover
    public_get.assert_called_once_with(
        "https://open.spotify.com/oembed?url=https%3A%2F%2Fopen.spotify.com%2Ftrack%2F" + f"{1:022d}",
        timeout=(3, 8), allow_redirects=False)
    after = rows(sync, first) + rows(sync, second)
    for original, updated in zip(before, after):
        matches = updated["uri"] == uri("track", 1)
        assert updated.pop("thumbnail_url") == (cover if matches else None)
        assert updated.pop("thumbnail_checked") == (clock.time() if matches else 0)
        original.pop("thumbnail_url")
        original.pop("thumbnail_checked")
        assert updated == original
    errors = sync.snapshot()["items"][0]["errors"]
    assert [error["status"] for error in errors] == ["failed", "source"]
    assert all(error["uri"] == uri("track", 1) and error["thumbnail_url"] == cover for error in errors)
    assert sync.snapshot()["items"][1]["errors"][0]["thumbnail_url"] == cover
    assert sync.account == before_account and sync.sync == before_sync
    assert not runner.calls and sync.setting("auth_valid") is None
    assert list(sync.music_dir.rglob("*")) == before_paths
    checked = clock.time()
    clock.advance(300)
    third = add_item(sync, runner, 300, track(1))
    sync.reconcile(third, metadata(track(1)), "new-membership")
    assert await sync.media_thumbnail(uri("track", 1)) == cover
    assert public_get.call_count == 1
    assert rows(sync, third)[0]["thumbnail_url"] == cover
    assert rows(sync, third)[0]["thumbnail_checked"] == checked
    ttl = 24 * 60 * 60 if cover else 60 * 60
    clock.advance(ttl - 301)
    assert await sync.media_thumbnail(uri("track", 1)) == cover and public_get.call_count == 1
    clock.advance(1)
    public_get.side_effect = None
    public_get.return_value = oembed_response({"thumbnail_url": "https://i.scdn.co/image/refreshed"})
    assert await sync.media_thumbnail(uri("track", 1)) == "https://i.scdn.co/image/refreshed"
    assert public_get.call_count == 2
    assert all(rows(sync, ident)[0]["thumbnail_checked"] == clock.time() for ident in (first, second, third))


@pytest.mark.asyncio
async def test_reconcile_track_thumbnail_persists_optional_cover_and_errors_include_local_uri(service, public_get):
    sync, runner, clock = service
    local = track(2, uri="spotify:local:Artist:Album:Song:1", availability="local")
    item_id = add_item(sync, runner, 100)
    cover = "https://image-cdn-ak.spotifycdn.com/image/album-cover"
    sync.reconcile(item_id, metadata(track(1, availability="unavailable", thumbnail_url=cover), local), "seed")
    saved = rows(sync, item_id)[0]
    assert saved["thumbnail_url"] == cover and saved["thumbnail_checked"] == 0
    assert saved["status"] == "unavailable" and saved["availability"] == "unavailable"
    sync.db.execute("UPDATE tracks SET next_attempt=123,source_message='Backfill failed' WHERE item_id=? AND key=?",
                    (item_id, saved["key"]))
    public_get.side_effect = core.requests.Timeout("private-token")
    assert await sync.media_thumbnail(saved["uri"]) == cover
    for optional in ({}, {"thumbnail_url": None}, {"thumbnail_url": "https://evil.example/cover"}):
        sync.reconcile(item_id, metadata(track(1, availability="unavailable", **optional), local), "seed")
        row = rows(sync, item_id)[0]
        assert row["thumbnail_url"] == cover and row["thumbnail_checked"] == clock.time()
        assert row["next_attempt"] == 123 and row["source_message"] == "Backfill failed"
        assert row["status"] == "unavailable" and row["availability"] == "unavailable"
    assert await sync.media_thumbnail(local["uri"]) is None
    assert rows(sync, item_id)[1]["thumbnail_checked"] == 0
    errors = sync.snapshot()["items"][0]["errors"]
    assert [(error["uri"], error["thumbnail_url"], error["status"]) for error in errors] == [
        (saved["uri"], cover, "unavailable"), (local["uri"], None, "unavailable"),
        (saved["uri"], cover, "source")]
    sync.db.execute("UPDATE items SET message='Metadata failed',thumbnail_url=? WHERE id=?", (cover, item_id))
    assert sync.snapshot()["items"][0]["errors"][0] == {
        "name": "Test Playlist", "uri": uri("playlist", 100), "thumbnail_url": cover,
        "status": "metadata", "message": "Metadata failed"}
    assert public_get.call_count == 1 and not runner.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("cover", ["https://i.scdn.co/image/persisted", None])
async def test_track_thumbnail_old_schema_migration_preserves_rows_and_restart_cache(service, public_get, cover):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1, availability="unavailable"))
    sync.reconcile(item_id, metadata(track(1, availability="unavailable")), "seed")
    before = rows(sync, item_id)
    for name in ("thumbnail_url", "thumbnail_checked"):
        sync.db.execute(f"ALTER TABLE tracks DROP COLUMN {name}")
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner, now=clock.now, clock=clock.time)
    try:
        assert rows(restarted, item_id) == before
        public_get.side_effect = None
        public_get.return_value = oembed_response({"thumbnail_url": cover})
        assert await restarted.media_thumbnail(uri("track", 1)) == cover
    finally:
        await restarted.close()
    again = SyncService(sync.state_dir, sync.music_dir, runner=runner, now=clock.now, clock=clock.time)
    try:
        assert await again.media_thumbnail(uri("track", 1)) == cover and public_get.call_count == 1
        assert rows(again, item_id)[0]["thumbnail_url"] == cover
        assert rows(again, item_id)[0]["thumbnail_checked"] == clock.time()
        assert rows(again, item_id)[0]["status"] == before[0]["status"]
    finally:
        await again.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "spotify:track:short", uri("track", 999), uri("album", 999),
    uri("playlist", 999), "spotify:local:Unknown:Album:Song:1", "spotify:unresolved:entry",
    "https://evil.example/track/" + "1" * 22, "https://open.spotify.com.evil.example/track/" + "1" * 22,
    "https://user@open.spotify.com/track/" + "1" * 22, "https://open.spotify.com:443/track/" + "1" * 22])
async def test_media_thumbnail_rejects_invalid_or_untracked_uri_without_network(service, public_get, value):
    sync, runner, _ = service
    add_item(sync, runner)
    with pytest.raises(ValueError):
        await sync.media_thumbnail(value)
    public_get.assert_not_called()
    assert not runner.calls


@pytest.mark.asyncio
async def test_media_thumbnail_shares_track_item_cache_with_membership_rows(service, public_get):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 1, kind="track")
    playlist = add_item(sync, runner, 100, track(1))
    sync.reconcile(playlist, metadata(track(1)), "seed")
    cover = "https://i.scdn.co/image/track-and-playlist"
    public_get.side_effect = None
    public_get.return_value = oembed_response({"thumbnail_url": cover})
    assert await sync.media_thumbnail(uri("track", 1)) == cover
    assert await sync.thumbnail(item_id) == cover and public_get.call_count == 1
    assert sync.item(item_id)["thumbnail_checked"] == rows(sync, playlist)[0]["thumbnail_checked"] == clock.time()
    assert rows(sync, playlist)[0]["thumbnail_url"] == cover


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["remove", "uri_change", "raw_refresh"])
async def test_media_thumbnail_inflight_track_changes_do_not_update_unrelated_uri(service, public_get, action):
    sync, runner, _ = service
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, metadata(track(1)), "seed")
    entered, release = threading.Event(), threading.Event()

    def fetch(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return oembed_response({})

    public_get.side_effect = fetch
    task = asyncio.create_task(sync.media_thumbnail(uri("track", 1)))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        if action == "remove":
            sync.remove(item_id)
        elif action == "uri_change":
            sync.db.execute("UPDATE tracks SET uri=? WHERE item_id=?", (uri("track", 2), item_id))
        else:
            sync.reconcile(item_id, metadata(track(1, thumbnail_url="https://i.scdn.co/image/raw")), "seed")
    finally:
        release.set()
        result = await asyncio.wait_for(task, 3)
    assert result == ("https://i.scdn.co/image/raw" if action == "raw_refresh" else None)
    if action == "uri_change":
        assert rows(sync, item_id)[0]["thumbnail_url"] is None
        assert rows(sync, item_id)[0]["thumbnail_checked"] == 0


@pytest.mark.asyncio
async def test_all_items_first_pass_before_fixed_60_second_retry_rounds(service):
    sync, runner, clock = service
    add_item(sync, runner, 100, track(1), track(2))
    add_item(sync, runner, 200, track(3), track(4))
    failure = {"status": "error", "message": "Temporary audio-key failure"}
    runner.queue("download", uri("track", 1), failure, failure, failure)
    runner.queue("download", uri("track", 3), failure, failure)
    start = clock.time()

    await run_pass(sync, nightly=True)

    downloads = [c for c in runner.calls if c["command"] == "download"]
    assert [c["uri"] for c in downloads] == [uri("track", n) for n in (1, 2, 3, 4, 1, 3, 1, 3, 1)]
    assert [c["at"] - start for c in downloads] == [0, 0, 0, 0, 60, 60, 120, 120, 180]
    assert clock.sleeps and set(clock.sleeps) == {60}
    assert [item["status"] for item in sync.snapshot()["items"]] == ["synced", "synced"]
    assert sync.setting("job") is None


@pytest.mark.parametrize("value,expected", [(None, 60), (0, 60), (-1, 60), (30, 60),
                                             (125, 125), ("90", 90), ("bad", 60),
                                             (float("nan"), 60), (float("inf"), 60)])
def test_retry_delay_has_floor_and_honors_longer_retry_after(value, expected):
    assert SyncService.delay({"retry_after": value}) == expected


@pytest.mark.asyncio
async def test_timezone_label_uses_loaded_timezone_not_container_symlink(service, monkeypatch):
    sync, _, clock = service
    monkeypatch.delenv("TZ", raising=False)
    clock.value = clock.value.replace(tzinfo=ZoneInfo("America/New_York"))
    assert sync.snapshot()["schedule"]["timezone"] == "America/New_York"
    clock.value = clock.value.replace(tzinfo=timezone(timedelta(hours=-4), "EDT"))
    assert sync.snapshot()["schedule"]["timezone"] == "EDT"


@pytest.mark.asyncio
async def test_retry_after_prevents_early_retry_without_exponential_backoff(service):
    sync, runner, clock = service
    add_item(sync, runner, 100, track(1))
    runner.queue("download", uri("track", 1), {"status": "blocked", "retry_after": 125},
                 {"status": "error"}, {"status": "error"})
    start = clock.time()
    await run_pass(sync, nightly=True)
    assert [c["at"] - start for c in runner.calls if c["command"] == "download"] == [0, 180, 240, 300]
    assert set(clock.sleeps) == {60}


@pytest.mark.asyncio
async def test_initial_pass_finishes_all_items_even_after_8am(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=7, minute=59, second=30)
    add_item(sync, runner, 100, track(1))
    add_item(sync, runner, 200, track(2))
    runner.queue("download", uri("track", 1), {"status": "error"})

    async def slow_first_download(call):
        if call["command"] == "download" and call["uri"] == uri("track", 1):
            clock.advance(90)

    runner.on_call = slow_first_download
    await run_pass(sync, nightly=True)
    downloads = [c for c in runner.calls if c["command"] == "download"]
    assert [c["uri"] for c in downloads] == [uri("track", 1), uri("track", 2)]
    assert datetime.fromtimestamp(downloads[1]["at"], timezone.utc).hour == 8
    assert not clock.sleeps


@pytest.mark.asyncio
async def test_retry_cutoff_stops_new_attempts_at_8am_but_not_inflight_work(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=7, minute=58, second=30)
    item_id = add_item(sync, runner, 100, track(1), track(2))
    for n in (1, 2):
        runner.queue("download", uri("track", n), {"status": "error"}, {"status": "error"})

    async def cross_cutoff(call):
        if call["command"] == "download" and sum(c["command"] == "download" for c in runner.calls) == 3:
            clock.advance(45)

    runner.on_call = cross_cutoff
    await run_pass(sync, nightly=True)
    assert [c["uri"] for c in runner.calls if c["command"] == "download"] == [uri("track", n) for n in (1, 2, 1)]
    assert all(datetime.fromtimestamp(c["at"], timezone.utc).hour < 8 for c in runner.calls)
    assert [r["status"] for r in rows(sync, item_id)] == ["failed", "failed"]
    assert not sync.sync["running"]


@pytest.mark.asyncio
@pytest.mark.parametrize("hour", [0, 2, 8, 12, 23])
async def test_manual_sync_outside_window_is_one_pass(service, hour):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=hour)
    item_id = add_item(sync, runner, 100, track(1))
    runner.queue("download", uri("track", 1), {"status": "error"})
    await run_pass(sync, item_id)
    assert [c["command"] for c in runner.calls] == ["metadata", "download"]
    assert not clock.sleeps
    assert rows(sync, item_id)[0]["status"] == "failed"
    assert sync.setting("job") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("hour,minute,second,expected", [(2, 59, 59, False), (3, 0, 0, True),
                                                         (7, 59, 59, True), (8, 0, 0, False)])
async def test_scheduler_catchup_window_boundaries(service, hour, minute, second, expected):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=hour, minute=minute, second=second)
    add_item(sync, runner)
    assert in_window(clock.now()) is expected
    await sync.tick()
    assert (sync.task is not None) is expected
    if expected:
        await asyncio.wait_for(sync.task, 3)
        assert sync.setting("last_nightly") == clock.now().date().isoformat()
    else:
        assert not runner.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_first", [False, True])
async def test_nightly_runs_once_per_day_and_rechecks_next_day(service, failed_first):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    clock.value = clock.value.replace(hour=7, minute=59, second=30)
    if failed_first:
        runner.queue("download", uri("track", 1), {"status": "error"})
    await sync.tick()
    await asyncio.wait_for(sync.task, 3)
    first_calls = list(runner.calls)
    await sync.tick()
    assert runner.calls == first_calls
    clock.advance(24 * 60 * 60)
    clock.value = clock.value.replace(hour=3, minute=0, second=0)
    await sync.tick()
    await asyncio.wait_for(sync.task, 3)
    assert len([c for c in runner.calls if c["command"] == "metadata"]) == 2
    assert rows(sync, item_id)[0]["status"] == "downloaded"
    assert sync.setting("last_nightly") == clock.now().date().isoformat()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["not_ready", "disconnected", "empty"])
async def test_scheduler_does_not_start_without_prerequisites(service, reason):
    sync, runner, clock = service
    if reason != "empty":
        add_item(sync, runner)
    if reason == "not_ready":
        sync.ready = False
    if reason == "disconnected":
        sync.account["connected"] = False
    await sync.tick()
    assert sync.task is None
    assert not runner.calls


@pytest.mark.asyncio
async def test_running_job_excludes_other_jobs_connect_and_remove(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    entered, release = asyncio.Event(), asyncio.Event()

    async def hold_worker(call):
        entered.set()
        await release.wait()

    runner.on_call = hold_worker
    sync.start_job(nightly=True)
    try:
        await asyncio.wait_for(entered.wait(), 3)
        with pytest.raises(RuntimeError, match="already running"):
            sync.start_job(item_id)
        with pytest.raises(RuntimeError, match="already running"):
            sync.start_conversion()
        with pytest.raises(RuntimeError, match="current job"):
            sync.start_connect()
        with pytest.raises(RuntimeError, match="current job"):
            sync.remove(item_id)
        await sync.tick()
        assert len(runner.calls) == 1
    finally:
        release.set()
        await asyncio.wait_for(sync.task, 3)
    assert not sync.busy


@pytest.mark.asyncio
@pytest.mark.parametrize("from_worker", [False, True])
async def test_unavailable_excluded_from_retries_then_available_next_nightly(service, from_worker):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1, availability="available" if from_worker else "unavailable"), track(2))
    if from_worker:
        runner.queue("download", uri("track", 1), {"status": "unavailable", "message": "Account restriction"})
    runner.queue("download", uri("track", 2), {"status": "error"}, {"status": "error"})
    await run_pass(sync, nightly=True)
    assert rows(sync, item_id)[0]["status"] == "unavailable"
    assert sum(c.get("uri") == uri("track", 1) for c in runner.calls) == int(from_worker)
    runner.metadata[uri("playlist", 100)] = metadata(track(1), track(2))
    clock.advance(24 * 60 * 60)
    await sync.tick()
    await asyncio.wait_for(sync.task, 3)
    assert rows(sync, item_id)[0]["status"] == "downloaded"
    assert sum(c.get("uri") == uri("track", 1) for c in runner.calls) == int(from_worker) + 1


@pytest.mark.asyncio
async def test_partial_membership_keeps_omitted_rows_and_managed_files(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1), track(2))
    await run_pass(sync, item_id)
    before = rows(sync, item_id)
    runner.metadata[uri("playlist", 100)] = metadata(track(1), complete=False)
    await run_pass(sync, item_id)
    assert {r["key"] for r in rows(sync, item_id)} == {r["key"] for r in before}
    assert all(sync.path(r["path"]).is_file() and digest(sync.path(r["path"])) == r["hash"] for r in before)
    assert sync.item(item_id)["complete"] == 0
    assert sync.item(item_id)["status"] == "incomplete"


@pytest.mark.asyncio
@pytest.mark.parametrize("ownership", ["unchanged", "modified", "unhashed", "missing"])
async def test_complete_membership_deletes_only_unchanged_hashed_managed_files(service, ownership):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    path = sync.path(saved["path"])
    external = path.parent / "untracked-original.ogg"
    external.write_bytes(b"untracked original")
    if ownership == "modified":
        path.write_bytes(b"user edit")
    elif ownership == "unhashed":
        sync.db.execute("UPDATE tracks SET hash=NULL WHERE item_id=?", (item_id,))
    elif ownership == "missing":
        path.unlink()
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert rows(sync, item_id) == []
    assert path.exists() is (ownership in {"modified", "unhashed"})
    assert external.read_bytes() == b"untracked original"
    if ownership in {"modified", "unhashed"}:
        assert "file kept" in sync.item(item_id)["message"]
    assert sync.item(item_id)["complete"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["album", "track"])
async def test_nonplaylist_snapshots_never_mirror_delete(service, kind):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1), kind=kind)
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    runner.metadata[uri(kind, 100)] = metadata()
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["path"] == saved["path"]
    assert sync.path(saved["path"]).is_file()


@pytest.mark.asyncio
async def test_local_and_unresolved_members_are_retained_without_worker_download(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    local = track(1, id="spotify:local:Artist:Album:Song:1", uri="spotify:local:Artist:Album:Song:1", availability="local")
    unresolved = track(2, id="spotify:unresolved:entry", uri="spotify:unresolved:entry", availability="unknown")
    item_id = add_item(sync, runner, 100, local, unresolved)
    await run_pass(sync, item_id)
    assert [r["key"] for r in rows(sync, item_id)] == [local["id"], unresolved["id"]]
    assert [r["status"] for r in rows(sync, item_id)] == ["unavailable", "failed"]
    assert [c["command"] for c in runner.calls] == ["metadata"]
    await run_pass(sync, item_id)
    assert len(rows(sync, item_id)) == 2
    assert sync.item(item_id)["complete"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("availability,new_uri", [("local", "spotify:local:Artist:Album:Song:1"),
                                                   ("unknown", "spotify:unresolved:entry"),
                                                   ("unavailable", uri("track", 1))])
async def test_present_local_unresolved_or_unavailable_members_never_delete_existing_file(service, availability, new_uri):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    runner.metadata[uri("playlist", 100)] = metadata(track(1, uri=new_uri, availability=availability))
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["path"] == saved["path"]
    assert digest(sync.path(saved["path"])) == saved["hash"]
    assert len([c for c in runner.calls if c["command"] == "download"]) == 1


@pytest.mark.asyncio
async def test_untracking_keeps_downloaded_and_unmanaged_files(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    external = sync.path(saved["path"]).parent / "original.mp3"
    external.write_bytes(b"user original")
    sync.remove(item_id)
    assert sync.snapshot()["items"] == []
    assert rows(sync, item_id) == []
    assert digest(sync.path(saved["path"])) == saved["hash"]
    assert external.read_bytes() == b"user original"


@pytest.mark.asyncio
async def test_untracked_filename_collision_keeps_original_and_publishes_unique_output(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, runner.metadata[uri("playlist", 100)], "seed-job")
    folder = sync.path(sync.item(item_id)["folder"])
    original = folder / f"Test Artist - Song 1 [{1:022d}].ogg"
    original.write_bytes(b"untracked original")
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    assert original.read_bytes() == b"untracked original"
    assert sync.path(saved["path"]) != original
    assert saved["path"].endswith(" - 1.ogg")
    assert digest(sync.path(saved["path"])) == saved["hash"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "empty"])
async def test_missing_or_empty_downloaded_file_is_repaired(service, damage):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    original = sync.path(rows(sync, item_id)[0]["path"])
    if damage == "missing":
        original.unlink()
    else:
        original.write_bytes(b"")
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    assert saved["status"] == "downloaded"
    assert sync.path(saved["path"]).stat().st_size > 0
    assert digest(sync.path(saved["path"])) == saved["hash"]
    assert len([c for c in runner.calls if c["command"] == "download"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("new_format,suffix", [("mp3", ".mp3"), ("aac", ".m4a")])
async def test_format_change_affects_future_downloads_only(service, new_format, suffix):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    original = rows(sync, item_id)[0]
    sync.set_setting("format", new_format)
    runner.metadata[uri("playlist", 100)] = metadata(track(1), track(2))
    await run_pass(sync, item_id)
    first, second = rows(sync, item_id)
    assert (first["path"], first["hash"], first["format"]) == (original["path"], original["hash"], "ogg")
    assert digest(sync.path(first["path"])) == original["hash"]
    assert second["format"] == new_format and second["path"].endswith(suffix)
    assert [c["format"] for c in runner.calls if c["command"] == "download"] == ["ogg", "ogg"]
    assert [c["format"] for c in runner.calls if c["command"] == "convert"] == [new_format]


@pytest.mark.asyncio
async def test_transient_metadata_failure_is_incomplete_and_keeps_membership(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    original = rows(sync, item_id)[0]
    runner.queue("metadata", uri("playlist", 100), {"status": "error", "message": "Temporary metadata failure"})
    await run_pass(sync, item_id)
    item = sync.item(item_id)
    assert item["complete"] == 0 and item["status"] == "incomplete"
    assert item["next_metadata"] == clock.time() + 60
    assert "Temporary metadata failure" in item["message"]
    assert rows(sync, item_id)[0]["path"] == original["path"]
    assert digest(sync.path(original["path"])) == original["hash"]


@pytest.mark.asyncio
async def test_failed_metadata_retries_and_is_complete_only_after_success(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    runner.queue("metadata", uri("playlist", 100), {"status": "error"}, {"status": "error"})
    start = clock.time()

    async def inspect_failure(call):
        if call["command"] == "metadata" and len(runner.calls) > 1:
            assert sync.item(item_id)["complete"] == 0

    runner.on_call = inspect_failure
    await run_pass(sync, nightly=True)
    assert [c["at"] - start for c in runner.calls if c["command"] == "metadata"] == [0, 60, 120]
    assert sync.item(item_id)["complete"] == 1
    assert rows(sync, item_id)[0]["status"] == "downloaded"


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["metadata", "download"])
async def test_auth_failure_pauses_job_and_scheduler_until_reconnect(service, command):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1), track(2))
    add_item(sync, runner, 200, track(3))
    target = uri("playlist", 100) if command == "metadata" else uri("track", 1)
    runner.queue(command, target, {"status": "auth_required", "message": "Reconnect required"})
    await run_pass(sync, nightly=True)
    assert not sync.account["connected"]
    assert sync.setting("auth_valid") is False
    assert sync.account["message"] == "Reconnect required"
    assert not sync.sync["running"]
    assert sync.setting("job")["phase"] == "first_pass"
    assert not clock.sleeps
    excluded = {uri("track", 2), uri("track", 3)}
    if command == "metadata":
        excluded.add(uri("playlist", 200))
    assert all(c["uri"] not in excluded for c in runner.calls)
    if command == "download":
        assert rows(sync, first)[0]["status"] == "auth_required"
    calls = list(runner.calls)
    await sync.tick()
    assert runner.calls == calls
    paused_id = sync.setting("job")["id"]
    sync.start_connect()
    await asyncio.wait_for(sync.task, 3)
    assert sync.account["connected"] and sync.setting("auth_valid") is True
    await sync.tick()
    assert sync.setting("job")["id"] == paused_id
    await asyncio.wait_for(sync.task, 3)
    assert sync.setting("job") is None
    assert all(item["status"] == "synced" for item in sync.snapshot()["items"])


@pytest.mark.asyncio
async def test_sqlite_restart_resumes_same_date_first_pass_without_repeating_finished_work(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1), track(2))
    sync.set_setting("format", "aac")
    sync.set_setting("username", "fake-user")
    sync.set_setting("auth_valid", True)

    async def interrupt_second_track(call):
        if call["command"] == "download" and call["uri"] == uri("track", 2):
            raise asyncio.CancelledError

    runner.on_call = interrupt_second_track
    sync.start_job(nightly=True)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved_job = sync.setting("job")
    saved_track = rows(sync, item_id)[0]
    assert saved_job["phase"] == "first_pass"
    await sync.close()
    runner.on_call = None
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        assert restarted.setting("job") == saved_job
        assert restarted.setting("username") == "fake-user"
        assert restarted.setting("auth_valid") is True
        assert restarted.setting("format") == "aac"
        assert rows(restarted, item_id)[0]["hash"] == saved_track["hash"]
        start = len(runner.calls)
        await restarted.tick()
        assert restarted.setting("job")["id"] == saved_job["id"]
        await asyncio.wait_for(restarted.task, 3)
        assert [(c["command"], c.get("uri")) for c in runner.calls[start:]] == [
            ("download", uri("track", 2)), ("convert", uri("track", 2))]
        assert all(r["status"] == "downloaded" for r in rows(restarted, item_id))
        assert restarted.setting("job") is None
        assert digest(restarted.path(saved_track["path"])) == saved_track["hash"]
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_sqlite_restart_resumes_retry_phase_and_persisted_due_time(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    job = {"id": "persisted-fake-job", "date": clock.now().date().isoformat(),
           "phase": "retry", "item_ids": [item_id], "retry": True}
    sync.reconcile(item_id, runner.metadata[uri("playlist", 100)], job["id"])
    runner.queue("download", uri("track", 1), {"status": "error"})
    await sync.download(rows(sync, item_id)[0], job["id"])
    due = rows(sync, item_id)[0]["next_attempt"]
    sync.set_setting("job", job)
    sync.set_setting("last_nightly", job["date"])
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        start = len(runner.calls)
        await restarted.tick()
        await asyncio.wait_for(restarted.task, 3)
        assert [c["command"] for c in runner.calls[start:]] == ["download"]
        assert runner.calls[-1]["at"] == due
        assert rows(restarted, item_id)[0]["status"] == "downloaded"
        await restarted.tick()
        assert len(runner.calls) == start + 1
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_old_persisted_job_starts_fresh_nightly_instead_of_resuming(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner)
    old = {"id": "yesterday", "date": (clock.now() - timedelta(days=1)).date().isoformat(),
           "phase": "retry", "item_ids": [item_id], "retry": True}
    sync.set_setting("job", old)
    await sync.tick()
    assert sync.setting("job")["id"] != old["id"]
    assert sync.setting("job")["phase"] == "first_pass"
    await asyncio.wait_for(sync.task, 3)
    assert [c["command"] for c in runner.calls] == ["metadata"]


@pytest.mark.asyncio
@pytest.mark.parametrize("hour", [2, 8, 12])
async def test_persisted_job_is_not_resumed_outside_window(service, hour):
    sync, runner, clock = service
    item_id = add_item(sync, runner)
    clock.value = clock.value.replace(hour=hour)
    job = {"id": "paused-job", "date": clock.now().date().isoformat(),
           "phase": "retry", "item_ids": [item_id], "retry": True}
    sync.set_setting("job", job)
    await sync.tick()
    assert sync.task is None and not runner.calls
    assert sync.setting("job") == job


@pytest.mark.asyncio
@pytest.mark.parametrize("saved_auth", [True, False])
async def test_startup_probes_only_valid_saved_fake_credentials_and_catches_up(service, saved_auth):
    sync, runner, clock = service
    add_item(sync, runner)
    sync.ready = False
    sync.account["connected"] = False
    sync.set_setting("auth_valid", saved_auth)
    (sync.state_dir / "credentials.json").write_text('{"test_only": "not real credentials"}')
    sleeping = asyncio.Event()

    async def hold_scheduler(seconds):
        assert seconds == 10
        sleeping.set()
        await asyncio.Event().wait()

    sync.sleep = hold_scheduler
    sync.start()
    await asyncio.wait_for(sleeping.wait(), 3)
    assert sync.ready
    assert sync.account["connected"] is saved_auth
    if saved_auth:
        await asyncio.wait_for(sync.task, 3)
        assert [c["command"] for c in runner.calls] == ["probe", "metadata"]
    else:
        assert not runner.calls and sync.task is None


@pytest.mark.asyncio
@pytest.mark.parametrize("relative", ["../outside.ogg", "/tmp/outside.ogg", ".", "Songs/../../outside.ogg"])
async def test_managed_path_rejects_escape_and_empty_path(service, relative):
    sync, runner, clock = service
    with pytest.raises(OSError, match="Invalid managed file path"):
        sync.path(relative)


@pytest.mark.asyncio
async def test_symlink_parent_blocks_download_without_touching_outside_files(service, tmp_path):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "original.ogg"
    sentinel.write_bytes(b"outside original")
    (sync.music_dir / "Playlists").symlink_to(outside, target_is_directory=True)
    await run_pass(sync, item_id)
    assert [c["command"] for c in runner.calls] == ["metadata"]
    assert rows(sync, item_id) == []
    assert "paused" in sync.sync["message"]
    assert list(outside.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b"outside original"


@pytest.mark.asyncio
async def test_symlink_file_is_not_followed_or_deleted_on_reconciliation(service, tmp_path):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    path = sync.path(saved["path"])
    outside = tmp_path / "outside.ogg"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(outside)
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert path.is_symlink()
    assert digest(outside) == saved["hash"]
    assert "symlink" in sync.item(item_id)["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("output_kind", ["missing", "empty", "symlink"])
async def test_worker_ok_without_safe_nonempty_output_is_never_recorded_downloaded(service, tmp_path, output_kind):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    runner.output_kind = output_kind
    runner.outside_file = tmp_path / "outside.ogg"
    runner.outside_file.write_bytes(b"outside original")
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    assert saved["status"] != "downloaded" and saved["path"] is None and saved["hash"] is None
    assert "paused" in sync.sync["message"]
    assert runner.outside_file.read_bytes() == b"outside original"
    assert not visible_oggs(sync)


@pytest.mark.asyncio
@pytest.mark.parametrize("filename", ["../escape.ogg", "/tmp/escape.ogg", "nested/output.ogg", None])
async def test_worker_output_filename_cannot_escape_staging(service, filename):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    runner.queue("download", uri("track", 1), {"status": "ok", "filename": filename})
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["status"] != "downloaded"
    assert "paused" in sync.sync["message"]
    assert not list(sync.music_dir.rglob("*.ogg"))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["track", "album", "playlist"])
async def test_spotify_urls_normalize_and_duplicate_urls_or_uris_are_rejected(service, kind):
    sync, runner, clock = service
    ident = f"{123:022d}"
    added = sync.add(f" https://open.spotify.com/intl-en/{kind}/{ident}?si=fake ")
    assert added["uri"] == uri(kind, 123) and added["kind"] == kind
    for duplicate in (uri(kind, 123), f"https://open.spotify.com/{kind}/{ident}"):
        with pytest.raises(ValueError, match="already tracked"):
            sync.add(duplicate)
    assert len(sync.snapshot()["items"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "spotify:episode:" + "1" * 22,
                                    "spotify:artist:" + "1" * 22, "spotify:track:short",
                                    uri("track", 1) + ":extra", "http://open.spotify.com/track/" + "1" * 22,
                                    "https://evil.example/track/" + "1" * 22,
                                    "https://open.spotify.com.evil.example/track/" + "1" * 22,
                                    "https://open.spotify.com@evil.example/track/" + "1" * 22,
                                    "https://open.spotify.com/track/" + "1" * 22 + "#fragment"])
async def test_invalid_spotify_input_never_adds_items(service, value):
    sync, runner, clock = service
    with pytest.raises(ValueError):
        sync.add(value)
    assert sync.snapshot()["items"] == []
    assert not runner.calls


@pytest.mark.asyncio
async def test_duplicate_playlist_members_download_once(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1), track(1), track(2))
    await run_pass(sync, item_id)
    assert [r["position"] for r in rows(sync, item_id)] == [0, 2]
    assert [c["uri"] for c in runner.calls if c["command"] == "download"] == [uri("track", 1), uri("track", 2)]


@pytest.mark.asyncio
async def test_disconnect_reserves_slot_through_cleanup_and_coalesces_callers(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    entered, stopping, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def hold(call):
        entered.set()
        await asyncio.Event().wait()

    async def stop():
        runner.stop_count += 1
        stopping.set()
        await release.wait()

    runner.on_call = hold
    runner.stop = stop
    sync.start_job(item_id)
    await entered.wait()
    first = asyncio.create_task(sync.disconnect())
    await stopping.wait()
    second = asyncio.create_task(sync.disconnect())
    await asyncio.sleep(0)
    assert sync.task.done() and sync.busy
    with pytest.raises(RuntimeError):
        sync.start_connect()
    with pytest.raises(RuntimeError):
        sync.start_job(item_id)
    await sync.tick()
    first.cancel()
    first.cancel()
    assert runner.stop_count == 1
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert not sync.busy and not sync.account["connected"]
    runner.on_call = None
    sync.start_connect()
    await sync.task
    assert sync.account["connected"]


@pytest.mark.asyncio
@pytest.mark.parametrize("suppress_cancel", [False, True])
async def test_disconnect_excludes_startup_probe_and_prevents_auth_resurrection(service, suppress_cancel):
    sync, runner, clock = service
    sync.ready = False
    sync.account["connected"] = False
    (sync.state_dir / "credentials.json").write_text("{}")
    entered, sleeping = asyncio.Event(), asyncio.Event()

    async def probe(call):
        assert call["command"] == "probe"
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not suppress_cancel:
                raise

    async def scheduler_sleep(seconds):
        sleeping.set()
        await asyncio.Event().wait()

    runner.on_call = probe
    sync.sleep = scheduler_sleep
    sync.start()
    await entered.wait()
    with pytest.raises(RuntimeError):
        sync.start_connect()
    await sync.disconnect()
    await sleeping.wait()
    assert sync.ready and not sync.account["connected"]
    assert sync.setting("auth_valid") is False
    assert not (sync.state_dir / "credentials.json").exists()
    assert runner.stop_count == 1


@pytest.mark.asyncio
async def test_worker_repeated_cancel_kills_sigterm_ignoring_process_and_releases_slot(tmp_path, monkeypatch):
    runner = WorkerRunner(tmp_path)
    original_spawn = asyncio.create_subprocess_exec
    ready = asyncio.Event()
    processes = []

    async def spawn(*args, **kwargs):
        # Only isolated local stubs are spawned, never spotisync.worker.
        script = ("import json,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                  "print(json.dumps({'event':'ready'}),flush=True); time.sleep(60)") if not processes else (
                  "print('{\"event\":\"result\",\"status\":\"ok\"}',flush=True)")
        process = await original_spawn(sys.executable, "-c", script, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(runner.run("probe", on_event=lambda event: ready.set()))
    try:
        await asyncio.wait_for(ready.wait(), 3)
        task.cancel()
        while runner._stopping is None:
            await asyncio.sleep(0)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(RuntimeError, match="already running"):
            await runner.run("probe")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 8)
        assert processes[0].returncode == -signal.SIGKILL
        assert runner.process is None and not runner._running
        assert await runner.run("probe") == {"event": "result", "status": "ok"}
    finally:
        for process in processes:
            if process.returncode is None:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()


@pytest.mark.asyncio
async def test_worker_cancel_during_spawn_keeps_slot_and_cleans_up_child(tmp_path, monkeypatch):
    runner = WorkerRunner(tmp_path)
    original_spawn = asyncio.create_subprocess_exec
    entered, release = asyncio.Event(), asyncio.Event()
    processes = []

    async def spawn(*args, **kwargs):
        entered.set()
        await release.wait()
        process = await original_spawn(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(runner.run("probe"))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(RuntimeError):
        await runner.run("probe")
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
    assert processes[0].returncode is not None
    assert runner.process is None and not runner._running


@pytest.mark.asyncio
async def test_worker_conflict_does_not_repeat_persisted_job_every_scheduler_tick(service):
    sync, runner, clock = service
    add_item(sync, runner, 100, track(1))

    async def conflict(call):
        raise RuntimeError("Another Spotify worker is already running.")

    runner.on_call = conflict
    await sync.tick()
    await sync.task
    await sync.tick()
    assert len(runner.calls) == 1
    assert sync.setting("job") is None
    assert not sync.busy


@pytest.mark.asyncio
async def test_manual_within_window_runs_due_full_nightly_before_single_item_retries(service):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    runner.queue("download", uri("track", 1), *[{"status": "error"}] * 400)
    await run_pass(sync, first)
    downloads = [call["uri"] for call in runner.calls if call["command"] == "download"]
    assert downloads[:3] == [uri("track", 1), uri("track", 2), uri("track", 1)]
    assert rows(sync, second)[0]["status"] == "downloaded"
    assert clock.now().hour == 8
    assert sync.setting("last_nightly") == clock.now().date().isoformat()
    before = len(runner.calls)
    await sync.tick()
    assert len(runner.calls) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["metadata", "download"])
async def test_real_429_gates_rest_of_full_first_pass(service, command):
    sync, runner, clock = service
    add_item(sync, runner, 100, track(1))
    add_item(sync, runner, 200, track(2))
    target = uri("playlist", 100) if command == "metadata" else uri("track", 1)
    runner.queue(command, target, {"status": "blocked", "message": "Spotify rate limit reached.", "retry_after": 125})
    start = clock.time()
    await run_pass(sync, nightly=True)
    limited = next(i for i, call in enumerate(runner.calls) if call["command"] == command)
    assert all(call["at"] >= start + 125 for call in runner.calls[limited + 1:])
    assert sync.setting("server_cooldown") == start + 125
    assert all(item["status"] == "synced" for item in sync.snapshot()["items"])


@pytest.mark.asyncio
async def test_first_pass_waits_out_429_past_8am_without_abandoning_other_media(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=7, minute=59, second=30)
    add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    runner.queue("metadata", uri("playlist", 100),
                 {"status": "blocked", "message": "Spotify rate limit reached.", "retry_after": 180})
    start = clock.time()
    await run_pass(sync, nightly=True)
    assert [(call["command"], call["uri"]) for call in runner.calls] == [
        ("metadata", uri("playlist", 100)), ("metadata", uri("playlist", 200)), ("download", uri("track", 2))]
    assert runner.calls[1]["at"] == start + 180
    assert rows(sync, second)[0]["status"] == "downloaded"


@pytest.mark.asyncio
async def test_retry_rechecks_8am_cutoff_after_global_cooldown_wait(service):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, runner.metadata[uri("playlist", 100)], "retry-job")
    clock.value = clock.value.replace(hour=7, minute=59, second=30)
    sync.set_setting("server_cooldown", clock.time() + 125)
    job = {"id": "retry-job", "date": clock.now().date().isoformat(),
           "phase": "retry", "item_ids": [item_id], "retry": True}
    sync.set_setting("last_nightly", job["date"])
    await run_pass(sync, existing=job)
    assert not runner.calls
    assert rows(sync, item_id)[0]["attempt_job"] is None
    assert sync.setting("job") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [{"status": "error", "retry_after": 125},
                                    {"status": "blocked", "message": "Spotify denied this request; retry later.", "retry_after": 125}])
async def test_generic_failure_or_403_does_not_globally_throttle_first_pass(service, failure):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    add_item(sync, runner, 100, track(1))
    add_item(sync, runner, 200, track(2))
    runner.queue("metadata", uri("playlist", 100), failure)
    await run_pass(sync)
    assert sync.setting("server_cooldown") is None
    assert len({call["at"] for call in runner.calls}) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["auth", "probe", "metadata"])
async def test_server_cooldown_persists_restart_and_manual_auth_probe_cannot_bypass(service, command):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    runner.queue("metadata", uri("playlist", 100),
                 {"status": "blocked", "message": "Spotify rate limit reached.", "retry_after": 125})
    await run_pass(sync, item_id)
    due = sync.setting("server_cooldown")
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        if command == "metadata":
            restarted.ready = True
            restarted.account["connected"] = True
            await run_pass(restarted, item_id)
        else:
            await restarted.worker_call(command)
        assert runner.calls[1]["at"] == due
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_429_without_retry_after_still_sets_60_second_global_floor(service):
    sync, runner, clock = service
    runner.queue("probe", None, {"status": "blocked", "message": "Spotify rate limit reached."})
    start = clock.time()
    await sync.worker_call("probe")
    await sync.worker_call("probe")
    assert runner.calls[1]["at"] == start + 60


@pytest.mark.asyncio
async def test_deletion_captures_before_hash_and_keeps_replacement_at_original_name(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    original = sync.path(saved["path"])
    real_digest = core.digest

    def replace_after_hash(path):
        value = real_digest(path)
        if os.readlink(path).endswith("/captured"):
            original.write_bytes(b"external replacement")
        return value

    monkeypatch.setattr(core, "digest", replace_after_hash)
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert original.read_bytes() == b"external replacement"
    assert rows(sync, item_id) == []
    assert not list(original.parent.glob(".spotisync-quarantine-*"))


@pytest.mark.asyncio
async def test_deletion_mismatch_with_occupied_original_keeps_identifiable_quarantine(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    original = sync.path(saved["path"])
    original.write_bytes(b"user edit")
    real_digest = core.digest

    def occupy_original(path):
        value = real_digest(path)
        if os.readlink(path).endswith("/captured"):
            original.write_bytes(b"new external original")
        return value

    monkeypatch.setattr(core, "digest", occupy_original)
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    quarantine, = original.parent.glob(".spotisync-quarantine-*")
    assert (quarantine / "captured").read_bytes() == b"user edit"
    assert json.loads((quarantine / "intent.json").read_text())["original"] == saved["path"]
    assert original.read_bytes() == b"new external original"
    assert str(quarantine.name) in sync.item(item_id)["message"]


@pytest.mark.asyncio
async def test_crash_after_capture_leaves_recoverable_file_and_manifest(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    original = sync.path(saved["path"])

    def crash(path):
        raise asyncio.CancelledError

    monkeypatch.setattr(core, "digest", crash)
    with pytest.raises(asyncio.CancelledError):
        sync._remove_managed_file(saved)
    quarantine, = original.parent.glob(".spotisync-quarantine-*")
    assert not original.exists()
    assert digest(quarantine / "captured") == saved["hash"]
    assert json.loads((quarantine / "intent.json").read_text()) == {"original": saved["path"], "hash": saved["hash"]}
    assert quarantine.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
async def test_deletion_directory_swap_never_follows_new_symlink(service, tmp_path, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    original = sync.path(saved["path"])
    folder = original.parent
    moved = folder.with_name("detached")
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / original.name
    sentinel.write_bytes(b"outside original")
    real_digest = core.digest

    def swap_directory(path):
        value = real_digest(path)
        if os.readlink(path).endswith("/captured"):
            folder.rename(moved)
            folder.symlink_to(outside, target_is_directory=True)
        return value

    monkeypatch.setattr(core, "digest", swap_directory)
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert sentinel.read_bytes() == b"outside original"
    assert not (moved / original.name).exists()


@pytest.mark.asyncio
async def test_download_directory_swap_uses_anchored_staging_and_rolls_back_publication(service, tmp_path):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, runner.metadata[uri("playlist", 100)], "seed")
    folder = sync.path(sync.item(item_id)["folder"])
    moved = folder.with_name("detached")
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "original.ogg"
    sentinel.write_bytes(b"outside original")

    async def swap(call):
        if call["command"] == "download":
            folder.rename(moved)
            folder.symlink_to(outside, target_is_directory=True)

    runner.on_call = swap
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["status"] != "downloaded"
    assert list(outside.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b"outside original"
    assert not list(moved.rglob("*.ogg"))
    assert not list(sync.db.execute("SELECT * FROM publications"))


@pytest.mark.asyncio
async def test_database_publication_failure_rolls_back_own_file_without_orphan_duplicates(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.db.execute("""CREATE TEMP TRIGGER reject_commit BEFORE UPDATE OF path ON tracks
        WHEN NEW.status = 'downloaded' BEGIN SELECT RAISE(FAIL,'simulated database failure'); END""")
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["path"] is None
    assert not visible_oggs(sync)
    assert len(list(sync.db.execute("SELECT * FROM sources"))) == 1
    assert not list(sync.db.execute("SELECT * FROM publications"))
    sync.db.execute("DROP TRIGGER reject_commit")
    await run_pass(sync, item_id)
    assert len(visible_oggs(sync)) == 1
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert not visible_oggs(sync)


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_at", ["before_link", "after_link", "after_commit"])
async def test_publication_journal_recovers_crashes_without_duplicate_downloads(service, monkeypatch, crash_at):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    original_finish = sync.finish_publication
    original_link = os.link

    def crash(operation):
        if operation.get("kind") == "source":
            return original_finish(operation)
        if crash_at == "after_commit":
            original_finish(operation)
        raise asyncio.CancelledError

    if crash_at == "before_link":
        def link_crash(*args, **kwargs):
            if str(args[1]) == f"{1:022d}.ogg":
                return original_link(*args, **kwargs)
            raise asyncio.CancelledError
        monkeypatch.setattr(core.os, "link", link_crash)
    else:
        monkeypatch.setattr(sync, "finish_publication", crash)
    sync.start_job(nightly=True)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved_job = sync.setting("job")
    await sync.close()
    monkeypatch.setattr(core.os, "link", original_link)
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        assert restarted.setting("job") == saved_job
        if crash_at != "before_link":
            assert rows(restarted, item_id)[0]["status"] == "downloaded"
        await restarted.tick()
        await restarted.task
        assert rows(restarted, item_id)[0]["status"] == "downloaded"
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        assert len(visible_oggs(restarted)) == 1
        downloads = [call for call in runner.calls if call["command"] == "download"]
        assert len(downloads) == 1
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_publication_recovery_does_not_adopt_external_collision_with_identical_hash(service, monkeypatch):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    original_path = None
    original_link = os.link

    def collide(source, destination, *, src_dir_fd, dst_dir_fd, **kwargs):
        nonlocal original_path
        if destination == f"{1:022d}.ogg":
            return original_link(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd, **kwargs)
        with os.fdopen(os.open(source, os.O_RDONLY, dir_fd=src_dir_fd), "rb") as stream:
            data = stream.read()
        with os.fdopen(os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=dst_dir_fd), "wb") as stream:
            stream.write(data)
        original_path = sync.path(str(Path(sync.item(item_id)["folder"]) / destination))
        raise asyncio.CancelledError

    monkeypatch.setattr(core.os, "link", collide)
    sync.start_job(nightly=True)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    original_hash = digest(original_path)
    await sync.close()
    monkeypatch.setattr(core.os, "link", original_link)
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        assert rows(restarted, item_id)[0]["status"] == "pending"
        restarted.ready = True
        restarted.account["connected"] = True
        await restarted.tick()
        await restarted.task
        assert restarted.path(rows(restarted, item_id)[0]["path"]) != original_path
        clock.value = clock.value.replace(hour=12)
        runner.metadata[uri("playlist", 100)] = metadata()
        await run_pass(restarted, item_id)
        assert digest(original_path) == original_hash
        assert visible_oggs(restarted) == [original_path]
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_queue", [False, True])
async def test_removed_paused_item_does_not_discard_remaining_persisted_queue(service, legacy_queue):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    job = {"id": "paused", "date": clock.now().date().isoformat(),
           "phase": "first_pass", "item_ids": [first, second], "retry": True}
    sync.set_setting("job", job)
    sync.set_setting("last_nightly", job["date"])
    if legacy_queue:
        sync.db.execute("DELETE FROM items WHERE id=?", (first,))
    else:
        sync.remove(first)
        assert sync.setting("job")["item_ids"] == [second]
    await sync.tick()
    await sync.task
    assert rows(sync, second)[0]["status"] == "downloaded"
    assert sync.setting("job") is None
    assert all(call.get("uri") != uri("playlist", 100) for call in runner.calls)


@pytest.mark.asyncio
async def test_concurrent_publish_collision_advances_suffix_and_finishes_remaining_media(service, monkeypatch):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    original_link = os.link
    external_path = None

    def collide_once(source, destination, **kwargs):
        nonlocal external_path
        if destination in {f"{1:022d}.ogg", f"{2:022d}.ogg"}:
            return original_link(source, destination, **kwargs)
        if external_path is None:
            with os.fdopen(os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600,
                                   dir_fd=kwargs["dst_dir_fd"]), "wb") as stream:
                stream.write(b"concurrent external original")
            external_path = sync.path(str(Path(sync.item(first)["folder"]) / destination))
        return original_link(source, destination, **kwargs)

    monkeypatch.setattr(core.os, "link", collide_once)
    await run_pass(sync, nightly=True)
    assert external_path.read_bytes() == b"concurrent external original"
    assert rows(sync, first)[0]["path"].endswith(" - 1.ogg")
    assert rows(sync, second)[0]["status"] == "downloaded"
    assert not list(sync.db.execute("SELECT * FROM publications"))


@pytest.mark.asyncio
@pytest.mark.parametrize("format", ["ogg", "mp3", "aac"])
async def test_unicode_names_fit_full_utf8_component_budget_including_collision_suffix(service, format):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", format)
    song = track(1, name="\u591c" * 90, artist="\U0001f3b5" * 100)
    item_id = add_item(sync, runner, 100, song)
    second = add_item(sync, runner, 200, track(2))
    runner.metadata[uri("playlist", 100)]["name"] = "\u591c" * 90
    await run_pass(sync)
    saved = rows(sync, item_id)[0]
    original = sync.path(saved["path"])
    assert saved["status"] == "downloaded" and rows(sync, second)[0]["status"] == "downloaded"
    assert all(len(bit.encode("utf-8")) <= 255 for bit in Path(saved["path"]).parts)
    assert "\u591c" in sync.item(item_id)["folder"]
    assert "\U0001f3b5" in original.name
    original.write_bytes(b"")
    await run_pass(sync, item_id)
    repaired = rows(sync, item_id)[0]
    assert " - 1." in repaired["path"]
    assert len(Path(repaired["path"]).name.encode("utf-8")) <= 255


@pytest.mark.asyncio
@pytest.mark.parametrize("date,expected_hour", [(datetime(2026, 3, 8, 1), 7), (datetime(2026, 11, 1, 1), 8)])
async def test_next_nightly_timestamp_uses_zoneinfo_dst_rules(service, date, expected_hour):
    sync, runner, clock = service
    clock.value = date.replace(tzinfo=ZoneInfo("America/New_York"))
    next_run = datetime.fromisoformat(sync.snapshot()["schedule"]["next_run"])
    assert next_run.astimezone(timezone.utc).hour == expected_hour


@pytest.mark.asyncio
async def test_scheduler_promotes_paused_pre3am_manual_queue_to_due_full_nightly(service):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    sync.set_setting("job", {"id": "pre3am-manual", "date": clock.now().date().isoformat(),
                             "phase": "first_pass", "item_ids": [first], "retry": False})
    await sync.tick()
    assert sync.setting("job")["item_ids"] == [first, second]
    await sync.task
    assert rows(sync, second)[0]["status"] == "downloaded"
    assert sync.setting("last_nightly") == clock.now().date().isoformat()


@pytest.mark.asyncio
async def test_publication_rollback_never_deletes_external_replacement(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    original_finish = sync.finish_publication
    external = None

    def fail_after_replacement(operation):
        nonlocal external
        if operation.get("kind") == "source":
            return original_finish(operation)
        external = sync.path(operation["path"])
        external.unlink()
        external.write_bytes(b"external replacement during database failure")
        raise sqlite3.OperationalError("simulated database failure")

    monkeypatch.setattr(sync, "finish_publication", fail_after_replacement)
    await run_pass(sync, item_id)
    assert external.read_bytes() == b"external replacement during database failure"
    monkeypatch.setattr(sync, "finish_publication", original_finish)
    await run_pass(sync, item_id)
    assert sync.path(rows(sync, item_id)[0]["path"]) != external
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert visible_oggs(sync) == [external]


@pytest.mark.asyncio
async def test_quarantine_capture_refuses_to_overwrite_unknown_file(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    original = sync.path(saved["path"])
    real_rename = core.rename_noreplace

    def collision(source, destination, source_fd, destination_fd):
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=destination_fd)
        with os.fdopen(fd, "wb") as stream:
            stream.write(b"unknown quarantined original")
        real_rename(source, destination, source_fd, destination_fd)

    monkeypatch.setattr(core, "rename_noreplace", collision)
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert digest(original) == saved["hash"]
    quarantine, = original.parent.glob(".spotisync-quarantine-*")
    assert (quarantine / "captured").read_bytes() == b"unknown quarantined original"


@pytest.mark.asyncio
async def test_worker_cleanup_kills_sigterm_ignoring_descendant_after_leader_exits(tmp_path, monkeypatch):
    runner = WorkerRunner(tmp_path)
    original_spawn = asyncio.create_subprocess_exec
    ready = asyncio.Event()
    descendants = []
    processes = []

    async def spawn(*args, **kwargs):
        child = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"
        parent = ("import json,subprocess,sys,time; "
                  f"child=subprocess.Popen([sys.executable,'-c',{child!r}],stdout=subprocess.PIPE); "
                  "child.stdout.readline(); print(json.dumps({'event':'ready','pid':child.pid}),flush=True); time.sleep(60)")
        process = await original_spawn(sys.executable, "-c", parent, **kwargs)
        processes.append(process)
        return process

    def event(value):
        descendants.append(value["pid"])
        ready.set()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(runner.run("probe", on_event=event))
    try:
        await asyncio.wait_for(ready.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 8)
        assert runner.process is None and not runner._running
        for _ in range(100):
            try:
                state = Path(f"/proc/{descendants[0]}/stat").read_text().split(") ", 1)[1].split()[0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("SIGTERM-ignoring descendant survived worker cleanup")
    finally:
        for process in processes:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await process.wait()


@pytest.mark.asyncio
@pytest.mark.parametrize("removed", [False, True])
async def test_same_process_publication_recovery_precedes_metadata_and_does_not_duplicate(service, monkeypatch, removed):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    original_finish, original_remove = sync.finish_publication, sync._remove_managed_file

    def fail_commit(operation):
        if operation.get("kind") == "source":
            return original_finish(operation)
        raise sqlite3.OperationalError("simulated database failure")

    def fail_rollback(*args, **kwargs):
        raise OSError("simulated rollback failure")

    monkeypatch.setattr(sync, "finish_publication", fail_commit)
    monkeypatch.setattr(sync, "_remove_managed_file", fail_rollback)
    await run_pass(sync, item_id)
    assert len(visible_oggs(sync)) == 1
    assert rows(sync, item_id)[0]["path"] is None
    assert len(list(sync.db.execute("SELECT * FROM publications"))) == 1
    monkeypatch.setattr(sync, "finish_publication", original_finish)
    monkeypatch.setattr(sync, "_remove_managed_file", original_remove)
    if removed:
        runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, item_id)
    assert len(visible_oggs(sync)) == (0 if removed else 1)
    assert sum(call["command"] == "download" for call in runner.calls) == 1
    assert not list(sync.db.execute("SELECT * FROM publications"))


def source_row(service, number=1):
    return dict(service.db.execute("SELECT * FROM sources WHERE uri=?", (uri("track", number),)).fetchone())


def seed_legacy_downloads(sync, runner, fmt="aac", *numbers):
    item_id = add_item(sync, runner, 100, *(track(number) for number in numbers))
    sync.reconcile(item_id, runner.metadata[uri("playlist", 100)], "legacy")
    suffix = {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[fmt]
    for row in rows(sync, item_id):
        relative = str(Path(sync.item(item_id)["folder"]) / (row["key"] + suffix))
        sync.path(relative).write_bytes(b"verified legacy " + fmt.encode() + b":" + row["uri"].encode())
        sync.db.execute("""UPDATE tracks SET status='downloaded',path=?,hash=?,format=?
            WHERE item_id=? AND key=?""", (relative, digest(sync.path(relative)), fmt, item_id, row["key"]))
    return item_id


async def convert_pass(sync):
    sync.start_conversion()
    await asyncio.wait_for(sync.task, 3)


@pytest.mark.asyncio
async def test_native_originals_deduplicate_globally_and_survive_membership_and_untracking(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(1))
    sync.set_setting("format", "aac")
    await run_pass(sync)
    native = source_row(sync)
    original = sync.path(native["path"])
    assert native["path"] == f".spotisync-originals/{1:022d}.ogg"
    assert [c["format"] for c in runner.calls if c["command"] == "download"] == ["ogg"]
    assert len(list(sync.db.execute("SELECT * FROM sources"))) == 1
    assert all(rows(sync, ident)[0]["format"] == "aac" for ident in (first, second))
    assert native["path"] not in json.dumps(sync.snapshot())
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, first)
    sync.remove(first)
    sync.remove(second)
    assert digest(original) == native["hash"]
    third = add_item(sync, runner, 300, track(1))
    await run_pass(sync, third)
    assert rows(sync, third)[0]["format"] == "aac"
    assert len([c for c in runner.calls if c["command"] == "download"]) == 1


@pytest.mark.asyncio
async def test_ogg_playback_never_shares_original_inode_and_restore_preserves_native_bytes(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    native = source_row(sync)
    original, playback = sync.path(native["path"]), sync.path(saved["path"])
    data = original.read_bytes()
    assert playback.read_bytes() == data
    assert playback.stat().st_ino != original.stat().st_ino
    assert original.stat().st_mode & 0o222 == 0
    playback.write_bytes(b"external edit")
    assert original.read_bytes() == data
    playback.unlink()
    await run_pass(sync, item_id)
    restored = sync.path(rows(sync, item_id)[0]["path"])
    assert restored.read_bytes() == data and restored.stat().st_ino != original.stat().st_ino
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    assert rows(sync, item_id)[0]["format"] == "mp3"
    assert not restored.exists()
    sync.account["connected"] = False
    sync.set_setting("format", "ogg")
    await convert_pass(sync)
    restored = sync.path(rows(sync, item_id)[0]["path"])
    assert restored.read_bytes() == data and restored.stat().st_ino != original.stat().st_ino
    assert len([c for c in runner.calls if c["command"] == "download"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", ["ogg", "mp3", "aac"])
async def test_missing_output_restores_selected_future_format_from_original(service, fmt):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    sync.path(rows(sync, item_id)[0]["path"]).unlink()
    sync.set_setting("format", fmt)
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["format"] == fmt
    assert len([c for c in runner.calls if c["command"] == "download"]) == 1


@pytest.mark.asyncio
async def test_legacy_ogg_adoption_is_verified_independent_copy_without_spotify(service):
    sync, runner, _ = service
    item_id = seed_legacy_downloads(sync, runner, "ogg", 1)
    saved = rows(sync, item_id)[0]
    old = sync.path(saved["path"])
    data, inode = old.read_bytes(), old.stat().st_ino
    sync.account["connected"] = False
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    native = sync.path(source_row(sync)["path"])
    assert native.read_bytes() == data and native.stat().st_ino != inode
    assert not old.exists()
    assert [(c["command"], c["format"]) for c in runner.calls] == [("convert", "ogg"), ("convert", "mp3")]
    assert sync.conversion["completed"] == 1 and sync.conversion["failed"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", ["aac", "mp3"])
@pytest.mark.parametrize("explicit", [False, True])
async def test_legacy_lossy_backfill_downloads_native_once_and_keeps_format_until_explicit(service, fmt, explicit):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = seed_legacy_downloads(sync, runner, fmt, 1, 2)
    before = rows(sync, item_id)
    sync.set_setting("format", "ogg")
    if explicit:
        await convert_pass(sync)
    else:
        await run_pass(sync, item_id)
    assert [c["format"] for c in runner.calls if c["command"] == "download"] == ([] if explicit else ["ogg", "ogg"])
    assert len(list(sync.db.execute("SELECT * FROM sources"))) == (0 if explicit else 2)
    for old, current in zip(before, rows(sync, item_id)):
        assert current["status"] == "downloaded"
        if explicit:
            assert current == old
            assert sync.conversion["skipped"] == 2 and sync.conversion["failed"] == 0
        else:
            assert current["format"] == fmt and current["source_hash"] == source_row(sync, int(current["key"]))["hash"]
            assert not sync.path(old["path"]).exists()
    await run_pass(sync, item_id)
    assert len([c for c in runner.calls if c["command"] == "download"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "unavailable", "blocked"])
@pytest.mark.parametrize("explicit", [False, True])
async def test_backfill_failure_keeps_downloaded_playback_and_continues_other_tracks(service, failure, explicit):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = seed_legacy_downloads(sync, runner, "aac", 1, 2)
    old = rows(sync, item_id)[0]
    runner.queue("download", uri("track", 1), {"status": failure, "message": "Native backfill failed"})
    sync.set_setting("format", "ogg")
    if explicit:
        await convert_pass(sync)
        assert sync.conversion["completed"] == 0 and sync.conversion["failed"] == 0
        assert sync.conversion["skipped"] == 2 and sync.conversion["errors"] == []
        assert not runner.calls
        assert sync.setting("conversion_job") is None
    else:
        await run_pass(sync, item_id)
        assert rows(sync, item_id)[0]["source_message"] == "Native backfill failed"
        assert sync.snapshot()["items"][0]["failed_count"] == 0
    current = rows(sync, item_id)[0]
    assert (current["status"], current["path"], current["hash"], current["format"]) == (
        "downloaded", old["path"], old["hash"], "aac")
    assert digest(sync.path(old["path"])) == old["hash"]
    if not explicit:
        assert source_row(sync, 2)["format"] == "ogg"
    assert not clock.sleeps


@pytest.mark.asyncio
async def test_missing_original_offline_reports_clear_error_without_losing_playback(service):
    sync, runner, _ = service
    item_id = seed_legacy_downloads(sync, runner, "aac", 1)
    old = rows(sync, item_id)[0]
    sync.account["connected"] = False
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    assert not runner.calls
    assert sync.conversion["failed"] == 0 and sync.conversion["completed"] == 0
    assert sync.conversion["skipped"] == 1
    assert "Sync required" in sync.conversion["skips"][0]["message"]
    assert sync.setting("conversion_job") is None
    assert rows(sync, item_id)[0] == old
    assert rows(sync, item_id)[0]["status"] == "downloaded"
    assert digest(sync.path(old["path"])) == old["hash"]


@pytest.mark.asyncio
async def test_offline_conversion_snapshots_format_and_shares_busy_gate_without_cooldown(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1), track(2))
    await run_pass(sync, item_id)
    sync.account["connected"] = False
    sync.set_setting("server_cooldown", clock.time() + 3600)
    sync.set_setting("format", "aac")
    entered, release = asyncio.Event(), asyncio.Event()

    async def hold(call):
        assert call["command"] == "convert"
        assert Path(call["source_path"]).is_absolute()
        assert not Path(call["source_path"]).is_symlink()
        assert Path(call["source_path"]).suffix == ".ogg"
        assert not list(Path(call["output_dir"]).iterdir())
        entered.set()
        await release.wait()

    runner.on_call = hold
    start = len(runner.calls)
    sync.start_conversion()
    await entered.wait()
    snapshot = sync.snapshot()
    assert snapshot["sync"]["running"] and snapshot["sync"]["phase"] == "conversion"
    assert snapshot["conversion"] == {"running": True, "total": 2, "completed": 0, "failed": 0,
        "skipped": 0, "skips": [], "format": "aac", "quality": "auto", "current_track": "Song 1", "message": None, "errors": []}
    for operation in (sync.start_conversion, sync.start_job, sync.start_connect, lambda: sync.remove(item_id)):
        with pytest.raises(RuntimeError):
            operation()
    sync.set_setting("format", "mp3")
    await sync.tick()
    release.set()
    await sync.task
    assert [c["format"] for c in runner.calls[start:]] == ["aac", "aac"]
    assert all(row["format"] == "aac" for row in rows(sync, item_id))
    assert sync.conversion["completed"] == 2 and not sync.conversion["running"]
    assert not clock.sleeps and not sync.busy


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["modified", "symlink", "missing", "record_hash", "record_path", "directory_symlink"])
async def test_corrupt_original_blocks_conversion_without_lossy_fallback_or_download(service, tmp_path, damage):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    saved = rows(sync, item_id)[0]
    native = sync.path(source_row(sync)["path"])
    if damage == "modified":
        native.chmod(0o644)
        native.write_bytes(b"corrupt original")
    elif damage == "symlink":
        native.unlink()
        native.symlink_to(sync.path(saved["path"]))
    elif damage == "missing":
        native.unlink()
    elif damage == "record_hash":
        sync.db.execute("UPDATE sources SET hash='incorrect'")
    elif damage == "record_path":
        sync.db.execute("UPDATE sources SET path=?", (saved["path"],))
    else:
        native.parent.rename(tmp_path / "moved-originals")
        native.parent.symlink_to(tmp_path / "moved-originals", target_is_directory=True)
    before = len(runner.calls)
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    assert len(runner.calls) == before
    assert sync.conversion["failed"] == 1
    assert rows(sync, item_id)[0]["format"] == "aac"
    assert digest(sync.path(saved["path"])) == saved["hash"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["old_before", "old_during", "source_during"])
async def test_external_edits_before_or_during_conversion_keep_old_active_output(service, damage):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    path = sync.path(old["path"])
    if damage == "old_before":
        path.write_bytes(b"external edit")

    async def edit(call):
        if call["command"] == "convert":
            changed = sync.path(source_row(sync)["path"]) if damage == "source_during" else path
            if damage == "source_during":
                changed.chmod(0o644)
            changed.write_bytes(b"external edit")

    runner.on_call = edit
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    assert sync.conversion["failed"] == 1
    assert rows(sync, item_id)[0]["path"] == old["path"]
    assert rows(sync, item_id)[0]["status"] == "downloaded"
    assert path.exists() and not list(path.parent.glob("*.mp3"))
    if damage != "source_during":
        assert path.read_bytes() == b"external edit"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "empty", "symlink", "filename"])
async def test_failed_conversion_preserves_download_status_and_original_and_is_finite(service, tmp_path, failure):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    native = source_row(sync)
    sync.set_setting("format", "mp3")
    if failure == "error":
        runner.queue("convert", uri("track", 1), {"status": "error", "message": "encoder failed"})
    elif failure == "filename":
        runner.queue("convert", uri("track", 1), {"status": "ok", "filename": "../escape.mp3"})
    else:
        runner.output_kind = failure
        runner.outside_file = tmp_path / "external.mp3"
        runner.outside_file.write_bytes(b"external file")
    before = len(runner.calls)
    await convert_pass(sync)
    await sync.tick()
    assert len(runner.calls) == before + 1
    assert rows(sync, item_id)[0] == old
    assert sync.conversion["failed"] == 1 and sync.conversion["completed"] == 0
    assert digest(sync.path(old["path"])) == old["hash"]
    assert digest(sync.path(native["path"])) == native["hash"]
    assert sync.snapshot()["items"][0]["failed_count"] == 0
    assert sync.setting("conversion_job") is None
    assert not list(sync.db.execute("SELECT * FROM publications"))
    assert not clock.sleeps


@pytest.mark.asyncio
async def test_replacement_waits_for_verified_target_and_preserves_unknown_target_collision(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    path = sync.path(old["path"])
    unknown = path.with_suffix(".mp3")
    unknown.write_bytes(b"unknown external MP3")

    async def check(call):
        if call["command"] == "convert":
            assert digest(path) == old["hash"]
            assert rows(sync, item_id)[0]["path"] == old["path"]

    runner.on_call = check
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    current = rows(sync, item_id)[0]
    assert current["path"].endswith(" - 1.mp3")
    assert digest(sync.path(current["path"])) == current["hash"]
    assert not path.exists() and unknown.read_bytes() == b"unknown external MP3"
    assert sync.conversion["completed"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_at", ["before_link", "after_link", "after_commit", "during_capture"])
async def test_conversion_journal_resumes_offline_outside_nightly_window_without_orphans(service, monkeypatch, crash_at):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1), track(2))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)
    sync.set_setting("format", "aac")
    sync.account["connected"] = False
    real_finish, real_link, real_digest = sync.finish_publication, os.link, core.digest

    def crash_finish(operation):
        if crash_at == "after_commit":
            real_finish(operation)
        raise asyncio.CancelledError

    def crash_link(*args, **kwargs):
        raise asyncio.CancelledError

    def crash_capture(path):
        if ".spotisync-quarantine-" in os.readlink(path) and os.readlink(path).endswith("/captured"):
            raise asyncio.CancelledError
        return real_digest(path)

    if crash_at == "before_link":
        monkeypatch.setattr(core.os, "link", crash_link)
    elif crash_at == "during_capture":
        monkeypatch.setattr(core, "digest", crash_capture)
    else:
        monkeypatch.setattr(sync, "finish_publication", crash_finish)
    sync.start_conversion()
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    job = sync.setting("conversion_job")
    assert job["format"] == "aac" and job["next"] == 0
    sync.set_setting("format", "mp3")
    await sync.close()
    monkeypatch.setattr(core.os, "link", real_link)
    monkeypatch.setattr(core, "digest", real_digest)
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        assert not restarted.account["connected"]
        await restarted.tick()
        assert restarted.sync["phase"] == "conversion"
        await restarted.task
        assert restarted.setting("conversion_job") is None
        assert restarted.conversion["completed"] == 2 and restarted.conversion["failed"] == 0
        assert all(row["format"] == "aac" for row in rows(restarted, item_id))
        assert all(not restarted.path(row["path"]).exists() for row in old)
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        assert not list(restarted.music_dir.rglob(".spotisync-quarantine-*"))
        assert len(list(restarted.music_dir.rglob("*.m4a"))) == 2
        assert len([c for c in runner.calls if c["command"] == "download"]) == 2
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_database_migration_from_old_publications_preserves_legacy_download_and_adopts_ogg(service):
    sync, runner, clock = service
    item_id = seed_legacy_downloads(sync, runner, "ogg", 1)
    old = rows(sync, item_id)[0]
    for name in ("source_message", "source_next_attempt", "source_attempt_job", "source_quality"):
        sync.db.execute(f"ALTER TABLE tracks DROP COLUMN {name}")
    for name in ("kind", "source_uri", "old_path", "old_hash", "quality", "bitrate", "source_hash", "source_bitrate"):
        sync.db.execute(f"ALTER TABLE publications DROP COLUMN {name}")
    sync.db.execute("DROP TABLE sources")
    info = sync.path(old["path"]).stat()
    sync.db.execute("INSERT INTO publications VALUES (?,?,?,?,?,?,?,?,?)", (
        "old-publication", item_id, old["key"], old["path"], old["hash"], "ogg", "legacy", info.st_dev, info.st_ino))
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        assert rows(restarted, item_id)[0]["hash"] == old["hash"]
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        restarted.set_setting("format", "aac")
        await convert_pass(restarted)
        assert source_row(restarted)["hash"] == old["hash"]
        assert rows(restarted, item_id)[0]["format"] == "aac"
        assert [(c["command"], c["format"]) for c in runner.calls] == [("convert", "ogg"), ("convert", "aac")]
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_at", ["before_link", "after_link", "after_commit"])
async def test_canonical_publication_journal_recovers_native_before_visible_output(service, monkeypatch, crash_at):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.set_setting("format", "aac")
    real_finish, real_link = sync.finish_publication, os.link

    def crash(operation):
        if crash_at == "after_commit":
            real_finish(operation)
        raise asyncio.CancelledError

    def crash_link(*args, **kwargs):
        raise asyncio.CancelledError

    if crash_at == "before_link":
        monkeypatch.setattr(core.os, "link", crash_link)
    else:
        monkeypatch.setattr(sync, "finish_publication", crash)
    sync.start_job(item_id)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    assert rows(sync, item_id)[0]["status"] == "pending"
    assert not any(c["command"] == "convert" for c in runner.calls)
    await sync.close()
    monkeypatch.setattr(core.os, "link", real_link)
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        if crash_at != "before_link":
            assert source_row(restarted)["format"] == "ogg"
        await run_pass(restarted, item_id)
        assert rows(restarted, item_id)[0]["format"] == "aac"
        assert len(list(restarted.music_dir.rglob("*.ogg"))) == 1
        assert len(list(restarted.music_dir.rglob("*.m4a"))) == 1
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        assert len([c for c in runner.calls if c["command"] == "download"]) == (2 if crash_at == "before_link" else 1)
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_pending_native_publication_survives_untracking_even_before_recovery(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = seed_legacy_downloads(sync, runner, "aac", 1)
    old = rows(sync, item_id)[0]

    def crash(operation):
        raise asyncio.CancelledError

    monkeypatch.setattr(sync, "finish_publication", crash)
    sync.start_job(item_id)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    pending = dict(sync.db.execute("SELECT * FROM publications").fetchone())
    assert pending["kind"] == "source"
    monkeypatch.setattr(sync, "recover_publications", lambda: None)
    sync.remove(item_id)
    assert sync.path(old["path"]).exists()
    assert sync.db.execute("SELECT 1 FROM publications WHERE id=?", (pending["id"],)).fetchone()
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        assert restarted.snapshot()["items"] == []
        assert source_row(restarted)["hash"] == pending["hash"]
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        assert digest(restarted.path(old["path"])) == old["hash"]
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_replacement_cleanup_filesystem_failure_retains_durable_old_path_until_recovery(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    real_remove = sync._remove_managed_file

    def fail_cleanup(track, **kwargs):
        raise OSError("Simulated filesystem cleanup failure")

    monkeypatch.setattr(sync, "_remove_managed_file", fail_cleanup)
    sync.set_setting("format", "mp3")
    await convert_pass(sync)
    current = rows(sync, item_id)[0]
    pending = dict(sync.db.execute("SELECT * FROM publications").fetchone())
    assert pending["old_path"] == old["path"] and pending["old_hash"] == old["hash"]
    assert current["format"] == "mp3" and current["status"] == "downloaded"
    assert digest(sync.path(old["path"])) == old["hash"]
    assert digest(sync.path(current["path"])) == current["hash"]
    with pytest.raises(RuntimeError, match="recovery"):
        sync.remove(item_id)
    monkeypatch.setattr(sync, "_remove_managed_file", real_remove)
    sync.recover_publications()
    assert not sync.path(old["path"]).exists()
    assert digest(sync.path(current["path"])) == current["hash"]
    assert not list(sync.db.execute("SELECT * FROM publications"))


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ["unknown_file", "symlink_store"])
async def test_native_store_refuses_unknown_or_symlink_collisions_without_touching_external_files(service, tmp_path, collision):
    sync, runner, _ = service
    item_id = seed_legacy_downloads(sync, runner, "aac", 1)
    old = rows(sync, item_id)[0]
    if collision == "unknown_file":
        folder = sync.music_dir / ".spotisync-originals"
        folder.mkdir()
    else:
        folder = tmp_path / "outside"
        folder.mkdir()
        (sync.music_dir / ".spotisync-originals").symlink_to(folder, target_is_directory=True)
    unknown = folder / f"{1:022d}.ogg"
    unknown.write_bytes(b"external original")
    sync.set_setting("format", "ogg")
    await convert_pass(sync)
    assert sync.conversion["failed"] == 0 and sync.conversion["skipped"] == 1
    assert not runner.calls
    assert unknown.read_bytes() == b"external original"
    assert digest(sync.path(old["path"])) == old["hash"]
    assert not list(sync.db.execute("SELECT * FROM sources"))


@pytest.mark.asyncio
async def test_regular_sync_checks_original_integrity_before_skipping_completed_track(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1), track(2))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    native = sync.path(source_row(sync)["path"])
    native.chmod(0o644)
    native.write_bytes(b"corrupt original")
    before = len(runner.calls)
    await run_pass(sync, item_id)
    assert [c["command"] for c in runner.calls[before:]] == ["metadata"]
    assert rows(sync, item_id)[0]["source_message"]
    assert rows(sync, item_id)[0]["status"] == "downloaded"
    assert digest(sync.path(old["path"])) == old["hash"]


@pytest.mark.asyncio
async def test_startup_resumes_local_conversion_before_saved_auth_probe_cooldown(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    sync.set_setting("format", "aac")

    async def cancel(call):
        if call["command"] == "convert":
            raise asyncio.CancelledError

    runner.on_call = cancel
    sync.start_conversion()
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    await sync.close()
    runner.on_call = None
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        (restarted.state_dir / "credentials.json").write_text("{}")
        restarted.set_setting("auth_valid", True)
        restarted.set_setting("server_cooldown", clock.time() + 3600)
        cooldown = asyncio.Event()

        async def hold(seconds):
            assert seconds == 60
            cooldown.set()
            await asyncio.Event().wait()

        restarted.sleep = hold
        before = len(runner.calls)
        restarted.start()
        await asyncio.wait_for(cooldown.wait(), 3)
        assert [c["command"] for c in runner.calls[before:]] == ["convert"]
        assert rows(restarted, item_id)[0]["format"] == "aac"
        assert restarted.setting("conversion_job") is None
        assert restarted.conversion["completed"] == 1
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_explicit_native_backfill_inside_nightly_window_is_finite_initial_pass(service):
    sync, runner, clock = service
    item_id = seed_legacy_downloads(sync, runner, "aac", 1, 2)
    old = rows(sync, item_id)[0]
    sync.set_setting("format", "mp3")
    runner.queue("download", uri("track", 1), {"status": "error", "message": "Transient native backfill failure"})
    await convert_pass(sync)
    assert not runner.calls
    assert sync.conversion["completed"] == 0 and sync.conversion["failed"] == 0 and sync.conversion["skipped"] == 2
    assert rows(sync, item_id)[0]["path"] == old["path"]
    assert not clock.sleeps and sync.setting("conversion_job") is None


@pytest.mark.asyncio
async def test_regular_nightly_retries_native_backfill_after_other_tracks_without_changing_formats(service):
    sync, runner, clock = service
    item_id = seed_legacy_downloads(sync, runner, "aac", 1, 2)
    old = rows(sync, item_id)
    runner.queue("download", uri("track", 1), {"status": "error", "message": "Transient native backfill failure"})
    start = clock.time()
    await run_pass(sync, nightly=True)
    assert [(c["uri"], c["at"] - start) for c in runner.calls if c["command"] == "download"] == [
        (uri("track", 1), 0), (uri("track", 2), 0), (uri("track", 1), 60)]
    assert all(current["format"] == "aac" and current["source_hash"]
               for current, before in zip(rows(sync, item_id), old))
    assert all(current["source_message"] is None for current in rows(sync, item_id))
    assert len(list(sync.db.execute("SELECT * FROM sources"))) == 2


@pytest.mark.asyncio
async def test_disconnect_cancels_but_preserves_local_conversion_queue_for_offline_resume(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    entered = asyncio.Event()

    async def hold(call):
        if call["command"] == "convert":
            entered.set()
            await asyncio.Event().wait()

    runner.on_call = hold
    sync.set_setting("format", "aac")
    sync.start_conversion()
    await entered.wait()
    await sync.disconnect()
    assert not sync.account["connected"] and sync.setting("conversion_job")["format"] == "aac"
    assert digest(sync.path(old["path"])) == old["hash"]
    runner.on_call = None
    await sync.tick()
    await sync.task
    assert rows(sync, item_id)[0]["format"] == "aac"
    assert sync.conversion["completed"] == 1 and sync.setting("conversion_job") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_conversion_backfill_auth_loss_pauses_same_entry_and_resumes_saved_target(service, restart):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = seed_legacy_downloads(sync, runner, "aac", 1, 2, 3)
    before = rows(sync, item_id)
    await sync.ensure_source(before[0], "precache", quality="auto")
    queue = [{"item_id": item_id, "key": row["key"], "uri": row["uri"]} for row in before]
    saved = {"id": "old-auth-paused", "format": "mp3", "quality": "auto", "queue": queue,
             "next": 0, "total": 3, "completed": 0, "failed": 0, "errors": [], "waiting_for_auth": True}
    sync.set_setting("conversion_job", saved)
    runner.queue("download", uri("track", 2), {"status": "auth_required", "message": "Reconnect required"})
    sync.account["connected"] = False
    sync.set_setting("format", "aac")
    sync.set_setting("quality", "high")
    active = sync
    if restart:
        await sync.close()
        active = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                             now=clock.now, clock=clock.time, sleeper=clock.sleep)
        active.ready = True
        assert "Offline conversion" in active.conversion["message"]
    try:
        calls = len(runner.calls)
        await active.tick()
        assert active.setting("conversion_job")["id"] == saved["id"]
        assert active.setting("conversion_job")["format"] == "mp3"
        await active.task
        assert active.setting("conversion_job") is None
        assert active.conversion["completed"] == 1 and active.conversion["failed"] == 0
        assert active.conversion["skipped"] == 2 and not active.account["connected"]
        assert rows(active, item_id)[0]["format"] == "mp3"
        assert rows(active, item_id)[1:] == before[1:]
        assert [(c["command"], c.get("format")) for c in runner.calls[calls:]] == [("convert", "mp3")]
    finally:
        if restart:
            await active.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("next_day", [False, True])
async def test_regular_backfill_auth_loss_stops_metadata_and_resumes_unprocessed_queue_after_reconnect(service, next_day):
    sync, runner, clock = service
    first = seed_legacy_downloads(sync, runner, "aac", 1)
    second = add_item(sync, runner, 200, track(2))
    old = rows(sync, first)[0]
    runner.queue("download", uri("track", 1), {"status": "auth_required", "message": "Reconnect required"})
    await run_pass(sync, nightly=True)
    saved = sync.setting("job")
    assert saved["phase"] == "first_pass" and saved["item_ids"] == [first, second]
    assert [(c["command"], c["uri"]) for c in runner.calls] == [
        ("metadata", uri("playlist", 100)), ("metadata", uri("playlist", 200)), ("download", uri("track", 1))]
    assert rows(sync, first)[0]["status"] == "downloaded"
    assert rows(sync, first)[0]["source_attempt_job"] is None
    assert digest(sync.path(old["path"])) == old["hash"]
    assert rows(sync, second)[0]["status"] == "pending" and not sync.account["connected"]
    clock.advance(9 * 3600 + (24 * 3600 if next_day else 0))
    await sync.tick()
    assert sync.setting("job") == saved and len(runner.calls) == 3
    sync.start_connect()
    await sync.task
    sync.start_job(second)
    resumed = sync.setting("job")
    assert resumed["id"] == saved["id"] and resumed["date"] == saved["date"]
    assert resumed["item_ids"] == [first, second] and resumed["retry"] is False
    await sync.task
    assert sync.setting("job") is None
    assert rows(sync, first)[0]["source_hash"] and rows(sync, first)[0]["format"] == "aac"
    assert rows(sync, first)[0]["source_message"] is None
    assert rows(sync, second)[0]["status"] == "downloaded"
    assert [c["uri"] for c in runner.calls if c["command"] == "download"] == [uri("track", n) for n in (1, 1, 2)]
    assert [c["uri"] for c in runner.calls if c["command"] == "metadata"] == [uri("playlist", 100), uri("playlist", 200)]
    assert not clock.sleeps


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("failure", ["codec", "full decode", "metadata", "artwork", "URI", "arbitrary bytes"])
async def test_legacy_ogg_worker_validation_failure_never_publishes_canonical_or_redownloads(service, explicit, failure):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = seed_legacy_downloads(sync, runner, "ogg", 1)
    old = rows(sync, item_id)[0]
    message = "Source " + failure + " validation failed."
    if failure == "arbitrary bytes":
        sync.path(old["path"]).write_bytes(b"arbitrary nonempty bytes, not audio")
        sync.db.execute("UPDATE tracks SET hash=? WHERE item_id=?", (digest(sync.path(old["path"])), item_id))
        old = rows(sync, item_id)[0]
        message = "Fake native source validation failed."
    else:
        runner.queue("convert", uri("track", 1), {"status": "error", "message": message})
    sync.set_setting("format", "mp3")
    if explicit:
        sync.account["connected"] = False
        await convert_pass(sync)
        assert sync.conversion["failed"] == 1 and sync.conversion["errors"][0]["message"] == message
    else:
        await run_pass(sync, item_id)
        assert rows(sync, item_id)[0]["source_message"] == message
    validations = [c for c in runner.calls if c["command"] == "convert"]
    assert len(validations) == 1
    assert validations[0]["format"] == "ogg" and validations[0]["uri"] == old["uri"]
    assert Path(validations[0]["source_path"]).name == Path(old["path"]).name
    assert not any(c["command"] == "download" for c in runner.calls)
    assert not list(sync.db.execute("SELECT * FROM sources"))
    assert not list(sync.db.execute("SELECT * FROM publications"))
    assert not list((sync.music_dir / ".spotisync-originals").rglob("*.ogg"))
    current = rows(sync, item_id)[0]
    assert (current["status"], current["path"], current["format"]) == ("downloaded", old["path"], "ogg")
    assert digest(sync.path(old["path"])) == old["hash"]


@pytest.mark.asyncio
async def test_legacy_ogg_validation_bypasses_server_cooldown_and_keeps_playback_bytes(service):
    sync, runner, clock = service
    item_id = seed_legacy_downloads(sync, runner, "ogg", 1)
    old = rows(sync, item_id)[0]
    sync.account["connected"] = False
    sync.set_setting("server_cooldown", clock.time() + 3600)
    sync.set_setting("format", "ogg")

    async def validate(call):
        assert call["command"] == "convert" and call["format"] == "ogg"
        assert not list(Path(call["output_dir"]).iterdir())
        path = Path(call["source_path"])
        assert path.is_absolute() and not path.is_symlink() and path.read_bytes() == sync.path(old["path"]).read_bytes()

    runner.on_call = validate
    await convert_pass(sync)
    assert [(c["command"], c["format"]) for c in runner.calls] == [("convert", "ogg")]
    assert source_row(sync)["hash"] == old["hash"]
    assert rows(sync, item_id)[0]["path"] == old["path"]
    assert sync.path(source_row(sync)["path"]).stat().st_ino != sync.path(old["path"]).stat().st_ino
    assert not clock.sleeps


@pytest.mark.asyncio
@pytest.mark.parametrize("next_day", [False, True])
@pytest.mark.parametrize("transient_failure", [False, True])
async def test_manual_outside_window_prioritizes_saved_multi_item_first_pass_without_retries(service, next_day, transient_failure):
    sync, runner, clock = service
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))

    async def interrupt(call):
        if call["command"] == "metadata" and call["uri"] == uri("playlist", 200):
            raise asyncio.CancelledError

    runner.on_call = interrupt
    sync.start_job(nightly=True)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved = sync.setting("job")
    old = rows(sync, first)[0]
    runner.on_call = None
    if transient_failure:
        runner.queue("download", uri("track", 2), {"status": "error", "message": "Transient failure"})
    clock.advance(9 * 3600 + (24 * 3600 if next_day else 0))
    calls = len(runner.calls)
    await sync.tick()
    assert len(runner.calls) == calls and sync.setting("job") == saved
    sync.start_job(first)
    resumed = sync.setting("job")
    assert resumed["id"] == saved["id"] and resumed["date"] == saved["date"]
    assert resumed["item_ids"] == [first, second] and resumed["retry"] is False
    await sync.task
    assert [(c["command"], c["uri"]) for c in runner.calls[calls:]] == [
        ("metadata", uri("playlist", 200)), ("download", uri("track", 1)), ("download", uri("track", 2))]
    assert old["status"] == "pending" and rows(sync, first)[0]["status"] == "downloaded"
    assert rows(sync, second)[0]["status"] == ("failed" if transient_failure else "downloaded")
    assert sync.setting("job") is None and not clock.sleeps


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_shutdown", [False, True])
async def test_startup_awaiting_conversion_survives_disconnect_but_propagates_parent_shutdown(service, parent_shutdown):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    sync.set_setting("format", "aac")

    async def cancel(call):
        if call["command"] == "convert":
            raise asyncio.CancelledError

    runner.on_call = cancel
    sync.start_conversion()
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved = sync.setting("conversion_job")
    entered, stopping, release_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    polling, resume_poll, second_poll = asyncio.Event(), asyncio.Event(), asyncio.Event()
    attempts, polls = 0, 0

    async def hold_conversion(call):
        nonlocal attempts
        assert call["command"] == "convert"
        attempts += 1
        if attempts == 1:
            entered.set()
            await asyncio.Event().wait()

    async def hold_stop():
        stopping.set()
        await release_cleanup.wait()

    async def scheduler_sleep(seconds):
        nonlocal polls
        assert seconds == 10
        polls += 1
        if polls == 1:
            polling.set()
            await resume_poll.wait()
        else:
            second_poll.set()
            await asyncio.Event().wait()

    runner.on_call = hold_conversion
    sync.sleep = scheduler_sleep
    sync.account["connected"] = False
    sync.ready = False
    sync.start()
    await asyncio.wait_for(entered.wait(), 3)
    if parent_shutdown:
        await asyncio.wait_for(sync.close(), 3)
        assert sync.scheduler.cancelled() and sync.task.cancelled()
        with sqlite3.connect(sync.state_dir / "spotisync.sqlite3") as db:
            assert json.loads(db.execute("SELECT value FROM settings WHERE key='conversion_job'").fetchone()[0]) == saved
        return
    runner.stop = hold_stop
    disconnect = asyncio.create_task(sync.disconnect())
    try:
        await asyncio.wait_for(stopping.wait(), 3)
        await asyncio.wait_for(polling.wait(), 3)
        assert not sync.scheduler.done() and sync.ready and sync.busy
        assert attempts == 1 and sync.setting("conversion_job") == saved
        await sync.tick()
        assert attempts == 1
        release_cleanup.set()
        await disconnect
        assert not sync.account["connected"]
        resume_poll.set()
        await asyncio.wait_for(second_poll.wait(), 3)
        await asyncio.wait_for(sync.task, 3)
        assert attempts == 2 and not sync.scheduler.done()
        assert rows(sync, item_id)[0]["format"] == "aac"
        assert sync.setting("conversion_job") is None and sync.conversion["completed"] == 1
    finally:
        release_cleanup.set()
        await disconnect


def version_row(sync, quality, number=1):
    return dict(sync.db.execute("SELECT * FROM source_versions WHERE uri=? AND quality=?",
                                (uri("track", number), quality)).fetchone())


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", ["ogg", "mp3", "aac"])
async def test_quality_versions_regenerate_existing_format_and_switch_back_without_redownload(service, fmt):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", fmt)
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(1))
    await run_pass(sync)
    saved = {}
    for quality in ("auto", "normal", "high", "very_high", "auto", "high"):
        sync.set_setting("quality", quality)
        sync.set_setting("format", "mp3" if fmt == "ogg" else "ogg")
        await run_pass(sync)
        native = source_row(sync)
        path = sync.path(native["path"])
        saved.setdefault(native["quality"], (native, path.stat().st_ino, path.read_bytes()))
        assert native["bitrate"] == (max(v[0]["bitrate"] for v in saved.values()) if quality == "auto" else core.SOURCE_BITRATES[quality])
        assert path.stat().st_mode & 0o222 == 0
        for item_id in (first, second):
            row = rows(sync, item_id)[0]
            assert row["status"] == "downloaded" and row["format"] == fmt and row["source_quality"] == quality
            playback = sync.path(row["path"])
            assert playback.stat().st_ino != path.stat().st_ino
            assert digest(playback) == row["hash"]
            if fmt == "ogg":
                assert playback.read_bytes() == path.read_bytes()
        assert not sync.snapshot()["items"][0]["pending_quality_count"]
        assert all(item["status"] == "synced" for item in sync.snapshot()["items"])
    downloads = [call for call in runner.calls if call["command"] == "download"]
    assert [call["quality"] for call in downloads] == ["auto", "high", "very_high"]
    assert all(call["format"] == "ogg" for call in downloads)
    assert all(Path(call["source_path"]).suffix == ".ogg" for call in runner.calls if call["command"] == "convert")
    assert len(list(sync.db.execute("SELECT * FROM source_versions"))) == 3
    runner.metadata[uri("playlist", 100)] = metadata()
    await run_pass(sync, first)
    sync.remove(first)
    sync.remove(second)
    for quality, (version, inode, data) in saved.items():
        path = sync.path(version["path"])
        assert version_row(sync, quality)["hash"] == version["hash"]
        assert path.stat().st_ino == inode and path.read_bytes() == data and digest(path) == version["hash"]
    assert not clock.sleeps


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "blocked", "unavailable", "auth_required"])
async def test_failed_quality_upgrade_keeps_playback_and_exposes_pending_source_error(service, failure):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    native = version_row(sync, "auto")
    message = "Very high source quality requires Spotify Premium." if failure == "blocked" else "Native quality fetch failed."
    runner.queue("download", old["uri"], {"status": failure, "message": message})
    sync.set_setting("quality", "very_high")
    sync.set_setting("format", "ogg")
    before = len(runner.calls)
    await run_pass(sync, item_id)
    current = rows(sync, item_id)[0]
    assert (current["path"], current["hash"], current["format"], current["source_quality"], current["status"]) == (
        old["path"], old["hash"], "aac", "auto", "downloaded")
    assert digest(sync.path(old["path"])) == old["hash"]
    assert digest(sync.path(native["path"])) == native["hash"]
    assert source_row(sync)["quality"] == "auto"
    assert len(list(sync.db.execute("SELECT * FROM source_versions"))) == 1
    assert [(call["command"], call.get("quality")) for call in runner.calls[before:]] == [
        ("metadata", None), ("download", "very_high")]
    item = sync.snapshot()["items"][0]
    assert item["status"] == "failed" and item["downloaded_count"] == 1 and item["failed_count"] == 0
    assert item["pending_quality_count"] == 1 and item["errors"][0]["status"] == "source"
    assert item["errors"][0]["message"] == message
    assert sync.account["connected"] is (failure != "auth_required")
    assert not clock.sleeps


@pytest.mark.asyncio
async def test_quality_retry_uses_source_backoff_after_native_ready_and_commits_provenance_only_after_playback(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    clock.value = clock.value.replace(hour=3)
    sync.set_setting("quality", "high")
    sync.set_setting("format", "ogg")
    runner.queue("convert", old["uri"], {"status": "error", "message": "Temporary encoder failure", "retry_after": 125})
    before, start = len(runner.calls), clock.time()

    async def inspect(call):
        if call["command"] == "convert":
            assert call["format"] == "aac"
            assert source_row(sync)["quality"] == "high"
            assert rows(sync, item_id)[0]["source_quality"] == "auto"
            assert digest(sync.path(old["path"])) == old["hash"]
            if call["at"] > start:
                assert rows(sync, item_id)[0]["source_next_attempt"] == start + 125

    runner.on_call = inspect
    await run_pass(sync, nightly=True)
    calls = runner.calls[before:]
    assert [call["quality"] for call in calls if call["command"] == "download"] == ["high"]
    assert [call["at"] - start for call in calls if call["command"] == "convert"] == [0, 180]
    current = rows(sync, item_id)[0]
    assert current["source_quality"] == "high" and current["format"] == "aac"
    assert current["source_message"] is None and current["source_next_attempt"] == 0
    assert not sync.path(old["path"]).exists()
    assert sync.item(item_id)["status"] == "synced" and clock.sleeps == [60, 60, 60, 60]


@pytest.mark.asyncio
async def test_quality_backfill_retry_stops_at_8am_with_existing_output_kept(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    clock.value = clock.value.replace(hour=7, minute=59, second=30)
    sync.set_setting("quality", "high")
    runner.queue("download", old["uri"], {"status": "error", "message": "Temporary native quality failure"})
    before = len(runner.calls)
    await run_pass(sync, nightly=True)
    assert [call["quality"] for call in runner.calls[before:] if call["command"] == "download"] == ["high"]
    assert clock.now().hour == 8 and clock.sleeps == [60]
    assert rows(sync, item_id)[0]["source_quality"] == "auto" and rows(sync, item_id)[0]["status"] == "downloaded"
    assert digest(sync.path(old["path"])) == old["hash"]


@pytest.mark.asyncio
async def test_sync_and_conversion_snapshot_quality_even_when_settings_change_mid_pass(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    await run_pass(sync)
    runner.metadata[uri("playlist", 200)] = metadata(track(2), track(3))
    sync.set_setting("quality", "high")
    sync.set_setting("format", "mp3")

    async def race(call):
        if call["command"] == "download" and call["uri"] == uri("track", 1):
            sync.set_setting("quality", "normal")

    runner.on_call = race
    before = len(runner.calls)
    await run_pass(sync)
    assert [call["quality"] for call in runner.calls[before:] if call["command"] == "download"] == ["high"] * 3
    assert rows(sync, first)[0]["format"] == "ogg"
    assert [row["format"] for row in rows(sync, second)] == ["ogg", "mp3"]
    assert all(row["source_quality"] == "high" for ident in (first, second) for row in rows(sync, ident))
    sync.set_setting("format", "aac")

    async def conversion_race(call):
        if call["command"] == "convert":
            sync.set_setting("quality", "very_high")
            sync.set_setting("format", "ogg")

    runner.on_call = conversion_race
    before = len(runner.calls)
    await convert_pass(sync)
    assert not any(call["command"] == "download" for call in runner.calls[before:])
    assert [call["format"] for call in runner.calls[before:] if call["command"] == "convert"] == ["aac"] * 2
    assert sync.conversion["quality"] == "normal" and sync.conversion["format"] == "aac"
    assert rows(sync, first)[0]["source_quality"] == "normal" and rows(sync, first)[0]["format"] == "aac"
    assert rows(sync, second)[0]["source_quality"] == "normal" and rows(sync, second)[0]["format"] == "aac"
    assert rows(sync, second)[1]["source_quality"] == "high" and rows(sync, second)[1]["format"] == "mp3"
    assert sync.conversion["skipped"] == 1


@pytest.mark.asyncio
async def test_conversion_resumes_saved_quality_offline_from_cached_native_version(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old_auto = version_row(sync, "auto")
    await sync.ensure_source(rows(sync, item_id)[0], "cache-high", quality="high")
    sync.set_setting("quality", "high")
    sync.set_setting("format", "mp3")

    async def cancel(call):
        if call["command"] == "convert":
            raise asyncio.CancelledError

    runner.on_call = cancel
    sync.start_conversion()
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    assert sync.setting("conversion_job")["quality"] == "high"
    sync.set_setting("quality", "normal")
    sync.set_setting("format", "aac")
    await sync.close()
    runner.on_call = None
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        before = len(runner.calls)
        await restarted.tick()
        await restarted.task
        assert [(call["command"], call["format"]) for call in runner.calls[before:]] == [("convert", "mp3")]
        current = rows(restarted, item_id)[0]
        assert current["source_quality"] == "high" and current["format"] == "mp3"
        restarted.set_setting("format", "ogg")
        restarted.set_setting("quality", "auto")
        await convert_pass(restarted)
        assert restarted.path(rows(restarted, item_id)[0]["path"]).read_bytes() == restarted.path(version_row(restarted, "high")["path"]).read_bytes()
        assert digest(restarted.path(old_auto["path"])) == old_auto["hash"]
        assert [call["quality"] for call in runner.calls if call["command"] == "download"] == ["auto", "high"]
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["modified", "symlink", "missing", "record_hash"])
async def test_cached_quality_integrity_failure_never_redownloads_or_falls_back_to_playback(service, damage):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "mp3")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    await sync.ensure_source(old, "cache-high", quality="high")
    sync.set_setting("quality", "normal")
    await run_pass(sync, item_id)
    assert source_row(sync)["quality"] == "auto"
    native = sync.path(version_row(sync, "high")["path"])
    if damage == "modified":
        native.chmod(0o644)
        native.write_bytes(b"externally modified high-quality original")
    elif damage == "symlink":
        native.unlink()
        native.symlink_to(sync.path(old["path"]))
    elif damage == "missing":
        native.unlink()
    else:
        sync.db.execute("UPDATE source_versions SET hash='incorrect' WHERE quality='high'")
    sync.set_setting("quality", "high")
    before = len(runner.calls)
    await run_pass(sync, item_id)
    assert [call["command"] for call in runner.calls[before:]] == ["metadata"]
    assert source_row(sync)["quality"] == "auto"
    assert rows(sync, item_id)[0]["source_quality"] == "normal" and rows(sync, item_id)[0]["status"] == "downloaded"
    assert digest(sync.path(old["path"])) == old["hash"]
    assert sync.item(item_id)["status"] == "failed"


@pytest.mark.asyncio
async def test_quality_migration_backfills_legacy_auto_and_recovers_old_publication_without_relabeling(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    native = source_row(sync)
    old = rows(sync, item_id)[0]
    cover = "https://i.scdn.co/quality-migration-cover"
    sync.db.execute("UPDATE items SET thumbnail_url=? WHERE id=?", (cover, item_id))
    sync.db.execute("UPDATE tracks SET thumbnail_url=? WHERE item_id=?", (cover, item_id))
    sync.db.execute("DROP TABLE source_versions")
    sync.db.execute("ALTER TABLE sources DROP COLUMN quality")
    sync.db.execute("ALTER TABLE tracks DROP COLUMN source_quality")
    sync.db.execute("ALTER TABLE publications DROP COLUMN quality")
    info = sync.path(old["path"]).stat()
    sync.db.execute("""INSERT INTO publications
        (id,item_id,track_key,path,hash,format,job_id,device,inode) VALUES (?,?,?,?,?,?,?,?,?)""",
        ("legacy-quality-publication", item_id, old["key"], old["path"], old["hash"], "aac", "old-job", info.st_dev, info.st_ino))
    sync.set_setting("quality", "high")
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        assert source_row(restarted)["quality"] == "auto"
        assert version_row(restarted, "auto")["path"] == native["path"]
        assert version_row(restarted, "auto")["hash"] == native["hash"]
        assert rows(restarted, item_id)[0]["source_quality"] == "auto"
        assert rows(restarted, item_id)[0]["format"] == "aac"
        assert restarted.item(item_id)["thumbnail_url"] == cover
        assert rows(restarted, item_id)[0]["thumbnail_url"] == cover
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        assert len(list(restarted.db.execute("SELECT * FROM source_versions"))) == 1
        before = len(runner.calls)
        await run_pass(restarted, item_id)
        assert [call["quality"] for call in runner.calls[before:] if call["command"] == "download"] == ["high"]
        assert rows(restarted, item_id)[0]["source_quality"] == "high" and rows(restarted, item_id)[0]["format"] == "aac"
        assert digest(restarted.path(native["path"])) == native["hash"]
        assert len(list(restarted.db.execute("SELECT * FROM source_versions"))) == 2
    finally:
        await restarted.close()
    again = SyncService(sync.state_dir, sync.music_dir, runner=runner)
    try:
        assert len(list(again.db.execute("SELECT * FROM source_versions"))) == 2
        assert source_row(again)["quality"] == "high" and version_row(again, "auto")["hash"] == native["hash"]
    finally:
        await again.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["source", "output"])
@pytest.mark.parametrize("crash_at", ["before_link", "after_link", "after_commit"])
async def test_quality_publication_journal_recovers_version_and_playback_provenance(service, monkeypatch, kind, crash_at):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    native = version_row(sync, "auto")
    old = rows(sync, item_id)[0]
    sync.set_setting("quality", "high")
    sync.set_setting("format", "ogg")
    real_finish, real_link = sync.finish_publication, os.link

    def crash_finish(operation):
        if operation["kind"] == kind:
            assert operation["quality"] == "high"
            if kind == "output":
                assert operation["old_path"] == old["path"] and operation["old_hash"] == old["hash"]
            if crash_at == "after_commit":
                real_finish(operation)
            raise asyncio.CancelledError
        return real_finish(operation)

    def crash_link(source, destination, **kwargs):
        is_source = destination == f"{1:022d}-high.ogg"
        if is_source == (kind == "source"):
            raise asyncio.CancelledError
        return real_link(source, destination, **kwargs)

    if crash_at == "before_link":
        monkeypatch.setattr(core.os, "link", crash_link)
    else:
        monkeypatch.setattr(sync, "finish_publication", crash_finish)
    sync.start_job(item_id)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved = sync.setting("job")
    assert saved["quality"] == "high"
    if crash_at != "after_commit" or kind == "source":
        assert rows(sync, item_id)[0]["source_quality"] == "auto"
        assert digest(sync.path(old["path"])) == old["hash"]
    sync.set_setting("quality", "normal")
    await sync.close()
    monkeypatch.setattr(core.os, "link", real_link)
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        restarted.start_job(item_id)
        assert restarted.setting("job")["quality"] == "high"
        await restarted.task
        current = rows(restarted, item_id)[0]
        assert current["source_quality"] == "high" and current["format"] == "aac" and current["status"] == "downloaded"
        assert source_row(restarted)["quality"] == "high"
        assert digest(restarted.path(version_row(restarted, "high")["path"])) == version_row(restarted, "high")["hash"]
        assert digest(restarted.path(native["path"])) == native["hash"]
        assert len(list(restarted.db.execute("SELECT * FROM source_versions"))) == 2
        assert not list(restarted.db.execute("SELECT * FROM publications"))
        assert not restarted.path(old["path"]).exists()
        assert len(list(restarted.music_dir.rglob("*.m4a"))) == 1
        high_downloads = [call for call in runner.calls if call["command"] == "download" and call["quality"] == "high"]
        assert len(high_downloads) == (2 if kind == "source" and crash_at == "before_link" else 1)
        assert not any(call.get("quality") == "normal" for call in runner.calls)
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_quality_retry_sql_resumes_when_native_is_ready_but_playback_still_old_quality(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "mp3")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    await sync.ensure_source(old, "cached-high", quality="high")
    clock.value = clock.value.replace(hour=3)
    job = {"id": "quality-retry", "phase": "retry", "quality": "high", "date": clock.now().date().isoformat(),
           "item_ids": [item_id], "retry": True}
    sync.source_failed(old, job["id"], {"status": "error", "message": "Temporary local conversion failure", "retry_after": 125})
    due = rows(sync, item_id)[0]["source_next_attempt"]
    sync.set_setting("job", job)
    sync.set_setting("last_nightly", job["date"])
    sync.set_setting("quality", "normal")
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        before = len(runner.calls)
        await restarted.tick()
        assert restarted.setting("job")["quality"] == "high"
        await restarted.task
        calls = runner.calls[before:]
        assert [(call["command"], call.get("format")) for call in calls] == [("convert", "mp3")]
        assert calls[0]["at"] >= due and calls[0]["at"] < due + 60
        assert rows(restarted, item_id)[0]["source_quality"] == "high"
        assert rows(restarted, item_id)[0]["source_message"] is None
        assert restarted.setting("job") is None
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "conversion"])
async def test_shipped_jobs_without_quality_resume_legacy_auto_not_new_setting(service, kind):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    if kind == "conversion":
        await run_pass(sync, item_id)
        sync.set_setting("conversion_job", {"id": "old-conversion", "format": "mp3",
            "queue": [{"item_id": item_id, "key": f"{1:022d}", "uri": uri("track", 1)}],
            "next": 0, "total": 1, "completed": 0, "failed": 0, "errors": []})
    else:
        sync.set_setting("job", {"id": "old-sync", "date": clock.now().date().isoformat(),
            "phase": "first_pass", "item_ids": [item_id], "retry": False})
    sync.set_setting("quality", "high")
    if kind == "conversion":
        await convert_pass(sync)
        assert sync.conversion["quality"] == "auto"
    else:
        await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["source_quality"] == "auto"
    assert [call["quality"] for call in runner.calls if call["command"] == "download"] == ["auto"]
    assert len(list(sync.db.execute("SELECT * FROM source_versions"))) == 1


@pytest.mark.asyncio
async def test_quality_upgrade_preserves_external_changes_without_fetching_or_replacing(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    path = sync.path(old["path"])
    path.write_bytes(b"externally modified playback")
    sync.set_setting("quality", "high")
    before = len(runner.calls)
    await run_pass(sync, item_id)
    assert [call["command"] for call in runner.calls[before:]] == ["metadata"]
    assert rows(sync, item_id)[0]["source_quality"] == "auto" and rows(sync, item_id)[0]["path"] == old["path"]
    assert path.read_bytes() == b"externally modified playback"
    assert len(list(sync.db.execute("SELECT * FROM source_versions"))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_legacy_ogg_is_archived_as_auto_before_first_quality_upgrade_replaces_playback(service, explicit):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = seed_legacy_downloads(sync, runner, "ogg", 1)
    old = rows(sync, item_id)[0]
    data, inode = sync.path(old["path"]).read_bytes(), sync.path(old["path"]).stat().st_ino
    sync.set_setting("quality", "high")
    if explicit:
        await convert_pass(sync)
    else:
        await run_pass(sync, item_id)
    auto = version_row(sync, "auto")
    assert auto["hash"] == old["hash"]
    assert sync.path(auto["path"]).read_bytes() == data and sync.path(auto["path"]).stat().st_ino != inode
    assert sync.path(auto["path"]).stat().st_mode & 0o222 == 0
    assert [call["quality"] for call in runner.calls if call["command"] == "download"] == ([] if explicit else ["high"])
    assert [call["format"] for call in runner.calls if call["command"] == "convert"] == ["ogg"]
    current = rows(sync, item_id)[0]
    assert current["source_quality"] == ("auto" if explicit else "high") and current["format"] == "ogg"
    assert sync.path(old["path"]).exists() is explicit
    if explicit:
        assert sync.conversion["skipped"] == 1 and sync.conversion["failed"] == 0
    sync.account["connected"] = False
    sync.set_setting("quality", "auto")
    await convert_pass(sync)
    expected = data if explicit else sync.path(version_row(sync, "high")["path"]).read_bytes()
    assert sync.path(rows(sync, item_id)[0]["path"]).read_bytes() == expected
    assert len([call for call in runner.calls if call["command"] == "download"]) == (0 if explicit else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "conversion"])
async def test_quality_auth_pause_retains_saved_target_and_existing_playback_until_reconnect(service, kind):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    native = version_row(sync, "auto")
    sync.set_setting("quality", "high")
    sync.set_setting("format", "mp3")
    runner.queue("download", old["uri"], {"status": "auth_required", "message": "Reconnect for native quality download."})
    if kind == "sync":
        await run_pass(sync, item_id)
    else:
        await convert_pass(sync)
        assert sync.conversion["skipped"] == 1 and sync.conversion["failed"] == 0
        assert sync.setting("conversion_job") is None
        assert rows(sync, item_id)[0] == old
        assert sync.account["connected"]
        assert [call["quality"] for call in runner.calls if call["command"] == "download"] == ["auto"]
        return
    setting = "job" if kind == "sync" else "conversion_job"
    saved = sync.setting(setting)
    assert saved["quality"] == "high"
    if kind == "conversion":
        assert saved["next"] == 0 and saved["waiting_for_auth"] and saved["failed"] == 0
    assert rows(sync, item_id)[0]["source_quality"] == "auto" and rows(sync, item_id)[0]["status"] == "downloaded"
    assert digest(sync.path(old["path"])) == old["hash"]
    assert not sync.account["connected"]
    sync.set_setting("quality", "normal")
    sync.set_setting("format", "ogg")
    sync.start_connect()
    await sync.task
    if kind == "sync":
        sync.start_job(item_id)
    else:
        await sync.tick()
    assert sync.setting(setting)["quality"] == "high" and sync.setting(setting)["id"] == saved["id"]
    await sync.task
    current = rows(sync, item_id)[0]
    assert current["source_quality"] == "high" and current["format"] == ("aac" if kind == "sync" else "mp3")
    assert current["source_message"] is None and sync.setting(setting) is None
    assert source_row(sync)["quality"] == "high"
    assert digest(sync.path(native["path"])) == native["hash"]
    assert [call["quality"] for call in runner.calls if call["command"] == "download"] == ["auto", "high", "high"]


@pytest.mark.parametrize("nominal", [96000, 160000, 320000, 0, -1, 128000])
def test_actual_nominal_inspection_rejects_mutagen_fallback_estimates(tmp_path, monkeypatch, nominal):
    import struct
    from types import SimpleNamespace

    path = tmp_path / "nominal.ogg"
    path.write_bytes(b"fixture identification page")
    estimated = nominal if nominal in core.SOURCE_BITRATES.values() else 320000
    monkeypatch.setattr(core, "OggVorbis", lambda _: SimpleNamespace(info=SimpleNamespace(serial=7, bitrate=estimated)))
    packet = b"\x01vorbis" + b"\x00" * 13 + struct.pack("<i", nominal) + b"\x00" * 6
    monkeypatch.setattr(core, "OggPage", lambda _: SimpleNamespace(first=True, serial=7, packets=[packet]))
    if nominal in core.SOURCE_BITRATES.values():
        assert REAL_INSPECT_NATIVE_BITRATE(path) == nominal
    else:
        with pytest.raises(OSError, match="unknown or corrupt nominal"):
            REAL_INSPECT_NATIVE_BITRATE(path)


@pytest.mark.asyncio
@pytest.mark.parametrize("fmt", ["ogg", "mp3", "aac"])
async def test_auto_320_reuses_actual_very_high_without_alias_files_network_or_reencoding(service, native_bitrates, fmt):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    native_bitrates[b"mock worker-verified audio: " + uri("track", 1).encode()] = 320000
    sync.set_setting("format", fmt)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    native = version_row(sync, "auto")
    path = sync.path(native["path"])
    data, inode = path.read_bytes(), path.stat().st_ino
    before = len(runner.calls)
    sync.account["connected"] = False
    account = dict(sync.account)
    sync.set_setting("quality", "very_high")
    await convert_pass(sync)
    current = rows(sync, item_id)[0]
    assert len(runner.calls) == before and sync.account == account
    assert current["path"] == old["path"] and current["hash"] == old["hash"] and current["format"] == fmt
    assert current["source_quality"] == "very_high" and current["source_bitrate"] == 320000
    assert current["source_hash"] == native["hash"]
    assert sync.conversion["completed"] == 1 and sync.conversion["skipped"] == sync.conversion["failed"] == 0
    assert source_row(sync)["quality"] == "auto" and source_row(sync)["path"] == native["path"]
    assert len(list(sync.db.execute("SELECT * FROM source_versions"))) == 1
    assert path.read_bytes() == data and path.stat().st_ino == inode
    assert sync.snapshot()["items"][0]["pending_quality_count"] == 0
    sync.account["connected"] = True
    clock.value = clock.value.replace(hour=3)
    await run_pass(sync, nightly=True)
    assert [call["command"] for call in runner.calls[before:]] == ["metadata"]
    assert not clock.sleeps and sync.item(item_id)["status"] == "synced"


@pytest.mark.asyncio
async def test_actual_bitrate_migration_keeps_unproven_legacy_conversion_offline_and_preserves_duplicates(service, native_bitrates):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    body = b"mock worker-verified audio: " + uri("track", 1).encode()
    native_bitrates[body] = 320000
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    auto = version_row(sync, "auto")
    duplicate_path = core.native_path(uri("track", 1), "very_high")
    sync.path(duplicate_path).write_bytes(body)
    sync.path(duplicate_path).chmod(0o444)
    sync.db.execute("INSERT INTO source_versions(uri,quality,path,hash,format) VALUES (?,?,?,?,'ogg')",
                    (uri("track", 1), "very_high", duplicate_path, auto["hash"]))
    originals = [(row["path"], sync.path(row["path"]).stat().st_ino, sync.path(row["path"]).read_bytes())
                 for row in sync.db.execute("SELECT * FROM source_versions")]
    old = rows(sync, item_id)[0]
    for table, fields in {"tracks": ("source_hash", "source_bitrate"), "sources": ("bitrate",),
                          "source_versions": ("bitrate",), "publications": ("bitrate", "source_hash", "source_bitrate")}.items():
        for field in fields:
            sync.db.execute(f"ALTER TABLE {table} DROP COLUMN {field}")
    sync.set_setting("quality", "very_high")
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        assert rows(restarted, item_id)[0]["source_hash"] is None
        assert version_row(restarted, "auto")["bitrate"] is None
        before = len(runner.calls)
        await convert_pass(restarted)
        assert [(call["command"], call["format"]) for call in runner.calls[before:]] == [("convert", "aac")]
        assert restarted.conversion["completed"] == 1
        current = rows(restarted, item_id)[0]
        assert current["hash"] == old["hash"] and not restarted.path(old["path"]).exists()
        assert current["source_bitrate"] == 320000 and current["source_quality"] == "very_high"
        assert len(list(restarted.db.execute("SELECT * FROM source_versions"))) == 2
        assert all(row["bitrate"] == 320000 for row in restarted.db.execute("SELECT * FROM source_versions"))
        for path, inode, data in originals:
            assert restarted.path(path).stat().st_ino == inode and restarted.path(path).read_bytes() == data
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_offline_missing_160_skips_without_auth_backfill_or_source_failure_flags(service, native_bitrates):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    native_bitrates[b"mock worker-verified audio: " + uri("track", 1).encode()] = 320000
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    versions = [dict(row) for row in sync.db.execute("SELECT * FROM source_versions")]
    sync.account["connected"] = False
    account = dict(sync.account)
    sync.set_setting("quality", "high")
    sync.set_setting("format", "mp3")
    before = len(runner.calls)
    await convert_pass(sync)
    assert len(runner.calls) == before and sync.account == account and rows(sync, item_id)[0] == old
    assert sync.conversion["skipped"] == 1 and sync.conversion["completed"] == sync.conversion["failed"] == 0
    assert sync.conversion["skips"][0]["name"] == "Song 1"
    assert "160 kbps" in sync.conversion["skips"][0]["message"] and "Sync required" in sync.conversion["skips"][0]["message"]
    assert [dict(row) for row in sync.db.execute("SELECT * FROM source_versions")] == versions
    assert sync.setting("conversion_job") is None


@pytest.mark.asyncio
async def test_offline_conversion_auto_uses_highest_verified_cache_and_excludes_failed_pending_rows(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1), track(2), track(3))
    await run_pass(sync, item_id)
    await sync.ensure_source(rows(sync, item_id)[0], "precache", quality="very_high")
    sync.db.execute("UPDATE tracks SET status='failed',message='Old key failure' WHERE item_id=? AND key=?", (item_id, f"{2:022d}"))
    sync.db.execute("UPDATE tracks SET status='pending',message='Old pending work' WHERE item_id=? AND key=?", (item_id, f"{3:022d}"))
    before_rows = rows(sync, item_id)
    natives = [(row["path"], sync.path(row["path"]).read_bytes()) for row in sync.db.execute("SELECT * FROM source_versions")]
    sync.account["connected"] = False
    before = len(runner.calls)
    await convert_pass(sync)
    assert [(call["command"], call["uri"], call["format"]) for call in runner.calls[before:]] == [("convert", uri("track", 1), "aac")]
    assert rows(sync, item_id)[0]["source_bitrate"] == 320000 and rows(sync, item_id)[0]["source_quality"] == "auto"
    assert rows(sync, item_id)[1:] == before_rows[1:]
    assert sync.conversion["total"] == sync.conversion["completed"] == 1
    for path, data in natives:
        assert sync.path(path).read_bytes() == data


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["modified", "symlink", "unknown_nominal"])
async def test_offline_corrupt_original_is_not_used_removed_or_marked_as_source_download_failure(service, native_bitrates, damage):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    sync.set_setting("format", "aac")
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    old = rows(sync, item_id)[0]
    source = version_row(sync, "auto")
    native = sync.path(source["path"])
    if damage == "modified":
        native.chmod(0o644)
        native.write_bytes(b"externally modified native original")
    elif damage == "symlink":
        native.unlink()
        native.symlink_to(sync.path(old["path"]))
    else:
        native_bitrates[native.read_bytes()] = 0
        sync.db.execute("UPDATE source_versions SET bitrate=NULL")
        sync.db.execute("UPDATE sources SET bitrate=NULL")
    sync.account["connected"] = False
    sync.set_setting("format", "mp3")
    before = len(runner.calls)
    await convert_pass(sync)
    assert len(runner.calls) == before and rows(sync, item_id)[0] == old
    assert sync.conversion["failed"] == 1 and sync.conversion["skipped"] == 0
    assert native.exists() and digest(sync.path(old["path"])) == old["hash"]
    assert version_row(sync, "auto")["hash"] == source["hash"]


@pytest.mark.asyncio
async def test_sync_progress_counts_five_unique_new_uris_not_eight_old_songs_or_folder_copies(service, monkeypatch):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = add_item(sync, runner, 100, *(track(number) for number in range(1, 9)))
    await run_pass(sync, first)
    runner.metadata[uri("playlist", 100)] = metadata(*(track(number) for number in range(1, 12)))
    second = add_item(sync, runner, 200, track(1), track(9), track(12), track(13))
    before = len(runner.calls)
    real_finish = sync.finish_publication

    async def inspect(call):
        state = sync.snapshot()["sync"]
        if call["command"] == "metadata":
            assert not state["progress_ready"] and state["completed"] == 0
            assert not any(c["command"] == "download" for c in runner.calls[before:])
        if call["command"] == "download":
            assert state["progress_ready"] and state["total"] == 5

    def finish(operation):
        prior = sync.sync["completed"]
        real_finish(operation)
        if operation["kind"] == "source":
            assert sync.sync["completed"] == prior
        else:
            assert sync.sync["completed"] in (prior, prior + 1)

    runner.on_call = inspect
    monkeypatch.setattr(sync, "finish_publication", finish)
    await run_pass(sync)
    calls = runner.calls[before:]
    assert [call["command"] for call in calls[:2]] == ["metadata", "metadata"]
    assert [call["uri"] for call in calls if call["command"] == "download"] == [uri("track", n) for n in range(9, 14)]
    assert sync.sync["completed"] == sync.sync["total"] == 5 and sync.sync["progress_ready"]
    progress = sync.setting("sync_progress")
    assert progress["baseline"] == [uri("track", n) for n in range(1, 9)]
    assert set(progress["targets"]) == set(progress["completed"]) == {uri("track", n) for n in range(9, 14)}
    assert rows(sync, second)[0]["status"] == "downloaded"


@pytest.mark.asyncio
async def test_sync_progress_excludes_cached_repairs_existing_backfills_and_quality_upgrades(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = seed_legacy_downloads(sync, runner, "aac", 1)
    second = add_item(sync, runner, 200, track(2))
    sync.reconcile(second, runner.metadata[uri("playlist", 200)], "precache-metadata")
    await sync.ensure_source(rows(sync, second)[0], "precache", quality="auto")
    sync.set_setting("quality", "high")
    await run_pass(sync)
    assert sync.sync["total"] == sync.sync["completed"] == 0 and sync.sync["progress_ready"]
    assert set(sync.setting("sync_progress")["baseline"]) == {uri("track", 1), uri("track", 2)}
    assert rows(sync, first)[0]["format"] == "aac" and rows(sync, second)[0]["status"] == "downloaded"
    sync.remove(first)
    sync.remove(second)
    third = add_item(sync, runner, 300, track(2))
    before = len(runner.calls)
    await run_pass(sync, third)
    assert [call["command"] for call in runner.calls[before:]] == ["metadata"]
    assert sync.sync["total"] == sync.sync["completed"] == 0 and sync.sync["progress_ready"]


@pytest.mark.asyncio
async def test_sync_progress_excludes_local_unavailable_unresolved_and_increments_successful_retry_once(service):
    sync, runner, _ = service
    local = track(3, uri="spotify:local:A:B:C:1", availability="local")
    unresolved = track(4, uri="spotify:unresolved:entry", availability="unknown")
    first = add_item(sync, runner, 100, track(1), track(2, availability="unavailable"), local, unresolved)
    second = add_item(sync, runner, 200, track(1))
    runner.queue("download", uri("track", 1), {"status": "error", "message": "Temporary audio failure"}, {"status": "error"})
    await run_pass(sync, nightly=True)
    progress = sync.setting("sync_progress")
    assert progress["targets"] == progress["completed"] == [uri("track", 1)]
    assert sync.sync["total"] == sync.sync["completed"] == 1
    assert rows(sync, first)[0]["status"] == rows(sync, second)[0]["status"] == "downloaded"


@pytest.mark.asyncio
async def test_sync_progress_grows_with_incomplete_discovery_and_never_shrinks_when_target_is_removed(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=7, minute=57)
    item_id = add_item(sync, runner, 100, track(1), complete=False)
    runner.queue("metadata", uri("playlist", 100), metadata(track(1), complete=False),
        metadata(track(1), track(2), track(3), complete=False), metadata(track(1), track(2)))
    runner.queue("download", uri("track", 3), {"status": "unavailable", "message": "Unavailable after discovery"})
    seen = []

    async def observe(call):
        if call["command"] == "download":
            seen.append((sync.sync["completed"], sync.sync["total"]))

    runner.on_call = observe
    await run_pass(sync, nightly=True)
    assert seen == [(0, 1), (1, 3), (2, 3)]
    progress = sync.setting("sync_progress")
    assert progress["baseline"] == []
    assert set(progress["targets"]) == {uri("track", n) for n in (1, 2, 3)}
    assert set(progress["completed"]) == {uri("track", n) for n in (1, 2)}
    assert sync.sync["total"] == 3 and sync.sync["completed"] == 2 and not sync.sync["total_provisional"]
    assert len(rows(sync, item_id)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_at", ["native_commit", "output_link", "output_commit"])
async def test_sync_progress_publication_is_atomic_and_recovery_is_idempotent(service, monkeypatch, crash_at):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    real_finish = sync.finish_publication

    def crash(operation):
        if crash_at == "native_commit" and operation["kind"] == "source":
            real_finish(operation)
            assert sync.setting("sync_progress")["completed"] == []
            raise asyncio.CancelledError
        if operation["kind"] == "output" and crash_at in {"output_link", "output_commit"}:
            if crash_at == "output_commit":
                real_finish(operation)
            raise asyncio.CancelledError
        return real_finish(operation)

    monkeypatch.setattr(sync, "finish_publication", crash)
    sync.start_job(item_id)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved = sync.setting("sync_progress")
    assert saved["targets"] == [uri("track", 1)] and saved["baseline"] == []
    assert not saved["from_resume"] and not sync.sync["progress_from_resume"]
    assert saved["completed"] == ([uri("track", 1)] if crash_at == "output_commit" else [])
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        await run_pass(restarted, item_id)
        assert restarted.setting("sync_progress")["baseline"] == []
        assert not restarted.sync["progress_from_resume"]
        assert restarted.sync["completed"] == restarted.sync["total"] == 1
        assert restarted.setting("sync_progress")["job_id"] == saved["job_id"]
        restarted.recover_publications()
        restarted.recover_publications()
        assert restarted.sync["completed"] == restarted.sync["total"] == 1
        assert len([call for call in runner.calls if call["command"] == "download"]) == 1
    finally:
        await restarted.close()
    again = SyncService(sync.state_dir, sync.music_dir, runner=runner)
    try:
        assert again.sync["completed"] == again.sync["total"] == 1 and again.sync["progress_ready"]
    finally:
        await again.close()


@pytest.mark.asyncio
async def test_sync_progress_auth_resume_preserves_frozen_baseline_and_thumbnail_requests_do_not_mutate_it(service, public_get):
    sync, runner, clock = service
    item_id = add_item(sync, runner, 100, track(1), track(2))
    runner.queue("download", uri("track", 2), {"status": "auth_required", "message": "Reconnect required"})
    await run_pass(sync, nightly=True)
    saved = sync.setting("sync_progress")
    assert saved["baseline"] == [] and len(saved["targets"]) == 2 and saved["completed"] == [uri("track", 1)]
    public_get.side_effect = None
    public_get.return_value = oembed_response({"thumbnail_url": "https://i.scdn.co/progress-cover"})
    assert await sync.thumbnail(item_id) == "https://i.scdn.co/progress-cover"
    assert sync.setting("sync_progress") == saved
    sync.start_connect()
    await sync.task
    await sync.tick()
    await sync.task
    assert sync.setting("sync_progress")["baseline"] == []
    assert sync.sync["total"] == sync.sync["completed"] == 2
    assert sync.setting("sync_progress")["job_id"] == saved["job_id"]


@pytest.mark.asyncio
async def test_legacy_sync_queue_without_baseline_reports_remaining_segment_until_fresh_job(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.set_setting("job", {"id": "pre-counter-job", "date": clock.now().date().isoformat(),
        "phase": "first_pass", "item_ids": [item_id], "retry": False, "quality": "auto"})
    await run_pass(sync, item_id)
    assert sync.sync["progress_ready"] and sync.sync["progress_from_resume"]
    assert sync.setting("sync_progress")["from_resume"] and not sync.setting("sync_progress")["legacy"]
    assert not sync.sync["total_provisional"]
    assert sync.sync["completed"] == sync.sync["total"] == 1
    await run_pass(sync, item_id)
    assert sync.sync["progress_ready"] and not sync.setting("sync_progress")["legacy"]
    assert not sync.sync["progress_from_resume"] and not sync.setting("sync_progress")["from_resume"]
    assert sync.sync["completed"] == sync.sync["total"] == 0


@pytest.mark.asyncio
async def test_removed_target_before_audio_is_not_counted_as_success_and_total_does_not_shrink(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = add_item(sync, runner, 100, track(1))
    add_item(sync, runner, 200)

    async def remove_before_audio(call):
        if call["command"] == "metadata" and call["uri"] == uri("playlist", 200):
            assert sync.sync["total"] == 1 and not sync.sync["progress_ready"]
            sync.reconcile(first, metadata(), sync.setting("job")["id"])

    runner.on_call = remove_before_audio
    await run_pass(sync)
    assert [call["command"] for call in runner.calls] == ["metadata", "metadata"]
    assert sync.sync["completed"] == 0 and sync.sync["total"] == 1 and sync.sync["progress_ready"]
    assert sync.setting("sync_progress")["targets"] == [uri("track", 1)]
    assert sync.setting("sync_progress")["completed"] == []


@pytest.mark.asyncio
async def test_output_database_failure_rolls_back_progress_in_same_transaction(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.db.execute("""CREATE TEMP TRIGGER reject_counter_update BEFORE UPDATE ON settings
        WHEN NEW.key='sync_progress' AND json_array_length(json_extract(NEW.value,'$.completed'))>0
        BEGIN SELECT RAISE(FAIL,'simulated counter persistence failure'); END""")
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["status"] != "downloaded" and rows(sync, item_id)[0]["path"] is None
    assert sync.sync["completed"] == 0 and sync.sync["total"] == 1
    assert sync.setting("sync_progress")["completed"] == []
    assert len(list(sync.db.execute("SELECT * FROM source_versions"))) == 1
    assert not visible_oggs(sync)
    assert not list(sync.db.execute("SELECT * FROM publications"))


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["first_pass", "retry"])
@pytest.mark.parametrize("old_progress", ["missing", "legacy", "null_baseline"])
async def test_legacy_resume_counter_excludes_old_success_and_cached_original_then_counts_remaining_three(service, phase, old_progress):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    prior_output = rows(sync, item_id)[0]
    membership = metadata(*(track(number) for number in range(1, 6)))
    runner.metadata[uri("playlist", 100)] = membership
    sync.reconcile(item_id, membership, "old-job" if phase == "retry" else "earlier-metadata")
    await sync.ensure_source(rows(sync, item_id)[4], "old-cached-original", quality="auto")
    job = {"id": "old-job", "date": clock.now().date().isoformat(), "phase": phase,
           "item_ids": [item_id], "retry": phase == "retry", "quality": "auto"}
    sync.set_setting("job", job)
    if old_progress == "missing":
        sync.set_setting("sync_progress", None)
    else:
        sync.set_setting("sync_progress", {"job_id": job["id"], "baseline": [] if old_progress == "legacy" else None,
            "targets": [uri("track", number) for number in range(1, 6)], "completed": [uri("track", 1)],
            "ready": False, "legacy": old_progress == "legacy", "provisional": True})
    if phase == "retry":
        clock.value = clock.value.replace(hour=3)
        sync.set_setting("last_nightly", job["date"])
    calls = len(runner.calls)
    seen = []

    async def observe(call):
        state = sync.snapshot()["sync"]
        if call["command"] == "metadata":
            assert not state["progress_ready"] and state["completed"] == 0
        if call["command"] == "download":
            assert state["progress_ready"] and state["progress_from_resume"] and not state["total_provisional"]
            seen.append((state["completed"], state["total"]))

    runner.on_call = observe
    sync.start_job(existing=job)
    assert sync.sync["completed"] == 0 and sync.sync["total"] == 0
    assert sync.sync["progress_from_resume"] and not sync.sync["progress_ready"]
    await sync.task
    assert seen == [(0, 3), (1, 3), (2, 3)]
    state = sync.snapshot()["sync"]
    assert state["completed"] == state["total"] == 3 and state["progress_ready"] and state["progress_from_resume"]
    assert not state["total_provisional"]
    progress = sync.setting("sync_progress")
    assert progress["baseline"] == [uri("track", 1), uri("track", 5)]
    assert progress["targets"] == progress["completed"] == [uri("track", number) for number in (2, 3, 4)]
    assert progress["from_resume"] and not progress["legacy"]
    assert [call["uri"] for call in runner.calls[calls:] if call["command"] == "download"] == progress["targets"]
    assert rows(sync, item_id)[0]["path"] == prior_output["path"]


@pytest.mark.asyncio
async def test_remaining_resume_baseline_and_flag_survive_further_restart_without_reset(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    runner.metadata[uri("playlist", 100)] = metadata(*(track(number) for number in range(1, 5)))
    job = {"id": "old-resuming-job", "date": clock.now().date().isoformat(), "phase": "first_pass",
           "item_ids": [item_id], "retry": False, "quality": "auto"}
    sync.set_setting("job", job)
    sync.set_setting("sync_progress", None)

    async def interrupt(call):
        if call["command"] == "download" and call["uri"] == uri("track", 3):
            raise asyncio.CancelledError

    runner.on_call = interrupt
    sync.start_job(item_id)
    with pytest.raises(asyncio.CancelledError):
        await sync.task
    saved = sync.setting("sync_progress")
    assert saved["from_resume"] and saved["baseline"] == [uri("track", 1)]
    assert saved["completed"] == [uri("track", 2)] and len(saved["targets"]) == 3
    await sync.close()
    runner.on_call = None
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        restarted.ready = True
        restarted.account["connected"] = True
        assert restarted.snapshot()["sync"]["progress_from_resume"]
        assert restarted.sync["completed"] == 1 and restarted.sync["total"] == 3
        restarted.start_job(item_id)
        assert restarted.setting("sync_progress") == saved
        await restarted.task
        assert restarted.setting("sync_progress")["baseline"] == saved["baseline"]
        assert restarted.setting("sync_progress")["targets"] == saved["targets"]
        assert restarted.sync["completed"] == restarted.sync["total"] == 3
        assert restarted.sync["progress_from_resume"] and restarted.sync["progress_ready"]
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_remaining_resume_counter_grows_with_discovery_without_legacy_provisional_sticking(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    await run_pass(sync, item_id)
    clock.value = clock.value.replace(hour=3)
    job = {"id": "old-incomplete-job", "date": clock.now().date().isoformat(), "phase": "first_pass",
           "item_ids": [item_id], "retry": True, "quality": "auto"}
    sync.set_setting("job", job)
    sync.set_setting("sync_progress", None)
    sync.set_setting("last_nightly", job["date"])
    runner.queue("metadata", uri("playlist", 100), metadata(track(1), track(2), complete=False),
                 metadata(*(track(number) for number in range(1, 5)), complete=False),
                 metadata(*(track(number) for number in range(1, 5))))
    seen = []

    async def observe(call):
        if call["command"] == "download":
            assert sync.sync["progress_ready"] and sync.sync["progress_from_resume"]
            seen.append((sync.sync["completed"], sync.sync["total"], sync.sync["total_provisional"]))

    runner.on_call = observe
    sync.start_job(existing=job)
    await sync.task
    assert seen == [(0, 1, True), (1, 3, True), (2, 3, True)]
    assert sync.sync["completed"] == sync.sync["total"] == 3
    assert sync.sync["progress_ready"] and sync.sync["progress_from_resume"] and not sync.sync["total_provisional"]
    assert sync.setting("sync_progress")["baseline"] == [uri("track", 1)]


@pytest.mark.asyncio
async def test_fresh_empty_membership_does_not_count_prior_failed_track(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    runner.queue("download", uri("track", 1), {"status": "error", "message": "Prior key failure"})
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["status"] == "failed"
    runner.metadata[uri("playlist", 100)] = metadata()
    before = len(runner.calls)
    await run_pass(sync, item_id)
    assert [call["command"] for call in runner.calls[before:]] == ["metadata"]
    assert rows(sync, item_id) == [] and sync.item(item_id)["status"] == "synced"
    assert sync.sync["completed"] == sync.sync["total"] == 0 and sync.sync["progress_ready"]
    assert sync.setting("sync_progress")["targets"] == []


@pytest.mark.asyncio
async def test_target_collection_after_first_metadata_does_not_seed_second_unrefreshed_item(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    sync.reconcile(second, metadata(track(2)), "prior-job")
    sync.db.execute("UPDATE tracks SET status='failed',message='Prior failure' WHERE item_id=?", (second,))
    runner.metadata[uri("playlist", 200)] = metadata()

    async def observe(call):
        if call["command"] == "metadata" and call["uri"] == uri("playlist", 200):
            assert sync.setting("sync_progress")["targets"] == [uri("track", 1)]
            assert sync.sync["total"] == 1 and not sync.sync["progress_ready"]
        if call["command"] == "download":
            assert sync.sync["total"] == 1 and sync.sync["progress_ready"]

    runner.on_call = observe
    await run_pass(sync)
    assert [call["command"] for call in runner.calls] == ["metadata", "metadata", "download"]
    assert rows(sync, first)[0]["status"] == "downloaded" and rows(sync, second) == []
    assert sync.sync["completed"] == sync.sync["total"] == 1


@pytest.mark.asyncio
async def test_failed_metadata_does_not_validate_stale_membership_for_fresh_targets(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(1))
    sync.reconcile(item_id, metadata(track(1)), "prior-job")
    sync.db.execute("UPDATE tracks SET status='failed' WHERE item_id=?", (item_id,))
    runner.queue("metadata", uri("playlist", 100), {"status": "error", "message": "Metadata unavailable"})
    runner.queue("download", uri("track", 1), {"status": "error", "message": "Old membership retry failed"})
    await run_pass(sync, item_id)
    assert rows(sync, item_id)[0]["membership_job"] == "prior-job"
    assert sync.setting("sync_progress")["targets"] == []
    assert sync.sync["total"] == sync.sync["completed"] == 0
    assert sync.sync["progress_ready"] and sync.sync["total_provisional"]


@pytest.mark.asyncio
async def test_partial_current_membership_does_not_count_omitted_prior_rows_or_lose_targets_after_failure(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    item_id = add_item(sync, runner, 100, track(2), complete=False)
    sync.reconcile(item_id, metadata(track(1)), "prior-job")
    sync.db.execute("UPDATE tracks SET status='failed' WHERE item_id=?", (item_id,))
    job = {"id": "current-job", "date": clock.now().date().isoformat(), "phase": "first_pass",
           "item_ids": [item_id], "retry": False, "quality": "auto"}
    sync.set_setting("job", job)
    sync.begin_sync_progress(job)
    assert await sync.refresh(item_id, job["id"])
    assert sync.setting("sync_progress")["targets"] == [uri("track", 2)]
    assert rows(sync, item_id)[0]["membership_job"] == "prior-job"
    runner.queue("metadata", uri("playlist", 100), {"status": "error", "message": "Later metadata failure"})
    assert await sync.refresh(item_id, job["id"])
    sync.update_sync_targets(job["id"], ready=True)
    assert sync.setting("sync_progress")["targets"] == [uri("track", 2)]
    assert sync.sync["total"] == 1 and sync.sync["completed"] == 0 and sync.sync["total_provisional"]
    sync.reconcile(item_id, metadata(), job["id"])
    assert sync.setting("sync_progress")["targets"] == [uri("track", 2)]
    assert rows(sync, item_id) == [] and sync.sync["total"] == 1


@pytest.mark.asyncio
async def test_membership_job_migration_preserves_complete_legacy_job_but_not_failed_refresh_rows(service):
    sync, runner, clock = service
    clock.value = clock.value.replace(hour=12)
    first = add_item(sync, runner, 100, track(1))
    second = add_item(sync, runner, 200, track(2))
    for item_id, number in ((first, 1), (second, 2)):
        sync.reconcile(item_id, metadata(track(number)), "legacy-job")
    sync.db.execute("UPDATE items SET complete=0,message='Metadata failure' WHERE id=?", (second,))
    sync.db.execute("ALTER TABLE tracks DROP COLUMN membership_job")
    job = {"id": "legacy-job", "date": clock.now().date().isoformat(), "phase": "retry",
           "item_ids": [first, second], "retry": True, "quality": "auto"}
    sync.set_setting("job", job)
    await sync.close()
    restarted = SyncService(sync.state_dir, sync.music_dir, runner=runner,
                            now=clock.now, clock=clock.time, sleeper=clock.sleep)
    try:
        assert rows(restarted, first)[0]["membership_job"] == "legacy-job"
        assert rows(restarted, second)[0]["membership_job"] is None
        restarted.begin_sync_progress(job, legacy=True)
        restarted.update_sync_targets(job["id"], ready=True)
        assert restarted.setting("sync_progress")["targets"] == [uri("track", 1)]
        assert restarted.sync["progress_from_resume"] and restarted.sync["total_provisional"]
    finally:
        await restarted.close()
