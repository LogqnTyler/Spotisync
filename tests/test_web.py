"""Local HTTP contract tests; no live workers, credentials, or scheduler races."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import threading
from unittest.mock import AsyncMock, Mock

import pytest
import requests
from fastapi.testclient import TestClient

from spotisync.core import SyncService, WorkerRunner
from spotisync.web import create_app


URI = "spotify:playlist:" + "1" * 22
BASE_URL = "http://127.0.0.1"


@pytest.fixture(autouse=True)
def public_get(monkeypatch):
    get = Mock(side_effect=AssertionError("Tests must never request live Spotify oEmbed"))
    monkeypatch.setattr(requests, "get", get)
    return get


@pytest.fixture
def web(tmp_path, monkeypatch):
    async def forbidden_worker(*args, **kwargs):
        pytest.fail("Tests must never invoke a live Spotify worker")

    monkeypatch.setattr(WorkerRunner, "run", forbidden_worker)
    runner = AsyncMock()
    runner.run.return_value = {"status": "ok", "username": "fake-user"}
    now = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)
    service = SyncService(tmp_path / "state", tmp_path / "music", runner=runner,
                          now=lambda: now, clock=lambda: now.timestamp(), sleeper=AsyncMock())

    async def startup_without_scheduler():
        service.ready = True

    monkeypatch.setattr(service, "startup", startup_without_scheduler)
    with TestClient(create_app(service=service), base_url=BASE_URL) as client:
        client.portal.call(lambda: service.scheduler)
        yield client, service, runner


def await_task(client, service):
    async def wait():
        if service.task:
            await asyncio.wait_for(service.task, timeout=3)

    client.portal.call(wait)


def test_initial_state_health_and_security_headers(web):
    client, service, runner = web
    response = client.get("/api/state")
    assert response.status_code == 200
    state = response.json()
    assert state["account"] == {"connected": False, "username": None, "connecting": False,
                                "auth_url": None, "message": None}
    assert state["settings"] == {"format": "ogg", "quality": "auto"}
    assert state["items"] == [] and not state["sync"]["running"]
    assert state["schedule"]["time"] == "03:00" and state["schedule"]["retry_until"] == "08:00"
    assert state["schedule"]["next_run"] == "2026-10-09T03:00:00+00:00"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert client.get("/api/health").json() == {"status": "ok"}
    runner.run.assert_not_awaited()


def test_thumbnail_endpoint_fetches_public_cover_without_auth_music_or_credentials(web, public_get, monkeypatch):
    client, service, runner = web
    item = client.post("/api/items", json={"uri": URI}).json()
    assert item["thumbnail_url"] is None and item["thumbnail_checked"] == 0
    cover = "https://mosaic.scdn.co/300/" + "a" * 160
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response._content = ('{"thumbnail_url":"' + cover + '","title":"Ignored"}').encode()
    public_get.side_effect = None
    public_get.return_value = response
    before = service.snapshot()

    def forbidden_setting(*args, **kwargs):
        pytest.fail("Thumbnail lookup must not read saved authentication settings")

    with monkeypatch.context() as patch:
        patch.setattr(service, "setting", forbidden_setting)
        result = client.get(f"/api/items/{item['id']}/thumbnail")
    assert result.status_code == 200 and result.json() == {"thumbnail_url": cover}
    assert result.headers["cache-control"] == "no-store"
    assert client.get(f"/api/items/{item['id']}/thumbnail").json() == {"thumbnail_url": cover}
    public_get.assert_called_once_with(
        "https://open.spotify.com/oembed?url=https%3A%2F%2Fopen.spotify.com%2Fplaylist%2F" + "1" * 22,
        timeout=(3, 8), allow_redirects=False)
    state = client.get("/api/state").json()
    assert state["items"][0]["thumbnail_url"] == cover
    assert state["items"][0]["name"] == item["name"]
    assert state["account"] == before["account"] and state["sync"] == before["sync"]
    assert state["items"][0]["track_count"] == 0 and state["items"][0]["status"] == "pending"
    assert list(service.music_dir.iterdir()) == []
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("unsafe", [None, "https://evil.example/cover", "https://i.scdn.co:443/cover",
                                    "https://user@i.scdn.co/cover", "http://i.scdn.co/cover"])
@pytest.mark.parametrize("endpoint", ["item", "media"])
def test_thumbnail_endpoint_negative_cache_and_no_open_proxy(web, public_get, unsafe, endpoint):
    client, service, runner = web
    item = client.post("/api/items", json={"uri": URI}).json()
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.status_code = 200
    response.headers = {"Content-Type": "application/json"}
    response.json.return_value = {"thumbnail_url": unsafe, "html": '<img src="https://evil.example">'}
    public_get.side_effect = None
    public_get.return_value = response
    path = (f"/api/items/{item['id']}/thumbnail?url=https://evil.example/private" if endpoint == "item" else
            "/api/media/thumbnail?uri=" + URI + "&url=https://evil.example/private")
    assert client.get(path).json() == {"thumbnail_url": None}
    assert client.get(path).json() == {"thumbnail_url": None}
    assert public_get.call_count == 1
    assert public_get.call_args.args[0].startswith("https://open.spotify.com/oembed?url=")
    assert "evil.example" not in public_get.call_args.args[0]
    assert client.get("/api/thumbnail?url=https://evil.example").status_code == 404
    assert client.get("/api/state").json()["items"][0]["status"] == "pending"
    runner.run.assert_not_awaited()


def test_thumbnail_endpoint_unknown_item_maps_to_400_and_bad_id_to_422(web, public_get):
    client, _, _ = web
    result = client.get("/api/items/999/thumbnail")
    assert result.status_code == 400 and result.json() == {"detail": "Tracked item not found."}
    assert client.get("/api/items/not-an-id/thumbnail").status_code == 422
    public_get.assert_not_called()


def test_thumbnail_csp_allows_only_exact_image_domains_and_local_connections(web):
    client, _, _ = web
    policy = client.get("/").headers["content-security-policy"]
    directives = dict(directive.strip().split(" ", 1) for directive in policy.split(";"))
    assert set(directives["img-src"].split()) == {
        "'self'", "data:", "https://i.scdn.co", "https://mosaic.scdn.co",
        "https://image-cdn-ak.spotifycdn.com", "https://image-cdn-fa.spotifycdn.com",
    }
    assert directives["connect-src"] == "'self'"
    assert directives["frame-ancestors"] == "'none'"


def test_thumbnail_endpoint_stays_available_during_original_sync(web, public_get):
    client, service, runner = web
    service.account["connected"] = True
    item = client.post("/api/items", json={"uri": URI}).json()

    async def hold_metadata(*args, **kwargs):
        await asyncio.Event().wait()

    runner.run.side_effect = hold_metadata
    assert client.post("/api/sync", json={}).status_code == 202
    assert client.get("/api/state").json()["sync"]["running"]
    public_get.side_effect = requests.Timeout("private-token")
    try:
        response = client.get(f"/api/items/{item['id']}/thumbnail")
        assert response.status_code == 200 and response.json() == {"thumbnail_url": None}
        state = client.get("/api/state").json()
        assert state["sync"]["running"] and state["account"]["connected"]
        runner.run.assert_awaited_once_with("metadata", uri=URI)
        assert list(service.music_dir.iterdir()) == []
    finally:
        client.post("/api/account/disconnect")


def test_media_thumbnail_endpoint_covers_failed_song_and_preserves_error_state(web, public_get):
    client, service, runner = web
    item = client.post("/api/items", json={"uri": URI}).json()
    song_uri = "spotify:track:" + "2" * 22
    local_uri = "spotify:local:Artist:Album:Song:1"
    service.reconcile(item["id"], {"complete": True, "tracks": [
        {"id": "2" * 22, "uri": song_uri, "name": "Beckoning", "availability": "unavailable"},
        {"id": local_uri, "uri": local_uri, "name": "Local song", "availability": "local"}]}, "seed")
    service.db.execute("UPDATE tracks SET status='failed',message='Audio-key failed',source_message='Backfill failed' WHERE uri=?",
                       (song_uri,))
    before = client.get("/api/state").json()
    cover = "https://image-cdn-fa.spotifycdn.com/image/album-cover"
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/json"
    response._content = ('{"thumbnail_url":"' + cover + '"}').encode()
    public_get.side_effect = None
    public_get.return_value = response
    result = client.get("/api/media/thumbnail", params={"uri": song_uri})
    assert result.status_code == 200 and result.json() == {"thumbnail_url": cover}
    assert result.headers["cache-control"] == "no-store"
    assert client.get("/api/media/thumbnail", params={"uri": song_uri}).json() == {"thumbnail_url": cover}
    assert client.get("/api/media/thumbnail", params={"uri": local_uri}).json() == {"thumbnail_url": None}
    public_get.assert_called_once_with(
        "https://open.spotify.com/oembed?url=https%3A%2F%2Fopen.spotify.com%2Ftrack%2F" + "2" * 22,
        timeout=(3, 8), allow_redirects=False)
    after = client.get("/api/state").json()
    errors = after["items"][0]["errors"]
    assert [(error["name"], error["uri"], error["thumbnail_url"], error["status"]) for error in errors] == [
        ("Beckoning", song_uri, cover, "failed"), ("Local song", local_uri, None, "unavailable"),
        ("Beckoning", song_uri, cover, "source")]
    assert [error["message"] for error in errors] == [error["message"] for error in before["items"][0]["errors"]]
    assert after["account"] == before["account"] and after["sync"] == before["sync"]
    assert after["items"][0]["failed_count"] == before["items"][0]["failed_count"] == 1
    assert not list(service.music_dir.rglob("*.ogg"))
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("value", ["spotify:track:" + "9" * 22, "spotify:album:" + "9" * 22,
    "spotify:playlist:" + "9" * 22, "spotify:local:Unknown:Album:Song:1", "spotify:track:short",
    "https://evil.example/track/" + "1" * 22, "https://open.spotify.com.evil.example/track/" + "1" * 22,
    "https://user@open.spotify.com/track/" + "1" * 22, "https://open.spotify.com:443/track/" + "1" * 22])
def test_media_thumbnail_endpoint_rejects_unknown_or_unsafe_media(web, public_get, value):
    client, _, runner = web
    result = client.get("/api/media/thumbnail", params={"uri": value})
    assert result.status_code == 400
    public_get.assert_not_called()
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("params", [{}, {"uri": ""}, {"uri": "x" * 2049}])
def test_media_thumbnail_endpoint_validates_required_query(web, public_get, params):
    client, _, _ = web
    assert client.get("/api/media/thumbnail", params=params).status_code == 422
    public_get.assert_not_called()


def test_connect_disconnect_state_and_fake_credentials_are_not_returned(web):
    client, service, runner = web
    for name in ("credentials.json", "api_credentials.json"):
        (service.state_dir / name).write_text('{"test_only": "fake-private-token"}')
    response = client.post("/api/account/connect", headers={"origin": BASE_URL})
    assert response.status_code == 202 and response.json() == {"status": "connecting"}
    await_task(client, service)
    state = client.get("/api/state")
    assert state.json()["account"]["connected"]
    assert state.json()["account"]["username"] == "fake-user"
    assert "fake-private-token" not in state.text
    assert service.setting("auth_valid") is True
    runner.run.assert_awaited_once()
    assert runner.run.await_args.args == ("auth",)
    response = client.post("/api/account/disconnect", headers={"origin": BASE_URL})
    assert response.status_code == 200 and response.json() == {"status": "ok"}
    assert not client.get("/api/state").json()["account"]["connected"]
    assert service.setting("auth_valid") is False and service.setting("username") is None
    assert not (service.state_dir / "credentials.json").exists()
    assert not (service.state_dir / "api_credentials.json").exists()
    runner.stop.assert_awaited()


def test_connect_failure_is_visible_without_marking_account_connected(web):
    client, service, runner = web
    runner.run.return_value = {"status": "auth_required", "message": "Fake login rejected"}
    assert client.post("/api/account/connect").status_code == 202
    await_task(client, service)
    account = client.get("/api/state").json()["account"]
    assert not account["connected"] and not account["connecting"]
    assert account["username"] is None and account["auth_url"] is None
    assert account["message"] == "Fake login rejected"
    assert service.setting("auth_valid") is False


def test_api_add_duplicate_remove_and_missing_item(web):
    client, service, runner = web
    response = client.post("/api/items", json={"uri": "https://open.spotify.com/playlist/" + "1" * 22 + "?si=fake"})
    assert response.status_code == 201
    item = response.json()
    assert item["uri"] == URI and item["kind"] == "playlist" and item["status"] == "pending"
    state_item = client.get("/api/state").json()["items"][0]
    assert state_item["id"] == item["id"] and state_item["incomplete"]
    assert state_item["track_count"] == state_item["downloaded_count"] == 0
    duplicate = client.post("/api/items", json={"uri": URI})
    assert duplicate.status_code == 400 and "already tracked" in duplicate.json()["detail"]
    assert client.delete(f"/api/items/{item['id']}").json() == {"status": "ok"}
    assert client.get("/api/state").json()["items"] == []
    missing = client.delete(f"/api/items/{item['id']}")
    assert missing.status_code == 400 and "not found" in missing.json()["detail"]
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("value", ["not spotify", "spotify:artist:" + "1" * 22,
                                    "https://evil.example/playlist/" + "1" * 22])
def test_api_rejects_invalid_urls(web, value):
    client, service, runner = web
    response = client.post("/api/items", json={"uri": value})
    assert response.status_code == 400
    assert client.get("/api/state").json()["items"] == []


@pytest.mark.parametrize("body", [{}, {"uri": ""}, {"uri": None}, {"uri": "x" * 2049}])
def test_api_media_schema_validation(web, body):
    client, service, runner = web
    assert client.post("/api/items", json=body).status_code == 422
    assert service.snapshot()["items"] == []


@pytest.mark.parametrize("fmt", ["ogg", "mp3", "aac"])
def test_api_format_setting(web, fmt):
    client, service, runner = web
    response = client.put("/api/settings", json={"format": fmt})
    assert response.status_code == 200 and response.json() == {"format": fmt, "quality": "auto"}
    assert client.get("/api/state").json()["settings"]["format"] == fmt
    assert service.setting("format") == fmt
    runner.run.assert_not_awaited()


def test_conversion_endpoint_queues_without_requiring_spotify(web, monkeypatch):
    client, service, runner = web
    calls = []
    monkeypatch.setattr(service, "start_conversion", lambda: calls.append(service.setting("format", "ogg")))
    client.put("/api/settings", json={"format": "aac"})
    response = client.post("/api/convert", json={})
    assert response.status_code == 202 and response.json() == {"status": "queued"}
    assert calls == ["aac"]
    assert not client.get("/api/state").json()["account"]["connected"]
    runner.run.assert_not_awaited()


def test_empty_conversion_completes_without_network_work(web):
    client, service, runner = web
    assert client.post("/api/convert", json={}).status_code == 202
    await_task(client, service)
    conversion = client.get("/api/state").json()["conversion"]
    assert not conversion["running"] and conversion["total"] == conversion["completed"] == conversion["failed"] == 0
    assert conversion["message"] == "Conversion finished."
    runner.run.assert_not_awaited()


def test_conversion_endpoint_requires_json_and_rejects_busy_jobs(web, monkeypatch):
    client, service, runner = web
    assert client.post("/api/convert").status_code == 415

    def busy():
        raise RuntimeError("Another job is already running.")

    monkeypatch.setattr(service, "start_conversion", busy)
    response = client.post("/api/convert", json={})
    assert response.status_code == 409
    assert response.json()["detail"] == "Another job is already running."
    runner.run.assert_not_awaited()


def test_requested_heading_conversion_button_and_brand_color(web):
    client, _, _ = web
    page = client.get("/").text
    header = page.split("<header", 1)[1].split("</header>", 1)[0]
    assert 'id="account-status"' in header and 'id="connect-button"' in header and 'id="disconnect-button"' in header
    assert header.index('class="brand"') < header.index('class="account-panel"')
    assert page.count('id="account-status"') == 1
    assert 'id="sync-progress"' in page and 'id="sync-counter"' in page
    assert "Converts successful downloads offline" in page
    assert '<h1 id="intro-title">Track Spotify playlists, albums, and tracks in one place.</h1>' in page
    assert "Convert existing songs" in page and ".spotisync-originals" in page
    assert 'id="quality-select"' in page
    for label in ("Auto (best available)", "Normal (96 kbps)", "High (160 kbps)", "Very high (320 kbps, Premium)"):
        assert label in page
    assert "Less managing" not in page and "Kept close" not in page and "status-dot" not in page
    assert "Your local library" not in page and "<footer" not in page and "One account. Your own collection." not in page
    assert '<meta name="color-scheme" content="only light">' in page
    assert '<meta name="darkreader-lock">' in page
    assert '/static/style.css?v=' in page and '/static/app.js?v=' in page
    styles = client.get("/static/style.css").text
    assert "--green: #1ed760;" in styles
    assert "color-scheme: only light;" in styles and "--bg: #ffffff;" in styles
    assert ".source-art img" in styles and ".track-error .error-art" in styles
    assert "font-family: Georgia" not in styles


@pytest.mark.parametrize("fmt", ["flac", "OGG", "", "wav"])
def test_api_invalid_format_does_not_change_setting(web, fmt):
    client, service, runner = web
    response = client.put("/api/settings", json={"format": fmt})
    assert response.status_code == 400
    assert response.json()["detail"] == "Choose ogg, mp3, or aac."
    assert client.get("/api/state").json()["settings"]["format"] == "ogg"


def test_sync_api_requires_connection_and_items(web):
    client, service, runner = web
    response = client.post("/api/sync", json={})
    assert response.status_code == 409 and "Connect Spotify" in response.json()["detail"]
    assert client.post("/api/account/connect").status_code == 202
    await_task(client, service)
    response = client.post("/api/sync", json={})
    assert response.status_code == 400 and "Add a media item" in response.json()["detail"]
    assert client.post("/api/sync", json={"item_id": 999}).status_code == 400
    for body in ({"item_id": 0}, {"item_id": -1}, {"item_id": "bad"}):
        assert client.post("/api/sync", json=body).status_code == 422


def test_sync_api_queues_selected_item_and_reports_finished_state(web):
    client, service, runner = web
    runner.run.return_value = {"status": "ok", "name": "Fake empty playlist", "complete": True, "tracks": []}
    service.account["connected"] = True
    first = client.post("/api/items", json={"uri": URI}).json()
    second_uri = "spotify:playlist:" + "2" * 22
    second = client.post("/api/items", json={"uri": second_uri}).json()
    response = client.post("/api/sync", json={"item_id": first["id"]})
    assert response.status_code == 202 and response.json() == {"status": "queued"}
    await_task(client, service)
    items = client.get("/api/state").json()["items"]
    assert items[0]["status"] == "synced" and not items[0]["incomplete"]
    assert items[1]["id"] == second["id"] and items[1]["status"] == "pending"
    runner.run.assert_awaited_once_with("metadata", uri=URI)
    assert not client.get("/api/state").json()["sync"]["running"]


def test_busy_api_rejects_overlap_connect_and_remove(web):
    client, service, runner = web
    service.account["connected"] = True
    item = client.post("/api/items", json={"uri": URI}).json()

    async def hold_metadata(*args, **kwargs):
        await asyncio.Event().wait()

    runner.run.side_effect = hold_metadata
    assert client.post("/api/sync", json={}).status_code == 202
    assert client.get("/api/state").json()["sync"]["running"]
    assert client.post("/api/sync", json={}).status_code == 409
    assert client.post("/api/account/connect").status_code == 409
    assert client.delete(f"/api/items/{item['id']}").status_code == 409
    assert client.post("/api/account/disconnect").status_code == 200
    assert not client.get("/api/state").json()["sync"]["running"]
    assert len(client.get("/api/state").json()["items"]) == 1


@pytest.mark.parametrize("method,path,body", [("POST", "/api/items", {"uri": URI}),
                                               ("POST", "/api/sync", {}),
                                               ("POST", "/api/convert", {}),
                                              ("PUT", "/api/settings", {"format": "mp3"}),
                                              ("POST", "/api/account/connect", None),
                                              ("POST", "/api/account/disconnect", None),
                                              ("DELETE", "/api/items/1", None)])
@pytest.mark.parametrize("headers", [{"origin": "https://evil.example"},
                                    {"origin": "http://localhost"},
                                    {"origin": "null"},
                                    {"sec-fetch-site": "cross-site"},
                                    {"origin": BASE_URL, "sec-fetch-site": "cross-site"}])
def test_cross_origin_mutations_are_rejected_before_side_effects(web, method, path, body, headers):
    client, service, runner = web
    service.add(URI)
    response = client.request(method, path, json=body, headers=headers)
    assert response.status_code == 403
    assert response.json()["detail"] == "Cross-origin requests are not allowed."
    assert len(service.snapshot()["items"]) == 1
    assert service.setting("format", "ogg") == "ogg"
    assert service.task is None
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("headers", [{}, {"origin": BASE_URL},
                                    {"origin": BASE_URL, "sec-fetch-site": "same-origin"}])
def test_same_origin_or_nonbrowser_json_mutations_are_allowed(web, headers):
    client, service, runner = web
    assert client.post("/api/items", json={"uri": URI}, headers=headers).status_code == 201


@pytest.mark.parametrize("host", ["evil.example", "127.0.0.1.evil.example", "192.168.1.2", "localhost.evil.example"])
def test_untrusted_host_cannot_read_or_mutate(web, host):
    client, service, runner = web
    headers = {"host": host}
    assert client.get("/api/state", headers=headers).status_code == 400
    assert client.get("/api/media/thumbnail", params={"uri": URI}, headers=headers).text == "Invalid host header"
    assert client.post("/api/items", json={"uri": URI}, headers=headers).status_code == 400
    assert service.snapshot()["items"] == []


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "127.0.0.1:8000", "[::1]"])
def test_loopback_hosts_are_allowed(web, host):
    client, service, runner = web
    assert client.get("/api/state", headers={"host": host}).status_code == 200


@pytest.mark.parametrize("method,path", [("POST", "/api/items"), ("POST", "/api/sync"), ("PUT", "/api/settings")])
@pytest.mark.parametrize("content_type", [None, "text/plain", "application/x-www-form-urlencoded"])
def test_json_mutation_endpoints_reject_other_content_types(web, method, path, content_type):
    client, service, runner = web
    headers = {"content-type": content_type} if content_type else {}
    response = client.request(method, path, content='{"uri":"' + URI + '"}', headers=headers)
    assert response.status_code == 415 and response.json()["detail"] == "Use application/json."
    assert service.snapshot()["items"] == []
    runner.run.assert_not_awaited()


def test_json_charset_is_allowed_but_malformed_json_is_rejected(web):
    client, service, runner = web
    response = client.post("/api/items", content='{"uri":"' + URI + '"}',
                           headers={"content-type": "application/json; charset=utf-8"})
    assert response.status_code == 201
    assert client.put("/api/settings", content="{", headers={"content-type": "application/json"}).status_code == 422


def test_readonly_cross_origin_request_does_not_receive_cors_permission(web):
    client, service, runner = web
    response = client.get("/api/state", headers={"origin": "https://evil.example"})
    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers
    assert response.headers["cache-control"] == "no-store"


def test_disconnect_http_overlap_excludes_auth_sync_and_remove_until_cleanup_finishes(web):
    client, service, runner = web
    service.account["connected"] = True
    item = client.post("/api/items", json={"uri": URI}).json()
    stopping = threading.Event()
    release = asyncio.Event()

    async def hold_metadata(*args, **kwargs):
        await asyncio.Event().wait()

    async def hold_stop():
        stopping.set()
        await release.wait()

    runner.run.side_effect = hold_metadata
    runner.stop.side_effect = hold_stop
    assert client.post("/api/sync", json={}).status_code == 202
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.post, "/api/account/disconnect")
        assert stopping.wait(3)
        second = pool.submit(client.post, "/api/account/disconnect")
        try:
            assert service.task.done()
            assert client.post("/api/account/connect").status_code == 409
            assert client.post("/api/sync", json={}).status_code == 409
            assert client.delete(f"/api/items/{item['id']}").status_code == 409
            assert runner.stop.await_count == 1
        finally:
            client.portal.call(release.set)
        assert first.result(timeout=3).status_code == 200
        assert second.result(timeout=3).status_code == 200
    assert not client.get("/api/state").json()["account"]["connected"]
    runner.run.side_effect = None
    assert client.post("/api/account/connect").status_code == 202
    await_task(client, service)
    assert client.get("/api/state").json()["account"]["connected"]


def test_manual_item_http_request_starts_all_due_nightly_media_inside_window(web):
    client, service, runner = web
    now = datetime(2026, 10, 8, 3, tzinfo=timezone.utc)
    service.now = lambda: now
    service.clock = lambda: now.timestamp()
    service.account["connected"] = True
    runner.run.return_value = {"status": "ok", "name": "Fake empty playlist", "complete": True, "tracks": []}
    first = client.post("/api/items", json={"uri": URI}).json()
    second_uri = "spotify:playlist:" + "2" * 22
    client.post("/api/items", json={"uri": second_uri})
    assert client.post("/api/sync", json={"item_id": first["id"]}).status_code == 202
    await_task(client, service)
    assert [call.kwargs["uri"] for call in runner.run.await_args_list] == [URI, second_uri]
    assert all(item["status"] == "synced" for item in client.get("/api/state").json()["items"])
    assert service.setting("last_nightly") == now.date().isoformat()


@pytest.mark.parametrize("quality", ["auto", "normal", "high", "very_high"])
def test_api_quality_partial_update_preserves_format_and_returns_both_settings(web, quality):
    client, service, runner = web
    assert client.put("/api/settings", json={"format": "mp3"}).json() == {"format": "mp3", "quality": "auto"}
    response = client.put("/api/settings", json={"quality": quality})
    assert response.status_code == 200 and response.json() == {"format": "mp3", "quality": quality}
    assert client.get("/api/state").json()["settings"] == {"format": "mp3", "quality": quality}
    assert service.setting("quality") == quality
    assert client.put("/api/settings", json={"format": "aac"}).json() == {"format": "aac", "quality": quality}
    assert not service.account["connected"] and service.task is None
    assert not list(service.music_dir.iterdir())
    runner.run.assert_not_awaited()


def test_api_settings_dual_update_is_validated_before_either_setting_changes(web):
    client, service, runner = web
    assert client.put("/api/settings", json={"format": "aac", "quality": "high"}).json() == {"format": "aac", "quality": "high"}
    response = client.put("/api/settings", json={"format": "mp3", "quality": "invalid"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Choose auto, normal, high, or very_high source quality."
    assert client.get("/api/state").json()["settings"] == {"format": "aac", "quality": "high"}
    assert client.put("/api/settings", json={"format": "invalid", "quality": "normal"}).status_code == 400
    assert client.get("/api/state").json()["settings"] == {"format": "aac", "quality": "high"}
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("quality", ["", "HIGH", "very-high", "veryHigh", "high ", "96", "320", None])
def test_api_quality_enum_rejects_invalid_values_without_silent_downgrade(web, quality):
    client, service, runner = web
    assert client.put("/api/settings", json={"quality": "very_high"}).status_code == 200
    response = client.put("/api/settings", json={"quality": quality})
    assert response.status_code == 400
    assert response.json()["detail"] == "Choose auto, normal, high, or very_high source quality."
    assert client.get("/api/state").json()["settings"] == {"format": "ogg", "quality": "very_high"}
    runner.run.assert_not_awaited()


@pytest.mark.parametrize("body", [{"quality": 160}, {"quality": True}, {"quality": []}, {"quality": {}},
    {"format": 1}, {"format": True}, {"format": "ogg", "unknown": "high"}, {"unknown": "high"}])
def test_api_partial_settings_schema_validation(web, body):
    client, _, runner = web
    assert client.put("/api/settings", json=body).status_code == 422
    assert client.get("/api/state").json()["settings"] == {"format": "ogg", "quality": "auto"}
    runner.run.assert_not_awaited()


def test_api_partial_settings_requires_at_least_one_field_and_rejects_null_format(web):
    client, _, runner = web
    response = client.put("/api/settings", json={})
    assert response.status_code == 400 and response.json()["detail"] == "Provide format or quality."
    assert client.put("/api/settings", json={"format": None, "quality": "high"}).status_code == 400
    assert client.get("/api/state").json()["settings"] == {"format": "ogg", "quality": "auto"}
    runner.run.assert_not_awaited()


def test_settings_quality_change_is_lazy_and_preserves_existing_playback_and_artwork(web, public_get):
    client, service, runner = web
    item = service.add(URI)
    song_uri = "spotify:track:" + "2" * 22
    cover = "https://i.scdn.co/quality-settings-cover"
    service.reconcile(item["id"], {"complete": True, "thumbnail_url": cover, "tracks": [
        {"id": "2" * 22, "uri": song_uri, "name": "Saved song", "availability": "available", "thumbnail_url": cover}]}, "seed")
    service.db.execute("UPDATE tracks SET status='downloaded',format='aac',source_quality='auto' WHERE item_id=?", (item["id"],))
    before = dict(service.db.execute("SELECT * FROM tracks").fetchone())
    response = client.put("/api/settings", json={"quality": "very_high"})
    assert response.status_code == 200 and response.json() == {"format": "ogg", "quality": "very_high"}
    assert dict(service.db.execute("SELECT * FROM tracks").fetchone()) == before
    state = client.get("/api/state").json()
    assert state["items"][0]["thumbnail_url"] == cover
    assert state["items"][0]["downloaded_count"] == 1 and state["items"][0]["pending_quality_count"] == 1
    assert not list(service.db.execute("SELECT * FROM sources"))
    assert not list(service.db.execute("SELECT * FROM source_versions"))
    assert service.task is None
    runner.run.assert_not_awaited()
    public_get.assert_not_called()


def test_empty_conversion_reports_snapshotted_quality_without_requiring_spotify(web):
    client, _, runner = web
    client.put("/api/settings", json={"format": "aac", "quality": "high"})
    assert client.post("/api/convert", json={}).status_code == 202
    await_task(client, client.app.state.service)
    conversion = client.get("/api/state").json()["conversion"]
    assert conversion["quality"] == "high" and conversion["format"] == "aac"
    assert conversion["completed"] == conversion["failed"] == conversion["total"] == 0
    runner.run.assert_not_awaited()
