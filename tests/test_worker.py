"""Offline protocol and integration checks against pinned upstream libraries."""

import base64
import contextlib
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
from itertools import product
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit
from urllib.request import urlopen
from urllib.error import HTTPError

from spotisync import worker


TRACK_ID = "0" * 21 + "1"
ALBUM_ID = "0" * 21 + "2"
URI = "spotify:track:" + TRACK_ID


class WorkerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.staging = self.root / "stage"
        self.staging.mkdir()
        self.api = worker.initialize(self.state, self.staging)
        from librespot.proto import Metadata_pb2 as md
        self.md = md
        self.track = md.Track(gid=bytes.fromhex("00" * 15 + "01"), name="Song", duration=1000,
                              number=1, disc_number=1, explicit=False, popularity=0)
        self.track.album.CopyFrom(md.Album(gid=bytes.fromhex("00" * 15 + "02"), name="Album"))
        self.track.artist.add(name="Artist")
        self.track.file.add(file_id=b"f" * 20, format=md.AudioFile.OGG_VORBIS_160)
        self.album = md.Album(gid=self.track.album.gid, name="Album", label="Label", genre=["Genre"])
        self.album.artist.add(name="Artist")
        self.album.date.year = 2024
        self.album.disc.add(number=1).track.add().CopyFrom(self.track)
        image = self.album.cover_group.image.add(file_id=b"i" * 20, width=10, height=10)
        image.size = md.Image.DEFAULT
        self.album.copyright.add(text="Copyright")
        self.session = MagicMock()
        self.session.country_code = "US"
        self.session.get_user_attribute.return_value = "premium"
        self.session.credentials.return_value = {"username": "user", "type": "AUTHENTICATION_STORED_SPOTIFY_CREDENTIALS",
                                                "credentials": base64.b64encode(b"secret").decode()}
        self.session.api().get_metadata_4_track.return_value = self.track
        self.session.api().get_metadata_4_album.return_value = self.album

    def test_uri_validation(self):
        self.assertEqual(worker.normalize_uri("https://open.spotify.com/intl-en/track/" + TRACK_ID + "?si=x"), URI)
        for value in (None, "track:" + TRACK_ID, "https://evil.example/track/" + TRACK_ID,
                      "spotify:episode:" + TRACK_ID, URI + ":extra"):
            with self.subTest(value=value), self.assertRaises(worker.WorkerFailure):
                worker.normalize_uri(value)
        with self.assertRaises(worker.WorkerFailure):
            worker.normalize_uri("spotify:album:" + ALBUM_ID, track_only=True)

    def test_config_isolated_no_sidecars_no_retries(self):
        from zotify.config import Config
        self.assertEqual(Config.get_root_path(), self.staging)
        self.assertEqual(Config.get_root_podcast_path(), self.staging)
        self.assertEqual(Config.get_retry_attempts(), 0)
        self.assertFalse(Config.get_lyrics_to_file())
        self.assertFalse(Config.get_lyrics_to_metadata())
        self.assertFalse(Config.get_album_art_to_file())
        self.assertFalse(Config.get_export_m3u8())
        self.assertFalse(any(self.staging.iterdir()))
        conf = worker._builder(self.state).conf
        self.assertFalse(conf.cache_enabled)
        self.assertFalse(conf.store_credentials)
        self.assertFalse(conf.retry_on_chunk_error)

    def playlist(self):
        from librespot.proto.Playlist4External_pb2 import SelectedListContent
        playlist = SelectedListContent(revision=b"rev", length=4, owner_username="owner")
        playlist.attributes.name = "Playlist"
        playlist.contents.pos = 0
        playlist.contents.truncated = False
        for uri in (URI, "spotify:local:Artist:Album:Local%20Song:3", "spotify:episode:" + TRACK_ID, ""):
            playlist.contents.items.add(uri=uri)
        self.session.api().get_playlist.return_value = playlist
        return playlist

    def test_playlist_raw_members_preserved(self):
        self.playlist()
        result = worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
        self.assertFalse(result["complete"])
        self.assertEqual(result["revision"], b"rev".hex())
        self.assertEqual(len(result["tracks"]), 4)
        local = result["tracks"][1]
        self.assertEqual(local["id"], local["uri"])
        self.assertEqual(local["availability"], "local")
        self.assertEqual(local["name"], "Local Song")
        self.assertEqual(local["duration_ms"], 3000)
        self.assertEqual(result["tracks"][2]["availability"], "unknown")
        self.assertTrue(result["tracks"][3]["id"].startswith("spotify:unresolved:"))
        self.assertEqual(result, worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID))

    def test_playlist_complete_requires_all_evidence(self):
        for change in (lambda p: setattr(p.contents, "pos", 1),
                       lambda p: setattr(p.contents, "truncated", True),
                       lambda p: setattr(p, "length", 5), lambda p: p.ClearField("length"),
                       lambda p: p.ClearField("revision"), lambda p: p.contents.ClearField("pos"),
                       lambda p: p.contents.ClearField("truncated"), lambda p: setattr(p, "multiple_heads", True)):
            playlist = self.playlist()
            change(playlist)
            self.assertFalse(worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)["complete"])

    def test_empty_playlist_complete(self):
        playlist = self.playlist()
        del playlist.contents.items[:]
        playlist.length = 0
        result = worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
        self.assertTrue(result["complete"])
        self.assertEqual(result["tracks"], [])

    def test_member_fetch_failure_does_not_erase_membership(self):
        self.playlist().contents.items[-1].uri = URI
        self.session.api().get_metadata_4_track.side_effect = RuntimeError("token secret")
        result = worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
        self.assertTrue(result["complete"])
        self.assertEqual(result["tracks"][0]["id"], TRACK_ID)
        self.assertEqual(result["tracks"][0]["availability"], "unknown")
        self.assertNotIn("secret", json.dumps(result))

    def test_duplicate_members_preserved_fetch_cached(self):
        playlist = self.playlist()
        playlist.contents.items[1].uri = URI
        result = worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
        self.assertEqual(result["tracks"][0], result["tracks"][1])
        self.session.api().get_metadata_4_track.assert_called_once()

    def test_album_and_track(self):
        album = worker.metadata_result(self.session, "spotify:album:" + ALBUM_ID)
        self.assertTrue(album["complete"])
        self.assertEqual(album["tracks"][0]["id"], TRACK_ID)
        self.assertEqual(len(album["revision"]), 64)
        track = worker.metadata_result(self.session, URI)
        self.assertTrue(track["complete"])
        self.assertEqual(track["kind"], "track")

    def test_thumbnail_cover_uses_widest_valid_binary_id_without_requests(self):
        self.album.cover_group.image.add(file_id=b"large" * 4, width=640)
        self.album.cover_group.image.add(file_id=b"invalid", width=1000)
        self.track.album.cover_group.CopyFrom(self.album.cover_group)
        expected = "https://i.scdn.co/image/" + (b"large" * 4).hex()
        with (patch("requests.sessions.Session.send", side_effect=AssertionError("HTTP forbidden")),
              patch.object(worker, "_raw", wraps=worker._raw) as raw):
            self.assertEqual(worker._thumbnail_url(self.album), expected)
            self.assertEqual(worker._track_row(URI, self.track, self.session)["thumbnail_url"], expected)
            raw.assert_not_called()
            for uri in ("spotify:album:" + ALBUM_ID, URI):
                result = worker.metadata_result(self.session, uri)
                self.assertTrue(result["complete"])
                self.assertEqual(result["thumbnail_url"], expected)
                self.assertEqual(result["tracks"][0]["thumbnail_url"], expected)
        self.assertEqual(raw.call_count, 3)  # Existing album + member + track metadata only.

    def test_thumbnail_missing_or_invalid_ids_do_not_change_membership_or_status(self):
        playlist = self.playlist()
        playlist.contents.items[-1].uri = URI
        for picture in (b"p" * 20, b"", b"short", b"x" * 40, b"spotify:image:secret"):
            with self.subTest(picture=picture):
                playlist.attributes.picture = picture
                result = worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
                self.assertEqual(result["status"], "ok")
                self.assertTrue(result["complete"])
                self.assertEqual(result["revision"], b"rev".hex())
                expected = "https://i.scdn.co/image/" + picture.hex() if len(picture) == 20 else None
                self.assertEqual(result["thumbnail_url"], expected)
        for proto in (None, SimpleNamespace(), SimpleNamespace(cover_group=SimpleNamespace(image=[])),
                      SimpleNamespace(cover_group=SimpleNamespace(image=[SimpleNamespace(file_id="x" * 20)])),
                      SimpleNamespace(attributes=SimpleNamespace(picture="x" * 20))):
            self.assertIsNone(worker._thumbnail_url(proto))
        self.album.cover_group.Clear()
        self.album.cover_group.image.add(file_id=b"bad", width=1000)
        self.track.album.cover_group.CopyFrom(self.album.cover_group)
        for uri in ("spotify:album:" + ALBUM_ID, URI):
            result = worker.metadata_result(self.session, uri)
            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["complete"])
            self.assertIsNone(result["thumbnail_url"])
            self.assertIsNone(result["tracks"][0]["thumbnail_url"])
        self.assertIsNone(worker._track_row(URI)["thumbnail_url"])
        self.assertIsNone(worker._track_row("spotify:local:A:B:C:1")["thumbnail_url"])

    def test_availability_is_conservative(self):
        del self.track.file[:]
        self.assertEqual(worker._availability(self.track, self.session), "unknown")
        restriction = self.track.restriction.add(countries_forbidden="US", catalogue_str=["premium"])
        self.assertEqual(worker._availability(self.track, self.session), "unavailable")
        restriction.countries_forbidden = "GB"
        self.assertEqual(worker._availability(self.track, self.session), "unknown")
        restriction.countries_allowed = "GB"
        self.assertEqual(worker._availability(self.track, self.session), "unavailable")
        self.track.alternative.add(gid=b"a" * 16)
        self.assertEqual(worker._availability(self.track, self.session), "unknown")

    def test_actual_librespot_country_storage(self):
        self.session.country_code = "EN"
        self.session._Receiver__country_code = "US"
        self.track.restriction.add(countries_forbidden="US")
        self.assertEqual(worker._availability(self.track, self.session), "unavailable")

    def test_local_download_unavailable_without_auth(self):
        with patch.object(worker, "saved_session") as saved, self.assertRaises(worker.WorkerFailure) as caught:
            worker.execute({"command": "download", "state_dir": str(self.state),
                            "uri": "spotify:local:Artist:Album:Song:1", "output_dir": str(self.staging)}, lambda e: None)
        self.assertEqual(caught.exception.status, "unavailable")
        saved.assert_not_called()

    def test_saved_auth_never_falls_back(self):
        with patch.object(worker, "authenticate") as auth, patch.object(worker, "_builder") as builder:
            with self.assertRaises(worker.WorkerFailure) as caught:
                worker.execute({"command": "probe", "state_dir": str(self.state)}, lambda e: None)
            self.assertEqual(caught.exception.status, "auth_required")
            auth.assert_not_called()
            builder.assert_not_called()

    def test_credentials_secure_and_saved_login(self):
        worker._save_credentials(self.state, self.session)
        path = self.state / "credentials.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(self.state.iterdir()), [path])
        with patch.object(worker, "_builder") as builder:
            builder.return_value.stored.return_value.create.return_value = self.session
            self.assertIs(worker.saved_session(self.state), self.session)
            encoded = builder.return_value.stored.call_args.args[0]
            self.assertEqual(json.loads(base64.b64decode(encoded)), self.session.credentials())

    def test_probe_closes_session(self):
        with patch.object(worker, "saved_session", return_value=self.session):
            result = worker.execute({"command": "probe", "state_dir": str(self.state)}, lambda e: None)
        self.assertEqual(result, {"status": "ok", "username": "user"})
        self.session.close.assert_called_once()
        self.assertEqual(self.state.stat().st_mode & 0o777, 0o700)

    def test_download_quality_validation_protocol_before_account_or_filesystem_access(self):
        for quality in (None, "", "HIGH", "very-high", "high ", "320", 160, True, [], {"token": "secret"}):
            command = {"command": "download", "state_dir": str(self.state), "uri": URI,
                       "output_dir": str(self.staging), "format": "ogg", "quality": quality}
            stdout, stderr = io.StringIO(), io.StringIO()
            with (self.subTest(quality=quality), patch.object(worker, "initialize") as initialize,
                  patch.object(worker, "saved_session") as saved, patch.object(worker, "authenticate") as auth,
                  patch.object(worker, "_http_scope") as http, patch.object(worker, "_accesspoint_scope") as accesspoints,
                  patch("sys.stdin", io.StringIO(json.dumps(command))), contextlib.redirect_stdout(stdout),
                  contextlib.redirect_stderr(stderr)):
                worker.main()
            self.assertEqual(json.loads(stdout.getvalue()), {"event": "result", "status": "error",
                             "message": "quality must be auto, normal, high, or very_high."})
            for mock in (initialize, saved, auth, http, accesspoints):
                mock.assert_not_called()
            self.assertEqual(stderr.getvalue(), "")
            self.assertFalse(list(self.state.iterdir()))
            self.assertFalse(list(self.staging.iterdir()))

    def test_execute_download_forwards_quality_and_default_config(self):
        from zotify.config import Config

        for quality in (None, "auto", "normal", "high", "very_high"):
            command = {"command": "download", "state_dir": str(self.state), "uri": URI,
                       "output_dir": str(self.staging), "format": "ogg"}
            if quality is not None:
                command["quality"] = quality
            expected = quality or "auto"
            with (self.subTest(quality=quality), patch.object(worker, "saved_session", return_value=self.session),
                  patch.object(worker, "download_result", return_value={"status": "ok", "quality": "high"}) as download):
                self.assertEqual(worker.execute(command, lambda e: None), {"status": "ok", "quality": "high"})
            self.assertEqual(download.call_args.args[-2:], ("ogg", expected))
            self.assertEqual(Config.Values["DOWNLOAD_QUALITY"], expected)
        worker.initialize(self.state, self.staging)
        self.assertEqual(Config.Values["DOWNLOAD_QUALITY"], "auto")

    def ap_response(self, addresses):
        import requests

        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({"accesspoint": addresses}).encode()
        response._content_consumed = True
        return response

    def test_accesspoint_failover_prefers_443_preserves_choices_and_restores_scope(self):
        from librespot.core import ApResolver, Session

        original_resolve = ApResolver.get_random_accesspoint
        original_create = Session.ConnectionHolder.create
        addresses = ["legacy.invalid:4070", "first.invalid:443", "plain.invalid:80",
                     "FIRST.invalid:443", "second.invalid:443"]
        sock = MagicMock()
        with (patch("requests.get", return_value=self.ap_response(addresses)) as resolver,
              patch.object(socket, "create_connection", side_effect=[ConnectionRefusedError("secret"),
                           TimeoutError("secret"), OSError("secret"), sock]) as connect):
            with worker._accesspoint_scope():
                address = ApResolver.get_random_accesspoint()
                self.assertEqual(address, "first.invalid:443")
                connection = Session.ConnectionHolder.create(address, None)
                sock.settimeout.assert_called_once_with(None)
                connection.close()
            resolver.assert_called_once_with(ApResolver.base_url + "?type=accesspoint", timeout=10.0,
                                             allow_redirects=False)
        self.assertEqual([call.args[0] for call in connect.call_args_list],
                         [("first.invalid", 443), ("second.invalid", 443),
                          ("legacy.invalid", 4070), ("plain.invalid", 80)])
        self.assertTrue(all(0 < call.kwargs["timeout"] <= 10 for call in connect.call_args_list))
        sock.close.assert_called_once()
        self.assertIs(ApResolver.get_random_accesspoint, original_resolve)
        self.assertIs(Session.ConnectionHolder.create, original_create)

    def test_accesspoint_failed_socket_cleanup_before_next_endpoint(self):
        from librespot.core import ApResolver, Session

        failed, good = MagicMock(), MagicMock()
        failed.connect.side_effect = ConnectionRefusedError("secret")
        address_info = [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.1", 443))]
        with (patch("requests.get", return_value=self.ap_response(["first.invalid:443", "next.invalid:443"])),
              patch.object(socket, "getaddrinfo", return_value=address_info),
              patch.object(socket, "socket", side_effect=[failed, good]), worker._accesspoint_scope()):
            connection = Session.ConnectionHolder.create(ApResolver.get_random_accesspoint(), None)
            failed.close.assert_called_once()
            good.close.assert_not_called()
            self.assertEqual(good.settimeout.call_args.args, (None,))
            connection.close()
        good.close.assert_called_once()

    def test_accesspoint_socket_cleanup_when_restoring_blocking_fails(self):
        from librespot.core import ApResolver, Session

        failed, good = MagicMock(), MagicMock()
        failed.settimeout.side_effect = OSError("secret")
        with (patch("requests.get", return_value=self.ap_response(["first.invalid:443", "next.invalid:443"])),
              patch.object(socket, "create_connection", side_effect=[failed, good]), worker._accesspoint_scope()):
            connection = Session.ConnectionHolder.create(ApResolver.get_random_accesspoint(), None)
            failed.close.assert_called_once()
            good.settimeout.assert_called_once_with(None)
            connection.close()

    def test_accesspoint_all_fail_safe_distinct_attempts(self):
        from librespot.core import ApResolver, Session

        class UnsafeError(OSError):
            def __str__(self):
                raise AssertionError("Exception text must not be read")

        stdout, stderr = io.StringIO(), io.StringIO()
        with (patch("requests.get", return_value=self.ap_response(["first.invalid:443", "first.invalid:443",
                    "legacy.invalid:4070"])) as resolver,
              patch.object(socket, "create_connection", side_effect=UnsafeError("Bearer secret")) as connect,
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr),
              worker._accesspoint_scope(), self.assertRaises(worker.WorkerFailure) as caught):
            Session.ConnectionHolder.create(ApResolver.get_random_accesspoint(), None)
        self.assertEqual(caught.exception.result(), {"status": "error", "message":
                         "Could not connect to Spotify access points; retry later."})
        self.assertEqual(connect.call_count, 2)
        resolver.assert_called_once()
        self.assertEqual(stdout.getvalue() + stderr.getvalue(), "")

    def test_accesspoint_total_budget_includes_lookup_and_limits_attempts(self):
        from librespot.core import ApResolver, Session

        clock = [0.0]

        def resolve(*args, **kwargs):
            clock[0] += 3
            return self.ap_response([f"ap{index}.invalid:443" for index in range(8)])

        def connect(endpoint, timeout):
            clock[0] += timeout
            raise TimeoutError("secret")

        with (patch("requests.get", side_effect=resolve) as resolver,
              patch.object(worker.time, "monotonic", side_effect=lambda: clock[0]),
              patch.object(socket, "create_connection", side_effect=connect) as connections,
              worker._accesspoint_scope(), self.assertRaises(worker.WorkerFailure) as caught):
            Session.ConnectionHolder.create(ApResolver.get_random_accesspoint(), None)
        self.assertEqual(caught.exception.status, "error")
        self.assertEqual([call.kwargs["timeout"] for call in connections.call_args_list], [10.0, 10.0, 7.0])
        self.assertEqual(clock[0], 30)
        resolver.assert_called_once()

    def test_accesspoint_invalid_pool_and_lookup_failure_never_connect(self):
        from librespot.core import ApResolver
        import requests

        pools = [None, [], {}, {"accesspoint": "host.invalid:443"}, {"accesspoint": []},
                 {"accesspoint": [None, 12, "host.invalid", "user:secret@host.invalid:443",
                                  "https://host.invalid:443", "host.invalid:0", "host.invalid:65536",
                                  "host.invalid:443/path", "host.invalid:443?secret", "bad..invalid:443",
                                  "-bad.invalid:443", "host.invalid:443\n"]}]
        for pool in pools:
            response = self.ap_response([])
            response._content = json.dumps(pool).encode()
            with (self.subTest(pool=pool), patch("requests.get", return_value=response) as resolver,
                  patch.object(socket, "create_connection") as connect, worker._accesspoint_scope(),
                  self.assertRaises(worker.WorkerFailure) as caught):
                ApResolver.get_random_accesspoint()
            self.assertEqual(caught.exception.status, "error")
            self.assertNotIn("secret", caught.exception.message)
            connect.assert_not_called()
            resolver.assert_called_once()
        for error in (requests.ConnectionError("secret"), requests.Timeout("secret"), ValueError("secret")):
            with (self.subTest(error=type(error)), patch("requests.get", side_effect=error) as resolver,
                  patch.object(socket, "create_connection") as connect, worker._accesspoint_scope(),
                  self.assertRaises(worker.WorkerFailure) as caught):
                ApResolver.get_random_accesspoint()
            self.assertEqual(caught.exception.status, "error")
            self.assertNotIn("secret", caught.exception.message)
            connect.assert_not_called()
            resolver.assert_called_once()

    def test_accesspoint_resolver_http_classification_in_execute(self):
        import requests

        worker._save_credentials(self.state, self.session)
        for code, status in ((429, "blocked"), (401, "auth_required"), (403, "blocked"), (500, "error")):
            response = self.ap_response(["first.invalid:443"])
            response.status_code = code
            response.headers["Retry-After"] = "19"
            with (self.subTest(code=code), patch.object(requests.sessions.Session, "send", return_value=response) as send,
                  patch.object(socket, "create_connection") as connect, self.assertRaises(worker.WorkerFailure) as caught):
                worker.execute({"command": "probe", "state_dir": str(self.state)}, lambda e: None)
            self.assertEqual(caught.exception.status, status)
            if code == 429:
                self.assertEqual(caught.exception.retry_after, 19)
            send.assert_called_once()
            self.assertEqual(send.call_args.kwargs["timeout"], 10)
            connect.assert_not_called()

    def test_accesspoint_auth_rejection_not_retried(self):
        from librespot.core import Session
        from librespot.proto.Keyexchange_pb2 import APLoginFailed, ErrorCode

        worker._save_credentials(self.state, self.session)
        rejection = Session.SpotifyAuthenticationException(APLoginFailed(error_code=ErrorCode.BadCredentials))

        def reject(session, credentials):
            session.close()  # Stock authentication closes before raising rejection.
            raise rejection

        sock = MagicMock()
        with (patch("requests.get", return_value=self.ap_response(["first.invalid:443", "next.invalid:443"])) as resolver,
              patch.object(socket, "create_connection", return_value=sock) as connect,
              patch.object(Session, "connect"), patch.object(Session, "authenticate", autospec=True, side_effect=reject) as auth,
              self.assertRaises(worker.WorkerFailure) as caught):
            worker.execute({"command": "probe", "state_dir": str(self.state)}, lambda e: None)
        self.assertEqual(caught.exception.status, "auth_required")
        resolver.assert_called_once()
        connect.assert_called_once()
        auth.assert_called_once()
        sock.close.assert_called_once()

    def test_accesspoint_scope_covers_all_execute_actions_through_close(self):
        from librespot.core import ApResolver, Session

        original_create = Session.ConnectionHolder.create
        for action in ("auth", "probe", "metadata", "download"):
            command = {"command": action, "state_dir": str(self.state), "uri": URI,
                       "output_dir": str(self.staging)}
            sock = MagicMock()

            def session(*args):
                self.session.connection = Session.ConnectionHolder.create(ApResolver.get_random_accesspoint(), None)
                return self.session

            def close():
                self.assertIsNot(Session.ConnectionHolder.create, original_create)
                self.session.connection.close()

            self.session.close.side_effect = close
            self.session.close.reset_mock()
            with (self.subTest(action=action), patch.object(worker, "authenticate", side_effect=session),
                  patch.object(worker, "saved_session", side_effect=session),
                  patch.object(worker, "metadata_result", return_value={"status": "ok"}) as metadata,
                  patch.object(worker, "download_result", return_value={"status": "ok"}) as download,
                  patch("requests.get", return_value=self.ap_response(["first.invalid:443", "legacy.invalid:4070"])) as resolver,
                  patch.object(socket, "create_connection", side_effect=[ConnectionRefusedError("secret"), sock]) as connect):
                self.assertEqual(worker.execute(command, lambda e: None)["status"], "ok")
            self.session.close.assert_called_once()
            sock.close.assert_called_once()
            resolver.assert_called_once()
            self.assertEqual(connect.call_count, 2)
            self.assertEqual(metadata.call_count, int(action == "metadata"))
            self.assertEqual(download.call_count, int(action == "download"))
            self.assertIs(Session.ConnectionHolder.create, original_create)

    def test_accesspoint_stock_reconnect_fetches_fresh_pool(self):
        from librespot.core import ApResolver, Session

        session = Session.__new__(Session)
        session._Session__inner = SimpleNamespace(conf=None)
        session._Session__receiver = MagicMock()
        session._Session__ap_welcome = SimpleNamespace(reusable_auth_credentials_type=1,
                                                      canonical_username="user", reusable_auth_credentials=b"secret")
        first, second = MagicMock(), MagicMock()
        with (patch("requests.get", side_effect=[self.ap_response(["old.invalid:443"]),
                    self.ap_response(["new.invalid:4070", "fresh.invalid:443"])]) as resolver,
              patch.object(socket, "create_connection", side_effect=[first, ConnectionRefusedError("secret"), second]) as connect,
              patch.object(Session, "connect"), patch.object(Session, "_Session__authenticate_partial") as auth,
              worker._accesspoint_scope()):
            session.connection = Session.ConnectionHolder.create(ApResolver.get_random_accesspoint(), None)
            session.reconnect()
            session.connection.close()
        self.assertEqual(resolver.call_count, 2)
        self.assertEqual([call.args[0] for call in connect.call_args_list],
                         [("old.invalid", 443), ("fresh.invalid", 443), ("new.invalid", 4070)])
        first.close.assert_called_once()
        second.close.assert_called_once()
        session._Session__receiver.stop.assert_called_once()
        auth.assert_called_once()

    def test_fresh_staging_required(self):
        (self.staging / "existing").touch()
        with patch.object(worker, "saved_session") as saved, self.assertRaises(worker.WorkerFailure):
            worker.execute({"command": "download", "uri": URI, "state_dir": str(self.state),
                            "output_dir": str(self.staging)}, lambda e: None)
        saved.assert_not_called()

    def test_http_rate_limit_and_retries(self):
        import requests
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = "42"
        with patch.object(requests.sessions.Session, "send", return_value=response) as original:
            with worker._http_scope(), self.assertRaises(worker.WorkerFailure) as caught:
                requests.get("https://example.invalid")
            self.assertEqual(caught.exception.result(), {"status": "blocked", "message": "Spotify rate limit reached.", "retry_after": 42.0})
            original.assert_called_once()
            self.assertEqual(original.call_args.kwargs["timeout"], 30)

    def test_protocol_sanitizes_stdout_stderr_exceptions(self):
        stdout, stderr = io.StringIO(), io.StringIO()

        def execute(command, emit):
            print("credentials=secret")
            print("Bearer secret", file=sys.stderr)
            raise RuntimeError("access_token=secret")

        with (patch.object(worker, "execute", execute), patch("sys.stdin", io.StringIO('{}')),
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
            worker.main()
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["event"], "result")
        self.assertEqual(result["status"], "error")
        self.assertIn("RuntimeError at worker.py:", result["message"])
        self.assertRegex(result["message"], r"test_worker\.py:[0-9]+:execute\); retry later\.")
        self.assertNotIn("secret", stdout.getvalue() + stderr.getvalue())

    def test_protocol_failure_reports_only_last_three_safe_frames(self):
        class ConnectionError(BaseException):
            def __str__(self):
                raise AssertionError("Exception text must not be read")

        namespace = {"failure": ConnectionError("Bearer secret https://secret.invalid/?token=secret")}
        exec(compile("""
def execute(command, emit):
    credentials = 'secret'
    first()
def first():
    second()
def second():
    third()
def third():
    raise failure
""", "/credential=secret/core.py", "exec"), namespace)
        stdout, stderr = io.StringIO(), io.StringIO()
        with (patch.object(worker, "execute", namespace["execute"]), patch("sys.stdin", io.StringIO('{}')),
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
            worker.main()
        result = json.loads(stdout.getvalue())
        self.assertEqual(result, {"event": "result", "status": "error", "message":
                                "Spotify worker failed (ConnectionError at core.py:6:first -> "
                                "core.py:8:second -> core.py:10:third); retry later."})
        self.assertNotIn("secret", stdout.getvalue() + stderr.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    def test_protocol_failure_sanitizes_untrusted_type_and_source_names(self):
        namespace = {"failure": type("secret=https://secret.invalid", (BaseException,), {})("secret")}
        exec(compile("def execute(command, emit):\n    raise failure\n",
                     "/private/secret=https://secret.invalid?token=secret", "exec"), namespace)
        namespace["execute"].__code__ = namespace["execute"].__code__.replace(co_name="token=secret")
        stdout, stderr = io.StringIO(), io.StringIO()
        with (patch.object(worker, "execute", namespace["execute"]), patch("sys.stdin", io.StringIO('{}')),
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
            worker.main()
        result = json.loads(stdout.getvalue())
        self.assertIn("BaseException at worker.py:", result["message"])
        self.assertIn("<source>:2:<function>", result["message"])
        self.assertNotIn("secret", stdout.getvalue() + stderr.getvalue())

    def test_cli_invalid_and_missing_auth(self):
        for payload, status in (("not json", "error"), (json.dumps({"command": "probe", "state_dir": str(self.state)}), "auth_required")):
            result = subprocess.run([sys.executable, "-m", "spotisync.worker"], input=payload,
                                    capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(len(result.stdout.splitlines()), 1)
            self.assertEqual(json.loads(result.stdout)["status"], status)

    @contextlib.contextmanager
    def oauth_listener(self):
        listener = SimpleNamespace(port=None, addresses=[])
        original_init = worker.HTTPServer.__init__

        def initialize(server, address, *args, **kwargs):
            listener.addresses.append(address)
            original_init(server, (address[0], 0), *args, **kwargs)
            listener.port = server.server_address[1]

        with patch.object(worker.HTTPServer, "__init__", initialize):
            yield listener

    def test_auth_pkce_state_and_callback(self):
        from librespot.core import OAuth
        result = []
        urls = []
        errors = []
        verifiers = []

        def request_token(oauth):
            verifiers.append(oauth._OAuth__code_verifier)

        def emit(event):
            urls.append(event)

        def authenticate():
            try:
                result.append(worker.authenticate(self.state, emit))
            except BaseException as exc:
                errors.append(exc)

        with (self.oauth_listener() as listener, patch.object(OAuth, "request_token", request_token),
              patch.object(OAuth, "get_credentials", return_value="login"), patch.object(worker, "_builder") as builder):
            builder.return_value.create.return_value = self.session
            thread = threading.Thread(target=authenticate, daemon=True)
            thread.start()
            import time
            deadline = time.monotonic() + 5
            while not urls and not errors and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(urls, errors)
            params = parse_qs(urlsplit(urls[0]["url"]).query)
            self.assertEqual(params["redirect_uri"], ["http://127.0.0.1:4381/login"])
            self.assertEqual(params["code_challenge_method"], ["S256"])
            self.assertGreaterEqual(len(params["state"][0]), 40)
            callback_url = f"http://127.0.0.1:{listener.port}"
            for path in ("/login?code=test&state=wrong", "/login-extra?code=test&state=" + params["state"][0],
                         "/login?code=test&state=" + params["state"][0] + "&state=wrong"):
                with self.assertRaises(HTTPError):
                    urlopen(callback_url + path, timeout=2)
            with urlopen(callback_url + "/login?code=test&state=" + params["state"][0], timeout=2) as response:
                self.assertEqual(response.status, 200)
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(result, [self.session])
            self.assertGreaterEqual(len(verifiers[0]), 43)
            self.assertLessEqual(len(verifiers[0]), 128)
            challenge = base64.urlsafe_b64encode(hashlib.sha256(verifiers[0].encode()).digest()).decode().rstrip("=")
            self.assertEqual(params["code_challenge"], [challenge])

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_real_download_formats_and_embedded_metadata(self):
        self.track.artist.add(name="Guest Artist")
        self.album.artist.add(name="Album Guest")
        self.album.genre.append("Second Genre")
        self.album.type = self.md.Album.COMPILATION
        self.album.copyright.add(text="Second Copyright")
        self.track.external_id.add(type="isrc", id="USABC2400001")
        self.album.external_id.add(type="ean", id="0123456789012")
        self.album.external_id.add(type="upc", id="012345678901")
        self.album.original_title = "Original Album"
        self.album.version_title = "Deluxe"
        alternative = self.md.Track()
        alternative.CopyFrom(self.track)
        alternative.gid = b"a" * 16
        self.track.restriction.add(countries_forbidden="US")
        self.track.alternative.add(gid=alternative.gid)
        self.session.api().get_metadata_4_track.side_effect = lambda ident: (
            self.track if ident.to_spotify_uri() == URI else alternative)
        source = self.root / "source.ogg"
        image = self.root / "cover.jpg"
        subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                        "-c:a", "libvorbis", str(source)], check=True, capture_output=True)
        subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=red:s=16x16",
                        "-frames:v", "1", str(image)], check=True, capture_output=True)
        content = source.read_bytes()
        cover = SimpleNamespace(content=image.read_bytes(), raise_for_status=lambda: None)
        from zotify.config import Zotify

        dates = (({}, "2024"), ({"month": 3}, "2024-03"), ({"month": 3, "day": 7}, "2024-03-07"))
        for fmt, (fields, canonical_date) in product(("ogg", "mp3", "aac"), dates):
            with self.subTest(fmt=fmt, date=canonical_date):
                self.album.date.Clear()
                self.album.date.year = 2024
                for key, value in fields.items():
                    setattr(self.album.date, key, value)
                staging = self.root / (fmt + canonical_date)
                staging.mkdir()
                api = worker.initialize(self.state, staging, fmt)
                stream = io.BytesIO(content)
                self.session.content_feeder().load_track.return_value = SimpleNamespace(
                    input_stream=SimpleNamespace(size=len(content), stream=lambda: stream))
                with (patch.object(Zotify, "get_request", return_value=cover),
                      patch.object(worker, "_verify_file", wraps=worker._verify_file) as verify):
                    result = worker.download_result(self.session, api, URI, staging, fmt)
                self.assertEqual(result["status"], "ok")
                self.assertEqual(result["filename"], TRACK_ID + {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[fmt])
                self.assertEqual([p.name for p in staging.iterdir()], [result["filename"]])
                self.assertEqual(self.session.content_feeder().load_track.call_args.args[0].gid, alternative.gid)
                from music_tag import load_file

                tags = load_file(staging / result["filename"])
                self.assertEqual(tags["artist"].values, ["Artist", "Guest Artist"])
                self.assertEqual(tags["albumartist"].values, ["Artist", "Album Guest"])
                self.assertEqual(tags["genre"].values, ["Genre", "Second Genre"])
                self.assertTrue(bool(tags["compilation"].value))
                path, _, track, expected, tagger_class = verify.call_args.args
                self.assertEqual(track.album.release_date, canonical_date)
                self.assertEqual(expected[0]["year"], "2024")
                self.assertEqual(expected[2]["date"], canonical_date)
                self.assertEqual(tags["isrc"].value, "USABC2400001")
                if fmt != "ogg":
                    self.assertEqual(tags["year"].value, 2024)
                custom = tagger_class(path)
                for key, values in {"date": [canonical_date], "copyright": ["Copyright", "Second Copyright"],
                                    "isrc": ["USABC2400001"], "ean": ["0123456789012"],
                                    "upc": ["012345678901"], "popularity": ["0"], "explicit": ["false"]}.items():
                    physical_key = ("TXXX:" + key.upper() if fmt == "mp3" else
                                    "----:com.apple.iTunes:" + key if fmt == "aac" else key)
                    self.assertEqual(custom._get_raw(physical_key), values)
                metadata_key = ("TXXX:SPOTIFY_METADATA" if fmt == "mp3" else
                                "----:com.apple.iTunes:spotify_metadata" if fmt == "aac" else "spotify_metadata")
                descriptive = json.loads(custom._get_raw(metadata_key)[0])
                self.assertEqual(descriptive["album"]["date"], {"year": 2024, **fields})
                self.assertEqual(descriptive["popularity"], 0)
                self.assertEqual(descriptive["album"]["copyright"],
                                 [{"text": "Copyright"}, {"text": "Second Copyright"}])
                if fmt == "mp3":
                    self.assertEqual(tags.mfile.tags["TPE1"].text, ["Artist", "Guest Artist"])
                    self.assertEqual(tags.mfile.tags["TPE2"].text, ["Artist", "Album Guest"])
                    self.assertEqual(tags.mfile.tags["TCON"].text, ["Genre", "Second Genre"])

                # Same year is insufficient: the complete custom date must still match.
                date_key = ("TXXX:DATE" if fmt == "mp3" else
                            "----:com.apple.iTunes:date" if fmt == "aac" else "date")
                original_date = tags.mfile.tags[date_key]
                if fmt == "mp3":
                    from mutagen.id3 import TXXX
                    tags.mfile.tags[date_key] = TXXX(encoding=3, desc="DATE", text=["2024-12-31"])
                elif fmt == "aac":
                    from mutagen.mp4 import MP4FreeForm
                    tags.mfile.tags[date_key] = [MP4FreeForm(b"2024-12-31")]
                else:
                    tags.mfile.tags[date_key] = ["2024-12-31"]
                tags.save()
                with self.assertRaises(worker.WorkerFailure) as caught:
                    worker._verify_file(path, fmt, track, expected, tagger_class)
                self.assertIn("custom: date", caught.exception.message)
                tags.mfile.tags[date_key] = original_date
                if fmt == "ogg":
                    tags.mfile.tags["date"] = [canonical_date.replace("2024", "1999")]
                else:
                    tags["year"] = 1999
                tags.save()
                with self.assertRaises(worker.WorkerFailure) as caught:
                    worker._verify_file(path, fmt, track, expected, tagger_class)
                self.assertIn("reliable: year", caught.exception.message)

    def test_conversion_error_propagates_and_no_success(self):
        from zotify.config import Zotify
        stream = io.BytesIO(b"source")
        self.session.content_feeder().load_track.return_value = SimpleNamespace(
            input_stream=SimpleNamespace(size=6, stream=lambda: stream))
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with (patch.object(Zotify, "get_request", return_value=cover),
              patch.object(self.api.DLContent, "run_ffm", side_effect=RuntimeError("conversion failed")),
              self.assertRaises(RuntimeError)):
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")

    def canonical_original(self, date="2024-03-07", bitrate=160):
        """Generate local audio, then use the real download/tag/verify path offline."""
        self.track.artist.add(name="Guest Artist")
        self.album.artist.add(name="Album Guest")
        self.album.genre.append("Second Genre")
        self.album.type = self.md.Album.COMPILATION
        self.album.copyright.add(text="Second Copyright")
        self.track.external_id.add(type="isrc", id="USABC2400001")
        self.album.external_id.add(type="ean", id="0123456789012")
        self.album.date.Clear()
        for field, value in zip(("year", "month", "day"), date.split("-")):
            setattr(self.album.date, field, int(value))
        audio = self.root / ("generated-" + date + ".ogg")
        image = self.root / ("cover-" + date + ".jpg")
        subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
                        "-ac", "2", "-c:a", "libvorbis", "-b:a", f"{bitrate}k", str(audio)],
                       check=True, capture_output=True)
        subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=red:s=16x16",
                        "-frames:v", "1", str(image)], check=True, capture_output=True)
        content = audio.read_bytes()
        stream = io.BytesIO(content)
        self.session.content_feeder().load_track.return_value = SimpleNamespace(
            input_stream=SimpleNamespace(size=len(content), stream=lambda: stream))
        original_dir = self.root / ("original-" + date)
        original_dir.mkdir()
        api = worker.initialize(self.state, original_dir, "ogg")
        from zotify.config import Zotify
        from mutagen.oggvorbis import OggVorbis

        cover = SimpleNamespace(content=image.read_bytes(), raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover):
            result = worker.download_result(self.session, api, URI, original_dir, "ogg")
        original = original_dir / result["filename"]
        tags = OggVorbis(original)
        tags["custom_text"] = ["first", "second", "", "003"]
        tags["custom_id"] = ["000001"]
        tags["custom:TXXX:embedded"] = ["physical prefixes remain ordinary custom text"]
        tags.save()
        from music_tag import load_file
        from PIL import Image

        second_cover = io.BytesIO()
        Image.new("RGB", (8, 8), "blue").save(second_cover, format="PNG")
        artwork = load_file(original)
        artwork["artwork"] = artwork["artwork"].values + [second_cover.getvalue()]
        artwork.save()
        original.chmod(0o444)
        return original

    def convert_command(self, source, output, fmt="ogg", **extra):
        return {"command": "convert", "state_dir": str(self.state), "source_path": str(source),
                "output_dir": str(output), "format": fmt, **extra}

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_offline_convert_all_formats_dates_metadata_and_immutable_original(self):
        from music_tag import load_file
        from zotify.metadata import Tagger
        from zotify.config import Zotify
        from librespot.core import Session

        real_run = subprocess.run
        for date in ("2024", "2024-03", "2024-03-07"):
            source = self.canonical_original(date)
            before = source.read_bytes()
            source_inode = source.stat().st_ino
            source_tags = Tagger(source)
            original_custom = dict(source_tags.file_tags.mfile.tags)
            for fmt in ("ogg", "mp3", "aac"):
                output = self.root / ("playback-" + fmt + date)
                with (self.subTest(fmt=fmt, date=date),
                      patch.object(worker, "saved_session", side_effect=AssertionError("auth forbidden")),
                      patch.object(worker, "authenticate", side_effect=AssertionError("auth forbidden")),
                      patch.object(worker, "_http_scope", side_effect=AssertionError("HTTP scope forbidden")),
                      patch.object(worker, "_accesspoint_scope", side_effect=AssertionError("AP scope forbidden")),
                      patch.object(Session.Builder, "create", side_effect=AssertionError("Session forbidden")),
                      patch("requests.sessions.Session.send", side_effect=AssertionError("HTTP forbidden")),
                      patch.object(socket, "create_connection", side_effect=AssertionError("network forbidden")),
                      patch.object(Zotify, "get_request", side_effect=AssertionError("refetch forbidden")),
                      patch.object(self.api.DLContent, "run_ffm", side_effect=AssertionError("input deletion forbidden")),
                      patch.object(worker.subprocess, "run", wraps=real_run) as run,
                      patch.object(worker, "_verify_file", wraps=worker._verify_file) as verify):
                    result = worker.execute(self.convert_command(source, output, fmt, uri=URI, quality="very_high"),
                                            lambda e: self.fail(str(e)))
                self.assertEqual(result["status"], "ok")
                self.assertEqual(result["track"], {"id": TRACK_ID, "uri": URI, "name": "Song",
                                 "artist": ", ".join(original_custom["artist"]), "album": "Album",
                                 "duration_ms": 1000, "availability": "available"})
                path = output / result["filename"]
                self.assertEqual([p.name for p in output.iterdir()], [path.name])
                self.assertEqual(source.read_bytes(), before)
                self.assertEqual(source.stat().st_ino, source_inode)
                self.assertNotEqual(path.stat().st_ino, source_inode)
                self.assertEqual(source.stat().st_mode & 0o777, 0o444)
                self.assertFalse(list(self.state.iterdir()))
                self.assertEqual(verify.call_count, 2)
                self.assertNotIn("quality", result)
                from zotify.config import Config
                self.assertEqual(Config.Values["DOWNLOAD_QUALITY"], "auto")
                logical, _, custom = verify.call_args.args[3]
                self.assertEqual(logical["year"], 2024)
                self.assertEqual(custom["date"], [date])
                tags = load_file(path)
                for key in ("artist", "albumartist", "genre"):
                    self.assertEqual(tags[key].values, original_custom[key])
                for key, expected in {"tracknumber": 1, "totaltracks": 1, "discnumber": 1,
                                      "totaldiscs": 1, "compilation": True}.items():
                    self.assertEqual(tags[key].value, expected)
                self.assertEqual(tags["isrc"].value, "USABC2400001")
                self.assertEqual([art.data for art in tags["artwork"].values],
                                 [art.data for art in source_tags.file_tags["artwork"].values])
                actual = Tagger(path)
                for key, values in custom.items():
                    physical = ("TXXX:" + key.upper() if fmt == "mp3" else
                                "----:com.apple.iTunes:" + key if fmt == "aac" else key)
                    stored = actual.file_tags.mfile.tags[physical]
                    on_disk = (stored.text if fmt == "mp3" else
                               [v.decode("utf-8") for v in stored] if fmt == "aac" else stored)
                    self.assertEqual(on_disk, values, key)
                encodes = [call.args[0] for call in run.call_args_list if "-c:a" in call.args[0]]
                self.assertEqual(len(encodes), int(fmt != "ogg"))
                if fmt == "ogg":
                    self.assertEqual(path.read_bytes(), before)
                else:
                    self.assertEqual(tags["year"].value, 2024)
                    args = encodes[0]
                    self.assertEqual(args[args.index("-i") + 1], str(source))
                    self.assertEqual(args[args.index("-map") + 1], "0:a")
                    self.assertEqual(args[args.index("-map_metadata") + 1], "-1")
                    self.assertEqual(args[args.index("-b:a") + 1], "160000")
                if fmt == "mp3":
                    self.assertEqual(tags.mfile.tags["TPE1"].text, original_custom["artist"])
                    self.assertEqual(tags.mfile.tags["TPE2"].text, original_custom["albumartist"])
                    self.assertEqual(tags.mfile.tags["TCON"].text, original_custom["genre"])

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_uri_optional_and_protocol(self):
        source = self.canonical_original()
        command = self.convert_command(source, self.root / "offline-cli", "aac")
        stdout, stderr = io.StringIO(), io.StringIO()
        with (patch("sys.stdin", io.StringIO(json.dumps(command))), contextlib.redirect_stdout(stdout),
              contextlib.redirect_stderr(stderr), patch.object(worker, "saved_session") as saved):
            worker.main()
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["event"], "result")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["track"]["uri"], URI)
        saved.assert_not_called()
        self.assertNotIn("secret", stderr.getvalue())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_source_validation_no_lossy_fallback_or_overwrite(self):
        source = self.canonical_original()
        before = source.read_bytes()
        empty = self.root / "empty.ogg"
        empty.touch()
        invalid = self.root / "invalid.ogg"
        invalid.write_bytes(b"Bearer secret")
        symlink = self.root / "symlink.ogg"
        symlink.symlink_to(source)
        for codec, extension in (("libmp3lame", "mp3"), ("libopus", "ogg"), ("aac", "aac")):
            subprocess.run(["ffmpeg", "-v", "error", "-i", str(source), "-c:a", codec,
                            str(self.root / ("wrong-" + codec + "." + extension))], check=True, capture_output=True)
        wrong_mp3 = self.root / "wrong-libmp3lame.mp3"
        disguised = self.root / "disguised.ogg"
        shutil.copyfile(wrong_mp3, disguised)
        disguised_aac = self.root / "disguised-aac.ogg"
        shutil.copyfile(self.root / "wrong-aac.aac", disguised_aac)
        cases = (self.root / "missing.ogg", empty, invalid, symlink, self.root,
                 wrong_mp3, self.root / "wrong-libopus.ogg", disguised, disguised_aac)
        for index, bad_source in enumerate(cases):
            output = self.root / f"bad-source-{index}"
            with (self.subTest(source=bad_source.name), patch.object(worker, "saved_session") as saved,
                  self.assertRaises(worker.WorkerFailure) as caught):
                worker.execute(self.convert_command(bad_source, output), lambda e: None)
            self.assertEqual(caught.exception.status, "error")
            self.assertNotIn("secret", caught.exception.message)
            self.assertFalse(list(output.iterdir()))
            saved.assert_not_called()
        for extra in ({"source_path": "relative.ogg"}, {"format": "flac"}, {"uri": "spotify:album:" + ALBUM_ID}):
            with self.subTest(extra=extra), self.assertRaises(worker.WorkerFailure):
                worker.execute(self.convert_command(source, self.root / "invalid-command", **extra), lambda e: None)
        occupied = self.root / "occupied"
        occupied.mkdir()
        sentinel = occupied / (TRACK_ID + ".ogg")
        sentinel.write_bytes(b"keep existing")
        with self.assertRaises(worker.WorkerFailure):
            worker.execute(self.convert_command(source, occupied), lambda e: None)
        self.assertEqual(sentinel.read_bytes(), b"keep existing")
        # Exclusive output creation must not unlink a file created after staging validation.
        worker.initialize(self.state, occupied)
        with self.assertRaises(worker.WorkerFailure):
            worker.convert_result(source, occupied, "ogg", URI)
        self.assertEqual(sentinel.read_bytes(), b"keep existing")
        self.assertEqual(source.read_bytes(), before)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_requires_canonical_core_cover_and_spotify_identity_before_output(self):
        from mutagen.oggvorbis import OggVorbis
        from zotify.config import Zotify

        original = self.canonical_original()
        original_bytes = original.read_bytes()
        wrong_uri = "spotify:track:" + "9" * 22
        cases = [("untagged", None, None)]
        for key in ("title", "album", "artist", "metadata_block_picture", "uri"):
            cases.extend((("missing-" + key, key, None), ("blank-" + key, key, [" "])))
        cases.extend((("wrong-uri", "uri", [wrong_uri]),
                      ("invalid-uri", "uri", ["Bearer secret"]),
                      ("album-uri", "uri", ["spotify:album:" + ALBUM_ID]),
                      ("multiple-uris", "uri", [URI, wrong_uri]),
                      ("wrong-trackid", "trackid", ["9" * 22]),
                      ("blank-trackid", "trackid", [""]),
                      ("multiple-trackids", "trackid", [TRACK_ID, "9" * 22])))
        real_run = subprocess.run
        for name, key, values in cases:
            source = self.root / (name + ".ogg")
            shutil.copyfile(original, source)
            tags = OggVorbis(source)
            if name == "untagged":
                tags.tags.clear()
            elif values is None:
                del tags[key]
            else:
                tags[key] = values
            tags.save()
            source.chmod(0o444)
            before = source.read_bytes()
            inode = source.stat().st_ino
            for fmt, requested in product(("ogg", "mp3", "aac"), (True, False)):
                output = self.root / f"reject-{name}-{fmt}-{requested}"
                extra = {"uri": URI} if requested else {}
                with (self.subTest(case=name, fmt=fmt, requested=requested),
                      patch.object(worker, "saved_session", side_effect=AssertionError("auth forbidden")),
                      patch.object(worker, "authenticate", side_effect=AssertionError("auth forbidden")),
                      patch.object(worker, "_http_scope", side_effect=AssertionError("HTTP scope forbidden")),
                      patch.object(worker, "_accesspoint_scope", side_effect=AssertionError("AP scope forbidden")),
                      patch("requests.sessions.Session.send", side_effect=AssertionError("HTTP forbidden")),
                      patch.object(socket, "create_connection", side_effect=AssertionError("network forbidden")),
                      patch.object(Zotify, "get_request", side_effect=AssertionError("refetch forbidden")),
                      patch.object(worker.shutil, "copyfile", side_effect=AssertionError("copy forbidden")),
                      patch.object(worker.subprocess, "run", wraps=real_run) as run,
                      self.assertRaises(worker.WorkerFailure) as caught):
                    worker.execute(self.convert_command(source, output, fmt, **extra), lambda e: self.fail(str(e)))
                self.assertEqual(caught.exception.status, "error")
                self.assertNotIn("secret", caught.exception.message)
                self.assertNotIn(wrong_uri, caught.exception.message)
                self.assertFalse(any("-c:a" in call.args[0] for call in run.call_args_list))
                self.assertFalse(list(output.iterdir()))
                self.assertEqual(source.read_bytes(), before)
                self.assertEqual(source.stat().st_ino, inode)
                self.assertEqual(source.stat().st_mode & 0o777, 0o444)
        self.assertEqual(original.read_bytes(), original_bytes)
        self.assertFalse(list(self.state.iterdir()))

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_relinked_audio_preserves_requested_identity_and_metadata(self):
        from zotify.metadata import Tagger

        alternative = self.md.Track()
        alternative.CopyFrom(self.track)
        alternative.gid = b"a" * 16
        self.track.restriction.add(countries_forbidden="US")
        self.track.alternative.add(gid=alternative.gid)
        self.session.api().get_metadata_4_track.side_effect = lambda ident: (
            self.track if ident.to_spotify_uri() == URI else alternative)
        source = self.canonical_original()
        self.assertEqual(self.session.content_feeder().load_track.call_args.args[0].gid, alternative.gid)
        before = source.read_bytes()
        for fmt in ("ogg", "mp3", "aac"):
            output = self.root / ("relinked-" + fmt)
            with self.subTest(fmt=fmt), patch.object(worker, "saved_session", side_effect=AssertionError("auth forbidden")):
                result = worker.execute(self.convert_command(source, output, fmt, uri=URI), lambda e: None)
            self.assertEqual(result["track"]["uri"], URI)
            self.assertEqual(result["track"]["id"], TRACK_ID)
            tags = Tagger(output / result["filename"])
            for key, values in {"uri": [URI], "trackid": [TRACK_ID],
                                "spotify_metadata": Tagger(source)._get_raw("spotify_metadata")}.items():
                physical = ("TXXX:" + key.upper() if fmt == "mp3" else
                            "----:com.apple.iTunes:" + key if fmt == "aac" else key)
                self.assertEqual(tags._get_raw(physical), values)
            self.assertEqual(source.read_bytes(), before)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_ffmpeg_failure_preserves_input_and_sanitizes_protocol(self):
        source = self.canonical_original()
        before = source.read_bytes()
        output = self.root / "failed-encode"
        real_run = subprocess.run

        def run(args, **kwargs):
            if "-c:a" in args:
                Path(args[-1]).write_bytes(b"partial output")
                raise subprocess.CalledProcessError(1, args, stderr=b"Bearer secret")
            return real_run(args, **kwargs)

        stdout = io.StringIO()
        with (patch.object(worker.subprocess, "run", side_effect=run),
              patch("sys.stdin", io.StringIO(json.dumps(self.convert_command(source, output, "mp3")))),
              contextlib.redirect_stdout(stdout)):
            worker.main()
        self.assertEqual(json.loads(stdout.getvalue()), {"event": "result", "status": "error",
                         "message": "Offline conversion failed encoding."})
        self.assertEqual(source.read_bytes(), before)
        self.assertFalse(list(output.iterdir()))

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_target_metadata_artwork_codec_duration_corruption_rejected(self):
        from zotify.metadata import Tagger

        source = self.canonical_original()
        before = source.read_bytes()
        real_verify = worker._verify_file
        real_run = subprocess.run
        for fmt, corruption in product(("ogg", "mp3", "aac"),
                                       ("title", "artist", "genre", "custom", "artwork", "codec", "duration")):
            output = self.root / (fmt + "-corrupt-" + corruption)

            def verify(path, output_format, track, expected, tagger_class):
                if path != source:
                    if corruption == "codec":
                        replacement = self.root / "wrong-codec.wav"
                        real_run(["ffmpeg", "-v", "error", "-i", str(source), "-map", "0:a",
                                  "-c:a", "pcm_s16le", "-y", str(replacement)],
                                 check=True, capture_output=True)
                        shutil.copyfile(replacement, path)
                    elif corruption != "duration":
                        tags = Tagger(path)
                        if corruption == "custom":
                            with worker._tag_scope():
                                tags._set_custom("custom_text", ["first", "changed"])
                        elif corruption == "artwork":
                            physical = {"ogg": "metadata_block_picture", "mp3": "APIC", "aac": "covr"}[fmt]
                            if fmt == "mp3":
                                tags.file_tags.mfile.tags.delall(physical)
                            else:
                                del tags.file_tags.mfile.tags[physical]
                        else:
                            with worker._tag_scope():
                                tags.file_tags[{"title": "tracktitle"}.get(corruption, corruption)] = "changed"
                        tags.file_tags.save()
                return real_verify(path, output_format, track, expected, tagger_class)

            def run(args, **kwargs):
                result = real_run(args, **kwargs)
                if corruption == "duration" and args[0] == "ffprobe" and args[-1] != str(source):
                    info = json.loads(result.stdout)
                    info["format"]["duration"] = "20"
                    result.stdout = json.dumps(info).encode()
                return result

            with (self.subTest(fmt=fmt, corruption=corruption), patch.object(worker, "_verify_file", side_effect=verify),
                  patch.object(worker.subprocess, "run", side_effect=run), self.assertRaises(worker.WorkerFailure) as caught):
                worker.execute(self.convert_command(source, output, fmt), lambda e: None)
            self.assertEqual(caught.exception.status, "error")
            self.assertNotIn("secret", caught.exception.message)
            self.assertEqual(source.read_bytes(), before)
            self.assertFalse(list(output.iterdir()))

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_source_full_decode_failure_stops_before_output(self):
        source = self.canonical_original()
        before = source.read_bytes()
        output = self.root / "failed-decode"
        real_run = subprocess.run

        def run(args, **kwargs):
            if args[0] == "ffmpeg" and "-f" in args and args[args.index("-f") + 1] == "null":
                raise subprocess.CalledProcessError(1, args, stderr=b"secret decode error")
            return real_run(args, **kwargs)

        with (patch.object(worker.subprocess, "run", side_effect=run), self.assertRaises(worker.WorkerFailure) as caught):
            worker.execute(self.convert_command(source, output, "aac"), lambda e: None)
        self.assertIn("decode verification", caught.exception.message)
        self.assertNotIn("secret", caught.exception.message)
        self.assertEqual(source.read_bytes(), before)
        self.assertFalse(list(output.iterdir()))

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_detects_original_change_and_cleans_output(self):
        source = self.canonical_original()
        source.chmod(0o644)
        output = self.root / "changed-source"
        real_verify = worker._verify_file

        def verify(path, *args):
            result = real_verify(path, *args)
            if path != source:
                from mutagen.oggvorbis import OggVorbis

                tags = OggVorbis(source)
                tags["external_change"] = ["changed"]
                tags.save()
            return result

        with (patch.object(worker, "_verify_file", side_effect=verify), self.assertRaises(worker.WorkerFailure) as caught):
            worker.execute(self.convert_command(source, output, "mp3"), lambda e: None)
        self.assertEqual(caught.exception.message, "Original audio changed during conversion.")
        self.assertTrue(source.is_file())
        self.assertFalse(list(output.iterdir()))

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg/ffprobe required")
    def test_convert_bitrate_tiers_fallback_and_false_compilation(self):
        from mutagen.oggvorbis import OggVorbis
        from music_tag import load_file

        real_run = subprocess.run
        for bitrate, date in ((96, "2022"), (160, "2023"), (320, "2024")):
            source = self.canonical_original(date, bitrate)
            source.chmod(0o644)
            tags = OggVorbis(source)
            tags["compilation"] = ["0"]
            tags["composer"] = ["First Composer", "Second Composer"]
            tags["comment"] = ["First Comment", "Second Comment"]
            tags["lyrics"] = ["First Lyrics", "Second Lyrics"]
            tags.save()
            for fallback in (False, True):
                output = self.root / f"bitrate-{bitrate}-{fallback}"

                def run(args, **kwargs):
                    result = real_run(args, **kwargs)
                    if args[0] == "ffprobe" and args[-1] == str(source):
                        info = json.loads(result.stdout)
                        for stream in info["streams"]:
                            if stream["codec_type"] == "audio":
                                if fallback:
                                    stream.pop("bit_rate", None)
                                else:
                                    # Average stream rates can exceed the source's nominal tier.
                                    stream["bit_rate"] = str(bitrate * 1100)
                        result.stdout = json.dumps(info).encode()
                    return result

                with (self.subTest(bitrate=bitrate, fallback=fallback),
                      patch.object(worker.subprocess, "run", side_effect=run) as calls):
                    result = worker.execute(self.convert_command(source, output, "mp3"), lambda e: None)
                encode = next(call.args[0] for call in calls.call_args_list if "-c:a" in call.args[0])
                self.assertEqual(encode[encode.index("-b:a") + 1], str(bitrate * 1000))
                converted = load_file(output / result["filename"])
                self.assertFalse(converted["compilation"].value)
                self.assertEqual(converted["composer"].values, ["First Composer", "Second Composer"])
                self.assertEqual(converted["comment"].values, ["First Comment", "Second Comment"])
                self.assertEqual(converted["lyrics"].values, ["First Lyrics", "Second Lyrics"])

    def test_audio_key_denial_retryable(self):
        from zotify.config import Zotify
        self.session.content_feeder().load_track.side_effect = RuntimeError("Failed fetching audio key! secret")
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")
        self.session.content_feeder().load_track.assert_called_once()

    def test_swallowed_tag_failure_detected(self):
        from zotify.config import Zotify
        stream = io.BytesIO(b"source")
        self.session.content_feeder().load_track.return_value = SimpleNamespace(
            input_stream=SimpleNamespace(size=6, stream=lambda: stream))
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with (patch.object(Zotify, "get_request", return_value=cover),
              patch.object(self.api.DLContent, "run_ffm", return_value=""),
              patch.object(self.api.Track, "write_audio_tags", side_effect=RuntimeError("secret")),
              self.assertRaises(worker.WorkerFailure) as caught):
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")
        self.assertIn("metadata writing failed", caught.exception.message)
        self.assertNotIn("secret", caught.exception.message)

    def test_fileless_and_explicit_storage_restriction(self):
        from zotify.config import Zotify
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        del self.track.file[:]
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")
        self.track.file.add(file_id=b"f" * 20, format=self.md.AudioFile.OGG_VORBIS_160)
        self.session.content_feeder().load_track.side_effect = RuntimeError("Content is restricted!")
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "unavailable")

    def test_stream_size_mismatch_rejected(self):
        from zotify.config import Zotify
        stream = io.BytesIO(b"truncated")
        self.session.content_feeder().load_track.return_value = SimpleNamespace(
            input_stream=SimpleNamespace(size=100, stream=lambda: stream))
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with (patch.object(Zotify, "get_request", return_value=cover),
              patch.object(self.api.DLContent, "run_ffm") as convert,
              self.assertRaises(worker.WorkerFailure) as caught):
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertIn("truncated", caught.exception.message)
        convert.assert_not_called()
        self.assertTrue(stream.closed)

    def test_verification_rejects_wrong_codec_duration_tags_decode(self):
        track = SimpleNamespace(duration_ms=60000, name="Song", album=SimpleNamespace(name="Album"), artists=["Artist"])
        good = {"streams": [{"codec_type": "audio", "codec_name": "vorbis"}],
                "format": {"duration": "60", "tags": {"title": "Song", "artist": "Artist", "album": "Album"}}}
        tagger = MagicMock()
        tagger.return_value._match_tag_group.return_value = {}
        cases = {"probe": "probe verification", "codec": "actual=mp3, expected=vorbis",
                 "codec_secret": "actual=unknown, expected=vorbis", "streams": "actual=0, expected=1",
                 "duration": "actual=10, expected=60, tolerance=1.5", "nan": "actual=nan, expected=60, tolerance=1.5",
                 "tags": "probe: title", "decode": "decode verification"}
        for case, message in cases.items():
            info = json.loads(json.dumps(good))
            if case == "codec":
                info["streams"][0]["codec_name"] = "mp3"
            elif case == "codec_secret":
                info["streams"][0]["codec_name"] = "Bearer secret https://secret.invalid"
            elif case == "streams":
                info["streams"] = []
            elif case == "duration":
                info["format"]["duration"] = "10"
            elif case == "nan":
                info["format"]["duration"] = "nan"
            elif case == "tags":
                info["format"]["tags"]["title"] = "credentials=secret"
            calls = [SimpleNamespace(stdout=json.dumps(info).encode())]
            if case == "probe":
                calls = [subprocess.CalledProcessError(1, "ffprobe", output=b"token=secret", stderr=b"secret")]
            elif case == "decode":
                calls.append(subprocess.CalledProcessError(1, "ffmpeg", output=b"token=secret", stderr=b"secret"))
            with (self.subTest(case=case), patch.object(worker.subprocess, "run", side_effect=calls) as run,
                  self.assertRaises(worker.WorkerFailure) as caught):
                worker._verify_file(self.staging / "test.ogg", "ogg", track, ({}, {}, {}), tagger)
            self.assertEqual(run.call_count, 2 if case == "decode" else 1)
            self.assertIn(message, caught.exception.message)
            self.assertNotIn("secret", caught.exception.message)

    def test_verification_metadata_diagnostics_expose_keys_not_values(self):
        track = SimpleNamespace(duration_ms=1000, name="Song", album=SimpleNamespace(name="Album"), artists=[])
        info = {"streams": [{"codec_type": "audio", "codec_name": "vorbis"}], "format": {"duration": "1"}}
        cases = (({"artwork": b"secret artwork bytes"}, {}, {}, "reliable: artwork"),
                 ({}, {"totaltracks": 17}, {}, "optional: totaltracks"),
                 ({}, {}, {"spotify_metadata": '{"token":"secret"}'}, "custom: spotify_metadata"))
        for reliable, optional, custom, message in cases:
            tagger = MagicMock()
            tagger.return_value._match_tag_group.return_value = {"key": 'secret metadata dump'}
            tagger.return_value._get_raw.return_value = ["secret"]
            with (self.subTest(message=message), patch.object(worker.subprocess, "run", return_value=SimpleNamespace(stdout=json.dumps(info))),
                  self.assertRaises(worker.WorkerFailure) as caught):
                # Avoid probe-level title checks to reach detailed tag verification.
                track.name = track.album.name = ""
                worker._verify_file(self.staging / "test.ogg", "ogg", track, (reliable, optional, custom), tagger)
            self.assertIn(message, caught.exception.message)
            self.assertNotIn("secret", caught.exception.message)
            self.assertNotIn("bytes", caught.exception.message)

        tagger.return_value._match_tag_group.side_effect = ValueError("secret artwork bytes")
        with (patch.object(worker.subprocess, "run", return_value=SimpleNamespace(stdout=json.dumps(info))),
              self.assertRaises(worker.WorkerFailure) as caught):
            worker._verify_file(self.staging / "test.ogg", "ogg", track, ({"artwork": b"secret"}, {}, {}), tagger)
        self.assertIn("reliable: artwork", caught.exception.message)
        self.assertNotIn("secret", caught.exception.message)

    def test_ogg_year_verification_requires_single_matching_canonical_raw_date(self):
        track = SimpleNamespace(duration_ms=1000, name="", album=SimpleNamespace(name=""), artists=[])
        info = {"streams": [{"codec_type": "audio", "codec_name": "vorbis"}], "format": {"duration": "1"}}
        for dates in (None, [], ["1999-03"], ["2024-3"], ["2024-3-7"], ["2024", "1999"], ["secret"]):
            tagger = MagicMock()
            tagger.return_value._get_raw.return_value = dates
            with (self.subTest(dates=dates), patch.object(worker.subprocess, "run", return_value=SimpleNamespace(stdout=json.dumps(info))) as run,
                  self.assertRaises(worker.WorkerFailure) as caught):
                worker._verify_file(self.staging / "test.ogg", "ogg", track, ({"year": "2024"}, {}, {}), tagger)
            self.assertIn("reliable: year", caught.exception.message)
            self.assertNotIn("secret", caught.exception.message)
            run.assert_called_once()

    def test_explicit_source_quality_and_auto_select_exact_file_before_any_conversion(self):
        from zotify.config import Zotify

        self.track.file.add(file_id=b"n" * 20, format=self.md.AudioFile.OGG_VORBIS_96)
        self.track.file.add(file_id=b"v" * 20, format=self.md.AudioFile.OGG_VORBIS_320)
        self.track.file.add(file_id=b"m" * 20, format=self.md.AudioFile.MP3_320)
        formats = {"normal": self.md.AudioFile.OGG_VORBIS_96, "high": self.md.AudioFile.OGG_VORBIS_160,
                   "very_high": self.md.AudioFile.OGG_VORBIS_320}
        cases = [(account, quality, quality) for account in ("premium", "free") for quality in ("normal", "high")]
        cases += [("premium", "very_high", "very_high"), ("premium", "auto", "very_high"), ("free", "auto", "high")]
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        self.session.content_feeder().load_track.return_value = SimpleNamespace(input_stream=object())
        for fmt, (account, quality, actual) in product(("ogg", "mp3", "aac"), cases):
            self.session.get_user_attribute.return_value = account
            self.session.content_feeder().load_track.reset_mock()
            api = worker.initialize(self.state, self.staging, fmt, quality)
            destination = self.staging / (TRACK_ID + {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[fmt])

            def download(track, parent_stack):
                track.fetch_stream()
                track.convert_audio_format(self.root / "mock-source.ogg", destination)
                track.mark_downloaded(path=destination)

            with (self.subTest(fmt=fmt, account=account, quality=quality),
                  patch.object(Zotify, "get_request", return_value=cover),
                  patch.object(api.Track, "download", download), patch.object(api.DLContent, "run_ffm") as convert,
                  patch.object(worker, "_verify_file")):
                result = worker.download_result(self.session, api, URI, self.staging, fmt, quality)
            load = self.session.content_feeder().load_track
            load.assert_called_once()
            selector = load.call_args.args[1]
            self.assertEqual(selector.get_file(self.track.file).format, formats[actual])
            self.assertEqual(selector.preferred.name.lower(), actual)
            self.assertEqual(result["quality"], actual)
            self.assertEqual(result["status"], "ok")
            options = convert.call_args.args[-1]
            if fmt == "ogg":
                self.assertEqual(options, ["-c:a", "copy"])
            else:
                self.assertEqual(options, ["-c:a", {"mp3": "libmp3lame", "aac": "aac"}[fmt],
                                          "-b:a", {"normal": "96k", "high": "160k", "very_high": "320k"}[actual]])

    def test_very_high_free_requires_premium_before_metadata_or_audio(self):
        from zotify.config import Zotify

        self.session.get_user_attribute.return_value = "free"
        with (patch.object(worker, "_raw") as raw, patch.object(Zotify, "get_request") as cover,
              self.assertRaises(worker.WorkerFailure) as caught):
            worker.download_result(self.session, self.api, URI, self.staging, "ogg", "very_high")
        self.assertEqual(caught.exception.status, "blocked")
        self.assertIn("Spotify Premium", caught.exception.message)
        raw.assert_not_called()
        cover.assert_not_called()
        self.session.content_feeder.assert_not_called()

    def test_missing_explicit_source_quality_never_downgrades_or_upgrades(self):
        from zotify.config import Zotify

        formats = {"normal": self.md.AudioFile.OGG_VORBIS_96, "high": self.md.AudioFile.OGG_VORBIS_160,
                   "very_high": self.md.AudioFile.OGG_VORBIS_320}
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        for quality, requested in formats.items():
            self.track.ClearField("file")
            for file_format in formats.values():
                if file_format != requested:
                    self.track.file.add(file_id=b"f" * 20, format=file_format)
            with (self.subTest(quality=quality), patch.object(Zotify, "get_request", return_value=cover),
                  self.assertRaises(worker.WorkerFailure) as caught):
                worker.download_result(self.session, self.api, URI, self.staging, "ogg", quality)
            self.assertEqual(caught.exception.status, "blocked")
            self.assertIn("Requested source quality is unavailable", caught.exception.message)
            self.session.content_feeder.assert_not_called()

    def test_alternatives_keep_exact_requested_quality_after_audio_failure(self):
        from zotify.config import Zotify

        formats = {"normal": self.md.AudioFile.OGG_VORBIS_96, "high": self.md.AudioFile.OGG_VORBIS_160,
                   "very_high": self.md.AudioFile.OGG_VORBIS_320}
        self.track.ClearField("file")
        for file_format in formats.values():
            self.track.file.add(file_id=b"f" * 20, format=file_format)
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        for quality, requested in formats.items():
            self.track.ClearField("alternative")
            missing, good = self.md.Track(), self.md.Track()
            missing.CopyFrom(self.track)
            good.CopyFrom(self.track)
            missing.gid, good.gid = b"a" * 16, b"b" * 16
            missing.ClearField("file")
            for file_format in formats.values():
                if file_format != requested:
                    missing.file.add(file_id=b"m" * 20, format=file_format)
            self.track.alternative.add(gid=missing.gid)
            self.track.alternative.add(gid=good.gid)
            self.session.api().get_metadata_4_track.side_effect = [self.track, missing, good]
            load = self.session.content_feeder().load_track
            load.reset_mock()
            load.side_effect = [RuntimeError("Failed fetching audio key! secret"), SimpleNamespace(input_stream=object())]
            api = worker.initialize(self.state, self.staging, "ogg", quality)

            def download(track, parent_stack):
                track.fetch_stream()
                track.mark_downloaded(path=self.staging / (TRACK_ID + ".ogg"))

            with (self.subTest(quality=quality), patch.object(Zotify, "get_request", return_value=cover),
                  patch.object(api.Track, "download", download), patch.object(worker, "_verify_file")):
                result = worker.download_result(self.session, api, URI, self.staging, "ogg", quality)
            self.assertEqual(result["quality"], quality)
            self.assertEqual([call.args[0].gid for call in load.call_args_list], [self.track.gid, good.gid])
            self.assertEqual([call.args[1].get_file(call.args[0].file).format for call in load.call_args_list],
                             [requested, requested])

    def test_quality_missing_does_not_hide_retryable_audio_or_metadata_failures(self):
        from zotify.config import Zotify

        alternative = self.md.Track()
        alternative.CopyFrom(self.track)
        alternative.gid = b"a" * 16
        alternative.file[0].format = self.md.AudioFile.OGG_VORBIS_320
        self.track.alternative.add(gid=alternative.gid)
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        for failure in (RuntimeError("Failed fetching audio key! secret"), ConnectionRefusedError("secret")):
            self.session.api().get_metadata_4_track.side_effect = [self.track, alternative]
            load = self.session.content_feeder().load_track
            load.reset_mock()
            load.side_effect = failure
            with (self.subTest(failure=type(failure)), patch.object(Zotify, "get_request", return_value=cover),
                  self.assertRaises(worker.WorkerFailure) as caught):
                worker.download_result(self.session, self.api, URI, self.staging, "ogg", "very_high")
            self.assertEqual(caught.exception.status, "error")
            self.assertNotIn("secret", caught.exception.message)
            load.assert_called_once()
            self.assertEqual(load.call_args.args[1].get_file(alternative.file).format, self.md.AudioFile.OGG_VORBIS_320)
        self.session.api().get_metadata_4_track.side_effect = [self.track, ConnectionError("secret")]
        self.session.content_feeder.reset_mock()
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg", "very_high")
        self.assertEqual(caught.exception.status, "error")
        self.session.content_feeder.assert_not_called()

    def test_explicit_quality_restrictions_stay_unavailable_and_fileless_stays_retryable(self):
        from zotify.config import Zotify

        self.track.restriction.add(countries_forbidden="US")
        for quality in ("normal", "high", "very_high"):
            with self.subTest(quality=quality), self.assertRaises(worker.WorkerFailure) as caught:
                worker.download_result(self.session, self.api, URI, self.staging, "ogg", quality)
            self.assertEqual(caught.exception.status, "unavailable")
        self.track.ClearField("restriction")
        self.track.ClearField("file")
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg", "high")
        self.assertEqual(caught.exception.status, "error")
        self.session.content_feeder.assert_not_called()

    def test_source_quality_highest_available_and_free_cap(self):
        from zotify.config import Zotify
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        self.track.file[0].format = self.md.AudioFile.OGG_VORBIS_96
        self.track.file.add(file_id=b"x" * 20, format=self.md.AudioFile.OGG_VORBIS_320)
        self.track.file.add(file_id=b"y" * 20, format=self.md.AudioFile.OGG_VORBIS_160)

        def load(raw, quality, preload, listener):
            selected.append(quality.get_file(raw.file).format)
            raise RuntimeError("stop before network")

        selected = []
        self.session.content_feeder().load_track.side_effect = load
        for subscription in ("premium", "free"):
            self.session.get_user_attribute.return_value = subscription
            with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure):
                worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(selected, [self.md.AudioFile.OGG_VORBIS_320, self.md.AudioFile.OGG_VORBIS_160])

    def test_saved_credential_rejection_and_corruption(self):
        from librespot.core import Session
        from librespot.proto.Keyexchange_pb2 import APLoginFailed, ErrorCode

        worker._save_credentials(self.state, self.session)
        with patch.object(worker, "_builder") as builder, self.assertRaises(worker.WorkerFailure) as caught:
            builder.return_value.stored.return_value.create.side_effect = Session.SpotifyAuthenticationException(
                APLoginFailed(error_code=ErrorCode.BadCredentials))
            worker.saved_session(self.state)
        self.assertEqual(caught.exception.status, "auth_required")
        for credentials in ([], {}, {"username": "user", "type": "AUTHENTICATION_STORED_SPOTIFY_CREDENTIALS", "credentials": "not base64"}):
            with patch.object(worker.json, "loads", return_value=credentials), self.assertRaises(worker.WorkerFailure) as caught:
                worker.saved_session(self.state)
            self.assertEqual(caught.exception.status, "auth_required")

    def test_fresh_subprocess_has_no_home_music_or_config_side_effects(self):
        home = self.root / "home"
        home.mkdir()
        command = {"command": "probe", "state_dir": str(self.state)}
        result = subprocess.run([sys.executable, "-m", "spotisync.worker"], input=json.dumps(command),
                                capture_output=True, text=True, timeout=20, env={**os.environ, "HOME": str(home)})
        self.assertEqual(json.loads(result.stdout)["status"], "auth_required")
        self.assertEqual(list(home.iterdir()), [])
        self.assertEqual(list(self.state.iterdir()), [])

    def test_rate_limit_date_and_invalid_headers(self):
        import requests
        response = requests.Response()
        response.status_code = 429
        for header in ("Wed, 07 Oct 2099 12:00:00 GMT", "nan", "-1", "invalid"):
            response.headers["Retry-After"] = header
            with (patch.object(requests.sessions.Session, "send", return_value=response), worker._http_scope(),
                  self.assertRaises(worker.WorkerFailure) as caught):
                requests.get("https://example.invalid")
            if "2099" in header:
                self.assertGreater(caught.exception.retry_after, 0)
            else:
                self.assertIsNone(caught.exception.retry_after)

    def test_metadata_rate_limit_propagates_without_erasing_members(self):
        self.playlist()
        self.session.api().get_metadata_4_track.side_effect = worker.WorkerFailure("blocked", "Rate limit", 12)
        with self.assertRaises(worker.WorkerFailure) as caught:
            worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
        self.assertEqual(caught.exception.retry_after, 12)

    def test_member_identity_required_for_complete_snapshot(self):
        for value in ("", "spotify:unresolved:raw", "spotify:track:short", "not a URI", "spotify:local:broken"):
            with self.subTest(value=value):
                self.playlist().contents.items[-1].uri = value
                result = worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
                self.assertFalse(result["complete"])
                self.assertEqual(len(result["tracks"]), 4)
        self.playlist().contents.items[-1].uri = URI
        self.assertTrue(worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)["complete"])
        self.album.disc[0].track[0].ClearField("gid")
        result = worker.metadata_result(self.session, "spotify:album:" + ALBUM_ID)
        self.assertFalse(result["complete"])
        self.assertEqual(len(result["tracks"]), 1)

    def test_album_type_preserved_in_api_payload(self):
        self.album.type = self.md.Album.COMPILATION
        payload = worker._zotify_response(self.album, "album", "spotify:album:" + ALBUM_ID)
        self.assertEqual(payload["type"], "album")
        self.assertEqual(payload["album_type"], "compilation")
        album = self.api.Album("album:" + ALBUM_ID)
        album.parse_metadata(None, payload)
        self.assertTrue(album.compilation)

    def test_supported_alternatives_exhausted_before_unavailable(self):
        from zotify.config import Zotify

        alternatives = []
        for number in (3, 4):
            alternative = self.md.Track()
            alternative.CopyFrom(self.track)
            alternative.gid = bytes.fromhex("00" * 15 + f"{number:02x}")
            alternatives.append(alternative)
            self.track.alternative.add(gid=alternative.gid)
        original = self.track
        self.session.api().get_metadata_4_track.side_effect = [original, *alternatives]
        self.session.content_feeder().load_track.side_effect = [RuntimeError("Content is restricted!"),
                                                               RuntimeError("Content is restricted!"),
                                                               RuntimeError("conversion sentinel")]
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")
        calls = self.session.content_feeder().load_track.call_args_list
        self.assertEqual([call.args[0].gid for call in calls], [original.gid, *(a.gid for a in alternatives)])

    def test_raw_restriction_with_files_uses_playable_alternative(self):
        from zotify.config import Zotify

        alternative = self.md.Track()
        alternative.CopyFrom(self.track)
        alternative.gid = b"a" * 16
        self.track.restriction.add(countries_forbidden="US")
        self.track.alternative.add(gid=alternative.gid)
        self.session.api().get_metadata_4_track.side_effect = [self.track, alternative]
        self.session.content_feeder().load_track.side_effect = RuntimeError("stop after source selection")
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")
        self.assertEqual(self.session.content_feeder().load_track.call_args.args[0].gid, alternative.gid)
        self.session.content_feeder().load_track.assert_called_once()

    def test_unresolved_alternative_never_permanently_unavailable(self):
        from zotify.config import Zotify

        self.track.alternative.add(gid=b"a" * 16)
        self.session.api().get_metadata_4_track.side_effect = [self.track, ConnectionError("secret")]
        self.session.content_feeder().load_track.side_effect = RuntimeError("Content is restricted!")
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")

    def test_all_alternatives_confirmed_restricted(self):
        from zotify.config import Zotify

        alternative = self.md.Track()
        alternative.CopyFrom(self.track)
        alternative.gid = b"a" * 16
        self.track.alternative.add(gid=alternative.gid)
        self.session.api().get_metadata_4_track.side_effect = [self.track, alternative]
        self.session.content_feeder().load_track.side_effect = RuntimeError("Content is restricted!")
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "unavailable")
        self.assertEqual(self.session.content_feeder().load_track.call_count, 2)

    def test_unsupported_alternative_keeps_failure_retryable(self):
        from zotify.config import Zotify

        alternative = self.md.Track()
        alternative.CopyFrom(self.track)
        alternative.gid = b"a" * 16
        alternative.file[0].format = self.md.AudioFile.MP3_320
        self.track.alternative.add(gid=alternative.gid)
        self.session.api().get_metadata_4_track.side_effect = [self.track, alternative]
        self.session.content_feeder().load_track.side_effect = RuntimeError("Content is restricted!")
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with patch.object(Zotify, "get_request", return_value=cover), self.assertRaises(worker.WorkerFailure) as caught:
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(caught.exception.status, "error")
        self.session.content_feeder().load_track.assert_called_once()

    def test_extended_metadata_inner_errors_reach_stock_api_and_members(self):
        import requests
        from librespot.core import ApiClient, BatchedExtensionResponse
        from librespot.metadata import TrackId

        api = ApiClient.__new__(ApiClient)
        client = requests.Session()
        self.addCleanup(client.close)
        api._ApiClient__session = SimpleNamespace(client=lambda: client)
        api.build_request = lambda *args: requests.Request("POST", "https://example.invalid/extended-metadata/v0/extended-metadata").prepare()
        for code, status in ((401, "auth_required"), (403, "blocked"), (429, "blocked")):
            raw = BatchedExtensionResponse()
            entry = raw.extended_metadata.add().extension_data.add(entity_uri=URI)
            entry.header.status_code = code
            response = requests.Response()
            response.status_code = 200
            response._content = raw.SerializeToString()
            response.headers["Retry-After"] = "17"
            with (self.subTest(code=code), patch.object(requests.sessions.Session, "send", return_value=response),
                  worker._http_scope(), self.assertRaises(worker.WorkerFailure) as caught):
                api.get_metadata_4_track(TrackId.from_base62(TRACK_ID))
            self.assertEqual(caught.exception.status, status)
            if code == 429:
                self.assertEqual(caught.exception.retry_after, 17)
            self.playlist().contents.items[-1].uri = URI
            self.session.api().get_metadata_4_track.side_effect = lambda ident: api.get_metadata_4_track(ident)
            with (patch.object(requests.sessions.Session, "send", return_value=response), worker._http_scope(),
                  self.assertRaises(worker.WorkerFailure) as caught):
                worker.metadata_result(self.session, "spotify:playlist:" + TRACK_ID)
            self.assertEqual(caught.exception.status, status)

    def test_login5_structured_errors_and_success_through_token_provider(self):
        import requests
        from librespot.core import Login5, TokenProvider

        self.session.device_id.return_value = "device"
        self.session.username.return_value = "user"
        self.session.ap_welcome.return_value = SimpleNamespace(reusable_auth_credentials=b"credential")
        for code, status in ((Login5.INVALID_CREDENTIALS, "auth_required"), (Login5.UNKNOWN_IDENTIFIER, "auth_required"),
                             (Login5.TOO_MANY_ATTEMPTS, "blocked"), (Login5.TRY_AGAIN_LATER, "blocked"),
                             (Login5.TIMEOUT, "error")):
            response = requests.Response()
            response.status_code = 200
            response._content = Login5.LoginResponse(error=code).SerializeToString()
            response.headers["Retry-After"] = "23"
            provider = TokenProvider(self.session)
            provider._TokenProvider__tokens = []
            with (self.subTest(code=code), patch.object(requests.sessions.Session, "send", return_value=response),
                  worker._http_scope(), self.assertRaises(worker.WorkerFailure) as caught):
                provider.get("playlist-read")
            self.assertEqual(caught.exception.status, status)
            self.assertNotIn("credential", caught.exception.message.replace("credentials", ""))
            if status == "blocked":
                self.assertEqual(caught.exception.retry_after, 23)
        raw = Login5.LoginResponse()
        raw.ok.access_token = "token"
        raw.ok.access_token_expires_in = 3600
        response._content = raw.SerializeToString()
        with patch.object(requests.sessions.Session, "send", return_value=response), worker._http_scope():
            self.assertEqual(provider.get("playlist-read"), "token")

    def chunk_reader(self, data, fetch=None):
        from librespot.audio import CdnManager

        chunk_size = 128 * 1024
        count = (len(data) + chunk_size - 1) // chunk_size
        executor = ThreadPoolExecutor(max_workers=3)
        self.addCleanup(executor.shutdown, wait=True)
        streamer = SimpleNamespace(size=len(data), chunks=count, buffer=[b""] * count,
                                   available=[False] * count, requested=[False] * count,
                                   executor_service=executor, halt_listener=None)
        reader = CdnManager.Streamer.InternalStream(streamer, False)
        self.addCleanup(lambda: reader.close() if not reader.closed else None)

        def request(index):
            value = fetch(index) if fetch else data[index * chunk_size:(index + 1) * chunk_size]
            if not reader.closed:
                streamer.buffer[index] = value
                reader.notify_chunk_available(index)

        streamer.request_chunk = request
        return reader

    def test_chunk_reader_boundaries_and_no_positive_read_after_eof(self):
        data = b"a" * (128 * 1024) + b"b" * (128 * 1024)
        reader = self.chunk_reader(data)
        with worker._stream_scope():
            self.assertEqual(reader.read(128 * 1024 + 1), data[:128 * 1024 + 1])
            self.assertEqual(reader.read(128 * 1024), data[128 * 1024 + 1:])
            self.assertEqual(reader.read(20000), b"")
            self.assertEqual(reader.read(), b"")
            self.assertEqual(reader.pos(), len(data))

    def test_chunk_async_http_errors_preserve_status_and_retry_after(self):
        import requests

        for code, status in ((401, "auth_required"), (429, "blocked")):
            response = requests.Response()
            response.status_code = code
            response.headers["Retry-After"] = "31"
            reader = self.chunk_reader(b"x" * (128 * 1024), lambda index: requests.get("https://example.invalid").content)
            start = time.monotonic()
            with (self.subTest(code=code), patch.object(requests.sessions.Session, "send", return_value=response),
                  worker._http_scope(), worker._stream_scope(), self.assertRaises(worker.WorkerFailure) as caught):
                reader.read(20000)
            self.assertLess(time.monotonic() - start, 1)
            self.assertEqual(caught.exception.status, status)
            if code == 429:
                self.assertEqual(caught.exception.retry_after, 31)

    def test_chunk_stall_timeout_and_premature_eof(self):
        release = threading.Event()
        reader = self.chunk_reader(b"x" * (128 * 1024), lambda index: release.wait(2) and b"x")
        start = time.monotonic()
        try:
            with patch.object(worker, "CHUNK_TIMEOUT", 0.05), worker._stream_scope(), self.assertRaises(worker.WorkerFailure) as caught:
                reader.read(1)
            self.assertIn("timed out", caught.exception.message)
            self.assertLess(time.monotonic() - start, 0.5)
        finally:
            release.set()
        reader = self.chunk_reader(b"x" * (128 * 1024), lambda index: b"short")
        with worker._stream_scope(), self.assertRaises(worker.WorkerFailure) as caught:
            reader.read(20000)
        self.assertIn("truncated", caught.exception.message)

    def test_prefetched_terminal_failure_does_not_wait_on_other_chunks(self):
        release = threading.Event()

        def fetch(index):
            if index == 1:
                raise worker.WorkerFailure("blocked", "Rate limit", 7)
            release.wait(2)
            return b"x" * (128 * 1024)

        reader = self.chunk_reader(b"x" * (256 * 1024), fetch)
        start = time.monotonic()
        try:
            with worker._stream_scope(), self.assertRaises(worker.WorkerFailure) as caught:
                reader.read(1)
            self.assertEqual(caught.exception.retry_after, 7)
            self.assertLess(time.monotonic() - start, 0.5)
        finally:
            release.set()

    def test_download_stops_at_declared_chunk_aligned_eof(self):
        from zotify.config import Zotify

        data = b"x" * (128 * 1024)
        reader = self.chunk_reader(data)
        self.session.content_feeder().load_track.return_value = SimpleNamespace(
            input_stream=SimpleNamespace(size=len(data), stream=lambda: reader))
        cover = SimpleNamespace(content=b"cover", raise_for_status=lambda: None)
        with (patch.object(Zotify, "get_request", return_value=cover),
              patch.object(self.api.DLContent, "run_ffm", side_effect=RuntimeError("stop after fetch")),
              self.assertRaises(RuntimeError)):
            worker.download_result(self.session, self.api, URI, self.staging, "ogg")
        self.assertEqual(reader.pos(), len(data))
        self.assertEqual(next(self.staging.glob("*.tmp")).stat().st_size, len(data))

    def test_oauth_slow_request_does_not_block_valid_callback(self):
        from librespot.core import OAuth

        events, errors, results = [], [], []

        def authenticate():
            try:
                results.append(worker.authenticate(self.state, events.append))
            except BaseException as exc:
                errors.append(exc)

        with (self.oauth_listener() as listener, patch.object(OAuth, "request_token"), patch.object(OAuth, "get_credentials", return_value="login"),
              patch.object(worker, "_builder") as builder, patch.object(worker, "AUTH_CONNECTION_TIMEOUT", 0.1)):
            builder.return_value.create.return_value = self.session
            thread = threading.Thread(target=authenticate, daemon=True)
            thread.start()
            deadline = time.monotonic() + 3
            while not events and not errors and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(events, errors)
            state = parse_qs(urlsplit(events[0]["url"]).query)["state"][0]
            with socket.create_connection(("127.0.0.1", listener.port), timeout=1) as slow:
                slow.sendall(b"GET /login HTTP/1.1\r\nHost:")
                time.sleep(0.2)
                with urlopen(f"http://127.0.0.1:{listener.port}/login?code=test&state=" + state, timeout=1) as response:
                    self.assertEqual(response.status, 200)
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(results, [self.session])

    def test_oauth_request_cannot_outlive_auth_deadline(self):
        from librespot.core import OAuth

        events, errors = [], []

        def authenticate():
            try:
                worker.authenticate(self.state, events.append)
            except BaseException as exc:
                errors.append(exc)

        with (self.oauth_listener() as listener, patch.object(worker, "AUTH_TIMEOUT", 0.15),
              patch.object(OAuth, "request_token") as token):
            thread = threading.Thread(target=authenticate, daemon=True)
            thread.start()
            deadline = time.monotonic() + 2
            while not events and not errors and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(events, errors)
            with socket.create_connection(("127.0.0.1", listener.port), timeout=1) as slow:
                slow.sendall(b"GET /login HTTP/1.1\r\nHost:")
                # Continuous trickle must not reset absolute authentication deadline.
                for _ in range(8):
                    if not thread.is_alive():
                        break
                    time.sleep(0.03)
                    try:
                        slow.sendall(b"x")
                    except OSError:
                        break
                thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertIsInstance(errors[0], worker.WorkerFailure)
            self.assertIn("timed out", errors[0].message)
            token.assert_not_called()

    def test_oauth_listener_binding_is_explicit(self):
        for address in ("127.0.0.1", "0.0.0.0"):
            events = []
            with (self.subTest(address=address), self.oauth_listener() as listener,
                  patch.dict(os.environ, {"SPOTISYNC_OAUTH_BIND": address}), patch.object(worker, "AUTH_TIMEOUT", 0),
                  self.assertRaises(worker.WorkerFailure)):
                worker.authenticate(self.state, events.append)
            self.assertEqual(listener.addresses[-1], (address, 4381))
            self.assertNotEqual(listener.port, 4381)
            self.assertEqual(parse_qs(urlsplit(events[0]["url"]).query)["redirect_uri"], ["http://127.0.0.1:4381/login"])
        with patch.dict(os.environ, {"SPOTISYNC_OAUTH_BIND": "192.0.2.1"}), self.assertRaises(worker.WorkerFailure):
            worker.authenticate(self.state, lambda event: None)


if __name__ == "__main__":
    unittest.main()
