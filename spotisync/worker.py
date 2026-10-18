"""One-command, isolated adapter for Zotify 0.21.1.

Run with ``python -m spotisync.worker`` and send one JSON object on stdin.
All stdout lines are protocol events; upstream diagnostic text is never trusted.
"""

from __future__ import annotations

import base64
import contextlib
from concurrent.futures import TimeoutError as FutureTimeout
import hashlib
import io
import json
import logging
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import MethodType, SimpleNamespace
from urllib.parse import parse_qs, unquote, urlencode, urlsplit
from unittest.mock import patch


CHUNK_TIMEOUT = 30.0
AUTH_TIMEOUT = 180.0
AUTH_CONNECTION_TIMEOUT = 5.0
AP_CONNECTION_TIMEOUT = 10.0
AP_CONNECTION_BUDGET = 30.0


class WorkerFailure(Exception):
    def __init__(self, status: str, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.retry_after = retry_after

    def result(self) -> dict:
        result = {"status": self.status, "message": self.message}
        if self.retry_after is not None:
            result["retry_after"] = self.retry_after
        return result


def normalize_uri(value: object, *, track_only: bool = False) -> str:
    if not isinstance(value, str):
        raise WorkerFailure("error", "uri must be a Spotify track, album, or playlist URL/URI.")
    value = value.strip()
    if value.startswith("https://"):
        parsed = urlsplit(value)
        if parsed.netloc != "open.spotify.com" or parsed.fragment:
            raise WorkerFailure("error", "Unsupported Spotify URL.")
        path = parsed.path.strip("/")
        path = re.sub(r"^intl-[a-z]{2}/", "", path)
        value = "spotify:" + path.replace("/", ":")
    match = re.fullmatch(r"spotify:(track|album|playlist):([A-Za-z0-9]{22})", value)
    if not match or (track_only and match[1] != "track"):
        raise WorkerFailure("error", "Expected a Spotify track URI." if track_only else
                            "Expected a Spotify track, album, or playlist URL/URI.")
    return value


def _path(command: dict, name: str) -> Path:
    value = command.get(name)
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise WorkerFailure("error", f"{name} must be a nonempty filesystem path.")
    return Path(os.path.abspath(Path(value).expanduser()))


class _SafeLog(io.TextIOBase):
    """Do not forward arbitrary upstream text: it can contain tokens or auth data."""

    def __init__(self, destination):
        self.destination = destination
        self.reported = False

    def write(self, text):
        if text.strip() and not self.reported:
            self.destination.write("Upstream diagnostic output suppressed for credential safety.\n")
            self.destination.flush()
            self.reported = True
        return len(text)

    def flush(self):
        self.destination.flush()


def _check_status(status: int, headers):
    if status == 429:
        delay = None
        header = headers.get("Retry-After")
        if header:
            try:
                delay = float(header)
            except ValueError:
                try:
                    delay = (parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    pass
        if delay is not None and (not math.isfinite(delay) or delay < 0):
            delay = None
        raise WorkerFailure("blocked", "Spotify rate limit reached.", delay)
    if status == 401:
        raise WorkerFailure("auth_required", "Spotify rejected authentication.")
    if status == 403:
        raise WorkerFailure("blocked", "Spotify denied this request; retry later.")


@contextlib.contextmanager
def _http_scope():
    """Inspect HTTP and protobuf errors before upstream loses their structure."""
    import requests
    from librespot.core import BatchedExtensionResponse, Login5

    original = requests.sessions.Session.send

    def send(session, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = 30
        response = original(session, request, **kwargs)
        _check_status(response.status_code, response.headers)
        path = urlsplit(request.url).path
        if response.status_code == 200 and path == "/extended-metadata/v0/extended-metadata":
            raw = BatchedExtensionResponse.FromString(response.content)
            for extension in raw.extended_metadata:
                for entry in extension.extension_data:
                    _check_status(entry.header.status_code, response.headers)
        elif response.status_code == 200 and urlsplit(request.url).hostname == "login5.spotify.com":
            raw = Login5.LoginResponse.FromString(response.content)
            if raw.error != Login5.UNKNOWN_ERROR:
                if raw.error in {Login5.INVALID_CREDENTIALS, Login5.UNKNOWN_IDENTIFIER}:
                    raise WorkerFailure("auth_required", "Spotify rejected saved Login5 credentials.")
                if raw.error in {Login5.TOO_MANY_ATTEMPTS, Login5.TRY_AGAIN_LATER}:
                    _check_status(429, response.headers)
                raise WorkerFailure("error", "Spotify Login5 authentication failed; retry later.")
            if not raw.HasField("ok") or not raw.ok.access_token:
                raise WorkerFailure("error", "Spotify Login5 returned no access token; retry later.")
        return response

    with patch.object(requests.sessions.Session, "send", send):
        yield


@contextlib.contextmanager
def _accesspoint_scope():
    """Keep TCP failover active through session close, including stock reconnects."""
    import requests
    from librespot.core import ApResolver, Session

    endpoints = []
    deadline = 0.0

    def failure():
        return WorkerFailure("error", "Could not connect to Spotify access points; retry later.")

    def resolve():
        nonlocal endpoints, deadline
        endpoints = []
        deadline = time.monotonic() + AP_CONNECTION_BUDGET
        try:
            # Use the pinned resolver URL, not a fixed AP hostname or IP. One lookup
            # per connection cycle avoids stale pools without retrying HTTP failures.
            with requests.get(ApResolver.base_url + "?type=accesspoint",
                              timeout=min(AP_CONNECTION_TIMEOUT, AP_CONNECTION_BUDGET),
                              allow_redirects=False) as response:
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError()
                pool = response.json()
            addresses = pool.get("accesspoint") if isinstance(pool, dict) else None
            if not isinstance(addresses, list):
                raise ValueError()
            for address in addresses:
                if not isinstance(address, str):
                    continue
                match = re.fullmatch(r"([A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?):([0-9]{1,5})", address)
                if not match or not 1 <= int(match[2]) <= 65535:
                    continue
                host = match[1].lower()
                if len(host) > 253 or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                                          for label in host.split(".")):
                    continue
                endpoint = (host, int(match[2]))
                if endpoint not in endpoints:
                    endpoints.append(endpoint)
        except WorkerFailure:
            raise
        except (requests.RequestException, ValueError, TypeError):
            raise failure() from None
        endpoints.sort(key=lambda endpoint: endpoint[1] != 443)
        if not endpoints or time.monotonic() >= deadline:
            raise failure() from None
        host, port = endpoints[0]
        return f"{host}:{port}"

    def create(address, conf):
        # Builder and reconnect both select through resolve() immediately before
        # create(). Retry only TCP establishment, never authentication or a track.
        for endpoint in endpoints:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock = None
            try:
                # create_connection closes sockets for failed connect attempts.
                sock = socket.create_connection(endpoint, timeout=min(AP_CONNECTION_TIMEOUT, remaining))
                sock.settimeout(None)  # Receiver must not time out while idle.
                return Session.ConnectionHolder(sock)
            except OSError:
                if sock is not None:
                    sock.close()
            except BaseException:
                if sock is not None:
                    sock.close()
                raise
        raise failure() from None

    with (patch.object(ApResolver, "get_random_accesspoint", resolve),
          patch.object(Session.ConnectionHolder, "create", create)):
        yield


@contextlib.contextmanager
def _stream_scope():
    """Bound demanded chunks and retain asynchronous failures, including prefetched chunks."""
    from librespot.audio import CdnManager

    stream_class = CdnManager.Streamer.InternalStream
    chunk_size = 128 * 1024

    def request_chunk(self, index):
        futures = self.__dict__.setdefault("_worker_futures", {})
        if index not in futures:
            self.requested_chunks()[index] = True
            futures[index] = self.streamer.executor_service.submit(self.streamer.request_chunk, index)

    def check_errors(self):
        for future in self.__dict__.get("_worker_futures", {}).values():
            if future.done():
                try:
                    future.result()
                except WorkerFailure:
                    raise
                except Exception:
                    raise WorkerFailure("error", "Source audio chunk fetch failed; retry later.") from None

    def check_availability(self, chunk, wait, halted):
        check_errors(self)
        if self.closed:
            raise WorkerFailure("error", "Source audio stream closed before completion.")
        if chunk == self.chunks() and self.pos() == self.size():
            return
        if not 0 <= chunk < self.chunks():
            raise WorkerFailure("error", "Source audio chunk index is invalid.")
        for index in range(chunk, min(self.chunks(), chunk + 3)):
            if not self.available_chunks()[index]:
                request_chunk(self, index)
        if not wait:
            return
        deadline = time.monotonic() + CHUNK_TIMEOUT
        while not self.available_chunks()[chunk]:
            check_errors(self)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WorkerFailure("error", "Source audio chunk fetch timed out; retry later.")
            future = self._worker_futures[chunk]
            try:
                future.result(timeout=min(0.1, remaining))
            except FutureTimeout:
                if future.done():
                    check_errors(self)
                continue
            except WorkerFailure:
                raise
            except Exception:
                raise WorkerFailure("error", "Source audio chunk fetch failed; retry later.") from None
            if not self.available_chunks()[chunk]:
                raise WorkerFailure("error", "Source audio chunk completed without data.")
        check_errors(self)

    def read(self, size=0):
        check_errors(self)
        remaining = self.size() - self.pos()
        if remaining == 0:
            return b""
        if remaining < 0 or self.closed:
            raise WorkerFailure("error", "Source audio stream position is invalid.")
        needed = remaining if size <= 0 else min(size, remaining)
        parts = []
        while needed:
            position = self.pos()
            index, offset = divmod(position, chunk_size)
            check_availability(self, index, True, False)
            data = self.buffer()[index]
            count = min(needed, chunk_size - offset, len(data) - offset)
            if count <= 0:
                raise WorkerFailure("error", "Source audio stream was truncated; retry later.")
            parts.append(data[offset:offset + count])
            self._AbsChunkedInputStream__pos += count
            needed -= count
        return b"".join(parts)

    with (patch.object(stream_class, "request_chunk_from_stream", request_chunk),
          patch.object(stream_class, "check_availability", check_availability),
          patch.object(stream_class, "read", read)):
        yield


def initialize(state_dir: Path, root: Path, fmt: str = "ogg", quality: str = "auto"):
    # api.py creates class-level paths during import. Never call Zotify.boot/load.
    from zotify.config import CONFIG_VALUES, Config, Zotify
    from zotify.const import DEFAULT, TYPE

    values = {}
    for key, setup in CONFIG_VALUES.items():
        value = setup[DEFAULT]
        cast = setup[TYPE]
        values[key] = str(value).lower() == "true" if cast is bool else cast(value)
    values.update({
        "DEBUG": False, "ROOT_PATH": str(root), "ROOT_PODCAST_PATH": str(root),
        "CREDENTIALS_LOCATION": str(state_dir / "credentials.json"),
        "API_CREDENTIALS_LOCATION": str(state_dir / "api_credentials.json"),
        "SONG_ARCHIVE_LOCATION": str(root / ".song_archive"),
        "TEMP_DOWNLOAD_DIR": str(root), "OUTPUT": "{id}",
        "DOWNLOAD_FORMAT": fmt, "DOWNLOAD_QUALITY": quality, "API_CLIENT_ID": "",
        "API_CLIENT_LEGACY": False, "RETRY_ATTEMPTS": 0, "RETRY_DELAY": 0.0,
        "FETCH_DELAY": 0.0, "BULK_WAIT_TIME": 0.0, "OPTIMIZED_DOWNLOADING": False,
        "DISABLE_SONG_ARCHIVE": True, "DISABLE_DIRECTORY_ARCHIVES": True,
        "SKIP_EXISTING": False, "SKIP_PREVIOUSLY_DOWNLOADED": False,
        "LYRICS_TO_METADATA": False, "LYRICS_TO_FILE": False,
        "ALWAYS_CHECK_LYRICS": False, "ALBUM_ART_TO_FILE": False,
        "EXPORT_M3U8": False, "LIKED_SONGS_ARCHIVE_M3U8": False,
        "IMPORT_ZMD": False, "EXPORT_ZMD": False, "MD_ALLGENRES": True,
        "MD_ARTISTDELIMITER": "", "MD_GENREDELIMITER": "",
    })
    values.update({key: False for key in values if key.startswith("PRINT_")})
    Config.Values = values
    Zotify.TOTAL_API_CALLS = 0
    Zotify.FORCE_LIBRE_METADATA = True
    from zotify import api

    # Also support repeated offline invocations in one test interpreter.
    api.Content._path_root = root
    api.Episode._path_root = root
    api.Show._path_root = root
    api.Track._codec = {"ogg": "copy", "mp3": "libmp3lame", "aac": "aac"}[fmt]
    api.Track._ext = {"ogg": "ogg", "mp3": "mp3", "aac": "m4a"}[fmt]
    api.HierarchicalNode.ALL_NODES.clear()
    api.ParentStack.SKIPPABLE_CACHE.clear()
    api.ParentStack.PBARS = []
    return api


def _builder(state_dir: Path):
    from librespot.core import Session

    conf = Session.Configuration.Builder().build()
    conf.cache_enabled = False
    conf.cache_dir = str(state_dir / "cache")
    conf.do_cache_clean_up = False
    conf.store_credentials = False
    conf.stored_credentials_file = str(state_dir / "credentials.json")
    conf.retry_on_chunk_error = False
    return Session.Builder(conf).set_device_name("SpotiSync")


def saved_session(state_dir: Path):
    from librespot.core import Session
    from librespot.proto.Authentication_pb2 import AuthenticationType

    try:
        path = state_dir / "credentials.json"
        if path.is_symlink():
            raise ValueError()
        creds = json.loads(path.read_text())
        if (not isinstance(creds, dict) or not isinstance(creds.get("username"), str)
                or not creds["username"] or creds.get("type") !=
                AuthenticationType.Name(AuthenticationType.AUTHENTICATION_STORED_SPOTIFY_CREDENTIALS)):
            raise ValueError()
        if not base64.b64decode(creds["credentials"], validate=True):
            raise ValueError()
        path.chmod(0o600)
    except (OSError, ValueError, KeyError, TypeError):
        raise WorkerFailure("auth_required", "Saved Spotify credentials missing or invalid.") from None
    try:
        return _builder(state_dir).stored(base64.b64encode(json.dumps(creds).encode())).create()
    except Session.SpotifyAuthenticationException:
        raise WorkerFailure("auth_required", "Saved Spotify credentials rejected; authenticate again.") from None


def _save_credentials(state_dir: Path, session):
    fd, filename = tempfile.mkstemp(prefix=".credentials-", suffix=".json", dir=state_dir)
    try:
        with os.fdopen(fd, "w") as file:
            json.dump(session.credentials(), file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(filename, state_dir / "credentials.json")
    finally:
        if os.path.exists(filename):
            os.unlink(filename)


def authenticate(state_dir: Path, emit):
    from librespot.core import MercuryRequests, OAuth, Session

    oauth = OAuth(MercuryRequests.keymaster_client_id, "http://127.0.0.1:4381/login", None)
    state = secrets.token_urlsafe(32)
    outcome = {}

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.rfile = DeadlineReader(self.rfile, self.connection,
                                        min(self.server.auth_deadline, time.monotonic() + AUTH_CONNECTION_TIMEOUT))

        def do_GET(self):
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            valid_state = query.get("state", [])
            valid = (parsed.path == "/login" and len(valid_state) == 1
                     and valid_state[0].isascii()
                     and secrets.compare_digest(valid_state[0], state))
            codes = query.get("code", [])
            errors = query.get("error", [])
            if valid and len(errors) == 1 and errors[0]:
                outcome["denied"] = True
            elif valid and len(codes) == 1 and codes[0] and not errors:
                outcome["code"] = codes[0]
            else:
                valid = False
            remaining = self.rfile.deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError()
            self.connection.settimeout(remaining)
            self.send_response(200 if valid else 400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b"SpotiSync login received. You may close this window." if valid
                             else b"Invalid OAuth callback.")

        def log_message(self, *args):
            pass

    class DeadlineReader:
        def __init__(self, reader, connection, deadline):
            self.reader, self.connection, self.deadline = reader, connection, deadline

        def readline(self, limit=-1):
            data = bytearray()
            while limit < 0 or len(data) < limit:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError()
                self.connection.settimeout(remaining)
                byte = self.reader.read(1)
                data.extend(byte)
                if not byte or byte == b"\n":
                    break
            return bytes(data)

        def close(self):
            self.reader.close()

    class CallbackServer(HTTPServer):
        def get_request(self):
            connection, address = super().get_request()
            connection.settimeout(max(0.001, min(AUTH_CONNECTION_TIMEOUT, self.auth_deadline - time.monotonic())))
            return connection, address

    # Generate through stock librespot so its verifier is used by request_token().
    with patch.object(OAuth, "_OAuth__generate_generate_code_verifier",
                      lambda self: secrets.token_urlsafe(64)):
        url = oauth.get_auth_url() + "&" + urlencode({"state": state})
    bind = os.environ.get("SPOTISYNC_OAUTH_BIND", "127.0.0.1")
    if bind not in {"127.0.0.1", "0.0.0.0"}:
        raise WorkerFailure("error", "SPOTISYNC_OAUTH_BIND must be 127.0.0.1 or 0.0.0.0.")
    with CallbackServer((bind, 4381), Handler) as server:
        server.timeout = 1
        server.auth_deadline = time.monotonic() + AUTH_TIMEOUT
        # Bound before emission: browser callback cannot race server startup.
        emit({"event": "auth_url", "url": url})
        while not outcome:
            if time.monotonic() >= server.auth_deadline:
                raise WorkerFailure("error", "Spotify authentication timed out.")
            server.handle_request()
    if outcome.get("denied"):
        raise WorkerFailure("auth_required", "Spotify authorization was declined.")
    oauth.set_code(outcome["code"]).request_token()
    builder = _builder(state_dir)
    builder.login_credentials = oauth.get_credentials()
    try:
        session = builder.create()
    except Session.SpotifyAuthenticationException:
        raise WorkerFailure("auth_required", "Spotify rejected authentication.") from None
    try:
        _save_credentials(state_dir, session)
    except BaseException:
        session.close()
        raise
    return session


def _raw(session, kind: str, uri: str):
    from librespot import metadata

    ident = getattr(metadata, kind.title() + "Id").from_base62(uri.rsplit(":", 1)[1])
    api = session.api()
    return api.get_playlist(ident) if kind == "playlist" else getattr(api, f"get_metadata_4_{kind}")(ident)


def _gid_uri(proto, kind: str = "track") -> str | None:
    from librespot.metadata import Id

    gid = getattr(proto, "gid", b"")
    if len(gid) != 16:
        return None
    return f"spotify:{kind}:" + Id.b62.encode(gid).decode()


def _revision(proto) -> str:
    return hashlib.sha256(proto.SerializePartialToString(deterministic=True)).hexdigest()


def _availability(proto, session, *, consider_alternatives=True) -> str:
    # Fileless metadata and audio-key denials are ambiguous, never permanent skips.
    # Pinned librespot Receiver writes this mangled attribute, not public country_code.
    country = vars(session).get("_Receiver__country_code", session.country_code)
    if not isinstance(country, str) or country == "EN":  # Upstream placeholder, not ISO country.
        country = ""
    subscription = session.get_user_attribute("type")
    for restriction in getattr(proto, "restriction", []):
        catalogues = list(restriction.catalogue_str)
        if not catalogues and restriction.catalogue:
            catalogues = ["premium" if c == 1 else "free" if c == 0 else "all" if c == 2
                          else "unknown" for c in restriction.catalogue]
        if catalogues and "all" not in catalogues and subscription not in catalogues:
            continue
        if not country or len(country) != 2:
            continue
        allowed = restriction.countries_allowed
        forbidden = restriction.countries_forbidden
        if ((restriction.HasField("countries_allowed") and country not in
             [allowed[i:i + 2] for i in range(0, len(allowed), 2)]) or country in
                [forbidden[i:i + 2] for i in range(0, len(forbidden), 2)]):
            if consider_alternatives and getattr(proto, "alternative", []):
                return "unknown"
            return "unavailable"
    return "available" if getattr(proto, "file", []) else "unknown"


def _thumbnail_url(proto) -> str | None:
    images = getattr(getattr(proto, "cover_group", None), "image", ()) or ()
    valid = [image for image in images if isinstance(getattr(image, "file_id", None), bytes)
             and len(image.file_id) == 20]
    image = max(valid, key=lambda image: getattr(image, "width", 0) or 0, default=None)
    file_id = image.file_id if image is not None else getattr(getattr(proto, "attributes", None), "picture", None)
    if isinstance(file_id, bytes) and len(file_id) == 20:
        return "https://i.scdn.co/image/" + file_id.hex()
    return None


def _track_row(uri: str, proto=None, session=None) -> dict:
    valid = re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", uri)
    row = {"id": uri.rsplit(":", 1)[1] if valid else uri, "uri": uri,
           "name": "", "artist": "", "album": "", "duration_ms": 0,
           "availability": "unknown", "thumbnail_url": None}
    if uri.startswith("spotify:local:"):
        parts = uri.split(":")
        if len(parts) >= 6:
            row.update(artist=unquote(parts[2]), album=unquote(parts[3]), name=unquote(parts[4]))
            try:
                row["duration_ms"] = max(0, int(parts[5]) * 1000)
            except ValueError:
                pass
        row["availability"] = "local"
    elif proto is not None:
        row.update(name=proto.name, artist=", ".join(a.name for a in proto.artist),
                   album=proto.album.name, duration_ms=max(0, proto.duration),
                    availability=_availability(proto, session),
                    thumbnail_url=_thumbnail_url(proto.album))
    return row


def metadata_result(session, uri: str) -> dict:
    kind = uri.split(":")[1]
    raw = _raw(session, kind, uri)
    revision = _revision(raw)
    complete = raw.IsInitialized()
    if kind == "playlist":
        # Presence checks matter: protobuf default zero/false is not evidence.
        contents = raw.contents
        complete = bool(complete and raw.HasField("contents") and contents.HasField("pos")
                        and contents.pos == 0 and contents.HasField("truncated")
                        and not contents.truncated and raw.HasField("length")
                        and len(contents.items) == raw.length and raw.HasField("revision")
                        and raw.revision and not raw.multiple_heads)
        revision = raw.revision.hex() if raw.revision else ""
        members = [(item.uri or "spotify:unresolved:" + _revision(item), None)
                   for item in contents.items]
        name, artist = raw.attributes.name, raw.owner_username
    elif kind == "album":
        members = [(_gid_uri(track) or "spotify:unresolved:" + _revision(track), track)
                   for disc in raw.disc for track in disc.track]
        # Album v4 carries full disc lists, not a paginated playlist snapshot.
        complete = bool(complete and _gid_uri(raw, "album") and raw.name
                        and raw.disc and all(d.HasField("number") for d in raw.disc))
        name, artist = raw.name, ", ".join(a.name for a in raw.artist)
    else:
        members = [(uri, raw)]
        complete = bool(complete and _gid_uri(raw) and raw.name)
        name, artist = raw.name, ", ".join(a.name for a in raw.artist)
    tracks = []
    missing = False
    cache = {}
    for member_uri, member in members:
        identity_known = bool(re.fullmatch(r"spotify:(track|episode):[A-Za-z0-9]{22}", member_uri)
                              or re.fullmatch(r"spotify:local:[^:]*:[^:]*:[^:]*:[0-9]+", member_uri))
        if not identity_known:
            complete = False
        if re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", member_uri):
            if kind != "track":
                try:
                    if member_uri not in cache:
                        cache[member_uri] = _raw(session, "track", member_uri)
                    member = cache[member_uri]
                except WorkerFailure:
                    raise
                except Exception:
                    member = None
                    missing = True
            if member is None or not member.name:
                missing = True
        tracks.append(_track_row(member_uri, member, session))
    result = {"status": "ok", "kind": kind, "name": name, "artist": artist,
              "complete": complete, "revision": revision, "tracks": tracks,
              "thumbnail_url": _thumbnail_url(raw.album if kind == "track" else raw)}
    messages = []
    if not complete:
        messages.append("Membership snapshot is incomplete or cannot be verified.")
    if missing:
        messages.append("Some track metadata could not be resolved; raw members preserved.")
    if messages:
        result["message"] = " ".join(messages)
    return result


def _zotify_response(proto, kind: str, uri: str) -> dict:
    from google.protobuf.json_format import MessageToDict

    response = MessageToDict(proto, preserving_proto_field_name=True)
    if kind == "album":
        response["album_type"] = str(response.get("type", "album")).lower()
    response.update(uri=uri, type=kind)
    if getattr(proto, "gid", None):
        response["gid"] = proto.gid
    if kind == "track":
        response["duration_ms"] = response.pop("duration", 0)
    if isinstance(response.get("album"), dict):
        album = response["album"]
        album["album_type"] = str(album.pop("type", "album")).lower()
    return response


def _verify_file(path: Path, fmt: str, track, expected: tuple[dict, dict, dict], tagger_class):
    stage = "probe"
    try:
        probe = subprocess.run(["ffprobe", "-v", "error", "-show_format", "-show_streams",
                                "-of", "json", str(path)], capture_output=True, check=True, timeout=60)
        info = json.loads(probe.stdout)
        audio = [s for s in info["streams"] if s.get("codec_type") == "audio"]
        stage = "codec"
        expected_codec = {"ogg": "vorbis", "mp3": "mp3", "aac": "aac"}[fmt]
        if len(audio) != 1:
            raise WorkerFailure("error", f"Downloaded audio failed codec verification (audio streams: actual={len(audio)}, expected=1).")
        actual_codec = audio[0].get("codec_name")
        if actual_codec != expected_codec:
            # Only known codec names are safe to report from untrusted probe output.
            actual_codec = actual_codec if actual_codec in {"vorbis", "mp3", "aac", "opus", "flac", "pcm_s16le"} else "unknown"
            raise WorkerFailure("error", f"Downloaded audio failed codec verification (actual={actual_codec}, expected={expected_codec}).")
        stage = "duration"
        duration = float(info["format"].get("duration", audio[0].get("duration", 0)))
        expected_duration = (track.duration_ms or 0) / 1000
        tolerance = max(1.5, expected_duration * 0.01)
        if (not math.isfinite(duration) or expected_duration <= 0 or duration <= 0
                or abs(duration - expected_duration) > tolerance):
            raise WorkerFailure("error", f"Downloaded audio failed duration verification (seconds: actual={duration:g}, expected={expected_duration:g}, tolerance={tolerance:g}).")
        stage = "metadata (probe)"
        probe_tags = {k.lower(): str(v) for k, v in info["format"].get("tags", {}).items()}
        probe_tags.update({k.lower(): str(v) for k, v in audio[0].get("tags", {}).items()})
        for key, value in (("title", track.name), ("album", track.album.name)):
            if value and probe_tags.get(key) != value:
                raise WorkerFailure("error", f"Downloaded audio failed metadata verification (probe: {key}).")
        if track.artists and not probe_tags.get("artist"):
            raise WorkerFailure("error", "Downloaded audio failed metadata verification (probe: artist).")
        stage = "metadata (read)"
        tags = tagger_class(path)
        reliable, optional, custom = expected
        # Stock matches_metadata cannot verify custom tags absent from its own map.
        for name, group in (("reliable", reliable), ("optional", optional)):
            for key, value in group.items():
                safe_key = key if re.fullmatch(r"[A-Za-z0-9_]{1,80}", key) else "unknown"
                stage = f"metadata ({name}: {safe_key})"
                if fmt == "ogg" and key == "year" and value is not None:
                    # Ogg stores year and custom date in the same physical tag. The
                    # installed sanitizer cannot read YYYY-MM, so compare its year directly.
                    dates = tags._get_raw("date")
                    mismatch = (not dates or len(dates) != 1
                                or not re.fullmatch(r"[0-9]{4}(?:-[0-9]{2}(?:-[0-9]{2})?)?", dates[0])
                                or dates[0].split("-", 1)[0] != str(value))
                elif key == "artwork" and value is not None:
                    actual = [art.data for art in tags.file_tags[key].values]
                    wanted = value if isinstance(value, list) else [value]
                    mismatch = actual != [art if isinstance(art, bytes) else art.data for art in wanted]
                elif isinstance(value, (int, bool)):
                    item = tags.file_tags[key]
                    mismatch = not item.values or item.value != value
                else:
                    mismatch = any(tags._match_tag_group({key: value}).values())
                if mismatch:
                    raise WorkerFailure("error", f"Downloaded audio failed {stage} verification.")
        for key, value in custom.items():
            if value is None:
                continue
            safe_key = key if re.fullmatch(r"[A-Za-z0-9_]{1,80}", key) else "unknown"
            stage = f"metadata (custom: {safe_key})"
            physical_key = ("TXXX:" + key.upper() if fmt == "mp3" else
                            "----:com.apple.iTunes:" + key if fmt == "aac" else key)
            stored = tags.file_tags.mfile.tags.get(physical_key)
            on_disk = (stored.text if fmt == "mp3" and stored is not None else
                       [v.decode("utf-8") for v in stored] if fmt == "aac" and stored is not None else stored)
            values = value if isinstance(value, list) else [value]
            if on_disk != [str(v) for v in values]:
                raise WorkerFailure("error", f"Downloaded audio failed {stage} verification.")
        stage = "decode"
        subprocess.run(["ffmpeg", "-v", "error", "-xerror", "-err_detect", "explode", "-i", str(path),
                        "-map", "0:a:0", "-f", "null", "-"], capture_output=True, check=True, timeout=180)
        return info
    except WorkerFailure:
        raise
    except Exception:
        raise WorkerFailure("error", f"Downloaded audio failed {stage} verification.") from None


@contextlib.contextmanager
def _tag_scope():
    from zotify.metadata import Tagger
    from music_tag.id3 import Id3File

    def custom_ogg(self, key, value):
        values = value if isinstance(value, list) else [value]
        self.file_tags.mfile.tags[key] = [str(v) for v in values]

    def custom_m4a(self, key, value):
        from mutagen.mp4 import MP4FreeForm

        values = value if isinstance(value, list) else [value]
        self.file_tags.mfile.tags["----:com.apple.iTunes:" + key] = [
            MP4FreeForm(str(v).encode("utf-8")) for v in values]

    original_id3_setter = Id3File._ft_setter

    def id3_setter(self, key, value, appendable=True):
        if key not in {"TPE1", "TPE2", "TCON", "TIT2", "TALB", "TCOM", "TSRC", "COMM", "USLT"}:
            return original_id3_setter(self, key, value, appendable)
        from mutagen import id3

        self.mfile.tags.delall(key)
        if key == "USLT":
            for index, text in enumerate(value.values):
                self.mfile.tags.add(id3.USLT(encoding=3, lang="eng", desc=str(index), text=str(text)))
            return
        kwargs = {"lang": "eng"} if key == "COMM" else {}
        self.mfile.tags.add(getattr(id3, key)(encoding=3, text=[str(v) for v in value.values], **kwargs))

    # Upstream custom Ogg lists, MP4 integers and ID3 artist/genre lists are broken.
    with (patch.object(Tagger, "_custom_ogg_tag", custom_ogg),
          patch.object(Tagger, "_custom_m4a_tag", custom_m4a),
          patch.object(Id3File, "_ft_setter", id3_setter)):
        yield


def convert_result(source: Path, output_dir: Path, fmt: str, uri: str | None = None) -> dict:
    """Produce playback from an immutable, fully verified native Vorbis original."""
    from mutagen.oggvorbis import OggVorbis
    from zotify.metadata import Tagger

    target = None
    stage = "source validation"
    try:
        source_stat = source.lstat()
        if not stat.S_ISREG(source_stat.st_mode) or source_stat.st_size <= 0:
            raise WorkerFailure("error", "source_path must be a nonempty regular file, not a symlink.")
        with source.open("rb") as file:
            digest = hashlib.file_digest(file, "sha256").digest()
        # Mutagen and ffprobe must independently agree that this is real Vorbis.
        native = OggVorbis(source)
        tags = Tagger(source)
        raw = {key.lower(): list(values) for key, values in native.tags.items()}
        for key in ("title", "artist", "album"):
            if not raw.get(key) or any(not value.strip() for value in raw[key]):
                raise WorkerFailure("error", f"Original audio is missing required metadata ({key}).")
        embedded_uri = raw.get("uri", [])
        if len(embedded_uri) != 1 or not re.fullmatch(r"spotify:track:[A-Za-z0-9]{22}", embedded_uri[0]):
            raise WorkerFailure("error", "Original audio has invalid Spotify track identity.")
        if uri is not None and embedded_uri[0] != uri:
            raise WorkerFailure("error", "Original audio identity does not match the requested track.")
        uri = embedded_uri[0]
        ident = uri.rsplit(":", 1)[1]
        if "trackid" in raw and raw["trackid"] != [ident]:
            raise WorkerFailure("error", "Original audio has inconsistent Spotify track identity.")
        if not raw.get("metadata_block_picture"):
            raise WorkerFailure("error", "Original audio is missing embedded cover artwork.")
        artwork = tags.file_tags["artwork"].values
        if not artwork or any(not art.data for art in artwork):
            raise WorkerFailure("error", "Original audio is missing embedded cover artwork.")
        logical = {}
        physical = {"tracktitle": "title", "artist": "artist", "album": "album",
                    "albumartist": "albumartist", "tracknumber": "tracknumber",
                    "totaltracks": "tracktotal", "discnumber": "discnumber",
                    "totaldiscs": "disctotal", "genre": "genre", "isrc": "isrc",
                    "compilation": "compilation", "composer": "composer",
                    "lyrics": "lyrics", "comment": "comment"}
        for key, disk_key in physical.items():
            if disk_key in raw:
                item = tags.file_tags[key]
                logical[key] = item.value if item.type in {int, bool} else item.values
        dates = raw.get("date")
        if dates:
            if len(dates) != 1 or not re.fullmatch(r"[0-9]{4}(?:-[0-9]{2}(?:-[0-9]{2})?)?", dates[0]):
                raise WorkerFailure("error", "Original audio has an invalid release date.")
            # Ogg year/date share a physical tag; never use music_tag's month sanitizer.
            logical["year"] = int(dates[0].split("-", 1)[0])
        logical["artwork"] = artwork
        custom = {key: values for key, values in raw.items()
                  if key not in (set(physical.values()) - {"isrc"}) | {"metadata_block_picture"}}
        expected = (logical, {}, custom)
        track = SimpleNamespace(duration_ms=round(native.info.length * 1000),
                                name=raw.get("title", [""])[0],
                                album=SimpleNamespace(name=raw.get("album", [""])[0]),
                                artists=raw.get("artist", []))
        stage = "source verification"
        info = _verify_file(source, "ogg", track, expected, Tagger)
        destination_path = output_dir / (ident + {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[fmt])
        stage = "output creation"
        # Exclusive creation prevents overwriting an existing file or following a symlink.
        with destination_path.open("xb"):
            pass
        target = destination_path
        if os.path.samefile(source, target):
            raise WorkerFailure("error", "Conversion output must not share the original inode.")
        if fmt == "ogg":
            shutil.copyfile(source, target)
        else:
            stage = "encoding"
            audio = next(s for s in info["streams"] if s.get("codec_type") == "audio")
            bitrate = float(audio.get("bit_rate") or native.info.bitrate or 160000)
            bitrate = min((96000, 160000, 320000), key=lambda value: abs(value - bitrate))
            # No run_ffm: upstream deletes its input. No implicit cross-format metadata mapping.
            subprocess.run(["ffmpeg", "-v", "error", "-xerror", "-err_detect", "explode",
                            "-i", str(source), "-map", "0:a", "-map_metadata", "-1",
                            "-c:a", {"mp3": "libmp3lame", "aac": "aac"}[fmt],
                            "-b:a", str(bitrate), "-y", str(target)],
                           capture_output=True, check=True, timeout=180)
            stage = "metadata transfer"
            with _tag_scope():
                destination = Tagger(target)
                destination.file_tags.mfile.tags.clear()
                for key, value in logical.items():
                    destination.file_tags[key] = value
                for key, value in custom.items():
                    destination._set_custom(key, value)
                destination.file_tags.save()
        stage = "output verification"
        _verify_file(target, fmt, track, expected, Tagger)
        with source.open("rb") as file:
            unchanged = hashlib.file_digest(file, "sha256").digest() == digest
        after = source.lstat()
        if (not unchanged or not stat.S_ISREG(after.st_mode)
                or (after.st_dev, after.st_ino) != (source_stat.st_dev, source_stat.st_ino)
                or os.path.samefile(source, target)):
            raise WorkerFailure("error", "Original audio changed during conversion.")
        if fmt == "ogg":
            with target.open("rb") as file:
                if hashlib.file_digest(file, "sha256").digest() != digest:
                    raise WorkerFailure("error", "Ogg playback copy differs from the original.")
        row = {"id": ident, "uri": uri, "name": track.name,
               "artist": ", ".join(track.artists), "album": track.album.name,
               "duration_ms": track.duration_ms, "availability": "available"}
        return {"status": "ok", "filename": target.name, "track": row}
    except Exception as exc:
        try:
            if target is not None and target.exists() and (not source.exists() or not os.path.samefile(source, target)):
                target.unlink()
        except OSError:
            pass
        if isinstance(exc, WorkerFailure):
            raise
        raise WorkerFailure("error", f"Offline conversion failed {stage}.") from None


def download_result(session, api, uri: str, output_dir: Path, fmt: str, quality: str = "auto") -> dict:
    from zotify.config import Zotify
    from zotify.metadata import Tagger
    from librespot.audio import AudioKeyManager
    from librespot.audio.decoders import AudioQuality, FormatOnlyAudioQuality, SuperAudioFormat
    from librespot.proto.Metadata_pb2 import AudioFile

    if not isinstance(quality, str) or quality not in {"auto", "normal", "high", "very_high"}:
        raise WorkerFailure("error", "quality must be auto, normal, high, or very_high.")
    premium = session.get_user_attribute("type") == "premium"
    if quality == "very_high" and not premium:
        raise WorkerFailure("blocked", "Very high source quality (320 kbps) requires Spotify Premium.")
    raw = _raw(session, "track", uri)
    availability = _availability(raw, session)
    if availability == "unavailable":
        raise WorkerFailure("unavailable", "Track explicitly restricted in this account's country.")
    if not raw.name or raw.duration <= 0 or not _gid_uri(raw):
        raise WorkerFailure("error", "Spotify returned incomplete track metadata; retry later.")
    query = api.Query("spotisync")
    track = api.Track(uri.removeprefix("spotify:"))
    track.parse_metadata(None, _zotify_response(raw, "track", uri))
    if not track.album or not _gid_uri(raw.album, "album"):
        raise WorkerFailure("error", "Spotify returned incomplete album metadata; retry later.")
    album_raw = _raw(session, "album", _gid_uri(raw.album, "album"))
    track.album._hasMetadata = False
    track.album.parse_metadata(None, _zotify_response(album_raw, "album", _gid_uri(album_raw, "album")))
    # Protobuf presence preserves date precision; never invent a month or day.
    date = album_raw.date
    parts = []
    for field, width in (("year", 4), ("month", 2), ("day", 2)):
        if not date.HasField(field):
            break
        parts.append(f"{getattr(date, field):0{width}d}")
    track.album.release_date = "-".join(parts) or None
    track.album.year = parts[0] if parts else None
    # Genres can live on both album and artists. No optional metadata requests are swallowed.
    genres = list(album_raw.genre)
    for artist in raw.artist:
        artist_uri = _gid_uri(artist, "artist")
        if artist_uri:
            artist_raw = _raw(session, "artist", artist_uri)
            genres.extend(getattr(artist_raw, "genre", []))
    track.genres = list(dict.fromkeys(genres)) or None
    track.album.total_tracks = sum(len(d.track) for d in album_raw.disc)
    track.album.total_discs = len(album_raw.disc)
    if not track.album.name or not track.album.image_url:
        raise WorkerFailure("error", "Album metadata or cover unavailable; retry later.")
    cover_response = Zotify.get_request(track.album.image_url)
    cover_response.raise_for_status()
    track.album._image = cover_response.content
    if not track.album._image:
        raise WorkerFailure("error", "Album cover fetch failed; retry later.")
    track.uri = uri
    track.is_playable = True  # Unknown/fileless is retryable, not stock check_skippable's unavailable.
    extra = {"albumid": track.album.id, "date": track.album.release_date, "label": track.album.label,
             "explicit": str(raw.explicit).lower() if raw.HasField("explicit") else None,
             "popularity": raw.popularity if raw.HasField("popularity") else None,
             "album_original_title": album_raw.original_title or None,
             "album_version_title": album_raw.version_title or None,
             "copyright": [c.text for c in album_raw.copyright] or None}
    from google.protobuf.json_format import MessageToDict

    descriptive = MessageToDict(raw, preserving_proto_field_name=True)
    album_descriptive = MessageToDict(album_raw, preserving_proto_field_name=True)
    # Preserve remaining descriptive fields without embedding transport IDs or album membership.
    for key in ("file", "alternative", "preview", "restriction", "availability"):
        descriptive.pop(key, None)
        album_descriptive.pop(key, None)
    album_descriptive.pop("disc", None)
    descriptive["album"] = album_descriptive
    extra["spotify_metadata"] = json.dumps(descriptive, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    for obj in (raw, album_raw):
        for identifier in obj.external_id:
            if identifier.type.lower() in {"isrc", "ean", "upc"}:
                extra.setdefault(identifier.type.lower(), identifier.id)
    extra = {key: value for key, value in extra.items() if value is not None}
    expected = Tagger._content_to_tags(track)
    expected[2].update(extra)
    failures = []
    source_qualities = {AudioFile.OGG_VORBIS_96: "normal", AudioFile.OGG_VORBIS_160: "high",
                        AudioFile.OGG_VORBIS_320: "very_high"}
    actual_quality = None

    class BestVorbis(FormatOnlyAudioQuality):
        def get_file(self, files):
            supported = [f for f in files if f.HasField("format") and len(f.file_id) == 20 and f.format in
                         source_qualities and (premium or f.format != f.OGG_VORBIS_320)
                         and (quality == "auto" or source_qualities[f.format] == quality)]
            return max(supported, key=lambda f: AudioQuality.get_quality(f.format).value, default=None)

    preferred = (AudioQuality.VERY_HIGH if premium else AudioQuality.HIGH) if quality == "auto" else AudioQuality[quality.upper()]
    selector = BestVorbis(preferred, SuperAudioFormat.VORBIS)

    def fetch_stream(self):
        nonlocal actual_quality
        pending = [(raw, False)]
        seen = set()
        ambiguous = False
        quality_missing = False
        for playable, resolve in pending:
            candidate_uri = _gid_uri(playable)
            if not candidate_uri:
                ambiguous = True
                continue
            if candidate_uri in seen:
                continue
            seen.add(candidate_uri)
            try:
                if resolve:
                    playable = _raw(session, "track", candidate_uri)
                pending.extend((alternative, True) for alternative in playable.alternative)
                if _availability(playable, session, consider_alternatives=False) == "unavailable":
                    continue
                if not playable.name or playable.duration <= 0 or not _gid_uri(playable):
                    ambiguous = True
                    continue
                selected = selector.get_file(playable.file)
                if selected is None:
                    if quality != "auto" and any(f.HasField("format") and len(f.file_id) == 20
                                                 and f.format in source_qualities for f in playable.file):
                        quality_missing = True
                    else:
                        ambiguous = True
                    continue
                Zotify.DOWNLOAD_BITRATE = {selected.OGG_VORBIS_96: "96k", selected.OGG_VORBIS_160: "160k",
                                          selected.OGG_VORBIS_320: "320k"}[selected.format]
                stream = session.content_feeder().load_track(playable, selector, False, None).input_stream
                actual_quality = source_qualities[selected.format]
                return stream
            except WorkerFailure as exc:
                if exc.status in {"auth_required", "blocked"}:
                    raise
                if exc.status != "unavailable":
                    ambiguous = True
            except RuntimeError as exc:
                if exc.args != ("Content is restricted!",):
                    ambiguous = True
            except Exception:
                ambiguous = True
        if not ambiguous:
            if quality_missing:
                raise WorkerFailure("blocked", "Requested source quality is unavailable for this track and its resolved alternatives.")
            raise WorkerFailure("unavailable", "Spotify explicitly restricted all resolved audio alternatives.")
        raise WorkerFailure("error", "No playable source audio resolved; retry later.")

    def convert(self, source, destination):
        # Stock conversion silently falls back to an unconverted .ogg on failure.
        options = ["-c:a", self._codec]
        if fmt != "ogg":
            options.extend(["-b:a", Zotify.DOWNLOAD_BITRATE])
        self.run_ffm(source, None, destination, options)
        return "0s"

    original_write = track.write_audio_tags
    def fetch_content(self, stream, temp_path, parent_stack):
        reader = stream.stream()
        # Spotify Vorbis source starts after a 167-byte normalization header.
        start = reader.pos() if hasattr(reader, "pos") else reader.tell()
        try:
            remaining = stream.size - start
            if remaining <= 0:
                raise WorkerFailure("error", "Source audio stream was empty; retry later.")
            with Path(temp_path).open("wb") as destination:
                while remaining:
                    data = reader.read(min(20000, remaining))
                    if not data or len(data) > remaining:
                        raise WorkerFailure("error", "Source audio stream was truncated; retry later.")
                    destination.write(data)
                    remaining -= len(data)
            return "0s"
        finally:
            reader.close()
            for future in vars(reader).get("_worker_futures", {}).values():
                future.cancel()

    def write_tags(self, path):
        try:
            original_write(path)
            tags = Tagger(path)
            for key, value in extra.items():
                tags._set_custom(key, value)
            tags.file_tags.save()
        except Exception as exc:
            # Track.download catches this exception; retain evidence independently.
            failures.append(exc)
            raise

    def mark_downloaded(self, ps=None, path=None):
        if path:
            self._downloaded = True
            self._real_filepaths[ps] = path

    track.fetch_stream = MethodType(fetch_stream, track)
    track.fetch_stream_content = MethodType(fetch_content, track)
    # All adapter skip decisions already made. Stock archive checks touch files even disabled.
    track.check_skippable = MethodType(lambda self, ps: False, track)
    track.convert_audio_format = MethodType(convert, track)
    track.write_audio_tags = MethodType(write_tags, track)
    track.mark_downloaded = MethodType(mark_downloaded, track)
    original_key = AudioKeyManager.get_audio_key

    with (patch.object(AudioKeyManager, "get_audio_key",
                        lambda self, gid, file_id, retry=False: original_key(self, gid, file_id, False)),
          _tag_scope(), _stream_scope()):
        track.download(api.ParentStack([query, track]))
    if failures:
        raise WorkerFailure("error", "Audio metadata writing failed; retry later.")
    paths = list(track._real_filepaths.values())
    if len(paths) != 1:
        raise WorkerFailure("error", "Download produced no verified output; retry later.")
    path = Path(paths[0])
    if path.parent != output_dir or path.suffix != {"ogg": ".ogg", "mp3": ".mp3", "aac": ".m4a"}[fmt] or path.is_symlink():
        raise WorkerFailure("error", "Unexpected download output path or format.")
    _verify_file(path, fmt, track, expected, Tagger)
    row = _track_row(uri, raw, session)
    row["availability"] = "available"
    result = {"status": "ok", "filename": path.name, "track": row}
    if actual_quality is not None:
        result["quality"] = actual_quality
    return result


def execute(command: dict, emit) -> dict:
    if not isinstance(command, dict):
        raise WorkerFailure("error", "Command must be a JSON object.")
    action = command.get("command")
    if action not in {"auth", "probe", "metadata", "download", "convert"}:
        raise WorkerFailure("error", "command must be auth, probe, metadata, download, or convert.")
    state_dir = _path(command, "state_dir")
    if state_dir.is_symlink():
        raise WorkerFailure("error", "state_dir must not be a symlink.")
    if action == "download" and isinstance(command.get("uri"), str) and command["uri"].strip().startswith("spotify:local:"):
        raise WorkerFailure("unavailable", "Local Spotify files cannot be downloaded.")
    uri = normalize_uri(command.get("uri"), track_only=action == "download") if action in {"metadata", "download"} else None
    if action == "convert" and "uri" in command:
        uri = normalize_uri(command["uri"], track_only=True)
    fmt = command.get("format", "ogg")
    if action in {"download", "convert"} and fmt not in {"ogg", "mp3", "aac"}:
        raise WorkerFailure("error", "format must be ogg, mp3, or aac.")
    quality = command.get("quality", "auto") if action == "download" else "auto"
    if not isinstance(quality, str) or quality not in {"auto", "normal", "high", "very_high"}:
        raise WorkerFailure("error", "quality must be auto, normal, high, or very_high.")
    if action == "convert":
        source = _path(command, "source_path")
        if not Path(command["source_path"]).is_absolute():
            raise WorkerFailure("error", "source_path must be an absolute canonical Ogg path.")
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    state_dir.chmod(0o700)
    session = None
    with contextlib.ExitStack() as scope:
        if action in {"download", "convert"}:
            root = _path(command, "output_dir")
            if root.is_symlink() or root == state_dir or root in state_dir.parents:
                raise WorkerFailure("error", "output_dir must be fresh staging, not state_dir or its parent.")
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            if any(root.iterdir()):
                raise WorkerFailure("error", "output_dir must be an empty staging directory.")
        else:
            root = Path(scope.enter_context(tempfile.TemporaryDirectory(prefix="worker-", dir=state_dir)))
        api = initialize(state_dir, root, fmt if action in {"download", "convert"} else "ogg", quality)
        if action == "convert":
            return convert_result(source, root, fmt, uri)
        scope.enter_context(_http_scope())
        scope.enter_context(_accesspoint_scope())
        try:
            session = authenticate(state_dir, emit) if action == "auth" else saved_session(state_dir)
            from zotify.config import LoginHandler, Zotify

            Zotify.SESSION = session
            LoginHandler.SESSION = session
            LoginHandler.OAUTH = None
            if action in {"auth", "probe"}:
                return {"status": "ok", "username": session.credentials()["username"]}
            if action == "metadata":
                return metadata_result(session, uri)
            return download_result(session, api, uri, root, fmt, quality)
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass


def main():
    protocol = sys.stdout

    def emit(event):
        protocol.write(json.dumps(event, ensure_ascii=True, allow_nan=False) + "\n")
        protocol.flush()

    safe_log = _SafeLog(sys.stderr)
    previous_logging = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with contextlib.redirect_stdout(safe_log), contextlib.redirect_stderr(safe_log):
            try:
                command = json.load(sys.stdin)
                result = execute(command, emit)
            except WorkerFailure as exc:
                result = exc.result()
            except (json.JSONDecodeError, UnicodeError):
                result = {"status": "error", "message": "Expected one JSON command on stdin."}
            except BaseException as exc:
                # Never emit upstream exception strings, traceback locals, tokens, or URLs.
                kind = type(exc).__name__
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,79}", kind):
                    kind = "BaseException"
                frames = []
                tb = exc.__traceback__
                while tb is not None:
                    code = tb.tb_frame.f_code
                    filename = Path(code.co_filename).name
                    function = code.co_name
                    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,79}\.py", filename):
                        filename = "<source>"
                    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,79}|<module>|<lambda>", function):
                        function = "<function>"
                    frames.append(f"{filename}:{tb.tb_lineno}:{function}")
                    frames = frames[-3:]
                    tb = tb.tb_next
                location = " at " + " -> ".join(frames) if frames else ""
                result = {"status": "error", "message": f"Spotify worker failed ({kind}{location}); retry later."}
        emit({"event": "result", **result})
    finally:
        logging.disable(previous_logging)


if __name__ == "__main__":
    main()
