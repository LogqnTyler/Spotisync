# SpotiSync

SpotiSync downloads and syncs your Spotify playlists to your computer. It uses (Googolplexed0)[https://github.com/Googolplexed0]'s fork of [Zotify](https://github.com/Googolplexed0/zotify) to download the Spotify tracks and a simple web UI with FastAPI to connect to your account and queue up playlists to sync.

## Safety

**Important:** SpotiSync saves your Spotify credentials in the download destination folder (defaults to `${HOME}/Music`). These aren't encrypted. Be careful with them because they give full access to your Spotify account!

**Spotify account safety:** This project is unofficial and uses Zotify/librespot.
Spotify may rate-limit requests, invalidate sessions, change APIs, or suspend
accounts. Use only content you have permission to download and comply with applicable 
law and Spotify's terms.

## Docker Setup

Requires Docker Engine and Docker Compose on a Linux host. Run commands from this
repository. Create the music folder as your normal host user before starting:

```sh
mkdir -p "$HOME/Music"
export PUID="$(id -u)"
export PGID="$(id -g)"
docker compose up --build -d
```

Open <http://127.0.0.1:8000> and use the UI to connect Spotify and manage tracked
items. OAuth callbacks use host-local port `4381`; use a browser on the same
computer. Both published ports bind to `127.0.0.1`, not the LAN. Do not expose this
personal UI or OAuth listener through public port forwarding or a reverse proxy.

The exports match the container's UID/GID to your host user so downloaded files
are yours. Both build arguments default to `1000`. For persistent overrides,
optionally copy `.env.example` to `.env`, then set `PUID` and `PGID` to the outputs
of `id -u` and `id -g`. No `.env` is required, and it needs no Spotify secrets.
Exported shell variables take precedence over `.env` values.

To use another music directory, set `MUSIC_DIR` in `.env` to an **absolute** host
path, for example `MUSIC_DIR=/home/logan/Audio`, and create that directory with
`mkdir -p "/home/YourUsername/Audio"` first. Unset or empty `MUSIC_DIR` defaults to
`${HOME}/Music`, using the host's `HOME`, not the container's. Avoid `~` in `.env`
paths. The existing folder must be writable by the chosen UID/GID. If it does not
exist, Docker's bind mount can create a root-owned folder and the non-root app
will fail to write. Fix that folder's ownership rather than running the app as
root or making it world-writable.

### Storage and Runtime

| Setting | Container location | Host storage |
| --- | --- | --- |
| `SPOTISYNC_STATE_DIR` | `/data` | Compose named volume `app-state` |
| `SPOTISYNC_MUSIC_DIR` | `/music` | Read-write music folder bind mount |
| Local timezone | `/etc/localtime` | Read-only host `/etc/localtime` |

SQLite state, settings, sync records, and saved authentication live under `/data`.
A new named volume inherits the image directory's selected UID/GID ownership.
Rebuilding with different IDs does **not** change ownership of an existing volume
or host music folder. Stop the service and migrate their ownership if IDs change.
Keep the state volume private and back it up along with your music; authentication
files are sensitive and must not be committed or shared.

Mounting `/etc/localtime` makes scheduling follow the host computer's local time
and daylight-saving rules. `/etc/timezone` is deliberately not required because
hosts such as Arch Linux may not provide it. No `TZ` override is needed.

The app runs non-root with all Linux capabilities dropped and
`no-new-privileges` enabled. Compose supplies an init process, restarts the service
unless stopped, and allows 30 seconds for graceful shutdown before forced stop.
The healthcheck requests `/api/health`; it checks web availability, not Spotify
authentication or download success. Docker does not automatically restart a
container merely because its healthcheck fails.

```sh
docker compose ps
docker compose logs -f spotisync
curl --fail http://127.0.0.1:8000/api/health
docker compose stop
```

`docker compose down` removes the container and network but preserves the named
state volume and music bind folder. **Do not use `docker compose down -v` unless
you intend to delete saved state and authentication.** It deletes the state
volume, though it does not delete the host music folder.

## Sync Behavior

- Adding a track, album, or playlist tracks it for later sync; adding is
  not an immediate download. Use manual sync when you want a pass now.
- Scheduled sync begins at **03:00 host-local time**. Its first pass attempts the
  whole tracked list, even if completing that pass takes it past **08:00**.
- Initial playlist metadata is prepared before downloading. The progress banner
  counts successfully completed **unique new songs**, excluding already-downloaded
  music, cached-original folder copies, quality upgrades, and known local/unavailable
  entries. Failures do not increase completion. Total can grow if incomplete
  metadata later reveals additional missing songs; it never decreases as work finishes.
- Progress survives restarts. Jobs predating progress tracking start an accurate
  remaining-work counter when resumed, rather than inventing earlier completion.
- Failed items get retry rounds before 08:00, with a **60-second wait between
  rounds**. Spotify's `Retry-After` is honored when it requires a longer wait.
  No new retry round begins at or after 08:00; 08:00 is not a hard cancellation
  deadline for the initial full pass.
- On startup between 03:00 inclusive and 08:00 exclusive, the scheduler catches
  up on a missed run. Outside that window it waits for the next 03:00 run.
- Manual sync outside the scheduled window performs one pass, without overnight
  retry rounds. Authentication failures or rate limits can still prevent success.
- Inside the nightly window, a manual request also starts the full nightly first
  pass if it has not run yet. One failing source cannot prevent other sources
  from receiving their nightly pass. Jobs never overlap.
- Confirmed unavailable or local-only tracks are not retried as failed downloads.
  Their availability is checked again during the next nightly metadata refresh.
  Authentication failures pause work until reconnect; HTTP 429 cooldowns apply
  across subsequent Spotify requests, including manual requests.
- Removing a tracked item stops future sync; it keeps downloaded files.
- Playlist mirroring removes only SpotiSync-managed files that are no longer in
  the playlist, and only after a complete playlist snapshot is available.
  Unmanaged originals are protected. Failed or incomplete snapshots must not
  trigger deletion. Large playlists may be incomplete upstream; treat completeness
  warnings as advisory and inspect them before expecting an exact mirror.
- Files modified outside SpotiSync are preserved rather than deleted. Interrupted
  or mismatched deletion captures may remain under `.spotisync-quarantine` with
  an `intent.json` record inside the music folder for conservative recovery.

## Audio Output

Every download saves a verified native Ogg original in
`~/Music/.spotisync-originals/` (or `.spotisync-originals` under your configured
music bind folder). Originals are deduplicated by Spotify track URI and retained
even when a song leaves a playlist or you remove tracking. Back up this hidden
folder with the database and music. Originals and visible Ogg outputs are separate
copies, so editing playback tags cannot change the original.

Choose native Ogg, MP3, or AAC (stored in an `.m4a` container) for visible files in
the playlist/album/song folders. MP3 and AAC are encoded with FFmpeg from the saved
original, never from another lossy output. Selecting Ogg restores a byte-identical
copy of the original, not an MP3/AAC-to-Ogg transcode. Metadata and embedded cover
art are preserved and verified along with codec, duration, and full audio decode.
The best Spotify source quality available to the account is requested.

The dropdown applies to **future outputs**. Click **Convert existing songs** beside
it to update successfully downloaded songs from verified local Ogg originals only.
Conversion is strictly offline: no Spotify downloads, retries, source backfills,
or authentication requests. Missing originals or requested quality are skipped
with a "Sync required" explanation, separately from conversion failures.
Verified replacements publish before the previous managed output is removed;
modified or unknown files are preserved. Progress and errors appear below the
selector. Conversion jobs are serialized with sync jobs and resume after restart.
Conversion works without a Spotify connection and preserves prior playback on failure.

### Source Quality

The quality dropdown below Download format selects Spotify's original audio,
not an arbitrary MP3/AAC encoding bitrate:

| Selection | Native source |
| --- | --- |
| Auto | Best available quality allowed by the account |
| Normal | 96 kbps Vorbis |
| High | 160 kbps Vorbis |
| Very high | 320 kbps Vorbis, Spotify Premium required |

Changing quality updates existing songs on the next sync. Cached originals are
matched by verified **actual native bitrate**, not dropdown label: an Auto original
that is already 320 kbps also satisfies Very High without another download or copy.
Quality versions remain in `.spotisync-originals`; Auto conversion chooses the
highest verified quality already cached locally. If the required original is
missing, **Sync** retrieves it before playback is regenerated. Existing playback format stays unchanged unless
you explicitly convert it. Failed upgrades keep the previous playable output.
Explicit quality selections are never silently downgraded.

Sync and conversion jobs snapshot their quality selection, so changing the
dropdown mid-job affects the next job rather than producing a mixed-quality pass.

Existing native Ogg downloads are adopted into the original store after validation.
Older MP3/AAC downloads created before this feature require a **one-time native
Ogg download during Sync**. Conversion skips them until that original is available.
If backfill fails, the current output remains playable. Later format changes need
no Spotify downloads.

No extra cover images, lyric sidecars, playlist exports, or Zotify archive files
are produced. Playlist-managed deletions remain subject to complete-snapshot
safeguards and never delete canonical originals.

Playlist cards and failed-song details display Spotify cover thumbnails. Public
oEmbed artwork URLs are cached locally; thumbnails do not trigger audio downloads.
Unavailable artwork falls back to a label, including local-only songs that have
no Spotify cover. Failed-song thumbnails load as their details become visible.

## Local Development

Install Python 3.13, uv, Git, FFmpeg, and native Vorbis libraries (Debian's
`libvorbisfile3`, or Arch's `libvorbis`):

```sh
uv sync --frozen
uv run python main.py
```

`main.py` is the local server launcher. Local defaults are
`SPOTISYNC_STATE_DIR=./.spotisync` and `SPOTISYNC_MUSIC_DIR=~/Music`. Override them
with exported environment variables when needed. Compose's `.env` is for Compose
interpolation; do not assume it is loaded by the local Python launcher. Local
scheduling uses your computer's timezone too. Do not run local development and
the container against the same SQLite state or managed music tree concurrently.

Run offline tests with `uv run pytest -q`. Tests include scheduling, retries,
restart recovery, filesystem ownership, API protections, and generated audio
verification. They do not log into Spotify or download Spotify content.
Run browser-logic regressions with `node --test tests/test_ui.js` (Node required).

Live validation on 2026-10-11 synced all 17 tracks from the House playlist
(`1CWJA6g7rJfJTbxREGxcwd`) as Ogg, verified file hashes, full audio decoding,
metadata and embedded covers, and confirmed a repeat sync left files unchanged.
MP3 and AAC conversion/tagging are covered by generated-audio offline tests.

The container launches directly from the copied source package:

```sh
uvicorn spotisync.web:create_app --factory --host 0.0.0.0 --port 8000
```

Its internal wildcard binding is needed for Docker port forwarding; Compose's
host bindings remain loopback-only. Do not use this wildcard command directly on
the host if you want local-only access.

## Build Constraints

The dependency stage installs Git and CA certificates, then runs exactly
`uv sync --frozen --no-dev --no-install-project`. It installs the lockfile's
dependencies, including pinned Zotify and librespot Git revisions, without
installing SpotiSync itself. Application source and static UI assets are copied
into `/app`; the project virtual environment is copied into the runtime image.
No dependency resolution or installation occurs on container startup.

FFmpeg is required at runtime for audio validation and conversion. The image also
provides `libvorbisfile3` for PyOgg's native Vorbis loading. The inspected Zotify
and librespot dependency manifests do not require `soundfile` or `libsndfile1`,
so no explicit `libsndfile1` package is added; Debian's FFmpeg pulls it in
transitively. Git and uv stay in build stages.

Builds need network access to the image registries, Debian package repositories,
Python package index, and GitHub. Frozen installation does not repair stale or
missing lockfile entries: the maintainer must update `pyproject.toml` and `uv.lock`
together. Git revisions and Python dependencies come from the lockfile; this is
not a promise of byte-identical images because Python image tags and Debian
package repositories can change. uv is version-pinned in the Dockerfile.

No test, image-build, upstream-login, or live-download success is implied by
these deployment instructions.
