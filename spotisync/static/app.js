"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const rows = new Map();
  const pending = new Set();
  const artworkRequests = new Map();
  const artworkTargets = new WeakMap();
  const artworkObserver = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) continue;
      artworkObserver.unobserve(entry.target);
      const target = artworkTargets.get(entry.target);
      if (target) updateArtwork(target.art, target.uri, target.thumbnail_url);
    }
  }, { rootMargin: "200px" });
  let state = null;
  let stateRequest = null;
  let pollTimer = null;
  let authAttempt = null;
  let conversionErrorSignature = null;

  function text(node, value) {
    const next = value == null ? "" : String(value);
    if (node.textContent !== next) node.textContent = next;
  }

  function element(tag, className, value) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (value != null) text(node, value);
    return node;
  }

  function feedback(id, message, error = false) {
    const node = $(id);
    text(node, message);
    node.classList.toggle("is-error", error);
  }

  async function api(path, method = "GET", body) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(path, {
        method,
        credentials: "same-origin",
        cache: "no-store",
        signal: controller.signal,
        headers: body === undefined ? {} : { "Content-Type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const raw = await response.text();
      let data = null;
      try { data = raw ? JSON.parse(raw) : null; } catch { /* An error response may not be JSON. */ }
      if (!response.ok) {
        const message = data?.message || data?.error || data?.detail;
        throw new Error(typeof message === "string" ? message : `Request failed (${response.status}). Please try again.`);
      }
      return data;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("Request timed out. Check the connection before trying again.");
      if (error instanceof TypeError) throw new Error("Cannot reach SpotiSync. Check your connection and try again.");
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  function dateLabel(value) {
    if (!value) return "Never";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const options = { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" };
    if (state?.schedule?.timezone) options.timeZone = state.schedule.timezone;
    try { return new Intl.DateTimeFormat(undefined, options).format(date); }
    catch { delete options.timeZone; return new Intl.DateTimeFormat(undefined, options).format(date); }
  }

  function timeLabel(value, fallback) {
    const match = /^(\d{2}):(\d{2})$/.exec(value || "");
    if (!match || Number(match[1]) > 23 || Number(match[2]) > 59) return fallback;
    const hour = Number(match[1]);
    return `${hour % 12 || 12}:${match[2]} ${hour < 12 ? "AM" : "PM"}`;
  }

  function count(value) {
    return Number.isFinite(Number(value)) ? Math.max(0, Number(value)) : 0;
  }

  function readable(value) {
    const label = String(value || "Pending").replace(/[_-]+/g, " ");
    return label.charAt(0).toUpperCase() + label.slice(1);
  }

  function createArtwork(label) {
    const node = element("div", "source-art");
    node.setAttribute("aria-hidden", "true");
    const fallback = element("span", "", label);
    const image = element("img");
    image.alt = "";
    image.loading = "lazy";
    image.decoding = "async";
    image.referrerPolicy = "no-referrer";
    image.hidden = true;
    node.append(fallback, image);
    const art = { node, fallback, image, uri: null, url: null, failedUrl: null, requested: false, nextLookup: 0 };
    image.addEventListener("load", () => {
      node.classList.add("has-thumbnail");
      fallback.hidden = true;
    });
    image.addEventListener("error", () => {
      art.failedUrl = art.url;
      art.nextLookup = Date.now() + 60000;
      image.hidden = true;
      fallback.hidden = false;
      node.classList.remove("has-thumbnail");
    });
    return art;
  }

  function setArtwork(art, value, retry = false) {
    let url;
    try { url = new URL(value); } catch { return; }
    const hosts = ["i.scdn.co", "mosaic.scdn.co", "image-cdn-ak.spotifycdn.com", "image-cdn-fa.spotifycdn.com"];
    if (url.protocol !== "https:" || !hosts.includes(url.hostname) || url.username || url.password || url.port || url.hash) return;
    if (art.url === url.href && art.failedUrl !== url.href) return;
    if (art.failedUrl === url.href && !retry) return;
    art.url = url.href;
    art.failedUrl = null;
    art.node.classList.remove("has-thumbnail");
    art.fallback.hidden = false;
    art.image.hidden = false;
    art.image.src = url.href;
  }

  function updateArtwork(art, uri, value, lookup = true) {
    if (art.uri !== uri) {
      art.uri = uri;
      art.url = art.failedUrl = null;
      art.requested = false;
      art.nextLookup = 0;
      art.image.hidden = true;
      art.image.removeAttribute("src");
      art.fallback.hidden = false;
      art.node.classList.remove("has-thumbnail");
    }
    if (value) setArtwork(art, value);
    if (!lookup || art.requested || art.nextLookup > Date.now() || !/^spotify:(playlist|album|track):[A-Za-z0-9]{22}$/.test(uri || "")) return;
    art.requested = true;
    const previousUrl = art.url;
    if (!artworkRequests.has(uri)) {
      artworkRequests.set(uri, api(`/api/media/thumbnail?uri=${encodeURIComponent(uri)}`)
        .then((result) => result?.thumbnail_url).catch(() => null)
        .finally(() => artworkRequests.delete(uri)));
    }
    artworkRequests.get(uri).then((url) => {
      if (art.uri !== uri) return;
      art.requested = false;
      art.nextLookup = Date.now() + (url ? 86400000 : 60000);
      if (url && art.url === previousUrl) setArtwork(art, url, true);
    });
  }

  function acceptAuthUrl(value) {
    if (!value) return;
    let url;
    try { url = new URL(value); } catch { return; }
    if (!["https:", "http:"].includes(url.protocol) || url.username || url.password) return;
    $("auth-link").href = url.href;
    $("auth-link").hidden = false;
    if (!authAttempt || authAttempt.opened) return;
    // Navigate only once per connect attempt; later polls must not reopen authorization.
    authAttempt.opened = true;
    let opened = false;
    try {
      if (authAttempt.popup && !authAttempt.popup.closed) {
        authAttempt.popup.location.replace(url.href);
        opened = true;
      }
    } catch { /* The visible link remains usable if navigation is blocked. */ }
    text($("auth-help"), opened
      ? "Finish authorization in the opened tab. This page will update automatically."
      : "Authorization is ready. Use the link above if your browser blocked the new tab.");
    $("auth-help").hidden = false;
  }

  function clearAuth() {
    if (authAttempt && !authAttempt.opened) {
      try { authAttempt.popup?.close(); } catch { /* The user may have closed it already. */ }
    }
    authAttempt = null;
    $("auth-link").hidden = true;
    $("auth-link").removeAttribute("href");
    $("auth-help").hidden = true;
  }

  function renderControls() {
    const ready = Boolean(state);
    const account = state?.account;
    const running = Boolean(state?.sync?.running) || pending.has("convert");
    const converting = Boolean(state?.conversion?.running);
    const accountBusy = pending.has("connect") || pending.has("disconnect") || Boolean(account?.connecting);
    const settingsBusy = pending.has("format") || pending.has("quality");
    const removing = [...pending].some((key) => key.startsWith("remove:"));
    $("source-uri").disabled = !ready || pending.has("add");
    $("add-button").disabled = !ready || pending.has("add");
    text($("add-button"), pending.has("add") ? "Adding..." : "Add source");
    $("connect-button").disabled = !ready || accountBusy || converting || pending.has("convert");
    text($("connect-button"), pending.has("connect") || account?.connecting ? "Connecting..." : account?.connected ? "Reconnect" : "Connect Spotify");
    $("disconnect-button").hidden = !account?.connected;
    $("disconnect-button").disabled = accountBusy || converting || pending.has("convert");
    text($("disconnect-button"), pending.has("disconnect") ? "Disconnecting..." : "Disconnect");
    $("empty-connect-button").disabled = !ready || accountBusy;
    text($("empty-connect-button"), accountBusy ? "Connecting..." : "Connect Spotify");
    $("sync-all-button").disabled = !ready || !account?.connected || accountBusy || running || removing || settingsBusy || pending.has("sync") || !state?.items.length;
    text($("sync-all-button"), pending.has("sync") ? "Starting..." : running ? "Syncing..." : "Sync all");
    $("format-select").disabled = !ready || settingsBusy || converting || pending.has("convert");
    $("quality-select").disabled = !ready || settingsBusy || converting || pending.has("convert");
    $("convert-button").disabled = !ready || running || accountBusy || removing || settingsBusy || pending.has("sync") || !state?.items.some((item) => count(item.downloaded_count) > 0);
    text($("convert-button"), converting ? "Converting..." : pending.has("convert") ? "Starting..." : "Convert existing songs");
    for (const [id, row] of rows) {
      row.sync.disabled = !account?.connected || accountBusy || running || removing || settingsBusy || pending.has("sync");
      text(row.sync, pending.has("sync") && pending.has(`sync:${id}`) ? "Starting..." : "Sync");
      row.remove.disabled = running || pending.has("sync") || pending.has(`remove:${id}`);
      text(row.remove, pending.has(`remove:${id}`) ? "Removing..." : "Remove");
      row.sync.setAttribute("aria-busy", String(pending.has(`sync:${id}`)));
      row.remove.setAttribute("aria-busy", String(pending.has(`remove:${id}`)));
    }
    for (const [id, key] of [["add-button", "add"], ["connect-button", "connect"], ["disconnect-button", "disconnect"], ["sync-all-button", "sync"], ["format-select", "format"], ["quality-select", "quality"], ["convert-button", "convert"]]) {
      $(id).setAttribute("aria-busy", String(pending.has(key)));
    }
  }

  function createRow(item) {
    const root = element("article", "source");
    const main = element("div", "source-main");
    const art = createArtwork("SP");
    const info = element("div", "source-info");
    const name = element("h3", "source-name");
    const subtitle = element("p", "source-subtitle");
    const meta = element("div", "source-meta");
    const status = element("span", "status-chip");
    const saved = element("span");
    const lastSync = element("span");
    meta.append(status, saved, lastSync);
    info.append(name, subtitle, meta);
    const actions = element("div", "source-actions");
    const sync = element("button", "button button-secondary", "Sync");
    const remove = element("button", "button button-quiet button-danger", "Remove");
    sync.type = remove.type = "button";
    actions.append(sync, remove);
    main.append(art.node, info, actions);
    const details = element("details", "source-details");
    const summary = element("summary", "", "Details");
    const content = element("div", "detail-content");
    const counts = element("dl", "counts");
    const countFields = {};
    for (const [key, label] of [["track_count", "Tracks"], ["downloaded_count", "Downloaded"], ["failed_count", "Failed"], ["unavailable_count", "Unavailable"]]) {
      const group = element("div");
      countFields[key] = element("dd");
      group.append(element("dt", "", label), countFields[key]);
      counts.append(group);
    }
    const fields = element("dl", "detail-fields");
    const uri = element("dd", "detail-path");
    const folder = element("dd", "detail-path");
    const lastFull = element("dd");
    fields.append(element("dt", "", "Source URI"), uri, element("dt", "", "Folder"), folder, element("dt", "", "Last sync"), lastFull);
    const incomplete = element("p", "incomplete-note", "Incomplete: some tracks are not downloaded yet. Check counts and errors below.");
    const errors = element("ul", "error-list");
    content.append(counts, fields, incomplete, errors);
    details.append(summary, content);
    root.append(main, details);
    const row = { root, art, name, subtitle, status, saved, lastSync, lastFull, sync, remove, uri, folder, incomplete, errors, details, countFields, summary, item, errorSignature: null, errorArtworks: [] };
    sync.addEventListener("click", () => startSync(row.item.id));
    remove.addEventListener("click", () => removeItem(row.item));
    return row;
  }

  function updateRow(row, item) {
    row.item = item;
    row.root.dataset.kind = ["playlist", "album", "track"].includes(item.kind) ? item.kind : "source";
    text(row.art.fallback, ({ playlist: "PL", album: "AL", track: "TR" })[item.kind] || "SP");
    updateArtwork(row.art, item.uri, item.thumbnail_url);
    text(row.name, item.name || "Untitled source");
    text(row.subtitle, [readable(item.kind || "Source"), item.artist].filter(Boolean).join(" / "));
    const status = String(item.status || "pending").toLowerCase();
    text(row.status, `${readable(item.status)}${item.incomplete ? " / incomplete" : ""}${count(item.pending_quality_count) ? " / quality update pending" : ""}`);
    row.status.dataset.tone = ["failed", "error"].includes(status) ? "error"
      : item.incomplete || count(item.failed_count) || count(item.unavailable_count) || count(item.pending_quality_count) ? "warning"
      : ["synced", "complete", "completed", "ready", "syncing", "downloading"].includes(status) ? "good" : "neutral";
    text(row.saved, `${count(item.downloaded_count)} / ${count(item.track_count)} saved`);
    text(row.lastSync, item.last_sync ? `Last sync ${dateLabel(item.last_sync)}` : "Not synced yet");
    text(row.lastFull, dateLabel(item.last_sync));
    text(row.uri, item.uri || "Not available");
    text(row.folder, item.folder || "Not assigned yet");
    row.incomplete.hidden = !item.incomplete;
    for (const [key, node] of Object.entries(row.countFields)) text(node, count(item[key]));
    row.sync.setAttribute("aria-label", `Sync ${item.name || "source"}`);
    row.remove.setAttribute("aria-label", `Remove ${item.name || "source"}`);
    row.summary.setAttribute("aria-label", `Details for ${item.name || "source"}`);
    const errors = Array.isArray(item.errors) ? item.errors : [];
    const signature = JSON.stringify(errors);
    if (signature !== row.errorSignature) {
      row.errorSignature = signature;
      row.errorArtworks.forEach((entry) => artworkObserver.unobserve(entry.art.node));
      row.errorArtworks = [];
      row.errors.replaceChildren(...errors.map((error) => {
        const entry = element("li", "track-error");
        const art = createArtwork(error.uri?.startsWith("spotify:local:") ? "LOCAL" : error.uri?.startsWith("spotify:track:") ? "TR" : "SP");
        art.node.classList.add("error-art");
        const copy = element("div", "error-copy");
        copy.append(element("strong", "", error.name || "Track"), element("span", "muted", ` / ${readable(error.status || "error")}`), element("p", "", error.message || "No additional details."));
        entry.append(art.node, copy);
        const target = { art, uri: error.uri, thumbnail_url: error.thumbnail_url };
        row.errorArtworks.push(target);
        updateArtwork(art, error.uri, error.thumbnail_url, false);
        artworkTargets.set(art.node, target);
        artworkObserver.observe(art.node);
        return entry;
      }));
    }
    row.errors.hidden = !errors.length;
    if (row.details.open) row.errorArtworks.forEach((entry) => artworkObserver.observe(entry.art.node));
  }

  function renderState() {
    const { account, schedule, sync, items } = state;
    text($("account-status"), account.connecting ? "Connecting to Spotify..." : account.connected ? `Connected${account.username ? ` as ${account.username}` : " to Spotify"}` : "Connect Spotify to start syncing.");
    text($("account-message"), account.message);
    $("account-message").hidden = !account.message;
    if (authAttempt && account.connecting) authAttempt.sawConnecting = true;
    if (account.connected && !account.connecting && !pending.has("connect")) clearAuth();
    else if (account.auth_url && (!authAttempt || authAttempt.acknowledged) && (account.connecting || !account.connected)) acceptAuthUrl(account.auth_url);
    else if (authAttempt?.sawConnecting && !account.connecting && !account.auth_url && !pending.has("connect")) {
      clearAuth();
      feedback("action-feedback", account.message || "Spotify connection ended. Try connecting again.", true);
    }
    text($("source-count"), items.length);
    const liveIds = new Set(items.map((item) => item.id));
    for (const [id, row] of rows) {
      if (!liveIds.has(id)) {
        row.errorArtworks.forEach((entry) => artworkObserver.unobserve(entry.art.node));
        row.root.remove();
        rows.delete(id);
      }
    }
    const list = $("source-list");
    const focused = document.activeElement;
    items.forEach((item, index) => {
      let row = rows.get(item.id);
      if (!row) { row = createRow(item); rows.set(item.id, row); }
      updateRow(row, item);
      // Keep existing nodes in place so polling preserves focus and open details.
      if (list.children[index] !== row.root) list.insertBefore(row.root, list.children[index] || null);
    });
    if (focused && list.contains(focused) && document.activeElement !== focused) focused.focus({ preventScroll: true });
    $("empty-state").hidden = items.length > 0;
    text($("empty-title"), "Your collection starts here");
    text($("empty-copy"), account.connected ? "Paste a Spotify link above to track your first source. Nothing downloads until you sync." : "Connect your Spotify account, then add a playlist, album, or track. Your library stays on this machine.");
    $("empty-connect-button").hidden = account.connected;
    $("sync-panel").hidden = !sync.running && !sync.message;
    $("sync-panel").classList.toggle("is-running", Boolean(sync.running));
    text($("sync-title"), sync.phase === "conversion" ? "Conversion in progress" : sync.running ? `Sync in progress${sync.phase ? ` / ${readable(sync.phase)}` : ""}` : "Sync update");
    const syncDetail = [sync.current_item, sync.current_track, sync.message].filter(Boolean).join(" / ");
    text($("sync-detail"), syncDetail);
    $("sync-detail").hidden = !syncDetail;
    const converting = sync.phase === "conversion" || state.conversion?.running;
    const showProgress = !converting && (sync.running || sync.message);
    $("sync-progress").hidden = !showProgress;
    if (showProgress) {
      text($("sync-counter"), sync.progress_ready ? `${count(sync.completed)} / ${count(sync.total)}` : "Preparing...");
      text($("sync-counter-label"), sync.progress_ready ? "new songs" : "counting downloads");
      $("sync-progress").setAttribute("aria-label", sync.progress_ready
        ? `${count(sync.completed)} of ${count(sync.total)} new songs completed`
        : "Preparing download count");
      const scope = sync.progress_from_resume
        ? "Counts remaining new downloads since the service resumed."
        : "Only songs not already downloaded at the start of this job are counted.";
      $("sync-progress").title = scope + (sync.total_provisional
        ? " Playlist metadata is incomplete; total can increase as more songs are discovered." : "");
    }
    if (!pending.has("format") && document.activeElement !== $("format-select")) $("format-select").value = state.settings.format || "ogg";
    if (!pending.has("quality") && document.activeElement !== $("quality-select")) $("quality-select").value = state.settings.quality || "auto";
    text($("schedule-time"), `${timeLabel(schedule.time, "3:00 AM")} local time / retries through ${timeLabel(schedule.retry_until, "8:00 AM")}`);
    text($("schedule-zone"), `${schedule.timezone ? `${schedule.timezone}. ` : ""}Initial pass may continue past the retry cutoff.`);
    text($("next-run"), schedule.next_run ? `Next run: ${dateLabel(schedule.next_run)}` : "");
    $("next-run").hidden = !schedule.next_run;
    const conversion = state.conversion;
    const showConversion = Boolean(conversion?.running || conversion?.message);
    $("conversion-status").hidden = !showConversion;
    $("conversion-status").classList.toggle("is-error", count(conversion?.failed) > 0);
    text($("conversion-status"), showConversion ? [
      `${readable(conversion.format)}: ${count(conversion.completed) + count(conversion.failed) + count(conversion.skipped)} / ${count(conversion.total)} processed`,
      count(conversion.failed) ? `${count(conversion.failed)} failed` : null,
      count(conversion.skipped) ? `${count(conversion.skipped)} skipped` : null,
      conversion.current_track,
      conversion.message,
    ].filter(Boolean).join(" / ") : "");
    const conversionErrors = [...(conversion?.errors || []), ...(conversion?.skips || [])];
    const signature = JSON.stringify(conversionErrors);
    if (signature !== conversionErrorSignature) {
      conversionErrorSignature = signature;
      $("conversion-errors").replaceChildren(...conversionErrors.map((error) => {
        const entry = element("li");
        entry.append(element("strong", "", error.name || "Track"), element("p", "", error.message || "Conversion failed; existing output is kept."));
        return entry;
      }));
    }
    $("conversion-details").hidden = !conversionErrors.length;
    renderControls();
  }

  function refreshState() {
    if (stateRequest) return stateRequest;
    clearTimeout(pollTimer);
    stateRequest = (async () => {
      try {
        const next = await api("/api/state");
        if (!next?.account || !next?.settings || !next?.schedule || !next?.sync || !Array.isArray(next.items)) throw new Error("Unexpected state response from SpotiSync.");
        state = next;
        renderState();
        $("api-error").hidden = true;
      } catch (error) {
        text($("api-error"), `${error.message} ${state ? "Showing last known state." : "Waiting for your library."} Retrying automatically.`);
        $("api-error").hidden = false;
        if (!state) {
          text($("empty-title"), "Your library is unavailable");
          text($("empty-copy"), "SpotiSync could not be reached. This page will retry automatically.");
        }
      }
    })().finally(() => {
      stateRequest = null;
      pollTimer = setTimeout(refreshState, 2000);
    });
    return stateRequest;
  }

  async function runAction(key, task, feedbackId = "action-feedback") {
    if (pending.has(key)) return;
    pending.add(key);
    feedback(feedbackId, "");
    renderControls();
    try {
      await task();
      // An in-flight poll may predate the mutation. Fetch again after it completes.
      if (stateRequest) await stateRequest;
      await refreshState();
    } catch (error) {
      feedback(feedbackId, error.message, true);
    } finally {
      pending.delete(key);
      renderControls();
    }
  }

  function connectAccount() {
    if (!state || state.account.connecting || pending.has("connect") || pending.has("disconnect")) return;
    const confirmation = state.account.connected
      ? "Reconnect Spotify? This starts a new authorization flow. Tracked sources and downloaded files stay on this machine."
      : "Connect Spotify? Authorization opens in a new tab. Connecting does not start any downloads.";
    if (!window.confirm(confirmation)) return;
    clearAuth();
    let popup = null;
    try {
      // Open synchronously within the user gesture before awaiting the worker.
      popup = window.open("about:blank", "_blank");
      if (popup) {
        popup.opener = null;
        popup.document.title = "Connecting to Spotify";
        popup.document.body.textContent = "Waiting for Spotify authorization. Keep the SpotiSync tab open.";
      }
    } catch { /* Popup blockers are handled by the authorization link. */ }
    authAttempt = { popup, opened: false, acknowledged: false, sawConnecting: false };
    text($("auth-help"), "Waiting for the authorization link. This page will update automatically.");
    $("auth-help").hidden = false;
    runAction("connect", async () => {
      try {
        const result = await api("/api/account/connect", "POST", {});
        authAttempt.acknowledged = true;
        if (result?.url) acceptAuthUrl(result.url);
      } catch (error) { clearAuth(); throw error; }
    });
  }

  function startSync(itemId) {
    if (!state?.account.connected || state.account.connecting || state.sync.running || pending.has("sync") || pending.has("format") || pending.has("quality") || pending.has("connect") || pending.has("disconnect") || [...pending].some((key) => key.startsWith("remove:"))) return;
    const perItem = itemId !== undefined;
    if (perItem) pending.add(`sync:${itemId}`);
    runAction("sync", async () => {
      await api("/api/sync", "POST", perItem ? { item_id: itemId } : {});
      feedback("action-feedback", "Sync requested. Progress will appear here.");
    }).finally(() => { if (perItem) pending.delete(`sync:${itemId}`); renderControls(); });
  }

  function removeItem(item) {
    if (state?.sync.running || pending.has("sync") || pending.has(`remove:${item.id}`)) return;
    if (!window.confirm(`Remove "${item.name || "this source"}" from tracked sources? Existing downloaded files remain on disk. This source will no longer be synced.`)) return;
    runAction(`remove:${item.id}`, async () => {
      await api(`/api/items/${encodeURIComponent(item.id)}`, "DELETE");
      feedback("action-feedback", "Source removed. Downloaded files remain on disk.");
    });
  }

  $("add-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const input = $("source-uri");
    const uri = input.value.trim();
    if (!state || pending.has("add")) return;
    if (!uri) { feedback("add-feedback", "Enter a Spotify link or URI.", true); input.focus(); return; }
    runAction("add", async () => {
      await api("/api/items", "POST", { uri });
      input.value = "";
      feedback("add-feedback", "Source tracked. Adding it does not start a download.");
    }, "add-feedback").finally(() => { if (document.activeElement === document.body) input.focus(); });
  });
  $("connect-button").addEventListener("click", connectAccount);
  $("empty-connect-button").addEventListener("click", connectAccount);
  $("disconnect-button").addEventListener("click", () => {
    if (!state?.account.connected || pending.has("disconnect") || pending.has("connect") || state.account.connecting) return;
    if (!window.confirm("Disconnect Spotify? Tracked sources and downloaded files remain. Reconnect to resume future syncs.")) return;
    runAction("disconnect", async () => {
      await api("/api/account/disconnect", "POST", {});
      clearAuth();
      feedback("action-feedback", "Spotify disconnected. Your sources and files remain.");
    });
  });
  $("sync-all-button").addEventListener("click", () => startSync());
  $("convert-button").addEventListener("click", () => {
    if (!state || state.sync.running || pending.has("convert") || pending.has("format") || pending.has("quality") || pending.has("sync")) return;
    const target = ({ ogg: "original Ogg", mp3: "MP3", aac: "AAC/M4A" })[state.settings.format];
    if (!window.confirm(`Convert successfully downloaded songs to ${target} using saved Ogg originals only? No Spotify downloads will be started. Missing originals or source qualities are skipped; use Sync to fetch them. Verified outputs replace previous outputs; originals are kept.`)) return;
    runAction("convert", async () => {
      await api("/api/convert", "POST", {});
      feedback("action-feedback", "Offline conversion requested for successful downloads only. Missing originals are skipped, never downloaded by conversion.");
    });
  });
  $("format-select").addEventListener("change", () => {
    const select = $("format-select");
    if (!state || pending.has("format") || pending.has("quality")) return;
    const previous = state.settings.format;
    const format = select.value;
    runAction("format", async () => {
      try {
        await api("/api/settings", "PUT", { format });
        feedback("action-feedback", "Output format saved. Original Oggs are always kept; use Convert existing songs to update current outputs.");
      } catch (error) { select.value = previous; throw error; }
    }).finally(() => { select.value = state.settings.format; });
  });
  $("quality-select").addEventListener("change", () => {
    const select = $("quality-select");
    if (!state || pending.has("quality") || pending.has("format")) return;
    const previous = state.settings.quality || "auto";
    const quality = select.value;
    runAction("quality", async () => {
      try {
        await api("/api/settings", "PUT", { quality });
        feedback("action-feedback", "Spotify source quality saved. Existing songs update on next sync; saved Ogg quality versions are reused.");
      } catch (error) { select.value = previous; throw error; }
    }).finally(() => { select.value = state.settings.quality || "auto"; });
  });
  refreshState();
})();
