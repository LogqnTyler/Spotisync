"use strict";

const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { join } = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");

const source = readFileSync(join(__dirname, "../spotisync/static/app.js"), "utf8");
const ending = /  refreshState\(\);\s*\}\)\(\);\s*$/;
assert.match(source, ending);
// Expose the IIFE only in memory; production source has no test hooks.
const testSource = source.replace(ending, `  globalThis.ui = {
    createArtwork, setArtwork, updateArtwork, createRow, updateRow, renderState,
    setState(value) { state = value; }
  };
})();`);
const URI = "spotify:track:" + "1".repeat(22);
const PLAYLIST = "spotify:playlist:" + "2".repeat(22);
const COVER = "https://i.scdn.co/image/old";
const NEW_COVER = "https://i.scdn.co/image/new";

class Element {
  constructor(tag = "div") {
    this.tagName = tag;
    this.children = [];
    this.dataset = {};
    this.textContent = this.value = "";
    this.hidden = this.open = false;
    this.listeners = new Map();
    this.attributes = new Map();
    const classes = new Set();
    this.classList = {
      add: (name) => classes.add(name), remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
      toggle: (name, on = !classes.has(name)) => on ? classes.add(name) : classes.delete(name),
    };
  }
  setAttribute(name, value) { this.attributes.set(name, value); }
  removeAttribute(name) { this.attributes.delete(name); delete this[name]; }
  addEventListener(type, handler) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(handler);
  }
  emit(type) { for (const handler of this.listeners.get(type) || []) handler({ target: this, preventDefault() {} }); }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  insertBefore(node, before) {
    this.children = this.children.filter((child) => child !== node);
    const index = this.children.indexOf(before);
    this.children.splice(index < 0 ? this.children.length : index, 0, node);
  }
  contains(node) { return this === node || this.children.some((child) => child.contains(node)); }
}

function response(data, status = 200) {
  return { ok: status >= 200 && status < 300, status, text: async () => JSON.stringify(data) };
}

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

// No real timers, DOM, or network. A new VM isolates each test's IIFE state.
function harness() {
  let now = 1000000, observer, fetchHandler;
  const elements = new Map(), calls = [], timers = new Map();
  const get = (id) => {
    if (!elements.has(id)) elements.set(id, new Element());
    return elements.get(id);
  };
  const state = { account: { connected: false }, settings: { format: "mp3", quality: "auto" },
    schedule: {}, sync: {}, items: [], conversion: { errors: [] } };
  fetchHandler = (path) => response(path === "/api/state" ? state : { thumbnail_url: null });
  const context = vm.createContext({
    URL, AbortController, Error, TypeError,
    Date: class extends Date { static now() { return now; } },
    document: { createElement: (tag) => new Element(tag), getElementById: get, activeElement: null },
    IntersectionObserver: class {
      constructor(callback) { this.callback = callback; this.nodes = new Set(); observer = this; }
      observe(node) { this.nodes.add(node); }
      unobserve(node) { this.nodes.delete(node); }
      fire() { this.callback([...this.nodes].map((target) => ({ target, isIntersecting: true }))); }
    },
    setTimeout: (fn, delay) => { const id = {}; timers.set(id, { fn, delay }); return id; },
    clearTimeout: (id) => timers.delete(id),
    fetch: async (path, options) => {
      assert.match(path, /^\/api\//, "Only mocked local API requests are allowed");
      calls.push({ path, ...options });
      return fetchHandler(path, options);
    },
  });
  vm.runInContext(testSource, context, { filename: "app.js" });
  context.ui.setState(state);
  return { ui: context.ui, get, state, calls, observer, advance: (ms) => { now += ms; },
    fetch: (handler) => { fetchHandler = handler; } };
}

const settle = () => new Promise((resolve) => setImmediate(resolve));
const thumbnailCalls = (h, uri) => h.calls.filter((call) => call.path.startsWith("/api/media/thumbnail?")
  && new URL(call.path, "http://localhost").searchParams.get("uri") === uri);
const item = (thumbnail_url = null) => ({ id: 1, uri: PLAYLIST, kind: "playlist", name: "Playlist",
  thumbnail_url: null, errors: [{ uri: URI, name: "Song", status: "failed", message: "Audio failed", thumbnail_url }] });

test("sync counter uses successful job targets, not library or attempted counts", () => {
  const h = harness();
  h.state.sync = { running: true, phase: "first_pass", progress_ready: true, completed: 2, total: 5, failed: 1 };
  h.state.items = [{ ...item(), track_count: 200, downloaded_count: 190, errors: [] }];
  h.ui.renderState();
  assert.equal(h.get("sync-progress").hidden, false);
  assert.equal(h.get("sync-counter").textContent, "2 / 5");
  assert.equal(h.get("sync-counter-label").textContent, "new songs");
  assert.equal(h.get("sync-progress").attributes.get("aria-label"), "2 of 5 new songs completed");
});

test("sync counter shows preparation instead of fabricated totals", () => {
  const h = harness();
  h.state.sync = { running: true, phase: "first_pass", progress_ready: false };
  h.ui.renderState();
  assert.equal(h.get("sync-counter").textContent, "Preparing...");
  assert.equal(h.get("sync-counter-label").textContent, "counting downloads");
});

test("sync counter supports zero targets, later discovery, and resumed cohorts", () => {
  const h = harness();
  h.state.sync = { running: true, phase: "retry", progress_ready: true, completed: 0, total: 0 };
  h.ui.renderState();
  assert.equal(h.get("sync-counter").textContent, "0 / 0");
  Object.assign(h.state.sync, { completed: 3, total: 7, total_provisional: true, progress_from_resume: true });
  h.ui.renderState();
  assert.equal(h.get("sync-counter").textContent, "3 / 7");
  assert.match(h.get("sync-progress").title, /since the service resumed/);
  assert.match(h.get("sync-progress").title, /total can increase/);
});

test("sync counter stays visible for finished update but is hidden during conversion", () => {
  const h = harness();
  h.state.sync = { running: false, message: "Finished", progress_ready: true, completed: 4, total: 5 };
  h.ui.renderState();
  assert.equal(h.get("sync-counter").textContent, "4 / 5");
  assert.equal(h.get("sync-progress").hidden, false);
  h.state.sync.phase = "conversion";
  h.state.sync.running = true;
  h.ui.renderState();
  assert.equal(h.get("sync-progress").hidden, true);
});

test("offline conversion skips are processed but not labeled failures", () => {
  const h = harness();
  h.state.conversion = { running: false, format: "aac", total: 5, completed: 2, failed: 1, skipped: 2,
    message: "Finished", errors: [{ name: "Broken", message: "Encode failed" }],
    skips: [{ name: "Missing", message: "Sync required" }] };
  h.ui.renderState();
  assert.match(h.get("conversion-status").textContent, /5 \/ 5 processed/);
  assert.match(h.get("conversion-status").textContent, /1 failed/);
  assert.match(h.get("conversion-status").textContent, /2 skipped/);
  assert.equal(h.get("conversion-errors").children.length, 2);
});

for (const failure of ["null", "http", "network"]) {
  test(`negative artwork lookup (${failure}) retries at 60 seconds, not before`, async () => {
    const h = harness(), art = h.ui.createArtwork("TR");
    h.fetch(() => {
      if (failure === "network") throw new TypeError("offline");
      return failure === "http" ? response({ detail: "Temporary failure" }, 503) : response({ thumbnail_url: null });
    });
    h.ui.updateArtwork(art, URI, null);
    await settle();
    assert.equal(art.requested, false);
    assert.equal(art.image.hidden, true);
    assert.equal(art.fallback.hidden, false);
    h.fetch(() => response({ thumbnail_url: COVER }));
    h.advance(59999);
    h.ui.updateArtwork(art, URI, null);
    assert.equal(h.calls.length, 1);
    h.advance(1);
    h.ui.updateArtwork(art, URI, null);
    await settle();
    assert.equal(h.calls.length, 2);
    assert.equal(art.image.src, COVER);
  });
}

test("same-URI lookups deduplicate only while in flight", async () => {
  const h = harness(), request = deferred();
  h.fetch(() => request.promise);
  const arts = [h.ui.createArtwork("TR"), h.ui.createArtwork("TR")];
  arts.forEach((art) => h.ui.updateArtwork(art, URI, null));
  h.ui.updateArtwork(arts[0], URI, null);
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].path, `/api/media/thumbnail?uri=${encodeURIComponent(URI)}`);
  request.resolve(response({ thumbnail_url: COVER }));
  await settle();
  arts.forEach((art) => { assert.equal(art.image.src, COVER); assert.equal(art.requested, false); });
  h.fetch(() => response({ thumbnail_url: NEW_COVER }));
  const fresh = h.ui.createArtwork("TR");
  h.ui.updateArtwork(fresh, URI, null);
  await settle();
  assert.equal(h.calls.length, 2);
  assert.equal(fresh.image.src, NEW_COVER);
});

test("positive artwork lookups refresh at 24 hours", async () => {
  const h = harness(), art = h.ui.createArtwork("TR");
  h.fetch(() => response({ thumbnail_url: COVER }));
  h.ui.updateArtwork(art, URI, null);
  await settle();
  h.advance(86400000 - 1);
  h.ui.updateArtwork(art, URI, null);
  assert.equal(h.calls.length, 1);
  h.advance(1);
  h.fetch(() => response({ thumbnail_url: NEW_COVER }));
  h.ui.updateArtwork(art, URI, null);
  await settle();
  assert.equal(h.calls.length, 2);
  assert.equal(art.image.src, NEW_COVER);
});

test("newer state artwork is not overwritten by an old in-flight lookup", async () => {
  const h = harness(), art = h.ui.createArtwork("TR"), request = deferred();
  h.fetch(() => request.promise);
  h.ui.updateArtwork(art, URI, COVER);
  h.ui.updateArtwork(art, URI, NEW_COVER);
  request.resolve(response({ thumbnail_url: COVER }));
  await settle();
  assert.equal(h.calls.length, 1);
  assert.equal(art.image.src, NEW_COVER);
  assert.equal(art.requested, false);
});

test("rebuilt error artwork never reuses an old resolved lookup", async () => {
  const h = harness(), media = item(), row = h.ui.createRow(media);
  h.fetch((path) => response({ thumbnail_url: path.includes(encodeURIComponent(URI)) ? COVER : null }));
  h.ui.updateRow(row, media);
  assert.equal(thumbnailCalls(h, URI).length, 0, "Error lookups are lazy");
  h.observer.fire();
  await settle();
  assert.equal(row.errorArtworks[0].art.image.src, COVER);
  media.errors[0].thumbnail_url = NEW_COVER;
  h.fetch(() => response({ thumbnail_url: NEW_COVER }));
  h.ui.updateRow(row, media);
  assert.equal(row.errorArtworks[0].art.image.src, NEW_COVER);
  h.observer.fire();
  await settle();
  assert.equal(thumbnailCalls(h, URI).length, 2);
  assert.equal(row.errorArtworks[0].art.image.src, NEW_COVER);
  assert.equal(media.errors[0].status, "failed");
  assert.equal(media.errors[0].message, "Audio failed");
});

test("open details reobserve unchanged error nodes and retry negative lookups", async () => {
  const h = harness(), media = item(), row = h.ui.createRow(media);
  row.details.open = true;
  h.ui.updateRow(row, media);
  const art = row.errorArtworks[0].art;
  h.observer.fire();
  await settle();
  assert.equal(h.observer.nodes.size, 0);
  h.advance(59999);
  h.ui.updateRow(row, media);
  h.observer.fire();
  assert.equal(thumbnailCalls(h, URI).length, 1);
  h.advance(1);
  h.fetch(() => response({ thumbnail_url: NEW_COVER }));
  h.ui.updateRow(row, media);
  h.observer.fire();
  await settle();
  assert.equal(row.errorArtworks[0].art, art);
  assert.equal(thumbnailCalls(h, URI).length, 2);
  assert.equal(art.image.src, NEW_COVER);
});

test("image failure restores fallback and retries the same URL after 60 seconds", async () => {
  const h = harness(), art = h.ui.createArtwork("TR");
  h.ui.updateArtwork(art, URI, COVER, false);
  assert.equal(art.image.loading, "lazy");
  assert.equal(art.image.referrerPolicy, "no-referrer");
  art.image.emit("load");
  assert.equal(art.fallback.hidden, true);
  assert.equal(art.node.classList.contains("has-thumbnail"), true);
  art.image.emit("error");
  assert.equal(art.image.hidden, true);
  assert.equal(art.fallback.hidden, false);
  assert.equal(art.node.classList.contains("has-thumbnail"), false);
  h.fetch(() => response({ thumbnail_url: COVER }));
  h.advance(59999);
  h.ui.updateArtwork(art, URI, COVER);
  assert.equal(h.calls.length, 0);
  h.advance(1);
  h.ui.updateArtwork(art, URI, COVER);
  await settle();
  assert.equal(art.failedUrl, null);
  assert.equal(art.image.hidden, false);
  art.image.emit("load");
  assert.equal(art.fallback.hidden, true);
});

test("local Spotify URIs retain fallback without any lookup", async () => {
  const h = harness(), art = h.ui.createArtwork("LOCAL");
  h.ui.updateArtwork(art, "spotify:local:Artist:Album:Song:1", null);
  h.advance(86400000);
  h.ui.updateArtwork(art, art.uri, null);
  await settle();
  assert.equal(h.calls.length, 0);
  assert.equal(art.image.hidden, true);
  assert.equal(art.fallback.textContent, "LOCAL");
});

for (const quality of ["auto", "normal", "high", "very_high"]) {
  test(`quality change sends only PUT {quality: ${quality}} and preserves format`, async () => {
    const h = harness();
    h.state.settings.quality = "normal";
    h.ui.renderState();
    h.fetch((path, options) => {
      if (path === "/api/settings") {
        assert.equal(options.method, "PUT");
        assert.equal(options.headers["Content-Type"], "application/json");
        assert.deepEqual(JSON.parse(options.body), { quality });
        h.state.settings.quality = quality;
        return response(h.state.settings);
      }
      assert.equal(path, "/api/state");
      return response(h.state);
    });
    const select = h.get("quality-select");
    select.value = quality;
    select.emit("change");
    assert.equal(select.disabled, true);
    await settle();
    assert.deepEqual(h.calls.map((call) => call.path), ["/api/settings", "/api/state"]);
    assert.equal(select.value, quality);
    assert.equal(select.disabled, false);
    assert.equal(h.get("format-select").value, "mp3");
  });
}

test("failed quality PUT rolls the dropdown back and displays the error", async () => {
  const h = harness();
  h.ui.renderState();
  h.fetch(() => response({ detail: "Quality setting rejected" }, 400));
  const select = h.get("quality-select");
  select.value = "very_high";
  select.emit("change");
  await settle();
  assert.equal(h.calls.length, 1);
  assert.deepEqual(JSON.parse(h.calls[0].body), { quality: "very_high" });
  assert.equal(select.value, "auto");
  assert.equal(select.disabled, false);
  assert.equal(h.state.settings.format, "mp3");
  assert.equal(h.get("action-feedback").textContent, "Quality setting rejected");
  assert.equal(h.get("action-feedback").classList.contains("is-error"), true);
});

test("artwork accepts exactly the HTTPS Spotify CDN hosts allowed by CSP", () => {
  const h = harness();
  const hosts = ["i.scdn.co", "mosaic.scdn.co", "image-cdn-ak.spotifycdn.com", "image-cdn-fa.spotifycdn.com"];
  const web = readFileSync(join(__dirname, "../spotisync/web.py"), "utf8");
  const policy = web.match(/response\.headers\["Content-Security-Policy"\] = "([^"]+)"/)[1];
  const images = policy.match(/(?:^|;)\s*img-src ([^;]+)/)[1].split(" ");
  assert.deepEqual(JSON.parse(source.match(/const hosts = (\[[^;]+\]);/)[1]), hosts);
  assert.deepEqual(images.filter((entry) => entry.startsWith("https:")), hosts.map((host) => `https://${host}`));
  assert.match(policy, /(?:^|;)\s*connect-src 'self';/);
  for (const host of hosts) {
    const art = h.ui.createArtwork("SP"), url = `https://${host}/image/cover`;
    h.ui.setArtwork(art, url);
    assert.equal(art.image.src, url);
  }
  for (const url of [null, {}, "", "/cover", "http://i.scdn.co/cover", "https://evil.example/cover",
    "https://sub.i.scdn.co/cover", "https://i.scdn.co.evil.example/cover", "https://i.scdn.co@evil.example/cover",
    "https://user@i.scdn.co/cover", "https://user:pass@i.scdn.co/cover", "https://i.scdn.co:8443/cover",
    "https://i.scdn.co/cover#fragment", "data:image/png;base64,AA==", "javascript:alert(1)"]) {
    const art = h.ui.createArtwork("SP");
    h.ui.setArtwork(art, url);
    assert.equal(art.image.src, undefined, String(url));
    assert.equal(art.image.hidden, true);
    assert.equal(art.fallback.hidden, false);
  }
  assert.equal(h.calls.length, 0);
});
