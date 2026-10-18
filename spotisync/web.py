"""Local-only HTTP surface. No credentials are returned through the API."""

from contextlib import asynccontextmanager
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .core import SOURCE_QUALITIES, SyncService


class MediaInput(BaseModel):
    uri: str = Field(min_length=1, max_length=2048)


class SyncInput(BaseModel):
    item_id: int | None = Field(default=None, gt=0)


class SettingsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    format: str | None = None
    quality: str | None = None


def create_app(*, service=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.service = service or SyncService(
            Path(os.environ.get("SPOTISYNC_STATE_DIR", ".spotisync")),
            Path(os.environ.get("SPOTISYNC_MUSIC_DIR", str(Path.home() / "Music"))),
        )
        app.state.service.start()
        try:
            yield
        finally:
            await app.state.service.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]"])

    @app.middleware("http")
    async def same_origin(request: Request, call_next):
        if request.method not in {"GET", "HEAD"}:
            origin = request.headers.get("origin")
            if request.headers.get("sec-fetch-site") == "cross-site" or (
                origin and origin != f"{request.url.scheme}://{request.headers.get('host')}"
            ):
                return JSONResponse({"detail": "Cross-origin requests are not allowed."}, status_code=403)
            if request.url.path in {"/api/items", "/api/sync", "/api/settings", "/api/convert"} and request.method != "DELETE":
                if request.headers.get("content-type", "").split(";", 1)[0] != "application/json":
                    return JSONResponse({"detail": "Use application/json."}, status_code=415)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data: https://i.scdn.co https://mosaic.scdn.co https://image-cdn-ak.spotifycdn.com https://image-cdn-fa.spotifycdn.com; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(ValueError)
    async def bad_input(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.exception_handler(RuntimeError)
    async def conflict(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

    @app.get("/api/state")
    async def state(request: Request):
        return request.app.state.service.snapshot()

    @app.post("/api/items", status_code=201)
    async def add(value: MediaInput, request: Request):
        return request.app.state.service.add(value.uri)

    @app.get("/api/items/{item_id}/thumbnail")
    async def thumbnail(item_id: int, request: Request):
        return {"thumbnail_url": await request.app.state.service.thumbnail(item_id)}

    @app.get("/api/media/thumbnail")
    async def media_thumbnail(request: Request, uri: str = Query(min_length=1, max_length=2048)):
        return {"thumbnail_url": await request.app.state.service.media_thumbnail(uri)}

    @app.delete("/api/items/{item_id}")
    async def remove(item_id: int, request: Request):
        request.app.state.service.remove(item_id)
        return {"status": "ok"}

    @app.post("/api/sync", status_code=202)
    async def sync(value: SyncInput, request: Request):
        request.app.state.service.start_job(value.item_id)
        return {"status": "queued"}

    @app.put("/api/settings")
    async def settings(value: SettingsInput, request: Request):
        fields = value.model_fields_set
        if not fields:
            raise HTTPException(400, "Provide format or quality.")
        if "format" in fields and value.format not in {"ogg", "mp3", "aac"}:
            raise HTTPException(400, "Choose ogg, mp3, or aac.")
        if "quality" in fields and value.quality not in SOURCE_QUALITIES:
            raise HTTPException(400, "Choose auto, normal, high, or very_high source quality.")
        service = request.app.state.service
        service.db.execute("BEGIN IMMEDIATE")
        try:
            for field in fields:
                service.set_setting(field, getattr(value, field))
            service.db.execute("COMMIT")
        except BaseException:
            service.db.execute("ROLLBACK")
            raise
        return {"format": service.setting("format", "ogg"), "quality": service.setting("quality", "auto")}

    @app.post("/api/convert", status_code=202)
    async def convert(request: Request):
        request.app.state.service.start_conversion()
        return {"status": "queued"}

    @app.post("/api/account/connect", status_code=202)
    async def connect(request: Request):
        request.app.state.service.start_connect()
        return {"status": "connecting"}

    @app.post("/api/account/disconnect")
    async def disconnect(request: Request):
        await request.app.state.service.disconnect()
        return {"status": "ok"}

    static = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static), name="static")

    @app.get("/")
    async def index():
        return FileResponse(static / "index.html")

    return app
