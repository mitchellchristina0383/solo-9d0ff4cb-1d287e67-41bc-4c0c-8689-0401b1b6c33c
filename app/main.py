"""FastAPI application for the log search service."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from .indexer import (
    AlertAlreadyAcked,
    AlertNotFound,
    IndexManager,
    IndexUnavailable,
    RebuildInProgress,
    RuleExists,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"


class LogIn(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    service: str = Field(min_length=1, max_length=100)
    body: str = Field(min_length=1, max_length=20_000)


class RuleIn(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    service: str = Field(min_length=1, max_length=100)
    keyword: str = Field(min_length=1, max_length=500)
    window_seconds: int = Field(ge=1, le=86_400)
    threshold: int = Field(ge=1, le=100_000)


def create_app(db_path: str | None = None, *, manager: IndexManager | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("LOGDB_PATH", "/data/logs.db")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        idx = manager or IndexManager(db_path)
        await run_in_threadpool(idx.initialize)
        app.state.index_manager = idx
        try:
            yield
        finally:
            idx.shutdown()

    app = FastAPI(title="日志检索", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    def get_manager() -> IndexManager:
        return app.state.index_manager

    @app.get("/", include_in_schema=False)
    def index_page() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/healthz")
    def healthz():
        idx = get_manager()
        if not idx.health():
            raise HTTPException(status_code=503, detail="数据库或索引不可用")
        return {"ok": True}

    @app.post("/api/logs", status_code=201)
    async def add_log(payload: LogIn):
        idx = get_manager()
        return await run_in_threadpool(idx.add_log, payload.service, payload.body)

    @app.get("/api/logs")
    async def search_logs(
        q: Annotated[str | None, Query(max_length=500)] = None,
        service: Annotated[str | None, Query(max_length=100)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        idx = get_manager()
        try:
            result = await run_in_threadpool(idx.search, q, service, limit=limit, offset=offset)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IndexUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return result

    @app.get("/api/services")
    async def list_services():
        idx = get_manager()
        return {"items": await run_in_threadpool(idx.services)}

    @app.post("/api/rules", status_code=201)
    async def create_rule(payload: RuleIn):
        idx = get_manager()
        try:
            return await run_in_threadpool(
                idx.create_rule,
                payload.service,
                payload.keyword,
                payload.window_seconds,
                payload.threshold,
            )
        except RuleExists as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/rules")
    async def list_rules():
        idx = get_manager()
        return {"items": await run_in_threadpool(idx.list_rules)}

    @app.delete("/api/rules/{rule_id}", status_code=204)
    async def delete_rule(rule_id: int):
        idx = get_manager()
        if not await run_in_threadpool(idx.delete_rule, rule_id):
            raise HTTPException(status_code=404, detail="规则不存在")

    @app.get("/api/alerts")
    async def list_alerts(
        status: Annotated[Literal["open", "ack", "all"], Query()] = "open",
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        idx = get_manager()
        status_filter = None if status == "all" else status
        return await run_in_threadpool(
            idx.list_alerts, status_filter, limit=limit, offset=offset
        )

    @app.post("/api/alerts/{alert_id}/ack")
    async def ack_alert(alert_id: int):
        idx = get_manager()
        try:
            return await run_in_threadpool(idx.ack_alert, alert_id)
        except AlertNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except AlertAlreadyAcked as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/reindex", status_code=202)
    async def start_reindex():
        idx = get_manager()
        try:
            generation = await run_in_threadpool(idx.start_rebuild)
        except RebuildInProgress as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except IndexUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return {"building": generation}

    @app.get("/api/status")
    async def status():
        idx = get_manager()
        return await run_in_threadpool(idx.status)

    return app


app = create_app()
