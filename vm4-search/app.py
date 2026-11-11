"""Run with: uvicorn app:app --reload"""

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from ranking import rank
from store import InvalidExport, SearchStore


class VersionRequest(BaseModel):
    version: str


def create_app(serving_dir: Path | None = None) -> FastAPI:
    app = FastAPI(title="VM4 Search Node")
    app.state.store = SearchStore(serving_dir or Path(os.getenv("SERVING_DIR", Path(__file__).parent / "serving")))

    @app.get("/")
    def home():
        return FileResponse(Path(__file__).parent / "static" / "index.html")

    @app.get("/health")
    def health():
        return {"status": "ok" if app.state.store.version else "no_data", "version": app.state.store.version}

    @app.get("/version")
    def version():
        return {"version": app.state.store.version}

    @app.post("/version/activate")
    def activate(request: VersionRequest):
        try:
            app.state.store.activate(request.version)
        except InvalidExport as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"version": app.state.store.version}

    @app.get("/search")
    def search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=100)):
        store = app.state.store
        if not store.version:
            raise HTTPException(status_code=503, detail="No active export")
        results = rank(q, store.documents, store.index, store.pagerank, limit)
        return {"query": q, "version": store.version, "results": results}

    return app


app = create_app()
