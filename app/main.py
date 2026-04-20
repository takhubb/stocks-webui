from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.services.bulk_cache import BulkDataCache
from app.services.jquants_client import JQuantsClient
from app.services.stock_service import StockAnalysisService

load_dotenv()

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="stocks-webui", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

client = JQuantsClient()
bulk_cache = BulkDataCache(client=client)
analysis_service = StockAnalysisService(client=client, bulk_cache=bulk_cache)


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/analyze")
async def analyze(code: str) -> JSONResponse:
    try:
        payload = analysis_service.analyze(code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        logging.exception("analysis failed for code=%s", code)
        raise HTTPException(
            status_code=500,
            detail="分析中に予期しないエラーが発生しました。ログを確認してください。",
        ) from exc

    return JSONResponse(payload)
