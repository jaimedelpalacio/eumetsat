"""
app.py — FastAPI del microservicio LSA SAF FRP-PIXEL → JSON

Características:
- Mantiene en RAM un doble buffer (current/previous) con el último dataset válido.
- Endpoint /reload (protegible con API key) que descarga → parsea → filtra (BBOX Iberia) → conmuta current/previous de forma atómica.
- Endpoint /frp-pixel/latest que sirve SIEMPRE desde RAM (refiltra por BBOX pequeña y umbrales en lectura).
- Endpoint /frp-pixel/by-ts para traer un slot concreto bajo demanda (NO altera el buffer).
- Endpoint /health para checks en Render.

Cron de Render:
- Defínelo desde el panel para hacer un GET a /reload?secret=TU_API_KEY cada 5 min.
"""

import os
import asyncio
from typing import Optional, Tuple, List

from fastapi import FastAPI, Query, Header, HTTPException, Response, status
from fastapi.responses import ORJSONResponse
from starlette.middleware.gzip import GZipMiddleware

from ingest import (
    startup_warmup,
    reload_latest_async,
    get_snapshot_for_serve,
    ingest_slot_by_ts_async,
    DEFAULT_IBERIA_BBOX,
)

APP_TITLE = "LSA SAF FRP-PIXEL (Meteosat) → JSON (RAM double-buffer)"
API_KEY = os.getenv("API_KEY", "")  # opcional: si se define, protege /reload

app = FastAPI(title=APP_TITLE, default_response_class=ORJSONResponse)
app.add_middleware(GZipMiddleware, minimum_size=1024)  # comprime JSON grandes


def _parse_bbox(bbox: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """Parsea "w,s,e,n" de la query a tupla de floats."""
    if not bbox:
        return None
    try:
        w, s, e, n = map(float, bbox.split(","))
        return (w, s, e, n)
    except Exception:
        raise HTTPException(status_code=400, detail="bbox inválido; usa w,s,e,n (lon/lat)")


@app.on_event("startup")
async def _warmup_on_start():
    """
    Warmup al iniciar el proceso:
    - Realiza una primera ingesta en background (no bloqueante) para llenar 'current' pronto.
    - Si falla, el siguiente cron del panel hará /reload.
    """
    asyncio.create_task(startup_warmup())


@app.get("/health")
async def health():
    """
    Devuelve información básica de salud y del último slot en memoria.
    """
    snap = get_snapshot_for_serve()  # puede ser None en arranque frío
    if not snap:
        return ORJSONResponse(
            {"ok": True, "status": "cold", "message": "arranque frío; esperando /reload"},
            status_code=status.HTTP_200_OK,
        )
    return ORJSONResponse(
        {
            "ok": True,
            "status": "warm",
            "last_slot": snap["slot_ts"],
            "downloaded_at": snap["downloaded_at"],
            "count": len(snap["rows"]),
        },
        status_code=status.HTTP_200_OK,
    )


@app.get("/reload")
async def reload_endpoint(secret: Optional[str] = None, x_api_key: Optional[str] = Header(default=None)):
    """
    Ejecuta la ingesta y conmuta el buffer si el nuevo slot es válido.
    Seguridad:
    - Si API_KEY está definido en entorno, hay que pasar secret=API_KEY o cabecera X-API-Key: API_KEY.
    - Si no hay API_KEY, /reload queda abierto (útil durante pruebas).
    """
    if API_KEY:
        provided = secret or x_api_key or ""
        if provided != API_KEY:
            raise HTTPException(status_code=403, detail="API key inválida")

    result = await reload_latest_async()
    code = 200 if result.get("ok") else 503
    return ORJSONResponse(result, status_code=code)


@app.get("/frp-pixel/latest")
async def frp_latest(
    bbox: Optional[str] = Query(default=None, description="w,s,e,n (lon/lat) para refiltrar en lectura"),
    min_frp: Optional[float] = Query(default=None, description="umbral FRP mínimo (MW)"),
    min_conf: Optional[float] = Query(default=None, description="umbral de confianza mínimo (0–100)"),
    response: Response = None,
):
    """
    Sirve SIEMPRE desde RAM el último dataset válido (preprocesado con BBOX Iberia por defecto).
    - Permite refiltro por BBOX más pequeño y umbrales en lectura.
    - Añade ETag y Last-Modified con metadatos del snapshot.
    """
    snap = get_snapshot_for_serve()  # current si existe; si no, previous; si no, None
    if not snap:
        raise HTTPException(status_code=503, detail="No hay snapshot en memoria aún")

    bbox_tuple = _parse_bbox(bbox)
    rows = snap["rows"]

    # Refiltrado en lectura (barato)
    if bbox_tuple:
        w, s, e, n = bbox_tuple
        rows = [r for r in rows if (r["lon"] is not None and r["lat"] is not None and w <= r["lon"] <= e and s <= r["lat"] <= n)]

    if min_frp is not None:
        rows = [r for r in rows if (r["frp_mw"] is not None and r["frp_mw"] >= float(min_frp))]

    if min_conf is not None:
        rows = [r for r in rows if (r["confidence"] is not None and float(r["confidence"]) >= float(min_conf))]

    # Encabezados de cacheado condicional
    if response is not None:
        response.headers["ETag"] = snap.get("etag", snap.get("sha256", snap["slot_ts"]))
        response.headers["Last-Modified"] = snap["downloaded_at"]
        response.headers["Cache-Control"] = "no-cache"

    return {
        "ok": True,
        "slot_ts": snap["slot_ts"],
        "downloaded_at": snap["downloaded_at"],
        "bbox_used": snap["bbox_used"],
        "count": len(rows),
        "source_url": snap["source_url"],
        "sha256": snap.get("sha256"),
        "rows": rows,
        "defaults": {"iberia_bbox": DEFAULT_IBERIA_BBOX},
    }


@app.get("/frp-pixel/by-ts")
async def frp_by_ts(
    ts: str = Query(..., description="Timestamp del slot: YYYYMMDDHHMM (UTC)"),
    bbox: Optional[str] = Query(default=None, description="w,s,e,n (lon/lat) para filtrar el resultado"),
    min_frp: Optional[float] = None,
    min_conf: Optional[float] = None,
):
    """
    Ingresa y devuelve un slot concreto bajo demanda (NO actualiza el buffer).
    Útil para auditorías o para cubrir ventanas específicas.
    """
    bbox_tuple = _parse_bbox(bbox)
    result = await ingest_slot_by_ts_async(ts, bbox_tuple, min_frp=min_frp, min_conf=min_conf)
    code = 200 if result.get("ok") else 404
    return ORJSONResponse(result, status_code=code)
