# app.py — FastAPI del microservicio LSA SAF FRP-PIXEL → JSON
# -----------------------------------------------------------------------------
# Qué hace este servicio:
#  - Mantiene en RAM un doble buffer (current/previous) con el último slot válido.
#  - /reload (con API key opcional, cabecera X-API-Key) descarga el último slot disponible y
#    conmuta el buffer. Responde 503 solo si no hay dato o el servido está desfasado.
#  - /frp-pixel/latest sirve SIEMPRE desde RAM (permite refiltrar por BBOX y umbrales) e
#    informa de la antigüedad del dato (age_min / stale).
#  - /frp-pixel/by-ts descarga/parcea un slot concreto bajo demanda (NO toca el buffer).
#  - /health informa del estado (warm/cold), el último slot, su antigüedad y la última recarga.
#
# NOTA IMPORTANTE
#  - El snapshot en RAM ya está recortado por el BBOX por defecto (Iberia).
#    /frp-pixel/latest puede refiltrar (hacerlo más pequeño o el mundo entero),
#    pero no “amplía” más allá de lo que ya hay en memoria.
#  - Si necesitas el mundo entero, usa /frp-pixel/by-ts con bbox global.
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import hmac
import asyncio
import datetime as dt
from typing import Optional, Tuple, List, Dict

from fastapi import FastAPI, Header, Query, HTTPException, Response
from fastapi.responses import ORJSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

# Importamos utilidades del ingestor
from ingest import (
    startup_warmup,
    reload_latest_async,
    get_snapshot_for_serve,
    ingest_slot_by_ts_async,
    snapshot_age_min,
    is_stale,
    get_last_reload,
    DEFAULT_IBERIA_BBOX,
    STALE_MIN,
)

# ----------------------------- Configuración -------------------------------

APP_TITLE = "LSA SAF FRP-PIXEL (Meteosat) → JSON (RAM double-buffer)"
# Opcional; si se define, protege /reload. Se envía en la cabecera X-API-Key
# (?key= se sigue aceptando por compatibilidad, pero deja la clave en los logs).
API_KEY = os.getenv("API_KEY", "")

# (Sólo para construir URL informativa en respuestas; no se usa para descargar aquí)
LSA_HOST = os.getenv("LSA_HOST", "https://datalsasaf.lsasvcs.ipma.pt")
FRP_VARIANT = os.getenv("FRP_VARIANT", "ListProduct")

# --------------------------- Inicialización FastAPI ------------------------

app = FastAPI(title=APP_TITLE, default_response_class=ORJSONResponse)

# CORS (abierto por simplicidad; ciérralo si lo necesitas)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# GZip para respuestas más ligeras
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ------------------------------ Helpers -----------------------------------

def _parse_bbox(bbox: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """
    Convierte "w,s,e,n" (lon/lat) en tupla de 4 floats.
    Devuelve None si bbox es None o cadena vacía.
    Lanza 400 si el formato es inválido.
    """
    if not bbox:
        return None
    try:
        parts = [float(x.strip()) for x in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError("bbox debe tener 4 números: w,s,e,n")
        w, s, e, n = parts
        if not (-180 <= w < e <= 180 and -90 <= s < n <= 90):
            raise ValueError("rangos inválidos para bbox (lon/lat)")
        return w, s, e, n
    except Exception as ex:
        raise HTTPException(status_code=400, detail=f"bbox inválido: {ex}")

def _iso_to_ts(slot_iso: str) -> str:
    """
    Convierte 'YYYY-MM-DDTHH:MM:SSZ' → 'YYYYMMDDHHMM' (útil para armar URL informativa).
    """
    d = dt.datetime.strptime(slot_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    return d.strftime("%Y%m%d%H%M")

def _build_info_url(slot_iso: str) -> str:
    """
    Construye una URL informativa del fichero HDF5 a partir del ISO del slot.
    (SÓLO informativa en la respuesta; las descargas las hace ingest.py)
    """
    ts = _iso_to_ts(slot_iso)  # p.ej. 202509121800
    yyyy, mm, dd = ts[:4], ts[4:6], ts[6:8]
    fname = f"HDF5_LSASAF_MSG_FRP-PIXEL-{FRP_VARIANT}_MSG-Disk_{ts}"
    path = f"/PRODUCTS/MSG/FRP-PIXEL/HDF5/{yyyy}/{mm}/{dd}/{fname}"
    return f"{LSA_HOST}{path}"

def _refilter_rows(
    rows: List[Dict],
    bbox: Optional[Tuple[float, float, float, float]],
    min_frp: Optional[float],
    min_conf: Optional[float],
) -> List[Dict]:
    """
    Refiltra filas en lectura usando claves correctas ('latitude'/'longitude').
    - min_conf acepta 0–1 o 0–100.
    """
    out = rows
    if bbox:
        w, s, e, n = bbox
        out = [
            r for r in out
            if (r.get("longitude") is not None and r.get("latitude") is not None
                and w <= float(r["longitude"]) <= e
                and s <= float(r["latitude"]) <= n)
        ]
    if min_frp is not None:
        thr = float(min_frp)
        out = [r for r in out if (r.get("frp_mw") is not None and float(r["frp_mw"]) >= thr)]
    if min_conf is not None:
        thr = float(min_conf)
        if thr > 1.0:  # si viene en 0–100 lo pasamos a 0–1
            thr = thr / 100.0
        out = [r for r in out if (r.get("confidence") is not None and float(r["confidence"]) >= thr)]
    return out

# ------------------------------- Endpoints ---------------------------------

@app.get("/", response_class=PlainTextResponse)
async def root():
    return (
        f"{APP_TITLE}\n"
        f"- /health\n"
        f"- /reload (cabecera X-API-Key)\n"
        f"- /frp-pixel/latest?bbox=w,s,e,n&min_frp=&min_conf=\n"
        f"- /frp-pixel/by-ts?ts=YYYYMMDDHHMM&bbox=w,s,e,n&min_frp=&min_conf=\n"
    )

@app.on_event("startup")
async def _on_startup():
    # Warm-up no bloqueante (si falla, el cron lo arregla)
    asyncio.create_task(startup_warmup())

@app.get("/health")
async def health():
    """
    - cold: no hay snapshot aún.
    - warm: hay snapshot en memoria (current o, si no, previous).
    Siempre 200 (el proceso está vivo); la frescura del dato va en stale/age_min.
    """
    snap = get_snapshot_for_serve()
    if not snap:
        return {"ok": True, "status": "cold", "stale": True, "last_reload": get_last_reload()}
    age = snapshot_age_min(snap)
    return {
        "ok": True,
        "status": "warm",
        "last_slot": snap.get("slot_ts"),
        "downloaded_at": snap.get("downloaded_at"),
        "count": snap.get("count", 0),
        "age_min": age,
        "stale": is_stale(age),
        "stale_after_min": STALE_MIN,
        "last_reload": get_last_reload(),
    }

@app.get("/reload")
async def reload(
    x_api_key: Optional[str] = Header(default=None),
    key: Optional[str] = Query(default=None, description="(obsoleto) API key por query; usar X-API-Key"),
):
    """
    Descarga el último slot disponible (con fallbacks) y publica el snapshot
    si pasa validaciones. Protegible con API_KEY (env) + cabecera X-API-Key.
    """
    if API_KEY:
        provided = x_api_key or key or ""
        if not hmac.compare_digest(provided.encode(), API_KEY.encode()):
            raise HTTPException(status_code=401, detail="API key inválida")
    result = await reload_latest_async()
    code = 200 if result.get("ok") else 503
    return ORJSONResponse(result, status_code=code)

@app.get("/frp-pixel/latest")
async def frp_latest(
    bbox: Optional[str] = Query(default=None, description="w,s,e,n (lon/lat) para refiltrar en lectura"),
    min_frp: Optional[float] = Query(default=None, description="umbral FRP mínimo (MW)"),
    min_conf: Optional[float] = Query(default=None, description="umbral de confianza mínimo (0–1 o 0–100)"),
    response: Response = None,
):
    """
    Sirve SIEMPRE desde RAM el último dataset válido (preprocesado con BBOX Iberia por defecto).
    Permite:
      - Refiltrar por un BBOX más pequeño (o el mundo entero).
      - Filtrar por FRP mínimo y confianza mínima.
    """
    snap = get_snapshot_for_serve()  # current si existe; si no, previous; si no, None
    if not snap:
        raise HTTPException(status_code=503, detail="No hay snapshot en memoria aún")

    # Parseo del bbox "w,s,e,n" → tupla de floats; None si no se pasa
    bbox_tuple: Optional[Tuple[float, float, float, float]] = _parse_bbox(bbox)

    # Partimos de las filas ya preparadas en memoria
    rows = snap.get("rows", [])
    rows = _refilter_rows(rows, bbox_tuple, min_frp=min_frp, min_conf=min_conf)

    # Cabeceras útiles (opcional)
    etag = snap.get("sha256", "")
    lastmod = snap.get("downloaded_at", "")
    if response is None:
        response = Response()
    if etag:
        response.headers["ETag"] = etag
    if lastmod:
        response.headers["Last-Modified"] = lastmod
    response.headers["Cache-Control"] = "no-cache"

    # Respuesta
    age = snapshot_age_min(snap)
    payload = {
        "ok": True,
        "slot_ts": snap.get("slot_ts"),
        "downloaded_at": snap.get("downloaded_at"),
        "age_min": age,                # minutos desde el inicio del slot servido
        "stale": is_stale(age),        # True si age_min > stale_after_min (dato desfasado)
        "stale_after_min": STALE_MIN,
        "bbox_used": {"w": DEFAULT_IBERIA_BBOX[0], "s": DEFAULT_IBERIA_BBOX[1],
                      "e": DEFAULT_IBERIA_BBOX[2], "n": DEFAULT_IBERIA_BBOX[3]},
        "count": len(rows),
        "source_url": _build_info_url(snap.get("slot_ts")),  # informativo
        "sha256": snap.get("sha256"),
        "rows": rows,
        "defaults": {"iberia_bbox": DEFAULT_IBERIA_BBOX},
    }
    return ORJSONResponse(payload, status_code=200)

@app.get("/frp-pixel/by-ts")
async def frp_by_ts(
    ts: str = Query(..., description="Timestamp del slot: YYYYMMDDHHMM (UTC)"),
    bbox: Optional[str] = Query(default=None, description="w,s,e,n (lon/lat) para filtrar el resultado"),
    min_frp: Optional[float] = Query(default=None, description="umbral FRP mínimo (MW)"),
    min_conf: Optional[float] = Query(default=None, description="umbral de confianza mínimo (0–1 o 0–100)"),
):
    """
    Ingresa y devuelve un slot concreto bajo demanda (NO actualiza el buffer).
    Útil para auditorías o para cubrir ventanas específicas.
    """
    bbox_tuple = _parse_bbox(bbox)
    result = await ingest_slot_by_ts_async(ts, bbox_tuple, min_frp=min_frp, min_conf=min_conf)
    code = 200 if result.get("ok") else 404
    # Añadimos URL informativa si el parseo ha ido bien
    if result.get("ok") and result.get("slot_ts"):
        try:
            result["source_url"] = _build_info_url(result["slot_ts"])
        except Exception:
            pass
    return ORJSONResponse(result, status_code=code)

# ------------------------------ Main (dev) --------------------------------

if __name__ == "__main__":
    # Para ejecución local: uvicorn app:app --reload
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
