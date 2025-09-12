"""
ingest.py — Lógica de ingesta/parseo y doble buffer en RAM.

- Mantiene dos snapshots globales en memoria: 'current' (último válido) y 'previous' (penúltimo).
- 'reload_latest_async()' descarga, valida, parsea y, si es correcto, conmute current/previous de forma atómica.
- 'get_snapshot_for_serve()' expone el snapshot para servirlo (current > previous).
- 'ingest_slot_by_ts_async()' procesa un slot concreto a demanda (sin tocar el buffer).
"""

import os
import io
import re
import hashlib
import asyncio
import datetime as dt
from typing import Optional, Tuple, List, Dict, Any

import requests
import h5py

# ==========================
# Configuración por entorno
# ==========================

# Margen para calcular "último slot" (en minutos). 30 reduce 404 por latencia.
LAG_MIN = int(os.getenv("LAG_MIN", "30"))

# BBOX Iberia por defecto (w,s,e,n). Ajusta si quieres incluir/excluir zonas.
DEFAULT_IBERIA_BBOX = tuple(map(float, os.getenv("IBERIA_BBOX", "-9.5,35.5,3.5,44.5").split(",")))

# Credenciales LSA SAF (Basic Auth). Se usan solo desde el servidor.
LSASAF_USER = os.getenv("LSASAF_USER", "")
LSASAF_PASS = os.getenv("LSASAF_PASS", "")

# Intentos de fallback si 404: t, t-15, t-30 (FALLBACKS=2)
FALLBACKS = int(os.getenv("FALLBACKS", "2"))

# Timeout de red (seg)
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))

# Host LSA SAF (HDF5 FRP-PIXEL)
LSA_HOST = "https://datalsasaf.lsasvcs.ipma.pt"


# =========================================
# Estado global: doble buffer + sincronía
# =========================================

_current: Optional[Dict[str, Any]] = None
_previous: Optional[Dict[str, Any]] = None
_lock = asyncio.Lock()  # protege las conmutaciones y lecturas/actualizaciones


# ==========================
# Utilidades genéricas
# ==========================

def _floor_to_quarter(dt_utc: dt.datetime) -> dt.datetime:
    """Redondea hacia abajo al cuarto de hora en UTC."""
    minute = (dt_utc.minute // 15) * 15
    return dt_utc.replace(minute=minute, second=0, microsecond=0)


def _ts_for_latest(now_utc: Optional[dt.datetime] = None, lag_min: int = LAG_MIN) -> str:
    """Devuelve el timestamp del slot candidato (YYYYMMDDHHMM) usando floorUTC - lag."""
    if now_utc is None:
        now_utc = dt.datetime.utcnow()
    base = _floor_to_quarter(now_utc) - dt.timedelta(minutes=lag_min)
    return base.strftime("%Y%m%d%H%M")


def _candidate_ts_list(now_utc: Optional[dt.datetime] = None) -> List[str]:
    """
    Genera lista de TS a probar: t, t-15, t-30, ... según FALLBACKS.
    """
    t0 = dt.datetime.strptime(_ts_for_latest(now_utc), "%Y%m%d%H%M")
    out = [t0.strftime("%Y%m%d%H%M")]
    for i in range(1, FALLBACKS + 1):
        ti = t0 - dt.timedelta(minutes=15 * i)
        out.append(ti.strftime("%Y%m%d%H%M"))
    return out


def _build_url_from_ts(ts: str) -> str:
    """Construye la URL determinista del HDF5 FRP-PIXEL a partir del TS."""
    yyyy, mm, dd, hh, mi = ts[:4], ts[4:6], ts[6:8], ts[8:10], ts[10:12]
    path = f"/PRODUCTS/MSG/FRP-PIXEL/HDF5/{yyyy}/{mm}/{dd}/HDF5_LSASAF_MSG_FRP-PIXEL-ListProduct_MSG-Disk_{ts}"
    return f"{LSA_HOST}{path}"


def _download_hdf5(url: str) -> bytes:
    """Descarga el binario HDF5 con Basic Auth, valida cabeceras y devuelve bytes."""
    auth = (LSASAF_USER, LSASAF_PASS) if LSASAF_USER and LSASAF_PASS else None
    r = requests.get(
        url,
        auth=auth,
        headers={
            "Accept": "application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": "LSA-SAF-FRP-Bridge/1.0",
            "Cache-Control": "no-cache",
        },
        timeout=HTTP_TIMEOUT,
    )
    if r.status_code == 404:
        raise FileNotFoundError("404 Not Found")
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {r.reason}")
    b = r.content
    # Firma HDF5: 0x89 0x48 0x44 0x46 0x0D 0x0A 0x1A 0x0A
    if len(b) < 8 or b[:8] != b"\x89HDF\r\n\x1a\n":
        raise ValueError("Contenido no parece HDF5 (firma inválida)")
    return b


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _flatten(x):
    """Convierte arrays de h5py (incl. Nx1) a listas nativas de Python."""
    try:
        import numpy as np  # opcional, por si está presente
        arr = np.array(x)
        return arr.reshape(-1).tolist()
    except Exception:
        # Sin numpy: convertir con list() y aplanar lo básico
        try:
            return [v for v in x]
        except Exception:
            return []


def _parse_h5(h5bytes: bytes) -> Dict[str, Any]:
    """
    Parsea el HDF5 de FRP-PIXEL de forma robusta (descubre datasets por nombre).
    Devuelve un dict con 'rows': lista de detecciones.
    """
    with h5py.File(io.BytesIO(h5bytes), "r") as f:
        paths: List[str] = []
        f.visit(paths.append)

        def find_one(regex_list: List[str]):
            for p in paths:
                for rg in regex_list:
                    if re.search(rg, p, re.IGNORECASE):
                        try:
                            return f[p][:]
                        except Exception:
                            pass
            return None

        # Buscamos datasets habituales por regex tolerantes
        lat = find_one([r"/lat(i(tude)?)?$", r"/.*lat$"])
        lon = find_one([r"/lon(g(i(tude)?)?)?$", r"/.*lon$"])
        frp = find_one([r"/frp(?!.*grid)"])
        unc = find_one([r"/frp_?unc(|_mw)?$", r"/uncert"])
        conf = find_one([r"/conf(idence)?$"])
        area = find_one([r"/(pixel_)?area"])
        tim = find_one([r"/time"])

        # Aplanamos a listas
        latL = _flatten(lat) if lat is not None else []
        lonL = _flatten(lon) if lon is not None else []
        frpL = _flatten(frp) if frp is not None else []
        uncL = _flatten(unc) if unc is not None else []
        confL = _flatten(conf) if conf is not None else []
        areaL = _flatten(area) if area is not None else []
        timL = _flatten(tim) if tim is not None else []

        n = max(len(latL), len(lonL), len(frpL), len(uncL), len(confL), len(areaL), len(timL), 0)

        rows: List[Dict[str, Any]] = []
        for i in range(n):
            rows.append(
                {
                    "lat": float(latL[i]) if i < len(latL) else None,
                    "lon": float(lonL[i]) if i < len(lonL) else None,
                    "frp_mw": float(frpL[i]) if i < len(frpL) else None,
                    "frp_unc_mw": float(uncL[i]) if i < len(uncL) else None,
                    "confidence": float(confL[i]) if i < len(confL) else None,
                    "pixel_km2": float(areaL[i]) if i < len(areaL) else None,
                    "time_raw": float(timL[i]) if i < len(timL) else None,
                }
            )

        return {"rows": rows}


def _filter_bbox(rows: List[Dict[str, Any]], bbox: Tuple[float, float, float, float]) -> List[Dict[str, Any]]:
    """Filtra por BBOX (w,s,e,n)."""
    w, s, e, n = bbox
    out = []
    for r in rows:
        la, lo = r.get("lat"), r.get("lon")
        if la is None or lo is None:
            continue
        if w <= lo <= e and s <= la <= n:
            out.append(r)
    return out


def _apply_thresholds(
    rows: List[Dict[str, Any]],
    min_frp: Optional[float] = None,
    min_conf: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Aplica umbrales opcionales en memoria."""
    out = rows
    if min_frp is not None:
        out = [r for r in out if (r["frp_mw"] is not None and r["frp_mw"] >= float(min_frp))]
    if min_conf is not None:
        out = [r for r in out if (r["confidence"] is not None and float(r["confidence"]) >= float(min_conf))]
    return out


def _slot_iso_from_ts(ts: str) -> str:
    """Convierte YYYYMMDDHHMM (UTC) a ISO Z."""
    d = dt.datetime.strptime(ts, "%Y%m%d%H%M")
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


# ==================================
# API interna para el doble buffer
# ==================================

def _build_snapshot(ts: str, h5bytes: bytes, bbox_used: Tuple[float, float, float, float]) -> Dict[str, Any]:
    """Crea el snapshot listo para servir (filtrado Iberia + metadatos)."""
    parsed = _parse_h5(h5bytes)
    rows_all: List[Dict[str, Any]] = parsed["rows"]
    rows_bbox = _filter_bbox(rows_all, bbox_used)

    iso = _slot_iso_from_ts(ts)
    sha = _sha256(h5bytes)

    snap = {
        "slot_ts": iso,
        "downloaded_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bbox_used": list(bbox_used),
        "count": len(rows_bbox),
        "rows": rows_bbox,
        "sha256": sha,
        "etag": sha,
        "source_url": _build_url_from_ts(ts),
    }
    return snap


def get_snapshot_for_serve() -> Optional[Dict[str, Any]]:
    """Devuelve current si hay; si no, previous; si no, None."""
    # No requiere lock para lectura simple (lecturas de referencias son atómicas en CPython),
    # pero si quieres máxima seguridad, puedes envolver en lock.
    return _current or _previous


async def reload_latest_async() -> Dict[str, Any]:
    """
    Descarga/parsea/filtra el último slot candidato y conmuta el buffer si es válido.
    Reglas:
      - Monotonía: no reemplazar por slots más antiguos.
      - Idempotencia: si sha256 coincide, no conmuta.
      - Sanidad: no aceptar vacíos si antes teníamos datos (opcional).
    """
    async with _lock:
        now = dt.datetime.utcnow()
        tried: List[str] = []
        last_err: Optional[str] = None

        for ts in _candidate_ts_list(now):
            tried.append(ts)
            url = _build_url_from_ts(ts)
            try:
                h5 = _download_hdf5(url)
                new_snap = _build_snapshot(ts, h5, DEFAULT_IBERIA_BBOX)

                # Monotonía
                if _current:
                    cur_ts = _current["slot_ts"]
                    if new_snap["slot_ts"] < cur_ts:
                        return {"ok": False, "reason": "older_slot", "tried": tried, "chosen": ts}

                # Idempotencia
                if _current and new_snap["sha256"] == _current.get("sha256"):
                    return {"ok": True, "reason": "not_changed", "slot_ts": new_snap["slot_ts"], "count": new_snap["count"]}

                # Sanidad (opcional): no aceptar vacío si antes había datos
                if _current and new_snap["count"] == 0 and _current["count"] > 0:
                    last_err = "candidate_empty_rejected"
                    continue

                # Conmutación atómica
                global _current, _previous
                _previous = _current
                _current = new_snap

                return {"ok": True, "reason": "swapped", "slot_ts": new_snap["slot_ts"], "count": new_snap["count"], "url": url}

            except FileNotFoundError:
                last_err = "404"
                continue
            except Exception as e:
                last_err = str(e)
                continue

        # Si llegamos aquí, no hubo suerte con ninguno de los fallbacks
        return {"ok": False, "reason": last_err or "unknown", "tried": tried}


async def ingest_slot_by_ts_async(
    ts: str,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    min_frp: Optional[float] = None,
    min_conf: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Procesa un TS concreto bajo demanda y devuelve JSON (NO altera el buffer en RAM).
    """
    # Validación TS
    if not re.fullmatch(r"\d{12}", ts):
        return {"ok": False, "error": "ts inválido (esperado YYYYMMDDHHMM)"}

    url = _build_url_from_ts(ts)
    try:
        h5 = _download_hdf5(url)
    except FileNotFoundError:
        return {"ok": False, "error": "404 Not Found", "ts": ts}
    except Exception as e:
        return {"ok": False, "error": f"HTTP/descarga: {e}", "ts": ts}

    parsed = _parse_h5(h5)
    rows = parsed["rows"]

    # Filtrado por BBOX (si no se especifica, usamos Iberia por defecto)
    bx = bbox or DEFAULT_IBERIA_BBOX
    rows = _filter_bbox(rows, bx)

    # Umbrales opcionales
    rows = _apply_thresholds(rows, min_frp=min_frp, min_conf=min_conf)

    return {
        "ok": True,
        "slot_ts": _slot_iso_from_ts(ts),
        "downloaded_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bbox_used": list(bx),
        "count": len(rows),
        "source_url": url,
        "sha256": _sha256(h5),
        "rows": rows,
    }


async def startup_warmup():
    """
    Intenta una primera ingesta de arranque en background. No levanta excepción.
    """
    try:
        await reload_latest_async()
    except Exception:
        # Silencioso: el cron del panel reintenta en minutos
        pass
