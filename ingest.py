# ingest.py — Microservicio LSA SAF FRP-PIXEL → JSON (doble buffer en RAM)
# -----------------------------------------------------------------------------
# Qué hace:
#  - Descarga ficheros HDF5 del FRP-PIXEL (Meteosat, LSA SAF) vía HTTP (Basic Auth opcional).
#  - Parsea el “List Product” vinculando datasets por nombre real (LATITUDE/LONGITUDE/FRP…).
#  - Aplica SCALING_FACTOR (÷) y MISSING_VALUE (máscara); compatibilidad con scale_factor/_FillValue.
#  - Añade acq_time_utc (ISO) a partir de time_raw (HHMM) usando la fecha del slot.
#  - Valida sanidad del slot y mantiene doble buffer en memoria (current/previous).
#  - Expone helpers para FastAPI: startup_warmup, reload_latest_async, get_snapshot_for_serve,
#    ingest_slot_by_ts_async y la constante DEFAULT_IBERIA_BBOX.
# -----------------------------------------------------------------------------
# NOTA: Mantiene la misma interfaz pública esperada por app.py.

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import io
import os
from typing import Any, Dict, List, Optional, Tuple

import h5py  # type: ignore
import numpy as np  # type: ignore
import requests  # type: ignore

# ----------------------------- Configuración -------------------------------

# BBOX Iberia por defecto (w,s,e,n). Se usa cuando no se especifica bbox.
DEFAULT_IBERIA_BBOX: Tuple[float, float, float, float] = tuple(
    map(float, os.getenv("IBERIA_BBOX", "-9.5,35.5,3.5,44.5").split(","))
)  # type: ignore

# Credenciales LSA SAF (Basic Auth).
LSASAF_USER = os.getenv("LSASAF_USER", "")
LSASAF_PASS = os.getenv("LSASAF_PASS", "")

# Host base de descarga (IPMA/LSA SAF).
LSA_HOST = os.getenv("LSA_HOST", "https://datalsasaf.lsasvcs.ipma.pt")

# Variante de fichero para el nombre FRP-PIXEL-<VAR> (normalmente 'ListProduct').
FRP_VARIANT = os.getenv("FRP_VARIANT", "ListProduct")

# Minutos de margen para que el último slot esté publicado (evita race).
LAG_MIN = int(os.getenv("LAG_MIN", "30"))

# Intentos de fallback: t, t-15, t-30, ... (6 ⇒ 90 minutos hacia atrás).
FALLBACKS = int(os.getenv("FALLBACKS", "8"))

# Timeout de red (segundos).
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

# ---------------------------- Estado en memoria ---------------------------

_lock = asyncio.Lock()
_current: Optional[Dict[str, Any]] = None
_previous: Optional[Dict[str, Any]] = None

# ----------------------------- Utilidades ---------------------------------

def _floor_to_quarter(t: dt.datetime) -> dt.datetime:
    """Redondea hacia abajo a múltiplos de 15 minutos (en UTC)."""
    m = (t.minute // 15) * 15
    return t.replace(minute=m, second=0, microsecond=0)

def _ts_for_latest(now_utc: Optional[dt.datetime] = None) -> str:
    """Devuelve el ts YYYYMMDDHHMM del último slot *publicable* con LAG_MIN aplicado."""
    if now_utc is None:
        now_utc = dt.datetime.utcnow()
    base = _floor_to_quarter(now_utc - dt.timedelta(minutes=LAG_MIN))
    return base.strftime("%Y%m%d%H%M")

def _candidate_ts_list(now_utc: Optional[dt.datetime] = None) -> List[str]:
    """Lista de candidatos: [t, t-15, t-30, ...] en formato YYYYMMDDHHMM."""
    start = _ts_for_latest(now_utc)
    base_dt = dt.datetime.strptime(start, "%Y%m%d%H%M")
    return [(base_dt - dt.timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M") for i in range(FALLBACKS + 1)]

def _build_url_from_ts(ts: str) -> str:
    """Construye la URL de descarga a partir del timestamp."""
    yyyy, mm, dd = ts[:4], ts[4:6], ts[6:8]
    fname = f"HDF5_LSASAF_MSG_FRP-PIXEL-{FRP_VARIANT}_MSG-Disk_{ts}"
    path = f"/PRODUCTS/MSG/FRP-PIXEL/HDF5/{yyyy}/{mm}/{dd}/{fname}"
    return f"{LSA_HOST}{path}"

def _download_hdf5(url: str) -> bytes:
    """Descarga el HDF5 (bytes) con Basic Auth opcional."""
    auth = (LSASAF_USER, LSASAF_PASS) if LSASAF_USER or LSASAF_PASS else None
    r = requests.get(url, timeout=HTTP_TIMEOUT, auth=auth)
    r.raise_for_status()
    return r.content

def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _slot_iso_from_ts(ts: str) -> str:
    """Devuelve el inicio de slot en ISO Zulu (p.ej. '2025-09-12T18:00:00Z')."""
    dtobj = dt.datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=dt.timezone.utc)
    return dtobj.strftime("%Y-%m-%dT%H:%M:%SZ")

# ----------------------- Lectura segura de datasets -----------------------

def _read_scaled(dset: h5py.Dataset) -> np.ma.MaskedArray:
    """
    Lee un dataset HDF5 y aplica:
    - Enmascarado por MISSING_VALUE o _FillValue.
    - Escalado:
        * Si existe SCALING_FACTOR ⇒ valor_final = valor_crudo / SCALING_FACTOR
        * En su defecto, usar scale_factor y add_offset (CF) ⇒ valor*scale_factor + add_offset
    Devuelve un masked array 1D/ND (se respeta la forma original).
    """
    data = np.array(dset[()])  # ndarray
    # Máscara por MISSING_VALUE o _FillValue
    missing = dset.attrs.get("MISSING_VALUE", dset.attrs.get("_FillValue", None))
    if missing is not None:
        data = np.ma.masked_where(data == missing, data)
    else:
        data = np.ma.masked_invalid(data)

    # Escalado
    if "SCALING_FACTOR" in dset.attrs:
        sf = float(dset.attrs.get("SCALING_FACTOR", 1.0) or 1.0)
        if sf not in (0.0, 1.0):
            data = data.astype(np.float64) / sf
        else:
            data = data.astype(np.float64)
    else:
        scale = float(dset.attrs.get("scale_factor", 1.0) or 1.0)
        offset = float(dset.attrs.get("add_offset", 0.0) or 0.0)
        data = data.astype(np.float64) * scale + offset

    return data

def _find_datasets_by_name(f: h5py.File) -> Dict[str, h5py.Dataset]:
    """
    Vinculación ESTRICTA por nombre (case-insensitive), sin asumir grupos.
    Se toma el *último componente* de la ruta HDF5 y se iguala a alguno de estos:
      - LATITUDE / LONGITUDE / FRP  (obligatorios)
      - FIRE_CONFIDENCE (opcional)
      - PIXEL_SIZE (área en km², opcional)
      - ACQTIME (opcional)
    Se incluyen alias razonables por compatibilidad.
    """
    mapping = {
        "lat":  ["LATITUDE", "Latitude", "lat"],
        "lon":  ["LONGITUDE", "Longitude", "lon"],
        "frp":  ["FRP", "FRP_MW", "Fire_Radiative_Power"],
        "conf": ["FIRE_CONFIDENCE", "CONFIDENCE", "Confidence"],
        "area": ["PIXEL_SIZE", "Pixel_size", "Pixel_area", "PixelArea", "Pixel_Area"],
        "time": ["ACQTIME", "TIME_UTC", "Time"],
        # ↓↓↓ nuevos (si no existen en el archivo, se ignorarán sin romper nada)
        "unc":     ["FRP_UNCERTAINTY", "FRP_Uncertainty", "Uncertainty_of_FRP", "FRP_unc", "FRPunc"],
        "vza":     ["PIXEL_VZA", "Pixel_VZA", "VZA", "View_Zenith_Angle"],
        "bt_mir":  ["BT_MIR", "BrightnessTemp_MIR", "BT_3.9um", "BT_39"],
        "bt_tir":  ["BT_TIR", "BrightnessTemp_TIR", "BT_10.8um", "BT_108"],
    }
    found: Dict[str, h5py.Dataset] = {}
    names: List[str] = []
    f.visit(names.append)  # recorre todas las rutas
    for key, options in mapping.items():
        for p in names:
            try:
                obj = f[p]
            except Exception:
                continue
            if not isinstance(obj, h5py.Dataset):
                continue
            final = p.split("/")[-1]
            if any(final.lower() == opt.lower() for opt in options):
                found[key] = obj
                break
    return found

def _coerce_1d(arr: np.ma.MaskedArray) -> np.ma.MaskedArray:
    """Aplana a 1D si es posible (el List Product es 1D)."""
    if arr.ndim == 1:
        return arr
    return arr.reshape(-1)

def _build_rows(
    lat: np.ma.MaskedArray,
    lon: np.ma.MaskedArray,
    frp: np.ma.MaskedArray,
    conf: Optional[np.ma.MaskedArray],
    area: Optional[np.ma.MaskedArray],
    tim:  Optional[np.ma.MaskedArray],
    unc:  Optional[np.ma.MaskedArray],
    vza:  Optional[np.ma.MaskedArray],
    bt_mir: Optional[np.ma.MaskedArray],
    bt_tir: Optional[np.ma.MaskedArray],
) -> List[Dict[str, Any]]:
    """
    Une campos por índice y genera la lista de detecciones. Los valores enmascarados → None.
    Indexado seguro: si algún opcional es más corto que lat/lon/frp, se rellena con None.
    """
    def _coerce(a: Optional[np.ma.MaskedArray]) -> Optional[np.ma.MaskedArray]:
        if a is None: return None
        return a if a.ndim == 1 else a.reshape(-1)

    lat   = _coerce(lat)   # obligatorios
    lon   = _coerce(lon)
    frp   = _coerce(frp)
    conf  = _coerce(conf)  # opcionales
    area  = _coerce(area)
    tim   = _coerce(tim)
    unc   = _coerce(unc)
    vza   = _coerce(vza)
    bt_mir= _coerce(bt_mir)
    bt_tir= _coerce(bt_tir)

    # longitud base = min de los obligatorios
    n = min(lat.shape[0], lon.shape[0], frp.shape[0])  # type: ignore

    def val(arr: Optional[np.ma.MaskedArray], i: int) -> Optional[float]:
        if arr is None: return None
        if i >= arr.shape[0]: return None
        x = arr[i]
        return None if np.ma.is_masked(x) else float(x)

    rows: List[Dict[str, Any]] = []
    for i in range(n):
        rows.append(
            {
                "latitude":   val(lat, i),
                "longitude":  val(lon, i),
                "frp_mw":     val(frp, i),
                "confidence": val(conf, i),
                "pixel_km2":  val(area, i),
                "time_raw":   val(tim, i),
                "frp_unc_mw": val(unc, i),
                "vza_deg":    val(vza, i),
                "bt_mir_k":   val(bt_mir, i),
                "bt_tir_k":   val(bt_tir, i),
            }
        )
    return rows

def _parse_h5(h5bytes: bytes) -> Dict[str, Any]:
    """
    Parsea el HDF5 FRP-PIXEL List Product aplicando escala/máscara correctamente.
    Si faltan datasets clave (lat/lon/frp) → levanta ValueError.
    """
    with h5py.File(io.BytesIO(h5bytes), "r") as f:
        found = _find_datasets_by_name(f)
        required = ("lat", "lon", "frp")
        if not all(k in found for k in required):
            raise ValueError("Datasets clave no encontrados (lat/lon/frp).")
        lat = _read_scaled(found["lat"])
        lon = _read_scaled(found["lon"])
        frp = _read_scaled(found["frp"])
        conf = _read_scaled(found["conf"]) if "conf" in found else None
        area = _read_scaled(found["area"]) if "area" in found else None
        tim = _read_scaled(found["time"]) if "time" in found else None
        unc     = _read_scaled(found["unc"])     if "unc"     in found else None
        vza     = _read_scaled(found["vza"])     if "vza"     in found else None
        bt_mir  = _read_scaled(found["bt_mir"])  if "bt_mir"  in found else None
        bt_tir  = _read_scaled(found["bt_tir"])  if "bt_tir"  in found else None
        rows = _build_rows(lat, lon, frp, conf, area, tim, unc, vza, bt_mir, bt_tir)
    return {"rows": rows}

# --------------------------- Filtros en lectura ---------------------------

def _filter_bbox(rows: List[Dict[str, Any]], bbox: Tuple[float, float, float, float]) -> List[Dict[str, Any]]:
    """Filtra por (w,s,e,n) en lon/lat."""
    w, s, e, n = bbox
    out = []
    for r in rows:
        la = r.get("latitude")
        lo = r.get("longitude")
        if la is None or lo is None:
            continue
        if (w <= lo <= e) and (s <= la <= n):
            out.append(r)
    return out

def _apply_thresholds(rows: List[Dict[str, Any]], min_frp: Optional[float], min_conf: Optional[float]) -> List[Dict[str, Any]]:
    """Aplica filtros por mínimos (si se dan).
    - FRP en MW.
    - Confianza admite 0–1 o 0–100 (normalizamos si viene en porcentaje).
    """
    out = []
    # Normalizamos confianza si viene en 0–100
    thr_conf = None
    if min_conf is not None:
        thr_conf = float(min_conf)
        if thr_conf > 1.0:  # p.ej. 90 → 0.90
            thr_conf /= 100.0

    thr_frp = float(min_frp) if min_frp is not None else None

    for r in rows:
        frp = r.get("frp_mw")
        conf = r.get("confidence")
        if thr_frp is not None and (frp is None or float(frp) < thr_frp):
            continue
        if thr_conf is not None and (conf is None or float(conf) < thr_conf):
            continue
        out.append(r)
    return out


# --------------------------- Validaciones slot ----------------------------

def _sanity_checks(rows: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Sanidad mínima para aceptar un slot (sobre el conjunto GLOBAL):
    - ≥ 1 fila con lat, lon y frp.
    - Si hay ≥3 filas: FRP no debe ser constante en todas.
    - (Opcional) Chequeo ligero de decimales cuando hay suficientes filas.
    """
    # Filas válidas con los tres campos presentes
    valid = [r for r in rows if r.get("frp_mw") is not None
                         and r.get("latitude") is not None
                         and r.get("longitude") is not None]
    if not valid:
        return False, "sin_filas"

    frps = [float(r["frp_mw"]) for r in valid]
    if len(frps) >= 3 and len(set(round(x, 3) for x in frps)) == 1:
        return False, "frp_constante"

    # Solo tiene sentido mirar decimales si hay varias decenas de puntos
    if len(valid) >= 10:
        any_dec = any(
            (abs(float(r["latitude"])  - round(float(r["latitude"])))  > 1e-6) or
            (abs(float(r["longitude"]) - round(float(r["longitude"]))) > 1e-6)
            for r in valid[:50]
        )
        if not any_dec:
            return False, "coord_sin_decimales"

    return True, "ok"


# ----------------------- Derivados: hora de adquisición -------------------

def _add_acq_time_utc(rows: List[Dict[str, Any]], slot_ts_iso: str) -> List[Dict[str, Any]]:
    """
    Convierte time_raw (HHMM) en acq_time_utc (ISO Z) usando la fecha del slot.
    No elimina time_raw; sólo añade acq_time_utc si es consistente.
    """
    base = dt.datetime.strptime(slot_ts_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        tr = rr.get("time_raw")
        if tr is not None:
            try:
                v = int(tr)
                hh, mm = v // 100, v % 100
                if 0 <= hh <= 23 and 0 <= mm <= 59:
                    rr["acq_time_utc"] = base.replace(hour=hh, minute=mm).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pass  # si no cuadra, no añadimos el campo
        out.append(rr)
    return out

# ------------------------------ Snapshots ---------------------------------

def _build_snapshot(ts: str, h5bytes: bytes, bbox: Tuple[float, float, float, float]) -> Dict[str, Any]:
    parsed = _parse_h5(h5bytes)
    rows_all = parsed["rows"]

    # ✅ Sanidad sobre el conjunto global (detecta parseos malos, no la ausencia regional)
    ok, reason = _sanity_checks(rows_all)

    # Luego aplicamos el BBOX de Iberia solo para la salida
    rows_bbox = _filter_bbox(rows_all, bbox)

    slot_iso = _slot_iso_from_ts(ts)
    rows_bbox = _add_acq_time_utc(rows_bbox, slot_iso)

    snap = {
        "ok": ok,
        "reason": (None if ok else reason),
        "slot_ts": slot_iso,
        "downloaded_at": dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows": rows_bbox,
        "count": len(rows_bbox),
        "sha256": _sha256(h5bytes),
    }
    return snap


# -------------------------- API para FastAPI ------------------------------

def get_snapshot_for_serve() -> Optional[Dict[str, Any]]:
    """Devuelve el snapshot 'current' si existe; si no, 'previous'; si no, None."""
    return _current or _previous

async def reload_latest_async() -> Dict[str, Any]:
    """
    Descarga y publica el último slot disponible (con fallbacks). Actualiza el doble buffer.
    Reglas:
      - Monotonía temporal (no retroceder).
      - Idempotencia por sha256.
      - Sanidad (_sanity_checks debe pasar).
    """
    global _current, _previous
    async with _lock:
        now = dt.datetime.utcnow()
        last_err: Optional[str] = None
        for ts in _candidate_ts_list(now):
            url = _build_url_from_ts(ts)
            try:
                h5 = _download_hdf5(url)
            except Exception as e:
                last_err = f"descarga_fallida:{e}"
                continue
            snap = _build_snapshot(ts, h5, DEFAULT_IBERIA_BBOX)
            if not snap["ok"]:
                last_err = f"slot_invalido:{snap['reason']}"
                continue
            # Monotonía: no retroceder
            if _current and snap["slot_ts"] <= _current["slot_ts"]:
                return {
                    "ok": True,
                    "status": "noop_monotonia",
                    "slot_ts": _current["slot_ts"],
                    "count": _current["count"],
                }
            # Idempotencia
            if _current and snap["sha256"] == _current.get("sha256"):
                return {
                    "ok": True,
                    "status": "noop_idempotente",
                    "slot_ts": _current["slot_ts"],
                    "count": _current["count"],
                }
            # Conmutación atómica
            _previous, _current = _current, snap
            return {"ok": True, "status": "publicado", "slot_ts": snap["slot_ts"], "count": snap["count"]}
        # Si llegamos aquí, no hubo candidatos válidos
        return {"ok": False, "status": "sin_candidato_valido", "error": last_err or "descargas_fallidas"}

async def ingest_slot_by_ts_async(
    ts: str,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    min_frp: Optional[float] = None,
    min_conf: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Descarga y parsea UN slot (no altera el buffer). Permite refiltrar por bbox y umbrales.
    """
    bbox = bbox or DEFAULT_IBERIA_BBOX
    url = _build_url_from_ts(ts)
    try:
        h5 = _download_hdf5(url)
    except Exception as e:
        return {"ok": False, "status": "descarga_fallida", "error": str(e)}
    try:
        parsed = _parse_h5(h5)
    except Exception as e:
        return {"ok": False, "status": "parse_fallido", "error": str(e)}
    rows = _filter_bbox(parsed["rows"], bbox)
    rows = _apply_thresholds(rows, min_frp=min_frp, min_conf=min_conf)
    slot_iso = _slot_iso_from_ts(ts)
    rows = _add_acq_time_utc(rows, slot_iso)
    resp = {
        "ok": True,
        "slot_ts": slot_iso,
        "downloaded_at": dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(rows),
        "bbox": {"w": bbox[0], "s": bbox[1], "e": bbox[2], "n": bbox[3]},
        "rows": rows,
    }
    return resp

async def startup_warmup():
    """Lanza una recarga en background al iniciar el proceso (silenciosa)."""
    try:
        await reload_latest_async()
    except Exception:
        # Silencioso: el cron hará /reload en minutos
        pass




