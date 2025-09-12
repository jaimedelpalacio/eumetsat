# ingest.py — Microservicio LSA SAF FRP-PIXEL → JSON (doble buffer en RAM)
# -----------------------------------------------------------------------------
# Qué hace:
#  - Descarga ficheros HDF5 del producto FRP-PIXEL (Meteosat, LSA SAF) vía HTTP Basic Auth.
#  - Parsea la lista de detecciones (ListProduct) de forma robusta:
#       1) por nombres (regex) y
#       2) si no encuentra nada, heurística “flex” por rangos/longitudes.
#  - Filtra por BBOX Iberia (por defecto) y devuelve JSON con metadatos.
#  - Mantiene en memoria dos snapshots: current (último válido) y previous (penúltimo).
#  - Expone funciones para FastAPI: startup_warmup, reload_latest_async,
#    get_snapshot_for_serve, ingest_slot_by_ts_async y la constante DEFAULT_IBERIA_BBOX.
# -----------------------------------------------------------------------------
# NOTA: Este fichero está listo para reemplazar tu ingest.py actual (drop-in).
#       No requiere cambios en app.py (si usas el que te pasé).

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

# Margen (min) para calcular “slot candidato” si lo necesitas en otras partes (no imprescindible aquí).
LAG_MIN = int(os.getenv("LAG_MIN", "30"))

# BBOX Iberia por defecto (w,s,e,n). Ajusta si quieres incluir/excluir zonas.
DEFAULT_IBERIA_BBOX: Tuple[float, float, float, float] = tuple(
    map(float, os.getenv("IBERIA_BBOX", "-9.5,35.5,3.5,44.5").split(","))
)  # type: ignore

# Credenciales LSA SAF (Basic Auth). Se usan solo desde el servidor.
LSASAF_USER = os.getenv("LSASAF_USER", "")
LSASAF_PASS = os.getenv("LSASAF_PASS", "")

# Intentos de fallback si el último slot aún no está publicado: t, t-15, t-30, …
FALLBACKS = int(os.getenv("FALLBACKS", "6"))  # p.ej. 6 = cubre 90 minutos

# Timeout de red (segundos)
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))

# Host LSA SAF (HDF5 FRP-PIXEL)
LSA_HOST = "https://datalsasaf.lsasvcs.ipma.pt"

# Variante del producto que construye el nombre de fichero:
#   - "ListProduct" (recomendado; contiene la lista de detecciones)
#   - "QualityProduct" (banderas/calidad; NO trae la lista de puntos)
FRP_VARIANT = os.getenv("FRP_VARIANT", "ListProduct")


# =========================================
# Estado global: doble buffer + sincronía
# =========================================

_current: Optional[Dict[str, Any]] = None
_previous: Optional[Dict[str, Any]] = None
_lock = asyncio.Lock()  # asegura conmutaciones atómicas


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
    Genera la lista de timestamps candidatos, de más reciente a más antiguo:
    - Primero el slot actual (UTC redondeado al cuarto de hora).
    - Luego retrocede en pasos de 15 min hasta 'FALLBACKS'.
    Mantiene 'return out' para ser sustitución directa.
    """
    if now_utc is None:
        now_utc = dt.datetime.utcnow()

    base = _floor_to_quarter(now_utc)  # p. ej., 15:30, 15:45, etc. (UTC)
    out: List[str] = []
    for i in range(0, FALLBACKS + 1):
        ti = base - dt.timedelta(minutes=15 * i)
        out.append(ti.strftime("%Y%m%d%H%M"))

    return out


def _build_url_from_ts(ts: str) -> str:
    """URL del HDF5 FRP-PIXEL para el timestamp y la variante elegida."""
    yyyy, mm, dd = ts[:4], ts[4:6], ts[6:8]
    fname = f"HDF5_LSASAF_MSG_FRP-PIXEL-{FRP_VARIANT}_MSG-Disk_{ts}"
    path = f"/PRODUCTS/MSG/FRP-PIXEL/HDF5/{yyyy}/{mm}/{dd}/{fname}"
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


def _slot_iso_from_ts(ts: str) -> str:
    """Convierte YYYYMMDDHHMM (UTC) a ISO Z."""
    d = dt.datetime.strptime(ts, "%Y%m%d%H%M")
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


# ==========================
# Filtrado y umbrales
# ==========================

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


# ==========================
# Parser HDF5 robusto
# ==========================

def _flatten_1d_to_list(x) -> List[float]:
    """
    Aplana datasets 1D a lista de float sin depender de numpy.
    Si la entrada es escalar o multidimensional, devuelve lista vacía.
    """
    try:
        # h5py Dataset: acceso con [:] si 1D
        if hasattr(x, "shape"):
            if len(x.shape) != 1 or x.shape[0] == 0:
                return []
            return [float(v) for v in x[:]]
        # memoria ya cargada (lista/tupla)
        if isinstance(x, (list, tuple)):
            return [float(v) for v in x]
    except Exception:
        pass
    return []


def _parse_h5(h5bytes: bytes) -> Dict[str, Any]:
    """
    Parsea el HDF5 de FRP-PIXEL.
    - 1º: intenta localizar datasets por nombre (regex).
    - 2º: si no hay filas, fallback “flex” que inspecciona datasets 1D
         y detecta lat/lon/FRP por heurística de rangos/valores.
    Devuelve dict con 'rows': lista de detecciones.
    """
    def _by_regex(f: h5py.File) -> Dict[str, Any]:
        paths: List[str] = []
        f.visit(paths.append)

        def find_one(regex_list: List[str]):
            for p in paths:
                for rg in regex_list:
                    if re.search(rg, p, re.IGNORECASE):
                        try:
                            return f[p]
                        except Exception:
                            pass
            return None

        lat = find_one([r"/lat(i(tude)?)?$", r"/(?:^|/)lat$"])
        lon = find_one([r"/lon(g(i(tude)?)?)?$", r"/(?:^|/)lon$"])
        frp = find_one([r"/frp(?!.*grid)"])
        unc = find_one([r"/frp_?unc(|_mw)?$", r"/uncert"])
        conf = find_one([r"/conf(idence)?$"])
        area = find_one([r"/(pixel_)?area"])
        tim = find_one([r"/time"])

        latL = _flatten_1d_to_list(lat) if lat is not None else []
        lonL = _flatten_1d_to_list(lon) if lon is not None else []
        frpL = _flatten_1d_to_list(frp) if frp is not None else []
        uncL = _flatten_1d_to_list(unc) if unc is not None else []
        confL = _flatten_1d_to_list(conf) if conf is not None else []
        areaL = _flatten_1d_to_list(area) if area is not None else []
        timL = _flatten_1d_to_list(tim) if tim is not None else []

        n = max(len(latL), len(lonL), len(frpL), len(uncL), len(confL), len(areaL), len(timL), 0)
        if n == 0:
            return {"rows": []}

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

def _parse_h5(h5bytes: bytes) -> Dict[str, Any]:
    """
    Parsea el HDF5 de FRP-PIXEL y devuelve {'rows': [...] }.
    Orden de intentos:
      1) Datasets 1D por NOMBRE (regex) -> lat/lon/frp/etc.
      2) Datasets COMPUESTOS (dtype.names) -> extrae campos lat/lon/frp/etc.
      3) Fallback FLEX (heurística sobre 1D numéricos).
    """
    def _flatten_1d_to_list(x) -> List[float]:
        # Aplana dataset 1D a lista de float; otras formas -> []
        try:
            if hasattr(x, "shape"):
                if len(x.shape) != 1 or x.shape[0] == 0:
                    return []
                return [float(v) for v in x[:]]
            if isinstance(x, (list, tuple)):
                return [float(v) for v in x]
        except Exception:
            pass
        return []

    def _by_regex(f: h5py.File) -> List[Dict[str, Any]]:
        # Busca por nombres típicos
        paths: List[str] = []
        f.visit(paths.append)

        def find_one(regex_list: List[str]):
            for p in paths:
                for rg in regex_list:
                    if re.search(rg, p, re.IGNORECASE):
                        try:
                            return f[p]
                        except Exception:
                            pass
            return None

        lat = find_one([r"/lat(i(tude)?)?$", r"/(?:^|/)lat$"])
        lon = find_one([r"/lon(g(i(tude)?)?)?$", r"/(?:^|/)lon$"])
        frp = find_one([r"/frp(?!.*grid)"])
        unc = find_one([r"/frp_?unc(|_mw)?$", r"/uncert"])
        conf = find_one([r"/conf(idence)?$"])
        area = find_one([r"/(pixel_)?area"])
        tim = find_one([r"/time"])

        latL = _flatten_1d_to_list(lat)  if lat  is not None else []
        lonL = _flatten_1d_to_list(lon)  if lon  is not None else []
        frpL = _flatten_1d_to_list(frp)  if frp  is not None else []
        uncL = _flatten_1d_to_list(unc)  if unc  is not None else []
        conL = _flatten_1d_to_list(conf) if conf is not None else []
        areL = _flatten_1d_to_list(area) if area is not None else []
        timL = _flatten_1d_to_list(tim)  if tim  is not None else []

        n = max(len(latL), len(lonL), len(frpL), len(uncL), len(conL), len(areL), len(timL), 0)
        if n == 0:
            return []

        rows: List[Dict[str, Any]] = []
        for i in range(n):
            rows.append({
                "lat":        float(latL[i])  if i < len(latL) else None,
                "lon":        float(lonL[i])  if i < len(lonL) else None,
                "frp_mw":     float(frpL[i])  if i < len(frpL) else None,
                "frp_unc_mw": float(uncL[i])  if i < len(uncL) else None,
                "confidence": float(conL[i])  if i < len(conL) else None,
                "pixel_km2":  float(areL[i])  if i < len(areL) else None,
                "time_raw":   float(timL[i])  if i < len(timL) else None,
            })
        return rows

    def _by_compound(f: h5py.File) -> List[Dict[str, Any]]:
        """
        Soporte para datasets COMPUESTOS (dtype.names). Busca cualquier dataset 1D con campos.
        Intenta mapear campos por nombre a: lat/lon/frp/confidence/area/time.
        """
        rows_all: List[Dict[str, Any]] = []

        def try_one(ds: h5py.Dataset):
            # Solo 1D y con dtype compuesto
            if not isinstance(ds, h5py.Dataset) or ds.ndim != 1 or not getattr(ds.dtype, "names", None):
                return []

            names = [n.lower() for n in ds.dtype.names]  # p. ej. ('LAT','LON','FRP',...)
            def pick(keys: List[str]) -> Optional[str]:
                for k in keys:
                    for n in names:
                        if k in n:
                            return n
                return None

            k_lat = pick(["lat"])            # 'lat', 'latitude'
            k_lon = pick(["lon", "long"])    # 'lon', 'long', 'longitude'
            k_frp = pick(["frp"])            # 'frp', 'frp_mw'
            k_con = pick(["conf"])           # 'conf','confidence'
            k_are = pick(["area"])           # 'area','pixel'
            k_tim = pick(["time","tstamp"])  # 'time','timestamp'

            # Si no hay al menos lat/lon o frp, saltamos
            if not (k_lat or k_lon or k_frp):
                return []

            out: List[Dict[str, Any]] = []
            data = ds[:]  # array de registros
            for rec in data:
                def getf(k):
                    if not k: return None
                    try:
                        v = rec[k]
                        # v puede ser numpy scalar -> cast a float si posible
                        return float(v) if v is not None else None
                    except Exception:
                        return None

                out.append({
                    "lat":        getf(k_lat),
                    "lon":        getf(k_lon),
                    "frp_mw":     getf(k_frp),
                    "frp_unc_mw": None,
                    "confidence": getf(k_con),
                    "pixel_km2":  getf(k_are),
                    "time_raw":   getf(k_tim),
                })
            return out

        f.visititems(lambda name, obj: rows_all.extend(try_one(obj)))
        return rows_all

    def _by_flex(f: h5py.File) -> List[Dict[str, Any]]:
        """Heurístico sin numpy sobre datasets 1D numéricos (como ya te monté)."""
        # Candidatos 1D numéricos
        cands: List[Tuple[str, List[float]]] = []

        def _is_numeric_dtype(d: Any) -> bool:
            s = str(d)
            return any(k in s for k in ("int", "float", "i1", "i2", "i4", "i8", "f4", "f8"))

        def _visitor(name, obj):
            if isinstance(obj, h5py.Dataset) and obj.ndim == 1 and obj.size > 0 and _is_numeric_dtype(obj.dtype):
                try:
                    m = min(obj.size, 500000)
                    vals = [float(v) for v in obj[:m]]
                    cands.append((name, vals))
                except Exception:
                    pass
        f.visititems(_visitor)
        if not cands:
            return []

        # Agrupa por longitud (preferimos la mayor)
        groups: Dict[int, List[Tuple[str, List[float]]]] = {}
        for name, arr in cands:
            groups.setdefault(len(arr), []).append((name, arr))
        length = max(groups.keys())
        group = groups[length]

        def frac_in_range(a: List[float], lo: float, hi: float) -> float:
            tot = ok = 0
            for v in a:
                if v == v:
                    tot += 1
                    if lo <= v <= hi: ok += 1
            return (ok / tot) if tot else 0.0

        def some_positive(a: List[float]) -> bool:
            return any((v == v and v > 0) for v in a)

        def nonneg_fraction(a: List[float]) -> float:
            tot = ok = 0
            for v in a:
                if v == v:
                    tot += 1
                    if v >= 0: ok += 1
            return (ok / tot) if tot else 0.0

        def name_score(n: str, keys: List[str]) -> float:
            nlow = n.lower();  return 1.0 if any(k in nlow for k in keys) else 0.0

        # Selección lat/lon
        best_lat = (-1.0, -1); best_lon = (-1.0, -1)
        for i, (n, a) in enumerate(group):
            sc = frac_in_range(a, -90, 90) + 0.1 * name_score(n, ["lat"])
            if sc > best_lat[0]: best_lat = (sc, i)
        for i, (n, a) in enumerate(group):
            sc = frac_in_range(a, -180, 180) + 0.1 * name_score(n, ["lon"])
            if sc > best_lon[0]: best_lon = (sc, i)

        lat_arr = group[best_lat[1]][1] if best_lat[1] >= 0 else None
        lon_arr = group[best_lon[1]][1] if best_lon[1] >= 0 else None

        # FRP
        best_frp = (-1.0, -1)
        for i, (n, a) in enumerate(group):
            if some_positive(a):
                sc = nonneg_fraction(a) + 0.05 * name_score(n, ["frp"])
                if sc > best_frp[0]: best_frp = (sc, i)
        frp_arr = group[best_frp[1]][1] if best_frp[1] >= 0 else None

        # time (monotonía parcial)
        def inc_score(a: List[float], k: int = 2048) -> float:
            k = min(k, len(a) - 1) if len(a) > 1 else 0
            if k <= 0: return 0.0
            inc = 0; prev = a[0]
            for i in range(1, k + 1):
                cur = a[i]
                if cur == cur and prev == prev and cur >= prev: inc += 1
                prev = cur
            return float(inc)

        best_t = (-1.0, -1)
        for i, (n, a) in enumerate(group):
            sc = inc_score(a) + 0.05 * name_score(n, ["time", "tstamp", "date"])
            if sc > best_t[0]: best_t = (sc, i)
        time_arr = group[best_t[1]][1] if best_t[1] >= 0 else None

        if lat_arr is None and lon_arr is None and frp_arr is None:
            return []

        rows: List[Dict[str, Any]] = []
        for i in range(length):
            rows.append({
                "lat":        float(lat_arr[i]) if lat_arr is not None else None,
                "lon":        float(lon_arr[i]) if lon_arr is not None else None,
                "frp_mw":     float(frp_arr[i]) if frp_arr is not None else None,
                "frp_unc_mw": None,
                "confidence": None,
                "pixel_km2":  None,
                "time_raw":   float(time_arr[i]) if (time_arr is not None and i < len(time_arr)) else None,
            })
        return rows

    # --- ejecución ---
    with h5py.File(io.BytesIO(h5bytes), "r") as f:
        rows = _by_regex(f)
        if rows:
            return {"rows": rows}
        rows = _by_compound(f)
        if rows:
            return {"rows": rows}
        rows = _by_flex(f)
        return {"rows": rows}



def _h5_datasets_summary(h5bytes: bytes, sample: int = 3) -> List[Dict[str, Any]]:
    """
    Devuelve un resumen de datasets del HDF5: path, shape, dtype y muestra de valores (si 1D).
    Útil para diagnóstico (/by-ts?debug=1).
    """
    out: List[Dict[str, Any]] = []
    with h5py.File(io.BytesIO(h5bytes), "r") as f:
        def _visitor(name, obj):
            if isinstance(obj, h5py.Dataset):
                info = {
                    "path": name,
                    "shape": tuple(obj.shape),
                    "dtype": str(obj.dtype),
                }
                try:
                    if obj.ndim == 1 and obj.size > 0:
                        m = min(sample, obj.size)
                        info["sample"] = [float(v) for v in obj[:m]]
                except Exception:
                    pass
                out.append(info)
        f.visititems(_visitor)
    return out


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

    snap: Dict[str, Any] = {
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
    return _current or _previous


async def reload_latest_async() -> Dict[str, Any]:
    """
    Descarga/parsea/filtra el último slot candidato y conmuta el buffer si es válido.
    Reglas:
      - Monotonía: no reemplazar por slots más antiguos.
      - Idempotencia: si sha256 coincide, no conmuta.
      - Sanidad: no aceptar vacíos si antes había datos (opcional).
    """
    global _current, _previous  # <-- IMPORTANTE: antes de usar

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

                # Monotonía: no retroceder en el tiempo
                if _current and new_snap["slot_ts"] < _current["slot_ts"]:
                    continue

                # Idempotencia
                if _current and new_snap["sha256"] == _current.get("sha256"):
                    return {
                        "ok": True,
                        "reason": "not_changed",
                        "slot_ts": new_snap["slot_ts"],
                        "count": new_snap["count"],
                    }

                # Sanidad (opcional): evita vacíos si antes había filas
                if _current and new_snap["count"] == 0 and _current["count"] > 0:
                    last_err = "candidate_empty_rejected"
                    continue

                # Conmutación atómica
                _previous = _current
                _current = new_snap

                return {
                    "ok": True,
                    "reason": "swapped",
                    "slot_ts": new_snap["slot_ts"],
                    "count": new_snap["count"],
                    "url": url,
                }

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
    debug: bool = False,
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

    resp: Dict[str, Any] = {
        "ok": True,
        "slot_ts": _slot_iso_from_ts(ts),
        "downloaded_at": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bbox_used": list(bx),
        "count": len(rows),
        "source_url": url,
        "sha256": _sha256(h5),
        "rows": rows,
    }

    if debug:
        resp["datasets"] = _h5_datasets_summary(h5, sample=3)

    return resp


async def startup_warmup():
    """
    Intenta una primera ingesta de arranque en background. No levanta excepción.
    """
    try:
        await reload_latest_async()
    except Exception:
        # Silencioso: el cron del panel reintenta en minutos
        pass

