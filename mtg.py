# mtg.py — MTG FRP-Pixel (LSA SAF LSA-509, Meteosat-12 FCI) → JSON, aparte del producto MSG de ingest.py
# -----------------------------------------------------------------------------
# Qué hace:
#  - Descarga el NetCDF del FRP-Pixel de MTG (1 km, cada 10 min) del mismo servidor y con la misma cuenta que MSG:
#      {LSA_HOST}/PRODUCTS/MTG/MTFRPPixel/NATIVE/AAAA/MM/DD/LSA-509_MTG_MTFRPPIXEL_MTG-FD_AAAAMMDDHHMM.nc
#    (~2 MB; publicado ~18-25 min después del inicio del slot). Producto en DEMOSTRACIÓN en LSA SAF desde oct-2025:
#    su formato puede cambiar → /mtg-frp/esquema enseña los grupos y variables de un fichero real.
#  - El NetCDF-4 es un HDF5: se lee con h5py. Las detecciones están en el grupo «ListProduct» (LATITUDE, LONGITUDE,
#    FRP, FIRE_CONFIDENCE en %, ABS_LINE, ABS_SAMP…, como el lector de satpy); el grupo «QualityProduct» (disco
#    completo) no se toca. Lectura CF propia (_leer_cf): atributos de 1 elemento (netCDF-C los guarda con forma (1,)),
#    scale_factor/add_offset, _FillValue/missing_value, valid_range/min/max, _Unsigned y NaN. No usa _read_scaled de
#    MSG, que se queda como está.
#  - Filas con las MISMAS claves básicas que MSG (latitude, longitude, frp_mw, confidence 0–1, acq_time_utc…).
#    acq_time_utc = inicio del slot (el fichero no trae hora por píxel; FCI barre de sur a norte y la Península se ve
#    unos minutos después del inicio).
#  - Se recorta al bbox con numpy ANTES de crear las filas: el disco completo puede traer decenas de miles de píxeles.
#  - Doble buffer propio (current/previous) recargado por un bucle interno (no depende del cron de MSG). Un fallo de MTG
#    nunca toca el buffer de MSG ni /reload. La cuenta de IPMA es la misma que la de MSG: si IPMA rechaza la cuenta
#    (401/403) el bucle espera cada vez más (hasta MTG_BACKOFF_MAX_S) y un fichero que no se pudo leer no se vuelve a
#    bajar. MTG_ENABLED=0 lo apaga sin tocar código.
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import datetime as dt
import io
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import h5py  # type: ignore
import numpy as np  # type: ignore
import requests  # type: ignore

from ingest import (
    DEFAULT_IBERIA_BBOX,
    HTTP_TIMEOUT,
    LSA_HOST,
    LSASAF_PASS,
    LSASAF_USER,
    _describe_error,
    _iso,
    _looks_like_hdf5,
    _sha256,
    _utcnow,
)


def _env_int(nombre: str, defecto: int, minimo: int) -> int:
    """Entero de una variable de entorno; si está mal escrita, el valor por defecto (no tumba el arranque de MSG)."""
    try:
        return max(minimo, int(os.getenv(nombre, str(defecto))))
    except ValueError:
        print(f"[MTG] {nombre} no es un entero: uso {defecto}", flush=True)
        return defecto


# ----------------------------- Configuración -------------------------------

MTG_ENABLED = os.getenv("MTG_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
MTG_PATH = os.getenv("MTG_PATH", "/PRODUCTS/MTG/MTFRPPixel/NATIVE")
MTG_PREFIX = os.getenv("MTG_PREFIX", "LSA-509_MTG_MTFRPPIXEL_MTG-FD_")
MTG_GROUP = os.getenv("MTG_GROUP", "ListProduct")
MTG_LAG_MIN = _env_int("MTG_LAG_MIN", 18, 0)               # margen para que el slot esté publicado
MTG_FALLBACKS = _env_int("MTG_FALLBACKS", 6, 0)            # t, t-10, … (6 ⇒ 1 h hacia atrás)
MTG_STALE_MIN = _env_int("MTG_STALE_MIN", 60, 10)          # normal: 18–35 min desde el inicio del slot
MTG_RELOAD_EVERY_S = _env_int("MTG_RELOAD_EVERY_S", 300, 60)
MTG_RELOAD_BUDGET_S = _env_int("MTG_RELOAD_BUDGET_S", 120, 10)
MTG_BACKOFF_MAX_S = _env_int("MTG_BACKOFF_MAX_S", 3600, 60)

_MTG_VARS = {                                              # variable → nombres aceptados (sin distinguir mayúsculas)
    "lat": ["LATITUDE", "lat"],
    "lon": ["LONGITUDE", "lon"],
    "frp": ["FRP", "FRP_MW"],
    "conf": ["FIRE_CONFIDENCE", "CONFIDENCE"],
    "unc": ["FRP_UNCERTAINTY", "FRP_UNC"],
    "area": ["PIXEL_SIZE", "PIXEL_AREA"],
    "line": ["ABS_LINE"],
    "samp": ["ABS_SAMP"],
}
_CLAVES_FILA = {"lat": "latitude", "lon": "longitude", "frp": "frp_mw", "conf": "confidence", "unc": "frp_unc_mw",
                "area": "pixel_km2", "line": "abs_line", "samp": "abs_samp"}

# ---------------------------- Estado en memoria ---------------------------

_lock = asyncio.Lock()
_bajo_demanda = asyncio.Semaphore(1)                       # /by-ts y /esquema: de uno en uno (2 MB y CPU cada uno)
_current: Optional[Dict[str, Any]] = None
_previous: Optional[Dict[str, Any]] = None
_last_reload: Optional[Dict[str, Any]] = None
_ts_ilegibles: Dict[str, str] = {}                         # ts → motivo: ficheros que no se pudieron leer (no se rebajan)


class CuentaRechazada(Exception):
    """IPMA rechaza la cuenta (401/403): esperar antes de volver a intentarlo."""


class NoEsHdf5(Exception):
    """IPMA devolvió algo que no es el fichero (p. ej. una página de mantenimiento): pasajero, se reintenta."""


# ----------------------------- Utilidades ---------------------------------


def _floor_to_10(t: dt.datetime) -> dt.datetime:
    return t.replace(minute=(t.minute // 10) * 10, second=0, microsecond=0)


def candidate_ts_list(now_utc: Optional[dt.datetime] = None) -> List[str]:
    """[t, t-10, …] en YYYYMMDDHHMM: el último slot publicable con MTG_LAG_MIN y los anteriores."""
    base = _floor_to_10((now_utc or _utcnow()) - dt.timedelta(minutes=MTG_LAG_MIN))
    return [(base - dt.timedelta(minutes=10 * i)).strftime("%Y%m%d%H%M") for i in range(MTG_FALLBACKS + 1)]


def build_url(ts: str) -> str:
    return f"{LSA_HOST}{MTG_PATH}/{ts[:4]}/{ts[4:6]}/{ts[6:8]}/{MTG_PREFIX}{ts}.nc"


def slot_iso(ts: str) -> str:
    return _iso(dt.datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=dt.timezone.utc))


def valid_ts(ts: str) -> bool:
    if len(ts) != 12 or not ts.isascii() or not ts.isdigit():
        return False
    try:
        d = dt.datetime.strptime(ts, "%Y%m%d%H%M")
    except ValueError:
        return False
    return d.minute % 10 == 0


def _bajar(url: str) -> Tuple[bytes, str]:
    """(contenido, content-type). 401/403 → CuentaRechazada; otros HTTP ≥ 400 → requests.HTTPError."""
    auth = (LSASAF_USER, LSASAF_PASS) if LSASAF_USER or LSASAF_PASS else None
    r = requests.get(url, timeout=HTTP_TIMEOUT, auth=auth)
    if r.status_code in (401, 403):
        raise CuentaRechazada(f"HTTP {r.status_code}")
    r.raise_for_status()
    return r.content, r.headers.get("Content-Type", "?")


def _bajar_hdf5(ts: str) -> bytes:
    b, ctype = _bajar(build_url(ts))
    if not _looks_like_hdf5(b):
        raise NoEsHdf5(f"respuesta_no_hdf5 ({ctype}, {len(b)} bytes, empieza por {b[:8]!r})")
    return b


# ----------------------------- Lectura del NetCDF -------------------------


def _attr(d: h5py.Dataset, nombre: str) -> Any:
    """Atributo como escalar (netCDF-C guarda los numéricos con forma (1,)); None si no está o no se puede leer."""
    try:
        if nombre not in d.attrs:
            return None
        v = d.attrs[nombre]
    except Exception:
        return None
    if isinstance(v, bytes):
        return v.decode(errors="replace")
    a = np.asarray(v)
    if a.size == 0:
        return None
    if a.size == 1:
        x = a.reshape(-1)[0].item()
        return x.decode(errors="replace") if isinstance(x, bytes) else x
    return a.reshape(-1)


def _leer_cf(d: h5py.Dataset) -> np.ndarray:
    """Valores físicos en float64 con NaN donde no hay dato (CF: _Unsigned, _FillValue, missing_value, valid_range/
    valid_min/valid_max sobre el valor bruto; luego scale_factor y add_offset)."""
    raw = np.asarray(d[()]).reshape(-1)
    malos = np.zeros(raw.shape, bool)
    # el relleno se compara en el tipo guardado (con _Unsigned, netCDF-C lo guarda con signo)
    for n in ("_FillValue", "missing_value"):
        v = _attr(d, n)
        if v is not None and not isinstance(v, str):
            malos |= np.isin(raw, np.asarray(v).reshape(-1).astype(raw.dtype))
    vr = _attr(d, "valid_range")
    vmin, vmax = (vr[0], vr[1]) if isinstance(vr, np.ndarray) and vr.size == 2 else (_attr(d, "valid_min"), _attr(d, "valid_max"))
    if str(_attr(d, "_Unsigned") or "").lower() == "true" and raw.dtype.kind == "i":
        sin_signo = raw.dtype.str.replace("i", "u")
        a_sin_signo = lambda x: None if x is None or isinstance(x, str) else \
            np.asarray(x).astype(raw.dtype).view(sin_signo).item()
        raw, vmin, vmax = raw.view(sin_signo), a_sin_signo(vmin), a_sin_signo(vmax)
    if vmin is not None and not isinstance(vmin, str):
        malos |= raw < vmin
    if vmax is not None and not isinstance(vmax, str):
        malos |= raw > vmax
    x = raw.astype(np.float64)
    sf, ao = _attr(d, "scale_factor"), _attr(d, "add_offset")
    if isinstance(sf, (int, float)):
        x = x * float(sf)
    if isinstance(ao, (int, float)):
        x = x + float(ao)
    x[malos | ~np.isfinite(x)] = np.nan
    return x


def _grupo(f: h5py.File) -> h5py.Group:
    for k in f.keys():
        if k.lower() == MTG_GROUP.lower() and isinstance(f[k], h5py.Group):
            return f[k]
    return f            # por si un día las variables van en la raíz


def leer_arrays(b: bytes) -> Dict[str, np.ndarray]:
    """Arrays (float64, NaN = sin dato) del ListProduct. ValueError si faltan LATITUDE/LONGITUDE/FRP."""
    with h5py.File(io.BytesIO(b), "r") as f:
        g = _grupo(f)
        nombres = {k.lower(): k for k in g.keys() if isinstance(g[k], h5py.Dataset)}
        out: Dict[str, np.ndarray] = {}
        for clave, opciones in _MTG_VARS.items():
            for o in opciones:
                if o.lower() in nombres:
                    out[clave] = _leer_cf(g[nombres[o.lower()]])
                    break
        if not all(k in out for k in ("lat", "lon", "frp")):
            raise ValueError(f"variables clave no encontradas en {g.name}: {sorted(g.keys())[:20]}")
    n = min(out["lat"].size, out["lon"].size, out["frp"].size)
    out = {k: (v[:n] if v.size >= n else np.concatenate([v, np.full(n - v.size, np.nan)])) for k, v in out.items()}
    if "conf" in out:                                      # MTG la da en % (MSG en 0–1): se deja en 0–1
        c = out["conf"]
        if np.isfinite(c).any() and np.nanmax(c) > 1.0:
            out["conf"] = c / 100.0
    return out


def _sanidad(a: Dict[str, np.ndarray]) -> Tuple[bool, str]:
    """Como _sanity_checks de MSG, sobre el disco completo: ≥ 1 píxel con lat/lon/frp y FRP no constante."""
    ok = np.isfinite(a["lat"]) & np.isfinite(a["lon"]) & np.isfinite(a["frp"])
    if not ok.any():
        return False, "sin_filas"
    frp = np.round(a["frp"][ok], 3)
    if frp.size >= 3 and np.unique(frp).size == 1:
        return False, "frp_constante"
    return True, "ok"


def filas(a: Dict[str, np.ndarray], bbox: Tuple[float, float, float, float], iso: str,
          min_frp: Optional[float] = None, min_conf: Optional[float] = None) -> List[Dict[str, Any]]:
    """Recorta con numpy (bbox y umbrales) y solo entonces crea las filas."""
    w, s, e, n = bbox
    m = (a["lon"] >= w) & (a["lon"] <= e) & (a["lat"] >= s) & (a["lat"] <= n)
    if min_frp is not None:
        m &= a["frp"] >= float(min_frp)
    if min_conf is not None:
        thr = float(min_conf) / (100.0 if float(min_conf) > 1.0 else 1.0)
        m &= a.get("conf", np.full(m.shape, np.nan)) >= thr
    idx = np.nonzero(m)[0]
    cols = {_CLAVES_FILA[k]: [None if np.isnan(x) else float(x) for x in v[idx].tolist()] for k, v in a.items()}
    return [dict({k: cols[k][i] for k in cols}, acq_time_utc=iso) for i in range(idx.size)]


def esquema(b: bytes, ctype: str) -> Dict[str, Any]:
    """Grupos y variables (nombre, forma, tipo, atributos) sin leer los datos; si no es HDF5, la firma y el tipo."""
    if not _looks_like_hdf5(b):
        return {"hdf5": False, "content_type": ctype, "bytes": len(b), "empieza_por": repr(b[:16])}

    def attrs(o: Any) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for k in list(o.attrs.keys())[:25]:
            if k in ("DIMENSION_LIST", "REFERENCE_LIST"):
                continue
            try:
                v = o.attrs[k]
                s = v.decode(errors="replace") if isinstance(v, bytes) else str(v)
            except Exception as ex:
                s = f"<no legible: {type(ex).__name__}>"
            out[k] = s[:200]
        return out

    with h5py.File(io.BytesIO(b), "r") as f:
        items: List[Dict[str, Any]] = []

        def visitar(nombre: str, o: Any) -> None:
            if len(items) >= 200:
                return
            if isinstance(o, h5py.Dataset):
                items.append({"ruta": nombre, "forma": list(o.shape), "tipo": str(o.dtype), "attrs": attrs(o)})
            else:
                items.append({"ruta": nombre, "grupo": True, "attrs": attrs(o)})

        f.visititems(visitar)
        return {"hdf5": True, "bytes": len(b), "raiz": attrs(f), "objetos": items}


def _snapshot(ts: str, b: bytes) -> Dict[str, Any]:
    a = leer_arrays(b)
    ok, reason = _sanidad(a)
    iso = slot_iso(ts)
    rows = filas(a, DEFAULT_IBERIA_BBOX, iso)
    return {"ok": ok, "reason": None if ok else reason, "slot_ts": iso, "downloaded_at": _iso(_utcnow()),
            "rows": rows, "count": len(rows), "count_disco": int(a["frp"].size), "sha256": _sha256(b)}


def _fetch(ts: str) -> Dict[str, Any]:
    return _snapshot(ts, _bajar_hdf5(ts))


# ----------------------------- Frescura -----------------------------------


def age_min(snap: Optional[Dict[str, Any]], now_utc: Optional[dt.datetime] = None) -> Optional[int]:
    if not snap or not snap.get("slot_ts"):
        return None
    t = dt.datetime.strptime(snap["slot_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    return int(((now_utc or _utcnow()) - t).total_seconds() // 60)


def is_stale(age: Optional[int]) -> bool:
    return age is None or age > MTG_STALE_MIN


# -------------------------- API para FastAPI ------------------------------


def _vetar(ts: str, motivo: str) -> None:
    _ts_ilegibles[ts] = motivo[:200]
    if len(_ts_ilegibles) > 500:
        _ts_ilegibles.pop(next(iter(_ts_ilegibles)))


def get_snapshot() -> Optional[Dict[str, Any]]:
    return _current or _previous


def get_last_reload() -> Optional[Dict[str, Any]]:
    return _last_reload


async def reload_latest() -> Dict[str, Any]:
    """Como reload_latest_async de MSG: solo slots más nuevos que el publicado, salta los que fallan, idempotente por
    sha256; ok = el dato servido es fresco (edad ≤ MTG_STALE_MIN). Un fichero ilegible no se vuelve a bajar; si IPMA
    rechaza la cuenta se para la vuelta (cuenta_rechazada=True: el bucle espera más)."""
    global _current, _previous, _last_reload
    async with _lock:
        started = time.monotonic()
        errores: List[str] = []
        result: Optional[Dict[str, Any]] = None
        rechazada = False
        intentos = sin_publicar = 0
        cur = _current["slot_ts"] if _current else None
        for ts in candidate_ts_list():
            if cur and slot_iso(ts) <= cur:
                break
            if ts in _ts_ilegibles:
                continue
            if time.monotonic() - started > MTG_RELOAD_BUDGET_S:
                errores.append(f"presupuesto_agotado ({MTG_RELOAD_BUDGET_S}s)")
                break
            intentos += 1
            try:
                snap = await asyncio.to_thread(_fetch, ts)
            except CuentaRechazada as e:
                errores.append(f"{ts}: cuenta_rechazada ({e})")
                rechazada = True
                break
            except (requests.RequestException, NoEsHdf5) as e:     # red, HTTP (404 = aún no publicado), página
                desc = _describe_error(e) if isinstance(e, requests.RequestException) else str(e)
                sin_publicar += desc == "HTTP 404"
                errores.append(f"{ts}: {desc}")
                continue
            except Exception as e:                                  # fichero ilegible (h5py, formato): no se rebaja
                _vetar(ts, f"{type(e).__name__}: {e}")
                errores.append(f"{ts}: ilegible ({_ts_ilegibles[ts]})")
                continue
            if not snap["ok"]:
                _vetar(ts, f"slot_invalido ({snap['reason']})")
                errores.append(f"{ts}: slot_invalido ({snap['reason']})")
                continue
            if _current and snap["sha256"] == _current.get("sha256"):
                result = {"status": "noop_idempotente"}
                break
            _previous, _current = _current, snap
            result = {"status": "publicado"}
            break
        snap = _current or _previous
        age = age_min(snap)
        stale = is_stale(age)
        if result is None:
            result = {"status": "sin_datos" if snap is None else ("datos_desfasados" if stale else "noop_monotonia")}
        result.update({"ok": not stale, "slot_ts": snap["slot_ts"] if snap else None,
                       "count": snap["count"] if snap else 0, "age_min": age, "stale": stale})
        if rechazada:
            result["cuenta_rechazada"] = True
        if stale and intentos and sin_publicar == intentos:        # todo 404 y el dato viejo: ruta o producto cambiados
            result["todo_404"] = True
        if errores:
            result["errores"] = errores
        _last_reload = {"at": _iso(_utcnow()), **result}
        return result


async def by_ts(ts: str, bbox: Optional[Tuple[float, float, float, float]] = None,
                min_frp: Optional[float] = None, min_conf: Optional[float] = None) -> Dict[str, Any]:
    """Un slot concreto bajo demanda (no toca el buffer). Hay datos desde el 1-ene-2025. De uno en uno."""
    bbox = bbox or DEFAULT_IBERIA_BBOX
    iso = slot_iso(ts)

    def trabajo() -> Dict[str, Any]:
        try:
            b = _bajar_hdf5(ts)
        except Exception as e:
            return {"ok": False, "status": "descarga_fallida", "error": _describe_error(e)}
        try:
            a = leer_arrays(b)
        except Exception as e:
            return {"ok": False, "status": "parse_fallido", "error": f"{type(e).__name__}: {e}"[:300]}
        rows = filas(a, bbox, iso, min_frp=min_frp, min_conf=min_conf)
        return {"ok": True, "satellite": "MTG", "slot_ts": iso, "downloaded_at": _iso(_utcnow()), "count": len(rows),
                "count_disco": int(a["frp"].size), "bbox": {"w": bbox[0], "s": bbox[1], "e": bbox[2], "n": bbox[3]},
                "rows": rows}

    async with _bajo_demanda:
        return await asyncio.to_thread(trabajo)


async def esquema_ts(ts: str) -> Dict[str, Any]:
    def trabajo() -> Dict[str, Any]:
        try:
            b, ctype = _bajar(build_url(ts))
        except Exception as e:
            return {"ok": False, "status": "descarga_fallida", "error": _describe_error(e)}
        try:
            return {"ok": True, "slot_ts": slot_iso(ts), **esquema(b, ctype)}
        except Exception as e:
            return {"ok": False, "status": "esquema_fallido", "error": f"{type(e).__name__}: {e}"[:300]}

    async with _bajo_demanda:
        return await asyncio.to_thread(trabajo)


async def reload_loop() -> None:
    """Recarga MTG cada MTG_RELOAD_EVERY_S. Si IPMA rechaza la cuenta, o todo da 404 con el dato ya viejo, espera el
    doble cada vez (hasta MTG_BACKOFF_MAX_S) para no arriesgar la cuenta que comparte con MSG. Nunca termina por un
    error."""
    if not MTG_ENABLED:
        print("[MTG] desactivado (MTG_ENABLED=0)", flush=True)
        return
    espera = MTG_RELOAD_EVERY_S
    while True:
        try:
            r = await reload_latest()
            if r.get("status") == "publicado" or not r.get("ok"):
                print(f"[MTG] {r.get('status')} slot={r.get('slot_ts')} count={r.get('count')} "
                      f"errores={r.get('errores', [])}", flush=True)
            mal = r.get("cuenta_rechazada") or r.get("todo_404")
            espera = min(MTG_BACKOFF_MAX_S, espera * 2) if mal else MTG_RELOAD_EVERY_S
        except Exception as e:
            print(f"[MTG] recarga fallida: {type(e).__name__}: {e}", flush=True)
            espera = MTG_RELOAD_EVERY_S
        await asyncio.sleep(espera)
