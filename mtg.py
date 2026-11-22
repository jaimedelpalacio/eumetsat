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
#  - Histórico anterior a ~mayo de 2026: IPMA solo tiene el ListProduct en CSV comprimido
#      …/LSA-509_MTG_MTFRPPIXEL-ListProduct_MTG-FD_AAAAMMDDHHMM.csv.gz
#    (el .nc de esas fechas es solo el QualityProduct, con otro nombre). /mtg-frp/by-ts lo usa si el .nc da 404
#    (fuente=auto) o si se pide (fuente=csv). La cabecera se busca por nombre de columna, como las variables del .nc;
#    si no se reconoce, el error enseña la cabecera. El bucle de recarga sigue leyendo solo el .nc.
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import gzip
import io
import os
import time
import zlib
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
MTG_PREFIX_CSV = os.getenv("MTG_PREFIX_CSV", "LSA-509_MTG_MTFRPPIXEL-ListProduct_MTG-FD_")
FUENTES = ("auto", "nc", "csv")                            # /mtg-frp/by-ts y /esquema: qué fichero de IPMA leer
def _env_ts(nombre: str, defecto: str) -> str:
    """AAAAMMDDHHMM de una variable de entorno; si está mal escrita, el valor por defecto (se comparan como texto)."""
    v = os.getenv(nombre, defecto).strip()
    if len(v) == 12 and v.isascii() and v.isdigit():
        try:
            dt.datetime.strptime(v, "%Y%m%d%H%M")
            return v
        except ValueError:
            pass
    print(f"[MTG] {nombre}={v!r} no es AAAAMMDDHHMM: uso {defecto}", flush=True)
    return defecto


MTG_DESDE = _env_ts("MTG_DESDE", "202501010000")           # primer slot del archivo de IPMA (antes: 404 sin pedir)
MTG_CSV_HASTA = _env_ts("MTG_CSV_HASTA", "202607010000")   # fuente=auto prueba el CSV solo para slots anteriores
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


def build_url_csv(ts: str) -> str:
    return f"{LSA_HOST}{MTG_PATH}/{ts[:4]}/{ts[4:6]}/{ts[6:8]}/{MTG_PREFIX_CSV}{ts}.csv.gz"


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
    return _conf_a_01(out)


def _conf_a_01(out: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """MTG da la confianza en % (MSG en 0–1): se deja en 0–1."""
    if "conf" in out:
        c = out["conf"]
        if np.isfinite(c).any() and np.nanmax(c) > 1.0:
            out["conf"] = c / 100.0
    return out


def _num(s: str) -> float:
    try:
        x = float(s.strip().strip('"'))
    except ValueError:
        return np.nan
    return x if np.isfinite(x) else np.nan


def texto_csv(b: bytes) -> str:
    """Texto del CSV: se descomprime solo si empieza por la firma gzip (si IPMA lo sirve con Content-Encoding: gzip,
    requests ya lo ha descomprimido)."""
    if b[:2] == b"\x1f\x8b":
        try:
            b = gzip.decompress(b)
        except (OSError, EOFError, zlib.error) as e:
            raise ValueError(f"csv.gz ilegible: {type(e).__name__}: {e}")
    return b.decode("utf-8-sig", errors="replace")


def _cabecera(linea: str) -> Optional[Tuple[Optional[str], List[str]]]:
    """(separador, columnas en minúsculas) si la línea nombra latitud y longitud; si no, None."""
    l2 = linea.lstrip("#").strip()
    if not l2:
        return None
    delim: Optional[str] = max((",", ";", "\t"), key=l2.count)
    delim = delim if l2.count(delim) else None
    trozos = next(csv.reader([l2], delimiter=delim)) if delim else l2.split()
    cols = [c.strip().strip('"').lower() for c in trozos]
    if delim and len(cols) > 1 and cols[-1] == "":              # separador al final de la cabecera
        cols = cols[:-1]
    if any(o.lower() in cols for o in _MTG_VARS["lat"]) and any(o.lower() in cols for o in _MTG_VARS["lon"]):
        return delim, cols
    return None


def _parece_datos(linea: str) -> bool:
    """Línea sin «#» con al menos 2 campos numéricos (para no saltarse datos antes de la cabecera)."""
    if not linea.strip() or linea.lstrip().startswith("#"):
        return False
    return sum(np.isfinite(_num(t)) for t in linea.replace(";", ",").replace("\t", ",").replace(" ", ",").split(",")) >= 2


def leer_arrays_csv(b: bytes) -> Dict[str, np.ndarray]:
    """Como leer_arrays, pero del ListProduct en CSV comprimido (histórico anterior a ~mayo de 2026); el formato real no
    se ha podido ver, así que todo lo dudoso FALLA con el texto delante en vez de dar datos erróneos:
      - cabecera: la primera línea SIN «#» que nombra latitud y longitud (si no hay, una con «#»); separador «,», «;»,
        tabulador o espacios; columnas por nombre, como las variables del .nc;
      - si antes de la cabecera hay líneas que parecen datos, falla (la cabecera elegida no es la buena);
      - la mayoría de las filas debe tener tantos campos como la cabecera (si no, columnas desplazadas); la fila suelta
        que no cuadra se descarta entera (todo NaN), nunca se lee por posición;
      - la mayoría debe tener lat, lon y FRP válidos (si no, unidades o formato numérico distintos: enteros escalados,
        coma decimal…). Un campo vacío o no numérico, un FRP negativo o una lat/lon fuera de rango → NaN;
      - escalas raras: FRP > 100 000 MW, ≥ 20 FRP todos enteros, confianza fuera de 0–1 tras pasar de % o confianza
        en clases (≥ 20 filas con ≤ 3 valores enteros) → falla.
    Un CSV con solo la cabecera es válido (0 detecciones)."""
    lineas = texto_csv(b).splitlines()
    cab = None
    for con_almohadilla in (False, True):
        for i, linea in enumerate(lineas[:50]):
            if linea.lstrip().startswith("#") != con_almohadilla:
                continue
            c = _cabecera(linea)
            if c:
                cab = (i, *c)
                break
        if cab:
            break
    if cab is None:
        raise ValueError(f"cabecera CSV no reconocida; empieza por {lineas[:2]!r}"[:300])
    i, delim, cols = cab
    if any(_parece_datos(l) for l in lineas[:i]):
        raise ValueError(f"hay datos antes de la cabecera elegida (línea {i + 1}); empieza por {lineas[:2]!r}"[:400])
    resto = [l for l in lineas[i + 1:] if l.strip() and not l.lstrip().startswith("#")]
    filas_csv = list(csv.reader(resto, delimiter=delim)) if delim else [l.split() for l in resto]
    muestra = f"cabecera {cols[:12]}, 1.ª fila {filas_csv[0][:12] if filas_csv else None}"
    if filas_csv:
        cuadra = lambda f: len(f) == len(cols) or (len(f) == len(cols) + 1 and not f[-1].strip())
        cuadran = sum(map(cuadra, filas_csv))
        if cuadran < 0.5 * len(filas_csv):
            raise ValueError(f"las filas no tienen tantos campos como la cabecera ({cuadran}/{len(filas_csv)}); {muestra}"[:400])
        filas_csv = [f if cuadra(f) else [] for f in filas_csv]     # la que no cuadra: sin dato, nunca por posición
    out: Dict[str, np.ndarray] = {}
    for clave, opciones in _MTG_VARS.items():
        for o in opciones:
            if o.lower() in cols:
                j = cols.index(o.lower())
                out[clave] = np.array([_num(f[j]) if j < len(f) else np.nan for f in filas_csv], dtype=np.float64)
                break
    if not all(k in out for k in ("lat", "lon", "frp")):
        raise ValueError(f"columnas clave no encontradas en el CSV: {cols[:20]}"[:300])
    out["lat"][np.abs(out["lat"]) > 90] = np.nan
    out["lon"][np.abs(out["lon"]) > 180] = np.nan
    for k in ("frp", "unc", "area"):                           # relleno negativo (-1, -999…) → sin dato
        if k in out:
            out[k][out[k] < 0] = np.nan
    if filas_csv:
        buenas = int((np.isfinite(out["lat"]) & np.isfinite(out["lon"]) & np.isfinite(out["frp"])).sum())
        if buenas < 0.5 * len(filas_csv):
            raise ValueError(f"lat/lon/FRP no válidos en la mayoría de las filas ({buenas}/{len(filas_csv)}); {muestra}"[:400])
    frp = out["frp"][np.isfinite(out["frp"])]
    if frp.size and frp.max() > 1e5:
        raise ValueError(f"FRP de hasta {frp.max():.0f}: ¿otra unidad o escala? {muestra}"[:400])
    if frp.size >= 20 and np.all(frp == np.round(frp)):
        raise ValueError(f"todos los FRP son enteros: ¿valores escalados? {muestra}"[:400])
    if "conf" in out:
        c = out["conf"][np.isfinite(out["conf"])]
        if c.size and (c.min() < 0 or (np.median(c) <= 1 < c.max())):   # relleno, o 0–1 mezclado con valores > 1
            raise ValueError(f"confianza dudosa (de {c.min():.2f} a {c.max():.2f}, mediana {np.median(c):.2f}): {muestra}"[:400])
        if c.size and np.median(c) > 1:                    # en % (la de MTG): a 0–1
            out["conf"] = out["conf"] / 100.0
        c = out["conf"][np.isfinite(out["conf"])]
        if c.size and c.max() > 1:
            raise ValueError(f"confianza fuera de 0–1 tras pasar de % (hasta {c.max():.2f}): ¿otra escala? {muestra}"[:400])
        orig = c * 100.0
        if c.size >= 20 and np.unique(c).size <= 3 and np.all(np.abs(orig - np.round(orig)) < 1e-6) and c.max() <= 0.03:
            raise ValueError(f"confianza en clases (valores {sorted(set(np.round(orig, 2).tolist()))}), no en %: {muestra}"[:400])
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
    for k in _CLAVES_FILA.values():                        # una variable que el fichero no trae: la clave con null
        cols.setdefault(k, [None] * idx.size)
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


def _bajar_fuente(ts: str, fuente: str, hdf5: bool, intentadas: List[str]) -> Tuple[bytes, str, str]:
    """(contenido, content-type, «nc»|«csv.gz») según fuente: nc, csv o auto (el .nc y, solo si da 404 y el slot es
    anterior a MTG_CSV_HASTA, el CSV: así un slot que no existe no cuesta dos peticiones a la cuenta que comparte MSG).
    Con hdf5=True el .nc se valida como HDF5 (NoEsHdf5 si no lo es). Las excepciones de descarga se propagan; en
    intentadas quedan las URL pedidas, en orden."""
    if fuente in ("auto", "nc"):
        try:
            intentadas.append(build_url(ts))
            if hdf5:
                return _bajar_hdf5(ts), "application/x-netcdf", "nc"
            b, ctype = _bajar(build_url(ts))
            return b, ctype, "nc"
        except requests.HTTPError as e:
            if fuente == "nc" or _describe_error(e) != "HTTP 404" or ts >= MTG_CSV_HASTA:
                raise
    intentadas.append(build_url_csv(ts))
    b, ctype = _bajar(build_url_csv(ts))
    return b, ctype, "csv.gz"


def _fallo_descarga(e: Exception, intentadas: List[str]) -> Dict[str, Any]:
    return {"ok": False, "status": "descarga_fallida", "error": _describe_error(e), "intentado": intentadas}


def _fuera_de_archivo(ts: str) -> Optional[Dict[str, Any]]:
    """404 sin pedir nada a IPMA si el slot es anterior al archivo o aún no ha empezado (la ruta es pública)."""
    if ts < MTG_DESDE or slot_iso(ts) > _iso(_utcnow()):
        return {"ok": False, "status": "descarga_fallida", "error": "HTTP 404",
                "motivo": f"fuera del archivo (desde {MTG_DESDE} hasta ahora)"}
    return None


async def by_ts(ts: str, bbox: Optional[Tuple[float, float, float, float]] = None,
                min_frp: Optional[float] = None, min_conf: Optional[float] = None,
                fuente: str = "auto") -> Dict[str, Any]:
    """Un slot concreto bajo demanda (no toca el buffer). Hay datos desde el 1-ene-2025 (antes de ~mayo de 2026, solo
    en CSV: fuente=auto lo coge si el .nc da 404). De uno en uno."""
    bbox = bbox or DEFAULT_IBERIA_BBOX
    iso = slot_iso(ts)

    def trabajo() -> Dict[str, Any]:
        fuera = _fuera_de_archivo(ts)
        if fuera:
            return fuera
        intentadas: List[str] = []
        try:
            b, _, usada = _bajar_fuente(ts, fuente, hdf5=True, intentadas=intentadas)
        except Exception as e:
            return _fallo_descarga(e, intentadas)
        try:
            a = leer_arrays(b) if usada == "nc" else leer_arrays_csv(b)
        except Exception as e:
            return {"ok": False, "status": "parse_fallido", "fuente": usada, "error": f"{type(e).__name__}: {e}"[:500]}
        rows = filas(a, bbox, iso, min_frp=min_frp, min_conf=min_conf)
        return {"ok": True, "satellite": "MTG", "slot_ts": iso, "downloaded_at": _iso(_utcnow()), "fuente": usada,
                "count": len(rows), "count_disco": int(a["frp"].size),
                "bbox": {"w": bbox[0], "s": bbox[1], "e": bbox[2], "n": bbox[3]}, "rows": rows}

    async with _bajo_demanda:
        return await asyncio.to_thread(trabajo)


async def esquema_ts(ts: str, fuente: str = "auto") -> Dict[str, Any]:
    def trabajo() -> Dict[str, Any]:
        fuera = _fuera_de_archivo(ts)
        if fuera:
            return fuera
        intentadas: List[str] = []
        try:
            b, ctype, usada = _bajar_fuente(ts, fuente, hdf5=False, intentadas=intentadas)
        except Exception as e:
            return _fallo_descarga(e, intentadas)
        try:
            if usada == "csv.gz":
                lineas = texto_csv(b).splitlines()
                return {"ok": True, "slot_ts": slot_iso(ts), "fuente": usada, "csv": True, "bytes": len(b),
                        "content_type": ctype, "lineas": len(lineas), "primeras_lineas": [l[:300] for l in lineas[:6]]}
            return {"ok": True, "slot_ts": slot_iso(ts), "fuente": usada, **esquema(b, ctype)}
        except Exception as e:
            return {"ok": False, "status": "esquema_fallido", "fuente": usada, "empieza_por": repr(b[:16]),
                    "error": f"{type(e).__name__}: {e}"[:300]}

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
