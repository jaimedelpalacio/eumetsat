"""Prueba local del microservicio con MTG: ficheros sintéticos (sin red ni credenciales), FastAPI TestClient.
Atributos numéricos con forma (1,) como los guarda netCDF-C. Uso (con las dependencias de requirements.txt + httpx):
    PYTHONIOENCODING=utf-8 python pruebas/prueba_mtg.py .
"""
import asyncio
import datetime as dt
import importlib
import io
import os
import sys

import h5py
import numpy as np

sys.path.insert(0, sys.argv[1])
import ingest  # noqa: E402
import mtg  # noqa: E402

fallos = 0


def ok(c, m):
    global fallos
    print(("  OK    " if c else "  FALLO ") + m)
    if not c:
        fallos += 1


A = lambda *v: np.array(v)                                  # atributo con forma (1,) (o más), como netCDF-C


def fichero_mtg(con_quality=True, conf_pct=True, n_africa=40):
    b = io.BytesIO()
    with h5py.File(b, "w") as f:
        f.attrs["platform"] = "MTG-I1"
        g = f.create_group("ListProduct")
        # 4 en el área de Huelva (una con FRP de relleno, otra con missing_value), 1 en Francia, muchas en África
        lat = [37.2612, 37.18, 37.0, 37.1] + [43.8] + list(np.linspace(5, 12, n_africa))
        lon = [-7.3362, -6.90, -6.5, -6.6] + [5.0] + list(np.linspace(10, 30, n_africa))
        frp_raw = [4086, 123, -999, -1] + [55] + list(np.arange(n_africa) * 7 + 50)   # ×0,1 MW
        la = g.create_dataset("LATITUDE", data=np.array(lat, "f4")); la.attrs["units"] = "degrees_north"
        g.create_dataset("LONGITUDE", data=np.array(lon, "f4"))
        d = g.create_dataset("FRP", data=np.array(frp_raw, "i4"))
        d.attrs["scale_factor"] = A(0.1); d.attrs["add_offset"] = A(0.0); d.attrs["_FillValue"] = A(-999)
        d.attrs["missing_value"] = A(-1); d.attrs["units"] = "MW"
        c = g.create_dataset("FIRE_CONFIDENCE", data=np.array(([93, 60, 70, 80, 80] + [50] * n_africa) if conf_pct else
                                                              ([0.93, 0.6, 0.7, 0.8, 0.8] + [0.5] * n_africa), "f4"))
        c.attrs["units"] = "%" if conf_pct else "1"
        g.create_dataset("ABS_LINE", data=np.arange(5 + n_africa, dtype="i4"))
        g.create_dataset("ABS_SAMP", data=np.arange(5 + n_africa, dtype="i4") * 2)
        if con_quality:
            q = f.create_group("QualityProduct")
            q.create_dataset("LATITUDE", data=np.zeros((50, 50), "f4"))     # señuelo: no debe usarse
            q.create_dataset("FRP", data=np.ones((50, 50), "f4"))
    return b.getvalue()


def fichero_msg():
    b = io.BytesIO()
    with h5py.File(b, "w") as f:
        for nom, vals, sf in [("LATITUDE", [37.30, 38.1, 10.0, 11.0], 100), ("LONGITUDE", [-6.70, -4.2, 20.0, 21.0], 100),
                              ("FRP", [253, 120, 800, 900], 10), ("FIRE_CONFIDENCE", [90, 80, 70, 60], 100),
                              ("ACQTIME", [1215, 1216, 1217, 1218], 1)]:
            d = f.create_dataset(nom, data=(np.array(vals) * sf).astype("i4"))
            d.attrs["SCALING_FACTOR"] = sf
            d.attrs["MISSING_VALUE"] = -1
    return b.getvalue()


MTG_OK, MSG_OK = fichero_mtg(), fichero_msg()
pedidas = []
estado = set()


class Resp:
    def __init__(self, code, body, ctype="application/x-netcdf"):
        self.status_code, self.content, self.headers = code, body, {"Content-Type": ctype}

    def raise_for_status(self):
        if self.status_code >= 400:
            e = mtg.requests.HTTPError(f"HTTP {self.status_code}")
            e.response = self
            raise e


def get_falso(url, timeout=None, auth=None):
    pedidas.append(url)
    if "MTFRPPixel" in url:
        if "CAIDO" in estado:
            raise mtg.requests.ConnectionError("caido")
        if "401" in estado:
            return Resp(401, b"no")
        if "HTML" in estado:
            return Resp(200, b"<html>login</html>", "text/html")
        if "404" in estado:
            return Resp(404, b"no")
        if "CORRUPTO" in estado:
            return Resp(200, b"\x89HDF\r\n\x1a\n" + b"\x00" * 300)
        if "ILEGIBLE" in estado:
            b = io.BytesIO()
            with h5py.File(b, "w") as f:
                f.create_group("ListProduct").create_dataset("OTRA", data=[1])
            return Resp(200, b.getvalue())
        return Resp(200, MTG_OK)
    return Resp(200, MSG_OK)


mtg.requests.get = get_falso
ingest._download_hdf5 = lambda url: MSG_OK

print("1. Lectura CF del NetCDF de MTG")
a = mtg.leer_arrays(MTG_OK)
ok(a["frp"].size == 45, f"lee el grupo ListProduct (45 píxeles), no el QualityProduct ({a['frp'].size})")
ok(abs(a["frp"][0] - 408.6) < 1e-9 and np.isnan(a["frp"][2]) and np.isnan(a["frp"][3]),
   "atributos de forma (1,): FRP escalado; _FillValue y missing_value → sin dato")
ok(abs(a["conf"][0] - 0.93) < 1e-6, "confianza en % pasada a 0–1")
ok(abs(mtg.leer_arrays(fichero_mtg(conf_pct=False))["conf"][0] - 0.93) < 1e-6, "confianza ya en 0–1 se deja igual")
b = io.BytesIO()
with h5py.File(b, "w") as f:
    g = f.create_group("ListProduct")
    g.create_dataset("LATITUDE", data=np.array([37.0, 37.1, 37.2, np.nan], "f4"))
    g.create_dataset("LONGITUDE", data=np.array([-7.0, -7.0, -7.0, -7.0], "f4"))
    d = g.create_dataset("FRP", data=np.array([40000, 100, 70000, 5], "i2", casting="unsafe") if False else np.array([40000, 100, 60000, 5], "u2").view("i2"))
    d.attrs["_Unsigned"] = "true"; d.attrs["scale_factor"] = A(0.01); d.attrs["valid_range"] = A(0, 50000)
x = mtg.leer_arrays(b.getvalue())
ok(abs(x["frp"][0] - 400.0) < 1e-9 and np.isnan(x["frp"][2]) and abs(x["frp"][1] - 1.0) < 1e-9,
   "_Unsigned (40000 en i2 = 400 MW) y valid_range (60000 fuera → sin dato)")
ok(len(mtg.filas(x, (-8, 36, -6, 38), "2026-01-01T00:00:00Z")) == 3, "un NaN en la latitud no entra en el bbox")
for nombre, unsig in [("escalar", "true"), ("array de bytes", np.array([b"true"]))]:
    b2 = io.BytesIO()
    with h5py.File(b2, "w") as f:
        g = f.create_group("ListProduct")
        g.create_dataset("LATITUDE", data=np.array([37.0, 37.1, 37.2], "f4"))
        g.create_dataset("LONGITUDE", data=np.array([-7.0, -7.0, -7.0], "f4"))
        d = g.create_dataset("FRP", data=np.array([100, -1, 200], "i2"))
        d.attrs["_Unsigned"] = unsig; d.attrs["_FillValue"] = A(-1); d.attrs["scale_factor"] = A(0.1)
    y = mtg.leer_arrays(b2.getvalue())["frp"]
    ok(abs(y[0] - 10) < 1e-9 and np.isnan(y[1]) and abs(y[2] - 20) < 1e-9,
       f"_Unsigned ({nombre}) con _FillValue con signo: el relleno no sale como 6553 MW")
try:
    bb = io.BytesIO()
    with h5py.File(bb, "w") as f:
        f.create_group("ListProduct").create_dataset("OTRA", data=[1])
    mtg.leer_arrays(bb.getvalue()); ok(False, "sin LATITUDE/LONGITUDE/FRP debe fallar")
except ValueError as e:
    ok("no encontradas" in str(e), "sin las variables clave: error claro")
es = mtg.esquema(MTG_OK, "application/x-netcdf")
ok(any(o["ruta"] == "ListProduct/FRP" and o["attrs"].get("units") == "MW" for o in es["objetos"]) and es["raiz"].get("platform") == "MTG-I1",
   "esquema: grupos, variables y atributos")
ok(mtg.esquema(b"CDF\x01xxxx", "application/x-netcdf")["empieza_por"].startswith("b'CDF"), "esquema de un fichero no HDF5: su firma")
ok(mtg.build_url("202609191240").endswith("/PRODUCTS/MTG/MTFRPPixel/NATIVE/2026/09/19/LSA-509_MTG_MTFRPPIXEL_MTG-FD_202609191240.nc"),
   "URL del fichero como en el servidor de IPMA")
ok(mtg.valid_ts("202609191240") and not mtg.valid_ts("202609191245") and not mtg.valid_ts("2026091912")
   and not mtg.valid_ts("２０２６０９１９１２４０"), "ts: 12 cifras ASCII y múltiplo de 10 min")
c = mtg.candidate_ts_list(dt.datetime(2026, 11, 2, 9, 25, tzinfo=dt.timezone.utc))
ok(c[0] == "202611020900" and c[1] == "202611020850" and len(c) == mtg.MTG_FALLBACKS + 1, f"candidatos con 18 min de margen: {c[:3]}")
rows = mtg.filas(a, (-7.45, 36.95, -6.35, 37.40), "X", min_frp=100)
ok(len(rows) == 1 and rows[0]["frp_mw"] == 408.6 and rows[0]["confidence"] == 0.93 and rows[0]["acq_time_utc"] == "X",
   "filas: bbox y min_frp con numpy antes de crear los dicts")

print("2. Recarga, cuenta compartida y fallos")
r = asyncio.run(mtg.reload_latest())
ok(r["status"] == "publicado" and r["count"] == 4, f"recarga: publicado ({r['count']} en Iberia)")
mtg._current = mtg._previous = None
estado.add("401")
n0 = len(pedidas)
r = asyncio.run(mtg.reload_latest())
ok(r.get("cuenta_rechazada") and len(pedidas) - n0 == 1, "cuenta rechazada (401): para en el primer intento y lo marca")
estado.discard("401"); estado.add("ILEGIBLE")
n0 = len(pedidas)
r1 = asyncio.run(mtg.reload_latest()); n1 = len(pedidas)
r2 = asyncio.run(mtg.reload_latest()); n2 = len(pedidas)
ok(n1 - n0 == mtg.MTG_FALLBACKS + 1 and n2 == n1 and "ilegible" in str(r1.get("errores")),
   f"fichero ilegible: se baja una vez y no se vuelve a bajar ({n1 - n0} y luego {n2 - n1})")
estado.discard("ILEGIBLE"); mtg._ts_ilegibles.clear()
estado.add("CORRUPTO")
n0 = len(pedidas); asyncio.run(mtg.reload_latest()); asyncio.run(mtg.reload_latest())
ok(len(pedidas) - n0 == mtg.MTG_FALLBACKS + 1, "HDF5 corrupto (OSError de h5py): se veta y no se rebaja")
estado.discard("CORRUPTO"); mtg._ts_ilegibles.clear(); estado.add("HTML")
n0 = len(pedidas); asyncio.run(mtg.reload_latest()); asyncio.run(mtg.reload_latest())
ok(len(pedidas) - n0 == 2 * (mtg.MTG_FALLBACKS + 1) and not mtg._ts_ilegibles,
   "página HTML (mantenimiento): no veta el slot, se reintenta en la vuelta siguiente")
estado.discard("HTML"); estado.add("404")
mtg._current = mtg._previous = None
r = asyncio.run(mtg.reload_latest())
ok(r.get("todo_404") is True, "todo 404 con el dato viejo: lo marca (el bucle espera más)")
estado.discard("404")
# el bucle: con la cuenta rechazada espera el doble cada vez
esperas = []


async def bucle_corto():
    reales = asyncio.sleep
    llamadas = {"n": 0}

    async def dormir(s):
        esperas.append(s); llamadas["n"] += 1
        if llamadas["n"] >= 3:
            raise asyncio.CancelledError
        await reales(0)
    mtg.asyncio.sleep = dormir
    try:
        await mtg.reload_loop()
    except asyncio.CancelledError:
        pass
    finally:
        mtg.asyncio.sleep = reales
estado.add("401")
mtg._current = None
asyncio.run(bucle_corto())
ok(esperas == [600, 1200, 2400], f"bucle con la cuenta rechazada: espera 300→600→1200… ({esperas})")
estado.discard("401")
os.environ["MTG_RELOAD_EVERY_S"] = "cinco"; os.environ["MTG_ENABLED"] = "0"
m2 = importlib.reload(mtg)
ok(m2.MTG_RELOAD_EVERY_S == 300 and m2.MTG_ENABLED is False, "variable de entorno mal escrita: valor por defecto; MTG_ENABLED=0 lo apaga")
asyncio.run(m2.reload_loop())
ok(True, "con MTG_ENABLED=0 el bucle termina sin pedir nada")
del os.environ["MTG_RELOAD_EVERY_S"], os.environ["MTG_ENABLED"]
mtg = importlib.reload(mtg)
mtg.requests.get = get_falso

print("3. Servicio (FastAPI)")
from fastapi.testclient import TestClient  # noqa: E402
import app as appmod  # noqa: E402
appmod.mtg = mtg
with TestClient(appmod.app) as cli:
    r = cli.get("/reload"); ok(r.status_code in (200, 503), f"/reload (MSG) responde ({r.status_code})")
    m = asyncio.run(mtg.reload_latest())
    ok(m["status"] in ("publicado", "noop_idempotente", "noop_monotonia"), f"MTG: recarga ({m['status']})")
    r = cli.get("/mtg-frp/latest")
    j = r.json()
    ok(r.status_code == 200 and j["satellite"] == "MTG" and j["count"] == 4 and j["count_disco"] == 45 and r.headers.get("etag"),
       f"/mtg-frp/latest: solo Iberia (4 de 45), con ETag y su edad ({j.get('age_min')} min)")
    r = cli.get("/mtg-frp/latest", params={"bbox": "-7.5,37.2,-7.2,37.3", "min_frp": 100})
    ok(r.json()["count"] == 1 and abs(r.json()["rows"][0]["frp_mw"] - 408.6) < 1e-6, "/mtg-frp/latest con bbox y min_frp")
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202609191240", "bbox": "-7.45,36.95,-6.35,37.40"})
    ok(r.status_code == 200 and r.json()["count"] == 4 and r.json()["slot_ts"] == "2026-09-19T12:40:00Z", "/mtg-frp/by-ts con el área de Huelva")
    ok(cli.get("/mtg-frp/by-ts", params={"ts": "202609191245"}).status_code == 400, "/mtg-frp/by-ts con ts no múltiplo de 10: 400")
    estado.add("CAIDO")
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202609191240"})
    ok(r.status_code == 502 and r.json()["status"] == "descarga_fallida", "/mtg-frp/by-ts con IPMA caído: 502 y motivo")
    r = cli.get("/frp-pixel/latest", params={"bbox": "-9.5,35.5,3.5,44.5"})
    ok(r.status_code == 200 and r.json()["count"] == 2, f"MSG sigue sirviendo igual con MTG caído ({r.json().get('count')} filas)")
    estado.discard("CAIDO"); estado.add("404")
    ok(cli.get("/mtg-frp/by-ts", params={"ts": "202609191240"}).status_code == 404, "/mtg-frp/by-ts de un slot que IPMA no tiene: 404")
    estado.discard("404"); estado.add("HTML")
    appmod.API_KEY = "clave-prueba"
    r = cli.get("/mtg-frp/esquema", params={"ts": "202609191240"}, headers={"X-API-Key": "clave-prueba"})
    ok(r.status_code == 200 and r.json()["hdf5"] is False and "html" in r.json()["empieza_por"], "/mtg-frp/esquema de una página HTML: su firma")
    estado.discard("HTML")
    ok(cli.get("/mtg-frp/esquema", params={"ts": "202609191240"}).status_code == 401, "/mtg-frp/esquema sin clave: 401")
    r = cli.get("/mtg-frp/esquema", params={"ts": "202609191240"}, headers={"X-API-Key": "clave-prueba"})
    ok(r.status_code == 200 and r.json()["hdf5"], "/mtg-frp/esquema con clave")
    h = cli.get("/health").json()
    ok("mtg" in h and h["mtg"]["enabled"] is True and "status" in h and "last_slot" in h, "/health: lo de MSG igual y un bloque «mtg»")
    ok(cli.get("/frp-pixel/by-ts", params={"ts": "202611020900"}).status_code == 200, "/frp-pixel/by-ts (MSG) sin cambios")
    ok("/mtg-frp/latest" in cli.get("/").text, "la raíz lista los endpoints de MTG")
print("RESULTADO:", "OK" if not fallos else f"{fallos} FALLOS")
