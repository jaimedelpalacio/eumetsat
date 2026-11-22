"""Prueba local del microservicio con MTG: ficheros sintéticos (sin red ni credenciales), FastAPI TestClient.
Atributos numéricos con forma (1,) como los guarda netCDF-C. Uso (con las dependencias de requirements.txt + httpx):
    PYTHONIOENCODING=utf-8 python pruebas/prueba_mtg.py .
"""
import asyncio
import datetime as dt
import gzip
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


def csv_gz(texto):
    return gzip.compress(texto.encode("utf-8"))


# ListProduct en CSV como el histórico de IPMA anterior a ~mayo de 2026 (formato real SIN VER: se prueba con variantes)
CSV_OK = csv_gz("LATITUDE,LONGITUDE,FRP,FRP_UNCERTAINTY,FIRE_CONFIDENCE,PIXEL_SIZE,ABS_LINE,ABS_SAMP\n"
                "37.2612,-7.3362,408.6,20.1,93,1.4,100,200\n"
                "37.18,-6.90,12.3,1.0,60,1.4,101,201\n"
                "43.8,5.0,5.5,0.5,80,1.6,102,202\n"
                "8.0,20.0,30.0,2.0,50,1.1,103,203\n")


def get_falso(url, timeout=None, auth=None):
    pedidas.append(url)
    if "MTFRPPixel" in url and url.endswith(".csv.gz"):
        if "CSV404" in estado or "404" in estado:      # «404»: IPMA no tiene ni el .nc ni el CSV
            return Resp(404, b"no")
        if "CSVMALO" in estado:
            return Resp(200, csv_gz("a,b,c\n1,2,3\n"))
        return Resp(200, CSV_OK)
    if "MTFRPPixel" in url and "NC404" in estado:
        return Resp(404, b"no")
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

print("4. Histórico en CSV comprimido (anterior a ~mayo de 2026)")
a = mtg.leer_arrays_csv(CSV_OK)
ok(a["frp"].size == 4 and abs(a["frp"][0] - 408.6) < 1e-9 and abs(a["conf"][0] - 0.93) < 1e-9 and abs(a["unc"][0] - 20.1) < 1e-9
   and a["line"][2] == 102, "lee las columnas por nombre; confianza en % pasada a 0–1")
variantes = {
    "punto y coma, minúsculas y comillas": '"latitude";"longitude";"frp";"fire_confidence"\n"37.2612";"-7.3362";"408.6";"93"\n',
    "cabecera con # y comentarios": "# LSA-509 ListProduct\n#LATITUDE\tLONGITUDE\tFRP\tFIRE_CONFIDENCE\n# otro comentario\n37.2612\t-7.3362\t408.6\t93\n",
    "separado por espacios, sin confianza": "LATITUDE LONGITUDE FRP\n37.2612 -7.3362 408.6\n\n",
    "BOM UTF-8 y CRLF": "﻿LATITUDE,LONGITUDE,FRP,FIRE_CONFIDENCE\r\n37.2612,-7.3362,408.6,0.93\r\n",
}
for nombre, txt in variantes.items():
    x = mtg.leer_arrays_csv(csv_gz(txt))
    ok(x["frp"].size == 1 and abs(x["frp"][0] - 408.6) < 1e-9 and abs(x["lat"][0] - 37.2612) < 1e-9
       and ("conf" not in x or abs(x["conf"][0] - 0.93) < 1e-9), f"CSV: {nombre}")
x = mtg.leer_arrays_csv(csv_gz("LATITUDE,LONGITUDE,FRP\n37.0,-7.0,\n37.1,-7.0,-999\n95,-7.0,5\n37.2,-7.0,abc\n37.3,-7.0,7.5\n37.4\n"
                                + "".join(f"37.{i},-7.0,{i}\n" for i in range(5, 10))))
ok(np.isnan(x["frp"][[0, 1, 3]]).all() and x["frp"][2] == 5 and abs(x["frp"][4] - 7.5) < 1e-9 and np.isnan(x["lat"][2])
   and np.isnan(x["lon"][5]), "CSV: vacío, negativo, no numérico, lat fuera de rango y fila corta → sin dato")
ok(len(mtg.filas(x, (-8, 36, -6, 38), "X")) == 9, "CSV: sin lat/lon no entra en el bbox (sin FRP sí, como en el .nc)")
for nombre, b in [("sin cabecera reconocible", csv_gz("a,b,c\n1,2,3\n")), ("sin FRP", csv_gz("LATITUDE,LONGITUDE\n1,2\n")),
                  ("gzip corrupto", b"\x1f\x8b" + b"\x00" * 30),
                  ("columna índice sin nombre (campos desplazados)", csv_gz("LATITUDE,LONGITUDE,FRP\n0,37.26,-7.33,408.6\n1,37.3,-7.3,12\n")),
                  ("cabecera con un nombre de dos palabras (espacios)", csv_gz("LATITUDE LONGITUDE PIXEL SIZE FRP\n37.26 -7.33 1.4 408.6\n")),
                  ("enteros escalados", csv_gz("LATITUDE,LONGITUDE,FRP\n3726,-733,4086\n3718,-690,123\n")),
                  ("coma decimal con punto y coma", csv_gz("LATITUDE;LONGITUDE;FRP\n37,26;-7,33;408,6\n37,18;-6,9;12,3\n"))]:
    try:
        mtg.leer_arrays_csv(b); ok(False, f"CSV {nombre}: debe fallar")
    except ValueError as e:
        ok(True, f"CSV {nombre}: falla con el texto delante ({str(e)[:70]})")
x = mtg.leer_arrays_csv(b"FRP,FIRE_CONFIDENCE,LONGITUDE,LATITUDE\n408.6,93,-7.3362,37.2612\n")
ok(abs(x["frp"][0] - 408.6) < 1e-9 and abs(x["lat"][0] - 37.2612) < 1e-9 and abs(x["lon"][0] + 7.3362) < 1e-9,
   "CSV en otro orden y ya descomprimido (Content-Encoding: gzip): se lee por nombre")
x = mtg.leer_arrays_csv(csv_gz("# columnas: LONGITUDE LATITUDE FRP\nLATITUDE,LONGITUDE,FRP\n37.2612,-7.3362,408.6\n"))
ok(abs(x["lat"][0] - 37.2612) < 1e-9, "CSV: un comentario que nombra las columnas en otro orden no se toma como cabecera")
ok(mtg.leer_arrays_csv(csv_gz("LATITUDE,LONGITUDE,FRP\n"))["frp"].size == 0, "CSV con solo la cabecera: válido, 0 detecciones")
r1 = mtg.filas(mtg.leer_arrays_csv(csv_gz("LATITUDE,LONGITUDE,FRP\n37.2,-7.0,5\n")), (-8, 36, -6, 38), "X")[0]
ok(r1["frp_unc_mw"] is None and r1["confidence"] is None and "pixel_km2" in r1,
   "filas: las variables que el fichero no trae salen con null (mismas claves siempre)")
ok(mtg.build_url_csv("202603151300").endswith(
    "/PRODUCTS/MTG/MTFRPPixel/NATIVE/2026/03/15/LSA-509_MTG_MTFRPPIXEL-ListProduct_MTG-FD_202603151300.csv.gz"),
   "URL del CSV como en el servidor de IPMA")
with TestClient(appmod.app) as cli:
    estado.add("NC404"); n0 = len(pedidas)
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202603151300", "bbox": "-7.45,36.95,-6.35,37.40"})
    j = r.json()
    ok(r.status_code == 200 and j["fuente"] == "csv.gz" and j["count"] == 2 and j["count_disco"] == 4
       and j["source_url"].endswith(".csv.gz") and len(pedidas) - n0 == 2,
       f"by-ts auto: el .nc da 404 → lee el CSV (2 en Huelva de 4; {len(pedidas) - n0} descargas)")
    ok(cli.get("/mtg-frp/by-ts", params={"ts": "202603151300", "fuente": "nc"}).status_code == 404,
       "by-ts fuente=nc: no prueba el CSV (404)")
    estado.add("CSV404")
    ok(cli.get("/mtg-frp/by-ts", params={"ts": "202603151300"}).status_code == 404, "by-ts auto sin .nc ni CSV: 404")
    estado.discard("CSV404"); estado.add("CSVMALO")
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202603151300"})
    ok(r.status_code == 502 and r.json()["status"] == "parse_fallido" and r.json()["fuente"] == "csv.gz"
       and "a,b,c" in r.json()["error"], "by-ts con un CSV de formato desconocido: 502 y la cabecera en el error")
    estado.discard("CSVMALO"); estado.discard("NC404"); n0 = len(pedidas)
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202609191240", "bbox": "-7.45,36.95,-6.35,37.40"})
    ok(r.json()["fuente"] == "nc" and r.json()["count"] == 4 and len(pedidas) - n0 == 1,
       "by-ts auto con el .nc disponible: igual que antes (no pide el CSV)")
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202609191240", "fuente": "csv", "bbox": "-7.45,36.95,-6.35,37.40"})
    ok(r.json()["fuente"] == "csv.gz" and r.json()["count"] == 2, "by-ts fuente=csv: lee el CSV aunque haya .nc (para comparar)")
    ok(cli.get("/mtg-frp/by-ts", params={"ts": "202609191240", "fuente": "zip"}).status_code == 400, "fuente no válida: 400")
    estado.add("NC404"); estado.add("CAIDO_CSV")
    mtg.requests.get, original = (lambda url, timeout=None, auth=None: (_ for _ in ()).throw(mtg.requests.ConnectionError("caido"))
                                  if url.endswith(".csv.gz") else get_falso(url, timeout, auth)), mtg.requests.get
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202603151300"})
    ok(r.status_code == 502 and r.json()["status"] == "descarga_fallida" and len(r.json()["intentado"]) == 2
       and r.json()["intentado"][1].endswith(".csv.gz"), "by-ts: .nc 404 y CSV caído → 502, con las URL intentadas")
    mtg.requests.get = original; estado.discard("CAIDO_CSV")
    estado.discard("NC404"); estado.add("401")
    n0 = len(pedidas)
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202603151300"})
    ok(r.status_code == 502 and len(pedidas) - n0 == 1, "by-ts con la cuenta rechazada (401): no prueba el CSV")
    estado.discard("401"); estado.add("NC404")
    r = cli.get("/mtg-frp/esquema", params={"ts": "202603151300"}, headers={"X-API-Key": "clave-prueba"})
    ok(r.status_code == 200 and r.json()["csv"] and r.json()["primeras_lineas"][0].startswith("LATITUDE,")
       and r.json()["lineas"] == 5, "esquema del CSV: sus primeras líneas")
    estado.discard("NC404")
    r = cli.get("/mtg-frp/esquema", params={"ts": "202609191240", "fuente": "nc"}, headers={"X-API-Key": "clave-prueba"})
    ok(r.status_code == 200 and r.json()["hdf5"] and r.json()["fuente"] == "nc", "esquema del .nc igual que antes")
    n0 = len(pedidas)
    r = cli.get("/mtg-frp/by-ts", params={"ts": "203001011200"})
    ok(r.status_code == 404 and len(pedidas) == n0, "by-ts de un slot futuro: 404 sin pedir nada a IPMA")
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202412311200"})
    ok(r.status_code == 404 and len(pedidas) == n0, "by-ts anterior al archivo (2025): 404 sin pedir nada a IPMA")
    r = cli.get("/mtg-frp/esquema", params={"ts": "203001011200"}, headers={"X-API-Key": "clave-prueba"})
    ok(r.status_code == 404 and len(pedidas) == n0, "esquema de un slot futuro: 404 sin pedir nada a IPMA")
    estado.add("NC404")
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202609191240"})
    ok(r.status_code == 404 and len(pedidas) - n0 == 1, "by-ts auto posterior a MTG_CSV_HASTA con el .nc en 404: no prueba el CSV")
    estado.discard("NC404"); estado.add("CSVMALO")
    n0 = len(pedidas)
    r = cli.get("/mtg-frp/by-ts", params={"ts": "202609191240", "fuente": "csv"})
    ok(r.status_code == 502 and "cabecera" in r.json()["error"], "by-ts fuente=csv se puede pedir siempre (para comparar)")
    estado.discard("CSVMALO")
    m = asyncio.run(mtg.reload_latest()); n0 = len(pedidas)
    estado.add("NC404"); mtg._current = mtg._previous = None
    hasta, mtg.MTG_CSV_HASTA = mtg.MTG_CSV_HASTA, "209901010000"   # que auto SÍ probaría el CSV para estos slots
    asyncio.run(mtg.reload_latest())
    ok(len(pedidas) > n0 and not any(u.endswith(".csv.gz") for u in pedidas[n0:]),
       "el bucle de recarga no pide nunca el CSV (aunque auto lo probaría para esos slots)")
    mtg.MTG_CSV_HASTA = hasta
    estado.discard("NC404")

print("5. Formatos y escalas dudosos del CSV (segunda revisión)")
x = mtg.leer_arrays_csv(csv_gz("LATITUDE,LONGITUDE,FRP_UNCERTAINTY,FRP,FIRE_CONFIDENCE\n37.2,-7.0,1.5,40.5,80\n37.3,-7.0,2.5,30.5,70\n"
                               "37.9,-7.0,700,80\n37.4,-7.0,1.0,20.5,90,texto,suelto\n"))
ok(abs(x["frp"][0] - 40.5) < 1e-9 and np.isnan(x["frp"][2]) and np.isnan(x["unc"][2]) and np.isnan(x["frp"][3])
   and np.nanmax(x["conf"]) <= 1, "fila corta o con campos de más: entera sin dato (nunca leída por posición)")
x = mtg.leer_arrays_csv(csv_gz('LATITUDE,LONGITUDE,FRP,"PIXEL SIZE, km2"\n37.2,-7.0,40.5,1.4\n'))
ok(abs(x["frp"][0] - 40.5) < 1e-9, "cabecera con un nombre entre comillas que lleva el separador")
x = mtg.leer_arrays_csv(csv_gz("LATITUDE,LONGITUDE,FRP,\n37.2,-7.0,40.5\n37.3,-7.0,30.5,\n"))
ok(x["frp"].size == 2 and abs(x["frp"][1] - 30.5) < 1e-9, "separador al final de la cabecera, con o sin él en las filas")
veinte = lambda frp, conf: csv_gz("LATITUDE,LONGITUDE,FRP,FIRE_CONFIDENCE\n" +
                                  "".join(f"37.{i:02d},-7.0,{frp(i)},{conf(i)}\n" for i in range(20)))
for nombre, b in [("FRP en vatios", veinte(lambda i: 408600000 + i, lambda i: 80)),
                  ("FRP todo enteros (escalado ×10)", veinte(lambda i: 4086 + i, lambda i: 80)),
                  ("confianza 0–10000", veinte(lambda i: 40.5 + i, lambda i: 9300)),
                  ("confianza en clases 1–3", veinte(lambda i: 40.5 + i, lambda i: 1 + i % 3)),
                  ("datos antes de la cabecera", csv_gz("# LATITUDE,LONGITUDE,FRP\n37.2,-7.0,40.5\nLATITUDE,LONGITUDE,FRP\n"))]:
    try:
        mtg.leer_arrays_csv(b); ok(False, f"CSV {nombre}: debe fallar")
    except ValueError as e:
        ok(True, f"CSV {nombre}: falla ({str(e)[:70]})")
ok(mtg.leer_arrays_csv(veinte(lambda i: 40.5 + i, lambda i: 50 + i))["frp"].size == 20, "20 filas normales: se leen")
roto = bytearray(csv_gz("LATITUDE,LONGITUDE,FRP\n" * 50)); roto[12:20] = b"\xff" * 8
for nombre, b in [("gzip con datos dañados (zlib.error)", bytes(roto)),
                  ("confianza 0–1 con un valor > 1", csv_gz("LATITUDE,LONGITUDE,FRP,FIRE_CONFIDENCE\n37.2,-7,40.5,0.9\n37.3,-7,30.5,0.8\n37.4,-7,20.5,1.5\n")),
                  ("relleno -1 en la confianza", csv_gz("LATITUDE,LONGITUDE,FRP,FIRE_CONFIDENCE\n37.2,-7,40.5,80\n37.3,-7,30.5,-1\n"))]:
    try:
        mtg.leer_arrays_csv(b); ok(False, f"CSV {nombre}: debe fallar")
    except ValueError as e:
        ok(True, f"CSV {nombre}: falla ({str(e)[:70]})")
x = mtg.leer_arrays_csv(csv_gz("LATITUDE,LONGITUDE,FRP,FRP_UNCERTAINTY,PIXEL_SIZE\n37.2,-7,40.5,-999,-1\n"))
ok(np.isnan(x["unc"][0]) and np.isnan(x["area"][0]) and abs(x["frp"][0] - 40.5) < 1e-9, "relleno negativo en incertidumbre y área → sin dato")
os.environ["MTG_CSV_HASTA"] = "2026-07-01"
m3 = importlib.reload(mtg)
ok(m3.MTG_CSV_HASTA == "202607010000", "MTG_CSV_HASTA mal escrita: valor por defecto")
del os.environ["MTG_CSV_HASTA"]
mtg = importlib.reload(mtg)
print("RESULTADO:", "OK" if not fallos else f"{fallos} FALLOS")
