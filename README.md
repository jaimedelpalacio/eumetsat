# LSA SAF FRP-PIXEL → JSON (Meteosat)

Microservicio **FastAPI** que descarga el producto **FRP-PIXEL List Product** de **LSA SAF**
(detección de focos de calor desde Meteosat, un slot cada 15 min), lo recorta a Iberia y
lo sirve en **JSON** desde RAM. Lo consume Pulse (programa `ap_Detección calor Satélites NASA
y EUMESAT`) con `GET /frp-pixel/latest?bbox=...`.

## Cómo funciona

- Un **cron** de Render llama a `/reload` cada 5 min (`2-59/5 * * * *`).
- `/reload` prueba los slots desde el más reciente publicable (ahora − `LAG_MIN`, redondeado a
  15 min) hacia atrás (`FALLBACKS` intentos), pero **solo los más nuevos que el publicado**:
  si no hay nada nuevo no descarga nada.
  - Un candidato que falla (404, timeout, respuesta que no es HDF5, fichero truncado, slot
    que no pasa la sanidad) se salta y se prueba el anterior; el motivo queda en `errores`.
  - Las descargas y el parseo van en un hilo aparte: mientras se descarga, el resto de
    endpoints sigue respondiendo al instante.
  - Un `/reload` no dedica más de `RELOAD_BUDGET_S` segundos a probar candidatos.
- Doble buffer en RAM (`current`/`previous`). Tras un redeploy, el arranque carga el último
  slot en segundos (`[BOOT] warmup: ...` en el log).

### Frescura del dato

Cada respuesta incluye `age_min` (minutos desde el inicio del slot servido) y `stale`
(`true` si `age_min > STALE_MIN`). Lo normal son 32–47 min.

- `/frp-pixel/latest` sigue devolviendo 200 con el último dato aunque esté desfasado: quien
  consume decide con `stale`.
- `/reload` responde **503** (y el cron sale fallido → email de Render) **solo** si no hay dato
  o el que se sirve está desfasado. Un fallo puntual de IPMA con el dato aún fresco da 200 con
  `errores`.
- `/health` siempre 200 mientras el proceso vive; incluye `stale`, `age_min` y `last_reload`.

## Endpoints

| Endpoint | Descripción |
|---|---|
| `GET /frp-pixel/latest?bbox=w,s,e,n&min_frp=&min_conf=` | Último slot desde RAM, refiltrable (no amplía más allá de Iberia). 503 si aún no hay dato. |
| `GET /frp-pixel/by-ts?ts=YYYYMMDDHHMM&bbox=&min_frp=&min_conf=` | Descarga y procesa un slot concreto bajo demanda (no toca el buffer). 404 si falla. |
| `GET /reload` con cabecera `X-API-Key: <API_KEY>` | Recarga (lo llama el cron). `?key=` se acepta aún por compatibilidad, pero deja la clave en los logs. |
| `GET /health` | Estado (`warm`/`cold`), último slot, frescura y resultado de la última recarga. |

Campos de cada fila: `latitude`, `longitude`, `frp_mw`, `confidence`, `pixel_km2`, `time_raw`,
`acq_time_utc`, `frp_unc_mw`, `vza_deg`, `bt_mir_k`, `bt_tir_k` (null si el producto no los trae).

## Configuración (variables de entorno)

| Variable | Por defecto | |
|---|---|---|
| `LSASAF_USER`, `LSASAF_PASS` | — | Credenciales de LSA SAF (secretos). |
| `API_KEY` | — | Protege `/reload`. La misma variable va en el cron. |
| `LAG_MIN` | 30 | Margen de publicación del slot. |
| `FALLBACKS` | 8 | Slots hacia atrás que se prueban (8 ⇒ 2 h). |
| `HTTP_TIMEOUT` | 30 | Timeout por descarga (s). |
| `RELOAD_BUDGET_S` | 120 | Tiempo máximo de un `/reload` probando candidatos (s). |
| `STALE_MIN` | 75 | Antigüedad a partir de la cual el dato está desfasado (min). |
| `IBERIA_BBOX` | `-9.5,35.5,3.5,44.5` | Recorte que se guarda en RAM. |
| `LSA_HOST` | `https://datalsasaf.lsasvcs.ipma.pt` | Origen de descarga (útil para probar contra un mock). |

## Despliegue en Render

- **Web service** `eumetsat` (Docker, Frankfurt, 1 instancia — el estado vive en RAM),
  auto-deploy desde `main`. Health check: `/health`.
- **Cron** `eumesatcron` (imagen `curlimages/curl`), con la variable `API_KEY`:

  ```
  curl -fsS --max-time 300 -H X-API-Key:${API_KEY} https://eumetsat-dyfr.onrender.com/reload
  ```

`render.yaml` es solo de referencia: el servicio y el cron se gestionan desde el panel.
