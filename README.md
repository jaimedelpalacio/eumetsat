\# LSA SAF FRP-PIXEL → JSON (Meteosat) — Microservicio para Render (RAM doble buffer)



Servicio \*\*FastAPI\*\* que descarga y procesa el producto \*\*FRP-PIXEL (LSA-502)\*\* de \*\*LSA SAF\*\* (Meteosat),

mantiene en \*\*RAM\*\* el \*\*último\*\* y el \*\*penúltimo\*\* dataset (doble buffer) y lo sirve en \*\*JSON\*\*.

Pensado para que \*\*MT Neo\*\* consuma `GET /frp-pixel/latest?bbox=...` con latencia mínima.



\## Qué hace

\- Cada \*\*/reload\*\* (llamado por cron del panel cada 5 min):

&nbsp; - Calcula el último slot (floorUTC − LAG\_MIN), con \*\*fallback −15/−30 min\*\* si 404.

&nbsp; - Descarga HDF5 (Basic Auth con `LSASAF\_USER/PASS`), valida firma y \*\*parsea con h5py\*\*.

&nbsp; - Filtra por \*\*BBOX Iberia (configurable)\*\* y agrega metadatos: `slot\_ts`, `downloaded\_at`, `sha256`, etc.

&nbsp; - Si el slot es \*\*nuevo y válido\*\*, conmuta \*\*current ← new\*\*, \*\*previous ← old\*\* (atómico).

\- \*\*/frp-pixel/latest\*\* sirve SIEMPRE desde \*\*RAM\*\* y permite \*\*refiltro\*\* por BBOX pequeña y \*\*umbrales\*\*.

\- \*\*/frp-pixel/by-ts\*\* procesa un slot concreto \*\*bajo demanda\*\* (no toca el buffer).

\- \*\*/health\*\* para checks.



\## Despliegue en Render (plan PRO)

1\. \*\*Conecta el repo\*\* (este código) a Render → \*\*New Web Service\*\*.

2\. \*\*Runtime\*\*: Docker, usa este repo con el `Dockerfile`.

3\. \*\*Port\*\*: 8080 (Render lo detecta).

4\. \*\*Health Check\*\*: `/health`.

5\. \*\*Escalado\*\*: \*\*1 instancia\*\* (recomendado con RAM-only).

6\. \*\*Variables de entorno\*\*:

&nbsp;  - `LSASAF\_USER`, `LSASAF\_PASS` \*(Secrets)\*.

&nbsp;  - `API\_KEY` \*(Secret)\* para proteger `/reload`.

&nbsp;  - `LAG\_MIN=30`, `FALLBACKS=2` \*(opcional)\*.

&nbsp;  - `IBERIA\_BBOX=-9.5,35.5,3.5,44.5` \*(opcional)\*.

7\. \*\*Cron desde el panel\*\*:

&nbsp;  - Crea un \*\*cron job\*\* tipo “Web URL”.

&nbsp;  - Método: \*\*GET\*\*

&nbsp;  - URL: `https://<tu-servicio>.onrender.com/reload?secret=<API\_KEY>`

&nbsp;  - Programación: `\*/5 \* \* \* \*` (cada 5 min).



> \*\*Nota:\*\* Como este diseño es \*\*RAM-only\*\*, tras un redeploy puede haber una ventana corta sin snapshot hasta que el cron o el warmup carguen el último slot.



\## Endpoints

\- `GET /frp-pixel/latest?bbox=w,s,e,n\&min\_frp=...\&min\_conf=...`

\- `GET /frp-pixel/by-ts?ts=YYYYMMDDHHMM\&bbox=w,s,e,n\&min\_frp=...\&min\_conf=...`

\- `GET /reload?secret=<API\_KEY>`

\- `GET /health`



\## Ejemplo de uso desde MT Neo

\- \*\*v0 (SCRIPT)\*\*:  

&nbsp; `return "https://<tu-servicio>.onrender.com/frp-pixel/latest?bbox=-7.5,37.0,-6.5,38.0\&min\_conf=70";`

\- \*\*v1 (HTTP GET $v0)\*\*:  

&nbsp; Headers: `Accept: application/json`

\- \*\*v2 (SCRIPT)\*\*:  

&nbsp; `const j = JSON.parse($v1.content); return j;`



\## Campos por detección

```json

{

&nbsp; "lat": 40.1234,

&nbsp; "lon": -3.5678,

&nbsp; "frp\_mw": 12.3,

&nbsp; "frp\_unc\_mw": 2.1,

&nbsp; "confidence": 85.0,

&nbsp; "pixel\_km2": 3.8,

&nbsp; "time\_raw": 1.7261e9,

&nbsp; "slot\_ts": "2025-09-12T11:00:00Z",

&nbsp; "downloaded\_at": "2025-09-12T11:08:21Z"

}



