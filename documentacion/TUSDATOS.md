# TUSDATOS — Documentación (API puente para TusDatos.co)

Wrapper experimental para poner a prueba la API comercial de
[TusDatos.co](https://docs.tusdatos.co/redoc) (validación de antecedentes de
personas naturales, empresas y vehículos). La implementación vive dentro del
backend `integrappi`, como un endpoint/router propio:

- **Router**: `integrappi/rutas/tusdatos.py` (montado en `integrappi/main.py`,
  prefijo `/tusdatos`, sin auth — experimental)
- **Documentación oficial de la API**: https://docs.tusdatos.co/redoc
  (el spec máquina está en `https://docs.tusdatos.co/openapi.json` por si se
  quiere consultar o comparar versiones)

> Es un módulo **independiente** de `/seguridad` (ver
> [`SEGURIDAD.md`](SEGURIDAD.md)): no comparte colecciones, cobro ni
> auditoría. Sirve para **poner a prueba** la API comercial de TusDatos.co;
> si alguna fuente resulta útil, el módulo propio la implementa con sus bots.

---

## 1. Cómo funciona la API de TusDatos (lo esencial)

**Autenticación** — HTTP **Basic Auth** `email:password` de la cuenta de
TusDatos (la misma con la que se entra al dashboard) en TODOS los endpoints.
Opcionalmente un API token Bearer (`api_td_...`, máx 10 activos) creado con
`POST /api/v1/auth/token`.

**Base URL de producción**: `https://dash-board.tusdatos.co`
(las respuestas de docs.tusdatos.co son estáticas, solo de ejemplo).

**Flujo de una consulta** (persona o empresa):

```
1. POST /api/launch            {typedoc, doc, fechaE?, force?}
      └─> {jobid, nombre, validado}     ← jobid vigente 2 horas
2. GET  /api/results/{jobid}   (sondear cada ~5 s, tarda ~1 min en promedio)
      └─> {estado: procesando|finalizado|error, hallazgo, hallazgos,
           results: {Fuente: true|false|"Error"|""|"Página no disponible"}, id}
           (los 2 últimos estados NO están documentados pero llegan:
            "" = la fuente no aplicó a la consulta; ver §6)
                                                        │
3. GET  /api/v2/report/{id}        HTML del reporte     │  `id` aparece SOLO
    GET  /api/v2/report_pdf/{id}   PDF                  │  cuando finalizado
    GET  /api/report_json/{id}     JSON completo ───────┘
```

- `typedoc`: `CC`, `CE`, `INT`, `NIT`, `PP`, `PPT`, `NOMBRE`.
- `fechaE` (dd/mm/yyyy) es **obligatoria** para CE y PPT; opcional para CC.
- `force: true` relanza una cédula ya consultada.
- `hallazgos` = hallazgo de mayor categoría (`alto|medio|bajo|info|""`);
  `results` mapea cada fuente a `true` (hallazgo), `false` (sin hallazgo) o
  `"Error"`.
- Si `estado` queda en `error`, `GET /api/retry/{id}?typedoc=CC` reintenta
  las fuentes fallidas.

**Otros flujos**:

| Qué | Endpoint upstream | Notas |
|---|---|---|
| Vehículo | `POST /api/launch/car` | `{placa, doc, tdoc}` = documento del **propietario**. Reporte: `/api/v2/report_car[_pdf]/{id}` |
| Empresa por NIT | `POST /api/launch/verify/nit` | `{nit, nombre?}`. Reporte: `/api/v2/report_nit[_pdf]/{id}` |
| Lotes | `/api/v1/batches/launch`, `/progress/{job_id}`, `/result/{batch_id}` | Máx 3 lotes simultáneos por usuario |
| Cuenta | `GET /api/plans`, `GET /api/querys` | Plan/consumo y nº de consultas |
| Webhooks | `/api/v1/webhook/*` | Evitan el polling (no implementados) |

## 2. Credenciales (¿de dónde salen?)

No vienen en la documentación pública. Son el **usuario/contraseña de la
cuenta del dashboard** (`https://dash-board.tusdatos.co`):

1. Si Integra ya tiene cuenta en TusDatos → esas son.
2. Si no: registrarse en [www.tusdatos.co](https://www.tusdatos.co) (tienen
   demo gratuita con crédito de consultas).
3. El ambiente de pruebas (sandbox) usa el mismo host con cuentas que
   entrega su soporte/comercial ([centro de ayuda](https://tusdatos-ayuda.freshdesk.com)).

## 3. Configuración y arranque

Variables en `integrappi/.env`:

| Variable | Default | Descripción |
|---|---|---|
| `TUSDATOS_USER` | — | Email de la cuenta TusDatos (obligatoria) |
| `TUSDATOS_PASS` | — | Password de la cuenta (obligatoria) |
| `TUSDATOS_BASE_URL` | `https://dash-board.tusdatos.co` | Host de la API |

Sin credenciales el backend arranca normal; los endpoints `/tusdatos/*`
responden 503 con un mensaje claro.

```powershell
cd integrappi
uvicorn main:app --reload --port 8000
# Documentación: http://127.0.0.1:8000/docs  (sección "TusDatos")
```

⚠️ En esta máquina el puerto 8000 suele estar ocupado por otro proceso
python; si no sube, usar `--port 8001`.

## 4. Endpoints del wrapper (prefijo /tusdatos)

| Método | Ruta | Hace |
|---|---|---|
| GET | `/tusdatos/health` | Credenciales + conectividad (consulta el plan) |
| POST | `/tusdatos/consultas/lanzar` | Inicia consulta → `jobid` |
| GET | `/tusdatos/consultas/{jobid}` | Estado/resultado del sondeo |
| POST | `/tusdatos/consultas/completa` | **Lanza + espera el final** (bloquea ~1 min) |
| POST | `/tusdatos/vehiculos/completa` | Consulta de vehículo + espera |
| POST | `/tusdatos/nit/verificar` | Validación de empresa por NIT |
| GET | `/tusdatos/reportes/{id}/json` · `/html` · `/pdf` · `/pdf-nit` | Reportes por `id` de consulta |
| GET | `/tusdatos/reportes/{id}/reintentar?typedoc=CC` | Relanza fuentes en error |
| GET | `/tusdatos/plan` · `/tusdatos/historial` | Plan/consumo de la cuenta |
| POST | `/tusdatos/token` | Crea API token Bearer `api_td_...` (se muestra 1 vez) |
| ANY | `/tusdatos/proxy/{ruta}` | Passthrough crudo (⚠️ `/api/fuentes-colombia` ya no existe upstream — responde el HTML del SPA; ver §6) |

### Ejemplo rápido (persona)

```bash
# Todo en una sola llamada (espera hasta 5 min por defecto)
curl -X POST http://127.0.0.1:8000/tusdatos/consultas/completa \
  -H "Content-Type: application/json" \
  -d '{"typedoc":"CC","doc":"1234567890"}'

# Reporte final (id viene en resultado.id cuando finalizado)
curl http://127.0.0.1:8000/tusdatos/reportes/<id>/pdf -o reporte.pdf
```

## 5. Estado y hallazgos de la prueba real (2026-09-24)

**Probado end-to-end** con la cuenta de Integra (edwin.zarate@…, saldo
2896 a $12.480/consulta según `price_per_check` de `/api/plans`):

- CC 1010…3062 → `validado: true`, ~60 fuentes, `hallazgo: false`,
  categoría `bajo`. Consulta upstream: 42 s; total con polling: 46 s.
- Reporte PDF descargado por el wrapper sin costo adicional (2.5 MB).
- 8 fuentes quedaron en `Error` (simit, ruaf, rut, rut_estado, quibdojepms,
  colpsic, anec, cpbiol) — portales caídos/ lentos del momento; se relanzan
  con `/tusdatos/reportes/{id}/reintentar`.

**⚠️ Detalle técnico importante**: el servidor real de TusDatos responde
`308 Redirect` hacia los endpoints con barra final (`/api/plans` →
`/api/plans/`). El httpx del router usa `follow_redirects=True`; sin eso
llega HTML ("Redirecting...") y el `.json()` explota. Si algún día se
reescribe el cliente, no olvidar ese flag.

## 6. Consumidor real: los estudios automáticos de En Ruta (2026-09-28)

El wrapper dejó de ser solo experimental: **`Funciones/estudios_automaticos.py`
(En Ruta) consume los HANDLERS como funciones** (no por HTTP) para los
estudios de seguridad automáticos de los vehículos — cédulas deduplicadas de
conductor/tenedor/propietario/dueño de remolque + placa + empresas por NIT,
con vigencia (`ESTUDIOS_VIGENCIA_MESES`), **actualización MANUAL**
(`ESTUDIOS_AUTO_RENOVAR` y el barrido automático fueron ELIMINADOS el
2026-10-05; hoy se actualizan desde el módulo «Estudios por antigüedad» de
/revision o con «Volver a consultar»), un SWITCH pausable
(`config_estudios.auto_disparo` + env `ESTUDIOS_AUTO_DISPARAR=false`) para
el disparo al pasar a revisión, reutilización entre vehículos y PDFs
archivados en el bucket. Detalle completo en
[`EN_RUTA.md`](EN_RUTA.md) (bitácoras 2026-09-28 y 2026-10-05). Handlers usados:
`consulta_completa`, `consulta_vehiculo`, `verificar_nit`, `reporte_pdf`,
`reporte_nit_pdf`, `reintentar_fuentes`, `_esperar`, `_configurado`.

### Hallazgos del uso real (2026-09-28, calibrado con MVX48E)

- **`results` tiene MÁS estados que el `true|false|"Error"` documentado**
  (verificado en los datos crudos guardados): `''` (cadena vacía) = la fuente
  **NO APLICO** a esa consulta (no es fallo; p. ej. fuentes que no corren
  para ese tipo de búsqueda) y `'Página no disponible'` = caída del portal
  (variante de Error). Clasificación en 4 estados en /revision: ✔ hallazgo ·
  ✖ limpia · ⚠ caída (reintentable) · ⊘ no aplicó.
- **`force` importa**: sin `force:true`, relanzar la misma cédula devuelve
  el resultado CACHEADO del proveedor (no renueva nada). La actualización
  manual («Actualizar estudios» del módulo de antigüedad y «Volver a
  consultar») va con force (re-consulta real, se cobra).
- **`launch/car` exige placa de EXACTAMENTE 6 caracteres** (el registro de
  En Ruta acepta 4-7 → pre-validar; 7 chars = `placa_invalida`) y el `tdoc`
  del propietario debe derivarse del RUT (NIT→NIT, CE→CE, PP→PP, TI→TI,
  default CC) — con CC fijo sobre un propietario empresa el upstream
  rechaza el lanzamiento ("TusDatos no entregó jobid").
- **Fallo de lanzamiento transitorio**: `{'error': 'Se presentó una falla
  iniciando la consulta, realice la consulta nuevamente.'}` → un reintento
  tras ~5 s suele resolverlo.
- **`fechaE` opcional para CC sí ayuda**: afina el match en cédulas comunes
  (viaja dd/mm/yyyy desde `condFechaExpedicion`).
- **`GET /api/fuentes-colombia` YA NO EXISTE**: responde el HTML del SPA.
  El catálogo real sale de la unión de `results` guardados (50 fuentes de
  persona + 19 de vehículo a 2026-09-28; ver unión en la conversación/
  exportable desde Mongo).
- Catálogo de fuentes visto en producción: persona = SIMIT, Procuraduría,
  Contraloría, Policía, RUNT, RNDC, RUES, RUT, RUAF, SISBEN, SECOP 1/2,
  PEPs, OFAC/Clinton (doc y nombre), ONU, INTERPOL, EUROPOL, INPEC,
  SICAAC, SIRNA, SIGEP, FOPEP, IADB, Banco Mundial, Offshore Leaks,
  Defunciones, Libreta Militar, Delitos sexuales, SISCONMP, Insolvencias,
  Juzgados Tyba, RGM, RNMC, RUI, SIMUR, Personería Bogotá, Analisis
  Reputacional, Asociaciones/Sancionados varios…; vehículo = RUNT, RNDC
  (matrícula), SIMIT, SIMUR, Impuestos vehiculares por departamento (13),
  Omisiones, Accidentalidad Bogotá, Movilidad Bogotá.

## 8. NIT por el endpoint GENERAL (2026-11-19, fix del 403)

**El servicio dedicado `/api/launch/verify/nit` responde 403 `{'error': 'No cuentas con acceso a este servicio', 'status': False}` para la cuenta de Integra** — producto no habilitado, con saldo disponible y cumpliendo el contrato publicado en docs.tusdatos.co (Basic Auth + `{"nit": <num>}`; verificado en vivo con curl). Pendiente pedir la habilitación a sac@tusdatos.co.

**Solución aplicada**: los estudios de empresa (propietario/tenedor NIT en `estudios_automaticos._invocar_handler`) ahora lanzan por el **endpoint general** `POST /api/launch` con `{"typedoc": "NIT", "doc": <nit>}` — mismo flujo que las personas, soporta `force`, y SÍ funciona con las credenciales actuales. Caso real verificado end-to-end con 830503352 (HB INVERSIONES SAS): finalizado, hallazgo alto (RUES/RGM/RNBD/Supersociedades/Análisis Reputacional sobre 30 fuentes de empresa) y **PDF de 1.3 MB por `report_nit_pdf`** (⚠️ `report_pdf` responde 410 «identificador inválido» para estos ids — mismo patrón que los ids de vehículo).

Otros datos del diagnóstico: el catálogo `/api/v1/checks?country=CO&document_type=NIT` mostró consultas NIT exitosas de otro usuario de la cuenta (`aux.4torredecontrol@gmail.com`), también por la vía general.

## 7. Pendientes

- Lotes (`batches`) y webhooks disponibles vía `/tusdatos/proxy` hasta que
  se tipifiquen, si hacen falta.
- ~~Probar vehículo y NIT cuando haya un caso real~~ ✔ probados en
  producción por los estudios automáticos (2026-09-28).
- Si alguna fuente de TusDatos se quiere productivizar, evaluar si se
  consume por esta API o se implementa con bot propio como las demás
  fuentes de `/seguridad` (costo por consulta vs. desarrollo) — plan
  migración: seguriDatia reemplazará este consumo fuente por fuente.
