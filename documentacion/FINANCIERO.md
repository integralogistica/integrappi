# FINANCIERO — Módulo Financiero interno (schema `fin`)

Réplica del módulo financiero externo (`ModuloFinanciero-api` +
`ModuloFinanciero-front`, que se conservan intactos como referencia) **dentro
de la app IntegrApp**: un solo servicio de Render, un solo deploy de cPanel,
sin servicios externos nuevos. Vive en un **schema propio de PostgreSQL
(`fin`)** de la base que ya usa integrappi — nada toca Mongo, ni
`informe_guias_tms`, ni ninguna colección/tabla existente (principio del
proyecto: cambios 100 % aditivos).

- **Backend**: paquete `integrappi/financiero/` + routers
  `integrappi/rutas/fin_*.py` (prefijo `/fin`, ~132 rutas), montados en
  `integrappi/main.py` como bloque único al final.
- **Frontend**: `integrapp-next/src/Paginas/FinancieroP/` + wrappers
  `src/app/Financiero/*/page.tsx` (10 rutas estáticas). Entrada por el grupo
  **FINANZAS** de `/Modulos` (`src/Funciones/menuModulos.ts`).
- **Migraciones**: instancia Alembic dedicada `integrappi/fin_alembic/`
  (`fin_alembic.ini` en la raíz de integrappi) — solo conoce el schema `fin`
  y usa tabla de versiones propia (`fin.alembic_version_fin`). Cadena:
  `0001` inicial → `0002` contabilidad → `0003` permisos → `0004` terceros +
  adjuntos tipados → `0005` cajas/dian (maestro de sedes, transportadores,
  uploads, token, columnas de validación/cruce/drive) → `0006` pagos/quotes
  (campos bancarios de pagos, categorías/contratos de servicios, tabla
  `quotes` y `cat_contratos_servicios`).
- Documentación técnica interna adicional: `docs/financiero.md` (dentro de
  este mismo repo). Las docs generales del proyecto (SEGURIDAD, INDICADORES,
  EN_RUTA, README, CHANGELOG…) viven en `documentacion/` de este repo.

> El perfil **FINANCIERO de IntegrApp no se tocó**: sigue aterrizando en
> Otros Costos tras el login. El acceso al módulo financiero lo controla él
> mismo, por correo (ver §2).

---

## 1. Qué hace el módulo

| Sub-módulo | Flujo | Quién decide |
|---|---|---|
| **Requisiciones** | Radicación (formulario FORM-CPR-011 completo: autocompletado de proveedor contra catálogo, CEDIs, receptor, fechas, cotizaciones múltiples adjuntas) → jefe de área → compras (cotiza, propone pago urgente) → financiera → **OC automática** `AAAA-386+` → entrega (compras) → recepción con **calificación de proveedor** (9 criterios 0-15, letra A/B/C) | Financiera: ≤ tope ($100M configurables) aprueba Controller; > tope, Gerente financiero |
| **Prov. nuevos** | Nace del check «¿El proveedor es nuevo?» de la requisición (RUT + cert. bancaria + cámara de comercio leídos en el navegador) → bandeja de Compras aprueba/rechaza → Contabilidad marca la creación en Vulcano | Compras → Contabilidad |
| **Traslados** | Jefe de área pide (rubro del catálogo por grupo, mes afectado, **efecto sobre el presupuesto**: disponible = ppto − ejecutado ± aprobados) → **Presidencia** aprueba/rechaza; el efecto se congela al aprobar; formato MOVIMIENTO PRESUPUESTAL imprimible | Presidencia |
| **Viajes** | Solicitar (viajero propio, cargo/categoría del catálogo, resumen de días con festivos Ley Emiliani) → autorizar → **presupuesto de viáticos día×concepto** (matriz con tarifas A/B/C confidenciales, celdas +/−, otros gastos) → tiquetes (compras) → anticipo (tesorería) → legalizar → cerrar | Autorizadores oficiales por paso |
| **Facturas DIAN** | Carga Excel/ZIP con **vista previa por 5 categorías** (nuevas/propias/transportadores/duplicadas/acuses) · validación por área (Compras/Gerencia/Operaciones/General) · rechazo con catálogo + reactivación · **cruce NC↔factura** · trámite enriquecido (categoría de gasto, OC + PDF, obs. recepción) · **Drive Contable** (glosas de catálogo, nº causación, críticas >3 días, termómetro) · cuenta de cobro manual · envío a caja menor · ZIP de soportes · correos «Notificar» por Resend | Contabilidad / Tesorería |
| **Pagos urgentes** | Nace de una requisición con pago urgente (o manual, o de un anticipo interno) → bandeja de Tesorería con **vista día/semana/todos**, búsqueda, banner de vencidos y **datos bancarios heredados del catálogo por NIT** → **pagar** (bloqueado sin el chip «Contabilizada» que conmuta Contabilidad) o **devolver** (6 motivos frecuentes) · reversiones (reabrir/reconsiderar), edición, pago manual, Excel 16 columnas | Tesorería / casilla `es_aprobador_pagos` |
| **Cotizaciones y anticipos** | Anticipo interno con comprobante **EGC/ANT/BG-TR** (proceso Cajas Menores / Viajes / Otros + sede del maestro) y check «Programar también en Pagos Urgentes» (crea el pago) → **cruce con la factura DIAN** del mismo NIT ordenada por cercanía de valor → legalización con recibos → cerrar. Cotización ídem sin comprobante | Tesorería |
| **Cajas menores** | Circuito **FORM-FIN-007** de 6 etapas: borrador → radicar (consecutivo `CM-AAAA-NNN` + política semanal martes→viernes con semáforo) → jefe inmediato → Controller → Contabilidad registra **FC** (asiento por folios causados, auto-avance) → cruce Tesorería (**cuenta 110510**, BG-EF, subcuenta por sede) → **egreso del reembolso** (banco + PDFs). Checkboxes por folio (exclusión en rojo recalcula el total), maestro de sedes oficial, impresión del formato con 4 firmas, KPIs del pipeline | Jefe → Controller → Contabilidad → Tesorería |
| **Servicios públicos** | **Contratos fijos** del maestro (`cat_contratos_servicios`) replicados cada mes con «Generar mes» (fila `NO_GENERADO` hasta la factura, idempotente) · categorías públicos/corporativos/Orión · 6 estados de causación · **vista calendario** mensual con semáforo (VENCIDO/VENCE HOY/PRÓXIMO/A TIEMPO) · KPIs (facturación, progreso, alertas, recepción) · edición en línea por celda · urgencia de recepción ciclada · «👉 Pagar» en el portal · modo Auditoría Calidad (con/sin soporte) · Excel + ZIP | Tesorería / Contabilidad / Calidad (auditoría) |
| **Administración** | Usuarios del módulo (CRUD + Excel), bitácora (+export Excel), edición/reapertura/borrado admin con snapshot, respaldo JSON | Admin del módulo |

Transversales: **bloqueo optimista** (campo `version` → 409 si otro usuario
cambió el registro), **historial de cambios** por documento, **auditoría
inmutable** (`fin.auditoria_eventos`), adjuntos en GCS con hash SHA-256,
**tipados** (`rut`, `cert_bancaria`, `camara_comercio`, `cotizacion`,
`comprobante_fc`, `cruce`, `egreso_auxiliar`, `reembolso`, `soporte_caja`,
`orden_compra`), **impresión de formatos oficiales** (FORM-CPR-011/004 de
requisición y OC, MOVIMIENTO PRESUPUESTAL, FORM-FIN-007, solicitud y
presupuesto de viáticos con firmas progresivas) y **correos Resend** al
responsable del siguiente paso de cada circuito (fire-and-forget).

## 2. Autenticación y usuarios

No hay login propio. La dependencia `financiero/dependencies.py::
get_usuario_fin` acepta dos Bearer tokens, ambos firmados con el
`JWT_SECRET` de integrappi:

1. **El del login de IntegrApp** (`auth_source=baseusuarios`, sin claim
   `modulo` — filtro anti-suplantación): se resuelve el `sub` (ObjectId) a
   **correo** leyendo Mongo `baseusuarios` directamente (sin HTTP).
2. **Token de sesión del módulo** (`auth_source=fin_modulo`, **12 h**, `sub`
   = correo) que emite `POST /fin/auth/sesion`. El front lo canjea al entrar
   (el token de IntegrApp dura 20 min sin refresh).

El **rol lo define `fin.usuarios`**, keyed por correo (lowercase único), sin
claves — la contraseña es la de IntegrApp. 12 roles + 3 casillas
(`es_aprobador_pagos`, `es_admin_adicional`, `es_autorizador_viajes`);
índice unique parcial: **un solo jefe de área activo por área**.
Desactivar a alguien corta su acceso al instante (`activo` se revalida en
cada request). Alta por pantalla (Administración → Usuarios) o siembra.

### Alta de usuarios (simplificada)

- Se elige la **persona** de un autocompletado que solo lista usuarios de
  IntegrApp con **perfil FINANCIERO** (`GET /fin/usuarios/candidatos?q=`);
  marca los ya dados de alta. El correo no se escribe a mano.
- **Ni el nombre ni el usuario se piden**: el nombre se lee de `baseusuarios`
  por correo (y se **resincroniza** en cada edición) y el username se deriva
  del correo.
- **El rol `admin` no aparece en el formulario**: el Administrador Principal
  nace de la semilla o lo nombra otro Principal. Un `admin adicional`
  (casilla) tiene todo el poder operativo pero NO puede crear/ascender/degradar
  administradores ni otorgar la casilla — reglas 403 en el backend.

### Permisos finos por usuario (secciones)

Cada **rol** trae sus secciones por defecto (`DEFAULT_SECCIONES_POR_ROL`:
p. ej. Calidad solo ve Servicios Públicos; Tesorería todo menos traslados).
Sobre eso, `fin.usuarios.permisos` (JSONB) guarda **solo los overrides**
`{seccion: true|false}` de esa persona — se editan con switches en
Administración → Usuarios y el backend los respeta en las lecturas de cada
módulo (`require_seccion` → 403). El admin siempre ve todo. Secciones:
`requisiciones, traslados, viajes, facturas, pagos, servicios, cajas,
administracion, proveedores` (Compras y Contabilidad ven `proveedores`).

### Perfil FINANCIERO de IntegrApp

Tras el login no cae directo en Otros Costos: **elige** en `/EleccionFinanciero`
entre Otros Costos y el Módulo Financiero. Además, `/Modulos` (la Torre de
Control) está **blindado**: los perfiles micro-portal (FINANCIERO,
VISUALIZADOR, CLIENTE, CLIENTE_ESTUDIOS, PADRINO, Seguridad) que entren por
URL se redirigen a su hogar — solo los perfiles multi-módulo ven el menú.

### Notificaciones WhatsApp de Otros Costos

Interruptor por usuario `notificar_otros_costos` en `baseusuarios`
(checkbox en GestionUsuarios → editar): apagado = esa persona no recibe los
WhatsApp de Otros Costos aunque tenga el perfil (FINANCIERO, COORDINADOR,
CONTROL, ANALISTA). No afecta su acceso, solo la notificación.

## 3. Endpoints (resumen)

| Grupo | Rutas |
|---|---|
| Auth | `POST /fin/auth/sesion` · `GET /fin/auth/me` |
| Salud | `GET /fin/salud` (schema, tablas, RSS, config) |
| Requisiciones | `GET/POST /fin/requisiciones` · `GET/PUT /{id}` · `POST /{id}/transicion` (APROBAR/RECHAZAR/ENTREGAR/RECIBIR_TOTAL/ANULAR) · `POST /{id}/marcas` · `GET /formato-cargue` · `POST /importar-lineas` |
| Terceros (Prov. nuevos) | `GET/POST /fin/terceros` · `POST /{id}/aprobar` · `POST /{id}/rechazar` · `POST /{id}/cargado-software` (toggle Vulcano) · `PATCH /{id}` · `DELETE /{id}` (admin, borra adjuntos GCS) |
| Traslados | `GET/POST /fin/traslados` · `GET/PUT /{id}` · `POST /{id}/transicion` (acepta `datos_adicionales.lineas` para congelar el efecto) |
| Viajes | `GET/POST /fin/viajes` · `GET /viajes/autorizadores` · `GET/PUT /{id}` · `POST /{id}/transicion` · `POST /{id}/editar-paso` (presupuesto/tiquetes/anticipo) |
| Facturas DIAN | `GET /fin/contabilidad/facturas` (filtros validada/categoria/estado_contable/caja_menor) · `POST /facturas/previsualizar` (5 categorías sin insertar) · `POST /facturas/cargar` · `POST /{id}/transicion` (TRAMITAR enriquecido/ENVIAR_LEGALIZACION/RADICAR/CAUSAR/GLOSAR/ANULAR) · `POST /{id}/validar|quitar-validacion|rechazar|reactivar` · `GET /{id}/cruce-candidatos` · `POST /{id}/cruce` + `/cruce/deshacer` · `POST /{id}/enviar-caja-menor` · `POST /{id}/novedad` + `/{id}/reabrir` · `POST /facturas/cuenta-cobro` · `GET /facturas/zip-soportes` (backend GCS→ZIP) · CRUD `/transportadores` + `/{id}/archivar-pendientes` · `GET /uploads` · `GET/PUT /token` · `POST /correos/notificar` (Resend) · `GET /torre` · `GET /movimientos` (con cuadre) |
| Pagos urgentes | `GET /fin/pagos` (+q/tipo_doc/desde/hasta/solo_vencidos/orden; devuelve `{pagos, resumen}` con vencidos) · `POST ""` (manual con herencia bancaria del catálogo) · `PUT/{id}` · `DELETE /{id}` (admin) · `POST /{id}/pagar` (bloquea sin contabilizada) · `POST /{id}/rechazar` · `POST /{id}/contabilizada` (toggle) · `POST /{id}/reabrir` · `POST /{id}/reconsiderar` · `GET /motivos-rechazo` (config `pagos_motivos_rechazo`) |
| Cotizaciones y anticipos | `GET/POST /fin/quotes` (+segmento/q; `crear_pago` programa el pago) · `POST /{id}/legalizar` + `/deshacer-legalizacion` · `GET /{id}/candidatos` (por NIT, cercanía de valor) · `POST /{id}/cruzar` + `/descruzar` · `POST /{id}/cerrar` · `DELETE /{id}` |
| Servicios | `GET/POST/PUT /fin/servicios` (+categoria/solo_vencidos/modo_calidad/q; resumen con subtotales y KPIs) · `POST /{id}/transicion` (CAUSAR/PAGAR) · `PATCH /{id}/celda` (edición en línea) · `POST /{id}/urgencia` (cicla) · `POST /{id}/soporte-cargado` (avisa Tesorería) · `POST /generar-periodo?periodo=` (replica contratos, idempotente) · CRUD `/contratos` + `PUT /{id}/propagar?alcance=` · `DELETE /{id}?alcance=mes|definitivo` · `GET /zip-facturas?periodo=` |
| Cajas | `GET/POST /fin/cajas` (+semaforo/q) · `GET /kpis` · `POST /{id}/radicar` (consecutivo CM + política semanal) · `POST /{id}/transicion` (APROBAR_JEFE/APROBAR_CONTROLLER/CONTABILIZAR con FC/CRUZAR con BG-EF/EGRESAR/RECHAZAR/REABRIR) · `PATCH /{id}/items` (checkboxes por folio) · `POST /{id}/vincular-factura` · `DELETE /{id}` (desvincula facturas) |
| Adjuntos | `POST /fin/adjuntos/subir` (+`tipo_documento` tipado) · `GET ?modulo&ref` · `GET /{id}/descargar` (URL firmada v4) · `DELETE /{id}` |
| Catálogos | `GET /fin/catalogos/centros-costo?q` · `/proveedores?q` · `/presupuesto?periodo=` (base del efecto de traslados) · `/personal-viajes?q` · CRUD `/sedes-cajas` (maestro FORM-FIN-007, admin) |
| Configuración | `GET /fin/configuracion[/{clave}|/_estado/todo]` · `PUT /{clave}` (admin, 409 por versión) |
| Usuarios | `GET/POST/PUT/DELETE /fin/usuarios[/{id}]` · `GET /usuarios/candidatos?q=` · `POST /importar/vista-previa` · `POST /importar/confirmar` |
| Bitácora | `GET /fin/auditoria` (filtros + paginación) · `GET /exportar` (Excel) |
| Admin registros | `PATCH /fin/admin/{modulo}/{id}` (requisiciones, traslados, viajes, cajas, facturas) · `POST .../reabrir` · `DELETE .../{id}?motivo` (snapshot en bitácora; cajas desvincula facturas) |
| Respaldo | `GET /fin/respaldo` (JSON) · `POST /fin/respaldo/restaurar` (solo simulación) |

## 4. Datos (schema `fin`)

- **Flujo**: `usuarios`, `consecutivos` (tipos req/oc/tr/vj/**cm**; OC arranca
  en **386** — la 385 fue la última manual; formato `AAAA-386`),
  `requisiciones`, `terceros_compras` (proveedores nuevos con aprobación de
  Compras y check Vulcano), `traslados`, `viajes` (JSONB en items/líneas/
  historial; `version`).
- **Contabilidad normalizada**: `facturas_dian` (+ columnas de validación por
  área, rechazo, cruce, trámite enriquecido y drive contable),
  `pagos_urgentes` (+ datos bancarios/identificación, `contabilizada`,
  `origen` requisicion/manual/anticipo — `requisicion_id` nullable),
  `quotes` (cotizaciones y anticipos internos con factura y pago asociados),
  `servicios_publicos` (+ categoría/contrato/NIT/día de corte/URLs, 6 estados),
  `cajas_menores` (consecutivo CM, política semanal, folios con checkboxes),
  `movimientos_contables`.
- **Soporte**: `adjuntos` (sha256 + ruta GCS + `tipo_documento`),
  `auditoria_eventos` (inmutable), `configuracion` (áreas, `limite_controller`,
  tarifas de viajes, autorizadores, festivos, correos CC).
- **Catálogos sembrados**: `cat_centros_costo` (~49k), `cat_proveedores`
  (~2.7k), `cat_presupuesto_responsables`, `cat_personal_viajes`,
  `cat_sedes_cm` (9 sedes oficiales de caja menor con responsable/cédula/
  jefe/CCO/subcuenta 110510xx), `cat_contratos_servicios` (maestro de
  contratos fijos que alimenta «Generar mes»).
- **DIAN**: `transportadores_omitidos`, `dian_uploads` (histórico de cargas),
  `dian_token` (último token del portal).

Concurrencia: `SELECT ... FOR UPDATE` + `version` en cada transición, y
consecutivos atómicos `INSERT ... ON CONFLICT DO UPDATE ... RETURNING`
(imposible duplicar). Enums como `String` + CHECK (añadir estados no exige
`ALTER TYPE`).

## 5. Infraestructura que reutiliza

| Qué | Cómo |
|---|---|
| Postgres | Mismas `PG_*` de `bd/bd_postgres.py`; motor SQLAlchemy **perezoso** (`financiero/database.py`, pool 2+3, `PG_SSLMODE=require`) — si falta la config, solo fallan las rutas `/fin/*` |
| Adjuntos | GCS bucket **privado** `integrapp-privado`, carpeta `Financiero/{modulo}/{referencia}/`, descarga por URL firmada v4 de 15 min |
| Correos | Resend (`RESEND_API_KEY`, `MAIL_FROM` ya existentes) con whitelist `FIN_DOMINIOS_CORREO` |
| Real-time | **Ninguno**: polling 25 s + refetch tras mutación (worker único del plan starter) |

Variables nuevas: **ninguna obligatoria**. Opcionales: `FIN_BUCKET`,
`FIN_DB_SCHEMA`, `FIN_SESION_HORAS`, `FIN_DOMINIOS_CORREO`,
`FIN_DB_POOL_SIZE`, `FIN_ADJUNTO_MAX_MB`, `PG_SSLMODE`.

## 6. Front (`/Financiero/*`)

10 rutas estáticas (`output: 'export'`): **Inicio** (Torre de control:
compromisos del día, agenda semanal Lun-Sáb, accesos rápidos, últimas cargas
DIAN), Traslados, Viajes, Requisiciones, **Prov. nuevos**, Facturas DIAN
(6 pestañas: Pendientes / Tramitadas / Legalización / Drive Contable /
Indicadores / Cargar), Pagos urgentes (bandeja día/semana + pestaña interna
**Cotizaciones y anticipos**), Servicios públicos (tabla/**calendario** con
KPIs y contratos), Cajas menores (circuito FORM-FIN-007 con KPIs del
pipeline), Administración (solo admin).
Más `/EleccionFinanciero` (elección de destino del perfil FINANCIERO).

- Núcleo `Paginas/FinancieroP/nucleo/`: sesión (canje token → `finAccessToken`
  12 h en localStorage; 401 → login IntegrApp), `useFinDatos` (polling),
  componentes `fin-*` (header claro + filo lima, SweetAlert2 vía
  `nucleo/dialogos.ts`). Modales con scroll interno y pie fijo.
- **Utilidades** en `Paginas/FinancieroP/utiles/`: impresión de formatos
  oficiales (`imprimirRequisicion` FORM-CPR-011/004, `imprimirTraslado`,
  `imprimirViaje`, `imprimirCajaMenor` FORM-FIN-007), cálculos puros
  (`presupuestoTraslados`, `viajes/festivos` Ley Emiliani,
  `viajes/presupuesto` de viáticos, `parsearPdfTercero` — lectura client-side
  del RUT y la certificación bancaria).
- Capa API: `src/Funciones/ApiPedidos/financiero.ts` (Bearer; exportes y
  respaldo por fetch+blob porque el `<a>` no lleva Authorization).
- Los permisos del front son solo UX: **el backend nunca confía en el rol que
  muestra la pantalla** — revalida contra `fin.usuarios` en cada request.

## 7. Operación y deploy

```bash
# 1) Backend: push a Render y luego (BD de Render, mismas PG_*)
cd integrappi
alembic -c fin_alembic.ini upgrade head          # migraciones 0001..0006
python scripts/fin_semilla.py --admin edwin.zarate@integralogistica.com

# 2) Front: npm run build y subir out/ a cPanel (public_html/integrapp/)

# 3) Tests locales (SQLite, sin Postgres ni Docker)
python -m pytest tests/fin -q                   # 137 tests
```

Smoke post-deploy: login normal → `/Modulos` → FINANZAS → Inicio (Torre de
control); `GET /fin/salud` → `OPERACIONAL`; regresión de una página existente
(Otros Costos / indicadores). **Rollback**: revert del deploy de Render (el
schema `fin` queda inerte, no molesta) + restaurar la carpeta previa de
cPanel.

## 8. Decisiones de diseño (y por qué)

| Decisión | Alternativa descartada |
|---|---|
| SQLAlchemy ORM + Alembic dedicado | psycopg2 crudo: habría que reescribir a mano FOR UPDATE/409/ON CONFLICT — el mayor riesgo, justo en lo financiero |
| Contabilidad en tablas normalizadas | La fila JSONB única del original serializaba TODA la contabilidad con un solo `version` (409 por cualquier escritura simultánea) |
| Sesión de módulo 12 h | Re-login cada 20 min (token IntegrApp sin refresh) — inaceptable para un aprobador |
| Usuarios propios por correo | Meter roles financieros en `baseusuarios` contaminaría el login general |
| Polling | WebSocket/SSE: cada conexión ocuparía un hilo del worker único (512 MB starter) |
| Notificaciones por Resend | Teams webhooks del original: un solo canal de correo ya existente, sin variables nuevas |
| Estados de caja en String+CHECK recreable | Enum nativo de PG: añadir etapas exigiría ALTER TYPE |
| ZIP de soportes en backend | El front (static export) no puede descargar todos los blobs privados de GCS |

## 9. Referencia de repos externos

`ModuloFinanciero-api/` y `ModuloFinanciero-front/` **no se borran**: son la
referencia viva. Cada archivo portado lleva un comentario
`# puerto de app/...` indicando su origen. Notas históricas del proyecto en
la memoria de sesiones (`2026-11-24_modulo_financiero_dentro_integrapp.md`,
`2026-11-26_paridad_*.md`).

**Paridad con el original** (nov-2026): **los 8 módulos están a la par** —
Requisiciones, Prov. nuevos, Traslados, Viajes, Cajas menores, Facturas DIAN,
Pagos urgentes (+ Cotizaciones/Anticipos) y Servicios públicos, todos los
flujos de negocio completos y con tests. Piezas secundarias aprobadas pero
PENDIENTES de la tanda de cierre: Auto-Sync OneDrive (vigilancia de carpeta
de ZIPs), Excel del Drive + informe de corte mensual + Ficha Ejecutiva,
correo «Enviar a Aprobación (Gerencia)» en Pagos, botón de vincular factura
DIAN en la bandeja de Cajas, Excel de caja menor con plantilla, indicadores
de la rama general, corte mensual/actividad en vivo de la Torre, y las
secciones de Administración (correos por proceso, período contable, PIN,
bóveda histórica, diagnóstico, restauración de respaldo).
