# Módulo Financiero (schema `fin`)

Réplica interna del módulo financiero externo (`ModuloFinanciero-api` +
`ModuloFinanciero-front`, conservados como referencia). Vive DENTRO de
integrappi (una sola factura de Render) en un **schema propio de PostgreSQL
`fin`** — nada toca Mongo ni `informe_guias_tms`.

## Mapa rápido

| Pieza | Dónde |
|---|---|
| Modelos (14 tablas schema fin) | `financiero/models/` |
| Motor SQLAlchemy (perezoso, pool 2+3) | `financiero/database.py` |
| Migraciones (instancia dedicada) | `fin_alembic/` + `fin_alembic.ini` |
| Auth (Bearer, sesión 12 h) | `financiero/dependencies.py`, `rutas/fin_auth.py` |
| Routers | `rutas/fin_*.py` (prefix `/fin/...`) |
| Servicios | `financiero/services/` (máquinas de estado, consecutivos, locking, DIAN, correos…) |
| Siembra de catálogos | `scripts/fin_semilla.py` |
| Tests | `tests/fin/` (86; SQLite con `ATTACH AS fin`) |

## Autenticación

No hay login propio: el módulo acepta dos Bearer tokens firmados con el
`JWT_SECRET` de integrappi:

1. **El del login de IntegrApp** (`auth_source=baseusuarios`, sin claim
   `modulo`): la dependencia resuelve el `sub` (ObjectId) a correo leyendo
   Mongo `baseusuarios` directamente.
2. **El token de sesión del módulo** (`auth_source=fin_modulo`, 12 h, sub =
   correo) que emite `POST /fin/auth/sesion` — lo canjea el front al entrar
   para no re-loguear cada 20 min.

El **rol lo define `fin.usuarios`** (por correo, sin claves). Desactivar a
alguien corta su acceso al instante (se revalida `activo` en cada request).

### Alta y gobierno de usuarios (2026-11-24, post-F6)

- **Candidatos**: `GET /fin/usuarios/candidatos?q=` lista usuarios de
  IntegrApp con perfil FINANCIERO (Mongo `baseusuarios`, activos, con correo);
  el formulario autocompleta desde ahí y el correo no se escribe a mano.
- **Nombre y username derivados**: el nombre se lee de `baseusuarios` por
  correo (`_nombre_desde_baseusuarios`, se resincroniza en cada PUT) y el
  username se deriva del correo (`_derivar_username`). Nadie los escribe.
- **Permisos finos por usuario**: `fin.usuarios.permisos` (JSONB, migración
  0003) guarda solo los overrides `{seccion: bool}` sobre
  `DEFAULT_SECCIONES_POR_ROL`; la dependencia `require_seccion(seccion)`
  (aplicada a los GET de los 7 módulos) devuelve 403 si la persona no tiene
  la sección. El admin ve todo y no se recorta.
- **Gobierno**: el rol `admin` no se ofrece en pantalla. Solo el
  Administrador Principal (rol admin) puede otorgar la casilla
  `es_admin_adicional`, crear/ascender otros admin y cambiar el rol de un
  admin (403 para el admin adicional). No puede eliminarse a sí mismo ni
  borrar al último admin activo.
- **Perfil FINANCIERO de IntegrApp**: aterriza en `/EleccionFinanciero`
  (elige Otros Costos ⬅➡ Módulo Financiero) y `/Modulos` redirige a los
  perfiles micro-portal a su hogar (guardia en `ModulosP`, front).
- **WhatsApp de Otros Costos**: campo `notificar_otros_costos` (bool, default
  true) en `baseusuarios`; `_enviar_a_perfil` (otros_costos.py) saltea a
  quien lo tiene en False. Checkbox en GestionUsuarios → editar usuario.

## Flujos

- **Requisiciones**: radicación (jefe de área → compras → financiera) →
  **OC automática `AAAA-386+`** → entrega (compras) → recepción con
  **calificación de proveedor** (9 criterios 0-15, A/B/C). Pago urgente
  propuesto por compras, decidido por la financiera (≤ tope Controller /
  > tope Gerente — tope en `fin.configuracion`).
- **Pagos urgentes**: fila en `fin.pagos_urgentes` al aprobarse una req con
  pago urgente; Tesorería paga (referencia) o devuelve (motivo).
- **Traslados**: jefe de área → **Presidencia** aprueba/rechaza.
- **Viajes**: solicitar → autorizar (controller/gerente/presidencia/
  autorizador) → presupuesto **A/B/C confidencial** (enmascarada en el
  serializer) → tiquetes (compras) → anticipo (tesorería) → legalizar → cerrar.
- **Facturas DIAN**: carga Excel del portal o ZIP con XML UBL (+PDF → adjunto
  GCS); dedup por CUFE y NIT+número; se excluyen acuses y las facturas propias
  (NIT 900164363). Tramitar → radicar (OC) → **causar** (asiento
  gasto+IVA vs CxP que cuadra) · glosas con motivo.
- **Servicios públicos** y **cajas menores**: por inmueble/sede y periodo;
  contabilizar caja genera asiento cuadrado.

Todo: bloqueo optimista (`version` → 409), historial por documento, auditoría
inmutable (`fin.auditoria_eventos`) y correos Resend fire-and-forget al
responsable del siguiente paso.

## Infra que reutiliza

- **Postgres**: mismas `PG_*` que `bd/bd_postgres.py` (SSL require).
- **Adjuntos**: GCS bucket privado `integrapp-privado`, carpeta
  `Financiero/{modulo}/{referencia}/`, descarga por URL firmada v4 (JSON).
- **Correos**: Resend (`RESEND_API_KEY`, `MAIL_FROM`), whitelist
  `FIN_DOMINIOS_CORREO`.
- Sin Redis ni WebSockets: el front refresca por polling (25 s).

## Operación

```bash
# migrar (primera vez y cada deploy que traiga migraciones)
alembic -c fin_alembic.ini upgrade head

# sembrar catálogos + primer admin (idempotente)
python scripts/fin_semilla.py --admin edwin.zarate@integralogistica.com

# tests
python -m pytest tests/fin -q
```

- Salud: `GET /fin/salud` (schema, tablas, RSS, configuración).
- Backup: `GET /fin/respaldo` (JSON completo; restauración solo simulación).
- Env opcionales: `FIN_BUCKET`, `FIN_DB_SCHEMA`, `FIN_SESION_HORAS`,
  `FIN_DOMINIOS_CORREO`, `FIN_DB_POOL_SIZE`, `PG_SSLMODE`.

## Front

`integrapp-next` → `/Financiero/*` (9 rutas): Inicio (indicadores por rol),
Traslados, Viajes, Requisiciones, Facturas DIAN, Pagos urgentes, Servicios,
Cajas y Administración. Código en `src/Paginas/FinancieroP/` (núcleo +
pestañas), capa API `src/Funciones/ApiPedidos/financiero.ts`, entrada por el
grupo FINANZAS de `/Modulos`. El perfil FINANCIERO de IntegrApp NO se tocó
(sigue aterrizando en Otros Costos).
