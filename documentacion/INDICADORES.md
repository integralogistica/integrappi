# Módulos de Indicadores

Documentación de los dashboards de indicadores de **IntegrApp**: arquitectura, módulos
existentes, patrones compartidos y guía para agregar uno nuevo.

URL base (producción): `https://integralogistica.com/integrapp/indicadores`
Local: `http://localhost:3000/integrapp/indicadores`

---

## 1. Visión general

Cada módulo de indicadores es un dashboard independiente que cuelga del menú
`/integrapp/indicadores`. Hoy existen **dos** módulos en ese menú:

| Módulo | Ruta | Datos | Foco |
|---|---|---|---|
| **Clientes** | `/indicadores/clientes` | Por cliente: PostgreSQL o MongoDB | Panel con logo por cliente; cada uno lleva SU dashboard con SU fuente de cajas/pedidos |
| **Costo de Operación** | `/indicadores/costo-operacion` | MongoDB (3 colecciones) | Costo total de la operación (media milla + última milla + otros costos), sobrecosto/ahorro divergente, cajas por etapa y costo por caja con reglas por cliente |
| **Mis Indicadores** | `/indicadores/mis-clientes` | Mongo (`clientes_siscore` + asignación) | Portal de CLIENTES EXTERNOS (perfil `CLIENTE`): solo los clientes asignados al usuario, por ahora On Time + informe de guías |

> ⚠️ Módulos **eliminados** (2026-08-19): **Fletes** (`/indicadores/fletes`) y
> **OT/Transporte** (`/indicadores/ot`, antes `/indicadores/transporte`), ambos frontend y
> backend. Del helper de Fletes, `_expr_clientes_expandidos` migró a
> `indicadores_costo_operacion.py`; el router de OT queda en el historial de git. No volver
> a crear rutas con esos nombres.

> Existe además **`/indicadoresfmc`** (`app/indicadoresfmc/page.tsx` → `Paginas/Indicadores-fmc`),
> un módulo FMC **aparte**, legacy y fuera del menú `/indicadores`. No se documenta en detalle aquí.

El menú lista los módulos y enruta a cada uno:

- **Frontend** (Next.js 14, app router): `integrapp-next/`
- **Backend** (FastAPI): `integrappi/` — un router por módulo

---

## 2. Arquitectura

```
/integrapp/indicadores                        ← menú (tarjetas)
└─ /indicadores/<modulo>                      ← route page (wrapper)
   └─ Paginas/Indicadores<Modulo>/index.tsx   ← componente (gráficos recharts)
      └─ fetch → FastAPI  /indicadores-<modulo>/...   ← agregaciones
```

**Frontend (Next app router)**
- Menú: `src/app/indicadores/page.tsx` — array `indicadores` con `{ id, titulo, descripcion, icono, ruta }`.
- Cada módulo tiene un **route page** delgado que importa el componente con `dynamic(..., { ssr:false })`
  (render 100% cliente → funciona con `output: 'export'` para despliegue estático).
- El componente vive en `src/Paginas/Indicadores<Modulo>/index.tsx`.
- Gráficos: **`recharts`** (`BarChart`, `LineChart`, `PieChart`). Fechas: `date-fns` con locale `es`.
- Auth: se lee de cookies (`usuarioPedidosCookie`, `perfilPedidosCookie`, `regionalPedidosCookie`).
- API base: `process.env.NEXT_PUBLIC_API_BASE_URL`.

**Backend (FastAPI)**
- Un router por módulo en `integrappi/rutas/indicadores_<modulo>.py`, con `prefix="/indicadores-<modulo>"`.
- Se registra en `integrappi/main.py` (import + `app.include_router(...)`).
- Convención de respuesta: `{ "success": true, "data": {...} }` (igual que `/indicadores-costo-operacion/resumen`).

---

## 3. Módulo Indicadores por Cliente (`/indicadores/clientes`)

> Creado 2026-08-19 sobre las cenizas del módulo OT/Transporte (eliminado el mismo
> día). **La idea:** cada cliente tiene SU dashboard con SU fuente de datos —
> algunos clientes tomarán cajas/pedidos de PostgreSQL (`informe_guias_tms`, lo que
> usaba OT) y otros de las colecciones MongoDB de Costo de Operación
> (`pedidos_completados` / `pedidos_medical_historico` / `historico_otros_costos`).

**Flujo:**
```
/indicadores/clientes                      ← panel: tarjetas con logo por cliente
└─ /indicadores/clientes/<cliente-id>      ← dashboard del cliente (por ahora shell con título)
```

**Registro de clientes — `src/Paginas/IndicadoresClientes/clientes.ts`** (ÚNICO sitio
para dar de alta clientes): array `{ id, nombre, logo (URL), descripcion, fuente }`.
`fuente` es documentación por ahora (`'postgres'` | `'mongo'`). Para agregar un
cliente: entrada en el array + route page `src/app/indicadores/clientes/<id>/page.tsx`
con `<DashboardCliente clienteId="<id>" />`.

**Logos:** URLs públicas del bucket GCS `gs://integrapp/Imagenes/...` (p. ej.
`logo medical.svg`, `logo kabi.png` — ojo, kabi es **.png**). Se cargan con
`next/image` + `unoptimized` (export estático).

**Archivos**
- Panel: `src/app/indicadores/clientes/page.tsx` → `src/Paginas/IndicadoresClientes/PanelClientes/`
  (estilos propios `PC-*`, mismo look que el chrome IG-*).
- Dashboard por cliente: `src/Paginas/IndicadoresClientes/DashboardCliente/index.tsx`
  (+ `estilos.css` propios `DC-*`) — recibe `clienteId`; header con el **nombre** del
  cliente (2026-08-21: se quitó el logo del header; el logo sigue en las tarjetas del panel),
  filtros Año/Mes y gráficos del cliente.
- Backend: `integrappi/rutas/indicadores_cliente.py` (`prefix=/indicadores-cliente`).
  **Registro espejo `CLIENTES`** (mantener sincronizado con `clientes.ts`): cada
  cliente define su `$match` de detección por fuente (Kabi: `nit_cliente` en media milla)
  y **`pg_nombres`** — LISTA de valores exactos de `nombre_cliente` en
  `informe_guias_tms` para el informe de guías (2026-10-29: el tablero Kabi UNE los
  nombres con los que el TMS lo registra: `'FRESENIUS KABI'`, `'FK SERVICIO TECNICO'`
  y `'FRESENIUSKABISERVICIOTECNICO'` concatenado en registros viejos; el filtro es
  `nombre_cliente = ANY(%s)`, que sigue usando `idx_guias_cliente_fecha`) y
  **`pg_grupos`** — sub-grupos filtrables del informe (dropdown «Cliente» junto a
  Mes; etiqueta → sub-lista de nombres; `/guias` y `/guias/filas` aceptan
  `?grupo=<etiqueta>` y devuelven las etiquetas en `data.grupos`; 422 si es
  desconocido). El grupo solo viaja en las consultas de guías (cajas/uso son por
  NIT en Mongo, no aplica) y tiene miga propia (`👥 etiqueta ✕`).

**Endpoints**
- `GET /indicadores-cliente/{cliente_id}/cajas?anio=&mes=` — cantidad de **cajas por
  período (mensual+diaria)** desde la **media milla** (`pedidos_completados`). Misma
  lógica que la etapa media milla de Costo de Operación: filtro por `fecha_creacion`
  (string local), **dedup por `consecutivo_vehiculo`** (cajas del vehículo duplicadas
  por doc) y suma de `total_cajas_vehiculo`. Devuelve además `vehiculos` por período
  y `anios` disponibles. El filtro de cliente va **antes** del dedup.
- `GET /indicadores-cliente/{cliente_id}/uso-vehiculos?anio=&mes=&q=` (2026-09-18) —
  **% de uso de los Veh Solicitados**, UNA FILA POR VEHÍCULO (`consecutivo_vehiculo`,
  misma base/dedup que `/cajas`): `{fecha, tipo_solicitado, kg_reales, destino,
  categoria_real, uso_pct, costo_vehiculo (Total Solicitado: `total_flete_vehiculo`,
  con `$max` de `costo_real_vehiculo` por si un flujo lo guardó así), costo_teorico
  (`costo_teorico_vehiculo`), sobrecosto (`diferencia_flete` = real − teórico;
  >0 rojo / <0 verde ahorro), pedidos[]}`. **`uso_pct` = kg reales (`total_kilos_vehiculo`)
  ÷ tope de la categoría del tipo solicitado** (`tipo_vehiculo_sicetac`, split por `_`):
  CARRY 1.000 · NHR 2.300 · TURBO 4.500 · NIES 6.100 · SENCILLO 9.000 · PATINETA
  17.000 · TRACTOMULA **34.000** (sin tope natural → máxima capacidad legal como
  referencia). `categoria_real` = categoría que corresponde por los kg reales (misma
  tabla). `uso_pct` puede superar 100 (viajaron más kg de los que el tipo admite);
  tipo vacío/desconocido → `SIN TIPO` con uso null.
  `pedidos[]` — el desglose por pedido del vehículo (lo del "+" de PedidosCompletados):
  `{pedido (consecutivo_integrapp), pedido_vulcano (numero_pedido — así se guarda en
  esta colección el n° del Excel Vulcano), destinatario (ubicacion_descargue),
  destino_real, cliente (lookup por NIT), entrega (planilla_siscore — puede traer
  varias guías por coma), kilos (num_kilos)}`. Los docs son UNO POR PEDIDO y el
  `$match` de NIT solo deja los de Kabi, así que los pedidos de OTROS clientes del
  mismo consecutivo se consultan en una **2ª agregación SIN filtro de NIT** (la suma
  de pedidos cuadra con `total_kilos_vehiculo`).
  Con **`q` (trazabilidad)**: SIN filtro de fecha, trae los vehículos cuyo pedido
  Vulcano (`numero_pedido`, regex case-insensitive) coincida — en el frontend hay
  barra de búsqueda propia (arriba del gráfico): oculta el drill-down, muestra la
  tabla de detalle con los pedidos YA desplegados y el pedido que hizo match
  **resaltado en amarillo**; "✕ Limpiar" vuelve al drill-down.
  Constantes backend: `TOPES_TIPO_VEH` / `_categoria_por_kilos` (espejo frontend
  `TOPES_USO`); ojo que `tipo_por_kilos` de `pedidos.py` NO sirve: no
  tiene CARRY y usa kg SICETAC, no reales.
  **Gráfico "🚛 Uso de vehículos solicitados" (drill-down, bajo el de On Time):**
  todo se agrupa EN EL FRONTEND con esas filas — 4 niveles con breadcrumb
  navegable: **1)** barras por tipo solicitado (promedio % uso, ordenadas de
  MENOR a MAYOR uso — el peor aprovechado primero; semáforo a 2 colores:
  <80 **rojo** subutilizado / ≥80 verde, ReferenceLine 100%, SIN TIPO al final) →
  click; **2)** evolución del tipo por período (toggle Mensual/Diario propio) →
  click; **3)** barras horizontales por destino (también de menor a mayor uso,
  el peor arriba) → click; **4)** tabla casi a nivel
  `consecutivo_vehiculo` (el consecutivo SALTA DE LÍNEA, no se trunca) con
  **% Uso como 4ª columna** (tras el vehículo) y, al final, **Costo Veh.**
  (Total Solicitado), **Costo Teór.** y **Sobrecosto** (`diferencia_flete`:
  rojo >0, verde ahorro <0)
  y botón **"+"** que despliega los pedidos (sub-tabla:
  Pedido / Pedido Vulcano / Destinatario / Entrega (guía) / Kg / Cliente / Destino Real)
  + export Excel del alcance visible del drill-down (mismo orden de columnas
  que la tabla: % Uso tras el vehículo, Pedidos Vulcano ÚNICOS — un viaje
  puede ser un solo pedido repartido en varias entregas —, Destinatarios (kg),
  Entregas (guías) y los costos). Respeta los filtros Año/Mes.
- `GET /indicadores-cliente/{cliente_id}/guias?anio=&mes=` (**PG puro desde 2026-10-29**) —
  **Informe de guías TMS**: TODO sale de `informe_guias_tms` (antes: agregación Mongo
  `pedidos_completados` + JOIN por guía en lotes de 500 + cap 20k filas al frontend;
  2,5 s → **~14 ms con cache**). Arquitectura: **UNA query de columnas mínimas** de
  todas las filas del filtro (`guias_minimas_cliente`, índice
  `idx_guias_cliente_fecha (nombre_cliente, fecha_emision DESC)`) → Python computa
  resumen + **series OT mensual/diaria ya agregadas** (`serie_mensual`/`serie_diaria`;
  el frontend YA NO agrupa en el navegador) + 1ª página de filas (200). Cache TTL 300 s
  en el router (lo limpia `POST /citas`); pool `ThreadedConnectionPool` 1-5 lazy en
  `bd_postgres.py`. **Dieta del payload (2026-11-05)**: `ot_por_destinatario` con **top
  100** por cumplimiento (antes ~1.350 ítems = 180KB, la mitad del payload; ciudades van
  completas) y `?sin_filas=1` (lo envía el portal externo, que no renderiza la tabla:
  `filas=[]` y `paginacion.total=len(minimas)`; el dashboard interno sigue igual). Año
  Kabi: 372KB → **~53KB (−86%)**; `sin_filas` viaja en la clave de la cache. XLSX
  (SheetJS, 90KB gz) pasó a **import dinámico al clic** en los 4 exports del
  DashboardCliente — ya no viaja en la carga inicial. _Estudio de carga del portal
  (2026-11-05): lo que más demora es el **cold start de Render** (instancia free dormida:
  8–60s la primera consulta; caliente con cache 0,3s, cómputo frío 1–4,5s sobre ~31k
  guías). Pendientes: keep-alive externo (ping cada ~5min), `Cache-Control: immutable`
  para `/_next/static` en GoDaddy (hoy `max-age=86400`)._ Casts de `piezas`/`kilos` (TEXT)
  en SQL; `fecha_cita` cruda del SQL
  y parseada en Python (**`_parse_fecha_tms` acepta `D/M/YYYY`** — ojo: el flujo viejo
  solo parseaba ISO y las citas reales del TMS JAMÁS servían; al corregirlo el % OT
  subió ~15 pts porque ahora sí aplican). Población: TODAS las guías del cliente en el
  TMS (el flujo Mongo solo veía las de vehículos registrados). Estados basura del TMS
  (`''`, `'0000-00-00'`…) → `estado: null` + `estado_crudo` (la regla ANULADA >7 días
  DESAPARECIÓ: ya no hay guías sin registro TMS).
  - **On Time — LÓGICA DEL EXCEL REAL (rework total 2026-10-31)**: réplica de
    `calculo real ot.xlsx` (hoja BASE, fórmulas BB..BP). Misma función
    `_calcular_ot_filas` para gráfico Y tabla = consistencia garantizada.
    - **tipo_servicio** (BB): cliente `FK SERVICIO TECNICO` → SERVICIO TECNICO;
      guía `…REC` → RECOLECCION; `…DEV` → DEVOLUCION; `722…` → MUESTRA DE
      CALIDAD; resto CARGA. **tipo_st** (BC): ST+origen BOGOTA+CST → RECOLECCION;
      ST+origen BOGOTA → ENVIO DIRECTO; ST+otro origen → RECOLECCION.
    - **municipio/dpto real** (BD/BE): ENVIO DIRECTO→destino; RECOLECCION→ORIGEN;
      CARGA→destino; DEV/MUESTRA→origen — con `correcciones_nombres` (clave
      alfanumérica). Mientras el bot no cargue origen/dpto: ST sin origen asume
      ENVIO DIRECTO (CST asume RECOLECCION desde BOGOTA) y el lookup de días
      degrada (municipio,dpto) → municipio único → prefijo único (`_dias_tabla`).
      **(2026-11-11, fix 801226889)** `ciudad_destino` SIEMPRE viene
      `«CIUDAD - DEPARTAMENTO»` y `_ciudad_y_dpto` parte el sufijo SIEMPRE
      (antes solo cuando faltaba el dpto): con `departamento_destino` ya cargado
      por el bot el municipio quedaba `'BUCARAMANGA - SANTANDER'`, el lookup de
      `tiempos_entrega` fallaba y la guía entregada caía a «No aplica» sin
      promesa. Eran 99 guías Kabi con dpto cargado → tras el fix 96 Cumplieron
      / 3 No cumplieron (el % OT del histórico cambia). Tests:
      `tests/test_ot_pendientes.py` (`CiudadYDptoTests`, 3).
    - **Días** (BI): tablas Mongo `tiempos_entrega` (CARGA, col. Días 2026) y
      `tiempos_entrega_st` (ST: envío directo por destino / recolección por
      origen); estado CON NOVEDAD sin reporte → NO APLICA; DEV/MUESTRA →
      **fallback NACIONAL 12 / URBANO 8**. **Promesa** (BJ): `WORKDAY` ≡
      `_sumar_dias_habiles` (CARGA con motivo EN ACONDICIONAMIENTO cuenta desde
      `fecha_carga`); DEV/MUESTRA suman 12/8 en CALENDARIO. ⚠️ Ya NO se usan
      `tarifas.promesa_entrega_dias` ni `_dias_promesa_destino`.
    - **Cita** (BN): la MANUAL de `/CitasEntrega` (`citas_entrega`, 2026-11-09:
      fecha + franja horaria registrada por el operativo) **GANA**; si no hay,
      `citas_agendadas` (CITASACT del Excel, VLOOKUP primera aparición) — ni
      `fecha_cita` del TMS ni `citas_kabi` (traen re-agendadas
      que divergen). Borrada la manual, el OT vuelve a la agendada; la fila
      lleva `cita_entrega` (fecha manual o null, aditivo). Ver §3-bis.
      **(2026-11-10, fix 801232391)** Cuando el veredicto se juega por la CITA
      (`origen_promesa='CITA'`), la **`fecha_promesa` de la fila ES la cita**
      (`f_promesa = cita` en `_calcular_ot_filas`) — antes viajaba la promesa
      de TARIFA con el sello «cita» y el detalle/informe decía una fecha
      distinta a la Trazabilidad (que usa `fecha_cita_usada`). Ahora informe,
      modal del día, Excel («Días vs Promesa» = entrega − cita) y Trazabilidad
      muestran la MISMA fecha; los días del destino siguen en `dias_promesa`.
      **Novedad** (BP): SOLO `novedades_siscore` (reporte de
      Siscore, guía→MOTIVO) — ⚠️ la columna `novedad` del informe es el último
      evento ('ENTREGA DIGITALIZADA' en casi todas) y NO sirve como proxy.
      **(2026-11-10, caso 801232571)** Las guías **ANULADAS** también auto-cumplen
      como «Con novedad»: el servicio se canceló, no es un incumplimiento —
      `ot=1` + `cumple_novedad` + `anulada` en `_calcular_ot_filas` (antes caían
      a Pendientes vencidas por no tener entrega), NO son pendientes, salen de
      `en_proceso` del resumen y la Trazabilidad las pinta con chip morado
      **«⊘ Anulada»** propio (el detalle del día/informe con «⊘ Anul.» y el Excel
      con «SÍ (anulada)»). El panel 🚛 de Monitor no sale para anuladas.
      ⚠️ **El informe del TMS NUNCA trae 'ANULADA'** (estados reales:
      ENTREGADO/CON NOVEDAD/PENDIENTE/En distribucion/Transito Nacional/vacío)
      — la anulación solo la sabe el **WS de Siscore en vivo**. Por eso:
      `ANULADA` entró a `_ESTADOS_TMS` (por si algún día llega) y además
      `/consulta-guia` **persiste en `guias_anuladas`** cada guía que el WS
      reporta anulada (primera vez; limpia `_CACHE_GUIAS`+`_CACHE_PENDIENTES`+la
      entrada `guias_anuladas` de `_CACHE_TABLAS`) — el mapa
      `_mapa_guias_anuladas` (TTL 600 s) la hace auto-cumplir en TODO el
      dashboard. La detección es ORGÁNICA: una guía anulada entra al mapa la
      primera vez que alguien la consulta en la Trazabilidad.
      **(2026-11-11, caso 801216689) DEVOLUCIÓN TOTAL = mismo trato que la
      ANULADA**, pero la detección sale del PROPIO informe: NO es un estado,
      vive en la columna `novedad` del TMS (`novedad_tms`, 125 guías: el
      estado queda en 'CON NOVEDAD'/'PENDIENTE' y sin `fecha_entrega` la guía
      caía a «Pendientes vencidas»). `_calcular_ot_filas` detecta la
      SUBCADENA `'DEVOLUCION TOTAL'` (cubre variantes reales como 'SE HACE
      LA DEVOLUCION TOTAL POR MOTIVO DE ORDEN VE'; **'DEVOLUCION
      DIGITALIZADA' NO entra** — es la devolución ya surtida, con entrega) →
      `ot=1` + `cumple_novedad` + flag `devolucion_total` en la fila. Fuera
      de `en_proceso` del resumen y de `/pendientes`; el panel 🚛 de Monitor
      tampoco sale (no hay viaje que seguir). Chips: «↩ Devolución total»
      (Trazabilidad), «↩ Dev.Tot.» (tablas) y Excel «SÍ (devolución
      total)». Tests: `tests/test_ot_pendientes.py` (15).
    - **Veredicto** (BK): **(2026-11-11, fix 801226437) la CITA se consulta
      ANTES del «sin promesa → No aplica»** — hay guías con cita cuyo destino
      no está en `tiempos_entrega` ('DISFARMA CEDI BOGOTÁ', nombre de sitio) y
      el No aplica adelantado desperdiciaba la cita (12 guías 2026 → 11
      Cumplieron / 1 No cumplió). Referencia = la **entrega** (las guías EN
      CURSO ya NO
      se juzgan contra HOY — 2026-11-10: van a los segmentos **Pendientes** /
      **Pendientes vencidas** y quedan FUERA del %; `ot` = null con
      `ot_pendiente`/`ot_pendiente_vencida`). CITA RECHAZADA POR TIEMPOS DE
      TRANSITO → compara la PROMESA; otra novedad → «cumple con novedad»
      (auto-cumple — las en curso con novedad SIGUEN contando aquí); sin
      novedad: con cita compara la CITA, sin cita la PROMESA; sin promesa
      evaluable → No aplica (fuera del %). % OT = (cumplieron + conNovedad) /
      (cumplieron + noCumplieron + conNovedad).
    - **Conciliación** (meta del rework, `conciliar_ot_real.py`): **97,0% de
      match en entregadas** (28.739/29.620) contra el Excel; el 3% restante
      bloqueado por columnas que el Bot 001 debe cargar (ciudad/dpto origen, dpto
      destino, fecha_carga — YA creadas en la tabla): ST recolección por origen
      real, municipios ambiguos (ARMENIA QUINDIO/ANTIOQUIA), EN ACONDICIONAMIENTO.
      Las en curso divergen por diseño (HOY distinto al del archivo).
    - `dias_habiles` = hábiles Colombia emisión→entrega (Ley 51/1983). `origen_promesa`:
      CITA|TARIFA|FALLBACK. Filas nuevas: tipo_servicio/tipo_st/ruta/dias_promesa/cumple_novedad.
    - Tablas CARGADAS originalmente desde el Excel del área con
      `importar_tablas_ot_real.py`: tiempos_entrega (188), tiempos_entrega_st
      (237), correcciones_nombres (254), novedades_siscore (856), citas_agendadas
      (7.790). **(2026-11-20) `correcciones_nombres` DEPURADA: 254 → 26 docs** —
      se borraron las 230 identidades (`error == correccion`) que no hacían nada,
      PERO 11 claves identidad SE QUEDAN porque trabajan de verdad: la clave
      alfanumérica canonicaliza variantes rotas reales del TMS (`'BOYA CA'`→
      `BOYACA`, `'CUNDINAMARC A'`→`CUNDINAMARCA`, `'SANTAMARTA'`→`SANTA MARTA`,
      `'VALLEDEL CAUCA'`→`VALLE DEL CAUCA`…); sin ellas esas guías caen a «No
      aplica». También se eliminaron duplicados y quedó 1 doc por clave (las
      colisiones BOGOTA/APARTADO/etc. resolvían por orden de `find`, no
      determinista). Respaldo previo:
      `integrappi/correcciones_nombres_respaldo_2026-11-20.json`.
      **(2026-11-12) MONGO ES LA FUENTE DE VERDAD** — el Excel
      `calculo real ot.xlsx` fue ELIMINADO del repo y el script quedó
      **deprecated** (hacía delete+insert: ejecutarlo con un Excel viejo
      BORRARÍA toda edición manual). Los cambios de tiempos/correcciones van
      **directo en las colecciones** (ej.: PUERTO CONCORDIA/META/3 días agregado
      a `tiempos_entrega`, caso guía 801233225 que caía a «No aplica» por no
      estar el municipio). ⚠️ No hay re-importación posible: respaldo histórico
      del Excel solo en el historial de git.
  - `GET /{cliente_id}/guias/filas?anio=&mes=&q=&pagina=&limite=&usuario=` — páginas
    2..N de la tabla (200/pág) y fetch de export (limite 30000, un año Kabi ≈ 29k en
    ~2 s). Con `q`: **ILIKE parcial** sobre `guia`/`planilla`/`destinatario` en TODO el
    histórico (ignora año/mes; ~2,8 s — si molesta, GIN trgm pendiente).
  - `GET /{cliente_id}/pendientes?anio=&mes=&grupo=&usuario=&destinatario=&tipo_servicio=`
    (**2026-11-10**) — guías EN CURSO del cliente para la pestaña Operación del portal:
    los segmentos «Pendientes»/«Pendientes vencidas» del OT. **Respeta los MISMOS
    filtros del dashboard que `/guias`** (año/mes en SQL, grupo por nombres,
    destinatario en Python y tipo de servicio DESPUÉS del OT — mismo día se corrigió:
    antes traía TODO el histórico y saltaban guías fuera del filtro vigente; el portal
    externo arranca con tipo CARGA por defecto); las que van a segmento «Con novedad»
    quedan fuera. Devuelve
    `{vencidas[], pendientes[], no_aplica[], generado}` («No aplica» = sin cita ni
    promesa evaluable, la más reciente primero) — cada fila con el **límite** con el que se
    juzga (`fecha_cita_usada` — manual o agendada — y si no hay, `fecha_promesa`;
    `origen_limite` CITA|PROMESA) y **`dias_vs_limite`** = días calendario HOY − límite
    (>0 retraso, <0 aún en plazo). **(2026-11-12, fix 801233440) el HOY de TODA la
    lógica OT es `_hoy_colombia()`** (UTC−5 fijo): Render corre en UTC y después de
    las 7 pm colombianas `date.today()` ya devolvía el día SIGUIENTE — inflaba el
    retraso en 1 y marcaba guías «Pendientes vencidas» un día antes. Orden: vencidas
    de la MÁS atrasada a la menos; en
    curso de la que vence primero a la que vence después (sin límite evaluable al final
    de cada lista). Cache TTL 300 s (`_CACHE_PENDIENTES`, los filtros van en la clave)
    que también limpian las escrituras de citas/novedades (una cita nueva mueve una
    guía de segmento).
  - **Plan B citas — `POST /indicadores-cliente/citas?perfil=` (2026-08-21)**: Excel
    `{GUIA, FECHA_CITA}` → Mongo `citas_kabi` (upsert, lotes de 1000). Perfiles
    ADMIN/ANALISTA/COORDINADOR/CONTROL. Al cargar limpia el cache de `/guias`.

**Informe de guías en el dashboard (frontend):** sección colapsable "🚚 Informe de guías
TMS" bajo el gráfico de OT — toggle con tiles resumen (Guías/**Planillas**/Entregadas/
**On Time %**/En proceso/Sin info), **barra de trazabilidad** (input guía/planilla/
destinatario → `?q=` en `/guias/filas`, reemplaza SOLO la tabla; el gráfico OT queda
del período), tabla `IG-tabla` con scroll interno y thead sticky (columnas: Guía,
Planilla, Destino, Destinatario, F. Emisión, Piezas, Kilos, Estado, F. Entrega,
F. Promesa, Días háb., OT, F. Digitalización, F. Cita), chips de estado (ENTREGADO
verde, resto ámbar), fechas **DD-MM-AAAA**, footer «Mostrando X de Y» + botón
**«＋ Cargar más (200)»** (paginado contra `/guias/filas`; sin cap 20k), export .xlsx
(SheetJS) que baja el **rango completo** (fetch limite=30000) y botón "⬆ Cargar Citas"
(perfiles autorizados). El fetch del informe viaja **desde la carga inicial** junto con
`/cajas` (el gráfico OT lo necesita) y en cada "Filtrar".

**Gráfico "🎯 On Time por período":** `BarChart` **apilado** con toggle Mensual/Diario
propio. **Bucket = FECHA DE EMISIÓN**. **Segmentos = 6 (`SEGMENTOS_OT`, definición
única para barras/leyenda/tooltip)**: verde **Cumplieron** (entregadas a tiempo),
rojo **No cumplieron** (entregadas tarde), morado **Con novedad** (auto-cumplen:
reporte de Siscore, justificada manual, guía ANULADA, DEVOLUCIÓN TOTAL y —
2026-11-11, regla final del usuario — novedades TMS **REHUSADO/REPROGRAMADO
CON fecha de entrega** (el caso cerró; sin entrega van a Pendientes)),
ámbar **Pendientes** y naranja **Pendientes vencidas** (2026-11-10: guías EN CURSO
sin fecha de entrega — ya NO se juzgan contra HOY; quedan FUERA del % OT y `ot`
viaja null con `ot_pendiente`/`ot_pendiente_vencida`; incluye REHUSADO/
REPROGRAMADO sin entrega, 17 guías Kabi),
gris **No aplica** (sin
promesa evaluable, fuera del %). El % OT = (cumplieron + conNovedad)/evaluables
(conNovedad ya incluidos en evaluables, backend `_serie_ot`). ⚠️ Con este cambio
el % de períodos con guías en curso DIVERGE deliberadamente del Excel del área
(las entregadas siguen cuadrando). Badge con el rango de
filtros (el badge «% OT total» junto al título se QUITÓ 2026-10-30 a pedido del
usuario — el % vive en el tacómetro y el gráfico vs meta). Los buckets los agrega el BACKEND
(`serie_mensual`/`serie_diaria` de `/guias`; claves `pendientes`/
`pendientesVencidas`; el resumen lleva `ot_pendientes`/`ot_pendientes_vencidas`).
Tooltip con los segmentos >0, evaluables y %. En tablas/modales el chip «🕒 Pend.»/
«🕒 Venc.» (ámbar/naranja, fuera del %), en los Excel «Cumplió» =
PENDIENTE / PENDIENTE (VENCIDA) sin «Días vs Promesa», y en la consulta de guía
(Trazabilidad) chip «🕒 Pendiente» / «🕒 Pendiente (vencida)» (payload.ot.pendiente).
Tests: `tests/test_ot_pendientes.py` (5).

### Novedades Justificadas OT (2026-10-29)

Guías con **causal + justificación** cargadas por ADMIN/ANALISTA/COORDINADOR/CONTROL/OPERATIVO
(2026-11-11: entró ANALISTA — backend `PERFILES_NOVEDADES`, gate de la página
y menú MedicalCare)
desde **`/NovedadesOT`** (botón «🧾 Novedades OT Justificadas» en el menú MedicalCare,
`rutas/novedades_ot.py` + `Paginas/NovedadesOTP`). Una guía justificada **gana a su
estado TMS**: segmento **morado «Justificadas»** en el gráfico, chip morado
`💬 causal` a secas en las tablas (2026-10-30: sin la palabra «justificada» — el
title del chip trae causal + justificación + quién registró) y columnas «Causal
(justificada) / Justificación / Registrada por» en los Excel del informe y del OT.
**% OT (2026-10-30): las justificadas CUENTAN COMO CUMPLIDAS** — `(cumplieron +
justificadas) / (evaluables + justificadas)` en el `ot_pct` del resumen, el `pctOT`
por bucket del frontend, el tooltip, el badge y el Excel de la serie (antes quedaban
excluidas del %). **Todos los % van con 1 decimal y coma** (85,4% — `pctOT` redondea
a décimas; antes el gráfico de meta mostraba «79,0» por doble redondeo a entero).
El catálogo `causales_ot` parte de seed de 3 y se amplía con
`POST /novedades-ot/causales` (ADMIN) o directo en Mongo — a 2026-10-30 tiene 25
causales en MAYÚSCULAS (el matching del Excel es tolerante a mayúsculas/acentos).

### Citas de Entrega — cita manual que gana al OT (2026-11-09)

`/CitasEntrega` (`rutas/citas_entrega.py` + `Paginas/CitasEntregaP`, menú
hamburguesa 📅): registro MANUAL por guía de la **fecha de cita + franja
horaria** (hora inicio/fin en la que el cliente puede recibir). La fecha
registrada **GANA sobre `citas_agendadas`** en `_calcular_ot_filas`
(`_mapa_citas_entrega`, cache `_CACHE_TABLAS` igual que las demás tablas) —
si el TMS/Excel dice 8-nov y acá se registra 9-nov, el OT juzga con el 9-nov.
Borrada la manual → vuelve a la agendada. La franja es SOLO informativa (el
OT compara dates). `origen_promesa` sigue siendo «CITA» (el frontend compara
ese literal en 10+ sitios); la fila lleva `cita_entrega` (fecha manual) y
`fecha_cita_usada` (la cita con la que SE juzgó, manual o agendada).

- **Re-agendas**: una guía puede tener VARIAS citas — subir la MISMA guía con
  una fecha nueva (mismo archivo o carga posterior) deja la ÚLTIMA vigente
  para el OT; filas idénticas al estado actual → `sin_cambio` (no escriben).
  `historial[]` (cap 20) es un log de CAMBIOS: cada entrada
  `{previo, nuevo, por, fecha_cambio}` (de qué fecha → a cuál, quién y cuándo;
  las entradas viejas planas se normalizan a previo). El módulo muestra badge
  «🔁 N» y en el modal la tabla «Cambió a | Nueva franja | Antes | Hecho por |
  Cuándo» **de la más reciente a la más antigua**.
- **Excel TODO-O-NADA**: GUIA · **FECHA (DD/MM/AAAA)** · HORA INICIO · HORA
  FIN. La plantilla pide fecha plana dd/mm/aaaa (celda de fecha real
  DD/MM/YYYY; sin fila de nota — una fila sin guía rechazaría la propia
  plantilla); el parser acepta dd/mm/aaaa, ISO y celdas datetime. Franja
  inicio ≤ fin; UNA fila inválida → 422 con `errores[]` y NO se guarda NADA.
- **Eliminadas (auditoría)**: `DELETE /eliminar` mueve el doc COMPLETO a
  `citas_eliminadas` con `eliminada_por`/`eliminada_el`; `GET /eliminadas`
  (ADMIN/CONTROL) + sección 🗑 en el módulo (carga al abrir). Export Excel y
  bitácora `auditoria_citas_entrega` (append-only).
- **UX del módulo**: horas en dropdowns 24h de MEDIA HORA (48 opciones; no
  `<input type="time">` — muestra AM/PM según locale); al elegir inicio, el
  fin sale por defecto en **inicio + 2h** (tope 23:30) y nunca puede quedar
  antes; buscador de guía EN VIVO (debounce 350 ms) y, sin resultados, botón
  «＋ Crear cita para la guía 'X'».
- **Trazabilidad en el portal**: `/consulta-guia` devuelve `payload.cita_entrega`
  {fecha, franja, **nombre de quién la asignó**, fecha_asignada, anteriores[]
  (cada una {previo, nuevo, nombre}, más reciente primero)} y
  `payload.ot.fecha_cita_usada`/`cita_manual` — la Trazabilidad del portal
  pinta el panel verde «📅 Cita de entrega: {fecha} · {franja}» (con las
  re-agendas colapsadas) y el detalle del chip OT muestra la CITA real usada
  (antes ponía la promesa con etiqueta «cita»).
- **Ubicación del vehículo en curso (2026-11-09)**: si la guía consultada NO está
  entregada y tiene placa, el backend adjunta `payload.vehiculo` desde la **API
  Monitor de Mayasoft** (`Funciones/monitor_ws.py`; contrato `integrappi/api
  monitor.pdf`): `{ok, existe, vehiculo{placa, manifiesto, estado_viaje, conductor,
  ubicacion, lat, lng, fecha_reporte_gps, velocidad, estado_gps, eta_fecha,
  eta_distancia_km}}` — el manifiesto VIGENTE de la placa (no finalizado). El
  portal pinta el panel azul «🚛 Ubicación del vehículo» (tras los links
  tracking/evidencia) con ubicación textual, chips (velocidad/estado GPS/viaje/
  manifiesto/conductor), reporte GPS + ETA y **mapa embebido** de Google Maps
  (`output=embed`, sin API key) + link externo. Criterio "entregada" = mismo del
  frontend (`Estado` con ENTREGA/DIGITALIZADA). Fallo de Monitor o env vars
  `MONITOR_*` ausentes → panel simplemente no aparece (degradación silenciosa).
  Base URL real: `https://integra.mayasoft.ai` (⚠️ NO la de los ejemplos del
  PDF — `monitorpiloto`/`monitorqa` no aceptan las credenciales de Integra).
  **(2026-11-11) Validación del manifiesto vs las guías**: la consulta pide
  `lists=location,remittances` y `consultar_vehiculo_monitor(placa, guias=…)`
  recibe las guías del viaje (la consultada + las compañeras de
  `misma_placa`); cruza contra **`clientreferencenumber`** de las remesas
  (TEXTO con guías Siscore separadas por coma, p. ej. `"801233381,
  801233382"` — solo lo reportan despachos de Kabi; FALABELLA/MINISO/etc. lo
  mandan vacío) → `vehiculo.coincide_guia` (None si no se pidió validación).
  `coincide_guia=false` = el manifiesto vigente de la placa NO declara
  ninguna de esas guías (otro viaje/cliente): el **perfil CLIENTE no ve NADA
  de Monitor** (ni panel 🚛 ni la ETA del resumen — helper front
  `ocultarMonitorSinCoincidencia`); los perfiles internos del portal
  (ADMIN/VISUALIZADOR/ANALISTA) ven el panel normal y el aviso rojo urgente
  **«🚨 Posible carro sin planillar»** (`.DC-opVehiculoAviso`) como PRIMER
  elemento del resultado (encima del resumen). **(2026-11-11, caso
  801227068) Manifiesto FINALIZADO = no se ve NADA de Monitor**: si el
  manifiesto elegido está `Finalizado` (la placa no tiene viaje en curso —
  p. ej. guía «zombie» en Transito Nacional desde hace meses), NO se muestra
  el panel 🚛 completo ni la ETA del resumen para NINGÚN perfil (la posición
  sería de un despacho que ya terminó). Orden del resultado
  (2026-11-11, a pedido): aviso → resumen/ETA/OT → links → novedad → **📅
  Cita de entrega (Mongo `citas_entrega`) ANTES del mapa del vehículo** →
  panel 🚛 (ubicación/chips/mapa) → otras guías. Tests:
  `tests/test_monitor_ws.py` (13).
  Sonda: `python scripts/probar_monitor.py <placa|guía>` (resuelve guía→placa
  en el TMS). Verificado en vivo 2026-11-09 (guía 26032167 / SPW261). Ver
  `integrappi/README.md` 2026-11-09 (2.ª tanda).
- **ETA promovida al resumen (2026-11-09)**: además del panel 🚛, el resumen
  superior de la guía consulta (`DC-opResumen`) muestra un bloque **ETA**
  entre «Estado actual» y «On Time» — chip `🕒 fecha/hora` + km restantes,
  solo con guía no entregada + ETA reportada (reutiliza `DC-opEstadoActual`,
  mismo look del bloque OT). Responsive del resumen: tablet ≤1024px destino
  en fila completa y estados en 2.ª fila; móvil ≤768px estados como filas
  tipo lista (grid etiqueta 5.4rem | chip, detalle debajo). Front-only, va
  en el mismo dep del panel 🚛.
- Perfiles ADMIN/ANALISTA/COORDINADOR/CONTROL/OPERATIVO (2026-11-11: entró
  ANALISTA — backend `PERFILES_CITAS`, gate de la página y menú MedicalCare).
  ⚠️ Cada escritura limpia
  `_CACHE_GUIAS` **Y** la entrada `citas_entrega` de `_CACHE_TABLAS`
  (`_limpiar_caches_ot` del router) — sin eso el cambio tarda hasta 10 min en
  verse. ⚠️ **Trampa Mongo**: el append al historial NO puede ir con `$each`
  dentro de `$set` (Mongo guarda el dict literal y deja de ser lista — bug
  corregido el mismo día en citas_entrega Y novedades_ot); se reescribe el
  array completo saneado con `_historial_lista` (auto-repara docs corruptos);
  reparador puntual `reparar_historial_citas.py`. Tests:
  `tests/test_citas_entrega_ot.py` (15). **(2026-11-25) La columna «F. Cita» del
  informe y de TODOS los Excel ya NO muestra la del TMS**: prefiere
  `fecha_cita_usada` (la cita con la que SE juzgó — manual o agendada; caso
  801202826: origen CITA con F. Promesa llena y F. Cita vacía) y cae a la del
  TMS solo si no hubo cita usada. `diasVsPromesa` del Excel OT también resta
  la cita USADA (antes podía restar la del TMS, que diverge de la juzgada).
  Dep SOLO frontend.

### Columna `novedad` del TMS (2026-10-30)

`informe_guias_tms` trae una columna `novedad` (texto crudo del evento: «ENTREGA
DIGITALIZADA», «REHUSADO»…) que NO viajaba en ninguna consulta. Entró con alias
**`novedad_tms`** en `_COLUMNAS_PAGINA` (bd_postgres) — ⚠️ la clave `novedad` queda
RESERVADA para la justificada de Mongo (`_adjuntar_novedades` la pisa). En los 3
Excel como columna «Novedad TMS» junto a Estado (distinta de Causal/Justificación).

- **Export Excel de toda la base (2026-10-30)**: `GET /novedades-ot/exportar?usuario=`
  → .xlsx (openpyxl, `asyncio.to_thread`) con TODAS las justificaciones ordenadas
  por `updated_at` desc — columnas GUIA · CAUSAL · JUSTIFICACION · REGISTRADA POR ·
  CREADA · ACTUALIZADA POR · ACTUALIZADA. Botón **«⬇ Excel»** junto a la Plantilla
  en /NovedadesOT (link directo, perfiles NOVEDADES).
- **Registro manual y edición (2026-10-30)**: `POST /novedades-ot/guardar` (JSON
  `{guia, causal, justificacion}`) — crea o actualiza UNA justificación sin Excel,
  misma semántica que la carga masiva (upsert por guía, previo → historial, causal
  contra catálogo, auditoría, limpia cache de `/guias`). En `/NovedadesOT`: botón
  **«＋ Nueva justificación»** y **✏️ por fila** → modal (guía readonly al editar —
  es la clave del upsert; Escape/✕/fondo cierran).
- **Colecciones** (base `integra`): `causales_ot` (catálogo, seed: *reagenda cita*,
  *siniestro*, *derrumbe*; ampliable por ADMIN `POST /novedades-ot/causales`),
  `novedades_justificadas_ot` (UNA por guía, unique; `creado_por`/`actualizado_por`/
  `historial[]` cap 20) y `auditoria_novedades_ot` (bitácora append-only de cada
  carga/actualización/borrado con usuario/nombre/perfil/fecha — el borrado GUARDA el
  documento completo; `GET /novedades-ot/auditoria` para ADMIN/CONTROL).
- **Carga (TODO-O-NADA)**: `POST /novedades-ot/importar-excel` — columnas GUIA ·
  CAUSAL · JUSTIFICACION (plantilla en `GET /novedades-ot/plantilla`); causal validado
  contra el catálogo (tolerante a mayúsculas/acentos). Se valida el archivo COMPLETO
  antes de escribir: UNA fila inválida (causal no permitido, guía o justificación
  vacía) → 422 con `{mensaje, errores[]}` y NO se guarda nada. Upsert por guía con
  historial. Limpia el cache de `/guias`. **Escritura en LOTE (2026-10-30)**: antes hacía
  `find_one` + `update_one` + auditoría POR FILA (3 viajes a Mongo × fila — un Excel de
  5.000 filas ≈ 15.000 round trips secuenciales ≈ minutos, y al ser pymongo bloqueante
  en un `async def` congelaba TODA la API durante la carga); ahora previos en UNA
  consulta `$in`, upserts con `bulk_write` y auditoría con `insert_many` (lotes de
  1.000), todo en `asyncio.to_thread` → mismo resultado, ~6-10 operaciones totales.
- **Cruce**: `_adjuntar_novedades` marca `f['novedad']` en `/guias` y `/guias/filas`
  (resumen `ot_justificadas` incluido). Colección chica → se lee completa por
  consulta. **Botón «Excel»** junto al
toggle (isotipo Excel verde): descarga `on_time_<cliente>[_<día-zoom>]_<fecha>.xlsx` con 2
hojas — la hoja ② «Guías» usa `filaOTExcel` (2026-10-31): orden de lectura
profesional — Guía · F. Emisión · F. Entrega · F. Promesa · Origen Promesa ·
**«Días vs Promesa»** (entrega − promesa en calendario: negativo = temprano,
0 = justo, positivo = retraso) · Días Hábiles · **«Cumplió»** (SÍ/NO; «NO
(justificada)» si la novedad la rescata) · Estado · Resultado (segmento) ·
Novedad TMS + contexto y, al final, la justificación si la guía la tiene.
(«F. Conteo
OT» de `filaGuiaAExport` se corrigió a fecha de EMISIÓN — el bucket del gráfico;
decía entrega, stale desde 2026-10-29.)
**El export descarga SOLO lo filtrado en pantalla (2026-10-30)**: año/mes/grupo
(viajan en el fetch `/guias/filas` limite=30000), el **zoom de día** del breadcrumb (hoja ①
= UNA fila del día; hoja ② solo las guías emitidas ese día, recorte client-side por
`fecha_emision`) y el **filtro de leyenda** (hoja ① con columnas solo de los segmentos
seleccionados; hoja ② filtrada por `segmentoDeFila`). La trazabilidad NO aplica — el
gráfico siempre es del período.

**Interacciones OT + filtros (2026-10-29, tarde):**
- **Zoom por clic en barra**: en vista Mensual, clic en una barra mensual (`clicBarraOT`,
  `activeLabel`) **APLICA el filtro real** Año+Mes de esa barra (quedan marcados en
  los dropdowns, viajan en el fetch de todo el dashboard y aparecen como migas
  normales borrables — sin estado paralelo de zoom) y salta a la vista Diario, que
  llega ya acotada del backend. Cursor pointer en vista mensual como pista. Quitar
  la miga del mes (o del año) re-filtra al instante y la vista Diario se ensancha
  a lo que quede filtrado.
- **Zoom de día (2026-10-30)**: en vista Diario, clic en una barra diaria →
  `diaOT` ('YYYY-MM-DD') cambia el gráfico a **barras HORIZONTALES** con los
  segmentos >0 de ese día (mismo orden/color de la pila, LabelList con el conteo,
  tooltip con % del día y % OT). **(v5, misma fecha) YA NO es solo visual**: el
  clic RE-CONSULTA `/guias?dia=YYYY-MM-DD` (param nuevo del backend que recorta
  la pasada `minimas` por `fecha_emision` en Python; va en la clave del cache) y
  el backend recalcula TODO el payload para ese día — resumen (tacómetro),
  series y **OT por ciudad/destinatario siguen al día** (~1-3s con skeletons;
  el useEffect del informe depende de `diaOT`). Salir (nivel del mes del
  breadcrumb, o subir a año/histórico) re-consulta sin `dia`. `/guias/filas`
  NO acepta `dia` (informe/detalle filtran client-side).
- **Etiquetas del eje Y clicables en el zoom de día (2026-11-10)**: la etiqueta
  del segmento («Con novedad»…) abre el MISMO detalle que tocar la barra (mismo
  toggle) — con pocas guías la barra queda muy angosta (`tickSegmentoDia` +
  `.DC-tickSegmento`, hover azul subrayado, teclado). ⚠️ **Trampas de los ticks
  custom de recharts**: el texto va en `payload.value` (NO `props.value`) y
  `x`/`y` son el punto de anclaje con **y = CENTRO de la fila** (sumarle
  `height/2` corre la etiqueta media fila abajo); el tipo exige retornar
  SIEMPRE un elemento (sin `return null`); y ⚠️ **`width`/`height` del tick NO
  son de la fila** — recharts fusiona los props del EJE completo con los del
  tick (`height` ≈ 296px = todo el chart), así que la hit-area transparente
  debe derivar el alto de fila como `height ÷ visibleTicksCount` (prop que
  recharts SÍ pasa) acotada ~18-28px. Sin eso el rect de hit de cada etiqueta
  cubría la columna entera y el del ÚLTIMO tick (la fila de arriba) se pintaba
  encima: tocar «Cumplieron» disparaba «Con novedad» (fix misma tarde).
- **Detalle del día (2026-10-30; MODAL desde la misma fecha)**: en el zoom de
  día, clic en una barra-segmento abre un **modal** (`.DC-modalFondo` overlay +
  `.DC-modalCard` centrada 1000px, cierre con ✕, clic en el fondo o Escape) con
  la tabla de guías de ese día + segmento (`abrirDetalleSegmento`): título con el
  segmento y su color, nº de guías, columnas Guía/Destino/Destinatario/
  F. Emisión/Estado (chips de estado y de novedad `💬 causal`)/F. Entrega/
  *(2026-10-31: los 3 Excel del OT/informe llevan de PRIMERA columna «Cliente»
  = etiqueta de `pg_grupos` — Fresenius Kabi / FK Servicio Técnico —; viaja
  como `nombre_cliente` en `guias_minimas_cliente` y `_COLUMNAS_PAGINA`;
  espejo frontend `ETIQUETA_CLIENTE_TMS`)*
  F. Promesa/Días háb./OT (2026-10-30: SIN Planilla y fechas **dd/mm/aaaa** con
  barra), scroll interno con thead sticky y botón «Excel» que
  baja SOLO esas filas (`detalle_ot_<cliente>_<día>_<segmento>_<fecha>.xlsx`,
  mismas columnas analizadoras que la hoja «Guías» del export OT — `filaOTExcel`
  desde 2026-10-31: Guía · F. Emisión · F. Entrega · F. Promesa · Origen · Días
  vs Promesa · Días Hábiles · Cumplió (SÍ/NO) · Estado · Resultado + contexto y
  la justificación al final si la guía la tiene). En el portal oscuro el área de tabla va con
  fondo claro (legibilidad). La base del día se consulta UNA vez por día (cache
  `diaCacheOT`, recorte client-side por `fecha_emision` + `segmentoDeFila`) —
  clic en la barra activa, ✕, fondo o Escape cierra; cambiar día/filtros cierra
  y anula la cache.
- **Transición suave al cambiar de vista** (2026-10-29): el wrapper del gráfico se
  re-monta con `key={vistaOT-mesZoom}` → CSS `DC-swapOT` (fade + slide 8px, 280 ms
  ease-out, en `DashboardCliente/estilos.css`) mientras las barras crecen con la
  animación de recharts (`ANIM_BARRAS`: 650 ms ease-out). Respeta
  `prefers-reduced-motion` (sin animación en ambos casos). Aplica a Mensual↔Diario,
  zoom de mes y salida del zoom.
- **Leyenda sin paréntesis**: los 3 términos (`Cumplieron`/`No cumplieron`/`Pendientes`)
  van limpios con subrayado punteado (`DC-leyendaItem`) y su explicación en tooltip
  nativo (`title`), no en el subtítulo.
- **Leyenda = filtro (2026-10-30; rediseñada same-day)**: los estados del OT son
  **chips compactos** (`DC-otChip`: punto de color + texto 600, sin subrayados) que
  alternan su segmento — multi-selección; el gráfico (barras, tooltip, etiqueta de
  total y zoom de día) muestra SOLO los seleccionados. Sin selección = todos. Chip
  activo con filtro = borde + tinte de SU color (`${color}80/14`); inactivo =
  atenuado 42%. En **móvil** la leyenda es **colapsable** (`DC-otLeyendaToggle`,
  patrón de `filtrosAbiertos`: arranca cerrada ≤768px, ahorra ~140px) con resumen
  «N de 8 visibles». Las instrucciones quedaron SEPARADAS (`DC-otLeyendaInfo`):
  izq. «Toca un estado para filtrar» / «↺ Ver todos»; der. la acción de barra según
  vista; «por fecha de emisión» pasó al header del gráfico (`DC-otCtx`). **Persiste**
  al cambiar Mensual ↔ Diario ↔ Día y al re-filtrar (estado `segSelOT`, como el
  zoom de día: puramente visual, no viaja en ningún fetch). El tooltip con filtro
  activo muestra «Selección: X de Y total» y el % OT recalculado sobre los
  segmentos OT seleccionados (si la selección no toca evaluables, el % no se
  muestra); la etiqueta encima de la pila pasa a `totalSel` (suma de la
  selección). **El Excel del OT SÍ respeta el filtro de leyenda** (2026-10-30):
  hoja ① con columnas solo de los segmentos seleccionados y hoja ② filtrada
  por segmento (`segmentoDeFila`, espejo frontend de `_serie_ot`).
- **Breadcrumb de período (2026-10-30, rediseño UX)**: reemplazó a las migas-chip de
  año/mes/día. Ruta `Histórico › 2026 › Septiembre › 25 de septiembre` bajo la barra
  de filtros (`DC-breadcrumb`, SIEMPRE visible): los niveles anteriores son clicables
  para volver (`irAHistorico`/`irAlAnio`/`irAlMes` aplican el filtro de inmediato,
  sincronizan dropdowns y ajustan la vista OT: subir a año → Mensual, volver de día a
  mes → Diario); el último nivel es la ubicación actual (negrita, `aria-current`).
  Multi-selección se etiqueta «3 años»/«3 meses». Los filtros NO-período (👥 cliente,
  🔎 trazabilidad) van tras un divisor como chips removibles (`DC-miga`). Salir del
  zoom de día = clic en el nivel del mes.
- **Filtros (2026-10-30, noche)**: el de sub-grupo se llama **«Cliente
  interno»** y se agregó **«Cliente final»** = destinatario del TMS:
  autocomplete (`GET /{id}/destinatarios?q=`) que lista opciones **desde la
  5ª letra** (debounce 350ms), **multi-selección** con checkboxes, se aplica
  con «Filtrar» y viaja como `destinatario=` repetido en `/guias` (filtro
  Python sobre `minimas` — recalcula TODO el payload) y `/guias/filas`
  (filtro SQL en `pagina_guias_cliente` → tabla/Excel también). Miga
  removible `🎭 nombre ✕`. En el cache de `/guias` entra en la clave.
  **Performance (v4)**: la búsqueda por query hacía DISTINCT sobre ~100k filas
  (0,6-1,5 s; el índice trgm creado no ayudaba — el planner prefería el de
  cliente); ahora `_destinatarios_todos` cachea la lista DISTINCT COMPLETA en
  memoria con **stale-while-revalidate** (fresca <30 min directo; vencida se
  entrega igual y se refresca en hilo de fondo — solo la 1ª carga es síncrona
  ~1,3 s) y el autocomplete filtra **en memoria** (~4 ms, empiezan-por
  primero). El buscador es el componente **`InputDestinatario`** con estado
  LOCAL (las teclas re-renderizan solo el input, no todo el dashboard — antes
  cada letra re-renderizaba los gráficos) y avisa debounced 350 ms; el
  «Buscando…» solo aparece si tarda >250 ms. UX: píldora con 🔎 y ✕
  (`.DC-destBuscar`), y TODAS las listas de filtros del portal en tema oscuro
  (`.DC-portalOscuro .DC-dropdownFiltro*`). ⚠️ Trampa: ese bloque quedó una vez
  DENTRO del `@media (max-width:768px)` y no aplicaba en escritorio — los
  bloques nuevos van a nivel raíz.
- **Filtro «Tipo servicio» (2026-11-03)**: multi-selección a la derecha de
  «Cliente final» con las 5 opciones fijas de la fórmula BB del Excel
  (`TIPOS_SERVICIO` front / `TIPOS_SERVICIO_OT` backend): CARGA · SERVICIO
  TECNICO · RECOLECCION · DEVOLUCION · MUESTRA DE CALIDAD. Viaja como
  `tipo_servicio=` repetido en `/guias` y `/guias/filas` (422 si es
  desconocido). En `/guias` el filtro corre en Python **DESPUÉS** de
  `_calcular_ot_filas` (el tipo se deriva ahí) y recalcula TODO el payload
  (tacómetro, series, OT por ciudad/destinatario); entra en la clave del
  cache. En `/guias/filas` y en la 1ª página de `/guias` filtra en SQL con
  `_TIPO_SERVICIO_SQL` (CASE espejo de la fórmula BB, en `bd_postgres.py`;
  verificado 0 divergencias vs Python) → paginación y «Mostrando X de Y»
  exactos (a diferencia de destinatario, que NO filtra la 1ª página de
  `/guias`). Miga removible `🚚 tipo ✕`, entra en `resumenFiltros` (móvil) y
  los Excel del informe lo respetan (bajan por `/guias/filas`). Cajas/uso NO
  aplican (por NIT en Mongo). La trazabilidad (`q`) lo ignora, igual que los
  demás filtros.
- **Scroll horizontal OT en móvil (2026-10-30)**: «On Time por período» y «On Time
  vs meta» en pantallas ≤768px llevan ancho mínimo por barra (72px mensual / 26px
  diario, piso 320 tope 6000 — `anchoMinOT`) dentro de `.DC-scrollOT`
  (`overflow-x:auto` táctil): las barras conservan su tamaño y se deslizan con el
  dedo en vez de amontonarse. Escritorio sin cambios (sin minWidth no hay scroll);
  el zoom de día (barras horizontales) queda exento.
- **Filtros rediseñados (2026-10-30)**: escritorio = UNA fila (Año · Mes · Cliente ·
  Filtrar a la derecha, todo a 36px, labels uppercase discretos); móvil = panel
  plegable en grid de 2 columnas (Año+Mes lado a lado, Cliente y Filtrar a ancho
  completo, controles a 42px táctil, etiqueta arriba del control); contraído muestra
  un resumen discreto (`resumenFiltros`: «2026 · Septiembre») junto al toggle.
  Verificado 320–1440px sin overflow horizontal (`.DC-filtrosPanel.DC-filtrosOcultos`
  debe ir DESPUÉS del `display:grid` del media query o el panel no se oculta).
- **Header full-width (2026-10-30)**: `DC-headerInner` sin `max-width` — a todo el
  ancho en pantallas grandes (logo a la izquierda, como /PanelConductores). Fondo
  negro + acento verde `#98C74C` (letras «App» e ícono de usuario).

**Gráfico implementado (fresenius-kabi):** "📦 Cantidad de cajas" — LineChart de 1 línea
(gris `#606060`) con **toggle Mensual/Diario** propio, badge de vista, etiquetas de valor
corto (1.2k/12k) verticales adaptativas (pico→arriba, valle→abajo) y tooltip con cajas +
vehículos. En vista **diaria** añade área roja/azul alrededor de la **media** del período
(rojo por encima, azul por debajo; gradiente que corta exactamente en la media),
`ReferenceLine` punteada con la media y badge "% días > media (n/total)". Reutiliza el
chrome IG-*; el CSS de toggle/badge/sub/tooltip (`IG-toggle*`, `IG-graficoBadge`,
`IG-graficoSub`, `IG-tooltipSerie*`, `IG-tooltipEtapa`) **mudó al chrome compartido**
(`Componentes/IndicadoresChrome/estilos.css`) para que los dashboards de cliente lo hereden.

**Eje de tiempo jerárquico estilo Excel (2026-08-21, vista Diario):** bajo la fila de
días, **bandas de mes y año dibujadas como celdas con bordes** (tabla dinámica de Excel):
cada celda de mes encierra exactamente a sus días (va de su primer día hasta la mitad del
hueco con el siguiente mes) y la celda de año agrupa a sus meses. Detalles de implementación
(no obvios):
- La vista diaria usa un **eje numérico de tiempo**: `dataCajas` lleva `ts` (ms de la fecha)
  por punto y el `XAxis` es `type="number"` con `domain=[min,max]` y `ticks` de días
  continuos (paso adaptativo si el rango > 62 días). La vista mensual conserva el eje de
  categorías con etiquetas rotadas.
- Las bandas se dibujan con **`<Customized component={...}>`** de recharts: es el único
  gancho que recibe la geometría interna del chart — `xAxisMap[0].scale` (para convertir
  fechas→píxeles alineados al eje) y `offset`. ⚠️ `offset.bottom` NO es una coordenada: es
  el **espacio total reservado** bajo el plot (margin.bottom + height del XAxis); el borde
  inferior del plot está en `svgHeight - margin.bottom`, y de ahí hacia abajo van fila de
  días → banda de meses (20px) → banda de años (18px). El chart subió a 420px de alto con
  `margin.bottom=46` para alojarlas.
- Recharts NO apila varios `XAxis` bottom con offsets distintos por sí solo (se solapan);
  no intentar volver a eso — el `Customized` es el enfoque que funciona.

**Clientes dados de alta (2026-08-19):**
| id | Cliente | Fuente prevista |
|---|---|---|
| `fresenius-kabi` | Fresenius Kabi | postgres (`informe_guias_tms`) |
| `fresenius-medical-care` | Fresenius Medical Care | mongo (colecciones Costo Operación) |

> 💡 Datos útiles heredados de OT: `informe_guias_tms` (PostgreSQL, 195k filas) la
> carga el **Bot 001** (`AGENTE IA/Bot 001/`, repo `integralogistica/agentes` — repos
> separados; este repo solo hace SELECTs); índices creados 2026-08-19
> (`idx_guias_fecha`, `idx_guias_fecha_cliente`, `idx_guias_cliente_trgm` + ext pg_trgm).
> **Bot 001 (actualizado 2026-08-21)**: baja diario el Informe General del TMS por un
> proxy Squid (droplet DO) y UPSERT en lotes de 1000; refresca pendientes hasta **90 días
> atrás en TROZOS de 7 días** (CSV de 60+ días de una vez no genera a tiempo) con
> **re-login por intento** (la sesión caduca en corridas largas), reset de navegación
> entre trozos, NaN→None antes del batch, y **RESUMEN FINAL siempre** (éxito o error).
> Si un trozo falla 2 veces, la corrida sale ROJA indicando el rango y la siguiente
> retoma desde ahí. Carga `fecha_cita` cruda (TEXT) desde 2026-08-21.
> **(2026-10-31)** El bot ahora también carga las 4 columnas que necesita el OT real:
> `ciudad_origen`, `departamento_origen`, `departamento_destino` y `fecha_carga`
> (DATE; encabezados verificados contra el CSV real). Están en `CAMPOS_ACTUALIZAR`
> para que **re-correr un rango histórico** las llene vía ON CONFLICT (backfill en
> **trozos de ~15 días** — instrucciones en el README del bot). **(2026-11-20)
> `ciudad_destino` TAMBIÉN entró a `CAMPOS_ACTUALIZAR`**: antes solo se escribía
> al INSERT — una corrección de destino hecha en Siscore después de la primera
> carga quedaba ignorada para siempre (caso «Â CIENAGA»). ⚠️ Copia local
> modificada en `AGENTE IA/Bot 001/`; **PENDIENTE push al repo `agentes`**. Cron:
> diario 3AM (cambiar en el workflow si se quiere cada hora). Mientras el backfill
> no corra, el lookup de días del OT degrada (ver §3).

**Skeletons de carga (2026-08-21):** `SkeletonGrafico` (componente en DashboardCliente)
— cards con título/subtítulo shimmer + barras de ejemplo con alturas variadas (380px
cajas / 320px OT) y filas shimmer para la tabla del informe. Cargan en la carga inicial
(2 cards), al "Filtrar" (cajas y OT independientes) y al expandir el informe. Mismo
tamaño que el contenido real (cero salto de layout), respetan `prefers-reduced-motion`.

### Portal «Mis Indicadores» — clientes externos (2026-10-29)

`/indicadores/mis-clientes` — el mismo DashboardCliente pero para **clientes externos**
(nuevo perfil **`CLIENTE`**). El login los aterriza directo (patrón PADRINO/FINANCIERO:
sin selector de portales, cookies `cliente(s)PedidosCookie=MIS_INDICADORES`), el menú
`/indicadores` los redirige y las rutas usan `DashboardCliente` con la prop
**`modoExterno`**: oculta cajas y uso de vehículos (y salta sus fetches), oculta
«⬆ Cargar Citas», y el fetch de `/guias` viaja con `&usuario=` para que el backend
valide la asignación. Quedan «🎯 On Time por período» + «🚚 Informe de guías TMS».

- **Asignación de clientes (`clientes_siscore`)**: campo NUEVO en `baseusuarios` — lista
  de NITs del catálogo **`clientes_siscore`** (entidad/razon/nit). En GestionUsuarios
  **reemplazó los toggles KABI/MEDICAL_CARE**: botón «👥 Clientes» por fila (cualquier
  perfil) con modal multi-selección + buscador (`PATCH /baseusuarios/{id}/clientes-siscore`,
  valida NITs contra el catálogo → 422), y el mismo multi-select en «Nuevo usuario»
  (obligatorio para perfil CLIENTE). **UI de portales (2026-11-04 modal / 2026-11-05
  formulario)**: checks grandes de `PORTALES_CLIENTES` (Portal Kabi NIT 900402080 /
  Portal Medical Care NIT 901689684 — espejo de `PORTALES_POR_NIT` del backend) arriba
  y el catálogo completo (221) colapsado como «Otros clientes (avanzado)» con buscador,
  EXCLUYENDO los 2 NITs de portal; primero solo en el modal 👥, desde el 05-11 también
  en «Nuevo usuario» (antes lista plana — helpers `clientesFiltradosForm`/
  `toggleClienteSiscoreForm`, estados propios del filtro/plegado). Editar usuario no
  toca clientes (usa el modal). El campo `clientes` (portales del login) pasa a
  ser **derivado** con `PORTALES_POR_NIT` (900402080→KABI, 901689684→MEDICAL_CARE); los
  perfiles externos (CLIENTE/CLIENTE_FMC) no reciben portales. Usuarios previos sin el
  campo conservan su `clientes` intacto (la derivación solo aplica al guardar asignación).
- **NIT por cliente en `CLIENTES`** (backend): cada entrada lleva `nit` — identidad para
  el portal externo (hoy solo fresenius-kabi = NIT Kabi).
- **Endpoints**: `GET /indicadores-cliente/mis-indicadores?usuario=` (clientes asignados
  con `cliente_id` del dashboard o null = «Próximamente»; `_resolver_usuario` contra
  baseusuarios: 401 no existe / 403 inactivo) y `/guias` acepta `usuario` opcional —
  si el consultante es perfil CLIENTE exige el NIT en su asignación (403); sin
  `usuario` el dashboard interno queda igual que siempre.
- **ADMIN siempre ve todo**: `/mis-indicadores` le devuelve TODOS los dashboards
  publicados (sin depender de su asignación) y la derivación de portales le da
  KABI+MEDICAL_CARE aunque no tenga clientes asignados.
- **VISUALIZADOR también entra al portal (2026-11-05)**: mismo tratamiento que ADMIN
  en `/mis-indicadores` (ve todos los dashboards publicados, sin asignación) y en el
  gate del panel (`PERFILES_PERMITIDOS` de `PanelMisClientes` = CLIENTE/ADMIN/VISUALIZADOR).
  `_verificar_acceso_cliente` no lo restringe (solo aplica a perfil CLIENTE), así que
  el dashboard completo le responde igual que a un interno.
- **ANALISTA también entra al portal (2026-11-10)**: mismo tratamiento que
  ADMIN/VISUALIZADOR en `/mis-indicadores` (todos los dashboards publicados, sin
  asignación), en el gate del panel (`PERFILES_PERMITIDOS` = CLIENTE/ADMIN/
  VISUALIZADOR/ANALISTA) y en la tarjeta del menú.
- **OPERATIVO también entra al portal (2026-11-11)**: mismo tratamiento que
  ADMIN/VISUALIZADOR/ANALISTA en los 3 sitios (`/mis-indicadores`,
  `PERFILES_PERMITIDOS` y tarjeta del menú) y en `PERFILES_PORTAL_INTERNO`
  del DashboardCliente (ve el aviso «posible carro sin planillar»).
- **Guías en curso en la pestaña Operación (2026-11-10; rework con cards lazy por la
  tarde)**: las 3 cards de atención + **3 cards CLICABLES** — 🔴 Pendientes vencidas /
  🕒 Pendientes en curso / ⚪ No aplica — cuyos CONTADORES salen gratis del resumen de
  `/guias` (No aplica = `ot_no_evaluables` − pendientes − vencidas). **Carga perezosa**:
  el clic en una card abre SU tabla (una a la vez; ✕ o re-clic cierra) y SOLO entonces
  se consulta `/pendientes` — nada de tablas con la carga de la página; con la tabla
  abierta, un cambio de filtros re-consulta (firma de filtros vs la de los datos
  cargados). La tabla conserva el UX: encabezados ordenables (asc→desc→origen),
  buscador guía/destinatario/ciudad, guía clicable → Trazabilidad, zebra+hover, y
  **botón Excel** que baja TODAS las guías de la sección con los filtros del dashboard
  aplicados (SheetJS dinámico; el buscador de la tabla NO filtra el export). «No
  aplica» no lleva columna de días (no tiene límite evaluable) y va de la más reciente
  a la más vieja; en curso con el camión Lottie mientras carga.
- **Guía clicable → Trazabilidad (2026-11-10)**: en el modal del detalle del día
  (zoom de día del OT) y en las tablas de pendientes, la guía es un LINK (solo
  `modoExterno`) que abre `?pestana=trazabilidad&guia=<nº>` en una **pestaña nueva**
  del navegador; la pestaña Trazabilidad al montar con `?guia=` **consulta esa guía de
  una vez** y limpia el parámetro (volver a la pestaña no re-consulta). ⚠️ Toda
  consulta exitosa de guía **invalida los datos de las tablas de Operación**
  (`firmaPendCargados=null`): la consulta puede haber DETECTADO/persistido una
  anulada (→ segmento cambiado) y la tabla en memoria quedaría vieja.
- **Camión Lottie de marca en la espera de la consulta (2026-11-10)**: al
  Consultar, en vez del texto plano sale un camión animado (caja verde #98C74C +
  cabina oscura + ruedas girando + pista + rebote; loop 2 s). Componente
  reutilizable `Componentes/CamionCargando` (`{texto, alto}`), dependencia
  **`lottie-react`** con import DINÁMICO al montar (~250KB fuera del bundle
  inicial, patrón XLSX/@react-pdf); `camion.json` es Lottie artesanal (builder,
  sin asset externo). Respeta `prefers-reduced-motion` (🚛 + texto). ⚠️ **Trampa
  Lottie**: el `tr` (transform) de cada grupo debe ser el ÚLTIMO item DENTRO de
  `it` — como hermano de `it` lottie lo ignora y NO renderiza nada (0 `<path>`,
  sin error). Para validar Lotties sin abrir el navegador: Edge headless
  `--dump-dom` (contar `<path>`) + `--screenshot` (análisis de píxeles); jsdom
  no sirve.
- **Tarjeta en el menú `/indicadores`**: para ADMIN, VISUALIZADOR, ANALISTA y
  OPERATIVO («Mis
  Indicadores (portal clientes)» → `/indicadores/mis-clientes`, `indicadoresAdmin`
  en `page.tsx`) — el ADMIN la usa para probar el portal; el perfil CLIENTE no ve
  ese menú (login directo +
  redirect).
- **Tema CLARO del portal (2026-11-05)**: el dashboard externo pasó de oscuro (2026-10-30)
  a blancos/tonos suaves con el verde de marca `#98C74C` — clase raíz renombrada
  `DC-portalOscuro`→**`DC-portalClaro`** (header blanco con filo inferior verde, tarjetas
  blancas, pestañas/toggle/Filtrar activos en verde con texto `#0f1928`, chips semánticos
  con fondo claro y el color del segmento, tacómetro SVG en grises claros con cubo blanco
  — colores INLINE en el TSX). Texto verde pequeño sobre blanco: `#5e8a28` (#98c74c puro
  da contraste ~2:1). El dashboard interno (`/indicadores/clientes/<id>`) NO cambió.
- **Menú hamburguesa del portal (2026-11-05)**: el CLIENTE externo NO ve «Mis indicadores»
  (su portal ES esa ruta; le queda solo «Cerrar sesión», sin divisor huérfano) —
  ADMIN/VISUALIZADOR siguen viéndolo para volver al panel.
- **Pestañas del portal (2026-10-30, noche; 3 pestañas desde 2026-10-31)**: todo el
  contenido externo vive con selector arriba (`DC-pestanas`): **📊 Gerenciales**
  (tacómetro + meta, OT por período, ciudad/destinatario — todo lo de abajo),
  **🚛 Operación** — cards de atención del período filtrado (2026-10-31: 📦 pedidos
  entregados / 👥 destinatarios atendidos / 📍 ciudades atendidas; `DC-opStats`/
  `DC-opStat*` con shimmer al cargar; los conteos viajan en el resumen de `/guias`
  como `destinatarios_atendidos`/`ciudades_atendidas` = distintos sobre las guías
  ENTREGADAS — NO sirve `len(ot_por_*)`, esos grupos excluyen a los sin evaluables) —
  y **🧭 Trazabilidad** (2026-10-31: la consulta de guía nació como «Operación» y se
  movió aquí), con la **consulta de guía en Siscore**: input + botón → `GET
  /indicadores-cliente/{id}/consulta-guia?guia=` que llama al MISMO SOAP del bot de
  WhatsApp (`Funciones.siscore_ws_tracking.consultar_guia_ws`, token
  `SISCORE_SOAP_TOKEN`, proxy Vulcano) y devuelve el payload crudo; el frontend lo
  **(2026-11-13)** TODO el enriquecimiento del endpoint vive en el helper
  COMPARTIDO **`Funciones/trazabilidad_guia.py`** (`enriquecer_consulta_guia`) —
  el BOT de WhatsApp (flujo 🧾 CLIENTE) muestra la misma Trazabilidad (veredicto
  OT, cita, vehículo Monitor, evidencia…) para CUALQUIER guía, con
  `formatear_trazabilidad_guia` (`Funciones/siscore_ws_format.py`); el endpoint
  delega con `nombres_cliente=pg_nombres` (el bot no filtra) y la URL de
  evidencia digitalizada ahora la computa el backend (`payload.evidencia_url`).
  Tests: `tests/test_trazabilidad_guia.py`. Dep solo backend (el portal no
  cambia; la respuesta del endpoint queda igual). El frontend lo
  pinta como tarjeta (campos guía/cliente/servicio/piezas/fechas/estado,
  origen/destino/recibió, timeline de Movimientos y link a
  `cons_publica.php`). Valida `usuario` como /guias (perfil CLIENTE) y formato de
  guía (4-30 alfanum). Enriquecido (2026-10-30, tarde): **placa** de la guía
  desde `informe_guias_tms` (`guia_tms_placa`, + `ciudad_destino_tms`; desde
  2026-10-31 también estado/fecha_entrega/fecha_cita). Con **veredicto OT**
  (2026-10-31): `payload.ot` = correr la MISMA `_calcular_ot_filas` del informe
  sobre la fila del TMS → `{ot, fecha_emision, fecha_entrega, fecha_promesa,
  origen_promesa, dias_habiles, entregada}`; en el resumen de la consulta hay un
  bloque «On Time» junto al estado actual: chip ✔ Cumplió (verde) / ✘ No cumplió
  (rojo) / ✔ Justificada (morado — la novedad Mongo rescata una no cumplida) /
  Sin evaluar (gris), línea de fechas entrega↔promesa y title con origen de la
  promesa y días hábiles. El
  resultado pinta (orden de lectura, rediseño UX 2026-10-30 v3): **RESUMEN**
  (`.DC-opResumen`: destino principal con chips 📍 ciudad / 🚛 placa / Recibió +
  **badge de ESTADO ACTUAL** con fecha — verde si contiene «entrega», ámbar en
  curso; en móvil se apila y el estado pasa a fila debajo), **panel morado de
  novedad** (icono 🚧 + causal + justificación + quién la registró — sin la
  palabra «justificada») si hay registro en `novedades_justificadas_ot`
  (2026-11-02: va tras el link de seguimiento/evidencia y ANTES de «Otras
  guías del vehículo»),
  **línea de tiempo DUAL con la misma data** (mismo breakpoint 768px del hook
  `useEsMovil`): escritorio = tracking HORIZONTAL (cajas 📦, scroll interno si
  sobran, último destacado); **(2026-11-25) detalles de movimiento largos** (ej.
  guía 7220123570 «SOLICITA RECOLECCION EL DÍA…») se truncan con «…» —
  escritorio 2 líneas (alto fijo 2,6em: los marcadores siguen alineados) y móvil
  vertical 3 — y **CLIC/TOCA sobre el título lo EXPANDE en sitio** (estado
  `movAbierto`, resetea con cada consulta; clase `DC-opTlAbierto` sin clamp con
  fondo `#f1f5f9`; el hover ya tenía el completo en `title`); los títulos van
  en **minúsculas con SOLO la primera letra mayúscula** (`text-transform:
  lowercase` + `::first-letter uppercase` — el WS manda TODO EN CAPS) y el
  tamaño bajó un punto (escritorio 0,72rem / móvil 0,75rem); **móvil = timeline
  VERTICAL** (`.DC-opTlVert`:
  cajas a la izquierda sobre línea vertical, nombre+fecha a la derecha, sin
  scroll horizontal — v3 revirtió el «horizontal siempre»), «📄 Datos de la
  guía» (sin Estado/Fecha estado — viven en el badge) y **sección «🚛 Otras
  guías del vehículo»** con las demás guías de la MISMA placa emitidas el
  MISMO día (`guias_placa_dia` sobre `informe_guias_tms`; cada fila al tocarla
  consulta esa guía), + link de seguimiento. **(2026-11-02)** El link de
  seguimiento va ANTES de «Otras guías del vehículo» (tras «Datos de la guía»)
  y cuando el estado contiene **«DIGITALIZADA»** se REEMPLAZA por **«🖼️
  Evidencia de entrega digitalizada»**: URL pública de Siscore
  `…/app/<token>/<token>/imagenes/<año>/<mes>/<día de EMISIÓN>/<guía>.png`
  (emisión del TMS `ot.fecha_emision`, fallback `FecEnv` del WS; solo si es ISO
  evaluable, si no cae al tracking normal). Detalles del tracking
  horizontal (v2): marcador CAJA 📦 sobre línea verde tenue, título con ALTO
  FIJO de 3 líneas (marcadores alineados, nombre completo en title),
  scroll-snap x proximity, arranque en el estado más reciente
  (`scrollLeft = scrollWidth`) y hint «← Desliza…» solo si sobran pasos.
  Textos SIN mencionar Siscore/WhatsApp; errores genéricos
  (el detalle queda en log). El dashboard interno no tiene pestañas.
- **Portal rediseñado (2026-10-30, tarde)**: el dashboard externo pasó a **tema
  OSCURO** (raíz `.DC-portalOscuro`: fondo degradado #070d18→#0a121f, tarjetas
  #101a2c, textos claros, acento verde #98c74c en Filtrar/toggle activo — overrides
  SCOPED a esa clase; el dashboard interno no cambia) y **ANCHO COMPLETO**
  (`.DC-main` sin max-width). **Título** «TMS + nombre del cliente» (header y
  `document.title`). Se QUITÓ el «Informe de guías TMS» del portal
  (`{!modoExterno && ...}` — su fetch sigue alimentando las series del OT); en su
  lugar: **① tacómetro del OT** — SVG puro (`TacometroOT`, 2026-10-30 v2: se
  reemplazó el RadialBarChart que no se veía como velocímetro): dial de 240°,
  pista + arco de avance con umbrales PROPIOS alineados a la meta (2026-10-30:
  **≥96 verde · 90-95,9 naranja #ea580c · <90 rojo** — `colorMetaOT`, escala
  compartida con las barras del vs meta; NO es `colorOT`, que siguen usando las
  barras de ciudad/destinatario),
  ticks mayores con cifras 0..100 + menores, aguja con cubo central y % grande
  al pie; muestra el % del DÍA en zoom o del período filtrado) **de primeras**,
  al lado (derecha en ≥1200px, mismo grid) del gráfico **«🎯 On Time vs meta»**
  (2026-10-30 v5): una barra por período con el % de cumplimiento (misma
  fórmula del tacómetro), **ReferenceLine punteada en la META 96%**
  (`META_OT`, verde #98c74c, label «Meta 96%») y **barras con la MISMA escala
  `colorMetaOT` del tacómetro** (≥96 verde / 90-96 naranja / <90 rojo — antes
  solo rojo/verde); comparte vista Mensual/Diario y
  serie con el gráfico OT principal y sigue el zoom de día; etiquetas **%
  DIAGONALES DENTRO de cada barra** (2026-10-30: `etiquetaVerticalMeta`, rotadas
  -45° de abajo hacia arriba, blancas, 1 decimal coma, fuente 13; barras <40px
  de alto no llevan etiqueta), tooltip con delta vs meta; skeleton doble al cargar;
  **② barras horizontales «OT por ciudad de destino»** y **③ «OT por
  destinatario»** — top 12 **por volumen** evaluado (ordenar solo por % llenaba
  el top de destinos de 1-2 guías al 100%), **ordenadas del MÁS cumplido al
  menos**, % como longitud y etiqueta, color por umbral, tooltip con
  cumplieron/justificadas/no cumplieron. **Doble render** (2026-10-30 v3):
  escritorio = recharts horizontal; **móvil (≤768px) = lista táctil propia**
  (`useEsMovil` por matchMedia): nombre a la izq. con ellipsis + % a la der.
  (1 decimal, coma) + **barra a ANCHO COMPLETO** sobre pista 0-100 (misma
  escala que el eje de escritorio), fila de 44px táctil y **tap que despliega
  el detalle** (cumplieron/justificadas/no cumplieron/totales — reemplaza el
  tooltip hover). ⚠️ Trampa CSS: para que el ellipsis funcione TODA la cadena
  flex/grid necesita `min-width: 0` (portalGrid → container → lista →
  filaTop → nombre) y la pista `width: 100%` (stretch no aplica solito dentro
  de `<button>`). **Scroll INTERNO con altura acotada** (2026-10-30 v4):
  ambas tarjetas quedan de **altura idéntica** (grid `align-items: stretch` +
  `DC-otCampoScroll` `max-height:560px` en escritorio / `60vh` en móvil, con
  scrollbar oscura propia) — la página completa ya no crece con la lista
  (~35.000px → ~1.800px). **SIN tope**
  (2026-10-30: el usuario pidió TODAS las opciones y acepta scroll — un año
  Kabi completo son ~120 ciudades y ~1.300 destinatarios; el gráfico crece a
  ~34.000px con filas compactas de 26px cuando la lista pasa de 60; el
  payload de /guias sube a ~300KB, cacheado). Datos:
  `ot_por_ciudad`/`ot_por_destinatario` en el payload de `/guias`
  (`_ot_por_campo` sobre la pasada `minimas`; `destinatario` entró al SELECT de
  `guias_minimas_cliente`; mismo criterio del % OT — justificadas cumplen).
  **Skeletons**: mientras carga `/guias` el portal muestra `SkeletonTacometro`
  (título + dial shimmer `DC-skelDial`) y dos `SkeletonGrafico bajo` dentro del
  grid (mismo `cargandoGuias` que el gráfico OT; en tema oscuro los skeletons
  ya tienen overrides).
- **Logos del panel**: la tarjeta del cliente usa el MISMO registro `clientes.ts` del
  panel interno (`obtenerCliente(cliente_id).logo`, `next/image` + `unoptimized`).
  Con logo: SOLO logo + «Ver indicadores →» (sin nombre — igual que el panel interno);
  sin logo en el registro: icono edificio + nombre; sin dashboard: candado + nombre +
  «Próximamente». Al publicar un cliente nuevo, agregarlo a `clientes.ts` le hereda
  logo en ambos paneles.
- **Login directo al tablero (2026-10-30)**: el branch CLIENTE de LoginUsuarios
  consulta `GET /mis-indicadores?usuario=` tras el login; si el usuario tiene
  **UN solo cliente asignado CON dashboard** (`cliente_id` no nulo), aterriza
  directo en `/indicadores/mis-clientes/<cliente_id>` sin pasar por el panel de
  selección (con **confetti**, mismo del selector de portales, y redirect a
  800ms para que se vea); con 0, varios o clientes sin dashboard → al panel
  como siempre (y si la consulta falla, al panel — degradación segura).
- ⚠️ **Trampa login**: el backend rellena `clientes: ['KABI']` por default cuando la
  lista va vacía (`or ["KABI"]`); el branch CLIENTE del login pisa las cookies con
  `MIS_INDICADORES` para no dejar un portal clásico colgado. ⚠️ `POST /baseusuarios/login`
  hace `encontrado["regional"]` directo — un usuario sin `regional` da 500.
- **Publicar el tablero de un cliente nuevo**: entrada `nit` en `CLIENTES` (backend) +
  entrada en `clientes.ts` (logo/nombre) + ruta estática
  `src/app/indicadores/mis-clientes/<cliente-id>/page.tsx` con
  `<DashboardCliente clienteId="..." modoExterno />`.

---


## 4. Módulo Costo de Operación

**Datos:** MongoDB, **tres** colecciones que representan las etapas del viaje:

| Etapa | Colección | Campo $ sumado (costo) | Campo $ sumado (cajas) | Cliente | Fecha eje |
|---|---|---|---|---|---|
| **Media milla** (salida Funza) | `pedidos_completados` | `total_flete_vehiculo` (costo real: flete+desvío+puntos+cargue) | `total_cajas_vehiculo` | `nit_cliente` → `nombre_cliente` (lookup `clientes`) | `fecha_creacion` (string local `YYYY-MM-DD HH:MM:SS`) |
| **Última milla** (cross-docking CDI) | `pedidos_medical_historico` | `total_solicitado` | `piezas` | `cliente_origen` (expandir fusionadas) | `fecha_movimiento_historico` (UTC, −5h) |
| **Otros costos** (cierre de entrega) | `historico_otros_costos` | `valor_total` | `datos_servicio.piezas` | `datos_servicio.cliente` | `created_at` (UTC, −5h) |

> **Concepto clave — las 3 etapas se SUMAN.** Son piernas distintas del mismo viaje: un mismo
> cliente puede tener costo en las tres y se acumulan (NO es doble conteo). Por eso ningún cliente
> se excluye por fuente.

> ⚠️ `pedidos_completados` tiene **varios docs por vehículo** (uno por pedido) con los totales del
> vehículo duplicados; la agregación **agrupa por `consecutivo_vehiculo`** antes de sumar (si no, se
> sobreconta). `total_flete_vehiculo` YA es el costo real total del vehículo a pesar del nombre (ver
> `pedidos.py:813`). La fecha `fecha_creacion` se guarda/filtra como string **local sin offset**, así
> que el bucket mensual/diario se saca directo del prefijo del string (NO se resta 5 h, a diferencia
> de las otras dos colecciones que sí son UTC).

**Sobrecosto y ahorro (gráfico 2):** el backend devuelve el **bruto** por período
(`sobrecosto = Σ diferencias > 0`, `ahorro = Σ diferencias < 0`, negativo) y el **frontend muestra el
NETO** (`sobrecosto + ahorro`) como una sola barra con signo: rojo arriba si el neto es sobrecosto,
verde abajo si es ahorro (ej. +5000 y −6000 → −1000 → barra verde bajo el eje 0). Media milla usa
`diferencia_flete` (= costo_real − costo_teorico por vehículo); última milla usa `diferencia` (=
total_solicitado − tarifa_calculada). Otros costos no tiene tarifa teórica → no aporta a este gráfico.
El tooltip además muestra el bruto (sobrecosto y ahorro por separado).

### Etiquetas parametrizables (2026-08-11)

Los nombres visibles de las 3 etapas ("Media milla", "Última milla", "Otros costos") son
**parametrizables** y viven en el backend, en el dict `ETAPAS` (`indicadores_costo_operacion.py`).
`/resumen` los devuelve en `data.etiquetas`. El frontend los lee en `lbl` (con fallback local
`ETIQUETAS_DEFAULT`) y los usa en tooltip, leyenda, subtítulo, cabeceras CSV y los `name=` de los
`<Bar>`/`<Line>`. **Para renombrar lo que ve el usuario (p. ej. "Media milla" → "Fresenius kabi"),
solo se edita el valor en `ETAPAS` y se reinicia el backend** (Render); sin rebuild/redeploy del
frontend estático. Las **claves técnicas** (`media_milla`, `ultima_milla`, `otros_costos`) son fijas
(contrato backend↔frontend) y NO se cambian.

### Fusión (regla de cliente) — CLAVE, no obvia desde el código

`pedidos_medical_historico` guarda planillas **fusionadas** (varias planillas de distintos clientes
bajo un mismo consecutivo). El doc fusionado tiene `cliente_origen` **concatenado** (ej.
`"FRESENIUS KABI, DAVITA"`) y los clientes/piezas reales en `fusion_info.datos_originales[]`. Para
que el filtro y las cifras respeten el cliente real (y no vuelquen toda la fusionada en un solo
bucket o cliente equivocado), **la última milla se EXPANDE y reparte**:

- `_expr_ultima_milla_expandido()` (para `/resumen`): produce un sub-doc por original
  `{cliente, costo (total_solicitado), piezas, diferencia}` repartido proporcionalmente a las piezas
  del original (equitativo 1/n si la suma de piezas es 0). Se usa con `$project` + `$unwind` antes
  del `$group` por período.
- `_expr_costo_caja_ultima_expandido()` (para `/costo-por-caja`): igual pero `{cliente, costo, piezas}`.
- `_expr_clientes_expandidos()` — helper histórico (era de Fletes; migrado aquí al eliminarse ese
  módulo): sub-docs `{cliente, flete, sobrecosto}` para listado de clientes. Mismo reparto por
  piezas, sin residuo exacto — la pérdida de redondeo es despreciable a nivel indicador.

**Filtro de cliente post-expansión (CLAVE para consistencia):** cuando hay filtro de cliente, se
aplica un `$match` **después** del `$unwind` sobre `$_exp.cliente`, reteniendo solo la porción del
cliente filtrado. Así se **excluyen los "compañeros de fusión"** (los otros clientes que viajan en la
misma planilla). Sin este filtro post-expansión, filtrar "Fresenius Medical Care" mostraría también
las cajas/costo de Kabi, Davita, Congrupo, etc. que iban en planillas fusionadas con Medical Care.
El `$match` previo (sobre `cliente_origen` y `fusion_info.datos_originales.cliente_origen`) deja
entrar la fusionada completa; el `$match` post-expansión la recorta al cliente real.

> ⚠️ Trampa de nomenclatura: en `pedidos_completados` el campo `regional` NO es la regional logística
> (bodega/origen); la regional va **embebida en `consecutivo_vehiculo`** (formato
> `<ORIGEN>-<FECHA>-<X>-<FECHA>-<REGIONAL>-<N>`), y se extrae como el anteúltimo segmento del split
> por `-` (la regional no contiene guiones; puede tener espacios, ej. `EJE CAFETERO`).

**Archivos**
- Backend: `integrappi/rutas/indicadores_costo_operacion.py` (`prefix=/indicadores-costo-operacion`).
  Reutiliza de `rutas.siscore_consultas`: `coleccion_historico`, `_OFFSET_COLOMBIA`.
- Frontend: `src/app/indicadores/costo-operacion/page.tsx` → `src/Paginas/IndicadoresCostoOperacion/index.tsx` + `estilos.css`.

### Endpoints

- `GET /indicadores-costo-operacion/resumen` — consulta las 3 colecciones en paralelo
  (`asyncio.to_thread` + `asyncio.gather`, `allowDiskUse=True`) y combina. Filtros: `anio[]`,
  `mes[]`, `cliente[]`. Devuelve `{ success, data: { serieMensual, serieDiaria, anios, clientes,
  etiquetas } }` donde cada bucket lleva `media_milla`, `ultima_milla`, `otros_costos`, `total`,
  `cajas_media`, `cajas_ultima`, `cajas_otros`, `total_cajas`, `sobrecosto`, `ahorro`.
  - El filtro por fuente difiere (campos/fechas distintas): media milla filtra `fecha_creacion`
    (string; año por rango, mes por `substrCP`) + `nombre_cliente` tras `$lookup` a `clientes`;
    última milla y otros costos usan `_filtro_datetime_utc` (rango UTC alineado a día Colombia).
  - **Última milla expande fusionadas** (`_facet_ultima_milla_expandido`, con filtro post-expansión
    por cliente); media milla y otros costos no tienen fusionadas.
  - **Merge genérico** por período: cada fuente aporta sus medidas y se suman (`sobrecosto`/`ahorro`
    combinan media + última milla).
  - `anios` (unión global con datos) y `clientes` (respetan año/mes, **no** el filtro de cliente).

- `GET /indicadores-costo-operacion/costo-por-caja` (2026-08-11) — costo por caja por período con
  **reglas por cliente**. Filtros: `anio[]`, `mes[]`, `cliente[]`. Devuelve
  `{ success, data: { mensual:[{periodo, costo_por_caja, costo_kabi, costo_otros, cajas_kabi,
  cajas_otros}], diario:[...] } }`.
  - `costo_por_caja = (Σ costo_kabi + Σ costo_otros) / (Σ cajas_kabi + Σ cajas_otros)` por período
    (promedio ponderado, división protegida → 0).
  - **Reglas** (cada dólar/caja va a un solo grupo, sin doble conteo):
    - **Fresenius Kabi** (NIT `900402080` en media milla; nombre `"FRESENIUS KABI"` normalizado con
      `$toUpper`+`$trim` en última milla y otros): `cajas_kabi` = media milla (`total_cajas_vehiculo`);
      `costo_kabi` = media milla + última milla + otros costos.
    - **Otros clientes**: `cajas_otros` = última milla (`piezas`); `costo_otros` = última milla +
      otros costos (sin media milla — la regla excluye media milla del costo/cajas de otros clientes).
  - Identificación de Kabi: `_EXPR_KABI_NIT` (por `nit_cliente` en media milla) y
    `_expr_kabi_nombre(campo)` (por nombre en última milla/otros). Distingue de Fresenius Medical
    Care (NIT `901689684`).
  - Última milla expande fusionadas con filtro post-expansión por cliente. **Con filtro de cliente**,
    el cálculo se acota a ese cliente (igual que el resto del módulo). **Sin filtro**, combina los
    dos grupos en un solo ratio.
  - Cada colección aporta solo a los acumuladores que le corresponden: media milla → solo kabi;
    última milla → costo a ambos, cajas solo a otros; otros costos → solo costo (sin cajas).
  - **Nota**: el dinero total (`costo_kabi + costo_otros`) coincide con el `total` de `/resumen`,
    pero se reparte por **cliente** (no por etapa), porque el denominador (cajas) depende del cliente.

- `GET /indicadores-costo-operacion/pedidos-por-analista` (2026-08-21, seriado el mismo día) —
  **cuántos pedidos atendió cada analista** (perfil `ANALISTA` o `ADMIN`, cruce en Python con
  `baseusuarios` sin filtrar `activo`) **por período**. Mismos filtros `anio[]`/`mes[]`/`cliente[]`
  que `/resumen` (misma semántica de filtro por fuente, incl.
  `fusion_info.datos_originales.cliente_origen` en última milla, pero **sin expansión**: se cuentan
  docs, no dinero). Devuelve `{ success, data: { usuarios, serieMensual, serieDiaria, analistas,
  etiquetas } }`:
  - `usuarios`: orden fijo de las series (total descendente; "Sin asignar" al final). Cada entrada
    lleva `usuario` (o null), `nombre` (rótulo visible) y **`columna`** — la clave EXACTA de ese
    usuario en los buckets de las series (el código o `"Sin asignar"`): el frontend la usa como
    `dataKey` y `nombre` solo para leyenda/CSV. ⚠️ No usar `nombre` como clave del bucket (dos
    usuarios pueden llamarse igual, y no coincide con la columna).
  - `serieMensual`/`serieDiaria`: buckets `{periodo, <columna>: n, ..., total}` — misma forma que
    las series de `/resumen`, pero una columna por analista en vez de por etapa. Ambas salen de las
    mismas queries diarias; el mensual agrupa por el prefijo `YYYY-MM` en Python.
  - `analistas`: ranking `{usuario, nombre, perfil, pedidos}` del período filtrado (CSV/auditoría).
  - **"El analista que atiende" = quien hace el trámite Vulcano** de cada etapa. Unidad de conteo:
    **1 por documento** (en media milla NO se deduplica por `consecutivo_vehiculo` — cada doc es un
    pedido atendido; una planilla fusionada cuenta 1). Las 3 etapas se SUMAN por analista (un doc
    solo vive en una etapa, no hay doble conteo).

    | Etapa | Campo del tramitador | Dónde se llena |
    |---|---|---|
    | Media milla | `pedido_actualizado_vulcano_por` | `/cargar-masivo` (`pedidos.py`, subida del Excel Vulcano) |
    | Última milla | `usuario_pedido_vulcano` | `_procesar_pedido_vulcano` (`siscore_consultas.py`) |
    | Otros costos | `tramite_vulcano_info.usuario` | `/marcar-tramite-vulcano` (`otros_costos.py`) |
  - Los `$group` normalizan el usuario con `$toUpper`+`$trim` (`_expr_usuario_normalizado`) y
    `_grupo_usuario_o_senal` manda los vacíos a la señal interna `(SIN ASIGNAR)`: el campo guarda
    casing según el origen (cookie en última milla, valor canónico en las otras).
  - Docs sin tramitador registrado, otros perfiles o usuarios eliminados de `baseusuarios` →
    columna **"Sin asignar"** (`usuario: null`, siempre al final, gris `#64748b` en el gráfico).

**Vistas (orden del dashboard, responsive):**
1. **💰 Costo total de la operación** — barras **apiladas** con toggle **Mensual (default) / Diario**
   (propio de este gráfico). Tres `<Bar stackId="a">` (etiquetas `lbl.*`): media milla (azul), última
   milla (naranja), otros costos (aqua). La última barra lleva `<LabelList dataKey="total" position="top">`
   con el total encima. Subtítulo que mapea cada color a su etapa. Tooltip custom (las 3 etapas +
   total) y export CSV (incluye cajas por etapa, sobrecosto/ahorro).
2. **📉 Sobrecosto y ahorro de la operación** — barras **divergentes sobre el eje Y=0**
   (`<ReferenceLine y={0}>`), una por período con el **neto** (`sobrecosto + ahorro`), coloreada con
   `<Cell>` por signo: rojo por encima de 0 (sobrecosto neto) y verde por debajo (ahorro neto). Su
   **propio toggle** Mensual/Diario. Etiqueta adaptativa. Tooltip con el neto y el bruto desglosado.
3. **📦 Cantidad de cajas por etapa** (2026-08-11) — **`LineChart`** con 3 líneas (una por etapa,
   colores azul/naranja/aqua, etiquetas `lbl.*`) y su **propio toggle** Mensual/Diario. Muestra el
   volumen de cada etapa: media milla suma `total_cajas_vehiculo`, última milla y otros suman `piezas`
   (mezcla cajas+piezas — ver aclaración abajo). Cada punto lleva etiqueta del valor (entero corto,
   1.2k/12k). Tooltip con desglose por etapa + total. **Sin `<Legend>` nativo** (solo el subtítulo
   con los puntos de color, para no duplicar).
4. **💲 Costo por caja** (2026-08-11) — **`BarChart`** de barras verticales con su **propio toggle**
   Mensual/Diario. Una barra por período con el costo por caja, etiqueta encima con valor **completo**
   (`formatearMoneda`, no el corto). Tooltip simple: costo por caja + costo total + cajas totales del
   período (sin segregación por cliente, para no confundir).
5. **👥 Pedidos atendidos por analista** (2026-08-21; seriado por mes/día el mismo día) —
   **`BarChart` apilado por período**, igual estructura que el gráfico 1 de costo total: una pila
   por mes/día con **un segmento por ANALISTA** (la **leyenda es el usuario**), toggle **propio**
   Mensual/Diario, `<Legend>` nativo, total encima de la pila (`formatearEnteroCorto`) y export CSV
   (Fecha/usuarios/Total según la vista activa). El COLOR identifica al usuario: paleta categórica
   de 8 slots validada CVD (`#2a78d6, #eb6834, #1baf7a, #eda100, #e87ba4, #008300, #4a3aa7,
   #e34948`; `colorAnalista(i, usuario)`), asignada en orden fijo al orden de `usuarios` (total
   descendente); "Sin asignar" siempre gris neutro `#64748b`. Tooltip con pedidos por analista del
   período + total. Fetch silencioso aparte (`obtenerPedidosPorAnalista`), disparado junto con los
   otros en cada "Filtrar". `dataKey` = `u.columna` (clave exacta del bucket), `name` = `u.nombre`.

> **Aclaración cajas vs piezas (gráfico 3):** media milla guarda **cajas**
> (`total_cajas_vehiculo`); última milla y otros guardan **piezas** (`piezas`/`datos_servicio.piezas`).
> Se suman como una sola medida pero son técnicamente unidades distintas; las 3 líneas individuales
> sí son correctas (cada una suma su propio campo).

**Filtros:** Año, Mes, Cliente (multi-selección estilo Power BI, código inline con clases `IG-*`).
**Sin** Día ni Regional. El filtro de cliente **filtra toda la página**:
llega a las 3 colecciones en `/resumen` y `/costo-por-caja` (media milla por `nombre_cliente`/`nit_cliente`
tras `$lookup`; última milla por `cliente_origen` + `fusion_info.datos_originales.cliente_origen` y
filtro post-expansión; otros costos por `datos_servicio.cliente`). Al filtrar Fresenius Kabi, la media
milla (mayoritariamente Kabi) se mantiene y las otras etapas se acotan a Kabi.

**Layout:** `import '../IndicadoresOT/estilos.css'` + ajustes propios (ancho 90 % desktop,
reset de `order`), mismo chrome `IG-*` de OT. CSS propio: toggle segmentado `IG-toggle*`, badge
`IG-graficoBadge`, tooltip `IG-tooltipSerie*`, etapa `IG-tooltipEtapa`.

**Realidad de datos (2026-08-11):** media milla con datos desde 2025-09; última milla desde
**2026-06** (`pedidos_medical_historico` arranca ahí); `historico_otros_costos`
aún **vacía** (sin otros costos pagados). Fusionadas en última milla presentes desde 2026-06
(mezclan Kabi con Medical Care, Davita, Congrupo, etc.).

**Realidad de "Sin asignar" en pedidos por analista (2026-08-21):**
- **Última milla (838 docs a 2026-08):** hasta el 2026-08-21 el flujo masivo `/importar-vulcano`
  (Excel) **no enviaba usuario** — solo lo capturaba `/asignar-pedido-manual` (botón por planilla)
  — y la rama de fusiones de `_procesar_pedido_vulcano` nunca seteaba el campo en el raíz.
  **Fixeado ese día**: el endpoint acepta `usuario` (Form opcional), lo propaga y el frontend de
  SolicitudVehiculos lo envía (`formData.append('usuario', usuario)`); la fusión queda a nombre de
  quien asignó el ÚLTIMO original. El histórico previo al fix **no es recuperable** (nunca se guardó
  quién importó) → permanece "Sin asignar".
- **Media milla (479 docs):** todos los docs tienen tramitador; son perfiles fuera del filtro
  ANALISTA/ADMIN (verificado: SOVALLE = CONTROL inactivo, 193; DCASTRO = usuario ya eliminado de
  `baseusuarios`, 286). Ampliar el filtro en `_usuarios_analista_admin` los mostraría.

---

## 5. Patrones compartidos

- **Menú y envoltura:** el route page siempre es un wrapper `dynamic(ssr:false)` del componente en `Paginas/`.
- **CSS:** las clases `IG-*` (header, filtros Power-BI, tiles `IG-kpiTile` con acentos `Total/Cajas/Ton/Novedad`,
  `IG-graficoContainer`, `IG-barrasHorizontales`, modal `IG-modal*`) viven en
  `Componentes/IndicadoresChrome/estilos.css` (mudado ahí al eliminarse OT). Costo de
  Operación las reutiliza con `import '../../Componentes/IndicadoresChrome/estilos.css'`.
  El panel de Clientes usa estilos propios `PC-*` con el mismo look. **Nuevo módulo → importar
  ese CSS para no duplicar el chrome.**
- **Filtros:** dropdowns multi-selección estilo Power BI (Año, Mes, Día, Cliente).
  Patrón: estado de UI (seleccionados) + estado "aplicados" que dispara el `fetch` al cambiar.
- **Export:** botón que genera CSV con separador `;` y BOM (`﻿`) para abrir correcto en Excel-ES.

### Branding de headers — negro #000 + verde #98C74C (2026-10-31)

A pedido del usuario, **todos los headers de integrApp** se unificaron al look del
portal Mis Indicadores (`DC-header`): fondo **negro #000**, texto blanco, acento
**verde #98C74C** (la sílaba «App» del wordmark, ícono de usuario, miga activa) y
pill de usuario redondo. En los portales de indicadores se tocó SOLO el header:
- `Componentes/IndicadoresChrome/estilos.css` → **`.IG-header`** (fondo #000) y
  `.IG-brandAccent` (verde). ⚠️ El resto del chrome IG-* (filtros, botones, thead
  de tablas, footer) **sigue navy/naranja** (`#0f1928`/`#e8a000`) — pendiente si
  algún día se quiere migrar todo.
- `PanelClientes/estilos.css` → `.PC-header` (#0f1928 plano → #000) + acentos.
- `IndicadoresMisClientes/PanelMisClientes/estilos.css` → `.MC-header` + acentos.
- `src/app/indicadores/estilos.css` → `.IM-header` (menú `/indicadores`) + acento.
- `IndicadoresCostoOperacion/estilos.css` → header (línea ~145) + borde inferior.
Además, **INDICADORES es un GRUPO desplegable** del nuevo home de la Torre de
Control (`/Modulos`, ver `integrapp-next/README.md` 2026-10-31): tras el login los
perfiles multi-módulo aterrizan en `/Modulos`, donde el grupo **INDICADORES**
(2026-10-31: antes era ítem directo) contiene el sub-módulo **IntegrApp**
(`/indicadores`) + **9 tableros Power BI del área** como sub-módulos externos
(Cierre diario, Gestión de proveedores, Encuestas de satisfacción, Cumplimiento
de citas, Gestión de almacenamiento, Cumplimiento de devoluciones, On time,
Efectividad de inventarios, PQR — links de publicación `app.powerbi.com/view`,
pestaña nueva). Los links viven en `src/Funciones/menuModulos.ts` (grupo
`indicadores`); los demás enlaces externos (RNDC, Vulcano, Mayasoft, Siscore
WMS/TMS) siguen como ítems directos.

---

## 6. Cómo agregar un módulo nuevo (checklist)

1. **Backend** — crear `integrappi/rutas/indicadores_<modulo>.py`:
   ```python
   router = APIRouter(prefix="/indicadores-<modulo>", tags=["Indicadores <Modulo>"])
   @router.get("/resumen")
   def get_resumen(...): ...        # agregaciones; devolver {"success": True, "data": {...}}
   @router.get("/detalle")
   def get_detalle(...): ...        # drill-down del modal
   ```
2. **Registrar** el router en `integrappi/main.py`: import + `app.include_router(ruta_indicadores_<modulo>)`
   (junto a los otros, ~línea 44 import / ~191 include).
3. **Frontend route** — `src/app/indicadores/<modulo>/page.tsx`:
   ```tsx
   import dynamic from 'next/dynamic';
   const Cmp = dynamic(() => import('@/Paginas/Indicadores<Modulo>/index'), { ssr: false });
   export default function Page() { return <Cmp />; }
   ```
4. **Componente** — `src/Paginas/Indicadores<Modulo>/index.tsx` (copia la estructura de Costo de
   Operación: header, filtros, fetch a `/indicadores-<modulo>/resumen`, gráficos recharts, modal).
5. **CSS** — `src/Paginas/Indicadores<Modulo>/estilos.css` con
   `import '../../Componentes/IndicadoresChrome/estilos.css'` + extras.
6. **Menú** — añadir la tarjeta al array `indicadores` en `src/app/indicadores/page.tsx`
   (`id`, `titulo`, `descripcion`, `icono`, `ruta`).

---

## 7. Ejecución y despliegue

- **Frontend:** `cd integrapp-next && npm run dev` (dev) / `npm run build` (export estático para GoDaddy).
  Los componentes son `ssr:false` → no hay fetch de datos en build.
- **Backend:** `integrappi` (FastAPI). **Reiniciar el servidor tras agregar/Modificar un router** para que cargue.
  Variables: `MONGO_URI` (Costo de Operación; clientes fuente mongo), `PG_*` (clientes fuente postgres).
- **Tablas del OT en Mongo = fuente de verdad** (2026-11-12): el Excel
  `calculo real ot.xlsx` fue eliminado del repo y `importar_tablas_ot_real.py`
  quedó deprecated (delete+insert borraría ediciones manuales). Las 5
  colecciones (`tiempos_entrega` y hermanas, ver §3) se editan directo en
  Mongo; respaldo histórico del Excel únicamente en el historial de git.
- **Verificar un endpoint:** Costo de Operación:
  `GET http://127.0.0.1:8000/indicadores-costo-operacion/resumen?anio=2026` → `{"success": true, ...}`
  y `/indicadores-costo-operacion/costo-por-caja?anio=2026`.

---

## 8. Archivos relevantes

```
integrappi/main.py                                   registro de routers
integrappi/rutas/indicadores_costo_operacion.py      Costo de Operación (3 colecciones MongoDB)
integrappi/rutas/indicadores_cliente.py              Cliente + portal Mis Indicadores (/mis-indicadores, usuario en /guias)
integrappi/Funciones/trazabilidad_guia.py            enriquecimiento COMPARTIDO de consulta-guia (portal + bot WA)
integrappi/rutas/baseusuarios.py                     perfil CLIENTE + clientes_siscore (asignación por NIT, deriva portales)

integrapp-next/src/app/indicadores/page.tsx          menú (tarjetas; + tarjeta Mis Indicadores para ADMIN)
integrapp-next/src/app/indicadores/clientes/          panel + rutas por cliente
integrapp-next/src/app/indicadores/costo-operacion/   route Costo de Operación
integrapp-next/src/app/indicadores/mis-clientes/      NUEVO portal clientes externos (panel + fresenius-kabi)
integrapp-next/src/Paginas/IndicadoresClientes/       clientes.ts (registro) + PanelClientes + DashboardCliente (modoExterno)
integrapp-next/src/Paginas/IndicadoresMisClientes/    NUEVO PanelMisClientes (estilos MC-*)
integrapp-next/src/Paginas/GestionUsuariosP/          asignación de clientes (modal «👥 Clientes»)
integrapp-next/src/Paginas/LoginUsuarios/             aterrizaje directo CLIENTE
integrapp-next/src/Componentes/IndicadoresChrome/     estilos.css (chrome IG-* compartido)
integrapp-next/src/Componentes/CamionCargando/         camión Lottie de marca (lottie-react dinámico + camion.json)
integrapp-next/src/Paginas/IndicadoresCostoOperacion/ componente + estilos.css
```

---

## 9. Telemetría de uso del portal (2026-11-19)

Mide la adopción de los portales de clientes (`mis-clientes/*` y `clientes/*`): quién entra (cualquier perfil), frecuencia diaria, pestañas y acciones — para reportar números al cliente. Deploy backend+frontend JUNTOS (los beacons necesitan `POST /telemetria/evento` vivo); los datos se acumulan solo desde el deploy.

- **Captura**: beacons en `DashboardCliente` (componente compartido → los 5 wrappers; `cliente_id`+`modo` distinguen) vía `DashboardCliente/telemetria.ts` (POST `keepalive`, sesión `sessionStorage`, usuario de `usuarioPedidosCookie`). Eventos: `pageview` (con pestaña inicial), `pestana`, `consulta_guia`, `pendientes` (abrir tabla), `export_excel` (4 orígenes).
- **Backend** `rutas/telemetria.py`: colección `eventos_portal` (`fecha` UTC + `fecha_col` Colombia; **TTL 180 días sobre `fecha`**, no sobre el string), índices `(cliente_id, fecha_col)` / `(usuario, fecha_col)` / `evento` / `fecha_col`. `POST /evento` nunca falla (usuario best-effort, desconocido → `perfil: null`). `GET /resumen` solo ADMIN: kpi + por_dia/usuario/evento/pestana/cliente.
- **Consulta interna**: `/indicadores/uso-kabi` (`Paginas/UsoPortalKP`, solo ADMIN, tarjeta en el menú `/indicadores`) — filtros rango+portal, KPIs, barras por día, tabla de usuarios con días activos/última conexión/desglose, Excel de 3 hojas (SheetJS). **(2026-11-20)** Filtro **Audiencia** (todos / clientes=perfil CLIENTE / internos=resto, param `audiencia`), **foco en un usuario** (buscador por nombre con sugerencias sobre `por_usuario`, clic en sugerencia o fila de la tabla → param `usuario_filtro` acota TODO el resumen a ese usuario; chip verde «👤 … ✕ Ver todos»), la tabla SIN columna Usuario (el nombre basta; clic en la fila = foco) y las barras de «Actividad por día» muestran la **cantidad encima** de cada barra. **(2ª tanda same-day)** Migas de filtros aplicados removibles (patrón DC-miga: 📅 rango fijo + 🏢 portal + 👥 audiencia + 👤 foco; clic = quita y re-consulta), Audiencia aplica AL INSTANTE (sin «Consultar»), header de la app ESPEJO del DC-header del portal (logo Integra ONE clic→menú indicadores, título centrado, usuario con menú Menú-indicadores/Cerrar sesión, sticky) y ancho completo (sin max-width 1200), con las reglas de MÓVIL del
DC-header (≤768px: título a la 1ª fila a ancho completo, logo+usuario en la
2ª, userInfo oculto) y la tabla ordenada por **última conexión DESC** (la más
reciente arriba; orden solo en la tabla — el backend sigue ordenando por
eventos y el Excel usa ese orden). Verificado: audiencia clientes+internos cuadra con todos (26+103=129). Dep **backend+frontend JUNTOS** (backend antes si se separa: FastAPI ignora params desconocidos y el filtro no aplicaría).
- Tests: `integrappi/tests/test_telemetria_portal.py` (via `_stub_bd.py`).

---

## 11. Telemetría de uso de la app (2026-11-25)

Además del reporte de adopción del portal de clientes (`/indicadores/uso-kabi`, solo ADMIN, ver §9), existe la **telemetría GENERAL de toda la aplicación**:

- **`/indicadores/uso-app`** (`Paginas/UsoAppP`, tarjeta del menú `/indicadores` y submenú **INDICADORES → IntegrApp → Uso de la App** del hub, solo ADMIN): reutiliza el endpoint `GET /telemetria/resumen` (misma colección `eventos_portal`) con el filtro `modulo` y las agregaciones `por_modulo` y `por_hora` (hora Colombia 0-23 computada en el backend). Contiene: KPIs, **histograma de horas pico**, **barras apiladas por módulo** en «Actividad por hora» y «por día» (paleta estable: el orden de colores sigue a la tabla por módulo; eventos del portal de clientes en gris; leyenda compartida), tabla por módulo (clic = filtro), tabla por usuario, desglose de acciones y Excel de 5 hojas con columnas por módulo. **Separado de uso-kabi a propósito**: ese reporte se presenta al cliente Kabi y no debe mezclarse con eventos internos.
- Captura de datos: componente global `TelemetriaGlobal` (un pageview por cambio de ruta, módulo derivado de la URL) + `trackAccion` (~20 acciones clave: exports, búsquedas, OC, usuarios, logins) + los beacons del propio portal Kabi. Los eventos del portal Kabi previos al 25-nov-2026 no traen `modulo` ni `hora_col` (se clasifican como `portal_<cliente_id>`; quedan fuera del histograma por hora).
- **Menú del hub**: las opciones internas de IntegrApp (por cliente, costo operación, mis indicadores, uso-app, uso-kabi) también cuelgan como **submenú del ítem IntegrApp** en el grupo INDICADORES de `/Modulos`, con las mismas visibilidades que las tarjetas de `/indicadores` (esa página sigue existiendo: es el hogar del VISUALIZADOR). Ver `menuModulos.ts` (`ind_*` en el mapa de visibilidad).
