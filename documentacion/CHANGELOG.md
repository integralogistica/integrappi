# Changelog - Integra Sistema de Gestión

Todos los cambios notables del proyecto se documentan en este archivo.

## [2026-11-23] - Auditoría de tarifarios: CONTROL edita /Tarifas y /GestionTarifasRutas + historial de cambios

### ✨ Perfil CONTROL en ambos tarifarios
- `/GestionTarifasRutas` pasa de solo ADMIN a **ADMIN + CONTROL** (guard de `GestionTarifasRutasP`; el nav de Medical Care ya mostraba el ítem a CONTROL).
- `/Tarifas` queda **solo lectura** para perfiles ≠ ADMIN/CONTROL (sin botones de Nueva tarifa/Editar/Eliminar/Cargar Archivo; Exportar sigue para todos). El ítem «Tarifas» del menú de usuario de Pedidos y PedidosCompletados pasa de solo ADMIN a ADMIN+CONTROL.

### ✨ Colección nueva `historial_tarifas` (auditoría: quién/cuándo/qué)
- Un documento por evento: `{accion (creacion|edicion|eliminacion|carga_masiva), tarifario (fletes|fletes_rutas_fmc), registro ("ORIGEN → DESTINO" / "CC → RUTA"), registro_id, usuario, nombre_usuario, rol, fecha UTC, campos_modificados [{campo, valor_anterior, valor_nuevo}], snapshot (eliminaciones: doc completo), resumen (carga masiva: insertados/errores/borrados + snapshot_borrados en fletes, cuyo masivo REEMPLAZA TODO), ip}`. Sobrevive a eliminaciones de tarifas.
- Módulo nuevo `integrappi/rutas/historial_tarifas.py`: helpers `_resolver_usuario`/`_requiere` (patrón otros_costos — perfil REAL de baseusuarios, el backend no confía en el frontend), `_diff_campos` (ignora `_id`), `_registrar_evento` (nunca rompe el flujo de negocio).
- Escrituras de `/fletes` y `/fletes-rutas-fmc` (POST/PUT/DELETE/cargar-masivo): `usuario` **obligatorio** (Query en REST, Form en masivos) + guard `PERFILES_EDICION_TARIFAS = {ADMIN, CONTROL}`. Los GET no cambian (Pedidos lee `tarifas` directo por Mongo). `/fletes/cargar-otros-costos` no se toca.
- Nuevo router **`GET /historial-tarifas/`**: `usuario` (obligatorio), `tarifario`, `registro` (historial de UNA tarifa), `accion`, `incluir_masivas` (requiere tarifario), `limite` (máx 2000) — eventos más reciente primero, solo ADMIN/CONTROL.

### ✨ UI de historial
- Botón **«Historial»** en ambas páginas (ADMIN/CONTROL) → `Componentes/ModalHistorialTarifas`: fecha (hora Colombia), usuario+rol, acción con color, diffs antes→después, snapshot expandible de eliminaciones, filtro por tarifario y **export a Excel**. Es el único lugar donde se ven eliminaciones y cargas masivas.
- **«Historial de esta tarifa» dentro del modal de edición** de ambos tarifarios (`HistorialPorRegistro`): línea de tiempo de lo que ha sufrido esa tarifa; en `/Tarifas` incluye las cargas masivas (reemplazan todo el tarifario); en GTR se ancla a los valores originales (`modalEditar`).
- API clients `fletes.tsx`/`tarifasRutasFmc.tsx` envían `usuario` (cookie `usuarioPedidosCookie`) en toda escritura; nuevo `historialTarifas.tsx`.

### 🔧 Archivos modificados
- `integrappi/rutas/historial_tarifas.py` (nuevo), `rutas/fletes.py`, `rutas/tarifas_rutas_fmc.py`, `main.py`
- `integrappi/tests/test_historial_tarifas.py` (nuevo, 26 tests)
- `integrapp-next/src/Componentes/ModalHistorialTarifas/` (nuevo), `src/Funciones/ApiPedidos/historialTarifas.tsx` (nuevo), `fletes.tsx`, `tarifasRutasFmc.tsx`
- `integrapp-next/src/Paginas/TarifasP/index.tsx`, `src/Paginas/GestionTarifasRutasP/index.tsx`, `src/Paginas/Pedidos/index.tsx`, `src/Paginas/PedidosCompletados/index.tsx`

⚠️ Deploy **backend PRIMERO**, luego frontend (si el frontend nuevo llega antes, toda escritura de tarifas falla con 422 por falta de `usuario`).

---

## [2026-10-06] - Excel de Otros Costos: fechas dd/mm/aaaa sin hora

### ✨ Fechas del Excel de Otros Costos sin hora (backend)
- El Excel de /OtrosCostos (`POST /otros-costos/exportar-excel`, `integrappi/rutas/otros_costos.py`) mostraba las fechas como ISO completo con hora (`2026-10-05T21:30:00`). Ahora las columnas **Fecha Creación**, **Fecha Aprobación** y **Fecha Pago** salen como **dd/mm/aaaa**.
- Nuevo helper `_fecha_ddmmyyyy()`: convierte la fecha UTC (naive) de Mongo a zona Colombia (−5 h) antes de formatear, y también acepta strings ISO (con o sin `Z`). `_fecha_a_str()` queda intacto para el resto de usos (serialización API/UI).
- PENDIENTE deploy en Render.

## [2026-09-26] - % Uso del vehículo en 4 páginas, filtro y orden por columna, regionales de la operación y fix de zona horaria

### ✨ Columna «% Uso» del vehículo solicitado (Pedidos, PedidosCompletados, SolicitudVehiculos, HistoricoPedidos)
- `% uso = kg reales ÷ tope del tipo solicitado × 100`, con los mismos topes del tablero de costo-operación (`TOPES_TIPO_VEH`: CARRY 1.000 … TRACTOMULA 34.000). Tipo = `tipo_vehiculo_sicetac` con respaldo `tipo_vehiculo` (split por `_`); sin tope conocido → `—` gris.
- Se pinta como **badge con FONDO de color y letra blanca** (rango de los gráficos): rojo `#e34948` < 80%, verde `#1baf7a` ≥ 80%.
- Helper compartido `integrapp-next/src/Funciones/usoVehiculoSolicitado.ts` (`usoVehiculoSolicitado`, `colorUsoVehiculo`, `badgeUsoVehiculo`, `cumpleFiltroUsoVehiculo`), espejo de `TOPES_TIPO_VEH`/`_tipo_solicitado` del backend.
- Posición: a la izquierda de Estado(s) en Pedidos, SolicitudVehiculos (tabla de resultados) e HistoricoPedidos; en PedidosCompletados quedó antes de «Flete Solicitado». La tabla de solicitudes pendientes (sin columna Estado) la lleva tras «Tipo Vehículo». También en el modal de detalle del Histórico. Ajuste de `colSpan` en las filas de detalle expandibles.

### ✨ Filtro por % Uso (HistoricoPedidos y PedidosCompletados)
- Select «% Uso»: todos / menor a 30% / menor a 80% / 80% o más (`cumpleFiltroUsoVehiculo`; sin tipo evaluable queda fuera de los rangos).
- Se aplica en `cargarHistorico`/`fetchData` (lado cliente) para que SOLO tome efecto al presionar Buscar/Filtrar. Incluye el modal móvil de filtros de Completados.

### ✨ Orden por columna estilo Excel (PedidosCompletados y HistoricoPedidos)
- Clic en el título: asc → desc → orden original. Flecha ▲/▼ solo en la columna activa.
- Números por valor, texto con collation `es` numérico ("G9" < "G10"), nulos SIEMPRE al final. Respeta el filtro de % uso y el buscador (ordena lo visible).
- Comparador compartido `src/Funciones/ordenTabla.ts`; helpers `valorOrden`/`thOrden` por página.

### ✨ Regionales de la operación (corrección del filtro regional)
- La regional vive DENTRO del consecutivo del vehículo: `CELTA-20260924-M-2026924-ANTIOQUIA-2` → **ANTIOQUIA**. Las 15 regionales: ANTIOQUIA, BOGOTA CENTRO ALTO/BAJO/NORTE ALTO, CENTRO 1-4, COSTA NORTE 1-3, EJE CAFETERO, OCCIDENTE, SANTANDER, SUR.
- El filtro de /Pedidos y /PedidosCompletados usa ESA lista y filtra **en cliente** (`src/Funciones/regionalVehiculo.ts` → `regionalDeVehiculo`): el campo `regional` de los documentos es la bodega (CELTA, FUNZA…), no esta regional. Alcance por perfil sin cambios (restringidos siguen limitados por backend).
- Nota: el Excel de Completados ya no respeta la regional del select (el backend no puede filtrar por ese token).

### ✨ Excel de Completados e Histórico con % Uso y Regional (backend)
- **% Uso** en los 4 export: `/exportar-completados` («% Uso (vehículo)»), detallado hoja Vehículos («% Uso» tras Kg Sicetac) y hoja Pedidos («% Uso (vehículo)»), e histórico (`% Uso` tras «Vehículo SICETAC», con semáforo de fuente). Hoja «Detalle por Pedido» del histórico SIN la columna (fila por pedido Vulcano, no aplica).
- El valor se guarda como **decimal** (0,794) con formato Excel `0,0%` (se lee 79,4% pero opera en fórmulas); celda vacía si no hay tope.
- **Regional** extraída del consecutivo (`_regional_de_vehiculo` en `pedidos.py`, fallback al campo `regional`); el export simple no tenía columna Regional y se agregó tras «Vehículo».

### 🐛 Fix — Zona horaria Colombia en PedidosCompletados
- Frontend: `today` ahora sale de `Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' })` — antes `toISOString()` (UTC) y después de las 7 p. m. el filtro amanecía al día siguiente.
- Backend: `_filtro_fecha_colombia()` en `pedidos.py` aplica el rango `05:00 UTC → 05:00 UTC del día siguiente` en listar/exportar/exportar-detallado — antes `00:00–23:59` UTC y lo tramitado tras las 7 p. m. quedaba en el día siguiente del filtro (mismo criterio que ya usaba el histórico). La columna «Fecha» de los Excel resta 5 h.
- Consecuencia: datos vespertinos cambian al día correcto de Colombia en filtros y Excel.

### 🔧 Detalles menores
- Kg Reales / Kg Sicetac de PedidosCompletados ahora con separador de miles (`15.000`, alineados a la derecha); el orden por columna sigue usando el valor numérico.

### 🔧 Archivos modificados
- `integrapp-next/src/Funciones/usoVehiculoSolicitado.ts` (nuevo), `ordenTabla.ts` (nuevo), `regionalVehiculo.ts` (nuevo)
- `integrapp-next/src/Componentes/PedidosComponentes/TablaPedidos.tsx` — % Uso, regionales de la operación
- `integrapp-next/src/Componentes/PedidosComponentes/TablaPedidosCompletados.tsx` — % Uso, filtro uso, orden por columna, regionales, kg con miles, today Colombia
- `integrapp-next/src/app/SolicitudVehiculos/page.tsx` — % Uso (2 tablas)
- `integrapp-next/src/Paginas/HistoricoPedidosP/index.tsx` — % Uso (+modal), filtro uso, orden por columna
- `integrappi/rutas/pedidos.py` — `_uso_vehiculo_pct`, `_regional_de_vehiculo`, `_filtro_fecha_colombia`, columnas en los 3 export
- `integrappi/rutas/siscore_consultas.py` — % Uso en `/historico/exportar-excel`

---

## [2026-08-04] - Otros Costos: regional, causales en colección, formulario simplificado y tope $5M; fix OOM en Banco

### 🐛 Fix — Caída del servidor (OOM) al procesar extractos bancarios
El servicio en Render se caía (OOM kill silencioso, sin log) al tramitar PDFs grandes de extractos bancarios en `POST /banco/pdf-a-excel` (plan starter = 512 MB compartidos con Chromium). El endpoint era `async def` con trabajo síncrono pesado (pdfplumber + openpyxl), lo que además bloqueaba el event loop y atascaba los webhooks de WhatsApp.
- **`rutas/banco.py`**: el parseo del PDF y la generación del Excel ahora corren en un hilo (`asyncio.to_thread`); los estilos de openpyxl (`Font`/`Alignment`/`PatternFill`) se reutilizan en vez de crearse por celda; nueva función `_mem_info()` que registra RSS/pico de memoria en 3 puntos del request para diagnosticar futuros OOM.

### ✨ Otros Costos — Filtrado por regional (alineado con SolicitudVehiculos)
- **OPERATIVO** ahora ve **todas** las solicitudes (activas e históricas) de su regional (antes solo las que él creó). **ADMIN/ANALISTA/COORDINADOR/CONTROL** ven todo y tienen un **dropdown opcional** de regional. **FINANCIERO** sin cambios.
- Backend: helpers `_normalizar_regional`/`_aplicar_filtro_regional`/`_doc_coincide_regional` que normalizan los 3 formatos (código CO `CO05`, ciudad `CALI`, bodega `YUMBO`) y filtran por `$or` sobre `regional_registro` + `datos_servicio.centro_distribucion`. `_scope_lectura` aplica la regional del OPERATIVO (fallback a propias si no tiene); el parámetro `regional` del dropdown sólo se aplica a perfiles globales (anti-bypass). Detalle: OPERATIVO ve cualquier solicitud de su regional; **editar** sigue restringido al creador.

### ✨ Otros Costos — Causales/tipos de costo en colección (`causales_otros_costos`)
- `GET /otros-costos/tipos-costo` ahora lee de la colección `causales_otros_costos` con **auto-siembra** la primera vez (mismo patrón que `clientes_otros_costos`); editable directamente en Mongo. Lista por defecto: AFORO, CARGUE, DESCARGUE, DESVIO, DEVOLUCIONES, ENTREGA EN VEREDA, OTROS, PUNTO ADICIONAL, RECOLECCIONES, REQUERIMIENTO, STAND BY, TRASBORDO, TRASLADO, URGENCIA.

### 🔧 Otros Costos — Simplificación del formulario «Nueva solicitud»
- **Eliminado el campo «Concepto»**: cada costo queda con `tipo_costo`, `descripcion` y `valor` (la descripción ya cumplía esa función). Quitado del modelo `CostoConcepto`, la validación, el input y las tablas de detalle (activos e histórico). Los datos viejos con `concepto` quedan inertes en Mongo.
- **Eliminado el campo «Observaciones»** del formulario de creación/edición (todos los perfiles). Quitado de `CrearOtroCostoRequest`/`EditarOtroCostoRequest` y del frontend. Las solicitudes viejas que ya tienen observaciones las siguen mostrando en el detalle. **No** se tocó `observaciones` del pago (la nota de FINANCIERO).

### 🐛 Fix — Tope de $5.000.000 al valor total de la solicitud
El campo «Valor» permitía cifras absurdas. Ahora el total no puede superar **$5.000.000** (constante `LIMITE_VALOR_SOLICITUD`): el input limita la entrada y `validarForm` bloquea con *"El valor total ($X) supera el máximo permitido ($5.000.000)"*; el backend valida lo mismo en `_validar_solicitud` (crear y editar).

### 🔧 Archivos modificados
- `integrappi/rutas/banco.py` — fix OOM + `_mem_info`
- `integrappi/rutas/otros_costos.py` — regional, causales en colección, eliminar Concepto/Observaciones, tope $5M
- `integrapp-next/src/Funciones/ApiPedidos/otrosCostos.tsx` — `regional` en filtros; tipo `CostoConcepto` sin `concepto`
- `integrapp-next/src/Paginas/OtrosCostosP/index.tsx` — dropdown regional, eliminar Concepto/Observaciones, tope $5M
- `integrapp-next/src/Paginas/HistoricoOtrosCostosP/index.tsx` — dropdown regional, eliminar columna Concepto

---

## [2026-06-01] - HistoricoPedidos: campo Diferencia, tabla ampliada y fixes SolicitudVehiculos

### 🎯 Campo `diferencia` persistido en MongoDB
Se agregó el campo `diferencia = total_solicitado - tarifa_calculada` en todos los endpoints que escriben a las colecciones `pedidos_medical`, `pedidos_medical_historico` y `solicitud_veh_medical`:

- `POST /siscore/guardar-busqueda` — Calcula `diferencia` al crear planilla
- `POST /siscore/guardar-solicitud` — Calcula `diferencia` al crear solicitud
- `PUT /siscore/actualizar-solicitud` — Calcula `diferencia` al actualizar solicitud
- `PUT /siscore/actualizar-planilla-pedidos` — Calcula `diferencia` usando `tarifa_calculada` del documento existente

### ✨ HistoricoPedidos — Tabla ampliada a 26 columnas
La tabla pasó de 13 a 26 columnas, replicando las mismas columnas de SolicitudVehiculos. Columnas nuevas: **Estado** (badge colores), **Placa**, **Cant. Pedidos**, **Tipo Vehículo**, **Flete Solicitado** (tarifa_base), **Vehículo SICETAC**, **Descargue**, **Punto Adic.**, **Desvío**, **Aforo**, **Cant. Destinos**, **Código Pedido**, **Observaciones** (causal).

### ✨ HistoricoPedidos — Nuevo Excel con datos de MongoDB
- **Antes**: Exportaba formato transporte Siscore (REMESAS, MANIFIESTO, etc.) usando lista de planillas
- **Ahora**: Consulta directamente `pedidos_medical_historico` con los mismos filtros de la vista (fechas, perfil, regional, búsqueda)
- Filtra por regional automáticamente si el perfil es operativo
- Excel limpio con las 26 columnas, estilos por estado, formato monetario y auto-filtro

### 🐛 Fix — Display de recargos en SolicitudVehiculos
Se corrigió que las columnas Descargue, Punto Adicional y Desvío mostraban valores hardcodeados (`$50.000`, `$80.000`, `$100.000`) en vez del valor numérico real que el usuario digitó. Ahora muestran el valor dinámico con fallback a los valores por defecto solo para datos booleanos legacy.

### 🔧 Archivos modificados
- `integrappi/rutas/siscore_consultas.py`
  - Campo `diferencia` en 4 endpoints
  - Nuevo modelo `ExportarHistoricoExcelRequest`
  - Endpoint `/historico/exportar-excel` reescrito
- `integrapp-next/src/Paginas/HistoricoPedidosP/index.tsx`
  - Tabla ampliada de 13 a 26 columnas
  - Descarga Excel con filtros
  - Lee `diferencia` de MongoDB
- `integrapp-next/src/app/SolicitudVehiculos/page.tsx`
  - Fix display dinámico de recargos (3 columnas)
  - Fix cálculo de total_solicitado en solicitudes pendientes (2 lugares)

---

## [2026-05-07] - Gestión de Tarifas de Rutas FMC

### 🎯 Nuevo Módulo
- **Gestión de Tarifas de Rutas FMC** (`/GestionTarifasRutas`)
  - Acceso restringido a perfil ADMIN
  - CRUD completo para tarifas de rutas
  - Carga masiva desde archivos Excel
  - Descarga de plantilla Excel
  - Filtrado por Ruta y Centro de Costo
  - Validación de duplicados (centro_costo + ruta)

### ✨ Mejoras
- **MedicalCare**
  - Unificación de colores de botones a tono profesional (#005f56)
  - Layout vertical de botones para mejor usabilidad
  - Nuevo botón de acceso a Gestión de Tarifas

### 🔧 Backend
- Nuevo archivo: `integrappi/rutas/tarifas_rutas_fmc.py`
  - Endpoints CRUD para tarifas
  - Validación de duplicados con índice único
  - Carga masiva con validaciones
  - Generación de plantilla Excel

### 🎨 Frontend
- Nuevo componente: `GestionTarifasRutasP`
  - Tabla con todas las tarifas
  - Modales para crear/editar
  - Spinner animado de carga
  - Filtros combinados
- Nueva API: `tarifasRutasFmc.tsx`

### 📊 Base de Datos
- Nueva colección: `fletes_rutas_fmc`
  - Índice único en `centro_costo` + `ruta`

---

## [2026-04-XX] - Recuperación de Clave y Perfiles

### ✨ Nuevas Funcionalidades
- Recuperación de contraseña para baseusuarios
- Perfil CLIENTE_FMC (solo notificaciones)
- Notificaciones filtradas por tipo y CEDI

### 🔧 Archivos Modificados
- `integrappi/rutas/baseusuarios.py`
- `integrapp-next/src/Paginas/GestionUsuariosP/`

---

## Formato de Versionado

Este proyecto sigue el formato de versionado semántico: `MAJOR.MINOR.PATCH`

- **MAJOR**: Cambios incompatibles en la API
- **MINOR**: Nuevas funcionalidades compatibles
- **PATCH**: Correcciones de errores compatibles

---

## Convenciones de Commit

- `feat:` - Nuevas funcionalidades
- `fix:` - Correcciones de errores
- `docs:` - Cambios en documentación
- `style:` - Cambios de formato (espacios, etc.)
- `refactor:` - Refactorización de código
- `test:` - Adición de pruebas
- `chore:` - Cambios en procesos de construcción
