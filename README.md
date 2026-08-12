# Integra API - Backend (FastAPI)

API REST para el sistema de gestión de pedidos y pacientes Medical Care.

## Endpoints Principales

### Base de Usuarios (`/baseusuarios`)
- `POST /` - Crear usuario
- `GET /` - Listar usuarios
- `GET /{id}` - Obtener usuario por ID
- `PUT /{id}` - Actualizar usuario completo
- `PATCH /{id}/datos` - Actualizar datos básicos (nombre, correo, regional, celular, clave, usuario)
- `PATCH /{id}/clientes` - Actualizar clientes permitidos
- `PATCH /{id}/perfil` - Cambiar perfil de usuario
- `PATCH /{id}/activo` - Activar/desactivar usuario
- `DELETE /{id}` - Eliminar usuario
- `POST /login` - Login de usuario
- `POST /loginseguridad` - Login seguridad
- `POST /loginConductor` - Login conductor
- `GET /perfiles-disponibles` - Lista de perfiles válidos
- `GET /despachadores` - Lista de despachadores

### Pacientes Medical Care (`/pacientes-medical-care`)
- Carga masiva de pacientes desde Excel
- Obtener pacientes con paginación
- Recalcular cruce con V3 (SSE para progreso)
- Obtener ocupación de rutas
- Obtener V3 sin paciente (con filtros)
- Obtener histórico por meses
- Exportar a Excel
- Gestión de cronograma de pacientes

### Pedidos V3 (`/pedidos-v3`)
- `GET /` - Obtener pedidos V3 con paginación y filtros
  - Parámetros: `skip`, `limit`, `estado`, `mes_actual`, `bodega`
  - **Filtro por `bodega_origen`** para restringir por regional
- `GET /estados` - Lista de estados únicos
- `POST /cargar-masivo-stream` - Carga desde Excel (SSE)
- `POST /cargar-desde-api-stream` - Sincronización desde API Siscore (SSE)
- `GET /exportar-excel` - Exportar a Excel
  - **Respeta filtro por `bodega_origen`**
- `PUT /{id}` - Actualizar pedido
- `DELETE /{id}` - Eliminar pedido

### Siscore Consultas (`/siscore`)
- `POST /consultar-planillas` - Consulta planillas en API de Siscore V3
  - **Parámetros**:
    - `planillas`: Lista de planillas a buscar
    - `fecha_inicio`, `fecha_fin`: Rango de fechas (opcional, se calcula automáticamente si está vacío)
    - `perfil`: Perfil del usuario (para determinar filtro por regional)
    - `centro_distribucion`: Centro de distribución del usuario (para operativos)
  - **Rango automático**: 40 días hábiles hacia atrás desde hoy
  - **Días hábiles**: Excluye fines de semana y festivos de Colombia
  - **Filtro por regional**:
    - Perfiles globales (ADMIN, COORDINADOR, CONTROL, ANALISTA): `centro_distribucion = "TODOS"`
    - Perfiles operativos: Envía su regional (con conversión CO07 → "FUNZA - SAN DIEGO 7G")
  - **Incluye pedidos manuales**: `incluir_pedidos_manuales = "SI"`
  - **Timeout**: 5 minutos para consultas largas
  - **Proxy**: Configuración opcional vía variable de entorno `VULCANO_PROXY_URL`
- `GET /test-connection` - Prueba de conexión con Siscore
- `POST /consultar-tarifa` - Consulta tarifa según ruta y tipo de vehículo
- `POST /guardar-solicitud` - Guarda solicitud en `solicitud_veh_medical`
- `POST /enviar-tramite` - Envía a `tramite_fmc`
- `GET /obtener-solicitudes-pendientes` - Obtiene solicitudes sin enviar (filtrado por usuario/perfil)
- `POST /guardar-busqueda` - Guarda planillas en `pedidos_medical`
  - **Parámetro adicional**: `planillas_a_eliminar` - Lista de planillas a eliminar (para fusión)
- `GET /obtener-resultados-recientes` - Obtiene todas las planillas guardadas
- `PUT /actualizar-planilla-pedidos` - Actualiza planilla en `pedidos_medical`
- `PUT /actualizar-estado-planilla` - Actualiza estado de aprobación
- `DELETE /eliminar-planilla` - Elimina una planilla de `pedidos_medical` (con trazabilidad)
  - **Campos actualizables**: `tarifa_base`, `requiere_descargue`, `punto_adicional`, `desvio`, `aforo`, `placa`, `tipo_veh_sicetac`
  - **Gestión de estados**: `estado`, `aprobado_por`, `fecha_aprobacion`
  - **Campo de causal**: `causal` (OBLIGATORIO si hay sobrecosto)
- `POST /exportar-planillas-excel` - Exporta planillas a Excel (columna de Observaciones/causal; desde 2026-07-31 también **Ahorro** + **Observación Ahorro** al final de la hoja `plantilla`)

### Causales (`/causales`)
- `GET /causales` - Obtiene causales activas para dropdown de fusión
- `GET /causales/todas` - Obtiene todas las causales (activas e inactivas) - solo admin
- `POST /causales` - Crea nueva causal de fusión
- `PUT /causales/{id}` - Actualiza causal (nombre, activo/inactivo)
- `POST /causales/inicializar` - Inicializa causales por defecto si no existen

### Sync V3 (`/sync-v3`)
- `POST /recalcular` - Recalcular cruce completo
- `POST /notificar-retraso-operacion` - Enviar notificaciones
- `GET /estado` - Estado de última sincronización

## Filtros por Regional

### Campo `bodega_origen`

Los pedidos V3 tienen un campo `bodega_origen` que indica la regional:

| Código | Regional       |
|--------|----------------|
| CO04   | BARRANQUILLA   |
| CO05   | CALI           |
| CO06   | BUCARAMANGA    |
| CO07   | FUNZA          |
| CO09   | MEDELLIN       |

### Implementación del Filtro

**En endpoints GET:**
```python
if bodega:
    filtro['bodega_origen'] = bodega
```

**En frontend:**
- OPERADORES obtienen su regional de cookies
- Se mapea nombre de regional a código (CALI → CO05)
- Se pasa como parámetro `bodega` a la API

## Base de Datos

### Colecciones

- **`baseusuarios`** - Usuarios del sistema
- **`pacientes_medical_care`** - Pacientes de Medical Care
- **`v3`** - Pedidos V3 sincronizados
- **`cache_cruce_mc`** - Cache del cruce pacientes-V3
- **`notificaciones_mc_historial`** - Historial de notificaciones
- **`powerbi_notificaciones`** - Datos para PowerBI
- **`solicitud_veh_medical`** - Solicitudes de vehículos con estados de aprobación
- **`pedidos_medical`** - Planillas consultadas en Siscore (documentos independientes)
- **`causales`** - Causales para fusión de planillas
- **`pedidos_medical_historico`** - Planillas movidas después de importación Vulcano (histórico)
- **`config_otros_costos`** - ⚠️ Tabla de **configuración** de costos por `tipo_vehiculo` (`valor_punto_adicional`, `cargue_descargue`, `max_puntos` y versiones `_cliente`). La carga `POST /fletes/cargar-otros-costos` (`rutas/fletes.py`) y la leen los recálculos de `rutas/pedidos.py` (fusión `/pedidos/fusionar-vehiculos` y cálculo masivo). **No es** lo mismo que las solicitudes de Otros Costos.
- **`otros_costos`** - Solicitudes del módulo Otros Costos (ciclo activo). **No es** la configuración de arriba.
- **`historico_otros_costos`** - Solicitudes de otros costos pagadas
- **`anulados_otros_costos`** - Solicitudes de otros costos anuladas
- **`clientes_otros_costos`** - Catálogo de clientes sugeridos en el formulario de Otros Costos (`GET /otros-costos/clientes`; auto-siembra 9 clientes por defecto la primera vez)
- **`causales_otros_costos`** - Catálogo de causales/tipos de costo del formulario de Otros Costos (`GET /otros-costos/tipos-costo`; auto-siembra 14 causales por defecto la primera vez, editable en Mongo)

> **⚠️ REGLA DE ORO — una colección = un propósito.** Nunca reuses el nombre de una colección existente para un módulo/ruta nuevo. Hasta el 2026-07-29 la configuración de Pedidos y las solicitudes de Otros Costos **compartían** `otros_costos`: cada recarga del Excel hacía `delete_many({})` y borraba las solicitudes, y la configuración terminó vacía (rompió la fusión con *"No hay configuración de 'otros_costos' para el tipo 'TRACTOMULA'"*). Antes de crear un módulo: (1) revisa los nombres de colección ya usados en esta lista y con `grep 'db\["' rutas/`; (2) antes de cualquier `delete_many({})`, confirma que la colección sea de **un único propósito**; (3) si necesitas una tabla de configuración/parámetros, usa su propia colección (`config_*`), nunca la de las solicitudes.

## Tecnologías

- **Framework:** FastAPI
- **Python:** 3.13
- **Base de datos:** MongoDB
- **SSE:** Server-Sent Events para progreso en tiempo real
- **Excel:** openpyxl
- **HTTP:** httpx para llamadas a APIs externas

## Optimizaciones Recientes (2026-05-08)

- **Nuevo endpoint `/siscore/consultar-planillas`**: Consulta de planillas en API Siscore V3
  - Cálculo automático de rango de 40 días hábiles
  - Filtrado por perfil y regional
  - Incluye pedidos manuales
  - Timeout de 5 minutos para consultas largas
  - Proxy configurable para llamadas externas

## Actualizaciones Recientes (2026-05-27)

### Sistema de Consecutivos Únicos para Planillas

- **Formato del consecutivo**: `REGIONAL-YYYYMMDD-NUMERO`
  - Ejemplo: `FUNZA-20260527-1`, `MEDELLIN-20260527-1`
- **Independiente por regional y fecha**: Cada regional tiene su propia secuencia por fecha
- **Reutilización de huecos**: Si se elimina una planilla, su número queda disponible
- **Planillas fusionadas**: Usan el mismo número base con letras (A, B, C...)
  - Ejemplo: `FUNZA-20260527-1A`, `FUNZA-20260527-1B`
- **Asignación automática**: El sistema asigna el menor número disponible al guardar
- **Visible en frontend**: Columna "Consecutivo" en la tabla de planillas

**Reglas de generación**:
1. Busca consecutivos existentes para la misma regional y fecha
2. Identifica números usados (individuales) y fusiones activas
3. Para planillas individuales: asigna el menor número disponible
4. Para fusiones: usa el mismo número base con letras A, B, C...
5. Si se elimina una fusión completa, el número base queda libre
6. Si se elimina una planilla dentro de una fusión, la letra queda disponible

**Campos en MongoDB**:
- `consecutivo`: Consecutivo completo (ej: `FUNZA-20260527-1A`)
- `consecutivo_base`: Base sin letra (ej: `FUNZA-20260527-1`)
- `numero_consecutivo`: Solo el número (ej: `1`)
- `letra_consecutivo`: Letra si es fusión (ej: `A`)
- `es_fusionada_consecutivo`: Booleano si es parte de una fusión

## Actualizaciones Recientes (2026-05-26)

### Sistema de Estados de Aprobación para Planillas

- **Cuatro estados de aprobación**: PREAPROBADO, REQUIERE_APROBACION_COORDINADOR, REQUIERE_APROBACION_CONTROL, APROBADO
  - **PREAPROBADO**: El total solicitado es igual o menor al teórico
  - **REQUIERE_APROBACION_COORDINADOR**: El total solicitado es mayor al teórico, diferencia ≤ 7%
  - **REQUIERE_APROBACION_CONTROL**: El total solicitado es mayor al teórico, diferencia > 7%
  - **APROBADO**: Planilla aprobada por coordinador, control o admin

### Reglas de Aprobación por Perfil

| Perfil | Coordinador (≤7%) | Control (>7%) | Sin Tarifa (=$0) | Observaciones |
|--------|-------------------|---------------|------------------|---------------|
| ADMIN | ✅ | ✅ | ✅ | Puede aprobar todo |
| CONTROL | ✅ | ✅ | ✅ | Puede aprobar todo |
| COORDINADOR | ✅ | ❌ | ✅ | Solo hasta 7% y sin tarifa |
| ANALISTA | ❌ | ❌ | ❌ | No puede aprobar |
| OPERATIVO | ❌ | ❌ | ❌ | No puede aprobar |

**Caso especial: Flete teórico = $0**
- Cuando `tarifa_calculada` = 0, la planilla se marca visualmente
- Fondo gris oscuro con borde izquierdo gris
- Badge "SIN TARIFA" adicional
- Funciona normalmente: edición, guardado, aprobación
- No requiere CONTROL exclusivo (coord. puede aprobar)

### Gestión de Causales para Modificaciones

- **Causal OBLIGATORIO** cuando hay sobrecosto (total > teórico)
- **Causal opcional** cuando no hay sobrecosto (total ≤ teórico)
- **Auto-limpieza**: La causal se elimina automáticamente si el total vuelve a ser ≤ teórico
- **Validación en backend**: No permite guardar si hay sobrecosto sin causal

### Trazabilidad Completa de Planillas

**Campos de trazabilidad en `pedidos_medical`**:
- `usuario_registro`: Usuario que consultó/registró la planilla
- `usuario_modificacion`: Último usuario que editó la planilla
- `fecha_modificacion`: Fecha de la última modificación
- `usuario_solicitud_autorizacion`: Usuario que hizo el cambio que requirió autorización
- `fecha_solicitud_autorizacion`: Fecha en que se solicitó autorización
- `aprobado_por`: Usuario que aprobó la planilla
- `fecha_aprobacion`: Fecha de aprobación
- `historial_cambios`: Array con todas las modificaciones realizadas

**Estructura del historial de cambios**:
```json
{
  "historial_cambios": [
    {
      "fecha": "2026-05-26T17:14:45.553Z",
      "usuario": "PPRUEBA",
      "accion": "edicion",
      "campos_modificados": [
        {
          "campo": "tarifa_base",
          "valor_anterior": 500000,
          "valor_nuevo": 550000
        }
      ],
      "causal": "lleva paqueteo"
    },
    {
      "fecha": "2026-05-26T18:30:00.000Z",
      "usuario": "COORDINADOR",
      "accion": "cambio_estado",
      "campos_modificados": [
        {
          "campo": "estado",
          "valor_anterior": "REQUIERE_APROBACION",
          "valor_nuevo": "APROBADO"
        }
      ]
    }
  ]
}
```

**Endpoint `/actualizar-planilla-pedidos`**:
- Recibe parámetro `usuario_modificacion` para trazabilidad
- Compara valores anteriores con nuevos para detectar cambios
- Agrega entrada al historial por cada modificación
- Registra usuario y fecha de solicitud de autorización cuando estado → REQUIERE_APROBACION

**Endpoint `/actualizar-estado-planilla`**:
- Agrega entrada al historial cuando cambia el estado
- Registra quién aprobó (usuario + fecha)

**Exportación a Excel**:
- Columnas de trazabilidad: Usuario Registro, Usuario Modificación, Fecha Modificación, Usuario Solicitud Aut., Fecha Solicitud Aut., Aprobado Por, Fecha Aprobación

### Persistencia en MongoDB

- **Fusión de planillas**: Las planillas originales se eliminan de MongoDB al fusionar
- **División de planillas**: La planilla fusionada se elimina al dividir
- **Endpoint `/guardar-busqueda`**: Recibe `planillas_a_eliminar` para borrar documentos

## Actualizaciones Recientes (2026-06-03)

### Fix: Fusión de planillas después de importación Vulcano

**Problema**: Al fusionar planillas que ya habían sido movidas a `pedidos_medical_historico` por la importación Vulcano, las planillas originales seguían apareciendo en el histórico porque la fusión solo actualizaba `pedidos_medical`.

**Solución**:

1. **`guardar-busqueda` (fusión)**:
   - Ahora ejecuta `delete_many` en **ambas** colecciones (`pedidos_medical` y `pedidos_medical_historico`)
   - Elimina las planillas originales completamente, no las marca
   - Los datos originales se preservan en `fusion_info.datos_originales` dentro de la planilla fusionada

2. **`dividir-fusion` (división)**:
   - Lee `fusion_info.datos_originales` de la planilla fusionada
   - Reconstruye cada planilla original con `insert_one` en `pedidos_medical`
   - Incluye todos los campos: consecutivo, tarifa, estado, etc.
   - Elimina la planilla fusionada

### Nuevo campo: `flete_cobrado_fmc`

- **Cálculo**: `piezas × $20,000`
- **Almacenado en**: `pedidos_medical` y `pedidos_medical_historico`
- **Se incluye en**:
  - `guardar-busqueda`: Almacenado como campo del documento
  - `dividir-fusion`: Restaurado desde `datos_originales`

**Estructura actualizada del documento**:
```json
{
  "planilla": "824986",
  "piezas": 15,
  "peso_real": 396,
  "flete_cobrado_fmc": 300000,
  "total_solicitado": 500000,
  "consecutivo": "FUNZA-20260603-1"
}
```

### Endpoints de Histórico

- `GET /siscore/historico`: Obtiene planillas de `pedidos_medical_historico` (las originales fusionadas ya no existen porque fueron eliminadas)
- `POST /siscore/historico/exportar-excel`: Exporta a Excel (consistente con la vista)

### Recálculo de Estado

- **Edición automática**: Cualquier modificación en "Editar Planilla" recalcula el estado
- **Cálculo explícito**: Usa valores de `tempEdicion` para evitar datos obsoletos
- **Reset de aprobación**: Si el estado era APROBADO y hay modificaciones, se mantiene APROBADO

### Campos de Estado en `pedidos_medical`

```json
{
  "estado": "PREAPROBADO",
  "aprobado_por": null,
  "fecha_aprobacion": null,
  "causal": "lleva paqueteo",
  "usuario_registro": "PPRUEBA",
  "usuario_modificacion": "COORDINADOR",
  "fecha_modificacion": "2026-05-26T18:30:00.000Z"
}
```

## Optimizaciones Recientes (2026-05-06)

- **Filtro por regional:** Ahora usa `bodega_origen` directamente en consulta MongoDB (antes cruzaba rutas)
- **Count_documents:** El total respeta todos los filtros aplicados
- **Skip/Limit:** Se aplican en base de datos, no en Python
- **Indexado:** Índice en `fecha_preferente` para optimizar consultas por mes

## Estructura del Proyecto

```
integrappi/
├── main.py                 # Entry point
├── bd/
│   └── bd_cliente.py      # Cliente MongoDB
├── rutas/
│   ├── baseusuarios.py     # Gestión de usuarios
│   ├── pacientes_medical_care.py  # Pacientes y cruce
│   ├── pedidos_v3.py      # Pedidos V3
│   ├── siscore_consultas.py  # Consultas a Siscore (planillas)
│   └── ...                # Otras rutas
├── Funciones/
│   ├── normalizacion_medical_care.py
│   └── sync_api_v3.py      # Sincronización V3
└── requirements.txt        # Dependencias
```

## Cálculo de Días Hábiles

Para el módulo de Solicitud de Vehículos, el sistema implementa cálculo de días hábiles para Colombia:

### Festivos Considerados
- **Festivos fijos**: 1 de enero, 6 de enero, 1 de mayo, 20 de julio, 7 de agosto, 8 de diciembre, 25 de diciembre
- **Festivos móviles**: Jueves Santo, Viernes Santo, Ascensión, Corpus Christi, Sagrada Eucaristía
- **Fines de semana**: Sábados y domingos

### Implementación
- Cálculo de Pascua usando algoritmo de Meeus/Jones/Butcher
- Ley Emiliani: festivos que caen en martes se mueven al lunes anterior
- Función `_obtener_festivos_colombia(anio)` retorna lista en formato YYYY-MM-DD
- Función `_calcular_rango_3_dias_habiles()` retrocede 40 días hábiles desde hoy

## Actualizaciones Recientes (2026-06-16)

### Nuevo módulo: Indicadores de Transporte (`rutas/indicadores_transporte.py`)

Endpoints para el dashboard de indicadores de transporte del frontend:

- **`GET /indicadores-transporte/guias`**: Indicadores agregados (KPIs, gráficos, distribución por cliente)
  - Filtros: `fecha_inicio`, `fecha_fin`, `estado`, `cliente` (múltiple), `anio` (múltiple), `mes` (múltiple)
  - Devuelve: KPIs (totales, conteo por estado, piezas, toneladas), `datosGrafico` (pedidos por día/estado), `datosCajas` (cajas por día/estado), `datosPorCliente`, `datosCajasPorCliente` (cajas reales por cliente/estado), listas de estados/clientes/años disponibles
  - **Filtro de cliente**: La lista de clientes (`clientes_lista`) SIEMPRE devuelve todos los disponibles (sin aplicar filtro de cliente), mientras que los datos sí se filtran por cliente
- **`GET /indicadores-transporte/guias/detalle`**: Registros individuales de un día específico
  - Filtros: `fecha`, `cliente` (múltiple), `estado` (múltiple)

#### Características técnicas
- **Conexión a PostgreSQL** vía `psycopg2-binary` (variables de entorno `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USUARIO`, `PG_CLAVE`)
- **Agregación server-side**: usa `GROUP BY`, `COUNT FILTER`, `SUM` en SQL en lugar de traer registros individuales (rendimiento tipo Power BI)
- Filtro de año convertido a **rangos de fecha** (sargable) para aprovechar índices
- Columnas `piezas`/`kilos` (tipo `text`) convertidas a numérico con `CASE WHEN ... ~ '^[0-9]+...'`
- Tabla origen: `informe_guias_tms`
- Dependencia agregada: `psycopg2-binary==2.9.10` en `requirements.txt`
- Router registrado en `main.py` como `ruta_indicadores_transporte`

#### Variables de entorno necesarias (Render / GitHub Secrets)
- `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USUARIO`, `PG_CLAVE`

#### Índices recomendados en PostgreSQL
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_guias_cliente_trgm ON informe_guias_tms USING gin (nombre_cliente gin_trgm_ops);
CREATE INDEX idx_guias_fecha ON informe_guias_tms (fecha_emision);
CREATE INDEX idx_guias_fecha_cliente ON informe_guias_tms (fecha_emision, nombre_cliente);
```

## Actualizaciones Recientes (2026-06-16)

### Mejoras en el módulo de Indicadores de Transporte

- **Nuevo campo `datosCajasPorCliente`**: Ahora el endpoint `/indicadores-transporte/guias` devuelve cajas reales por cliente y estado
  - Antes: El frontend calculaba cajas por cliente usando proporciones basadas en conteo de guías (incorrecto)
  - Ahora: El backend calcula directamente la suma de piezas por cliente y estado
  - Query SQL: `SELECT cliente, estado, SUM(piezas) FROM informe_guias_tms GROUP BY cliente, estado`

- **Corrección del filtro de clientes**: La lista de clientes ahora siempre muestra todos los disponibles
  - **Problema**: Al seleccionar un cliente, el dropdown solo mostraba ese cliente, impidiendo selección múltiple
  - **Solución**: Crear cláusula WHERE separada (`where_sin_cliente`) que excluye el filtro de cliente
  - **Resultado**: `clientes_lista` siempre contiene todos los clientes, mientras que los datos sí se filtran

- **Separación de consultas**:
  - **Datos (KPIs, gráficos)**: Usan `where` con todos los filtros incluyendo cliente
  - **Lista de clientes**: Usa `where_sin_cliente` sin filtro de cliente
  - Ambas consultas comparten filtros de año, mes, estado, y rango de fechas

### Impacto en el Frontend

- **Gráfico "Cajas por Cliente"**: Ahora muestra valores correctos sin distorsión por proporción
- **Filtro de Cliente**: Transformado en dropdown estilo Power BI con checkboxes
  - Búsqueda interna dentro del dropdown
  - Selección múltiple sin restricciones
  - Visualización clara de clientes seleccionados
- **Optimización móvil**: Lista interactiva de clientes en pantallas pequeñas (≤768px)
  - Gráfico de pie oculto en móvil
  - Lista touch-friendly con indicadores de color
  - Panel de información del cliente seleccionado

## Actualizaciones Recientes (2026-06-18)

### Bot de scraping de Siscore (reemplazo del WS de planillas)

El WS de Siscore V3 (`integra-wms.appsiscore.com/app/ws/informe_v3.php`) dejó de funcionar para la consulta de planillas. Se implementó un **bot con Playwright** que scrapea el portal `https://integra.appsiscore.com/app/index.php` y devuelve la misma estructura que consume el frontend.

**Nuevos archivos:**
- `Funciones/bot_siscore.py` — núcleo del bot: login, navegación del menú (SB Admin 2: GESTIÓN DE INFORMES → básica → Informes Mensajeros → Planilla de despacho), captura del popup, descarga del Excel por planilla y lectura. Incluye **`BotSessionManager`**: reúsa la sesión entre requests (mantiene el navegador logueado vivo sobre un `ProactorEventLoop` dedicado) para no loguearse en cada consulta y **evitar bloqueos del TMS** por exceso de logins; re-loguea solo al expirar (TTL configurable).
- `Funciones/siscore_excel_mapper.py` — lee el Excel (que el portal entrega como **HTML disfrazado de `.xls`** vía `pd.read_html`), descarta la fila de pie de página, normaliza columnas, mapea al contrato del frontend y enriquece **Ruta/Departamento** desde la colección `divipolas`.

**Nuevo endpoint:** `POST /siscore/consultar-planillas-bot`
- Mismo request/response que `/siscore/consultar-planillas` (el endpoint viejo **se conserva intacto**).
- Mapeo de campos del Excel: `Entidad`→Cliente Origen, `Guia`→Codigo Pedido, `Destino`→Municipio Destino, `Peso`→Peso Real, `Piezas`, `Placa`, `Manifiesto`.

**Consideraciones técnicas:**
- **Proxy autorizado**: el TMS solo admite IPs autorizadas. El tráfico sale por el proxy de Digital Ocean `64.227.95.70:3128` (var `SISCORE_BOT_PROXY_URL`) con **auth nativa de Playwright** (no requiere forwarder local, a diferencia de Chrome/Selenium crudo).
- **Windows + uvicorn**: el loop de uvicorn es `SelectorEventLoop`, que no soporta `subprocess` (lanzar Chromium). El bot corre en un `ProactorEventLoop` dedicado (endpoint `def` síncrono + wrapper `consultar_planillas_via_bot_sync`). En Linux (Render) es no-op.
- **Variables de entorno** (Render/local): `SISCORE_BOT_USER`, `SISCORE_BOT_PASS`, `SISCORE_BOT_PROXY_URL`, `SISCORE_BOT_HEADED`, `SISCORE_BOT_SESSION_TTL`, `SISCORE_BOT_REQUEST_TIMEOUT`, `SISCORE_BOT_DOWNLOAD_DIR`, y `MONGO_URI`.
- **Dependencias** añadidas a `requirements.txt`: `playwright==1.49.0`, `lxml==5.3.0`.
- **Deploy en Render**: `Dockerfile` (imagen oficial `mcr.microsoft.com/playwright/python:v1.49.0`, ya trae Chromium + deps), `render.yaml`, `.dockerignore`. `main.py` ahora respeta la variable `PORT`.

### Prevención de colisión de consecutivos tras "Importar Vulcano"
- **Problema**: `_generar_consecutivo` solo consultaba `pedidos_medical`; al moverse las planillas a `pedidos_medical_historico` (Vulcano), sus consecutivos dejaban de verse y se podían **reutilizar** el mismo día.
- **Solución**: ahora consulta **también `pedidos_medical_historico`** al calcular el número máximo usado → los consecutivos históricos quedan **reservados**.
- **Índice**: se crea un índice sobre `consecutivo` en ambas colecciones (idempotente, en el arranque del backend) para que la consulta por prefijo (`^REGIONAL-YYYYMMDD-`) sea rápida aunque el histórico crezca a miles/millones.

### Regional guardada como bodega para OPERATIVO
- Al guardar (`guardar-busqueda`), si el perfil es **OPERATIVO**, el campo `regional` se guarda como la **bodega de origen** (CALI→YUMBO, BARRANQUILLA→JUAN MINA, MEDELLIN→GIRARDOTA) mediante `regional_a_origen_bodega`. El consecutivo **no** se transforma (sigue con el nombre, ej: `CALI-...`). Solo aplica a OPERATIVO.

### Exportación a Excel (`exportar-planillas-excel`)
- **Origen**: `CALI` → `YUMBO` (junto al ya existente `BARRANQUILLA` → `JUAN MINA`).
- **Cliente**: `FRESENIUS KABI` → `900402080` (junto al ya existente `FRESENIUS MEDICAL CARE` → `901689684`), vía diccionario `CLIENTE_A_NIT` (insensible a mayúsculas/espacios).

## Actualizaciones Recientes (2026-06-19)

### Exportación a Excel — planillas fusionadas y KABI (`/siscore/exportar-planillas-excel`)
- **Planillas fusionadas → filas separadas**: una planilla fusionada ya no se exporta como una sola fila. Se generan **N filas** (una por cada planilla original en `fusion_info.datos_originales`), cada una con su **consecutivo original** y sus datos propios (cliente, destino, piezas, peso). El campo **"Flete unidad"** (`total_solicitado`) se **reparte proporcionalmente por piezas** (división entera; la última fila absorbe el residuo → suma exacta). Helpers: `_repartir_flete`, `_expandir_doc_a_filas`, `_consecutivo_original`, `_escribir_fila_planilla`, `_mapear_tipo_vehiculo`.
- **FRESENIUS KABI → filas duplicadas por destinatario**: para KABI, además de la fila normal, se genera **una fila por cada Nombre único** de `registros_detalle` (todas con el mismo consecutivo y datos), donde **"Ubicación Descargue"** = `FKC_<Nombre>_<Cedula>`. FRESENIUS MEDICAL CARE sigue con una sola fila. Helpers: `_es_cliente_kabi`, `_expandir_fila_kabi`.

### Detalle por guía en MongoDB (`guardar-busqueda`)
- Nuevo campo **`registros_detalle`** en cada documento de `pedidos_medical`: array con un item por guía/fila del Excel del portal, incluyendo **`Cedula`**, `Nombre`, `Direccion`, `Producto`, `Valor Declarado`, etc. (para auditoría).
- `dividir-fusion` reconstruye `registros_detalle` desde `datos_originales`.
- `obtener-resultados-recientes` lo excluye con projection `{"registros_detalle": 0}` para no inflar la carga de la tabla.
- Mapper (`Funciones/siscore_excel_mapper.py`): `mapear_fila_a_registro` ahora expone todos los campos reconocidos del Excel (`Cedula`, `Origen`, `Producto`, `Codigo`, `Mensajero`, `Usuario`, `Conductor`, `Fecha`, `Estado`, `Valor Declarado`).

### División de consecutivo en carros (NUEVO)
- **`POST /siscore/dividir-consecutivo`**: divide una planilla en hasta **4 carros** (consecutivo con letra: `3A`, `3B`, `3C`, `3D`). El frontend envía el peso de cada carro; el backend valida que la **suma de pesos == peso total** (±1 kg), duplica los datos de la original en cada carro con su peso/tipo/flete propios, guarda `division_info` (snapshot de la original) en cada carro y elimina la original. Helper: `_generar_consecutivo_division`.
- **`POST /siscore/unir-carros`**: revierte la división (elimina los carros y reconstruye la original desde `division_info.datos_original`).

### Rutas: listado y asignación manual (NUEVO)
- **`GET /siscore/rutas`**: devuelve las rutas con tarifa (`distinct("ruta")` sobre `fletes_rutas_fmc`), para el autocompletar del frontend.
- **`actualizar-planilla-pedidos`**: ahora persiste el campo `ruta` (edición de ruta desde el modal). `guardar-busqueda` ya persistía `ruta`; el frontend ahora la asigna obligatoriamente antes de guardar.

## Actualizaciones Recientes (2026-07-28)

### Nuevo módulo: Otros Costos (`rutas/otros_costos.py`)

Gestión de costos adicionales posteriores al servicio (parqueadero, peaje, cargue, horas extra, etc.) asociados a pedidos de Vulcano del histórico. Router `/otros-costos`, registrado en `main.py`.

**Colecciones**: `otros_costos` (activo), `historico_otros_costos` (pagadas), `anulados_otros_costos` (anuladas). Lookup en `pedidos_medical_historico`. Movimientos entre colecciones con patrón delete-first + insert idempotente.

> **⚠️ No confundir con `config_otros_costos`.** Esa es la tabla de configuración de Pedidos (`valor_punto_adicional`/`cargue_descargue` por `tipo_vehiculo`, cargada por `POST /fletes/cargar-otros-costos` en `rutas/fletes.py`). Hasta 2026-07-29 **ambas compartían la colección `otros_costos`** y se borraban mutuamente (la recarga del Excel hacía `delete_many({})` sobre las solicitudes, y la configuración terminó vacía). Desde entonces están separadas: este módulo es el **único** dueño de `otros_costos`; la configuración vive en `config_otros_costos`.

**Seguridad**: el perfil NO se confía del frontend. Cada endpoint recibe `usuario` y lo resuelve en `baseusuarios` (`_resolver_usuario`) para autorizar con el perfil real. Umbral coordinador `LIMITE_COORDINADOR = 500000`.

**Endpoints**:
- `GET /otros-costos/tipos-costo`, `GET /otros-costos/bancos`, `GET /otros-costos/tipos-cuenta` — enums para dropdowns
- `POST /otros-costos/buscar-pedidos` — busca en `pedidos_medical_historico` (tolerante a ceros a la izquierda y separadores)
- `POST /otros-costos/verificar-duplicado` — advertencia de duplicados (no bloquea)
- `POST /otros-costos/crear` — crea en `borrador` o `pendiente_aprobacion`
- `PUT /otros-costos/editar` — sólo si no está aprobada/pagada/anulada
- `POST /otros-costos/enviar-aprobacion`, `/aprobar`, `/marcar-tramite-vulcano`, `/devolver`, `/rechazar`, `/registrar-pago`, `/anular`
- `GET /otros-costos/` — listado paginado con filtros (scope por perfil)
- `GET /otros-costos/{consecutivo}` — detalle + trazabilidad
- `GET /otros-costos/historico` y `/historico/{consecutivo}`
- `POST /otros-costos/exportar-excel`

**Permisos por perfil**:
| Acción | Perfiles |
|--------|----------|
| Crear / editar (propias, pre-aprobación) | OPERATIVO, ADMIN |
| Aprobar ≤ $500.000 | COORDINADOR, CONTROL, ADMIN |
| Aprobar > $500.000 | CONTROL, ADMIN |
| Marcar trámite Vulcano (`tramite_vulcano` ok/pendiente) | ANALISTA, ADMIN |
| Devolver / Rechazar | COORDINADOR, CONTROL, ADMIN |
| Registrar pago (requiere `tramite_vulcano == "ok"`) | FINANCIERO, ADMIN |
| Anular | ADMIN |

**Trazabilidad**: cada acción agrega a `historial_movimientos` `{accion, estado_anterior, estado_nuevo, usuario, nombre_usuario, rol, fecha(UTC), observacion, ip}`. Datos bancarios (`numero_cuenta`, `cedula_titular`) enmascarados en las respuestas salvo FINANCIERO/ADMIN. **Sin soportes/archivos** (solo valores).

**Perfil FINANCIERO** agregado a `PERFILES_VALIDOS` en `baseusuarios.py`. Consecutivo `OC-AAAAMMDD-NNNN` (índice unique + reintento ante colisión).

## Actualizaciones Recientes (2026-07-31)

### Otros Costos — paso ANALISTA «trámite Vulcano» y formulario de un solo pedido

- **Nuevo eslabón del flujo**: `aprobado → [tramite_vulcano = "ok" marcado por ANALISTA] → pagado`. Al aprobar nace `tramite_vulcano = "pendiente"` (campo `tramite_vulcano_info` + movimiento de acción `tramite_vulcano`).
- **`POST /otros-costos/marcar-tramite-vulcano`** (ANALISTA + ADMIN): avanza `→ok` o revierte `→pendiente`, solo sobre aprobados (update atómico).
- **`POST /otros-costos/registrar-pago`**: bloquea con `422` hasta que `tramite_vulcano == "ok"`; su filtro atómico también lo exige (anti-doble-pago coherente).
- **Scope por perfil**: FINANCIERO ve en activos solo `aprobado` + `tramite_vulcano == "ok"` (su bandeja = listos para pagar); ANALISTA ve `aprobado` (para tramitar/revertir).
- **Frontend** (`OtrosCostosP`): botón que alterna OK/Revertir (tabla + modal); el de **Pagar** solo aparece con `tramite_vulcano == "ok"`. Perfil ANALISTA habilitado como **rol compartido** en MEDICAL_CARE (no es micro-portal).
- **Formulario «Nueva solicitud»**: una planilla = **un solo** pedido de Vulcano. Label en singular y validación que rechaza varios (separadores `, - ; /`) tanto al Buscar como al Guardar.

### SolicitudVehiculos — pedido Vulcano por planilla en fusiones

Antes, al asignar el pedido **manualmente** sobre una fusión, el `pedido_vulcano` quedaba en el raíz de toda la fusión y se movía al histórico de golpe, perdiendo la asociación pedido↔planilla. Ahora el flujo manual hace la misma cascada que el Excel.

- **Helper compartido `_procesar_pedido_vulcano(consecutivo, pedido, usuario, requiere_aprobado=False)`** en `siscore_consultas.py`: cascada completa (búsqueda raíz no-fusionada → histórico; original embebido en `fusion_info.datos_originales` → asignación con `array_filters`, evaluación de completitud, concatenación `", ".join(pedidos)` en el raíz y paso al histórico **solo cuando TODOS** los originales tienen pedido, + notificación WhatsApp). Devuelve `{tipo: normal|fusion_parcial|fusion_completa|no_encontrado|no_aprobado}` sin lanzar `HTTPException`.
  - `POST /siscore/importar-vulcano`: refactorizado para usar el helper (mismo comportamiento, sin duplicación).
  - `POST /siscore/asignar-pedido-manual`: usa el helper con `requiere_aprobado=True` (sigue validando `APROBADO`). La firma del request no cambia (`consecutivo, pedido, usuario`).
- **Frontend** (`app/SolicitudVehiculos/page.tsx`, `handleAsignarPedidoManual`): si la fila es una fusión, modal con **un campo de pedido por cada planilla original** (precargado si ya tenía); asigna secuencialmente y muestra resumen (asignados / fusiones completadas / parciales pendientes / errores). Planilla no fusionada → Swal simple.
- **Histórico** (`HistoricoPedidosP`): columna **"Pedido Vulcano"** por original en la tabla de planillas originales de la fusión (el listado ya filtra por `pedido_vulcano` raíz concatenado, así que buscar un pedido encuentra la fusión y se ve a qué planilla pertenece).

## Actualizaciones del 2026-07-31 (2.ª tanda) — Ahorro/observación en planillas y clientes de Otros Costos

### Campos `ahorro` y `observacion` en planillas (SolicitudVehiculos)
El usuario operativo puede registrar, en el modal **Editar Planilla**, un **ahorro** (numérico, **máx. $5.000.000**) y una **observación** que lo justifique (ej: evitó un vehículo adicional por una fusión). **No afecta** total solicitado, diferencia ni estado: es metadato. Aplica a registros nuevos/ediciones (los pasados quedan en `0`/`""`).

- **`PUT /siscore/actualizar-planilla-pedidos`** (`siscore_consultas.py`): el modelo `ActualizarPlanillaPedidosRequest` y `campos_actualizar` ahora incluyen `ahorro`/`observacion`; validación **HTTP 400** si `ahorro > 5.000.000`.
- **Persistencia**: se guardan en `pedidos_medical`; viajan **solos** al histórico porque `_procesar_pedido_vulcano` copia el documento completo a `pedidos_medical_historico`, y sobreviven al re-consultar porque `guardar-busqueda` hace `$set` (no replace).
- **Excel `POST /siscore/exportar-planillas-excel`**: dos columnas nuevas **al final** de la hoja `plantilla` → "Ahorro" (numérico) y "Observación Ahorro" (texto, máx 300 car.). En planillas fusionadas el ahorro va sólo en la primera fila para no duplicar la suma.
- **Indicadores de Fletes** (`indicadores_fletes.py`, `GET /indicadores-fletes/resumen`): la pipeline de KPIs ahora suma `ahorro` (`"$sum": _num("ahorro")`) y lo devuelve en `kpis.ahorro`; redondeado como moneda COP.

### Catálogo de clientes de Otros Costos (`otros_costos.py`)
- **`GET /otros-costos/clientes`**: devuelve los clientes sugeridos para el campo Cliente del formulario. **Auto-siembra** la colección `clientes_otros_costos` con 9 clientes por defecto la primera vez (si está vacía); a partir de ahí es editable directamente en Mongo (documentos `{ "nombre": "..." }`) sin tocar código.
- **Fix UI OtrosCostos**: los SweetAlert de "Guardar y enviar", aprobar, pagar, etc. quedaban detrás del modal (overlay a `z-index: 9999` vs. Swal por defecto ~1060). Solución: `.swal2-container { z-index: 99999 !important; }` en `OtrosCostosP/estilos.css`.

## Actualizaciones Recientes (2026-08-03)

### Otros Costos — búsqueda de pedidos: datos individuales en planillas fusionadas

**Problema**: al buscar un pedido de Vulcano en el formulario «Nueva solicitud» (`POST /otros-costos/buscar-pedidos`), si el pedido estaba dentro de una **planilla fusionada**, la búsqueda hacía match sobre la fusión entera (`pedido_vulcano` raíz = concatenado de todos los pedidos) y proyectaba los campos **top-level del vehículo fusionado** (piezas/peso totales, destino único), no los del pedido individual → el usuario veía datos que no correspondían sólo a su pedido.

**Fix** (`rutas/otros_costos.py`): `_buscar_pedidos_historico` ahora detecta si el documento es una fusión (`fusion_info.es_fusionada`) y, en ese caso, recorre `fusion_info.datos_originales[]`, localiza el original cuyo `pedido_vulcano`/`codigo_pedido` coincide con el buscado y lo proyecta con **sus datos propios** (piezas, peso, placa, destino, cliente, regional, etc.). Nueva función `_proyeccion_pedido_desde_original(original, doc_fusion)`; los campos comunes del vehículo (`manifiesto`, `transportador`) se toman del documento raíz. Las planillas **no fusionadas** siguen proyectándose igual que antes. Sólo backend; el frontend no cambió.

> **Modelo de fusiones en `pedidos_medical_historico`**: al fusionar, los originales se eliminan y el documento fusionado guarda los campos top-level **agregados** y `pedido_vulcano` **concatenado** (`"120795, 120796, ..."`); los datos individuales por pedido se conservan en `fusion_info.datos_originales[]`. Cualquier lookup por pedido en esta colección debe leer de ahí (no del raíz) cuando `fusion_info.es_fusionada` sea verdadero.

## Actualizaciones Recientes (2026-08-04)

### Fix OOM en Banco (`rutas/banco.py`)
El servicio en Render se caía (OOM kill silencioso, sin log) al procesar PDFs de extracto bancario grandes en `POST /banco/pdf-a-excel` (plan starter = 512 MB compartidos con Chromium). El endpoint `async def` hacía trabajo síncrono pesado (pdfplumber + openpyxl) y bloqueaba el event loop, atascando los webhooks de WhatsApp.
- **`asyncio.to_thread`** para `extract_transactions` y `create_excel` → el event loop queda libre.
- **Estilos reutilizados** en openpyxl (`Font`/`Alignment`/`PatternFill` se crean una vez fuera del bucle, no por celda) → menor pico de RAM.
- **`_mem_info()`**: lee `VmRSS`/`VmPeak` de `/proc/self/status` y se registra en 3 puntos del request (recibe PDF, post-pdfplumber, post-Excel) para diagnosticar futuros OOM.
- No sube la RAM del plan; si los extractos siguen siendo muy grandes, conviene un plan con más memoria.

### Otros Costos — Filtrado por regional (alinea con SolicitudVehiculos)
- **OPERATIVO** ve todas las solicitudes (activas/históricas) de su regional (antes solo las propias). **ADMIN/ANALISTA/COORDINADOR/CONTROL** ven todo + dropdown opcional de regional. **FINANCIERO** sin cambios.
- Helpers nuevos en `rutas/otros_costos.py`: `_normalizar_regional`, `_aplicar_filtro_regional`, `_doc_coincide_regional`, maps `CO_A_REGIONAL`/`REGIONAL_A_BODEGA`/`BODEGA_A_REGIONAL`, `PERFILES_GLOBALES_OC`. Normalizan los 3 formatos (código CO `CO05`, ciudad `CALI`, bodega `YUMBO`) y filtran por `$or` sobre `regional_registro` + `datos_servicio.centro_distribucion` (cubre docs viejos sin `regional_registro`).
- `_scope_lectura` aplica la regional del OPERATIVO (fallback a `usuario_registro` + `logger.warning` si no tiene regional). El parámetro `regional` del dropdown sólo se aplica a `PERFILES_GLOBALES_OC` (anti-bypass vía `?regional=`).
- `_obtener_detalle`: OPERATIVO ve el detalle de cualquier solicitud de su regional. **Editar** sigue restringido al creador. Eliminados los overrides manuales de `usuario_registro` en `/historico` y `/exportar-excel`.

### Otros Costos — Causales en colección (`causales_otros_costos`)
- `GET /otros-costos/tipos-costo` ahora lee de la colección `causales_otros_costos` con auto-siembra la primera vez (mismo patrón que `clientes_otros_costos`); editable en Mongo. 14 causales por defecto (AFORO, CARGUE, DESCARGUE, DESVIO, DEVOLUCIONES, ENTREGA EN VEREDA, OTROS, PUNTO ADICIONAL, RECOLECCIONES, REQUERIMIENTO, STAND BY, TRASBORDO, TRASLADO, URGENCIA).

### Otros Costos — Simplificación del formulario y tope de valor
- **Eliminado el campo `concepto`** del modelo `CostoConcepto` (queda `tipo_costo`, `descripcion`, `valor`), la validación y las tablas. La descripción ya cumplía esa función. Datos viejos inertes en Mongo.
- **Eliminado `observaciones`** de `CrearOtroCostoRequest`/`EditarOtroCostoRequest` y del guardado/edición. (No se tocó `observaciones` del pago en `RegistrarPagoRequest`.)
- **Tope `LIMITE_VALOR_SOLICITUD = 5_000_000`**: en `_validar_solicitud` (crear y editar) se rechaza con 422 si el valor total supera $5.000.000.

## Actualizaciones Recientes (2026-08-06)

### Pedidos V3 (`rutas/pedidos.py`) — ahorro/observación, causal del sobre costo y fix de memoria

#### Ahorro y observación en edición de vehículos
- Modelo `AjusteVehiculo` + endpoint `PUT /pedidos/ajustar-totales-vehiculo`: nuevos campos `ahorro` (float, máx 5.000.000) y `observacion` (str). Se validan (`HTTPException 400` si `ahorro > 5.000.000`) y se persisten en `update_fields` de `coleccion_pedidos` por `consecutivo_vehiculo`. Son **metadata**: no participan de `costo_real`, `diferencia_flete` ni `estado`.
- Agregaciones `listar_pedidos_vehiculos` y `listar_vehiculos_completados`: exponen `ahorro`/`observacion` (`$first`) en el `$group` y en la respuesta.

#### Causal del sobre costo en fusión y división (fix del «sin definir»)
- La causal del sobre costo es el campo `Observaciones_ajustes`. **Fusión** (`fusionar-vehiculos`) y **división** (`dividir-vehiculo`) recalculaban `diferencia_flete` pero nunca seteaban la causal → los sobre costos quedaban sin causal.
- Nuevo campo `causal_sobrecosto` en `FusionVehiculosPayload` y `DividirHastaTresPayload`: si la operación genera sobre costo (`costo_real - costo_teorico > 0`) y no viene causal → **HTTP 400**; si viene, se guarda en `Observaciones_ajustes`.
- La **división** calcula todos los carros resultantes, pre-valida la causal y **sólo entonces aplica** (evita una división a medias si falla la validación).
- Nuevo `PUT /pedidos/asignar-causal-completado` (modelo `AsignarCausalCompletadoPayload`): setea `Observaciones_ajustes` (+`usuario_causal`/`fecha_causal`) en `pedidos_completados` por `consecutivo_vehiculo`, para arreglar históricos.

#### Excel de PedidosCompletados reestructurado (`GET /pedidos/exportar-completados`)
- Antes: `pd.DataFrame(docs)` sobre `find()` → volcado crudo con nombres snake_case.
- Ahora: **una sola hoja** con **una fila por pedido/planilla** (query plana, sin `$group`), nombres legibles y primera columna **Fecha** en `DD/MM/AAAA`. Incluye **Planilla** (`planilla_siscore`) y **Destinatario (Ubicación Descargue)**; los totales del vehículo (flete, sobre costo, causal, ahorro) se repiten por pedido etiquetados «(vehículo)». Columna «Causal del sobre costo» = `Sin causal` cuando hay sobre costo sin causal.

#### Fix de memoria/cuelgue con rangos largos (6+ meses)
- Las agregaciones con `$sort` superaban el límite de **32 MB** de MongoDB → `OperationFailure` **code 292** (`QueryExceededMemoryLimitNoDiskUseAllowed`), porque el `$group` arrastraba `pedidos: $$ROOT` (docs completos).
- **`$push` slim** en `listar-vehiculo-completados` y `exportar-completados`: sólo los ~11 campos del detalle en vez de `$$ROOT` → **5,6× menos datos** (18 MB → 3,3 MB a 6 meses) y **6× más rápido**.
- **`allowDiskUse=True`** en las 4 agregaciones (`listar_pedidos_vehiculos`, `listar-vehiculo-completados`, `exportar-completados` y el agregado interno de listado por destinatarios) → Mongo usa disco y nunca falla por el tope de memoria.
- **`asyncio.to_thread`** en `listar-vehiculo-completados` → la agregación bloqueante no cuelga el event loop (mismo patrón que el fix OOM de `banco.py`).

### Solicitud de Vehículos — Devolución de planilla al operativo (`PUT /siscore/actualizar-estado-planilla`)
Hasta ahora COORDINADOR/CONTROL solo podían **Aprobar**; no tenían cómo decirle al operativo que corrija algo, así que la planilla quedaba «trabada» en `REQUIERE_APROBACION_COORDINADOR`/`REQUIERE_APROBACION_CONTROL` sin feedback. Ahora se puede **Devolver**:
- El modelo `ActualizarEstadoPlanillaRequest` suma `motivo_devolucion: Optional[str]`. Cuando se vuelve a `CREADO` **con motivo**, el endpoint persiste `motivo_devolucion`, `devuelto_por` (`aprobado_por`) y `fecha_devolucion`, y deja en `historial_cambios` una entrada con `accion="devolucion"` + `motivo` (en vez del `cambio_estado` genérico). El flujo existente de volver a `CREADO` sin motivo (reapertura de ADMIN/ANALISTA) se mantiene intacto.
- Nueva función **`_notificar_devolucion_operativo(doc, motivo)`**: WhatsApp al `usuario_registro` (resuelto en `baseusuarios`) con la plantilla **`devolucion_planilla`** (`es_CO`, 3 vars: nombre, consecutivo, motivo truncado a 200 car.). Fire-and-forget (solo log). ⚠️ Requiere crear/aprobar la plantilla `devolucion_planilla` en Meta Business Manager; mientras no exista, el WhatsApp falla en silencio y la devolución igual se completa (el motivo queda visible en la app).
- La devolución no dispara las notificaciones a analistas ni de solicitud de autorización (ésas solo aplican al *salir* de `CREADO`).

## Actualizaciones Recientes (2026-08-12)

### Otros Costos — Notificaciones WhatsApp a los actores del flujo

Cada transición del flujo ahora dispara un WhatsApp al actor que debe actuar a continuación (o al creador en los hitos), para que las solicitudes no se estanquen esperando a que alguien abra la app. Patrón idéntico al de SolicitudVehiculos: helpers `_notificar_*` que resuelven celular y nombre en `baseusuarios` y disparan `enviar_template_sync` (de `Funciones/whatsapp_utils_integra`) **fire-and-forget** (si Meta falla o la plantilla no existe, solo queda en log y la acción del flujo igual se completa). Se disparan con `asyncio.to_thread` para no bloquear el event loop.

| Endpoint | Transición | Notifica a | Plantilla |
|----------|-----------|------------|-----------|
| `/enviar-aprobacion` y `/editar`(enviar) | →`pendiente_aprobacion` | COORDINADOR y CONTROL (solo CONTROL si valor > $500.000) | `oc_solicitud_aprobacion` |
| `/aprobar` | →`aprobado` | ANALISTA | `oc_para_tramite` |
| `/marcar-tramite-vulcano` | tramite→`ok` | FINANCIERO | `oc_para_pago` |
| `/registrar-pago` | →`pagado` | OPERATIVO creador | `oc_pago_realizado` |
| `/devolver` | →`devuelto` | OPERATIVO creador | `oc_devuelta` |
| `/rechazar` | →`rechazado` | OPERATIVO creador | `oc_rechazada_anulada` |
| `/anular` | →`anulado` | OPERATIVO creador | `oc_rechazada_anulada` |

**Alcance**: el OPERATIVO creador recibe WhatsApp **solo en hitos clave** (devuelta, pagada, rechazada, anulada); no se le notifica al aprobar ni al marcar trámite OK (lo ve en la app). `/marcar-tramite-vulcano` solo notifica al pasar a `ok` (no al revertir a `pendiente`).

**Plantillas a crear/aprobar en Meta Business Manager** (idioma `es_CO`, 6 en total). Variables en orden (se pasan en `body_params`):

1. **`oc_solicitud_aprobacion`** (→ COORDINADOR/CONTROL): `Hola {{1}}, tienes una solicitud de Otros Costos pendiente de aprobación: {{2}}. Valor: ${{3}}. Revísala en integrApp.` — `{{1}}` nombre · `{{2}}` consecutivo · `{{3}}` valor.
2. **`oc_para_tramite`** (→ ANALISTA): `Hola {{1}}, la solicitud de Otros Costos {{2}} fue aprobada y requiere tu trámite en Vulcano. Valor: ${{3}}.` — `{{1}}` nombre · `{{2}}` consecutivo · `{{3}}` valor.
3. **`oc_para_pago`** (→ FINANCIERO): `Hola {{1}}, la solicitud de Otros Costos {{2}} está lista para pagar. Valor: ${{3}}.` — `{{1}}` nombre · `{{2}}` consecutivo · `{{3}}` valor.
4. **`oc_pago_realizado`** (→ creador): `Hola {{1}}, tu solicitud de Otros Costos {{2}} fue pagada. Valor: ${{3}}.` — `{{1}}` nombre · `{{2}}` consecutivo · `{{3}}` valor.
5. **`oc_devuelta`** (→ creador): `Hola {{1}}, tu solicitud de Otros Costos {{2}} fue devuelta. Motivo: {{3}}. Corrígela y reenvía en integrApp.` — `{{1}}` nombre · `{{2}}` consecutivo · `{{3}}` motivo (≤200 car.).
6. **`oc_rechazada_anulada`** (→ creador): `Hola {{1}}, tu solicitud de Otros Costos {{2}} fue {{3}}. Motivo: {{4}}.` — `{{1}}` nombre · `{{2}}` consecutivo · `{{3}}` la palabra `rechazada` o `anulada` · `{{4}}` motivo.

El valor se formatea en COP con punto de miles (`320.000`). Requiere `WHATSAPP_API_TOKEN` y `WHATSAPP_PHONE_NUMBER_ID` (las mismas vars que SolicitudVehiculos). Mientras las plantillas no estén aprobadas, los logs mostrarán `[NOTIF OC] WhatsApp NO enviado … revisar plantilla …` y el flujo seguirá funcionando. **Archivo**: `rutas/otros_costos.py`.
