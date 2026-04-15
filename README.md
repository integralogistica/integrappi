# IntegraPPI — Backend API de Integra Logística

Backend del sistema de gestión logística de **Integra Cadena de Servicios S.A.S.**, construido con FastAPI y MongoDB. Centraliza la operación de transportadores, empleados, clientes y pedidos, con integración a WhatsApp, sistemas de rastreo externos y generación de documentos.

**Frontend:** ver `../integrapp-next/` (Next.js 14 App Router — migrado desde `../integrapp/` en marzo 2025)

---

## Qué hace este sistema

Sistema integral de gestión logística que centraliza toda la operación de Integra Cadena de Servicios S.A.S.:

### Gestión de Usuarios y Autenticación
- **Transportadores**: Registro, login, recuperación de clave, gestión de perfil
- **Despachadores**: Acceso a torre de control, gestión de pedidos
- **Seguridad**: Inspección de vehículos, aprobación de documentos
- **Admin**: Gestión completa de usuarios, asignación de clientes y permisos
- **Conductores**: Panel de conductor con documentos y firma digital
- Autenticación JWT con expiración de 20 minutos
- Recuperación de contraseña por correo con tokens de un solo uso (expiran en 30 minutos)
- Verificación de códigos de seguridad para recuperación de clave de conductores

### Gestión de Vehículos y Documentación
- Registro de vehículos con placa, marca, modelo, capacidad
- Carga de documentos: SOAT, tecnomecánica, tarjeta de propiedad, seguro, licencia de conducción
- Carga de fotos del vehículo (exterior, interior, documentación)
- Firma digital del propietario
- Biometría: captura y verificación de huellas dactilares (5 dedos por mano)
- Estados del vehículo: pendiente, en_revisión, aprobado, rechazado
- Flujo de aprobación: Registro → Revisión por seguridad → Aprobado/Rechazado
- Notificaciones por correo al cambiar estado del vehículo
- Eliminación de documentos y fotos con limpieza en Google Cloud Storage

### Gestión de Pedidos y Operaciones
- Creación de pedidos individuales con: cliente, destinatario, dirección, municipio, departamento, región, kilos, costo real, costo teórico, observaciones
- **Carga masiva de pedidos desde Excel**: soporta formatos con múltiples plantillas
- Agrupación de pedidos por `consecutivo_vehiculo` (ej: FUNZA-20250711-FUN123)
- Cálculo automático de totales por vehículo: kilos totales, costo real, costo teórico
- Estados de pedidos: AUTORIZADO, PREAUTORIZADO, PENDIENTE, NO AUTORIZADO
- Autorización de vehículos según perfil:
  - ADMIN: puede autorizar cualquier estado
  - OPERATIVO: puede autorizar AUTORIZADO y PREAUTORIZADO
  - SEGURIDAD: no puede autorizar
- Ajustes de totales por vehículo: kilos, tarifas, overrides manuales
- **Fusión de vehículos**: combina dos o más vehículos en uno solo
- **División de vehículos**: divide un vehículo en hasta 3 vehículos según criterios (destinatarios o consecutivos)
- Carga de números de pedido desde Vulcano (integración masiva)
- Exportación de pedidos autorizados a Excel con cálculo de tarifas y tipos de vehículo
- Lista de pedidos completados con filtros por usuario, estados y regionales
- API Power BI para estadísticas completadas por rango de fechas

### Gestión de Clientes y Tarifas
- **Clientes estándar**: NIT, nombre, contacto
- **Clientes Siscore**: Entidad, NIT para integración con rastreo
- **Clientes generales**: Base de datos de destinatarios con cliente_destinatario, dirección, municipio, departamento, coordenadas (lat, lon)
- **Carga masiva** de clientes desde Excel (clientes, clientes_siscore, clientes_general)
- **Tarifas de flete**: Origen, destino, tipos de vehículo (CAMIONETA, CARRY, 4X2, 6X2, 6X4, 8X2, 8X4) con costos asociados
- Búsqueda de tarifa específica por origen, destino y tipo de vehículo
- Carga masiva de tarifas desde Excel

### Gestión de Municipios y Geolocalización
- Base de datos de municipios colombianos con: municipio, departamento, latitud, longitud
- Búsqueda de ubicación por nombre de municipio
- Carga masiva de municipios desde Excel
- Geocodificación automática usando Nominatim (OpenStreetMap)

### Gestión de Manifiestos y Pagos
- **Manifiestos activos** (ExtraePagosNoAplicados): Guías sin liquidar, con información de flete, saldos, fechas
- **Manifiestos pagados** (ExtraePagosAplicados): Guías liquidadas con detalles de pago
- **Novedades**: Registro de incidentes en manifiestos con fecha, descripción, estado
- Consulta de manifiestos por tenedor (propietario de vehículo)

### Chatbot WhatsApp Multi-rol
Sistema de mensajería automatizado con máquina de estados para tres tipos de usuarios:

**Transportador:**
- Consulta de manifiestos activos y pagados
- Verificación de saldos pendientes
- Recuperación de clave de acceso
- Consulta de manifiestos por año específico
- Navegación intuitiva con menú numérico

**Empleado:**
- Solicitud de certificado laboral
- Validación de identidad con cédula
- Envío de certificado en PDF por correo electrónico
- Opción de incluir o no información salarial

**Cliente:**
- Rastreo de guías de envío
- Consulta de estado de guías (integración Siscore)
- Visualización de imágenes de trazabilidad
- Recepción de resultados por WhatsApp

**Características técnicas:**
- Máquina de estados con expiración automática de sesiones
- Detección y prevención de mensajes duplicados
- Registro detallado de todas las interacciones en MongoDB
- Consultas asíncronas a Vulcano y Siscore
- Soporte para proxies en redes restringidas

### Integración con Sistemas Externos

**Vulcano (Sistema de Manifiestos):**
- Consulta de manifiestos por cédula de tenedor
- Extracción de número de manifiesto, fecha, origen, destino, destinatario, estado
- Consulta de manifiestos detallados con pagos y saldos
- Autenticación JWT propia de Vulcano
- Soporte para proxies y configuración de timeout
- Manejo de errores y reintentos automáticos

**Siscore (Rastreo de Guías - SOAP/XML):**
- Consulta de trazabilidad de guías vía SOAP
- Obtención de imágenes de trazabilidad (fotos de entrega)
- Formateo de respuestas XML a JSON
- Soporte para proxy en redes restringidas
- Validación de existencia de guía antes de consultar

**Google Cloud Storage:**
- Almacenamiento de documentos de vehículos (SOAT, tecnomecánica, tarjeta, etc.)
- Almacenamiento de fotos de vehículos
- Almacenamiento de firmas digitales
- Almacenamiento de imágenes de huellas dactilares
- Optimización automática de imágenes (WebP, redimensionamiento)
- Eliminación segura de archivos

**Resend (Correo Electrónico):**
- Envío de certificados laborales (PDF adjunto)
- Envío de enlaces de recuperación de contraseña
- Envío de códigos de verificación
- Notificaciones de cambio de estado de vehículos
- Envío de correos silenciosos (sin bloqueo de la API)

### Gestión de Pacientes - Fresenius Medical Care
Sistema completo de gestión de pacientes con importación masiva, normalización de datos, validación de duplicados y operaciones CRUD individuales.

**Importación Masiva con Streaming SSE:**
- **Endpoint `/cargar-masivo-stream`**: Carga de pacientes desde Excel (.xlsx, .xls, .xlsm) con progreso en tiempo real via Server-Sent Events (SSE)
- **Progreso en tiempo real**: El backend envía eventos SSE con el estado actual de la carga:
  - `stage: 'reading'` - Leyendo archivo Excel
  - `stage: 'processing'` - Procesando registros (muestra progreso % y registros procesados/total)
  - `stage: 'saving'` - Guardando en base de datos
  - `stage: 'complete'` - Carga completada con estadísticas finales
- **Solución a error "I/O operation on closed file"**: El contenido del archivo se lee ANTES de iniciar el StreamingResponse para evitar que el archivo se cierre antes de poder procesarlo
- **Validación temprana**: Valida tipo de archivo y contenido vacío antes de iniciar el generador de SSE
- **Métricas de respuesta**: Tiempo de procesamiento, registros exitosos, registros con errores, lista detallada de errores (primeros 50)
- **Manejo de errores robusto**: Try-catch específico para lectura de archivo y lectura de Excel con mensajes de error claros enviados via SSE

**Normalización Automática de Datos:**
- Implementada en `Funciones/normalizacion_medical_care.py`. **Solo se normalizan los siguientes campos**; el resto se guarda tal cual viene del Excel:
  - `fx_normalizar_paciente()` → campo `paciente`: Primeras 4 palabras, sin signos de puntuación, mayúsculas, reordenamiento alfabético
    - Ejemplo: "Zarate Edwin" → "EDWIN ZARATE"
  - `fx_normalizar_cedula()` → campo `cedula`: Solo dígitos
  - `fx_normalizar_direccion()` → campo `direccion`: Normalización completa con corrección de errores comunes, reordenamiento alfabético
    - Ejemplo: "CALLE 123 BARRIO CENTRO" → "123 BARRIO CALLE CENTRO"
    - Corrige errores comunes: "CAKLE" → "CALLE", "CARREA" → "CARRERA", "TRASVERSAL" → "TRANSVERSAL"
    - Normaliza abreviaturas: "KRA" → "CARRERA", "CLL" → "CALLE", "TV" → "TRANSVERSAL"
  - `fx_separar_telefonos()` → campos `telefono1` y `telefono2`: Separa hasta dos números del campo celular usando separadores comunes (` - `, `/`, `,`, `;`, `|`, `y`). Limpia caracteres no numéricos al inicio/fin antes de partir. Guarda también `celular_original`
    - Ejemplo: `"3168517637 - 3165334389"` → `telefono1: "3168517637"`, `telefono2: "3165334389"`
    - Ejemplo: `"-3123418728"` → `telefono1: "3123418728"`, `telefono2: ""`
- **Campos sin normalización** (se guardan como vienen): `sede`, `departamento`, `municipio`, `ruta`, `cedi`
- **Corrección de caracteres mal codificados**: UTF-8 leído como Latin-1 (ej: `Ã³` → `O`, `Ãº` → `U`)

**Validación de Duplicados:**
- **Duplicados en el archivo**: Valida que no haya cédulas repetidas dentro del mismo archivo de carga
- **Duplicados en base de datos**: Consulta MongoDB antes de insertar para evitar cédulas ya existentes
- **Registra errores detallados**: Cada duplicado se registra con el número de fila y el valor de la cédula duplicada

**Almacenamiento de Campos Originales y Normalizados:**
- Los campos normalizados tienen su versión `_original`: `paciente_original`, `cedula_original`, `direccion_original`, `celular_original`
- Los demás campos (`sede`, `departamento`, `municipio`, `ruta`, `cedi`) se guardan una sola vez tal cual vienen del Excel
- Los teléfonos se almacenan en `telefono1`, `telefono2` (normalizados) y `celular_original` (valor crudo del Excel). El campo `celular` = `telefono1` por compatibilidad
- **Ejemplo**:
  - `cedula_original: "57 310 123 4567"` → `cedula: "573101234567"`
  - `paciente_original: "MARÍA GONZÁLEZ, PEREZ"` → `paciente: "GONZALEZ MARIA PEREZ"`
  - `celular_original: "3168517637 - 3165334389"` → `telefono1: "3168517637"`, `telefono2: "3165334389"`

**Campo `llave`:**
- Campo calculado automáticamente al crear/actualizar/cargar un paciente
- Fórmula: `paciente_normalizado + " " + direccion_normalizada`
- Sirve para cruzar pacientes con pedidos V3 mediante similitud de texto (fuzzy matching)

**Campo `estado`:**
- Valores posibles: `ACTIVO`, `INACTIVO`, `FALLECIDO`
- Default en carga masiva y creación individual: `ACTIVO`
- Solo modificable en la edición individual de cada paciente

**Validación de Columnas:**
- **Columna requerida**: solo `cedula` es obligatoria. `paciente` es opcional: si viene vacío el registro se crea sin nombre
- **Columnas opcionales**: `sede`, `paciente`, `direccion`, `departamento`, `municipio`, `ruta`, `cedi`, `celular`
- **Validación case-insensitive**: Las columnas pueden estar en mayúsculas, minúsculas o mezcladas
- **Mensaje de error claro**: Indica qué columnas faltan si el archivo no cumple con el formato

**Registro de Auditoría:**
- **Usuario de carga**: Guarda el nombre del usuario que realizó la carga (`usuario_carga`)
- **Fecha de carga**: Guarda fecha y hora exacta de la carga (`fecha_carga`)
- **Usuario de actualización**: Guarda el usuario que actualizó un registro (`usuario_actualizacion`)
- **Fecha de actualización**: Guarda fecha y hora de actualización (`fecha_actualizacion`)

**CRUD Completo de Pacientes:**
- **Crear paciente individual**: `POST /pacientes-medical-care/`
  - Valida campos obligatorios (paciente, cedula)
  - Normaliza todos los campos automáticamente
  - Valida duplicados en base de datos
  - Retorna el paciente creado con ID
- **Actualizar paciente existente**: `PUT /pacientes-medical-care/{paciente_id}`
  - Permite editar todos los campos
  - Si la cédula cambia, valida que no exista en otro paciente
  - Registra usuario y fecha de actualización
  - Retorna el paciente actualizado
- **Eliminar paciente individual**: `DELETE /pacientes-medical-care/{paciente_id}`
  - Valida que el paciente existe
  - Elimina el registro de MongoDB
  - Retorna confirmación con ID del paciente eliminado
- **Obtener paciente por ID**: `GET /pacientes-medical-care/{paciente_id}`
  - Retorna todos los campos del paciente (originales y normalizados)
  - Convierte ObjectId a string

**Búsqueda y Listado:**
- **Listado con paginación**: `GET /pacientes-medical-care/?skip=0&limit=100`
  - Retorna lista de pacientes con paginación
  - Convierte ObjectId a string en cada documento
  - Total de registros en la respuesta
- **Búsqueda por cédula**: `GET /pacientes-medical-care/buscar?cedula=123456789`
  - Normaliza la cédula antes de buscar
  - Búsqueda exacta en campo normalizado
- **Búsqueda por nombre**: `GET /pacientes-medical-care/buscar?paciente=Juan`
  - Búsqueda parcial con regex case-insensitive
  - Busca en campo normalizado de paciente

**Eliminación Masiva:**
- **Endpoint**: `DELETE /pacientes-medical-care/eliminar-todos?usuario=USUARIO`
- **Restricción**: Solo perfil ADMIN
- **Retorna**: Número de registros eliminados y usuario que realizó la eliminación

**Base de Datos:**
- **Base de datos**: `integra` (MongoDB)
- **Colección principal**: `pacientes_medical_care` — índice único en campo `cedula`
- **Colección de cache**: `cache_cruce_mc` — documento único `{ tipo: "cruce_completo" }` con los resultados del último cruce, `fecha_calculo` y `calculado_por`
- **Conexión**: Usa `bd.bd_cliente` desde `bd/bd_cliente.py`

**Manejo de Errores:**
- **Errores por fila**: Registra errores sin detener la carga completa
- **Primeros 50 errores**: La respuesta incluye hasta 50 errores para no saturar la respuesta
- **Mensajes descriptivos**: Cada error indica la fila y el motivo específico
- **Códigos de estado HTTP**:
  - 200 OK - Carga completada (con o sin errores)
  - 400 Bad Request - Archivo inválido, columnas faltantes, campos obligatorios
  - 409 Conflict - Duplicado encontrado
  - 500 Internal Server Error - Error inesperado en el servidor

**Endpoints Completos:**

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pacientes-medical-care/cargar-masivo-stream?usuario=USUARIO` | Carga masiva con progreso SSE (streaming) |
| POST | `/pacientes-medical-care/cargar-masivo` | Carga masiva versión clásica (sin streaming) |
| GET | `/pacientes-medical-care/?skip=0&limit=100&cedi=BARRANQUILLA` | Listar pacientes con paginación (filtro `cedi` opcional) |
| GET | `/pacientes-medical-care/buscar?cedula=XXX&paciente=XXX&cedi=CALI` | Buscar por cédula o nombre (filtro `cedi` opcional) |
| GET | `/pacientes-medical-care/ocupacion-rutas` | Lee el cruce desde cache `cache_cruce_mc` en MongoDB |
| GET | `/pacientes-medical-care/v3-sin-paciente` | Lee V3 sin paciente desde cache `cache_cruce_mc` |
| POST | `/pacientes-medical-care/recalcular-cruce?usuario=USUARIO` | Recalcula y guarda el cruce completo con progreso SSE |
| GET | `/pacientes-medical-care/exportar-cruce-excel?cedi=FUNZA` | Exporta el cruce a Excel con 2 hojas (filtro `cedi` opcional) |
| POST | `/pacientes-medical-care/?usuario=USUARIO` | Crear paciente individual |
| PUT | `/pacientes-medical-care/{id}?usuario=USUARIO` | Actualizar paciente (incluye campo `estado`) |
| DELETE | `/pacientes-medical-care/{id}?usuario=USUARIO` | Eliminar paciente |
| GET | `/pacientes-medical-care/{id}` | Obtener paciente por ID |
| DELETE | `/pacientes-medical-care/eliminar-todos?usuario=USUARIO` | Eliminar todos (solo ADMIN) |

**Cruce Pacientes ↔ V3 con Cache:**

El cruce es una operación O(n×m) (rapidfuzz sobre todas las combinaciones). Para evitar recalcularlo en cada petición:

- **Cache en MongoDB**: los resultados se guardan en la colección `cache_cruce_mc` con un único documento `{ tipo: "cruce_completo" }` (upsert). Los endpoints GET leen desde ahí instantáneamente
- **Recalcular bajo demanda**: `POST /recalcular-cruce` es el único punto que dispara el cálculo real. Devuelve un stream SSE con progreso real:
  - `stage: 'loading'` (0-8%) — Cargando pacientes y pedidos V3 desde MongoDB
  - `stage: 'comparing_patients'` (10-60%) — Comparando cada paciente contra todos los V3 (reporta cada 5%)
  - `stage: 'comparing_v3'` (62-90%) — Identificando V3 sin paciente coincidente
  - `stage: 'saving'` (95%) — Guardando resultado en `cache_cruce_mc`
  - `stage: 'complete'` (100%) — Datos completos embebidos en el evento final
- **Filtro por CEDI**: `GET /` y `GET /buscar` aceptan el param `cedi` (regex case-insensitive) para restringir resultados. Usado para control de acceso regional desde el frontend
- **`/ocupacion-rutas` incluye `total_sin_paciente`**: el endpoint GET retorna también el total de V3 sin paciente para que el frontend pueda mostrar el badge desde el primer cargue, sin esperar a que el usuario abra la pestaña

**Algoritmo de cruce (motor):**
- **Motor de similitud**: `rapidfuzz.fuzz.ratio` (extensión C++) — 20-50× más rápido que `difflib.SequenceMatcher`
- **Criterio 1 — Llave** (fuzzy, prioridad): se compara la `llave` del paciente contra todas las llaves V3. Si la similitud ≥ 74%, se marca como cruce con `match_tipo: 'llave'`. El score fuzzy se guarda **siempre** en `similitud`, independientemente de cuál criterio ganó
- **Criterio 2 — Celular** (fallback): si la similitud fuzzy no supera 74%, se normalizan `telefono1` y `telefono2` del paciente y `telefono_original` del pedido V3 eliminando caracteres no numéricos (sin truncar). Si alguno coincide exactamente, se marca como cruce con `match_tipo: 'celular'`. El campo `similitud` sigue mostrando el score fuzzy real (no 100%)
- **`_normalizar_cel()`**: elimina todo carácter no numérico del número de teléfono — **no trunca a 10 dígitos** para evitar falsos positivos por coincidencia parcial de sufijos
- **Ordenamiento de pacientes por ruta**: dentro de cada ruta los pacientes se ordenan primero por `en_v3` descendente (los que sí cruzaron aparecen primero) y luego por `similitud` descendente dentro de cada grupo

**Campos del resultado de cruce por paciente:**
- `paciente`, `cedula`, `direccion_original`, `ruta`, `cedi`, `llave`, `similitud`, `match_tipo`, `llave_v3`, `en_v3`, `estado`
- Datos del pedido V3 cruzado: `estado_pedido`, `fecha_pedido`, `fecha_preferente`, `fecha_entrega`, `planilla`, `municipio_destino`, `divipola`
- `ruta_v3`: ruta del pedido V3 que cruzó (vacío si no cruzó). Se considera **cambio de ruta** cuando `ruta_v3` es diferente a la ruta del paciente en Medical Care, o cuando viene vacío. Estos casos se marcan visualmente en el frontend con fondo rojo oscuro y se cuentan en el badge ⚠️ del encabezado de cada tarjeta
- `celular_paciente`: `telefono1 / telefono2` del paciente (campos usados en el match por celular)
- `telefono_v3`: `telefono_original` del pedido V3 que cruzó

**Endpoint `exportar-cruce-excel`:**
- Lee el cruce desde `cache_cruce_mc`
- Aplica filtro `cedi` si se indica
- Genera un `.xlsx` con openpyxl con dos hojas:
  - **Hoja 1 "Ocupación por Rutas"**: una fila por paciente con CEDI, ruta, nombre, cédula, estado, en_v3, similitud, llave_v3. Filas coloreadas: verde (en_v3=True), amarillo (sim≥50%), rojo (sim<50%)
  - **Hoja 2 "V3 sin Paciente"**: una fila por pedido V3 sin cruce con CEDI, ruta, código_pedido, cliente, dirección, similitud, paciente_cercano. Filas rojas
  - Headers en negrita, freeze panes en fila 1, anchos de columna auto-ajustados

**Documentación:**
- **Archivo de documentación**: `docs/MEDICAL_CARE_EXCEL_IMPORT.md` con detalles completos del sistema
- **Plantilla de Excel**: `plantilla_pacientes.xlsx` en la raíz del proyecto

### Gestión de Pedidos V3 - Fresenius Medical Care
Sistema de gestión de pedidos específico para Medical Care con carga masiva desde Excel, normalización de datos y operaciones CRUD.

**Importación Masiva con Streaming SSE:**
- **Endpoint `/cargar-masivo-stream`**: Carga de pedidos desde Excel (.xlsx, .xls, .xlsm) con progreso en tiempo real via Server-Sent Events (SSE)
- **Progreso en tiempo real**: El backend envía eventos SSE con el estado actual de la carga:
  - `stage: 'reading'` - Leyendo archivo Excel
  - `stage: 'processing'` - Procesando registros (muestra progreso % y registros procesados/total)
  - `stage: 'saving'` - Guardando en base de datos
  - `stage: 'complete'` - Carga completada con estadísticas finales
- **Validación temprana**: Valida tipo de archivo y contenido vacío antes de iniciar el generador de SSE
- **Métricas de respuesta**: Tiempo de procesamiento, registros exitosos, registros con errores, lista detallada de errores (primeros 50)
- **Manejo de errores robusto**: Try-catch específico para lectura de archivo y lectura de Excel con mensajes de error claros enviados via SSE

**Normalización Automática de Datos:**
- **Solo se normalizan 3 campos**; el resto se guarda tal cual viene del Excel:
  - `cliente_destino`: `fx_normalizar_paciente()` — primeras 4 palabras, mayúsculas, reordenamiento alfabético. Guarda también `cliente_destino_original`
  - `direccion_destino`: `fx_normalizar_direccion()` — corrección de errores, abreviaturas, reordenamiento. Guarda también `direccion_destino_original`
  - `telefono`: `fx_normalizar_celular()` — elimina caracteres no numéricos (guarda todos los dígitos sin truncar). Guarda también `telefono_original`
- **Campos sin normalización** (sin `_original`): `codigo_pedido`, `codigo_cliente_destino`, `divipola`, `fecha_pedido`, `fecha_preferente`, `estado_pedido`, `piezas`, `peso_real`, `bodega_origen`, `ruta`, `municipio_destino`

**Filtro de Clientes Institucionales:**
- Los registros cuyo `Cliente Destino` (texto ORIGINAL en mayúsculas) contenga alguna de las siguientes palabras son **excluidos automáticamente** de la carga, tanto en carga manual como en sync automático:
  `DAVITA`, `VANTIVE`, `CLINICA`, `FARMA`, `HOSP`, `FUNDACION`, `RENAL`, `MEDICO`, `SOCIEDAD`, `INSTITUTO`
- La verificación se hace sobre el texto original (no normalizado) para no depender de transformaciones de texto
- Los registros filtrados se cuentan y se devuelven en `registros_filtrados` en la respuesta del SSE / sync
- Función helper: `_es_cliente_excluido(cliente_original.upper())` en `rutas/pedidos_v3.py`, importada también en `Funciones/sync_api_v3.py`

**Normalización de Fechas (`_parsear_fecha`):**
- Función `_parsear_fecha()` en `rutas/pedidos_v3.py` convierte cualquier formato de fecha a `DD/MM/YYYY`
- Soporta: serial numérico de Excel (ej: `46076` → `23/02/2026`), `datetime`/`date` de pandas, strings `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS` y `DD/MM/YYYY`
- Aplica a los campos `Fecha Pedido` y `Fecha Preferente` tanto en carga manual como en sync automático

**Campo `llave`:**
- Campo calculado automáticamente al cargar cada pedido
- Fórmula: `cliente_destino_normalizado + " " + direccion_destino_normalizada`
- Sirve para cruzar pedidos V3 con pacientes mediante similitud de texto

**Validación de Duplicados:**
- **Duplicados en el archivo**: Valida que no haya identificadores repetidos dentro del mismo archivo de carga
- **Duplicados en base de datos**: Consulta MongoDB antes de insertar para evitar registros ya existentes
- **Registra errores detallados**: Cada duplicado se registra con información del registro

**CRUD Completo de Pedidos:**
- **Listar pedidos**: `GET /pedidos-v3/?skip=0&limit=100`
  - Retorna lista de pedidos con paginación
  - Convierte ObjectId a string en cada documento
  - Total de registros en la respuesta
- **Crear pedido individual**: `POST /pedidos-v3/?usuario=USUARIO`
  - Valida campos obligatorios
  - Normaliza todos los campos automáticamente
  - Valida duplicados en base de datos
  - Retorna el pedido creado con ID
- **Actualizar pedido existente**: `PUT /pedidos-v3/{pedido_id}?usuario=USUARIO`
  - Permite editar todos los campos
  - Valida que no exista en otro pedido si cambia el identificador
  - Registra usuario y fecha de actualización
  - Retorna el pedido actualizado
- **Eliminar pedido individual**: `DELETE /pedidos-v3/{pedido_id}?usuario=USUARIO`
  - Valida que el pedido existe
  - Elimina el registro de MongoDB
  - Retorna confirmación con ID del pedido eliminado
- **Obtener pedido por ID**: `GET /pedidos-v3/{pedido_id}`
  - Retorna todos los campos del pedido
  - Convierte ObjectId a string

**Eliminación Masiva:**
- **Endpoint**: `DELETE /pedidos-v3/eliminar-todos?usuario=USUARIO`
- **Restricción**: Solo perfil ADMIN
- **Retorna**: Número de registros eliminados y usuario que realizó la eliminación

**Base de Datos:**
- **Base de datos**: `integra` (MongoDB)
- **Colección**: `pedidos_v3`
- **Conexión**: Usa `bd.bd_cliente` desde `bd/bd_cliente.py`

**Manejo de Errores:**
- **Errores por fila**: Registra errores sin detener la carga completa
- **Primeros 50 errores**: La respuesta incluye hasta 50 errores para no saturar la respuesta
- **Mensajes descriptivos**: Cada error indica la fila y el motivo específico
- **Códigos de estado HTTP**:
  - 200 OK - Carga completada (con o sin errores)
  - 400 Bad Request - Archivo inválido, campos obligatorios faltantes
  - 500 Internal Server Error - Error inesperado en el servidor

**Endpoints Completos:**

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pedidos-v3/cargar-masivo-stream?usuario=USUARIO` | Carga masiva con progreso SSE (streaming) |
| GET | `/pedidos-v3/?skip=0&limit=100` | Listar pedidos con paginación |
| POST | `/pedidos-v3/?usuario=USUARIO` | Crear pedido individual |
| PUT | `/pedidos-v3/{id}?usuario=USUARIO` | Actualizar pedido |
| DELETE | `/pedidos-v3/{id}?usuario=USUARIO` | Eliminar pedido |
| GET | `/pedidos-v3/{id}` | Obtener pedido por ID |
| DELETE | `/pedidos-v3/eliminar-todos?usuario=USUARIO` | Eliminar todos (solo ADMIN) |

---

### Sincronización Automática V3 (`/sync-v3`)

Sistema de sincronización periódica que simula el consumo de una API leyendo un archivo Excel local (`api_v3.xlsx`) y reemplazando la colección `v3` en MongoDB. Diseñado para cuando la API real esté disponible: solo se cambia la fuente en `Funciones/sync_api_v3.py`.

**Funcionamiento:**
- Al iniciar el servidor FastAPI, arranca una tarea de fondo (`asyncio`) que revisa cada 30 segundos si la hora actual coincide con alguno de los horarios configurados
- Cuando coincide: borra todos los pedidos actuales de la colección `v3` e inserta los nuevos del Excel/API
- El chequeo cada 30s es solo comparación de strings en memoria — sin costo de red ni base de datos
- La ejecución real (Excel + MongoDB) ocurre máximo N veces al día según los horarios definidos

**Configuración de horarios** en `rutas/sync_v3.py`:
```python
config = {
    "horarios": ["05:00", "10:30", "19:00"],  # hora Colombia (America/Bogota)
    "activo": True,
}
```

**Zona horaria:** `pytz America/Bogota` — funciona correctamente en Render (que corre en UTC)

**Alias de columnas:** acepta `Fecha Solicitada` como equivalente de `Fecha Preferente`

**Filtro de clientes institucionales:** el sync aplica el mismo filtro que la carga manual — registros con palabras institucionales en `Cliente Destino` (DAVITA, VANTIVE, CLINICA, etc.) son excluidos antes de insertar en MongoDB. El conteo de excluidos se devuelve en `filtrados` en el resultado del sync.

**Endpoints:**

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/sync-v3/config` | Ver horarios configurados y ruta del Excel |
| POST | `/sync-v3/config?activo=false` | Pausar o reanudar el sync |
| POST | `/sync-v3/config` + body `{"horarios":["06:00","14:00"]}` | Cambiar horarios en caliente (sin reiniciar) |
| POST | `/sync-v3/ejecutar` | Disparar sync manualmente ahora mismo |
| GET | `/sync-v3/estado` | Resultado del último sync: timestamp, exitosos, errores, segundos |

**Archivos:**
- `Funciones/sync_api_v3.py` — lógica core: lectura Excel, normalización, reemplazo en MongoDB
- `rutas/sync_v3.py` — endpoints y configuración de horarios
- `api_v3.xlsx` — archivo Excel que simula la API (debe estar en la raíz de `integrappi/`)

### Reportes y Análisis
- **Reporte de uso de WhatsApp**: Estadísticas generales de interacciones
- **Números únicos por estado**: Conteo de usuarios por rol y fecha
- **Descarga en Excel**: Exportación detallada de logs de WhatsApp
- **Reporte de pedidos**: Exportación a Excel con filtros personalizados
- **Reporte de pedidos completados**: Datos históricos para Power BI

### Funciones de Empleados y Certificados
- Base de datos de empleados con: identificación, nombres, apellidos, cargo, fecha ingreso, salario, estado civil, tipo de sangre, EPS, fondo de pensión, fondo de cesantías
- Generación de certificados laborales en PDF con ReportLab
- Opción de incluir u ocultar salario en el certificado
- Envío automático del certificado por correo
- Búsqueda de empleado por número de identificación
- Carga masiva de empleados desde Excel a MongoDB

### Herramientas de Debug y Diagnóstico
- **Debug de red**: Verificación de IP pública, variables de entorno, conectividad
- **Debug de Siscore**: Prueba de conexión SOAP, resolución de host, timeouts
- **Endpoints de diagnóstico** para troubleshooting de integraciones

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Framework web | FastAPI 0.115 + Uvicorn |
| Base de datos | MongoDB Atlas (PyMongo + Motor async) |
| Autenticación | JWT (PyJWT) + OAuth2 + bcrypt |
| Almacenamiento de archivos | Google Cloud Storage |
| Correo electrónico | Resend |
| Mensajería | WhatsApp Cloud API (Meta) |
| Rastreo externo | Vulcano (REST) + Siscore (SOAP/XML) |
| Generación de documentos | ReportLab (PDF) + openpyxl / xlsxwriter (Excel) |
| Procesamiento de datos | Pandas, Pillow |
| Validación | Pydantic v2 |

---

## Estructura del proyecto

```
integrappi/
├── main.py                  # Entrada de la app, registro de routers
├── .env                     # Variables de entorno (no se sube al repo)
├── credenciales.json        # Credenciales de Google Cloud (no se sube al repo)
├── requirements.txt         # Dependencias Python
│
├── bd/                      # Capa de datos
│   ├── bd_cliente.py        # Conexión a MongoDB
│   ├── models/              # Funciones de formato de documentos MongoDB
│   │   ├── usuario.py
│   │   └── saldos.py
│   └── schemas/             # Esquemas Pydantic para validación
│       ├── usuario.py
│       └── saldos.py
│
├── rutas/                   # Endpoints de la API (un archivo por dominio)
│   ├── aut2.py              # Autenticación y usuarios (transportadores)
│   ├── baseusuarios.py      # Usuarios base (despachadores, seguridad, conductores)
│   ├── vehiculos.py         # Registro y documentos de vehículos
│   ├── revision.py          # Revisión y observaciones de vehículos
│   ├── puente_biometrico.py # Almacenamiento de huellas
│   ├── consultar_biometrico.py # Verificación de huellas
│   ├── empleados.py         # Empleados y certificados laborales
│   ├── clientes.py          # Clientes estándar
│   ├── clientes_siscore.py  # Clientes integrados con Siscore
│   ├── clientes_general.py  # Destinos de entrega con geolocalización
│   ├── ciudades_general.py  # Municipios y coordenadas
│   ├── fletes.py            # Tarifas de flete
│   ├── pedidos.py           # Pedidos (CRUD, carga masiva, reportes)
│   ├── pagoSaldos.py        # Manifiestos de pago
│   ├── novedades.py         # Novedades
│   ├── whatsapp_integra.py  # Chatbot WhatsApp (webhook principal)
│   ├── whatsapp_report_integra.py # Reportes de uso de WhatsApp
│   ├── vulcano.py           # Cliente Vulcano (manifiestos)
│   ├── pacientes_medical_care.py  # Pacientes FMC: CRUD, carga SSE, cruce cache, exportar Excel
│   ├── pedidos_v3.py        # Pedidos V3 FMC: CRUD, carga masiva SSE, parseo de fechas
│   ├── sync_v3.py           # Sync automático V3: config de horarios, trigger manual, estado
│   ├── debug.py             # Diagnóstico de red y variables de entorno
│   └── debug_siscore.py     # Diagnóstico de conexión Siscore
│
├── Funciones/               # Utilidades y lógica de integraciones
│   ├── chat_state_integra.py          # Manejo de estado de conversaciones WhatsApp
│   ├── whatsapp_utils_integra.py      # Envío de mensajes, autenticación de transportadores
│   ├── whatsapp_logs_integra.py       # Registro de eventos WhatsApp en MongoDB
│   ├── whatsapp_certificado_integra.py # Generación de certificados por WhatsApp
│   ├── vulcano_whatsapp_format.py     # Formateo de manifiestos Vulcano para WhatsApp
│   ├── siscore_ws_tracking.py         # Cliente SOAP para rastreo Siscore
│   ├── siscore_ws_format.py           # Parseo de respuestas XML de Siscore
│   ├── normalizacion_medical_care.py  # Normalización pacientes/direcciones/teléfonos FMC
│   └── sync_api_v3.py                 # Lógica core del sync V3: lee Excel, normaliza, reemplaza MongoDB
│
├── scripts/                 # Scripts utilitarios (no son parte de la API)
│   ├── crear_indice_sesiones.py  # Crea índices TTL en MongoDB para sesiones
│   ├── subirEmpleados.py         # Carga masiva de empleados desde Excel a MongoDB
│   ├── subirEmpleados.spec       # Configuración de PyInstaller para el script anterior
│   ├── coor.py                   # Geocodifica municipios desde Excel (Nominatim)
│   └── empleados.xlsx            # Plantilla de empleados
│
└── docs/
    └── MANUAL WS TRACKING INTEGRA.docx  # Manual de uso del rastreo por WhatsApp
```

---

## Endpoints principales

### Autenticación y usuarios (`/usuarios`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/usuarios/token` | Login, retorna JWT |
| POST | `/usuarios/` | Crear usuario |
| GET | `/usuarios/{id}` | Obtener usuario |
| PUT | `/usuarios/{id}` | Actualizar usuario |
| DELETE | `/usuarios/{id}` | Eliminar usuario |
| POST | `/usuarios/recuperar/solicitar` | Solicitar recuperación de clave (envía correo) |
| POST | `/usuarios/recuperar/confirmar` | Confirmar nueva clave con token |
| POST | `/usuarios/cambiar-clave` | Cambiar clave autenticado |
| GET | `/usuarios/cedula-nombre` | Listar solo cédula y nombre de usuarios |

### Usuarios Torre de Control (`/baseusuarios`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/baseusuarios/` | Listar todos los usuarios |
| POST | `/baseusuarios/` | Crear usuario |
| PUT | `/baseusuarios/{id}` | Actualizar usuario |
| DELETE | `/baseusuarios/{id}` | Eliminar usuario |
| GET | `/baseusuarios/despachadores` | Listar solo despachadores |
| POST | `/baseusuarios/login` | Login estándar — retorna datos del usuario incluyendo `clientes` |
| POST | `/baseusuarios/loginseguridad` | Login perfil SEGURIDAD/ADMIN |
| POST | `/baseusuarios/loginConductor` | Login perfil CONDUCTOR |
| POST | `/baseusuarios/verificarRecuperacion` | Verificar recuperación (envía código) |
| POST | `/baseusuarios/validarCodigoRecuperacion` | Validar código de recuperación |
| POST | `/baseusuarios/cambiarClaveConductor` | Cambiar clave de conductor |
| PATCH | `/baseusuarios/{id}/clientes` | Actualizar lista de clientes permitidos para un usuario |

> **Campo `clientes`**: cada usuario en `baseusuarios` puede tener un array `clientes: ["KABI", "MEDICAL_CARE"]` que controla a qué portales de cliente tiene acceso. Si el campo no existe en el documento, se toma por defecto `["KABI"]` para compatibilidad con registros anteriores. Valores válidos: `KABI`, `MEDICAL_CARE`.

### Vehículos (`/vehiculos`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/vehiculos/crear` | Registrar vehículo |
| GET | `/vehiculos/obtener-vehiculos` | Listar vehículos del usuario |
| GET | `/vehiculos/obtener-vehiculo/{placa}` | Obtener vehículo por placa |
| PUT | `/vehiculos/actualizar-estado` | Cambiar estado (notifica por correo) |
| PUT | `/vehiculos/actualizar-informacion/{placa}` | Actualizar información del vehículo |
| PUT | `/vehiculos/subir-documento` | Subir documento (tarjeta, SOAT, etc.) |
| PUT | `/vehiculos/subir-estudio-seguridad` | Subir estudio de seguridad |
| PUT | `/vehiculos/subir-foto-seguridad` | Subir foto de seguridad |
| PUT | `/vehiculos/subir-fotos` | Subir múltiples fotos |
| PUT | `/vehiculos/subir-firma` | Subir imagen de firma |
| GET | `/vehiculos/obtener-firma` | Obtener firma en base64 |
| DELETE | `/vehiculos/eliminar-documento` | Eliminar documento |
| DELETE | `/vehiculos/eliminar-foto` | Eliminar foto |
| GET | `/vehiculos/obtener-vehiculos-incompletos` | Vehículos con documentación incompleta |
| GET | `/vehiculos/obtener-aprobados-paginados` | Vehículos aprobados (paginado) |

### Pedidos (`/pedidos`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pedidos/` | Listar pedidos agrupados por consecutivo_vehiculo |
| PUT | `/pedidos/autorizar-por-consecutivo-vehiculo` | Autorizar pedidos por vehículo |
| PUT | `/pedidos/confirmar-preautorizados` | Confirmar pedidos preautorizados |
| DELETE | `/pedidos/eliminar-por-consecutivo-vehiculo` | Eliminar pedidos por vehículo |
| POST | `/pedidos/cargar-masivo` | Carga masiva desde Excel |
| GET | `/pedidos/exportar-autorizados` | Exportar pedidos AUTORIZADOS a Excel |
| POST | `/pedidos/cargar-numeros-pedido` | Cargar números de pedido desde Vulcano |
| POST | `/pedidos/ajustes-vehiculos` | Ajustes por vehículo (kilos, tarifas) |
| POST | `/pedidos/fusion-vehiculos` | Fusionar vehículos |
| POST | `/pedidos/dividir-hasta-tres` | Dividir pedido en hasta 3 vehículos |
| GET | `/pedidos/exportar-completados` | Exportar pedidos completados a Excel |
| GET | `/pedidos/listar-completados` | Listar vehículos completados |
| GET | `/pedidos/pbi-documentos` | API Power BI: documentos por rango de fechas |

### WhatsApp (`/whatsapp-integra`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/whatsapp-integra/webhook` | Recibe mensajes entrantes |
| GET | `/whatsapp-integra/webhook` | Verificación del webhook (Meta) |

### WhatsApp - Reportes (`/whatsapp-report`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/whatsapp-report/resumen` | Estadísticas generales de uso |
| GET | `/whatsapp-report/numeros-por-estado` | Números únicos por rol por día |
| GET | `/whatsapp-report/numeros-por-estado/descargar-excel` | Descargar reporte de números en Excel |
| GET | `/whatsapp-report/excel` | Descargar reporte detallado en Excel |

### Biometría (`/biometria`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/biometria/capturar` | Capturar huella |
| POST | `/biometria/guardar_completo` | Guardar huellas completas (10 dedos) |
| POST | `/biometria/verificar` | Verificar huella |
| POST | `/biometria/subir-imagen` | Subir imagen de huella |
| GET | `/biometria/obtener-huellas-pdf/{cedula}` | Obtener huellas para generar PDF |

### Empleados (`/empleados`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/empleados/` | Listar todos los empleados |
| GET | `/empleados/buscar` | Buscar empleado por identificación |
| POST | `/empleados/enviar` | Enviar certificado laboral por correo |

### Clientes (`/clientes`, `/clientes-siscore`, `/clientes-general`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/clientes/` | Crear cliente estándar |
| GET | `/clientes/` | Listar clientes estándar |
| POST | `/clientes/cargar-masivo` | Carga masiva clientes estándar |
| POST | `/clientes-siscore/` | Crear cliente Siscore |
| GET | `/clientes-siscore/` | Listar clientes Siscore |
| GET | `/clientes-siscore/nit-por-entidad/{entidad}` | Obtener NIT por entidad |
| POST | `/clientes-siscore/cargar-masivo` | Carga masiva clientes Siscore |
| GET | `/clientes-general/` | Listar clientes generales |
| GET | `/clientes-general/por-destinatario/{destinatario}` | Obtener por destinatario |
| GET | `/clientes-general/por-cliente-destinatario/{cliente_dest}` | Obtener por cliente_destinatario |
| POST | `/clientes-general/cargar-masivo` | Carga masiva clientes generales |

### Ciudades (`/ciudades-general`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/ciudades-general/` | Listar municipios |
| GET | `/ciudades-general/ubicacion-por-municipio/{municipio}` | Obtener ubicación por municipio |
| GET | `/ciudades-general/por-municipio/{municipio}` | Obtener por municipio |
| POST | `/ciudades-general/cargar-masivo` | Carga masiva de municipios |

### Fletes (`/fletes`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/fletes/` | Crear tarifa de flete |
| GET | `/fletes/` | Listar todas las tarifas |
| GET | `/fletes/{origen}/{destino}` | Obtener tarifa por ruta |
| GET | `/fletes/buscar-tarifa` | Buscar tarifa específica por origen, destino, tipo |
| PUT | `/fletes/{origen}/{destino}` | Actualizar tarifa |
| DELETE | `/fletes/{origen}/{destino}` | Eliminar tarifa |
| POST | `/fletes/cargar-masivo` | Carga masiva de tarifas |

### Manifiestos y Pagos (`/pagoSaldos`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/pagoSaldos/` | Listar todos los manifiestos |
| GET | `/pagoSaldos/{manifiesto_id}` | Obtener manifiesto por ID |
| GET | `/pagoSaldos/tenedor/{tenedor}` | Listar manifiestos por tenedor |

### Novedades (`/novedades`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/novedades/` | Listar todas las novedades |
| GET | `/novedades/tenedor/{tenedor}` | Listar novedades por tenedor |

### Revisión (`/revision`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/revision/enviar-observaciones` | Enviar observaciones de revisión |

### Pacientes Medical Care (`/pacientes-medical-care`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pacientes-medical-care/cargar-masivo-stream?usuario=USUARIO` | Carga masiva con progreso SSE |
| POST | `/pacientes-medical-care/cargar-masivo?usuario=USUARIO` | Carga masiva clásica |
| GET | `/pacientes-medical-care/?skip=0&limit=100&cedi=BARRANQUILLA` | Listar pacientes (filtro `cedi` opcional) |
| GET | `/pacientes-medical-care/buscar?cedula=XXX&paciente=XXX&cedi=CALI` | Buscar (filtro `cedi` opcional) |
| GET | `/pacientes-medical-care/ocupacion-rutas` | Leer cruce desde cache |
| GET | `/pacientes-medical-care/v3-sin-paciente` | Leer V3 sin paciente desde cache |
| POST | `/pacientes-medical-care/recalcular-cruce?usuario=USUARIO` | Recalcular cruce completo (SSE streaming) |
| GET | `/pacientes-medical-care/exportar-cruce-excel?cedi=FUNZA` | Exportar cruce a Excel (filtro `cedi` opcional) |
| POST | `/pacientes-medical-care/?usuario=USUARIO` | Crear paciente individual |
| PUT | `/pacientes-medical-care/{id}?usuario=USUARIO` | Actualizar paciente |
| DELETE | `/pacientes-medical-care/{id}?usuario=USUARIO` | Eliminar paciente |
| GET | `/pacientes-medical-care/{id}` | Obtener paciente por ID |
| DELETE | `/pacientes-medical-care/eliminar-todos?usuario=USUARIO` | Eliminar todos (solo ADMIN) |

### Pedidos V3 Medical Care (`/pedidos-v3`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pedidos-v3/cargar-masivo-stream?usuario=USUARIO` | Carga masiva con progreso SSE |
| GET | `/pedidos-v3/?skip=0&limit=100` | Listar pedidos con paginación |
| POST | `/pedidos-v3/?usuario=USUARIO` | Crear pedido individual |
| PUT | `/pedidos-v3/{id}?usuario=USUARIO` | Actualizar pedido |
| DELETE | `/pedidos-v3/{id}?usuario=USUARIO` | Eliminar pedido |
| GET | `/pedidos-v3/{id}` | Obtener pedido por ID |
| DELETE | `/pedidos-v3/eliminar-todos?usuario=USUARIO` | Eliminar todos (solo ADMIN) |

### Debug (`/debug`, `/debug-siscore`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/debug/ip` | Obtener IP pública |
| GET | `/debug/env` | Obtener variables de entorno |
| GET | `/debug-siscore/siscore-test` | Probar conexión Siscore |

> La documentación interactiva completa está disponible en `/docs` cuando el servidor está corriendo.

---

## Integraciones externas

### Vulcano
Sistema de manifiestos y pagos de la operación logística. Se consulta vía REST con autenticación JWT propia. Provee el estado de guías (en tránsito, cumplidas, liquidadas) y el detalle de pagos a transportadores.

### Siscore
Sistema de rastreo de guías vía SOAP/XML. Se consulta el historial de movimientos de una guía y se obtienen imágenes de trazabilidad. Soporta proxy para redes restringidas.

### WhatsApp Cloud API (Meta)
Chatbot con máquina de estados para tres tipos de usuario:
- **Transportador**: consulta de manifiestos, saldos, recuperación de clave
- **Empleado**: solicitud de certificado laboral
- **Cliente**: rastreo de envíos vía Siscore

### Google Cloud Storage
Almacenamiento de documentos de vehículos (bucket `integrapp`) e imágenes de huellas dactilares. Las imágenes se comprimen automáticamente a WebP antes de subir.

### Resend
Envío de correos para: certificados laborales (PDF adjunto), recuperación de clave, notificaciones de revisión de vehículos y códigos de verificación.

---

## Variables de entorno

Crear un archivo `.env` en la raíz con las siguientes variables:

```env
# Base de datos
MONGO_URI=mongodb+srv://<usuario>:<clave>@<cluster>.mongodb.net/...

# Google Cloud (ruta al archivo de credenciales)
GOOGLE_APPLICATION_CREDENTIALS=./credenciales.json

# Correo (Resend)
RESEND_API_KEY=re_...
MAIL_FROM=no-reply@integralogistica.com

# JWT
JWT_SECRET=<clave_secreta_larga>
RESET_TOKEN_EXPIRE_MINUTES=30

# WhatsApp (Meta)
WHATSAPP_API_TOKEN=...
WHATSAPP_PHONE_NUMBER_ID=...
WHATSAPP_VERIFY_TOKEN=...

# Siscore (SOAP)
SISCORE_SOAP_TOKEN=...
SISCORE_SOAP_ENDPOINT=https://integra.appsiscore.com/app/ws/trazabilidad.php
SISCORE_SOAP_ACTION=ConsultarGuiaImagen

# Vulcano
VULCANO_HOST=https://...
VULCANO_BASE_PATH=/vulcano
VULCANO_USERNAME=...
VULCANO_IDNAME=...
VULCANO_AGENCY=001
VULCANO_PROJECT=1
VULCANO_IS_GROUP=0

# Proxy (opcional, para redes restringidas)
VULCANO_PROXY_URL=http://ip:puerto
VULCANO_VERIFY_SSL=true
VULCANO_CONNECT_TIMEOUT=10

# Frontend (para links de recuperación de clave)
FRONTEND_URL_RECUPERAR=https://integralogistica.com/integrapp/recuperar-clave
```

---

## Cómo correr el proyecto

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar variables de entorno
cp .env.example .env   # editar con los valores reales

# 3. Iniciar el servidor
python main.py
```

El servidor queda disponible en `http://localhost:8000`.

- Documentación interactiva (Swagger): `http://localhost:8000/docs`
- Documentación alternativa (ReDoc): `http://localhost:8000/redoc`

### CORS habilitado para:
- `http://localhost:5173` (frontend Vite legacy — `../integrapp/`)
- `http://localhost:3000` (frontend Next.js — `../integrapp-next/`)
- `http://127.0.0.1:3000`
- `https://integralogistica.com` (producción)
- `https://www.integralogistica.com`

---

## Scripts utilitarios

Estos scripts se corren de forma independiente, no son parte de la API:

| Script | Uso |
|---|---|
| `scripts/subirEmpleados.py` | Interfaz gráfica para cargar empleados desde Excel a MongoDB |
| `scripts/crear_indice_sesiones.py` | Crea índice TTL en MongoDB para expirar sesiones automáticamente |
| `scripts/coor.py` | Geocodifica municipios desde un Excel usando OpenStreetMap (Nominatim) |

---

## Seguridad

- Contraseñas almacenadas con bcrypt (nunca en texto plano)
- JWT con expiración de 20 minutos
- Tokens de recuperación de un solo uso, expiran en 30 minutos
- `.env` y `credenciales.json` excluidos del repositorio vía `.gitignore`
- Endpoints protegidos con OAuth2 Bearer Token

---

## Historial de cambios relevantes

### Marzo 2025 — Multi-cliente y panel de administración
- **`baseusuarios.py`**: añadido campo `clientes: Optional[List[str]]` al modelo `BaseUsuario`. El endpoint `/login` ahora devuelve el array `clientes` en la respuesta. Compatibilidad hacia atrás: documentos sin el campo retornan `["KABI"]` por defecto.
- **`baseusuarios.py`**: nuevo endpoint `PATCH /baseusuarios/{id}/clientes` para que administradores actualicen los portales de cliente a los que tiene acceso cada usuario. Solo acepta los valores `KABI` y `MEDICAL_CARE`.
- **`baseusuarios.py`**: `modelo_usuario()` actualizado para incluir `clientes` en todas las respuestas CRUD.
- **`main.py`**: CORS actualizado para incluir explícitamente `http://localhost:3000` y `http://127.0.0.1:3000`.