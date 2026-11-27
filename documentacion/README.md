# Integra - Sistema de Gestión de Rutas Medical Care

Sistema para la gestión de cruces entre pacientes de Medical Care y pedidos V3, con cálculo de ocupación de rutas y estados de alerta.

## Arquitectura

- **integrappi**: Backend en FastAPI (Python)
- **integrapp-next**: Frontend en Next.js 14 (TypeScript)

## Funcionalidades Implementadas

### Cruce de Pacientes V3

El sistema realiza un cruce entre los pacientes de Medical Care y los pedidos V3 para determinar:

1. **Ocupación de rutas**: Número de pacientes por ruta y porcentaje de ocupación
2. **Estados del cruce**: Alertas automáticas según condiciones específicas

#### Estados del Cruce

Los estados se calculan automáticamente en el backend durante el proceso de cruce:

| Estado | Condición |
|--------|-----------|
| `sin cruce` | El paciente NO está en V3 |
| `retraso FMC` | Días hábiles entre F. Pedido y F. Pref. Integra ≤ 6 |
| `retraso operación` | Estado = "POR PROGRAMAR" Y días hábiles desde hoy a F. Pref. Integra ≤ 3 |
| `—` | Ninguna de las condiciones anteriores (normal) |

#### Cálculo de Días Hábiles

El sistema calcula días hábiles excluyendo:
- Domingos
- Festivos de Colombia (incluyendo Ley Emiliani y Semana Santa)

**Formatos de fecha soportados:**
- `YYYY-MM-DD` (ISO)
- `DD/MM/YYYY` (colombiano)
- `DD MMM YYYY` (ej: "26 abr 2026")

### Endpoints Principales

#### Backend (integrappi)

**Pacientes Medical Care**
- `POST /pacientes-medical-care/cargar-masivo-stream` - Carga masiva de pacientes con SSE
- `GET /pacientes-medical-care/obtener-pacientes` - Lista paginada de pacientes
- `POST /pacientes-medical-care` - Crear paciente
- `PUT /pacientes-medical-care/{id}` - Actualizar paciente
- `DELETE /pacientes-medical-care/{id}` - Eliminar paciente

**Cruce y Ocupación**
- `POST /pacientes-medical-care/recalcular-cruce` - Recalcular cruce completo (SSE)
- `GET /pacientes-medical-care/ocupacion-rutas` - Obtener ocupación de rutas (cacheado)
- `GET /pacientes-medical-care/v3-sin-paciente` - Pedidos V3 sin paciente match
- `GET /pacientes-medical-care/historico-meses` - Lista de meses con histórico
- `GET /pacientes-medical-care/historico-meses-detalle` - Detalle de un mes específico

**Pedidos V3**
- `POST /pedidos-v3/cargar-masivo-stream` - Carga masiva de pedidos V3 con SSE
- `GET /pedidos-v3/` - Lista de pedidos V3 (filtrado por mes actual)
- `GET /pedidos-v3/estados` - Estados únicos de pedidos
- `DELETE /pedidos-v3/eliminar-todos` - Eliminar todos los pedidos (ADMIN)

**Cronograma**
- `POST /cronograma-mc/cargar-masivo-stream` - Cargar cronograma con SSE
- `GET /cronograma-mc/mes-actual` - Obtener cronograma del mes actual
- `DELETE /cronograma-mc/mes` - Eliminar cronograma de un mes (ADMIN)

## Frontend

### Página: CrucePacientesV3P

Ubicación: `integrapp-next/src/Paginas/CrucePacientesV3P/index.tsx`

Muestra:
- Lista de rutas con ocupación
- Pacientes por ruta con estado del cruce
- Pedidos V3 sin paciente match
- Filtros por ruta, CEDI y estado

## Archivos Modificados

### Backend
- `integrappi/rutas/pacientes_medical_care.py`:
  - Agregadas funciones para cálculo de días hábiles
  - Agregada función `_determinar_estado_cruce()` para estados automáticos
  - Modificados endpoints de cruce para calcular `estado_cruce`

### Frontend
- `integrapp-next/src/Funciones/ApiPedidos/tiposMedicalCare.tsx`:
  - Agregado campo `estado_cruce?: string` a `PacienteOcupacion`

- `integrapp-next/src/Paginas/CrucePacientesV3P/index.tsx`:
  - Removida lógica de cálculo de estados del frontend
  - Agregada función `getEstiloEstado()` para visualización
  - Actualizado rendering para usar `estado_cruce` del backend

## Notas Técnicas

### Cache del Cruce
El resultado del cruce se almacena en MongoDB (`cache_cruce_mc`) para evitar recalcular en cada consulta. El cache se invalida al hacer un recálculo manual.

### Server-Sent Events (SSE)
Los endpoints de carga masiva utilizan SSE para enviar progreso en tiempo real al frontend, permitiendo mostrar barras de progreso durante operaciones largas.

### Normalización de Fechas
Las fechas se normalizan al formato YYYY-MM-DD para almacenamiento, pero se pueden ingresar en múltiples formatos según la región.

## Historial de Cambios

### Mayo 2026 — Gestión de Tarifas de Rutas FMC y mejoras visuales

#### Nuevo Módulo: Gestión de Tarifas de Rutas FMC
- **Ubicación**: `/GestionTarifasRutas` (solo accesible para perfil ADMIN)
- **Funcionalidades**:
  - CRUD completo de tarifas de rutas (crear, leer, actualizar, eliminar)
  - Carga masiva desde archivos Excel (.xlsx, .xls)
  - Descarga de plantilla Excel con solo encabezados
  - Filtrado doble: por Ruta (búsqueda de texto) y Centro de Costo (dropdown)
  - Validación estricta de rutas duplicadas (centro_costo + ruta debe ser único)
  - Spinner de carga animado durante procesamiento de archivos
  - Mensajes informativos y alertas claras para errores

#### Campos de Tarifas de Rutas
| Campo | Descripción | Tipo |
|-------|-------------|------|
| CENTRO_COSTO | Centro de costo (regional) | String |
| RUTA | Código de ruta (ej: BOG-MED) | String |
| CARRY | Tarifa Carry | Number |
| NHR | Tarifa NHR | Number |
| TURBO | Tarifa Turbo | Number |
| NIES | Tarifa Nies | Number |
| SENCILLO | Tarifa Sencillo | Number |
| PATINETA | Tarifa Patineta | Number |
| TRACTOMULA | Tarifa Tractomula | Number |
| REQUIERE_DESCARGUE | Requiere descargue (SI/NO) | String |

#### Archivos Creados/Modificados
- `integrappi/rutas/tarifas_rutas_fmc.py` - Endpoints backend
- `integrappi/main.py` - Registro de nueva ruta
- `integrapp-next/src/Paginas/GestionTarifasRutasP/` - Componente frontend
- `integrapp-next/src/Funciones/ApiPedidos/tarifasRutasFmc.tsx` - API frontend
- `integrapp-next/src/Componentes/NavMedicalCare/index.tsx` - Agregado acceso al módulo

#### Mejoras Visuales en MedicalCare
- **Unificación de colores**: Todos los botones ahora usan color sólido profesional `#005f56`
- **Layout vertical**: Botones organizados uno debajo del otro para mejor usabilidad
- Botones actualizados:
  - Ir a Gestión de Pacientes
  - Ir a Gestión de Pedidos V3
  - Cruce Pacientes ↔ V3
  - Configuración Sync
  - Tarifas Rutas FMC (nuevo)

#### Endpoints Nuevos
- `GET /fletes-rutas-fmc/` - Listar todas las tarifas
- `POST /fletes-rutas-fmc/` - Crear tarifa individual
- `PUT /fletes-rutas-fmc/{id}` - Actualizar tarifa
- `DELETE /fletes-rutas-fmc/{id}` - Eliminar tarifa
- `POST /fletes-rutas-fmc/cargar-masivo` - Carga masiva desde Excel/CSV
- `GET /fletes-rutas-fmc/descargar-plantilla` - Descargar plantilla Excel

#### Base de Datos
- **Colección**: `fletes_rutas_fmc`
- **Índice único**: `{centro_costo: 1, ruta: 1}` para evitar duplicados

### Abril 2026 — Recuperación de clave, notificaciones filtradas y perfil CLIENTE_FMC

- **Recuperación de contraseña** para usuarios `baseusuarios`: flujo de 3 pasos (correo → código → nueva clave) vía Resend. Nueva página `/OlvidoClaveBaseUsuario` con toggle de visibilidad de contraseña.
- **Notificaciones filtradas por tipo y CEDI**: cada usuario recibe un correo individual. `retraso_operacion` envía una sola hoja con registros de ese estado; `sin_cruce` envía 2 hojas. ADMIN y CLIENTE_FMC ven todas las regionales; los demás solo su CEDI/regional.
- **Perfil CLIENTE_FMC**: usuarios sin acceso al sistema, solo reciben notificaciones por correo. Admin puede cambiar el perfil asignando contraseña desde GestionUsuarios.
- **GestionUsuarios mejorado**: columna correo visible, botón de reset de contraseña para admin, validaciones según perfil.

---

### Mayo 2026 — Solicitud de Vehículos

#### Nuevo Módulo: Solicitud de Vehículos
- **Ubicación**: `/SolicitudVehiculos` (Medical Care)
- **Funcionalidades**:
  - Consulta de planillas en API de Siscore V3
  - Búsqueda múltiple de planillas (separadas por coma)
  - Cálculo automático de rango de 40 días hábiles hacia atrás
  - Exclusión de fines de semana y festivos de Colombia
  - Filtrado por regional para perfiles operativos
  - Formato colombiano de números (separador de miles)
  - Animación de carga con camión durante consulta
  - Tiempo de consulta visible en segundos
  - Incluye pedidos manuales en la consulta

#### Información Mostrada
| Campo | Descripción |
|-------|-------------|
| Estado | Icono de encontrado/no encontrado |
| Planilla | Número de planilla |
| Piezas | Total de piezas acumuladas |
| Peso Real | Peso total formateado (ej: 2.489,00) |
| Ruta | Código de ruta |
| Código Pedido | Número de pedido |
| Cliente Destino | Nombre del cliente |
| Municipio Destino | Municipio de entrega |
| Departamento Destino | Departamento de entrega |

#### Mapeo de Regionales para API
| Código | Nombre | Envío a API |
|--------|--------|-------------|
| CO04 | BARRANQUILLA | BARRANQUILLA |
| CO05 | CALI | CALI |
| CO06 | BUCARAMANGA | BUCARAMANGA |
| CO07 | FUNZA | **FUNZA - SAN DIEGO 7G** |
| CO09 | MEDELLIN | MEDELLIN |

#### Archivos Creados/Modificados
- `integrappi/rutas/siscore_consultas.py` - Endpoints backend para consulta Siscore
- `integrappi/main.py` - Registro de nueva ruta
- `integrapp-next/src/app/SolicitudVehiculos/page.tsx` - Página frontend
- `integrapp-next/src/app/SolicitudVehiculos/estilos.css` - Estilos
- `integrapp-next/src/Componentes/NavMedicalCare/index.tsx` - Agregado al menú

#### Endpoints Nuevos
- `POST /siscore/consultar-planillas` - Consulta planillas en Siscore
- `GET /siscore/test-connection` - Prueba de conexión

#### Permisos por Perfil
- **ADMIN, COORDINADOR, CONTROL, ANALISTA**: Consultan todas las regionales (centro_distribucion = "TODOS")
- **OPERATIVO**: Consultan solo su regional asignada (con conversión especial para FUNZA)

---

## Estado Actual

El sistema está en producción y funcional. Incluye:
- Gestión completa de pacientes y pedidos V3
- Cruce automático con estados de alerta
- Ocupación de rutas en tiempo real
- **Gestión de tarifas de rutas FMC** para control de fletes
- **Solicitud de vehículos** con consulta a Siscore, cálculo de tarifas y gestión de solicitudes
- **Historial de Pedidos** accesible por ADMIN, ANALISTA, CONTROL, COORDINADOR y OPERATIVO
- **Fusión/División de planillas** con eliminación real de originales y reconstrucción desde datos embebidos
- **Gestión de Divipolas** para búsqueda automática de rutas
- **Otros Costos** — gestión de costos posteriores al servicio con aprobación por monto, pago, histórico y trazabilidad; incluye el perfil **FINANCIERO**
- **Menú unificado** NavMedicalCare en todos los módulos
- Mejoras visuales y de usabilidad en toda la interfaz

---

### 3 Junio 2026 — Acceso OPERATIVO a Histórico, Fix Fusión/División con Vulcano, Mejoras de UX

#### 1. Acceso del perfil OPERATIVO a Historial de Pedidos
- **Página**: `/integrapp/HistoricoPedidos`
- **Cambio**: Se agregó `OPERATIVO` al array de perfiles permitidos
- **Archivos modificados**:
  - `integrapp-next/src/Paginas/HistoricoPedidosP/index.tsx` — Validación de acceso
  - `integrapp-next/src/Componentes/NavMedicalCare/index.tsx` — Visibilidad en menú lateral

#### 2. Fix crítico: Fusión de planillas después de importación Vulcano
**Problema**: Al fusionar planillas que ya habían sido movidas a `pedidos_medical_historico` por la importación Vulcano, las planillas originales seguían "vivas" en el histórico. Solo la planilla fusionada viajaba correctamente.

**Causa raíz**: La fusión intentaba marcar las originales como `fusionada: true` en `pedidos_medical`, pero Vulcano ya las había borrado de ahí y copiado a `pedidos_medical_historico`.

**Solución**:
- **Fusión**: Las planillas originales ahora se **eliminan** (`delete_many`) de AMBAS colecciones (`pedidos_medical` y `pedidos_medical_historico`), no se marcan.
- **División**: Se **reconstruyen** las planillas originales desde `fusion_info.datos_originales` (campo embebido en la planilla fusionada) con `insert_one` en `pedidos_medical`.
- **Histórico**: Los endpoints `/historico` y `/historico/exportar-excel` ya no muestran planillas fusionadas porque las originales fueron eliminadas.

**Archivos modificados**:
- `integrappi/rutas/siscore_consultas.py`:
  - `guardar-busqueda`: `delete_many` en ambas colecciones en vez de `update_many` con `$set`
  - `dividir-fusion`: Reconstrucción desde `datos_originales` con `insert_one`
  - `obtener_historico`: Sin cambios adicionales (las originales ya no existen)
  - `exportar_historico_excel`: Sin cambios adicionales

#### 3. Botón flotante (FAB) mejorado
- **Posición**: Cambio de `position: fixed` a `position: sticky` → no sobrepasa el footer
- **Color**: Cambio de verde oscuro a naranja (`#e65100` → `#ff8f00` gradiente)
- **Ícono**: Cambio de `☰` a `<FaFileExport />` (documento con flecha de exportar)
- **Clic fuera**: Se cierra automáticamente al hacer clic fuera del botón
- **Archivos modificados**:
  - `integrapp-next/src/app/SolicitudVehiculos/page.tsx` — JSX y lógica click-outside
  - `integrapp-next/src/app/SolicitudVehiculos/estilos.css` — Estilos del FAB

#### 4. Menú de navegación (NavMedicalCare)
- **Clic fuera**: El menú desplegable se cierra automáticamente al hacer clic fuera de él
- **Aplica en todas las rutas** del sistema (es un componente compartido)
- **Archivo modificado**: `integrapp-next/src/Componentes/NavMedicalCare/index.tsx`

#### 5. Nuevo campo: `flete_cobrado_fmc`
- **Cálculo**: `piezas × $20,000`
- **Se almacena en MongoDB** junto con todos los demás campos de la planilla
- **Se calcula automáticamente** al:
  - Buscar planillas nuevas
  - Fusionar planillas
- **Se restaura correctamente** al cargar desde MongoDB o dividir una fusión
- **Archivos modificados**:
  - `integrapp-next/src/app/SolicitudVehiculos/page.tsx` — Interface, creación, fusión, restauración
  - `integrappi/rutas/siscore_consultas.py` — `guardar-busqueda` y `dividir-fusion`

**Estructura actualizada del documento en `pedidos_medical`**:
```json
{
  "planilla": "824986",
  "piezas": 15,
  "peso_real": 396,
  "ruta": "R126",
  "tarifa_calculada": 500000,
  "total_solicitado": 500000,
  "flete_cobrado_fmc": 300000,
  "estado": "PREAPROBADO",
  "consecutivo": "FUNZA-20260603-1",
  "consecutivo_base": "FUNZA-20260603-1"
}
```

---

### 12 Mayo 2026 - Mejoras de UX en Edición de Solicitud de Vehículos

#### Modificaciones de Interfaz
- **Botón de Editar**: Reemplaza el botón "Enviar a revisión" con un botón de edición (ícono de lápiz, azul) que abre el modal de detalles
- **Campo Planilla sin clic**: El campo Planilla ahora es texto plano, el clic solo funciona desde el botón de editar
- **Inicialización de campos calculados**:
  - `tarifa_base` se inicializa con el mismo valor de `tarifa_calculada`
  - `tipo_veh_sicetac` se inicializa con el mismo valor de `tipo_vehiculo`
- **Recargos editables en modal**: Descargue, Desvío y Punto Adicional ahora son campos de valor editables (inputs numéricos) en el modal, no select/checkbox
  - Descargue: valor numérico (50000 = SI, 0 = NO)
  - Punto Adicional: valor numérico (80000 = activado, 0 = inactivo)
  - Desvío: valor numérico (100000 = activado, 0 = inactivo)

#### Comportamiento de la Tabla
- Columnas de recargos muestran valores formateados: "$50.000", "$80.000", "$100.000" o "$0"
- Modal permite editar todos los campos con actualización en tiempo real del estado global

---

### 11 Mayo 2026 - Mejoras Solicitud de Vehículos

#### Nuevas Funcionalidades
- **Cálculo de tarifas automático**: Determinación de tipo de vehículo según peso con consulta a `fletes_rutas_fmc` (solo por ruta)
- **Recargos editables**: Descargue ($50.000), Punto Adicional ($80.000), Desvío ($100.000)
- **Total Solicitado**: Cálculo automático de tarifa base + recargos
- **Búsqueda por Divipola**: Si la ruta está vacía en Siscore, busca automáticamente en colección de divipolas
- **Añadir sin reemplazar**: Nuevas planillas se añaden a las existentes (detecta duplicados)
- **Input flexible**: Acepta comas, espacios, saltos de línea, punto y coma como separadores
- **Limpieza de input**: El input se limpia automáticamente después de buscar
- **Columna Regional**: Derivada de Bodega Origen (CO07→FUNZA, CO04→BARRANQUILLA, CO05→CALI, CO06→BUCARAMANGA, CO09→MEDELLIN)
- **Modal de detalles**: Clic en planilla para ver información completa (códigos de pedido, cliente origen, todos los campos)
- **Truncamiento visual**: Código Pedido y Cliente Origen muestran solo una línea con truncamiento
- **Documentos independientes**: Cada planilla se guarda como documento individual en `pedidos_medical` (acceso global para todos los usuarios)
- **Botones de Acciones al inicio**: Eliminar (rojo), Guardar (verde), Enviar (azul)

#### Tabla de Tipos de Vehículo por Peso
| Peso Real (kg) | Tipo Vehículo |
|-----------------|---------------|
| ≤ 1.000 | CARRY |
| 1.001 - 2.300 | NHR |
| 2.301 - 4.500 | TURBO |
| 4.501 - 6.100 | NIES |
| 6.101 - 9.000 | SENCILLO |
| 9.001 - 17.000 | PATINETA |
| > 17.000 | TRACTOMULA |

#### Funciones de Recargo
- **Descargue**: $50.000 (fijo)
- **Punto Adicional**: $80.000 (fijo)
- **Desvío**: $100.000 (fijo)

**Fórmula**: `Total Solicitado = tarifa_base + descargue + punto_adicional + desvio`

#### Nuevos Endpoints
- `POST /siscore/consultar-tarifa` - Consulta tarifa según ruta y tipo de vehículo
- `POST /siscore/guardar-solicitud` - Guarda solicitud en `solicitud_veh_medical`
- `POST /siscore/enviar-tramite` - Envía a `tramite_fmc`
- `GET /siscore/obtener-solicitudes-pendientes` - Obtiene solicitudes sin enviar (filtrado por usuario/perfil)
- `POST /siscore/guardar-busqueda` - Guarda planillas en `pedidos_medical` (documentos independientes)
- `GET /siscore/obtener-resultados-recientes` - Obtiene todas las planillas guardadas (limite 100, sin filtro por usuario)

#### Módulo Divipolas
- **Ubicación**: `/GestionDivipolas` (solo ADMIN)
- **Funcionalidades**: CRUD completo, carga masiva desde Excel, descarga de plantillas
- **Colección**: `divipolas` con campos `divipola` y `ruta`
- **Uso**: Búsqueda automática de rutas cuando Siscore no devuelve ruta

#### Estructura de Datos

**Colección `pedidos_medical`** (documentos independientes por planilla):
```json
{
  "planilla": "824986",
  "encontrada": true,
  "piezas": 12,
  "peso_real": 396,
  "ruta": "R126",
  "codigo_pedido": "3536763690",
  "cantidad_pedidos": 1,
  "cliente_origen": "FRESENIUS MEDICAL CARE",
  "municipio_destino": "DUITAMA",
  "departamento_destino": "BOYACA",
  "regional": "FUNZA",
  "tarifa_calculada": 0,
  "tipo_vehiculo": "CARRY",
  "total_solicitado": 0,
  "requiere_descargue": "NO",
  "punto_adicional": false,
  "desvio": false,
  "fecha_creacion": "2026-05-11T18:55:21.120Z"
}
```

---

### 12 Mayo 2026 - Sistema de Causales para Fusión de Planillas

#### Nueva Funcionalidad: Fusión de Planillas con Causales
- **Ubicación**: MedicalCare → Solicitud de Vehículos
- **Funcionalidades**:
  - **Fusión de planillas**: Seleccionar múltiples planillas y fusionarlas en una sola
  - **Números concatenados**: Las planillas fusionadas se muestran como "846476-846256"
  - **Selección de causal**: Dropdown con SweetAlert2 para elegir la razón de la fusión
  - **Causal guardada**: Se almacena en la planilla fusionada para auditoría

#### Gestión de Causales (Admin)
- **Ubicación**: MedicalCare → Gestión de Causales (solo perfil ADMIN)
- **Funcionalidades**:
  - **Ver causales**: Lista de todas las causales (activas e inactivas)
  - **Crear causal**: Input para agregar nuevas causales
  - **Editar causal**: Click en botón de lápiz para modificar nombre
  - **Activar/Desactivar**: Toggle para cambiar estado de causales
  - **Inicialización automática**: Crea causales por defecto si no existen

#### Causales por Defecto
| Nombre | Descripción |
|--------|-------------|
| lleva paqueteo | La planilla lleva paqueteo adicional |
| no se consiguio vehiculo | No se consiguió vehículo disponible |

#### Estructura de Datos - Causales

**Colección `causales`** en MongoDB:
```json
{
  "_id": ObjectId("..."),
  "nombre": "lleva paqueteo",
  "activo": true,
  "fecha_creacion": "2026-05-12T10:00:00.000Z"
}
```

#### Planilla Fusionada - Campo Causal
```json
{
  "planilla": "846476-846256",
  "causal": "lleva paqueteo",
  "piezas": 24,
  "peso_real": 792,
  "ruta": "R126",
  "tipo_vehiculo": "TURBO",
  "total_solicitado": 450000,
  "tarifa_base": 450000,
  "requiere_descargue": 0,
  "punto_adicional": 0,
  "desvio": 0,
  "aforo": 0,
  "placa": "ABC-123"
}
```

#### Endpoints Nuevos - Causales
- `GET /causales` - Obtiene causales activas (para dropdown fusión)
- `GET /causales/todas` - Obtiene todas las causales (admin)
- `POST /causales` - Crea nueva causal
- `PUT /causales/{id}` - Actualiza causal (nombre, estado)
- `POST /causales/inicializar` - Inicializa causales por defecto

#### Archivos Modificados
- `integrappi/rutas/siscore_consultas.py` - Endpoints de causales y fusión
- `integrapp-next/src/app/SolicitudVehiculos/page.tsx` - Fusión con causal
- `integrapp-next/src/Paginas/MedicalCareP/index.tsx` - Gestión de causales
- `integrapp-next/src/Paginas/MedicalCareP/estilos.css` - Estilos del modal

#### Flujo de Fusión con Causal
1. Usuario selecciona múltiples planillas en Solicitud de Vehículos
2. Click en "Fusionar Planillas"
3. Sistema muestra dropdown con causales activas
4. Usuario selecciona causal
5. Sistema calcula totales (piezas, peso) y consulta tarifa
6. Planilla fusionada se crea con:
   - Números concatenados: "846476-846256"
   - Campo `causal` con el valor seleccionado
   - Tarifa calculada según peso total
   - Placa ingresada por usuario
7. Planillas originales se eliminan de la lista y de MongoDB

#### Eliminación Individual de Planillas

- **Botón de basura**: Elimina planillas individualmente de MongoDB
- **Endpoint `DELETE /siscore/eliminar-planilla`**: Recibe planilla y usuario
- **Confirmación con SweetAlert**: Pide confirmación antes de eliminar
- **Trazabilidad**: Guarda qué usuario eliminó la planilla

#### Sistema de Consecutivos Únicos para Planillas

- **Formato**: `REGIONAL-YYYYMMDD-NUMERO`
  - Ejemplo: `FUNZA-20260527-1`
  - Fusión: `FUNZA-20260527-1A`, `FUNZA-20260527-1B`
- **Independiente por regional/CEDI y fecha**
- **Reutilización de huecos**: Números eliminados quedan disponibles
- **Asignación automática**: Menor número disponible al cargar desde API V3
- **Visible en frontend**: Columna "Consecutivo" con formato monospaciado

**Ejemplos de secuencia**:
```
FUNZA-20260527-1 (individual)
FUNZA-20260527-2 (individual)
FUNZA-20260527-3A (fusión, parte 1)
FUNZA-20260527-3B (fusión, parte 2)
FUNZA-20260527-4 (individual)
```

**Reutilización al eliminar**:
```
Existen: FUNZA-20260527-1, FUNZA-20260527-3
Eliminado: FUNZA-20260527-1
Siguiente: FUNZA-20260527-2 (reutiliza hueco)
```

---

### 26 Mayo 2026 - Sistema de Estados de Aprobación y Causales para Modificaciones

#### Sistema de Cuatro Estados de Aprobación con Jerarquía

**Estados disponibles**:
| Estado | Descripción | Color | Badge |
|--------|-------------|-------|-------|
| PREAPROBADO | Total ≤ teórico | Gris claro | "PREAPROBADO" |
| REQUIERE_APROBACION_COORDINADOR | Total > teórico, diferencia ≤ 7% | Amarillo | "COORDINADOR" |
| REQUIERE_APROBACION_CONTROL | Total > teórico, diferencia > 7% | Rojo | "CONTROL" |
| APROBADO | Aprobado por coordinador/control/admin | Verde | "APROBADO" |
| **SIN TARIFA** | Flete teórico = $0 (caso especial) | Gris oscuro | Solo fila gris |

**Reglas de aprobación por perfil**:
| Perfil | ¿Puede aprobar? | Estados que puede aprobar |
|--------|-----------------|---------------------------|
| ADMIN | Sí | Todos (Coord., Control, Sin Tarifa) |
| CONTROL | Sí | Todos (Coord., Control, Sin Tarifa) |
| COORDINADOR | Sí | Coordinador (≤7%) y Sin Tarifa |
| ANALISTA | No | - |
| OPERATIVO | No | - |

**Caso especial: Flete teórico = $0**
- Fila con fondo gris oscuro y borde izquierdo gris
- Badge adicional "SIN TARIFA" junto al estado
- Funciona normalmente: se puede editar, guardar, aprobar
- ADMIN, CONTROL y COORDINADOR pueden aprobar (no requiere CONTROL exclusivo)

**Recálculo automático**: Cualquier modificación en "Editar Planilla" recalcula el estado según el nuevo total.

#### Gestión de Causales para Modificaciones

**Causal OBLIGATORIO** cuando hay sobrecosto:
- Si `total_solicitado > tarifa_calculada` → DEBE seleccionar causal
- Validación en frontend y backend
- SweetAlert con mensaje de error si falta causal
- Campo con borde rojo cuando está vacío y es obligatorio

**Causal opcional** cuando no hay sobrecosto:
- Si `total_solicitado ≤ tarifa_calculada` → Causal opcional
- Texto verde indicando "Opcional - sin sobrecosto"
- Se puede dejar vacío

**Auto-limpieza de causal**:
- Si el usuario modifica valores y `total ≤ teórico`, la causal se elimina automáticamente
- Si vuelve a haber sobrecosto, debe seleccionar causal nuevamente

#### Persistencia en MongoDB para Fusión/División

**Fusión de planillas**:
- Planillas originales se eliminan de MongoDB (`delete_many`)
- Nueva planilla fusionada se guarda con número concatenado
- Endpoint `/guardar-busqueda` recibe `planillas_a_eliminar`

**División de planillas**:
- Planilla fusionada se elimina de MongoDB
- Planillas originales se restauran desde `fusion_info.datos_originales`

#### Exportación a Excel

**Nueva columna "Observaciones"**:
- Muestra la causal de modificación (si existe)
- Separada de "Causal Fusión" (para planillas fusionadas)
- Manejo robusto de tipos con try-catch

#### Mejoras Visuales y de UX

- **Colores corregidos**: PREAPROBADO ahora muestra fondo blanco (no verde)
- **SweetAlert sobre modal**: z-index corregido (99999) para mensajes de validación
- **Badge de estado**: Muestra el estado actual con color en la tabla
- **Iconos**: FaCheck agregado para validaciones visuales

#### Campos de Estado en MongoDB

```json
{
  "estado": "PREAPROBADO",
  "aprobado_por": null,
  "fecha_aprobacion": null,
  "causal": "lleva paqueteo"
}
```

**Lógica de aprobación**:
- Si `estado = "APROBADO"` → Se mantiene al editar
- Si `total > teorico` → Requiere aprobación
- Si `total ≤ teorico` → Preaprobado

#### Trazabilidad Completa para Dashboard de Indicadores

**Campos de trazabilidad en MongoDB**:
| Campo | Descripción | Ejemplo |
|-------|-------------|---------|
| `usuario_registro` | Quién consultó/registró la planilla | "PPRUEBA" |
| `usuario_modificacion` | Último usuario que editó | "COORDINADOR" |
| `fecha_modificacion` | Fecha de última modificación | "2026-05-26T18:30:00.000Z" |
| `usuario_solicitud_autorizacion` | Quién hizo el cambio que requirió autorización | "PPRUEBA" |
| `fecha_solicitud_autorizacion` | Fecha en que se solicitó autorización | "2026-05-26T17:14:45.553Z" |
| `aprobado_por` | Usuario que aprobó | "COORDINADOR" |
| `fecha_aprobacion` | Fecha de aprobación | "2026-05-26T18:30:00.000Z" |
| `historial_cambios` | Array con todas las modificaciones | Ver estructura abajo |

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
        },
        {
          "campo": "estado",
          "valor_anterior": "PREAPROBADO",
          "valor_nuevo": "REQUIERE_APROBACION"
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
- Recibe `usuario_modificacion` desde el frontend
- Compara valores anteriores con nuevos para detectar cambios
- Agrega entrada al historial por cada modificación
- Registra usuario/fecha cuando estado → REQUIERE_APROBACION

**Exportación a Excel con Trazabilidad**:
| Columna | Descripción |
|---------|-------------|
| Usuario Registro | Quién registró la planilla |
| Usuario Modificación | Último usuario que editó |
| Fecha Modificación | Fecha de última edición |
| Usuario Solicitud Aut. | Quién solicitó autorización |
| Fecha Solicitud Aut. | Fecha de solicitud |
| Aprobado Por | Quién aprobó |
| Fecha Aprobación | Fecha de aprobación |

## Actualizaciones Recientes

### 2026-05-27: Prevención de Búsqueda de Planillas Fusionadas

**Problema**: Cuando un usuario buscaba una planilla que estaba fusionada, el sistema iba a la API de Siscore (perdiendo tiempo) para luego decirle que ya existía.

**Solución implementada**:

1. **Nuevo endpoint `/siscore/verificar-planillas-fusionadas`**:
   - Verifica en MongoDB si las planillas tienen `fusionada: true`
   - Retorna lista de planillas fusionadas con su consecutivo
   - Se ejecuta ANTES de ir a Siscore

2. **Flujo optimizado**:
   ```
   Usuario busca "824986"
   ↓
   Verifica en MongoDB (milisegundos)
   ↓
   SI está fusionada:
     → Muestra SweetAlert inmediato
     → NO va a Siscore
   ↓
   NO está fusionada:
     → Continúa a Siscore normalmente
   ```

3. **Advertencia visual**:
   - Lista de planillas fusionadas encontradas
   - Consecutivo donde está fusionada (ej: FUNZA-20260527-1A)
   - Número de planilla fusionada (ej: 842058-824986)

### 2026-05-27: Corrección en Generación de Consecutivos

**Problema**: Las planillas nuevas reutilizaban números de planillas fusionadas.

**Ejemplo del problema**:
- Existen: FUNZA-20260527-1A (fusionada), FUNZA-20260527-3
- Nueva planilla recibía: FUNZA-20260527-1 (incorrecto)

**Solución**:
- `_generar_consecutivo` ahora considera fusionadas al buscar el máximo
- Usa `numero_consecutivo` y `letra_consecutivo` correctamente
- Nueva planilla recibe: FUNZA-20260527-4 (correcto)

### 2026-05-27: Filtrado de Planillas Fusionadas al Recargar

**Problema**: Al recargar la página, aparecían las planillas originales fusionadas junto con la fusionada.

**Solución**:
- Endpoint `/obtener-resultados-recientes` filtra `fusionada: true`
- Solo muestra planillas activas y fusionadas resultantes

### 2026-05-27: Paradigma de Fusión/División

**Fusión**:
- Originales se marcan `fusionada: true` (no se eliminan)
- Guardan info en `fusionada_en` (planilla fusionada, consecutivo, fecha, usuario)

**División**:
- Endpoint `/siscore/dividir-fusion`
- Reactiva originales (quita marca)
- Elimina planilla fusionada
- Consecutivos se preservan perfectamente

### 2026-05-27: Sonido de Notificación al Finalizar Consulta

**Mejora de UX implementada**:
- Sonido profesional tipo "ding" cuando termina la consulta de planillas
- Web Audio API para generar sonido sin archivos externos
- Frecuencia dinámica: 800Hz → 1200Hz → 800Hz
- Duración: 400ms (corto y agradable)
- Permite al usuario saber cuándo terminó sin mirar la pantalla

### 2026-05-27: Fallback de Regional del Usuario

**Problema**: Planillas sin "Bodega Origen" generaban consecutivos como "--20260527-1"

**Solución**:
- Detecta regionales inválidas: '-', 'TODOS', ''
- Usa la regional del usuario como fallback
- Prioridad: regional planilla → centro distribución → centro costo → **regional usuario** → "TODOS"

**Ejemplo**: Usuario de FUNZA sube planilla sin bodega → `FUNZA-20260527-1` ✓

### 2026-05-27: Eliminación en Cascada de Planillas Fusionadas

**Problema**: Al eliminar una planilla fusionada, las originales quedaban "vivas" en MongoDB

**Solución**:
- Endpoint `/eliminar-planilla` detecta fusiones
- Busca planillas con `fusionada: true` que apuntan a la fusionada
- Elimina originales + fusionada en cascada
- MongoDB queda limpio, sin "libros vivos" sin propósito

### 2026-05-27: Aprobación Masiva de Planillas

**Implementación**:
- Botón "Aprobar" al lado de botón "Fusionar"
- Solo aprueba planillas en estado PREAPROBADO
- Perfiles autorizados: ADMIN, CONTROL, COORDINADOR
- Aprovecha selectores de fusión (checkboxes)

**Validaciones**:
- Filtra automáticamente: Solo PREAPROBADO pasan a aprobación
- Requieren autorización (COORDINADOR/CONTROL): Se excluyen
- Mensaje indica cuáles se aprobarán y cuáles no

**UX**:
- Progreso con loader mientras aprobara
- Sonido de notificación al terminar
- Contador de aprobadas/errores

### 2026-05-27: Recálculo de Estado al Editar

**Problema**: Planilla APROBADA que se edita debe recalcular estado

**Solución**:
- Detecta cuando se edita planilla APROBADA
- Recalcula estado según nuevos valores (total vs teórico)
- Advertencia con SweetAlert encima del modal (z-index 99999)
- Si estado nuevo ≠ APROBADO: Limpia campos de aprobación

**Flujo**:
1. Usuario edita planilla APROBADA
2. Sistema detecta cambio en parámetros
3. Muestra advertencia con nuevo estado en color
4. Usuario acepta
5. Guarda con nuevo estado y campos limpios
6. Fila se pinta del color correspondiente

### 2026-05-27: Mejoras en Modal de Edición

**Problema**: Modal se cerraba al hacer click en inputs o seleccionar texto

**Solución**:
- Overlay ahora cierra con DOBLE CLICK (no click simple)
- Contenido bloquea TODOS los eventos del mouse y puntero
- Permite selección de texto con `userSelect: 'text'`

**Eventos bloqueados**:
- `onClick`, `onMouseDown`, `onMouseUp`
- `onPointerDownCapture`, `onPointerUpCapture`
- `onSelect`, `onInput`, `onChange`
- `onContextMenu`

### 2026-05-27: Validación de Fusión de Planillas Fusionadas

**Problema**: Intentar fusionar planilla ya fusionada crearía fusión anidada

**Solución**:
- Detecta planillas fusionadas antes de fusionar
- Valida: `fusion_info.es_fusionada` o número con guiones
- Bloquea fusión y muestra instrucciones
- Usuario debe dividir primero, luego fusionar

**Mensaje claro**:
```
⚠️ Planillas Fusionadas Detectadas

No puedes fusionar planillas que ya están fusionadas.

Paso 1: Usa el botón de dividir (↱️) para separar las planillas fusionadas
Paso 2: Luego intenta fusionar nuevamente
```

---

### 16 Junio 2026 - Nuevo Módulo: Indicadores de Transporte

#### Dashboard de Indicadores de Guías de Transporte
- **Ubicación**: `/integrapp/indicadores/transporte`
- **Frontend**: `integrapp-next/src/Paginas/IndicadoresTransporte/index.tsx`
- **Backend**: `integrappi/rutas/indicadores_transporte.py`
- **Base de datos**: PostgreSQL (`informe_guias_tms`)

#### Funcionalidades Implementadas

**KPIs en tiempo real**:
- Total de guías/pedidos
- Porcentaje de entregados, pendientes y con novedad
- Total de cajas (piezas) y toneladas
- Distribución por estado con barras horizontales

**Gráficos interactivos**:
- **Cajas por Día**: Gráfico de líneas con toggle para agrupar por mes
- **Pedidos Diarios**: Gráfico de barras apiladas por estado con toggle de agrupación mensual
- **Distribución por Estado**: Barras horizontales con porcentajes
- **Cajas por Cliente**: Gráfico tipo pie con interactividad

**Filtros avanzados (estilo Power BI)**:
- **Año**: Dropdown con checkboxes para selección múltiple de años
- **Mes**: Dropdown con checkboxes para selección de meses
- **Cliente**: Dropdown transformado con:
  - Búsqueda interna dentro del dropdown
  - Checkboxes para selección múltiple
  - Siempre muestra todos los clientes disponibles
  - Visualización de cantidad seleccionada

**Características técnicas**:
- **Conexión a PostgreSQL**: Usa `psycopg2-binary` para conexión directa
- **Agregación server-side**: Queries SQL con `GROUP BY`, `COUNT FILTER`, `SUM`
- **Filtro sargable**: Años convertidos a rangos de fecha para usar índices
- **Optimización móvil**: Lista interactiva de clientes en pantallas ≤768px
- **Cálculo real de cajas**: Backend suma piezas por cliente (no proporcional)

#### Mejoras Implementadas

**1. Corrección del cálculo de cajas por cliente**:
- **Antes**: Frontend calculaba usando proporciones basadas en conteo de guías (incorrecto)
- **Ahora**: Backend calcula directamente `SUM(piezas) GROUP BY cliente, estado`
- **Resultado**: Valores correctos sin distorsión (ej: KABI mantiene 789.273 cajas al seleccionar múltiples clientes)

**2. Transformación del filtro de Cliente**:
- **Antes**: Input de búsqueda + chips estilo Google
- **Ahora**: Dropdown estilo Power BI con búsqueda interna y checkboxes

**3. Optimización móvil del gráfico "Cajas por Cliente"**:
- **Desktop**: Gráfico de pie con labels para top 3 clientes
- **Móvil** (≤768px): Lista interactiva con panel de información al hacer click

**4. Corrección del filtro de clientes**:
- **Problema**: Al seleccionar un cliente, el dropdown solo mostraba ese cliente
- **Solución**: Backend usa cláusula WHERE separada para listar todos los clientes
- **Resultado**: Dropdown SIEMPRE muestra todos los clientes disponibles

#### Variables de Entorno Necesarias

**PostgreSQL**:
- `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USUARIO`, `PG_CLAVE`

#### Endpoints Nuevos

- `GET /indicadores-transporte/guias` - Indicadores agregados
  - Query params: `anio[]`, `mes[]`, `cliente[]`, `estado`, `fecha_inicio`, `fecha_fin`
  
- `GET /indicadores-transporte/guias/detalle` - Registros individuales de un día
  - Query params: `fecha`, `cliente[]`, `estado[]`

#### Archivos Creados/Modificados

**Backend**:
- `integrappi/rutas/indicadores_transporte.py` - Endpoints
- `integrappi/main.py` - Registro de router
- `integrappi/requirements.txt` - `psycopg2-binary==2.9.10`

**Frontend**:
- `integrapp-next/src/Paginas/IndicadoresTransporte/index.tsx` - Componente
- `integrapp-next/src/Paginas/IndicadoresTransporte/estilos.css` - Estilos responsive
- `integrapp-next/src/Funciones/ApiPedidos/indicadoresTransporte.tsx` - API calls

---

### 28 Julio 2026 — Nuevo Módulo: Otros Costos

Sistema para registrar los costos adicionales que algunos vehículos generan después de prestado el servicio (parqueadero, peaje, cargue, horas adicionales, etc.), asociados a uno o varios pedidos de Vulcano del histórico.

- **Rutas**: `/OtrosCostos` (solicitudes activas) y `/HistoricoOtrosCostos` (pagadas)
- **Flujo**: `borrador → pendiente_aprobacion → aprobado → [tramite_vulcano = "ok" marcado por ANALISTA] → pagado (→ histórico)`, con devolución, rechazo y anulación
- **Aprobación por monto**: hasta **$500.000** aprueba Coordinador/Control/Admin; más de $500.000 sólo Control/Admin
- **Paso ANALISTA «trámite Vulcano»**: una vez aprobado, el ANALISTA marca `tramite_vulcano` ("ok"/"pendiente") en `POST /otros-costos/marcar-tramite-vulcano`; Financiero sólo puede pagar cuando es "ok". Perfil ANALISTA habilitado como rol compartido en MEDICAL_CARE.
- **Perfil nuevo FINANCIERO**: acceso únicamente a Otros Costos y su histórico; registra el pago
- **Autorización robusta en backend**: el perfil se resuelve desde `baseusuarios` (no se confía en el frontend)
- **Trazabilidad completa** de cada movimiento (usuario, rol, fecha UTC, IP, observación)
- **Búsqueda tolerante de pedidos**: normaliza ceros a la izquierda y separadores (`, - ; /`)
- **Colecciones**: `otros_costos`, `historico_otros_costos`, `anulados_otros_costos` (solicitudes). ⚠️ La tabla de **configuración** de costos por `tipo_vehiculo` que usa Pedidos vive en `config_otros_costos` (cargada por `POST /fletes/cargar-otros-costos`), **no** en `otros_costos`. Hasta 2026-07-29 compartían nombre y se borraban mutuamente — ver "REGLA DE ORO" en `integrappi/README.md` y la entrada del 29 Jul 2026 más abajo.
- Ver detalle técnico en `integrappi/README.md` y `integrapp-next/README.md`

---

### 29 Julio 2026 — Fix: separación de `config_otros_costos` (colisión de colecciones)

**Problema**: La fusión de vehículos (`POST /pedidos/fusionar-vehiculos`) fallaba con *"No hay configuración de 'otros_costos' para el tipo 'TRACTOMULA'"* (HTTP 400). Causa raíz: la **tabla de configuración de costos por `tipo_vehiculo`** de Pedidos (que alimenta la fusión y el cálculo masivo) y las **solicitudes del módulo Otros Costos** compartían la **misma colección `otros_costos`**. Cada recarga del Excel de configuración (`POST /fletes/cargar-otros-costos`) hacía `delete_many({})` y borraba las solicitudes; la configuración terminó vacía, así que la fusión no encontraba el tipo y fallaba (para cualquier tipo, no solo TRACTOMULA).

**Fix**: la configuración ahora vive en su propia colección **`config_otros_costos`** — writer en `rutas/fletes.py` y los 5 lectores de `rutas/pedidos.py` (~líneas 319, 766, 789, 2026, 2345). Las solicitudes de Otros Costos siguen en `otros_costos`, intactas. Acción requerida para reactivar la fusión: re-subir el Excel de configuración por `/fletes/cargar-otros-costos` para poblar `config_otros_costos`.

**Lección (regla de oro, ver `integrappi/README.md`)**: una colección = un propósito. Nunca reusar nombres de colección entre módulos; revisar `grep 'db\["' rutas/` antes de crear uno nuevo; `delete_many({})` sólo sobre colecciones de propósito único; las tablas de configuración/parámetros van en su propia colección `config_*`.

---

### 31 Julio 2026 — Otros Costos: paso ANALISTA (trámite Vulcano); SolicitudVehiculos: pedido Vulcano por planilla en fusiones

#### Otros Costos
- **Nuevo eslabón del flujo**: `aprobado → [tramite_vulcano = "ok" por ANALISTA] → pagado`. Al aprobar nace `tramite_vulcano = "pendiente"`.
- `POST /otros-costos/marcar-tramite-vulcano` (ANALISTA + ADMIN): avanza `→ok` o revierte `→pendiente`.
- `POST /otros-costos/registrar-pago`: bloquea (422) hasta `tramite_vulcano == "ok"` (filtro atómico también).
- **Umbral coordinador corregido a $500.000** (antes $160.000).
- **Formulario «Nueva solicitud»**: **un solo** pedido de Vulcano por solicitud (label singular + validación que rechaza varios).
- Frontend: botón que alterna OK/Revertir; Pagar sólo con `tramite_vulcano == "ok"`; perfil ANALISTA como rol compartido en MEDICAL_CARE.

#### SolicitudVehiculos — pedido Vulcano por planilla en fusiones
- El flujo **manual** (`POST /siscore/asignar-pedido-manual`) ahora hace la cascada de fusiones que ya hacía el Excel: cada planilla original embebida en `fusion_info.datos_originales` recibe su propio `pedido_vulcano`, y la fusión pasa al histórico **sólo cuando TODAS** tienen pedido (concatenados en el raíz para que filtre).
- Helper compartido `_procesar_pedido_vulcano(consecutivo, pedido, usuario, requiere_aprobado)` en `rutas/siscore_consultas.py` (lo usan `importar-vulcano` y `asignar-pedido-manual`).
- Frontend: al asignar pedido sobre una fusión, modal con un campo por cada planilla original; en Histórico, columna "Pedido Vulcano" por original.
- Ver detalle técnico en `integrappi/README.md`.

---

### 31 Julio 2026 (2) — Ahorro/observación en planillas; catálogo de clientes en Otros Costos; fix SweetAlert

#### SolicitudVehiculos — campos `ahorro` y `observacion`
- En el modal **Editar Planilla**, el operativo puede registrar un **ahorro** (numérico, **máx. $5.000.000**) y una **observación** que lo explique (ej: evitó un vehículo adicional por una fusión). **No afecta** total, diferencia ni estado (es metadato); aplica a registros nuevos/ediciones.
- **Visible en**: columna en la tabla de SolicitudVehiculos, en el **Histórico** (tabla + modal de detalle) y en el **Excel de aprobados** (columnas "Ahorro" y "Observación Ahorro"). Además se totaliza en **Indicadores de Fletes** (tarjeta "Ahorro total").
- **Persistencia**: en `pedidos_medical` vía `PUT /siscore/actualizar-planilla-pedidos`; viaja solo al histórico (la copia es completa) y sobrevive al re-consultar (`guardar-busqueda` usa `$set`). Validación doble: Swal en el frontend y HTTP 400 en el backend si supera $5.000.000.

#### Otros Costos — catálogo de clientes (`clientes_otros_costos`)
- El campo **Cliente** del formulario "Nueva solicitud" ahora es un **desplegable** con los clientes frecuentes (FRESENIUS MEDICAL CARE, CONGRUPO, FRESENIUS KABI, DAVITA, FK SERVICIO TECNICO, ORTOPEDICOS FUTURO COLOMBIA, DISTRIBUIDORA COMTEK, QUIMICA AVANZADA, MINISO COLOMBIA, HUNTER DOUGLAS DE COLOMBIA SAS).
- Se elige de la lista cuando el pedido Vulcano **no** autocompleta el campo. La lista vive en la colección **`clientes_otros_costos`**, editable directo en Mongo (sin tocar código). `GET /otros-costos/clientes` siembra los 9 por defecto la primera vez.

#### Otros Costos — fix SweetAlert detrás del modal
- Los Swal de "Guardar y enviar", aprobar, pagar, etc. quedaban detrás del formulario y sólo aparecían al cerrarlo. Fix: `.swal2-container { z-index: 99999 }` en `OtrosCostosP/estilos.css` (el overlay del modal está a 9999; Swal usa ~1060 por defecto).

---

### 3 Agosto 2026 — Otros Costos: búsqueda individual en fusiones (backend) + header, filtros y validaciones (frontend)

#### Otros Costos — búsqueda de pedidos en planillas fusionadas (backend)
- **Problema**: en el formulario «Nueva solicitud», al buscar un pedido que estaba dentro de una **planilla fusionada**, se traían los datos **agregados del vehículo fusionado** (piezas/peso totales, destino único) en vez de los del pedido individual, porque la búsqueda hacía match sobre el `pedido_vulcano` raíz (concatenado de todos los pedidos) y proyectaba el top-level.
- **Fix** (`integrappi/rutas/otros_costos.py`): `_buscar_pedidos_historico` detecta fusiones (`fusion_info.es_fusionada`), localiza el **original individual** en `fusion_info.datos_originales[]` cuyo pedido coincide y proyecta sus datos propios; nueva `_proyeccion_pedido_desde_original`. `manifiesto`/`transportador` se toman del raíz (comunes al vehículo). Las no fusionadas siguen igual. Detalle técnico en `integrappi/README.md`.

#### Otros Costos — UI y validaciones (frontend)
- **Header dinámico** (`NavMedicalCare`): el badge del encabezado ahora muestra el **nombre de la sección actual** en todas las páginas (antes era fijo «Fresenius Medical Care»). Se quitó el `<h1>` «Otros Costos» redundante.
- **Barra de filtros** (`OtrosCostosP`): un **solo bloque centrado** (fechas · estado · Pedido · Placa · Manifiesto · Cliente · Buscar · Excel); en pantallas grandes se ve en **una fila** y se adapta (`flex-wrap`) en menores.
- **Validaciones en «Nueva solicitud»**: **Manifiesto** sólo dígitos y máx. 15 car.; **Tipo vehículo** = select (`CARRY, NHR, TURBO, NIES, SENCILLO, PATINETA, TRACTOMULA`); **Centro distribución** = select (`JUAN MINA, YUMBO, BUCARAMANGA, GIRARDOTA, FUNZA`). Detalle en `integrapp-next/README.md`.

## Actualizaciones Recientes (2026-08-06)

### Pedidos (`/integrapp/Pedidos` y `/integrapp/PedidosCompletados`) — ahorro/observación, causal del sobre costo y Excel legible

- **Ahorro y observación en «Editar vehículo»**: el modal de edición de un vehículo ahora permite registrar `ahorro` (numérico, máx $5.000.000) y `observacion` (texto), igual que en SolicitudVehiculos. Persisten en la colección `pedidos` vía `PUT /pedidos/ajustar-totales-vehiculo` (metadata: no afectan totales ni estado) y se ven en PedidosCompletados y en el Excel.
- **Fix del causal «sin definir»**: la causal del sobre costo en este módulo es el campo `Observaciones_ajustes`; la **fusión** y la **división** recalculaban el sobre costo pero nunca guardaban la causal, por lo que quedaba vacía (lo que se percibía como «sin definir»). Ahora ambos endpoints **exigen** la causal (`causal_sobrecosto`, HTTP 400) cuando generan sobre costo. La división pre-valida todos los carros resultantes antes de aplicar (no queda a medias). Nuevo `PUT /pedidos/asignar-causal-completado` + botón «⚠️ Sin causal» en PedidosCompletados para arreglar los históricos.
- **Excel de PedidosCompletados reestructurado**: antes era un volcado crudo de Mongo (nombres técnicos, sin filtrar). Ahora es **una sola hoja** con **una fila por pedido/planilla** y nombres legibles: **Fecha** en formato `DD/MM/AAAA`, **Planilla**, **Destinatario (Ubicación Descargue)**, Cliente, Kilos y los totales del vehículo — solicitado y **teórico** (Flete, Cargue/Descargue, Punto Adicional, **Total Solicitado**, **Total Teórico**, **Sobre costo**, **Causal del sobre costo**, **Ahorro**) etiquetados «(vehículo)» (se repiten en los pedidos del mismo vehículo; el sobre costo = Total Solicitado − Total Teórico).
- **Fix de caída con rangos largos (6+ meses)**: las agregaciones con `$sort` superaban el límite de 32 MB de MongoDB (`OperationFailure` code 292) al arrastrar cada grupo el array `pedidos` con `$$ROOT` (docs completos); el frontend lo mostraba como «Network Error». Fix: `$push` slim (solo los campos del detalle, ~5,6× menos datos), `allowDiskUse=True` en las agregaciones y `asyncio.to_thread` para no bloquear el event loop (mismo patrón que el fix de `/banco`). Validado a 6 meses y 1 año.
- Detalle técnico en `integrappi/README.md` y `integrapp-next/README.md`.

### Solicitud de Vehículos (`/integrapp/SolicitudVehiculos`) — botón «Devolver» planilla al operativo

- **Problema**: cuando una planilla pasaba a `REQUIERE_APROBACION_COORDINADOR`/`REQUIERE_APROBACION_CONTROL`, el COORDINADOR/CONTROL solo tenían botón **Aprobar**; no podían devolverla indicando qué corregir, así que el operativo la veía «trabada» sin saber qué arreglar.
- **Botón «Devolver»** (visible para ADMIN/COORDINADOR/CONTROL sobre esos estados): pide un **motivo obligatorio** y devuelve la planilla a **`CREADO`** (el borrador del operativo, único estado donde edita), reutilizando `PUT /siscore/actualizar-estado-planilla`. Guarda `motivo_devolucion` + `devuelto_por` + `fecha_devolucion` y una entrada `accion=devolucion` en `historial_cambios`.
- **Aviso al operativo**: mensaje **WhatsApp** (plantilla `devolucion_planilla`, al `usuario_registro`) **+ aviso visible en la app** (línea roja bajo el badge al recargar). ⚠️ La plantilla `devolucion_planilla` debe crearse/aprobarse en Meta Business Manager; mientras tanto el WhatsApp falla en silencio y la devolución funciona con el aviso in-app.
- Se distingue del botón existente «Volver a CREADO» (ADMIN/ANALISTA, sin motivo, reapertura genérica). Detalle técnico en `integrappi/README.md` y `integrapp-next/README.md`.

---

### 11 Septiembre 2026 — SolicitudVehiculos: bloqueo por municipio restringido (activable por .env)

#### Nueva regla de autorización por municipio (variable de entorno)
- **Variable**: `VALIDAR_MUNICIPIOS_RESTRINGIDOS` en el `.env` del backend (Render). Activada con `SI` (también `TRUE`/`1`/`ON`); ausente o `NO` → todo funciona como antes del cambio.
- **Regla**: planillas de **FUNZA** o **JUAN MINA/BARRANQUILLA** cuyo campo Municipio tenga **varios municipios mezclados** y uno de ellos sea **BOGOTÁ** (Funza) o **BARRANQUILLA** (Juan Mina) quedan bloqueadas en `REQUIERE_APROBACION_COORDINADOR` — aprueba **Coordinador, Control o Admin** (cualquiera de los dos), igual que cuando el flete excede el teórico.
- **Ejemplo**: el consecutivo `FUNZA-20260911-5` con municipios `BOGOTA, MOSQUERA` → no puede ser PREAPROBADO ni APROBADO sin pasar por autorización.
- **No bloquea**: municipio único (aunque sea Bogotá/Barranquilla), ni otras regionales.
- **Aplica en**: guardar/re-consultar planilla, botón «Enviar» (notifica por WhatsApp al Coordinador), y edición de la planilla. La fila se pinta ámbar con badge «COORDINADOR» (mismo estilo del sobrecosto).
- El frontend consulta la configuración al backend al cargar la página (`GET /siscore/config-municipios-restringidos`): prender o apagar la variable **surte efecto sin re-desplegar el frontend**.
- Detalle técnico en `integrappi/README.md`.

#### Mismo día — aprobación solo COORDINADOR/CONTROL/ADMIN y Devolver en PREAPROBADO
- El **ANALISTA ya no aprueba** planillas (antes podía aprobar PREAPROBADO individual y masivamente). La aprobación es exclusiva de **COORDINADOR, CONTROL y ADMIN**.
- El botón **«Devolver»** (rechazo con motivo obligatorio + WhatsApp al operativo) ahora también aparece sobre planillas **PREAPROBADAS**, no solo en las de Coordinador/Control. Visible para ADMIN/COORDINADOR/CONTROL. Sobre APROBADO sigue existiendo solo «Volver a CREADO» (ADMIN/ANALISTA, sin motivo).
- El backend `PUT /siscore/actualizar-estado-planilla` ahora **valida el perfil real** resolviendo el usuario en `baseusuarios` (403 si no corresponde) — ya no depende solo del frontend.

### 16 Septiembre 2026 — Histórico de Pedidos: hoja «Detalle por Pedido» en el Excel Y botón «>» de clientes en la tabla

#### Front: botón «>» (inline, como SolicitudVehiculos)
- `/HistoricoPedidos` tiene una **columna inicial con botón «>»** (todos los perfiles) que despliega bajo cada planilla el **detalle por guía agrupado por CLIENTE**: grupo `▾ CLIENTE` con piezas/peso, filas por guía (pedido, nombre, municipio, piezas, peso), subtotal por cliente y TOTAL. Cliente de cada guía = `registros_detalle['Cliente Origen']` con fallback al de la planilla; en fusiones, columna Planilla original. Archivos: `Paginas/HistoricoPedidosP/{index.tsx,estilos.css}` (clase nueva `HP-detalleRow`). El modal existente no cambió.

#### Excel: hoja «Detalle por Pedido»

- El Excel de `/HistoricoPedidos` (`POST /siscore/historico/exportar-excel`) ahora descarga **2 hojas**:
  1. **«Historico Pedidos»** (la de siempre, una fila por planilla).
  2. **«Detalle por Pedido»** (nueva): **una fila por PEDIDO VULCANO** (el nivel del negocio, no la guía de Siscore), con **Consecutivo Final** (el del documento raíz del histórico, al que quedan amarrados todos los carros de una fusión) y **Consecutivo Original** (el del carro/planilla original dentro de la fusión; en no fusionadas ambos son iguales), Planilla, Pedido Vulcano, Cliente, Piezas, Peso (kg) y **Costo Asignado** (cuánto terminó costando ese pedido Vulcano). Todas las columnas de identificación se repiten en cada fila (filtrables).
- Cada carro de la fusión aporta su propio `pedido_vulcano` (`fusion_info.datos_originales`); el cliente es el `cliente_origen` del carro. Un carro con pedido Vulcano vacío queda como fila con «-» (conserva su parte del costo para que los totales cuadren); si un carro tuviera varios pedidos Vulcano concatenados (docs viejos), se abre una fila por pedido con el flete repartido equitativamente.
- **Costo Asignado**: el `total_solicitado` del documento repartido **proporcionalmente por piezas (cajas)** entre los carros de una fusión (`_repartir_flete`, división entera con residuo en la última fila → suma **exacta**); cada carro entrega su flete a su pedido Vulcano. Misma política de reparto que el indicador de costo-operación.
- **Fecha Creación** del consecutivo en AMBAS hojas (nueva columna junto a «Fecha Preaprobado» en la hoja 1, y tras los consecutivos en la hoja 2): la `fecha_creacion` del documento (en la hoja 2, la del carro original con fallback a la del raíz). Formato `YYYY-MM-DD HH:MM` (UTC, como el resto de fechas del Excel).
- Fila **TOTALES** al final (piezas, peso y costo), auto-filtro y primera fila congelada. Sin cambios de frontend ni de request (misma respuesta, con una hoja más). PENDIENTE deploy.

#### 11 Septiembre 2026 — /Pedidos: «Importar Vulcano» acepta el mismo Excel de SolicitudVehiculos
- `POST /pedidos/cargar-numeros-pedido` acepta el mismo archivo (mismos dos campos CONSECUTIVO + PEDIDO, con todos los alias de encabezados de SV: `No. Pedido`, `NUMERO_PEDIDO`, etc.). Agregada la dependencia `xlrd` (faltaba; los `.xls` fallaban en ambos módulos). Detalle en `integrappi/README.md`.

---

### 23 Septiembre 2026 — Histórico de Pedidos: trazabilidad del flujo en el modal de detalle

- El modal de detalle de `/HistoricoPedidos` (clic en el consecutivo) ahora muestra, **al final del formulario**, la sección **«Trazabilidad del flujo»**: quién registró la planilla, quién solicitó autorización, quién la modificó por última vez, **quién la aprobó y cuándo**, quién asignó el Pedido Vulcano y, si aplica, quién la devolvió con su motivo. Todas las personas se muestran con **nombre y apellido** (el backend `GET /siscore/historico` resuelve los usernames contra `baseusuarios` y agrega campos `*_nombre`; fallback al username si no existe).
- También agrega la sección **«Historial de cambios»**: tabla con el flujo completo (fecha, usuario, acción y detalle `campo: anterior → nuevo`, causal, motivo). Sin scroll interno: el contenido fluye y el scroll queda **vertical en el modal padre**; la celda Detalle acota su ancho (máx 360px) para no ensanchar la tabla.
- El campo **«Diferencia»** del modal se pinta en **rojo cuando es superior a cero** (cuadro con borde izquierdo rojo, mismo lenguaje visual del sobrecosto).
- Archivos: `integrappi/rutas/siscore_consultas.py` (resolución de nombres), `integrapp-next/src/Paginas/HistoricoPedidosP/{index.tsx,estilos.css}`. Requiere deploy de backend y frontend.

#### Mismo día — SolicitudVehiculos: trazabilidad también en «Editar/Ver Planilla»
- El modal de SolicitudVehiculos muestra al final el mismo bloque **«Trazabilidad del flujo»** (Registrado por, Solicitó autorización, Última modificación, **Aprobado por + fecha**, Asignó Pedido Vulcano, Devolución con motivo) y la tabla **«Historial de cambios»**, con **nombres y apellidos**. El backend resolvió la resolución de nombres en el helper compartido `_resolver_nombres_trazabilidad`, aplicado tanto en `GET /siscore/historico` como en `GET /siscore/obtener-resultados-recientes`. Requiere deploy de backend y frontend.

#### Mismo día — SolicitudVehiculos: consecutivo clicable
- Antes el formulario de detalle solo se abría con el botón de lápiz (y para OPERATIVO desaparecía una vez enviada la planilla, quedando sin forma de ver el detalle). Ahora la celda **Consecutivo** es un link que abre el formulario completo con su trazabilidad; para OPERATIVO sobre planillas enviadas (estado ≠ CREADO) abre en **solo lectura** (sin edición ni botón Guardar). Solo frontend.

#### Mismo día — nombres de personas en todos los puntos (nunca usernames)
- Donde quedaba un username visible (aviso «⚠️ Devuelta por …» de las filas, o el estado justo después de aprobar/devolver antes del re-fetch), ahora se muestra el **nombre de la persona**. Nuevo `GET /siscore/nombres-usuarios` (sólo usuario+nombre de `baseusuarios`) que los frontends consultan una vez al montar; resolución con prioridad `*_nombre` del backend → mapa local → valor tal cual. Backend + frontend.

---

### 7 Octubre 2026 — Otros Costos: campo «Proveedor» por concepto de costo

- Nuevo campo **`proveedor`** (dropdown) **en cada concepto de costo** del formulario «Nueva solicitud» de Otros Costos. **Opcional en general, OBLIGATORIO si el tipo de costo es CARGUE o DESCARGUE** (cargues y descargues siempre deben llevar proveedor, validado en backend 422 y frontend).
- Catálogo en la colección **`proveedores_otros_costos`** (auto-siembra: **OTROS, ALL CARGO, OPERATIVO**; editable directo en Mongo con `{ "nombre": "..." }` — agregar proveedores no requiere código ni deploy). `GET /otros-costos/proveedores`.
- Se guarda dentro del array `costos` (viaja al histórico), se precarga al editar y hay columna «Proveedor» en la tabla de conceptos del modal de detalle. Detalle técnico en `integrappi/README.md`. PENDIENTE deploy (backend + frontend).

---

### 2 Octubre 2026 — Otros Costos: alcance de aprobación por cliente (COORDINADOR/CONTROL)

**Problema**: cualquier COORDINADOR/CONTROL aprobaba solicitudes de TODOS los clientes y el WhatsApp de aprobación les llegaba a todos, incluso de clientes con los que no tenían relación.

**Solución**: campo **`clientes_aprobacion`** en `baseusuarios` — `[]` = TODOS (retrocompatible), lista de clientes = sólo esos. Limita **aprobar, devolver, rechazar, ver (activos/histórico/Excel) y recibir WhatsApp** a esos clientes; sólo aplica a COORDINADOR/CONTROL (ADMIN siempre ve/aprueba todo). La asignación se hace en **GestionUsuarios** (columna "Alcance OC" + modal con checkboxes del catálogo `clientes_otros_costos` y opción TODOS); en Otros Costos un chip muestra el alcance propio. Si nadie tiene un cliente asignado, las notificaciones caen a los de alcance TODOS (ninguna solicitud queda huérfana). Detalle técnico en `integrappi/README.md`. PENDIENTE deploy.

---

### 31 Octubre 2026 — Rework TOTAL del indicador On Time (lógica del Excel operativo real)

El cálculo del % OT dejó de ser una aproximación y ahora **replica el Excel operativo real** (`integrappi/calculo real ot.xlsx`, hoja BASE, 30.201 filas, fórmulas BB..BP decodificadas):

- **5 tablas del Excel importadas a Mongo** (`importar_tablas_ot_real.py`): `tiempos_entrega` (188), `tiempos_entrega_st` (237), `correcciones_nombres` (254), `novedades_siscore` (856, guía→MOTIVO del reporte de Siscore) y `citas_agendadas` (7.790, CITASACT). **(2026-11-12) El Excel fue ELIMINADO y el script quedó deprecated — Mongo es la fuente de verdad** (edición directa en las colecciones; el script hacía delete+insert y borraría toda edición manual; respaldo histórico del Excel solo en git).
- **Motor `_calcular_ot_filas`** (`rutas/indicadores_cliente.py`): tipo de servicio → tipo ST → municipio real (con correcciones) → ruta URBANO/NACIONAL → días de las tablas → promesa con `WORKDAY` (días hábiles) → **veredicto contra la entrega ó HOY** (las guías en curso SÍ se juzgan).
- **Trampas que rompían el OT viejo**: la columna `novedad` del informe es el último evento ('ENTREGA DIGITALIZADA') y NO sirve como proxy; la cita válida es SOLO `citas_agendadas` (ni la del TMS ni citas_kabi); `tarifas.promesa_entrega_dias` ya no se usa.
- **4 veredictos** en vez de 8 segmentos: Cumplieron / No cumplieron (incluye vencidas en curso) / **Con novedad** (auto-cumplen: reporte de Siscore **o justificación manual de `/NovedadesOT`** — el módulo sigue vigente) / No aplica.
- **Conciliación: 97,0% de match en entregadas** (28.739/29.620) contra el Excel. El 3% restante espera las columnas del Bot 001.
- **Bot 001 extendido**: carga `ciudad_origen`, `departamento_origen`, `departamento_destino` y `fecha_carga` (copia local lista; pendiente push al repo `agentes` + backfill en trozos de ~15 días).
- Documentación completa en **`INDICADORES.md` §3**. Deploy backend + frontend JUNTOS.

---

### 31 Octubre 2026 — Nueva pantalla de Módulos (/Modulos) + branding unificado de headers

#### Pantalla «Módulos» — nuevo home tras el login
- **`/Modulos` reemplaza el selector de portales** (paso 2 del login): los perfiles multi-módulo (ADMIN, ANALISTA, COORDINADOR, CONTROL, OPERATIVO, DESPACHADOR) aterrizan en un hub con **menú lateral izquierdo siempre visible** y el logo corporativo centrado. Los micro-portales (FINANCIERO, VISUALIZADOR, CLIENTE, CLIENTE_ESTUDIOS, PADRINO) siguen entrando directos a su módulo.
- **Config única del menú** en `src/Funciones/menuModulos.ts` (`construirMenu()` filtra por perfil/clientes). Módulos principales (en negrita): **CUMPLIMIENTO RNDC** (Mintransporte), **GESTIÓN DE ÓRDENES** (Vulcano), **TORRE DE CONTROL** (Mayasoft), **ALMACENAMIENTO** (Siscore WMS), **TMS** (Siscore), **INDICADORES**; grupos desplegables **PROGRAMACIÓN** (Kabi, Medical Care) y **EN RUTA** (Seguridad, Flota Disponible). SICE-TAC y Admin Seguridad salieron del menú (las rutas siguen existiendo).
- **Sidebar SIEMPRE visible** en todos los tamaños (nunca drawer/hamburguesa): expandida en desktop; en pantallas ≤768px **ocupa toda la pantalla** (sin logo central). Usuario abajo con popover (perfil + cerrar sesión).
- **Cookie nueva `usuarioInfoPedidosCookie`** (JSON id/nombre/correo/perfil/perfiles_adicionales): el login la setea y `/Modulos` la usa para escribir las cookies `seguridad*` al entrar al portal de Seguridad sin backend.
- Al navegar setea las cookies de portal (`clientePedidosCookie`) antes del `router.push` — los guards de cada módulo no cambiaron.

#### Branding: todos los headers negro #000 + verde #98C74C
Unificados al look del portal Mis Indicadores en **~25 archivos**: componentes compartidos (`HeaderSesion`, `HeaderApp`, `HeaderLogo`, `IndicadoresChrome` —solo el header—), headers propios (Pedidos/KABI, PedidosCompletados, AdminSeguridad, indicadores, PanelClientes, PanelMisClientes, revision —tokens teal→verde—, PortalSeguridad, logins, landing). **No tocado**: `/VerificarEstudio` (portal público claro) y el contenido interno navy/naranja de IndicadoresChrome. Detalle por archivo en `integrapp-next/README.md` (2026-10-31).
