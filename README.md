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
  - **Campos actualizables**: `tarifa_base`, `requiere_descargue`, `punto_adicional`, `desvio`, `aforo`, `placa`, `tipo_veh_sicetac`
  - **Gestión de estados**: `estado`, `aprobado_por`, `fecha_aprobacion`
  - **Campo de causal**: `causal` (OBLIGATORIO si hay sobrecosto)
- `POST /exportar-planillas-excel` - Exporta planillas a Excel con columna de Observaciones (causal)

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
