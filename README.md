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
- **`solicitud_veh_medical`** - Solicitudes de vehículos con estados de aprobación
- **`pedidos_medical`** - Planillas consultadas en Siscore (documentos independientes)
- **`causales`** - Causales para fusión de planillas
- **`pedidos_medical_historico`** - Planillas movidas después de importación Vulcano (histórico)

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
