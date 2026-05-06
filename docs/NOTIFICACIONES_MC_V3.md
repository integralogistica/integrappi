# Sistema de Notificaciones WhatsApp V3 - Medical Care

## Descripción

Sistema de notificaciones automáticas que se ejecutan tras cada sincronización V3 exitosa. Envía mensajes personalizados a los usuarios según sus preferencias de notificación y guarda el historial en MongoDB para consumo por PowerBI.

## Tipos de Notificaciones

### 1. Retraso Operación
- **Código**: `retraso_operacion`
- **Reciben**: Usuarios operacionales que necesitan monitorear pedidos con retraso
- **Contenido**: Conteo de pacientes con estado_cruce = "retraso operación" (TODOS, sin filtro de urgencia)
- **Plantillas**:
  - **Operativos**: `retraso_operacion_fmc_` (2 parámetros: CEDI, total)
  - **Admin**: `confirmar_actualizacion` con desglose por CEDI
- **Ejemplo mensaje operativo**:
  ```
  🚨 RETRASO OPERACIÓN — FUNZA
  
  Tienes 54 pedidos con retraso operación que requieren montaje urgente.
  
  Por favor gestionar a la brevedad.
  _Integra Logística_
  ```

### 2. Sin Cruce
- **Código**: `sin_cruce`
- **Reciben**: Usuarios que necesitan monitorear pacientes no montados
- **Contenido**: Conteo de pacientes con en_v3 = False (CON filtro: fecha del mes actual y < 6 días hábiles)
- **Plantillas**:
  - **Operativos**: `pacientes_sin_montar_fmc` (2 parámetros: CEDI, total)
  - **Admin**: `confirmar_actualizacion` con desglose por CEDI
- **Ejemplo mensaje operativo**:
  ```
  ⚠️ PACIENTES SIN MONTAR — FUNZA
  
  Tienes 271 pacientes que aún no han sido montados por parte del cliente.
  
  Por favor gestionar con Fresenius Medical Care.
  _Integra Logística_
  ```

## Configuración de Usuarios

Los usuarios deben tener los siguientes campos configurados en MongoDB (colección `baseusuarios`):

```javascript
{
  "_id": ObjectId("..."),
  "nombre": "Juan Pérez",
  "usuario": "JPEREZ",
  "clientes": ["MEDICAL_CARE"],
  "regional": "CO04",           // CO04=BARRANQUILLA, CO05=CALI, etc.
  "celular": "3001234567",      // Sin indicativo, se agrega 57 automáticamente
  "notificaciones_mc": [        // Array con tipos de notificación deseados
    "retraso_operacion",
    "sin_cruce"
  ]
}
```

### Formatos soportados para `notificaciones_mc`:
- **Array (recomendado)**: `["retraso_operacion", "sin_cruce"]`
- **String (compatibilidad)**: `"retraso_operacion"`

### Regional Codes:
- `CO04`: BARRANQUILLA
- `CO05`: CALI
- `CO06`: BUCARAMANGA
- `CO07`: FUNZA
- `CO09`: MEDELLIN

## Colección MongoDB para PowerBI

### Colección: `notificaciones_mc_historial`

Almacena un registro por regional por cada sync V3 exitoso.

```javascript
{
  "_id": ObjectId("..."),
  "fecha_hora": "2026-05-05 10:30:00",
  "regional": "CO04",
  "nombre_cedi": "BARRANQUILLA",
  "total_retraso_operacion": 15,
  "total_sin_cruce": 8,
  "total_pacientes": 250,
  "usuarios_notificados": [
    {
      "usuario_id": "...",
      "usuario_nombre": "Juan Pérez",
      "celular": "573001234567",
      "notificaciones": ["retraso_operacion", "sin_cruce"]
    }
  ]
}
```

### Campos disponibles para PowerBI:
- `fecha_hora`: Fecha y hora de la notificación
- `regional`: Código de regional (CO04, CO05, etc.)
- `nombre_cedi`: Nombre del CEDI
- `total_retraso_operacion`: Total de pacientes con retraso operación
- `total_sin_cruce`: Total de pacientes sin montar
- `total_pacientes`: Total de pacientes en la regional
- `usuarios_notificados`: Array con detalle de usuarios notificados

### Query de ejemplo para PowerBI:
```javascript
// Promedio diario de retrasos operación por CEDI (últimos 30 días)
db.notificaciones_mc_historial.aggregate([
  {
    $match: {
      fecha_hora: {
        $gte: new Date(new Date() - 30 * 24 * 60 * 60 * 1000)
      }
    }
  },
  {
    $group: {
      _id: "$nombre_cedi",
      avg_retraso_operacion: { $avg: "$total_retraso_operacion" },
      avg_sin_cruce: { $avg: "$total_sin_cruce" },
      total_notificaciones: { $sum: 1 }
    }
  }
])
```

## Configuración de WhatsApp

### Variables de entorno requeridas:
```env
WHATSAPP_API_TOKEN=your_token_here
WHATSAPP_PHONE_NUMBER_ID=your_phone_id_here
```

### Plantillas de WhatsApp:

#### Para operativos (plantillas oficiales Meta):

**1. retraso_operacion_fmc_** (Retraso Operación)
- **Idioma**: `es_CO`
- **Parámetros**: `{{1}}` = CEDI/regional, `{{2}}` = total de pedidos
- **Formato**:
  ```
  🚨 *RETRASO OPERACIÓN* — {{1}}

    Tienes {{2}} pedidos con retraso operación que requieren montaje urgente.

    Por favor gestionar a la brevedad.
    _Integra Logística_
  ```

**2. pacientes_sin_montar_fmc** (Sin Cruce)
- **Idioma**: `es_CO`
- **Parámetros**: `{{1}}` = CEDI/regional, `{{2}}` = total de pacientes
- **Formato**:
  ```
  ⚠️  *PACIENTES SIN MONTAR* — {{1}}

    Tienes {{2}} pacientes que aún no han sido montados por parte del cliente.

    Por favor gestionar con Fresenius Medical Care.
    _Integra Logística_
  ```

#### Para admin (con desglose por CEDI):

**3. confirmar_actualizacion** (Plantilla genérica)
- **Idioma**: `es_CO`
- **Parámetros**: `{{1}}` = mensaje completo (una sola línea, sin saltos de línea)
- **Formato**:
  ```
  {{1}}

  _Integra Logística_
  ```
- **Nota**: Meta rechaza saltos de línea en templates genéricos, usar separador ` | `

## Funcionamiento

1. **Sync V3**: Se ejecuta según horarios configurados (05:00, 10:30, 19:00 hora Bogotá)
2. **Cruce automático**: Tras sync exitoso, se recalcula el cruce pacientes ↔ V3
3. **Notificaciones**:
   - Se lee el cache del cruce desde MongoDB
   - Se buscan usuarios con MEDICAL_CARE y notificaciones_mc configuradas
   - Se filtran por regional
   - Se calculan estadísticas por regional
   - Se envían mensajes WhatsApp personalizados
4. **PowerBI**: Se guarda historial en `notificaciones_mc_historial`

## Endpoint de Configuración

### Actualizar notificaciones de un usuario:
```http
PATCH /baseusuarios/{id}/notificaciones_mc
Content-Type: application/json

{
  "notificaciones_mc": ["retraso_operacion", "sin_cruce"]
}
```

## Logs

El sistema registra los siguientes logs:
- `[sync_v3] Enviando notificaciones a X usuarios`
- `[sync_v3] WS enviado a 57XXX (Nombre) - retraso_operacion`
- `[sync_v3] WS enviado a 57XXX (Nombre) - sin_cruce`
- `[sync_v3] Guardados X registros en notificaciones_mc_historial`

## Troubleshooting

### No se envían notificaciones:
1. Verificar que WHATSAPP_API_TOKEN y WHATSAPP_PHONE_NUMBER_ID estén configurados
2. Verificar que los usuarios tengan:
   - `clientes: ["MEDICAL_CARE"]`
   - `celular` válido
   - `notificaciones_mc` configurado
3. Revisar logs del servidor

### Mensajes no llegan:
1. Verificar que el número esté formateado correctamente (57 + 10 dígitos)
2. Verificar que las plantillas existan en Meta Business Suite:
   - `retraso_operacion_fmc_`
   - `pacientes_sin_montar_fmc`
   - `confirmar_actualizacion`
3. Revisar logs de error de WhatsApp API
4. Verificar que WHATSAPP_API_TOKEN y WHATSAPP_PHONE_NUMBER_ID estén configurados

### Error 400 "Number of parameters does not match":
- Las plantillas oficiales requieren exactamente 2 parámetros (CEDI, total)
- No enviar un solo mensaje completo como parámetro

### Error 400 "Param text cannot have new-line/tab characters":
- Los mensajes para admin (`confirmar_actualizacion`) deben ser de una sola línea
- Usar ` | ` como separador en lugar de `\n`

## Archivos del Sistema

- **Lógica**: `integrappi/Funciones/sync_api_v3.py`
  - Función: `_notificar_sync_v3(resultado: dict)`
  - Función: `_obtener_estadisticas_por_regional(cruce_cache: dict, regional: str)`
- **Endpoints**: `integrappi/rutas/sync_v3.py`
- **Cruce**: `integrappi/rutas/pacientes_medical_care.py`
- **Utilidades WhatsApp**: `integrappi/Funciones/whatsapp_utils_integra.py`
