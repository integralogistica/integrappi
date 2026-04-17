# Portal Medical Care - Importación de Excel

## Descripción

Funcionalidad implementada para importar pacientes de Fresenius Medical Care desde archivos Excel en el portal `/MedicalCare`.

## Características

### Backend (FastAPI)

**Archivo:** `rutas/pacientes_medical_care.py`

**Endpoints:**

1. **POST** `/pacientes-medical-care/cargar-masivo`
   - Carga masiva de pacientes desde Excel
   - Valida columnas requeridas
   - Normaliza datos según reglas de Power Query
   - Retorna estadísticas de carga y errores si los hay

2. **GET** `/pacientes-medical-care/`
   - Lista de pacientes con paginación
   - Parámetros: `skip`, `limit`

3. **GET** `/pacientes-medical-care/buscar`
   - Búsqueda por cédula o nombre de paciente
   - Parámetros: `cedula`, `paciente`

4. **DELETE** `/pacientes-medical-care/eliminar-todos`
   - Elimina todos los pacientes (solo ADMIN)

### Normalización de Datos

**Archivo:** `Funciones/normalizacion_medical_care.py`

Funciones portadas desde Power Query (logs.txt):

- `fx_normalizar_base()`: Uppercase, trim, clean caracteres especiales
- `fx_normalizar_paciente()`: Primeras **6** palabras, elimina signos de puntuación, **máximo 2 ocurrencias por palabra**
  - Ejemplo: "DUVAN DUVAN DUVAN ESPITIA F FELIPE" → "DUVAN DUVAN ESPITIA F F FELIPE" (tercera "DUVAN" eliminada)
- `fx_normalizar_direccion()`: Normalización completa con corrección de errores comunes
- `fx_normalizar_celular()`: Solo dígitos, últimos 10 dígitos
- `fx_normalizar_municipio()`: Normalización básica
- `fx_normalizar_cedula()`: Solo dígitos

### Frontend (Next.js)

**Componentes creados:**

1. **FabMedicalCare** (`Componentes/PedidosComponentes/FabMedicalCare.tsx`)
   - Botón flotante en esquina inferior derecha
   - Muestra opciones al hacer click
   - Opción: Importar Excel

2. **ImportarExcelMedicalCare** (`Componentes/PedidosComponentes/ImportarExcelMedicalCare.tsx`)
   - Selector de archivo Excel
   - Valida archivo (.xlsx, .xls, .xlsm)
   - Muestra loading durante carga
   - Alertas con SweetAlert2
   - Solo visible para perfiles ADMIN y OPERATIVO

3. **TablaPacientesMedicalCare** (`Componentes/PedidosComponentes/TablaPacientesMedicalCare.tsx`)
   - Tabla con todos los pacientes importados
   - Paginación (50 registros por página)
   - Estados: cargando, error, vacío, datos
   - Columnas: cédula, paciente, dirección, municipio, departamento, celular, sede, CEDI, ruta, fecha carga

**Página actualizada:** `Paginas/MedicalCareP/index.tsx`
- Reemplazó placeholder "En construcción" con tabla de pacientes
- Integra FAB y tabla de pacientes
- Recarga automática después de importar exitoso

## Formato de Excel

### Columnas Requeridas

| Columna | Tipo | Descripción | Normalización |
|----------|------|-------------|----------------|
| sede | Texto | Mayúsculas, trim, compactar espacios |
| paciente | Texto | Primeras 6 palabras, sin signos de puntuación, máximo 2 ocurrencias por palabra |
| cedula | Texto/Número | Solo dígitos |
| direccion | Texto | Normalización completa con corrección de errores |
| departamento | Texto | Mayúsculas, trim, compactar espacios |
| municipio | Texto | Mayúsculas, trim, compactar espacios |
| ruta | Texto | Mayúsculas, trim, compactar espacios |
| cedi | Texto | Mayúsculas, trim, compactar espacios |
| celular | Texto/Número | Solo últimos 10 dígitos |

### Ejemplo de Registro

```csv
sede,paciente,cedula,direccion,departamento,municipio,ruta,cedi,celular
BOGOTA,JUAN PEREZ GARCIA,12345678,CALLE 123 #45,CUNDINAMARCA,BOGOTA,RUTA1,CEDI1,3101234567
```

## Flujo de Uso

1. **Usuario** inicia sesión en `/LoginUsuario`
2. **Selecciona** cliente "Fresenius Medical Care"
3. **Redirige** a `/MedicalCare`
4. **Click** en botón flotante (+) en esquina inferior derecha
5. **Selecciona** opción "Importar Excel"
6. **Selecciona** archivo Excel (.xlsx, .xls, .xlsm)
7. **Sistema** valida columnas y normaliza datos
8. **Muestra** alerta con resultados:
   - Éxito: cantidad de registros importados, tiempo de procesamiento
   - Advertencia: registros exitosos + errores (muestra primeros 10 errores)
9. **Tabla** se actualiza automáticamente con nuevos registros

## Reglas de Negocio

### Perfiles Autorizados
- **ADMIN**: Puede importar Excel
- **OPERATIVO**: Puede importar Excel
- **SEGURIDAD**: No puede importar
- **CONDUCTOR**: No puede importar

### Validaciones
- Columnas obligatorias: `paciente`, `cedula`
- Tipo de archivo: Solo Excel (.xlsx, .xls, .xlsm)
- Tamaño máximo: Determinado por configuración de FastAPI
- Errores por fila: Se registran pero no detienen la carga
- Máximos 50 errores mostrados en alerta (para no saturar UI)

### Campos Normalizados
Ejemplo de normalización de dirección:

**Input:**
```
CRA 12 #45, BARRIO SAN JOSE, AP 201
```

**Output:**
```
CARRERA 12 NUMERO 45 BARRIO SAN JOSE APARTAMENTO 201
```

## Tecnologías

### Backend
- **Python 3.x**
- **FastAPI** 0.115
- **pandas**: Procesamiento de Excel
- **openpyxl**: Lectura de .xlsx
- **xlrd**: Lectura de .xls
- **PyMongo**: MongoDB Atlas
- **Pydantic v2**: Validación de tipos

### Frontend
- **TypeScript 5**
- **Next.js 14** (App Router)
- **Axios**: HTTP client
- **SweetAlert2**: Alertas modales
- **React Icons**: FaFileExcel, FaPlus
- **CSS**: Diseño responsivo con navy/amber

## Colección MongoDB

**Nombre:** `pacientes_medical_care`

**Estructura de documento:**
```javascript
{
  "_id": ObjectId,
  "sede": String,
  "paciente": String,
  "cedula": String,
  "direccion": String,
  "departamento": String,
  "municipio": String,
  "ruta": String,
  "cedi": String,
  "celular": String,
  "usuario_carga": String,
  "fecha_carga": String
}
```

## Errores Comunes

### Archivo
- "El archivo debe ser un Excel (.xlsx, .xls, .xlsm)"
- "Faltan las siguientes columnas: ..."

### Normalización
- "Error al normalizar 'paciente'": Campo vacío o inválido
- "Error al normalizar 'cedula'": Sin dígitos válidos

### API
- Error 400: Archivo inválido
- Error 207: Carga parcial (algunos registros con errores)
- Error 500: Error interno del servidor

## Métricas

### Tiempo de Procesamiento
- Registra tiempo en segundos
- Se muestra en alerta de éxito
- Útil para optimización de rendimiento

### Estadísticas
- Registros exitosos: Cantidad importada correctamente
- Registros con errores: Cantidad fallida
- Total: Suma de ambos

## Seguridad

- Solo usuarios autenticados pueden importar
- Solo perfiles ADMIN y OPERATIVO tienen acceso
- Archivo se valida antes de procesar
- Errores no exponen información sensible del sistema

## Sistema de Cruce Pacientes ↔ V3

### Algoritmo de Cruce

**Archivo:** `rutas/pacientes_medical_care.py` (funciones `_calcular_cruce`, `ejecutar_cruce_automatico`, `recalcular_cruce`)

**Motor de similitud:** `rapidfuzz.fuzz.ratio` (extensión C++) — 20-50× más rápido que `difflib.SequenceMatcher`

### Criterios de Cruce (en orden de prioridad)

1. **👤 Nombre (prioridad máxima)**
   - Compara: `paciente` normalizado vs `cliente_destino` normalizado de todos los pedidos V3
   - Umbral: similitud ≥ 95%
   - Si hay match: `match_tipo = 'nombre'`, `en_v3 = True`
   - Si hay match: NO se evalúan los demás criterios (prioridad máxima)
   - Score: guarda el porcentaje de similitud en `similitud`

2. **🔑 Llave (segunda prioridad)**
   - Solo se ejecuta si NO hubo cruce por nombre
   - Compara: `llave` del paciente (paciente+dirección) vs `llave` de todos los pedidos V3
   - Umbral: similitud ≥ 73%
   - Si hay match: `match_tipo = 'llave'`, `en_v3 = True`
   - Si hay match: NO se evalúa celular (segunda prioridad)
   - Score: guarda el porcentaje de similitud en `similitud`

3. **📱 Celular (tercera prioridad, fallback)**
   - Solo se ejecuta si NO hubo cruce por nombre ni llave
   - Compara: `telefono1` y `telefono2` del paciente vs `telefono_original` del pedido V3
   - Normalización: elimina caracteres no numéricos (SIN truncar a 10 dígitos)
   - Umbral: coincidencia exacta
   - Si hay match: `match_tipo = 'celular'`, `en_v3 = True`
   - Score: `similitud` mantiene el valor del cálculo de llave (no 100%)

4. **Sin cruce**
   - Si no se cumple ninguno de los criterios anteriores
   - `match_tipo = None`
   - `en_v3 = False`
   - `similitud` = mejor score encontrado (aunque sea < umbrales)

### Badges Visuales (Frontend)

**Archivo:** `integrapp-next/src/Paginas/CrucePacientesV3P/index.tsx`

| Emoji | Tipo | Color | Condición |
|-------|------|-------|-----------|
| 👤 | Nombre | Verde | Similitud nombre ≥ 95% |
| 🔑 | Llave | Morado | Similitud llave ≥ 73% |
| 📱 | Celular | Azul | Celular exacto |
| — | Sin badge | — | Sin cruce |

### Endpoints del Cruce

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/pacientes-medical-care/ocupacion-rutas` | Lee cruce desde cache MongoDB |
| GET | `/pacientes-medical-care/v3-sin-paciente` | Lee V3 sin paciente desde cache |
| POST | `/pacientes-medical-care/recalcular-cruce?usuario=USUARIO` | Recalcula cruce completo (SSE streaming) |
| GET | `/pacientes-medical-care/exportar-cruce-excel?cedi=FUNZA` | Exporta cruce a Excel (filtro regional opcional) |

### Cache en MongoDB

**Colección:** `cache_cruce_mc`
**Documento único:** `{ tipo: "cruce_completo" }`
**Campos:**
- `ocupacion_rutas`: array con pacientes agrupados por ruta
- `v3_sin_paciente`: array con pedidos V3 sin paciente coincidente
- `total_sin_paciente`: total de pedidos V3 sin paciente
- `fecha_calculo`: timestamp del último cálculo
- `calculado_por`: usuario que realizó el recálculo

### Progreso SSE (Server-Sent Events)

El endpoint `POST /recalcular-cruce` envía eventos en tiempo real:

| Etapa | Porcentaje | Mensaje |
|-------|-----------|---------|
| loading | 0-8% | Cargando pacientes y pedidos V3... |
| comparing_patients | 10-60% | Comparando paciente X de Y... |
| comparing_v3 | 62-90% | Verificando V3 X de Y... |
| saving | 95% | Guardando resultados... |
| complete | 100% | Cruce completado |

### Histórico Mensual

**Generación automática:** El último día de cada mes a las 00:00, se genera automáticamente un corte histórico del cruce.

**Endpoint:** `GET /pacientes-medical-care/historico-meses` - Lista los meses disponibles
**Endpoint:** `GET /pacientes-medical-care/historico-mes?anio=2026&mes=4` - Obtiene el corte de un mes específico

## Próximas Mejoras

- [x] Buscador en tabla de pacientes
- [x] Exportar tabla a Excel
- [x] Editar paciente individual
- [x] Eliminar paciente individual
- [x] Validación de cédulas duplicadas
- [x] Historial de cargas por usuario
- [x] Filtros por fecha de carga, sede, CEDI
- [x] Paginación mejorada con controles de página
- [x] Descarga de plantilla de Excel
- [x] Sistema de cruces con múltiples criterios
- [x] Badges visuales con emojis
- [x] Histórico mensual automático

## Soporte

Para reportar problemas o solicitar mejoras:
- Correo: edwin.zarate@integralogistica.com
- Teléfono: +57 312 544 3396