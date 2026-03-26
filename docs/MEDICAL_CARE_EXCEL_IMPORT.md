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
- `fx_normalizar_paciente()`: Primeras 4 palabras, elimina signos de puntuación
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
| paciente | Texto | Primeras 4 palabras, sin signos de puntuación |
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

## Próximas Mejoras

- [ ] Buscador en tabla de pacientes
- [ ] Exportar tabla a Excel
- [ ] Editar paciente individual
- [ ] Eliminar paciente individual
- [ ] Validación de cédulas duplicadas
- [ ] Historial de cargas por usuario
- [ ] Filtros por fecha de carga, sede, CEDI
- [ ] Paginación mejorada con controles de página
- [ ] Descarga de plantilla de Excel

## Soporte

Para reportar problemas o solicitar mejoras:
- Correo: edwin.zarate@integralogistica.com
- Teléfono: +57 312 544 3396