# Integración SICE-TAC / RNDC

## 1. Objetivo

Este módulo consulta los costos eficientes de transporte publicados por SICE-TAC mediante el web service SOAP del Registro Nacional de Despachos de Carga (RNDC). Reemplaza el scraping con Selenium como mecanismo principal y no depende de navegador, CAPTCHA, Excel ni de `Plantilla BOT - ejemplo.xlsx`.

La integración permite:

- Verificar manualmente una combinación de origen, destino y vehículo.
- Ejecutar periódicamente las combinaciones definidas en el código.
- Consultar el mes solicitado y, si no hay coincidencias, solamente el mes anterior.
- Guardar los resultados en MongoDB mediante operaciones idempotentes.
- Consultar posteriormente la información almacenada.

## 2. Arquitectura

Los componentes principales son:

| Archivo | Responsabilidad |
|---|---|
| `sicetac/config.py` | Combinaciones, horas logísticas, ambientes y configuración. |
| `sicetac/rndc_client.py` | Construcción del XML, envoltura SOAP, transporte, reintentos e interpretación de RNDC. |
| `sicetac/service.py` | Consulta de combinaciones, filtrado, retroceso de período, transformación y cálculos. |
| `sicetac/repository.py` | Validación de MongoDB, índice único, `upsert` y lectura. |
| `sicetac/models.py` | Validación de los cuerpos JSON y cálculos decimales. |
| `sicetac/errors.py` | Excepciones seguras y clasificadas. |
| `rutas/sicetac.py` | Endpoints FastAPI, autenticación, trabajos y respuestas JSON. |
| `tests/test_sicetac.py` | Pruebas unitarias del XML, dominio, reintentos y servicio. |

El flujo general es:

```text
Cliente/Swagger
      |
      v
FastAPI + autenticación de baseusuarios
      |
      v
Servicio SICE-TAC
      |
      +----> Web service SOAP RNDC
      |
      +----> MongoDB (solo en ejecuciones con dryRun=false)
```

## 3. Contrato RNDC utilizado

La solicitud de negocio usa:

```xml
<solicitud>
  <tipo>6</tipo>
  <procesoid>26</procesoid>
</solicitud>
```

Los filtros principales son:

- `PERIODO`: formato `AAAAMM`, por ejemplo `202608`.
- `CONFIGURACIONESID`: configuración vehicular, por ejemplo `3S3`.
- `ORIGEN`: código DIVIPOLA.
- `DESTINO`: código DIVIPOLA.
- `CONDICIONCARGAID`: `1` para cargado y `2` para vacío.

El endpoint operativo verificado el 24 de agosto de 2026 es:

```text
http://rndcws.mintransporte.gov.co:8080/ws/svr008w.dll/soap/IBPMServices
```

El servidor PLC devuelve `RNDC33` para el proceso 26. La ruta corta de `rndcws` también presentó respuestas `RNDC13`; por eso se usa la ruta histórica completa.

> Advertencia de seguridad: el endpoint oficial disponible usa HTTP sin TLS. Las credenciales viajan sin cifrado de transporte. Nunca deben aparecer en logs, respuestas, documentación ni repositorios.

## 4. Variables de entorno

La aplicación lee las siguientes variables:

```dotenv
RNDC_USERNAME=
RNDC_PASSWORD=
MONGO_URI=
MONGODB_DATABASE=sicetac
MONGODB_COLLECTION=consultas
RNDC_ENVIRONMENT=production
RNDC_SOAP_URL=
```

Notas:

- `RNDC_USERNAME`, `RNDC_PASSWORD` y `MONGO_URI` son obligatorias.
- Se reutiliza `MONGO_URI`; no se necesita otra variable con la misma conexión.
- `RNDC_SOAP_URL` puede quedar vacía en producción para utilizar el endpoint definido en `config.py`.
- `RNDC_ENVIRONMENT` admite `production` o `test`.
- En ambiente `test` debe configurarse una URL SOAP confirmada por RNDC.
- El archivo `.env` no debe subirse al repositorio.

Después de cambiar el `.env` o el código es necesario reiniciar el backend.

## 5. Autenticación y permisos

Todos los endpoints SICE-TAC están protegidos con el esquema OAuth2/JWT `BaseUsuariosOAuth2`.

En Swagger se utiliza el mismo usuario y contraseña del inicio de sesión del frontend. El token se obtiene mediante:

```http
POST /baseusuarios/token
```

El usuario debe estar activo y tener uno de estos perfiles:

```text
ADMIN
ADMINISTRADOR
```

En `http://127.0.0.1:8000/docs#/` se debe pulsar **Authorize**, ingresar las credenciales y luego ejecutar los endpoints de la sección **SICE-TAC**.

## 6. Explorar o verificar una ruta

### Endpoint

```http
POST /sicetac/rutas-disponibles
Content-Type: application/json
```

Este endpoint consulta RNDC y devuelve los registros encontrados, pero **no escribe en MongoDB**.

Ejemplo comprobado para Bogotá → Medellín:

```json
{
  "periodo": "202608",
  "configuracion": "3S3",
  "origen": "11001000",
  "destino": "05001000",
  "condicion_carga": "1",
  "unidad_transporte_nombre": "FURGON",
  "tipo_carga_nombre": "General",
  "horas_totales_cargue": 3,
  "horas_totales_descargue": 3,
  "limit": 200
}
```

Campos:

| Campo | Descripción |
|---|---|
| `periodo` | Mes que se desea verificar, en formato `AAAAMM`. |
| `configuracion` | Código de configuración vehicular. |
| `origen` | Código DIVIPOLA de ocho dígitos. |
| `destino` | Código DIVIPOLA de ocho dígitos. |
| `condicion_carga` | `1` cargado o `2` vacío. Por defecto es `1`. |
| `unidad_transporte_nombre` | Filtro opcional exacto, sin distinguir mayúsculas ni tildes; por ejemplo `FURGON`. |
| `tipo_carga_nombre` | Filtro opcional exacto, sin distinguir mayúsculas ni tildes; por ejemplo `General`. |
| `horas_totales_cargue` | Espera en origen más cargue. Mínimo 1; por defecto 3. |
| `horas_totales_descargue` | Espera en destino más descargue. Mínimo 1; por defecto 3. |
| `limit` | Máximo de rutas mostradas, entre 1 y 1000. Por defecto es 200. |

`limit` no modifica lo que RNDC busca. Solo limita el tamaño de la respuesta de nuestra API. Si RNDC devuelve 26 documentos, tanto `limit: 100` como `limit: 200` mostrarán los 26.

Los filtros `unidad_transporte_nombre` y `tipo_carga_nombre` se aplican localmente después de recibir la respuesta RNDC y antes de aplicar `limit`. La comparación es exacta pero ignora mayúsculas, espacios repetidos y tildes; `FURGON`, `furgon` y `Furgón` se consideran equivalentes. Por eso `documentos_recibidos` puede ser mayor que `rutas_unicas` y `rutas_mostradas`.

Las configuraciones aceptadas son:

```text
2, 3, 2L1, 2L2, 2L3, 2S2, 2S3, 3S2, 3S3, V2, V3, V4
```

Para las categorías livianas la API conserva los aliases publicados, pero los traduce a los IDs que realmente almacena RNDC: `2L1` → `2_9_105`, `2L2` → `2_8_9` y `2L3` → `2_7_8`. Esta traducción es necesaria porque el proceso 26 compara literalmente `CONFIGURACIONESID` y no realiza por sí mismo la conversión anunciada en la guía.

| Opción del portal | Código que recibe nuestra API |
|---|---|
| Camión dos ejes - PBV más de 10.500 kg | `2` |
| Camión dos ejes - Livianos PBV 7.500–8.000 kg | `2L3` |
| Camión dos ejes - Livianos PBV 8.001–9.000 kg | `2L2` |
| Camión dos ejes - Livianos PBV 9.001–10.500 kg | `2L1` |
| Tractocamión dos ejes con semirremolque de dos ejes | `2S2` |
| Tractocamión dos ejes con semirremolque de tres ejes | `2S3` |
| Camión tres ejes | `3` |
| Tractocamión tres ejes con semirremolque de dos ejes | `3S2` |
| Tractocamión tres ejes con semirremolque de tres ejes | `3S3` |
| Volqueta dos ejes | `V2` |
| Volqueta tres ejes | `V3` |
| Volqueta cuatro ejes | `V4` |

El consumidor siempre debe enviar el código público (`2L3`), no el ID interno (`2_7_8`).

Ejemplo resumido de respuesta:

```json
{
  "periodo": "202608",
  "configuracion": "3S3",
  "origen_consultado": "11001000",
  "destino_consultado": "05001000",
  "condicion_carga_consultada": "1",
  "documentos_recibidos": 26,
  "rutas_unicas": 26,
  "rutas_mostradas": 26,
  "rutas": [
    {
      "origen_codigo": "11001000",
      "origen_nombre": "BOGOTA BOGOTA D. C.",
      "destino_codigo": "05001000",
      "destino_nombre": "MEDELLIN ANTIOQUIA",
      "configuracion": "3S3",
      "condicion_carga": "CARGADO",
      "tipo_carga_nombre": "General",
      "unidad_transporte_nombre": "FURGON",
      "ruta_id": "106",
      "kilometros": "429",
      "horas_recorrido": "11.43",
      "valor_moviliza": "3994879",
      "valor_hora": "107530",
      "horas_totales_cargue": "3",
      "horas_totales_descargue": "3",
      "horas_logisticas_total": "6",
      "costo_total_calculado": "4640059"
    }
  ]
}
```

Una pareja origen-destino puede devolver muchos registros porque RNDC publica distintas vías, tipos de carga y unidades de transporte. No son duplicados: cada combinación puede tener valores diferentes.

Aunque la guía describe algunos filtros como opcionales, el servidor actual rechaza con `RNDC13` los intentos de enumerar todos los orígenes o destinos. Por eso este endpoint verifica una pareja concreta; no es un catálogo completo de municipios.

## 7. Configurar las rutas que se guardarán

Las rutas automáticas se editan en la constante `COMBINACIONES` de `sicetac/config.py`.

Ejemplo:

```python
{
    "origen": "BOGOTÁ",
    "origen_codigo": "11001000",
    "destino": "MEDELLÍN",
    "destino_codigo": "05001000",
    "configuracion": "Tractocamión tres ejes con semirremolque de tres ejes",
    "configuracion_codigo": "3S3",
    "unidad_transporte": "FURGON",
    "tipo_carga": "General",
    "condicion_carga": "CARGADO",
    "condicion_carga_codigo": "1"
}
```

Reglas:

- Los códigos de origen y destino deben contener ocho dígitos.
- La configuración debe pertenecer al catálogo permitido.
- La condición debe ser `1` o `2`.
- No se permiten combinaciones idénticas duplicadas.
- `tipo_carga` y `unidad_transporte` deben coincidir con los nombres devueltos por RNDC.

RNDC puede devolver un DIVIPOLA sin el cero inicial, por ejemplo Medellín como `5001000`. El servicio lo normaliza a `05001000` antes de comparar y responder.

## 8. Ejecutar y guardar las combinaciones

### Crear una ejecución

```http
POST /sicetac/consultas
```

Para consultar y guardar:

```json
{
  "periodo": "202608",
  "dryRun": false,
  "horas_totales_cargue": 3,
  "horas_totales_descargue": 3
}
```

Para probar sin escribir:

```json
{
  "periodo": "202608",
  "dryRun": true,
  "horas_totales_cargue": 3,
  "horas_totales_descargue": 3
}
```

La respuesta inicial tiene estado HTTP `202` e incluye un identificador:

```json
{
  "ejecucion_id": "470b387757664907830743dd37dc83f8",
  "estado": "pendiente",
  "progreso": 0,
  "dry_run": false
}
```

### Consultar el estado

```http
GET /sicetac/consultas/{ejecucion_id}
```

Estados posibles:

```text
pendiente
ejecutando
completada
fallida
```

Campos del resumen:

| Campo | Significado |
|---|---|
| `combinaciones_configuradas` | Cantidad de entradas en `COMBINACIONES`. |
| `consultas_exitosas` | Combinaciones procesadas sin error técnico. |
| `documentos_recibidos` | Documentos recibidos desde RNDC antes del filtrado local. |
| `documentos_insertados` | Documentos nuevos creados en MongoDB. |
| `documentos_actualizados` | Documentos existentes actualizados. |
| `combinaciones_sin_resultado` | Combinaciones válidas sin coincidencias. |
| `errores` | Errores clasificados por combinación. |

Una ejecución `completada` con cero documentos significa que la comunicación terminó correctamente, pero ninguna combinación coincidió. No significa que haya datos guardados.

Las ejecuciones creadas por `POST /sicetac/consultas` se mantienen en memoria. Su estado y `ejecucion_id` se pierden al reiniciar el backend. Solo se admite una ejecución simultánea por proceso; una segunda recibe HTTP `409`. Esto no aplica a los trabajos Excel asíncronos descritos más adelante, cuyo progreso sí se persiste en MongoDB.

## 9. Consultar resultados guardados

```http
GET /sicetac/resultados?periodo=202608&limit=100
```

Parámetros:

- `periodo` es opcional y filtra por `periodo_aplicado`.
- `limit` acepta de 1 a 1000 y por defecto es 100.

Si la ejecución se hizo con `dryRun: true`, no aparecerá información nueva en este endpoint.

## 9.1. Consultas masivas mediante Excel

La carga masiva es de solo lectura: consulta RNDC y devuelve otro Excel, pero no guarda resultados en MongoDB.

### Descargar la plantilla

```http
GET /sicetac/plantilla-excel
```

Descarga `plantilla_sicetac.xlsx` con las columnas admitidas y dos filas de ejemplo. Las columnas son:

```text
consulta_id_usuario
fila_original
periodo
configuracion
origen
destino
condicion_carga
unidad_transporte_nombre
tipo_carga_nombre
horas_totales_cargue
horas_totales_descargue
limit
```

Son obligatorias `periodo`, `configuracion`, `origen` y `destino`. `consulta_id_usuario` y `fila_original` son opcionales, pero se recomiendan para conservar trazabilidad al dividir archivos en lotes. Las demás columnas pueden quedar vacías. En la carga Excel, `limit` utiliza por defecto `20`; el resto conserva los valores predeterminados del endpoint JSON.

### Procesar el archivo

```http
POST /sicetac/consultas-excel
Content-Type: multipart/form-data
```

En Swagger se selecciona el archivo en el campo `archivo`. Solo se admiten archivos `.xlsx`, con un máximo de 5 MB y 200 filas de datos. La respuesta descarga `resultados_sicetac.xlsx`.

Una fila de entrada puede producir varias filas de salida, porque RNDC puede devolver distintas vías para la misma combinación. La columna `fila_entrada` permite rastrear cada resultado hasta la fila original. La columna `estado` puede contener:

```text
OK
SIN_RESULTADO
ERROR_VALIDACION
ERROR_RNDC
ERROR
```

Las filas inválidas no detienen todo el archivo; aparecen en el Excel de salida con su mensaje. Los códigos DIVIPOLA numéricos se normalizan a ocho dígitos para recuperar ceros iniciales perdidos por Excel.

La plantilla usa `limit = 20`, por lo que produce como máximo veinte filas de resultado por consulta. Si la celda `limit` queda vacía, también se aplica `20`. Las alternativas conservan el orden entregado por RNDC; no aparecen necesariamente ordenadas de la más barata, corta o rápida.

Las columnas de salida incluyen los datos originales necesarios, nombres reconocidos por RNDC, ruta, vía, kilómetros, horas de recorrido, valor de movilización, valor hora, horas logísticas y costo total calculado. `consulta_id_usuario` y `fila_original` se aceptan por compatibilidad en archivos de entrada, pero no se incluyen en el consolidado descargado; `fila_entrada` conserva la trazabilidad con la fila del Excel cargado.

Las columnas `kilometros`, `horas_recorrido`, `valor_moviliza`, `valor_hora`, `horas_logisticas_total` y `costo_total_calculado` se escriben como celdas numéricas reales. Los valores monetarios llevan formato de moneda y las distancias y horas admiten decimales, por lo que pueden sumarse, filtrarse y utilizarse en fórmulas de Excel.

### Trabajos asíncronos para archivos grandes

Para archivos de hasta 2.000 consultas se debe usar el flujo de trabajos. El usuario sube un solo Excel; la API lo divide lógicamente en bloques de 30, sin crear archivos intermedios visibles.

| Modalidad | Endpoint | Límite | Persistencia del progreso | Uso recomendado |
|---|---|---:|---|---|
| Excel síncrono | `POST /sicetac/consultas-excel` | 200 filas / 5 MB | No | Pruebas y archivos pequeños. |
| Excel asíncrono | `POST /sicetac/consultas-excel/jobs` | 2.000 filas / 20 MB | Sí, en MongoDB | Cargas grandes y procesos de varios minutos. |

El tamaño de lote `30` es interno: controla el reporte de avance y organiza el trabajo, pero no obliga al usuario a dividir el archivo. RNDC se consulta secuencialmente y conserva la pausa mínima entre solicitudes.

#### Crear el trabajo

```http
POST /sicetac/consultas-excel/jobs
Content-Type: multipart/form-data
```

El campo `archivo` admite `.xlsx` de hasta 20 MB. La respuesta HTTP `202` contiene:

```json
{
  "ejecucion_id": "abc123",
  "estado": "pendiente",
  "filas_totales": 1632,
  "filas_procesadas": 0,
  "tamano_lote": 30,
  "lotes_totales": 55,
  "progreso_porcentaje": 0
}
```

`ejecucion_id` se utiliza en los endpoints siguientes. La respuesta también conserva `job_id` como identificador interno equivalente.

#### Consultar el progreso

```http
GET /sicetac/consultas-excel/jobs/{ejecucion_id}
```

Para listar las diez ejecuciones Excel más recientes almacenadas en MongoDB:

```http
GET /sicetac/consultas-excel/jobs?limit=10
Authorization: Bearer <token_admin>
```

`limit` acepta valores entre 1 y 50. La respuesta permite recuperar un trabajo
aunque el navegador haya perdido su `localStorage`; cada elemento contiene el
`ejecucion_id`, estado, archivo, usuario creador, contadores y porcentaje.

La respuesta informa `filas_exitosas`, `filas_sin_resultado`, `filas_con_error`, `resultados_generados`, `lote_actual`, `lotes_totales` y `progreso_porcentaje`. Los estados del trabajo son:

```text
pendiente
ejecutando
completada
fallida
```

Ejemplo durante la ejecución:

```json
{
  "ejecucion_id": "abc123",
  "estado": "ejecutando",
  "filas_totales": 1632,
  "filas_procesadas": 450,
  "filas_exitosas": 445,
  "filas_sin_resultado": 3,
  "filas_con_error": 2,
  "resultados_generados": 510,
  "tamano_lote": 30,
  "lote_actual": 15,
  "lotes_totales": 55,
  "progreso_porcentaje": 27.57
}
```

#### Descargar el consolidado

```http
GET /sicetac/consultas-excel/jobs/{ejecucion_id}/resultado
```

Solo está disponible cuando el trabajo está `completada`; antes responde HTTP `409`. El Excel se construye desde los resultados persistidos en MongoDB y conserva los tipos numéricos y formatos de moneda.

El trabajo guarda sus metadatos en la colección `<MONGODB_COLLECTION>_excel_jobs` y cada fila en `<MONGODB_COLLECTION>_excel_job_rows`. Esto evita el límite de 16 MB por documento y permite que una consulta produzca varias rutas. En un reinicio normal del backend, los trabajos que estaban ejecutándose vuelven a estado pendiente y continúan desde las filas todavía no procesadas.

Los errores transitorios de transporte, XML o SOAP se reintentan por fila. Los errores de validación y las combinaciones sin coincidencia se conservan en el consolidado y no detienen las demás consultas. Solo debe desplegarse un worker del backend para este procesador mientras se utilice el mecanismo actual de recuperación.

#### Procedimiento en Swagger

1. Reiniciar el backend después de desplegar cambios.
2. Abrir `/docs#/` y autorizarse con un administrador.
3. Ejecutar `POST /sicetac/consultas-excel/jobs` y seleccionar el Excel completo.
4. Copiar `ejecucion_id` de la respuesta `202`.
5. Consultar periódicamente `GET /sicetac/consultas-excel/jobs/{ejecucion_id}`.
6. Esperar hasta que `estado` sea `completada`.
7. Ejecutar `GET /sicetac/consultas-excel/jobs/{ejecucion_id}/resultado`.
8. Descargar y conservar el consolidado.

No es necesario mantener abierta la solicitud de carga mientras RNDC procesa las filas. El trabajo continúa en segundo plano. Con 1.632 consultas y una pausa mínima de un segundo, la duración base supera 27 minutos y puede aumentar por los reintentos. `limit = 20` permite hasta veinte rutas por consulta, por lo que el número de filas del consolidado puede ser mayor que el número de filas de entrada.

La implementación fue validada integralmente creando un trabajo real, procesando sus filas, consultando el estado, generando el Excel y retirando posteriormente solo los documentos identificados como prueba.

#### Pantalla web

El frontend `integrapp-next` expone la ruta:

```text
/Sicetac
```

Solo permite acceso a perfiles `ADMIN` y `ADMINISTRADOR`, y aparece como opción **SICE-TAC** en el selector de portales para esos perfiles. La pantalla permite descargar la plantilla, seleccionar o arrastrar un `.xlsx`, crear el trabajo, observar el avance, revisar los contadores y descargar el consolidado.

El navegador consulta el estado cada cuatro segundos mientras el trabajo está `pendiente` o `ejecutando`. `ejecucion_id` se conserva en `localStorage` bajo `sicetacJobId`, por lo que recargar o volver a la pantalla recupera el trabajo activo. La sección **Ejecuciones recientes** consulta MongoDB mediante la API y permite abrir cualquiera de los últimos trabajos con un clic, incluso si se borró el almacenamiento local. El Bearer JWT entregado por `/baseusuarios/login` se conserva como `baseUsuarioAccessToken`; si vence o RNDC responde `401`, la pantalla limpia la sesión y redirige a `/LoginUsuario`.

La descarga usa el encabezado `Content-Disposition` del backend para conservar el nombre del archivo. El frontend valida antes de subir que el archivo termine en `.xlsx` y no exceda 20 MB.

## 10. Períodos examinados

`MESES_RETROCESO_PERIODO = 1` significa que se consulta:

1. El mes solicitado.
2. Solamente el mes inmediatamente anterior si no hay coincidencias.

Ejemplo para `202608`:

```text
202608 -> agosto de 2026
202607 -> julio de 2026
```

No se retrocede a junio ni a meses anteriores. El documento guardado diferencia:

- `periodo_solicitado`: el período pedido por el usuario.
- `periodo_aplicado`: el período del registro que efectivamente entregó RNDC.

## 11. Cálculo del costo

La fórmula utilizada es:

```text
costo total = valor movilización + (valor hora × horas logísticas)
```

Actualmente `config.py` define:

```text
HORAS_TOTALES_CARGUE_DEFAULT = 3
HORAS_TOTALES_DESCARGUE_DEFAULT = 3
HORAS LOGÍSTICAS TOTALES = 3 + 3 = 6
```

Los valores predeterminados solo se usan cuando el JSON no los especifica. Cada campo ya incluye su espera correspondiente; no existe una tercera variable independiente de espera.

Las horas no forman parte de los filtros SOAP del proceso 26. RNDC entrega `valor_moviliza` y `valor_hora`; nuestra API aplica las horas proporcionadas en el JSON para calcular el total de la misma forma conceptual que el formulario del portal.

Ejemplo Bogotá → Medellín, ruta 106, carga general y furgón:

```text
valor movilización = 3.994.879
valor hora          =   107.530
horas logísticas  =         6
costo total         = 4.640.059
```

Todos los cálculos usan `Decimal`; en MongoDB se almacenan como `Decimal128`, nunca como `float`.

## 12. Persistencia en MongoDB

La base y colección se controlan mediante:

```text
MONGODB_DATABASE
MONGODB_COLLECTION
```

Antes de ejecutar se comprueba la conexión y se crea el índice único:

```text
uq_sicetac_consulta_id
```

`consulta_id` es un SHA-256 determinístico que considera período aplicado, origen, destino, configuración, condición, tipo de carga, unidad y ruta RNDC. Esto permite escrituras idempotentes:

- Si el registro no existe, se inserta y se establece `creado_en`.
- Si ya existe, se actualizan sus datos y `actualizado_en`.
- Ejecutar de nuevo la misma consulta no crea duplicados.

Cada documento conserva la respuesta original de negocio en `respuesta_rndc`, además de una estructura normalizada para uso del backend y frontend.

La sección `costos` guardada tiene esta forma:

```json
{
  "valor_moviliza": "3994879",
  "valor_hora": "107530",
  "horas_totales_cargue": "3",
  "horas_totales_descargue": "3",
  "horas_logisticas_total": "6",
  "costo_total_calculado": "4640059"
}
```

En MongoDB esos importes y horas son `Decimal128`; aquí se representan como texto solamente para que el ejemplo JSON no pierda precisión.

### Persistencia de los trabajos Excel y su historial

Además de la colección principal de resultados, los trabajos asíncronos utilizan
dos colecciones derivadas del valor de `MONGODB_COLLECTION`:

```text
<MONGODB_COLLECTION>_excel_jobs
<MONGODB_COLLECTION>_excel_job_rows
```

Con la configuración actual (`MONGODB_DATABASE=integra` y
`MONGODB_COLLECTION=consultas_sicetac`) corresponden a:

```text
Base de datos: integra
Colección de trabajos: consultas_sicetac_excel_jobs
Colección de filas:     consultas_sicetac_excel_job_rows
```

`consultas_sicetac_excel_jobs` contiene un documento por archivo cargado: ID,
nombre del archivo, usuario creador, estado, fechas, totales y avance.
`consultas_sicetac_excel_job_rows` contiene un documento por fila de entrada,
incluidos el payload normalizado, estado, intentos y rutas obtenidas. Por ejemplo,
un archivo de 828 consultas crea un trabajo y 828 documentos de detalle.

Actualmente no existe vencimiento, índice TTL ni eliminación automática: tanto
los trabajos como sus filas se conservan indefinidamente hasta que se implemente
una política de retención o se eliminen explícitamente. Tampoco existe un máximo
global de históricos. El límite de 2.000 aplica a las filas de cada archivo, no
al número acumulado de ejecuciones.

La pantalla muestra los 10 trabajos más recientes. El endpoint de historial
acepta `limit` entre 1 y 50; este límite solo controla cuántos trabajos devuelve
la consulta y no elimina los anteriores. Una política futura posible es conservar
los encabezados indefinidamente y aplicar TTL solamente al detalle después de un
periodo definido, por ejemplo 90 días.

## 13. Reintentos y comportamiento observado de RNDC

El cliente utiliza:

- Timeout de conexión de 10 segundos.
- Timeout total de 45 segundos.
- Separación mínima de un segundo entre solicitudes del mismo cliente.
- Hasta tres intentos para errores de red y HTTP 5xx.
- Reconexión y reintento para `RNDC13`.

Se comprobó que distintos nodos internos de `rndcws` responden de forma inconsistente al mismo XML: uno puede devolver documentos y otro `RNDC13`. Por eso, al recibir `RNDC13`, el cliente cierra la conexión y abre otra antes de decidir que la combinación no existe. La reconexión conserva además el intervalo mínimo de un segundo respecto de la solicitud anterior; esto evita rechazos consecutivos al procesar varias filas de Excel.

Para una consulta exacta que finalmente no entrega datos, el servicio intenta una consulta más amplia por período, configuración y origen, y aplica localmente los demás filtros. Estas respuestas se reutilizan entre combinaciones equivalentes. El servidor puede rechazar también la consulta amplia; en ese caso se trata como ausencia de coincidencias y se prueba el mes anterior.

## 14. Errores frecuentes

| Error o comportamiento | Significado y acción |
|---|---|
| `Unauthorized` en Swagger | Autorizarse con `/baseusuarios/token`, usando un usuario activo de `baseusuarios`. |
| HTTP `403` | El usuario no tiene perfil `ADMIN` o `ADMINISTRADOR`. |
| HTTP `409` | Ya existe una ejecución SICE-TAC activa. |
| `RNDC13` | XML/filtros no aceptados o nodo RNDC inconsistente. El cliente lo reintenta automáticamente. |
| `RNDC33` | El proceso 26 fue enviado a una URL que no lo permite. Revisar `RNDC_SOAP_URL`. |
| `Atributo duplicado` | Envoltura SOAP con namespace `xsi` repetido. El constructor actual ya evita esta duplicación. |
| `Documento no encontrado` / `RNDC11` | No existen registros para los filtros. Se clasifica como ausencia de datos. |
| `Todas las combinaciones SICE-TAC fallaron` | Ninguna consulta terminó técnicamente bien; revisar el arreglo `errores`. |
| Ejecución completada con cero documentos | RNDC respondió, pero no hubo coincidencias para el mes solicitado ni el anterior. |
| Textos como `SÃ³lido` | Problema de codificación de caracteres de la respuesta RNDC; los códigos y valores numéricos no se alteran. |

## 15. Pruebas

Desde la carpeta `integrappi`:

```powershell
python -m unittest tests.test_sicetac -v
```

O para descubrir todas las pruebas relacionadas:

```powershell
python -m unittest discover -s tests -p "test_sicetac*.py"
```

Las pruebas unitarias utilizan XML simulado, transporte HTTP simulado y repositorios falsos. No requieren llamar al RNDC ni escribir en MongoDB.

Para una prueba operativa manual:

1. Reiniciar el backend.
2. Autorizarse en Swagger.
3. Ejecutar `POST /sicetac/rutas-disponibles` con Bogotá → Medellín.
4. Confirmar que devuelve registros.
5. Agregar la combinación deseada a `COMBINACIONES`.
6. Ejecutar `POST /sicetac/consultas` primero con `dryRun: true`.
7. Revisar el resumen.
8. Ejecutar con `dryRun: false`.
9. Verificar los documentos mediante `GET /sicetac/resultados`.

## 16. Limitaciones actuales

- RNDC no permite descubrir confiablemente todos los orígenes o destinos con filtros omitidos; las parejas se verifican individualmente.
- Los estados de `POST /sicetac/consultas` siguen siendo locales y se pierden al reiniciar; los trabajos de `/consultas-excel/jobs` sí son durables en MongoDB.
- El bloqueo RNDC es local al proceso. El procesador Excel durable debe ejecutarse con un solo worker del backend; un despliegue con varios workers necesita un bloqueo distribuido o una cola externa.
- La comunicación oficial observada usa HTTP sin TLS.
- Los textos RNDC pueden llegar con problemas de codificación de tildes.
- Las equivalencias con el portal deben validarse periódicamente porque los catálogos y valores son administrados por RNDC.

## 17. Buenas prácticas

- Probar una combinación con el explorador antes de agregarla al proceso automático.
- Conservar siempre los ceros iniciales en los códigos DIVIPOLA configurados.
- Usar `dryRun: true` después de cambiar combinaciones.
- No registrar el XML de acceso sin sanitizarlo.
- No compartir ni versionar credenciales.
- Rotar inmediatamente cualquier clave que haya sido expuesta en texto, capturas o conversaciones.
- Comparar periódicamente una muestra contra el portal oficial SICE-TAC.
