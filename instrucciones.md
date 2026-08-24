# Instrucciones para integrar SICE-TAC en el backend existente

## 1. Objetivo

Construye dentro del backend existente un módulo que consulte los valores de referencia de SICE-TAC mediante el web service SOAP oficial del RNDC, exponga la operación mediante la API actual y guarde los resultados en MongoDB.

No impongas Python ni crees una aplicación independiente. Primero inspecciona el repositorio donde estás trabajando e identifica su lenguaje, framework, arquitectura, autenticación, configuración, acceso a MongoDB, logging, pruebas y manejo de errores. Implementa la funcionalidad con esas mismas tecnologías y convenciones.

La nueva herramienta debe reemplazar la automatización con Selenium como mecanismo principal. No debe abrir Chrome, interactuar con el formulario web ni resolver CAPTCHA.

No debe leer ni depender del archivo `Plantilla BOT - ejemplo.xlsx`. Las combinaciones deben estar declaradas en una constante o módulo de configuración del backend, fácil de editar. Inicialmente, debe contener las mismas combinaciones que aparecen en la hoja `Sheet1` de la plantilla.

Conserva `sicetac.py` sin modificar mientras se desarrolla y valida la nueva solución. Créala en archivos nuevos.

## 2. Documentación disponible

Antes de implementar, revisa completamente estos documentos locales:

- `GUIA Uso del Web Service en el RNDC V5.pdf`: arquitectura SOAP y servidores vigentes, aprobada el 27 de mayo de 2026.
- `GUIA CONSULTA SICETAC- WEB SERVICE.pdf`: estructura actual de la consulta SICE-TAC, versión de agosto de 2025.
- `Consulta de SiceTac desde webservice y portal web.pdf`: explicación de los valores y fórmula del costo, versión de 2021.
- `SICETAC.MD`: descripción y riesgos del bot anterior.
- `sicetac.py`: comportamiento funcional que se reemplazará.

Cuando haya diferencias, da prioridad a la guía RNDC V5 de 2026 para infraestructura y a la guía específica SICE-TAC de 2025 para el XML de consulta. Usa la guía de 2021 solamente para conceptos que no hayan sido reemplazados, como la explicación de la fórmula.

## 3. Arquitectura esperada

Integra una solución pequeña, modular y comprobable en la arquitectura existente. La separación lógica esperada es similar a esta, pero adapta nombres y directorios al proyecto:

```text
módulo sicetac/
├── controller o route        # Endpoints de la API
├── service                    # Orquestación, selección y cálculo
├── rndc client                # Construcción y consumo de SOAP
├── repository                 # Persistencia en MongoDB
├── models/schemas             # Tipos, DTO y validaciones
├── config                     # Combinaciones y configuración no secreta
└── tests                      # Pruebas unitarias y de integración
```

Puedes ajustar los nombres si encuentras una organización más clara, pero mantén separadas estas responsabilidades:

1. Configuración editable.
2. Comunicación SOAP.
3. Lógica SICE-TAC.
4. Persistencia MongoDB.
5. Controlador/API, orquestación y reporte de ejecución.

No conviertas todo el programa en un único archivo.

## 4. Combinaciones editables

Declara en el módulo de configuración apropiado una constante llamada `COMBINACIONES`. Debe poder editarse sin tocar la lógica del cliente. Usa la sintaxis y el sistema de tipos del backend; el bloque siguiente expresa la estructura requerida y no obliga a usar Python.

No leas el Excel durante la ejecución. Las combinaciones iniciales deben ser:

```javascript
COMBINACIONES = [
    {
        "origen": "GALAPA - GALAPA - ATLANTICO",
        "origen_codigo": "08296000",
        "destino": "YUMBO",
        "destino_codigo": "76892000",
        "configuracion": "Camión dos ejes - Livianos PBV 7500-8000 Kg",
        "configuracion_codigo": "2L3",
        "unidad_transporte": "FURGON",
        "tipo_carga": "General",
        "condicion_carga": "CARGADO",
        "condicion_carga_codigo": "1",
    },
    {
        "origen": "BOGOTÁ",
        "origen_codigo": "11001000",
        "destino": "TOCANCIPÁ",
        "destino_codigo": "25817000",
        "configuracion": "Tractocamión tres ejes con semiremolque de tres ejes",
        "configuracion_codigo": "3S3",
        "unidad_transporte": "FURGON",
        "tipo_carga": "General",
        "condicion_carga": "CARGADO",
        "condicion_carga_codigo": "1",
    },
    {
        "origen": "BUCARAMANGA",
        "origen_codigo": "68001000",
        "destino": "BARRANQUILLA",
        "destino_codigo": "08001000",
        "configuracion": "Camión dos ejes - PBV mas de 10500 Kg",
        "configuracion_codigo": "2",
        "unidad_transporte": "FURGON",
        "tipo_carga": "General",
        "condicion_carga": "CARGADO",
        "condicion_carga_codigo": "1",
    },
    {
        "origen": "YUMBO",
        "origen_codigo": "76892000",
        "destino": "BOGOTÁ",
        "destino_codigo": "11001000",
        "configuracion": "Tractocamión tres ejes con semiremolque de tres ejes",
        "configuracion_codigo": "3S3",
        "unidad_transporte": "FURGON",
        "tipo_carga": "General",
        "condicion_carga": "CARGADO",
        "condicion_carga_codigo": "1",
    },
    {
        "origen": "GALAPA - GALAPA - ATLANTICO",
        "origen_codigo": "08296000",
        "destino": "SABANALARGA-ATLÁNTICO",
        "destino_codigo": "08638000",
        "configuracion": "Camión dos ejes - Livianos PBV 7500-8000 Kg",
        "configuracion_codigo": "2L3",
        "unidad_transporte": "FURGON",
        "tipo_carga": "General",
        "condicion_carga": "CARGADO",
        "condicion_carga_codigo": "1",
    },
]
```

Antes de usar los códigos en producción, valida los códigos DIVIPOLA contra el maestro vigente del RNDC o DANE. No uses el nombre visible como identificador técnico.

También define estas variables no secretas:

```text
HORAS_TOTALES_CARGUE_DEFAULT = 3
HORAS_TOTALES_DESCARGUE_DEFAULT = 3
MESES_RETROCESO_PERIODO = 1
```

El total inicial de tiempos logísticos es seis horas. Mantén los tres conceptos separados para poder cambiar la fórmula si la validación contra el portal demuestra que SICE-TAC distingue otros tiempos.

## 5. Configuración secreta y de entorno

No escribas credenciales en el código, en la configuración versionada, en archivos de prueba ni en los logs.

Carga mediante variables de entorno:

```text
RNDC_USERNAME=
RNDC_PASSWORD=
MONGODB_URI=
MONGODB_DATABASE=sicetac
MONGODB_COLLECTION=consultas
RNDC_ENVIRONMENT=production
```

Crea o actualiza el archivo de variables de ejemplo usado por el backend, únicamente con nombres y valores sin secretos. Reutiliza su sistema actual de configuración. Las variables de entorno deben tener precedencia y los archivos con secretos deben estar ignorados por Git.

Falla al inicio con un mensaje claro si falta alguna variable obligatoria. Nunca muestres el password ni la URI completa de MongoDB en mensajes de error.

## 6. Servidores vigentes

Según la guía RNDC V5 de mayo de 2026, las consultas deben enviarse al servidor dedicado del Portal Logístico de Colombia.

Producción:

```text
WSDL: http://plc.mintransporte.gov.co:8080/wsdl/IBPMServices
SOAP: http://plc.mintransporte.gov.co:8080/soap/IBPMServices
```

Pruebas:

```text
Base: http://rndcpruebas.mintransporte.gov.co:8080
WSDL: http://rndcpruebas.mintransporte.gov.co:8080/wsdl/IBPMServices
SOAP: obtener y confirmar desde el WSDL del ambiente de pruebas
```

La herramienta auxiliar para consultas es:

```text
https://rndc.mintransporte.gov.co/wstest/default3.aspx
```

Centraliza las URL por ambiente. No repitas literales en diferentes módulos.

El acceso puede estar bloqueado para IP extranjeras; el ambiente de pruebas exige una IP colombiana salvo autorización del Ministerio.

El WSDL actualmente publica HTTP. Agrega una advertencia visible en la documentación sobre el envío de credenciales sin TLS y permite reemplazar el endpoint por configuración. No desactives verificaciones TLS ni inventes una URL HTTPS.

## 7. Contrato SOAP

El servicio usa SOAP 1.1 con estilo RPC/encoded.

Datos confirmados por el WSDL:

```text
Servicio: IBPMServicesservice
Puerto: IBPMServicesPort
Operación: AtenderMensajeRNDC
Parámetro: Request, tipo string
Retorno: string
SOAPAction: urn:BPMServicesIntf-IBPMServices#AtenderMensajeRNDC
```

El contenido del parámetro `Request` es otro documento XML. Construye ambos niveles con librerías XML; no concatentes valores ingresados por el usuario sin escapar.

La envoltura deberá respetar el WSDL RPC/encoded. Usa el cliente HTTP y la biblioteca XML existentes en el backend, o un cliente SOAP que soporte correctamente RPC/encoded. Si agregas una biblioteca SOAP, prueba el XML real enviado. No cambies el lenguaje o framework del backend solamente para consumir SOAP.

Configura como mínimo:

- Timeout explícito de conexión y lectura.
- Encabezado `Content-Type` apropiado para SOAP 1.1.
- Encabezado `SOAPAction` exacto.
- Sesión HTTP reutilizable.
- Reintentos limitados solamente para errores transitorios de red y respuestas 5xx.
- Sin reintentos automáticos para errores funcionales devueltos por RNDC.

No registres el cuerpo SOAP completo porque contiene usuario y contraseña. Para depuración, genera una versión sanitizada donde ambos valores se sustituyan por `***`.

## 8. XML actual para consultar SICE-TAC

Usa la estructura documentada en agosto de 2025:

```xml
<?xml version="1.0" encoding="ISO-8859-1"?>
<root>
  <acceso>
    <username>USUARIO</username>
    <password>CLAVE</password>
  </acceso>
  <solicitud>
    <tipo>6</tipo>
    <procesoid>26</procesoid>
  </solicitud>
  <documento>
    <PERIODO>'202608'</PERIODO>
    <CONFIGURACIONESID>'3S3'</CONFIGURACIONESID>
    <CONDICIONCARGAID>'1'</CONDICIONCARGAID>
    <ORIGEN>'11001000'</ORIGEN>
    <DESTINO>'5001000'</DESTINO>
  </documento>
</root>
```

Detalles obligatorios:

- `tipo` debe ser `6`, no el valor antiguo `2`.
- `procesoid` debe ser `26`.
- Usa `CONFIGURACIONESID`, no el antiguo `CONFIGURACION`.
- `PERIODO` tiene formato `AAAAMM`.
- Conserva las comillas sencillas dentro del contenido de los filtros, tal como exige la documentación.
- `CONDICIONCARGAID`: `1` significa cargado y `2` vacío.
- Origen y destino son códigos DIVIPOLA, normalmente de ocho dígitos con `000` al final para cabecera municipal.

No uses los ejemplos de 2021 como contrato actual si contradicen la guía específica de 2025.

## 9. Periodo de consulta

Permite dos modos:

1. Periodo explícito mediante argumento `--periodo AAAAMM`.
2. Si no se entrega, usa el año y mes actual de la zona horaria `America/Bogota`.

La guía advierte que un periodo puede no tener registros porque continúan vigentes los valores del mes anterior. Si no se encuentran documentos, retrocede un mes y vuelve a consultar hasta `MESES_RETROCESO_PERIODO`.

Guarda ambos valores:

- `periodo_solicitado`.
- `periodo_aplicado`, obtenido de la respuesta.

No retrocedas ante errores de autenticación, transporte o XML; solamente cuando RNDC indique que no existen documentos para ese periodo y combinación.

## 10. Interpretación de la respuesta

La respuesta SOAP contiene un `return` de tipo string cuyo contenido vuelve a ser XML. El cliente debe:

1. Validar el estado HTTP.
2. Interpretar la envoltura SOAP y detectar `Fault`.
3. Extraer el texto de `return` sin depender de prefijos específicos de namespaces.
4. Interpretar el XML RNDC interno.
5. Detectar `ErrorMSG` antes de buscar documentos.
6. Convertir cada nodo `<documento>` a un objeto tipado.

La respuesta SICE-TAC actual puede incluir:

```text
periodo
fechaingreso
origen
nomorigen
destino
nomdestino
condicioncarga
configuracion
tipocarga
nombretipocarga
unidadtransporte
nombreunidadtransporte
kilometros
valormoviliza
valorhora
horasrecorrido
viaestandar
rutasid
via
```

Usa `Decimal`, no `float`, para valores monetarios. Conserva también la respuesta de negocio interpretada; no almacenes credenciales ni el SOAP crudo con secretos.

Puede haber varios documentos para la misma combinación debido a diferentes rutas y atributos. Filtra o clasifica por:

- Configuración.
- Condición de carga.
- Tipo de carga.
- Unidad de transporte.
- Origen y destino.

Compara textos normalizando mayúsculas, espacios y acentos, pero conserva el texto original entregado por RNDC. No des por sentado que `FURGON` y otra denominación histórica son equivalentes sin una regla documentada.

Si existen varias rutas válidas, guarda todas. Marca claramente `viaestandar` y `rutasid`; no elijas silenciosamente una sola.

## 11. Cálculo del valor total

La guía de 2021 documenta:

```text
valor mínimo del viaje =
valor de movilización +
(valor por hora × horas pactadas de cargue, descargue y espera)
```

Para la configuración inicial:

```text
horas_logisticas = horas_totales_cargue + horas_totales_descargue
costo_total_calculado = valormoviliza + valorhora * horas_logisticas
```

Implementa la fórmula en una función pura con pruebas unitarias. Guarda por separado:

- `valor_moviliza`.
- `valor_hora`.
- Cada cantidad de horas.
- `horas_logisticas_total`.
- `costo_total_calculado`.

No sobrescribas los valores originales recibidos.

Antes de considerar terminada la migración, compara manualmente por lo menos tres resultados con el portal SICE-TAC. Deben incluir una configuración `3S3`, una `2` y una `2L3`. Si hay diferencias, documenta los valores y determina si el portal usa tiempos adicionales o reglas de redondeo. No ajustes la fórmula mediante números mágicos.

## 12. Persistencia en MongoDB

Usa el ODM, driver o repositorio MongoDB que ya tenga el backend. No agregues una segunda conexión si MongoDB ya está resuelto. La base y colección deben configurarse mediante:

```text
MONGODB_DATABASE
MONGODB_COLLECTION
```

La ejecución debe ser idempotente. No insertes duplicados cada vez que corre el proceso. Crea un índice único compuesto o un campo determinístico `consulta_id` generado a partir de:

```text
periodo_aplicado
origen
destino
configuracion
condicion_carga
tipo_carga
unidad_transporte
rutasid
```

Usa la operación de `upsert` o escritura masiva equivalente disponible en el driver u ODM existente.

Ejemplo orientativo del documento almacenado:

```javascript
{
  "consulta_id": "hash-deterministico",
  "periodo_solicitado": "202608",
  "periodo_aplicado": "202608",
  "fecha_ingreso_rndc": "20260802",
  "origen": {
    "codigo": "11001000",
    "nombre_configurado": "BOGOTÁ",
    "nombre_rndc": "BOGOTA BOGOTA D. C."
  },
  "destino": {
    "codigo": "05001000",
    "nombre_configurado": "MEDELLÍN",
    "nombre_rndc": "MEDELLIN ANTIOQUIA"
  },
  "configuracion": {
    "codigo": "3S3",
    "nombre_configurado": "Tractocamión tres ejes con semiremolque de tres ejes"
  },
  "condicion_carga": "CARGADO",
  "tipo_carga": {
    "codigo": "2",
    "nombre": "Carga Refrigerada"
  },
  "unidad_transporte": {
    "codigo": "60",
    "nombre": "FURGON REFRIGERADO"
  },
  "ruta": {
    "id": "1",
    "via_estandar": true,
    "descripcion": "...",
    "kilometros": 505,
    "horas_recorrido": "12.07"
  },
  "costos": {
    "valor_moviliza": "3873858",
    "valor_hora": "101509",
    "horas_totales_cargue": "3",
    "horas_totales_descargue": "3",
    "horas_logisticas_total": "6",
    "costo_total_calculado": "4482912"
  },
  "fuente": "RNDC_SICETAC_WS",
  "consultado_en": ISODate("..."),
  "actualizado_en": ISODate("...")
}
```

Para evitar pérdida de precisión, almacena importes como `Decimal128` de BSON o como enteros si RNDC garantiza pesos enteros. Elige una estrategia, documenta la decisión y úsala consistentemente. No uses `float` para dinero.

Actualiza `actualizado_en` en cada upsert y conserva `creado_en` mediante `$setOnInsert`.

Antes de consultar el web service, verifica que la conexión MongoDB del backend esté disponible. Si no lo está, rechaza o marca como fallida la ejecución antes de consultar RNDC, salvo que el backend ya tenga una cola recuperable.

## 13. Resultado de la ejecución

La herramienta debe mostrar un resumen seguro y legible:

```text
Combinaciones configuradas: 5
Consultas exitosas: ...
Documentos recibidos: ...
Documentos insertados: ...
Documentos actualizados: ...
Combinaciones sin resultado: ...
Errores: ...
```

Devuelve código de salida distinto de cero si:

- Faltan variables obligatorias.
- Fallan las credenciales.
- No puede conectarse a MongoDB.
- El contrato SOAP no puede interpretarse.
- Todas las combinaciones fallan.

Una combinación sin datos no debe interrumpir las siguientes. Registra el motivo sin convertir automáticamente todos los errores en `Ruta no conocida`.

Usa `logging` con niveles y evita `print` dispersos. Nunca registres secretos.

## 14. API y ejecución del proceso

Integra la funcionalidad bajo el prefijo de rutas y el esquema de autenticación y autorización que ya utiliza el backend. Como diseño de referencia, implementa operaciones equivalentes a:

```text
POST /sicetac/consultas
GET  /sicetac/consultas/{ejecucionId}
GET  /sicetac/resultados
```

El `POST` debe aceptar opcionalmente un periodo `AAAAMM`; si no llega, aplica la regla del periodo actual. El consumidor de la API nunca debe enviar ni sobrescribir credenciales RNDC.

Incluye un modo protegido de validación o `dryRun` que permita validar solicitudes sin escribir en MongoDB. Separa claramente “no persistir” de “no llamar al RNDC”. El ambiente se obtiene de `RNDC_ENVIRONMENT`, no de un valor arbitrario enviado por cualquier consumidor.

Si las consultas pueden superar el timeout HTTP normal, reutiliza el sistema de colas o jobs existente y responde `202 Accepted` con un `ejecucionId`. El endpoint de estado debe informar progreso, resultados, errores y finalización. Si el backend no tiene colas y una medición demuestra que el proceso termina dentro de sus límites, puede ser síncrono dejando documentada la decisión.

Protege los endpoints con los permisos administrativos existentes y evita ejecuciones duplicadas o concurrentes accidentales.

## 15. Manejo de errores y reintentos

Define excepciones específicas, por ejemplo:

- `ConfigurationError`.
- `RNDCCredentialsError`.
- `RNDCTransportError`.
- `RNDCSoapFaultError`.
- `RNDCBusinessError`.
- `RNDCResponseParseError`.
- `MongoPersistenceError`.

Usa backoff acotado para timeouts, desconexiones y HTTP 5xx. No reintentes credenciales inválidas ni errores de documento.

Incluye suficiente contexto en los errores para identificar periodo, origen, destino y configuración, pero nunca usuario, contraseña o URI completa de MongoDB.

## 16. Pruebas obligatorias

Las pruebas no deben llamar por defecto al servicio real ni a una base MongoDB real. Usa el framework de pruebas y las utilidades de mocks existentes en el backend.

Incluye fixtures con respuestas SOAP/XML sanitizadas tomadas de la estructura documental y prueba:

1. Construcción correcta del XML SICE-TAC con comillas internas.
2. Escape seguro de caracteres especiales.
3. Construcción de la envoltura SOAP y `SOAPAction`.
4. Extracción de `return` con namespaces variables.
5. Interpretación de múltiples `<documento>`.
6. Interpretación de `ErrorMSG`.
7. Interpretación de SOAP Fault.
8. Conversión monetaria sin `float`.
9. Fórmula del costo total.
10. Retroceso de periodo cuando no hay registros.
11. No retroceder ante error de autenticación.
12. Identificador determinístico e idempotencia MongoDB.
13. Sanitización de credenciales en logs.
14. Validación completa de `COMBINACIONES`.

Usa mocks o adaptadores de prueba para HTTP y MongoDB compatibles con la pila existente. No introduzcas herramientas de otro ecosistema.

Agrega una prueba de integración opcional, desactivada por defecto, que solamente se ejecute cuando existan variables de entorno explícitas para pruebas.

## 17. Validaciones de configuración

Antes de conectarse, valida cada combinación:

- Código de municipio con exactamente ocho dígitos.
- Periodo con seis dígitos y mes válido.
- Configuración dentro de los códigos documentados: `3S3`, `3S2`, `2S3`, `2S2`, `3`, `2`, `2L1`, `2L2`, `2L3`, `V2`, `V3`, `V4`.
- Condición `1` o `2`.
- Textos obligatorios no vacíos.
- No duplicados exactos en `COMBINACIONES`.

Normaliza los caracteres dañados que existen en los archivos anteriores. Todo el código nuevo, documentación y pruebas debe guardarse en UTF-8.

El XML interno debe serializarse en la codificación requerida por RNDC, `ISO-8859-1`, manejando de forma explícita cualquier carácter que no pueda representarse.

## 18. Dependencias

Mantén las dependencias mínimas y reutiliza primero las que ya existan para HTTP, XML, MongoDB, configuración, validación, fechas, colas, logging y pruebas.

Si necesitas agregar un cliente SOAP o una biblioteca XML, comprueba que soporte SOAP 1.1 RPC/encoded, justifica su necesidad y fija una versión compatible. No agregues Python, Selenium, pandas, `openpyxl` ni un runtime paralelo: la nueva ejecución no requiere navegador ni Excel.

## 19. Documentación que debes entregar

Además del código, actualiza o crea documentación con:

- Instalación.
- Variables de entorno.
- Cómo editar `COMBINACIONES`.
- Endpoints, payloads, respuestas, permisos y ejecución en pruebas y producción.
- Cómo ejecutar los tests.
- Esquema de MongoDB e índice único.
- Política de reintentos.
- Advertencia sobre HTTP sin TLS.
- Procedimiento para comparar resultados contra el portal.
- Cómo agregar nuevas configuraciones o municipios.
- Diferencias respecto a `sicetac.py`.

No afirmes que el reemplazo está validado hasta completar las comparaciones con el portal usando credenciales autorizadas.

## 20. Secuencia de implementación

Sigue este orden:

1. Inspecciona el backend, sus instrucciones de desarrollo y sus convenciones.
2. Lee las tres guías y el código anterior de SICE-TAC.
3. Inspecciona el WSDL vigente sin enviar credenciales.
4. Diseña la integración usando el lenguaje, framework, MongoDB, autenticación y colas existentes.
5. Crea los módulos, rutas y configuración.
6. Implementa y prueba la generación del XML interno.
7. Implementa y prueba la envoltura y respuesta SOAP.
8. Implementa el modelo de resultados y el cálculo puro.
9. Implementa MongoDB, el índice único y los upserts.
10. Implementa la API, su seguridad, la ejecución síncrona o job y el resumen.
11. Ejecuta todas las pruebas sin conexiones reales.
12. Prueba primero en el ambiente RNDC de pruebas con credenciales autorizadas.
13. Compara al menos tres combinaciones contra el portal.
14. Documenta diferencias y corrige solamente con evidencia.
15. Habilita producción después de la validación.

No elimines el bot Selenium durante esta fase. Debe quedar como respaldo hasta confirmar que el web service reproduce los resultados requeridos.

## 21. Criterios de aceptación

La tarea estará completa cuando:

- La herramienta no lea `Plantilla BOT - ejemplo.xlsx`.
- Las cinco combinaciones estén en una variable editable.
- La implementación use el lenguaje, framework y arquitectura del backend existente.
- La funcionalidad esté disponible mediante endpoints protegidos de la API.
- No use Selenium ni abra un navegador.
- Consulte mediante `AtenderMensajeRNDC` y el servidor correcto para consultas.
- Use `tipo=6` y `procesoid=26`.
- Interprete respuestas múltiples y errores del RNDC.
- Calcule el costo sin usar `float`.
- Guarde todos los resultados pertinentes en MongoDB.
- Las ejecuciones repetidas no generen duplicados.
- No exponga credenciales.
- Tenga pruebas automatizadas que no dependan de servicios reales.
- Incluya instrucciones de instalación y operación.
- El código anterior permanezca disponible como respaldo.
- Las diferencias contra el portal estén validadas o claramente documentadas como pendientes.

## 22. Decisiones que no debes asumir

Detén la implementación y solicita información si falta alguno de estos datos para una prueba real:

- Credenciales RNDC autorizadas.
- URI de MongoDB.
- Confirmación del ambiente que se debe utilizar.

No inventes credenciales, no pruebes las que aparecen en ejemplos de los PDF y no guardes secretos en archivos versionados.

Si el servicio devuelve una nomenclatura distinta para `FURGON` o `General`, conserva la respuesta, muestra la discrepancia y solicita una decisión antes de crear equivalencias que puedan cambiar el costo.
