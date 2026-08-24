# Integración SICE-TAC por SOAP

Este módulo reemplaza como mecanismo principal al bot Selenium, sin eliminarlo. No abre un navegador ni lee `Plantilla BOT - ejemplo.xlsx`. Las cinco consultas se editan en `sicetac/config.py`, constante `COMBINACIONES`.

## Configuración

Copie los nombres de `.env.example` a su gestor de secretos. Son obligatorias `RNDC_USERNAME`, `RNDC_PASSWORD` y la variable `MONGO_URI` que ya utiliza el backend; no se registran sus valores. SICE-TAC reutiliza esa conexión y no necesita una segunda URI. `RNDC_ENVIRONMENT` admite `production` o `test`. En pruebas debe confirmar `RNDC_SOAP_URL` desde el WSDL. Las variables de entorno tienen precedencia.

El proceso especial SICE-TAC `26`, solicitud `tipo=6`, fue verificado el 24 de agosto de 2026 en `http://rndcws.mintransporte.gov.co:8080/ws/svr008w.dll/soap/IBPMServices`. Aunque la guía general RNDC V5 asigna consultas al servidor PLC, ese servidor devuelve `RNDC33` para esta operación. La ruta corta `/soap/IBPMServices` de `rndcws` también devolvió `RNDC13` intermitentemente, mientras la ruta completa histórica respondió inmediatamente con documentos para la misma solicitud y credenciales. **Advertencia:** es HTTP sin TLS, por lo que las credenciales viajan sin cifrado de transporte. El endpoint es reemplazable con `RNDC_SOAP_URL`; no se desactiva ninguna validación TLS ni se inventa HTTPS.

## API y permisos

Todos los endpoints usan el esquema OAuth2/JWT `BaseUsuariosOAuth2`, autenticado por `POST /baseusuarios/token` con el mismo usuario y clave de `LoginUsuario`. Requieren que el `perfil` vigente en `baseusuarios` sea `ADMIN` o `ADMINISTRADOR`. Los usuarios inactivos y los tokens vencidos son rechazados:

- `POST /sicetac/consultas`: cuerpo opcional `{"periodo":"202608","dryRun":false}`. Devuelve `202` y `ejecucion_id`. `dryRun` llama al RNDC y valida MongoDB, pero no escribe.
- `GET /sicetac/consultas/{ejecucion_id}`: progreso, resumen y errores seguros. El estado vive en memoria y se pierde al reiniciar el proceso.
- `GET /sicetac/resultados?periodo=202608&limit=100`: resultados persistidos.

Solo se permite una ejecución simultánea por proceso. El backend no dispone de una cola durable; por eso se usa su mecanismo de tareas en segundo plano. En despliegues con varios workers debe sustituirse el bloqueo local por una cola/bloqueo distribuido.

## MongoDB y dinero

La base y colección son configurables. Se crea el índice único `uq_sicetac_consulta_id`; las escrituras son `upsert` y conservan `creado_en`. El identificador SHA-256 incluye periodo aplicado, ruta y atributos técnicos. Importes y horas se calculan con `Decimal` y se guardan como BSON `Decimal128`, nunca `float`.

## Reintentos y periodos

HTTP usa sesión reutilizable, timeout de conexión de 10 s y total de 45 s. El cliente limita las solicitudes a una por segundo y reintenta errores de red y HTTP 5xx. Cuando una consulta exacta devuelve `RNDC11` o `RNDC13`, el servicio hace un fallback por periodo, configuración y origen, y aplica destino, condición, tipo de carga y unidad localmente. Las respuestas amplias se reutilizan entre combinaciones equivalentes para evitar llamadas duplicadas. Si tampoco hay coincidencias, consulta únicamente el mes inmediatamente anterior; otros errores no ocasionan retroceso.

`POST /sicetac/rutas-disponibles` permite verificar mediante un cuerpo JSON una combinación sin escribir en MongoDB. Requiere `periodo`, `configuracion`, `origen` y `destino`; `condicion_carga` acepta `1` (cargado) o `2` (vacío). El servidor RNDC actual rechaza con RNDC13 las consultas destinadas a enumerar todos los destinos omitiendo el destino, aunque la guía marque ese filtro como opcional.

## Pruebas

Ejecute desde `integrappi`:

```text
python -m unittest discover -s tests -p "test_sicetac*.py"
```

Las pruebas usan XML sanitizado, HTTP simulado y repositorio falso; no llaman a RNDC ni MongoDB. La prueba real queda deshabilitada hasta disponer de credenciales autorizadas, URI y confirmación de ambiente.

## Validación operativa pendiente

Antes de habilitar producción valide los DIVIPOLA contra el maestro vigente de RNDC/DANE y compare manualmente en el portal al menos una combinación `3S3`, una `2` y una `2L3`. Registre valor de movilización, valor hora, seis horas logísticas, total y cualquier redondeo. No se afirma equivalencia con el portal mientras esta actividad esté pendiente. Si RNDC devuelve nombres distintos de `FURGON` o `General`, se conserva la respuesta y se requiere una decisión antes de crear equivalencias.

Para agregar municipios o vehículos edite únicamente `COMBINACIONES`; los códigos municipales deben tener ocho dígitos y las configuraciones pertenecer al catálogo validado. A diferencia del antiguo `sicetac.py`, este módulo usa `tipo=6`, `procesoid=26`, `CONFIGURACIONESID`, SOAP, múltiples rutas, Mongo idempotente y no depende de CAPTCHA, Excel o Selenium.
