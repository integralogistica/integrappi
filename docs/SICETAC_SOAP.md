# Integración SICE-TAC por SOAP

Este módulo reemplaza como mecanismo principal al bot Selenium, sin eliminarlo. No abre un navegador ni lee `Plantilla BOT - ejemplo.xlsx`. Las cinco consultas se editan en `sicetac/config.py`, constante `COMBINACIONES`.

## Configuración

Copie los nombres de `.env.example` a su gestor de secretos. Son obligatorias `RNDC_USERNAME`, `RNDC_PASSWORD` y `MONGODB_URI`; no se registran sus valores. `RNDC_ENVIRONMENT` admite `production` o `test`. En pruebas debe confirmar `RNDC_SOAP_URL` desde el WSDL. Las variables de entorno tienen precedencia.

El WSDL de producción fue inspeccionado sin credenciales y publica SOAP 1.1 RPC/encoded en `http://plc.mintransporte.gov.co:8080/soap/IBPMServices`. **Advertencia:** es HTTP sin TLS, por lo que las credenciales viajan sin cifrado de transporte. El endpoint es reemplazable con `RNDC_SOAP_URL`; no se desactiva ninguna validación TLS ni se inventa HTTPS.

## API y permisos

Todos los endpoints usan el Bearer JWT actual y requieren rol `ADMIN` o `ADMINISTRADOR`:

- `POST /sicetac/consultas`: cuerpo opcional `{"periodo":"202608","dryRun":false}`. Devuelve `202` y `ejecucion_id`. `dryRun` llama al RNDC y valida MongoDB, pero no escribe.
- `GET /sicetac/consultas/{ejecucion_id}`: progreso, resumen y errores seguros. El estado vive en memoria y se pierde al reiniciar el proceso.
- `GET /sicetac/resultados?periodo=202608&limit=100`: resultados persistidos.

Solo se permite una ejecución simultánea por proceso. El backend no dispone de una cola durable; por eso se usa su mecanismo de tareas en segundo plano. En despliegues con varios workers debe sustituirse el bloqueo local por una cola/bloqueo distribuido.

## MongoDB y dinero

La base y colección son configurables. Se crea el índice único `uq_sicetac_consulta_id`; las escrituras son `upsert` y conservan `creado_en`. El identificador SHA-256 incluye periodo aplicado, ruta y atributos técnicos. Importes y horas se calculan con `Decimal` y se guardan como BSON `Decimal128`, nunca `float`.

## Reintentos y periodos

HTTP usa sesión reutilizable, timeout de conexión de 10 s y total de 45 s. Hay hasta tres intentos solo ante red o HTTP 5xx. Errores SOAP, autenticación, negocio y XML no se reintentan. Un resultado vacío retrocede hasta tres meses; otros errores no ocasionan retroceso.

## Pruebas

Ejecute desde `integrappi`:

```text
python -m unittest discover -s tests -p "test_sicetac*.py"
```

Las pruebas usan XML sanitizado, HTTP simulado y repositorio falso; no llaman a RNDC ni MongoDB. La prueba real queda deshabilitada hasta disponer de credenciales autorizadas, URI y confirmación de ambiente.

## Validación operativa pendiente

Antes de habilitar producción valide los DIVIPOLA contra el maestro vigente de RNDC/DANE y compare manualmente en el portal al menos una combinación `3S3`, una `2` y una `2L3`. Registre valor de movilización, valor hora, seis horas logísticas, total y cualquier redondeo. No se afirma equivalencia con el portal mientras esta actividad esté pendiente. Si RNDC devuelve nombres distintos de `FURGON` o `General`, se conserva la respuesta y se requiere una decisión antes de crear equivalencias.

Para agregar municipios o vehículos edite únicamente `COMBINACIONES`; los códigos municipales deben tener ocho dígitos y las configuraciones pertenecer al catálogo validado. A diferencia del antiguo `sicetac.py`, este módulo usa `tipo=6`, `procesoid=26`, `CONFIGURACIONESID`, SOAP, múltiples rutas, Mongo idempotente y no depende de CAPTCHA, Excel o Selenium.

