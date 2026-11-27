# Seguridad — Consultas por fuente y Estudios de Seguridad (multi-tenant + cobro)

Dieciocho capas bajo `/seguridad` (la 19ª es transversal, no una fuente):

1. **API por fuente** (v1, 2026-08-29): consulta individual de RNDC,
   Procuraduría o **Policía (2026-08-30)** con caché 24 h. Bearer.
2. **Estudios de Seguridad** (v2, 2026-08-29): servicio multi-empresa.
   Cédula → fuentes en paralelo → estado global honesto → PDF
   consolidado profesional (referencia TusDatos) → GCS privado → auditoría.
   Aislamiento por `empresa_id`, roles, rate limit, protección de datos.
3. **Planes y cobro postpago** (v3, 2026-08-29): el servicio como negocio.
   **Multi-plan por fuente (v3.1, 2026-08-29)**: cada empresa tiene un array
   `planes`; cupos independientes (o `None` = sin tope, pospago puro).
   **Planes acumulables (v3.2, 2026-08-29)**: varias entradas por fuente
   (AVANZADO + PRO conviven); asignación admin POR PLAN
   (`PUT /empresas/{id}/plan`), consumo FIFO por fecha de asignación.
   **El usuario elige el plan al consultar (v3.3, 2026-08-29)**: el plan
   elegido define las fuentes a correr y bajo qué precio se cobra.
   Reembolso automático en ERROR global (todos los consumos), pagos/ajustes
   manuales, cierres de mes con cuenta de cobro PDF. Panel admin
   (`/AdminSeguridad`, perfil ADMIN) y portal cliente (`/PortalSeguridad`,
   independiente de la Torre de Control; perfil **CLIENTE_ESTUDIOS**).
4. **Fuente Policía — Antecedentes Judiciales** (2026-08-30): tercera fuente
   del orquestador. Portal JSF/PrimeFaces con **reCAPTCHA v2 resuelto por
   2Captcha**; resultado = leyendas oficiales (SU-458/2012), **sin PDF**,
   trae el nombre del consultado. **OPT-IN por empresa**: el portal es de
   autoconsulta del titular (Decreto 019/2012) y prohíbe la consulta por
   terceros → NO va en `fuentes_habilitadas` default ni en el seed; se
   activa por empresa con autorización documentada del titular (Ley 1581).
5. **Fuente RUNT — Vehículo** (2026-08-30): CUARTA fuente; primera con un
   parámetro adicional: se consulta por **placa + cédula del PROPIETARIO
   ACTIVO**. SPA Angular con **captcha de imagen propio resuelto por 2Captcha
   (`method=base64`, ~US$0.001)**; resultado = datos del vehículo + póliza
   SOAT con vigencia (semáforo: **SOAT vencido → fuente ADVERTENCIA**) +
   historial de pólizas. Portal público de consulta ciudadana → SÍ va en el
   default (NO es opt-in como policía). El POST del estudio acepta `placa`
   (obligatoria si runt corre); la caché discrimina por **(tipo, cédula,
   placa)**. El portal cliente muestra el formulario DENTRO del plan
   (acordeón): pide placa si el plan incluye runt o simit (2026-09-01).
   **Propietario ≠ evaluado (2026-08-30 tarde)**: el RUNT valida contra el
   DUEÑO de la placa, que muchas veces NO es el conductor evaluado → el POST
   acepta `cedula_propietario` opcional (vacía = el evaluado es el dueño);
   runt se consulta/cachea con la cédula del PROPIETARIO; el doc persiste
   `vehiculos[{placa, cedula_propietario, propietario_es_evaluado}]` y el PDF
   diferencia "persona evaluada" de "propietario del vehículo" (badge ámbar
   cuando son distintos + fila de trazabilidad con cédula COMPLETA —
   decisión de negocio 2026-08-30: el cliente necesita ver las cédulas para
   cruzar; el enmascarado queda SOLO en el QR público y en logs/eventos).
6. **Servicio por API — API keys de integración (2026-08-30)**: los clientes
   consumen el servicio máquina a máquina con `Authorization: Bearer sek_…`
   (MISMO header que el JWT del portal; `actor_actual` enruta por prefijo).
   La clave se muestra UNA vez al crearla; en BD solo su SHA-256. Actor
   derivado = CONSULTADOR de la empresa (jamás admin), usuario "API: nombre",
   `usuario_id` None, **canal="api"**: estudio, movimientos CONSUMO y eventos
   quedan marcados con el origen — el historial del portal cliente y los
   movimientos del admin muestran badge "API" para distinguir manual vs
   integración. Gestión en `/AdminSeguridad` (sección "API keys" de la fila
   expandible de la empresa: crear/ver prefijo/último uso/revocar, máx 10
   activas por empresa). Anti-enumeración: clave inválida/revocada/empresa
   inactiva → mismo 401 genérico. `ultimo_uso_en` con throttle 1 write/min.
   Una API key ignora `aislamiento_usuario` (es una integración de la
   EMPRESA, ve todos sus estudios); el rate limit por empresa le aplica igual.
7. **Fuente SIMIT — Comparendos (2026-09-01)**: QUINTA fuente; consulta el
   estado de cuenta público de la FCM
   (`https://www.fcm.org.co/simit/#/estado-cuenta`) **SOLO por placa** (el
   campo único acepta cédula o placa; el módulo consulta por placa). SPA con
   hash-routing y **SIN CAPTCHA** (submit directo: fuente gratuita y rápida,
   ~10 s). Devuelve resumen (comparendos/multas/acuerdos/deuda total),
   **total EXIGIBLE** (`total_a_pagar`, agregado `#valorTotal` del portal —
   base del semáforo: >0 → fuente ADVERTENCIA, análogo SOAT vencido) y las
   filas de la primera página (totales globales del resumen; el detalle
   pagina 5/10/15). **La consulta es sobre la PLACA, no la persona**: nunca
   se presenta como antecedente personal (mismo espíritu que
   propietario ≠ evaluado). Va en defaults como runt (portal público de
   consulta ciudadana). Caché 24 h por **(tipo="simit", placa)** con
   `cedula: None` (la cédula del evaluado jamás participó en la consulta del
   portal: no fragmenta caché ni fabrica atribución). Con solo simit el doc
   persiste `placa` espejo pero **NO** `vehiculos[]` (solo runt valida la
   tríada placa+propietario). Trampa del portal: hay **modales ocultos con
   texto plantilla que menciona la placa consultada** — el bot lee solo
   contenido VISIBLE. Inconsistencia vista en producción (ZZZ999): filas con
   "valor a pagar" > 0 aunque el agregado diga $0 (prescritos) — el semáforo
   se fía del AGREGADO.
8. **Fuente SENA — Certificados de formación (2026-09-01)**: SEXTA fuente;
   consulta por **CÉDULA** el portal Certificado Digital del SENA
   (`certificados.sena.edu.co/CertificadoDigital/com.sena.consultacer`).
   Portal **GeneXUS** server-rendered (NO JSF, NO SPA: ids limpios y
   estables — `vNUMERO_DOCUMENTO`, `vCAPTCHATEXT`, botón `#CONSULTAR`; los
   selects ya vienen Documento+CC por defecto). Captcha de **imagen propio**
   (GIF embebido data:image → 2Captcha `method=base64`, ~US$0.001). Devuelve
   el LISTADO de certificados disponibles (registro, título, tipo Acta/
   Título/Certificado, programa, fechas certificación/firma en ISO) — **sin
   PDFs anexos** (decisión de negocio: solo listado). Es información de
   FORMACIÓN, no un antecedente: **SIEMPRE EXITO informativo** (registrar N
   certificados o ninguno son respuestas determinantes). Va en defaults como
   runt/simit (portal público sin restricción de terceros). Caché 24 h por
   **(tipo="sena", cédula)** — el default del módulo. **Trampa calibrada con
   la sonda**: "No hay resultados" es también el estado INICIAL de la página
   y lo que muestra un captcha equivocado (ErrorViewer GeneXUS "El texto
   digitado no corresponde con la imagen…") — evidencia de que el postback
   SE PROCESÓ = la imagen del captcha CAMBIÓ (el portal la regenera en cada
   respuesta).
9. **Fuente OFAC — Lista SDN / Lista Clinton (2026-09-01)**: SÉPTIMA fuente;
   consulta por **CÉDULA** el XML oficial de Specially Designated Nationals
   publicado por OFAC. No automatiza el formulario ni usa coincidencia difusa
   por nombre: normaliza el documento y exige una **coincidencia exacta de
   identificación**. El dataset (~29 MB) se indexa en memoria y se refresca
   cada 6 horas, por lo que no se descarga por cada consulta. Una coincidencia
   produce ADVERTENCIA y exige revisión humana de identidad, homonimia y alcance
   del programa; nunca decide un rechazo automático. Va en los planes/defaults,
   usa la caché de consulta de 24 h y no requiere captcha ni API key.
   **Ficha SDN completa en el match (2026-09-25, patrón TusDatos)**: la
   coincidencia arrastra título/cargo, fecha y lugar de nacimiento,
   nacionalidades, ciudadanías, direcciones, alias (≤5), observaciones (≤400)
   y el **link oficial** `sanctionssearch.ofac.treas.gov/Details.aspx?id={uid}`
   — todo ya presente en el XML (`_ficha_sdn` en bot_ofac; el PDF pinta la
   ficha con `_ficha_coincidencia_ofac`, retro-compatible con cachés previas:
   las filas vacías no se pintan). Aplica a `ofac` y `ofac_nit` (índice
   compartido). ⚠️ Al validar extracciones de texto del PDF: la marca de agua
   diagonal entremezcla caracteres — filtrar por `char["upright"]` en
   pdfplumber.
10. **Fuente OFAC empresarial por NIT (2026-09-01)**: OCTAVA fuente,
    identificada como `ofac_nit` y separada de `ofac`. Acepta el NIT completo
    con dígito de verificación y solo compara tipos inequívocamente tributarios
    del XML (`Tax ID No.`/códigos fiscales). No usa números de registro
    mercantil genéricos para evitar falsos positivos. Endpoint individual
    `GET /seguridad/ofac-nit`; caché, plan, portal y sección PDF independientes.
11. **Fuente Contraloría — Antecedentes Fiscales, persona natural
    (2026-09-02)**: NOVENA fuente por **cédula** (tipo doc fijo CC). Consulta el
    Certificado de Antecedentes Fiscales de la CGR contra el SIBOR (público y
    gratuito, exigible en contratación — Decreto 2150 de 1995). El portal
    Liferay embebe el form en un IFRAME WebForms de
    `cfiscal.contraloria.gov.co` con **reCAPTCHA v2 resuelto por 2Captcha**
    (`userrecaptcha`, ~US$0.003; token inyectado en el textarea — el servidor
    valida el campo POST, no el checkbox). **Trampas (hallazgos de la sonda)**:
    (a) el validationGroup exige TRES radios de una ENCUESTA OCULTA
    (`#jumbotronEncuesta`: rbdExpectativa/rbdOportunidad/rbdUtilidad) — sin
    marcarlas WebForms bloquea el postback SILENTEMENTE; (b) el submit es un
    postback PARCIAL async (ScriptManager): sin navegación ni evento load;
    (c) el certificado llega como DESCARGA PDF que se procesa en memoria con
    pdfplumber (veredicto "NO SE ENCUENTRA REPORTADO COMO RESPONSABLE
    FISCAL"/"SE ENCUENTRA REPORTADO…" + **código de verificación** del
    certificado) y NO se persiste ni expone. Veredicto limpio → EXITO;
    reportado → ADVERTENCIA (análogo procuraduría). Va en defaults (certificado
    de verificación de terceros por diseño); endpoint individual
    `GET /seguridad/contraloria`; caché 24 h por (tipo, cédula).
12. **Fuente RUES — Registro Mercantil por NIT (2026-09-03)**: DÉCIMA fuente,
    por **NIT sin dígito de verificación** (viaja en el campo `nit` del POST,
    como ofac_nit). La SPA de rues.org.co NO consulta backend propio: llama a
    un **API Elasticsearch público** (`elasticprd.rues.org.co`) cuyos payloads
    van **cifrados** con CryptoJS AES-256-CBC (formato OpenSSL "Salted__",
    passphrase `"qwerty"` = `REACT_APP_SECRET_KEY` embebida en el chunk
    `services/127.*.js` — hallazgo de la sonda 2026-09-03). El bot replica el
    cifrado y consulta el API **con requests puro, SIN navegador** (molde
    `bot_ofac.py`, NO `bot_simit.py`): ~1-2 s por consulta, **costo $0**, sin
    captcha, sin semáforo de Chromium ni `_SYNC_ORIGINALES`. Contrato:
    `POST /query` (búsqueda) → `POST /api/Expediente/DetalleRM` (detalle:
    estado, renovaciones, tipo societario, CIIU con descripciones) →
    `POST /api/ConsultFacultadesXCamYMatricula` (representante legal en HTML
    plano; enriquecimiento best-effort cuyo fallo NO tumba la fuente).
    **Semáforo (decisión de negocio 2026-09-03, análoga a SOAT vencido / saldo
    SIMIT)**: matrícula distinta de ACTIVA (CANCELADA / INACTIVA / cancelada
    por Ley 1429 / pérdida de calidad de comerciante) → **ADVERTENCIA**;
    ACTIVA → EXITO; NIT sin match exacto → EXITO determinante `no_registra`
    (sobre el NIT, jamás un "limpio"). **Trampas**: (a) un NIT inexistente NO
    devuelve 0 hits — el buscador lista TODO (`total: 10000`): solo vale el hit
    con coincidencia EXACTA de `numero_identificacion` (padding de ceros a la
    izquierda); (b) el filtro default `Status:["ACTIVA"]` OCULTA las
    canceladas → siempre se envía `["ACTIVA","CANCELADA"]` y el estado real se
    lee del hit; (c) el buscador acepta términos de ≤10 caracteres (11+ → 400);
    (d) el `estado` del DetalleRM llega con las letras espaciadas ("A C T I V
    A") → se normaliza. Si Confecámaras rota la passphrase → fuente
    NO_DISPONIBLE accionable: rotarla en `SEGURIDAD_RUES_SECRET_KEY` sin
    redeploy (plan B documentado: bot Playwright con UA real — el CDN de la SPA
    bloquea el UA `HeadlessChrome`). Caché 24 h por (tipo="rues", NIT en el
    campo cédula, como ofac_nit); endpoint individual `GET /seguridad/rues`.
13. **Fuente ONU/UE — Listas internacionales de sanciones (2026-09-14)**:
    por **cédula**. Agrega DOS datasets oficiales en UNA sola consulta
    (inspirada en el reporte de TusDatos, sprint 1): la **Lista Consolidada
    del Consejo de Seguridad de la ONU**
    (`scsanctions.un.org/resources/xml/en/consolidated.xml`, ~2 MB, 736
    personas) y el **Consolidated Financial Sanctions File 1.1 de la UE**
    (webgate.ec.europa.eu, ~25 MB, 4.462 personas). **Molde `bot_ofac`**: sin
    navegador, sin captcha, $0; datasets indexados en memoria con TTL 6 h.
    **Solo se indexan identificadores inequívocos de documento de identidad**
    (ONU `National Identification Number` · UE `identificationTypeCode="id"`);
    pasaportes y números tributarios quedan FUERA (misma disciplina
    anti-falsos-positivos de OFAC). Coincidencia exacta → **ADVERTENCIA +
    revisión humana** (nunca rechazo automático). **Degradación honesta**: si
    UNA lista no puede descargarse, la consulta sale con la otra y lo declara
    en `listas_no_disponibles`; si fallan ambas → NO_DISPONIBLE
    `sanciones_no_disponible`. Endpoint individual `GET /seguridad/sanciones`;
    caché 24 h por (tipo="onu_ue", cédula). **Trampa UE**: la descarga anónima
    de webgate sin token responde EU Login (401/HTML) — la URL lleva el token
    público `dG9rZW4tMjAxNw` (= "token-2017") documentado como distribución
    oficial en data.europa.eu (hallazgo de la sonda 2026-09-14). **UK OFSI
    fuera de alcance por ahora**: desde el 28/01/2026 el Reino Unido migró a
    una "single list" sin descarga masiva — solo API de búsqueda POST del SPA
    `search-uk-sanctions-list.service.gov.uk` (seguimiento futuro). Script de
    prueba: `python scripts/probar_sanciones.py 1033688842`.
14. **RTM del vehículo en runt (2026-09-14, sprint 1 parte 2)**: extensión de
    la fuente runt (NO una fuente nueva — mismo cobro/caché/endpoint): el bot
    expande además el panel "Certificado de revisión técnico mecánica y de
    emisiones contaminantes" del acordeón y parsea sus filas (`_RE_RTM`,
    calibrado con MVX48E 2026-09-14: certificado 184404264, CDA LA AGUACATALA,
    vigencia 04/10/2026). `rtm` (vigente + fecha_vigencia recalcuada contra
    hoy en cada hit de caché) + `revisiones[]` (≤5) persisten en el doc y la
    caché; **RTM vencida → fuente ADVERTENCIA** (análogo SOAT, decisión
    2026-09-14). PDF: banner combinado (SOAT VIGENTE · RTM VIGENTE con fecha /
    RTM VENCIDA en rojo), filas RTM en la tabla del vehículo, historial de
    revisiones ≤3 y fila resumen actualizada. Portal: badge ⛔ RTM vencida.
15. **Fuente `delitos_sexuales` — Inhabilidades Ley 1918 (2026-09-14, sprint
    2A)**: consulta por **cédula + FECHA DE EXPEDICIÓN del documento** (DD/MM/
    AAAA — campo NUEVO `fecha_expedicion` del POST, obligatorio 422 si la
    fuente corre; se persiste en el doc) al registro de inhabilidades por
    delitos sexuales contra menores de la DIJIN (Policía). Portal
    `inhabilidades.policia.gov.co:8080`: server-rendered, POST al action del
    form con JSESSIONID; además del par cédula+fecha exige **la empresa
    CONSULTANTE** (`nombreEmpresa`/`nitEmpresa` — la del estudio; el NIT CON
    DV, el bot lo calcula por módulo 11 si falta) y reCAPTCHA v2 (2Captcha
    ~US$0.003). **Trampa (main.js obfuscado, decodificado)**: jquery.validate
    exige `grecaptcha.getResponse()` y habilita el botón con el checkbox de
    términos — pero el submit NATIVO (`form.submit()`) no dispara los
    handlers: stub del getResponse + campo oculto `#captcha` con el token.
    Resultado server-rendered: "…el ciudadano identificado con cédula No. X,
    NO REGISTRA INHABILIDAD…" + hora DIJIN + empresa consultante + fundamento
    legal (Ley 1918, Decreto 753/2019, Leyes 1581/1712). Semáforo: registra →
    **ADVERTENCIA + revisión humana**; no registra → EXITO. **Consulta
    pública de terceros POR DISEÑO** (la ley la creó para que empresas
    verifiquen aspirantes; Ley 2375/2024 la extendió) → va en defaults. Sin
    veredicto legible (ej. fecha de expedición equivocada) → NO_DISPONIBLE
    SIN cachear (anti-envenenamiento). Caché 24 h por (tipo, cédula). Sonda:
    `scripts/probar_delitos_sexuales.py`; bot CLI:
    `python Funciones/bot_delitos_sexuales.py CEDULA FECHA_DD/MM/AAAA EMPRESA NIT`.
16. **Evidencias de consulta — pantallazos en el PDF (2026-09-24)**: cada
    fuente que consulta un portal CON NAVEGADOR (rndc, procuraduria,
    contraloria, delitos_sexuales, policia, runt, simit, sena, bdme, bdme_nit,
    rama_judicial) fotografía el VIEWPORT de la página de resultado al terminar
    la lectura (`captura_jpg` JPEG en el retorno del bot; helper
    `Funciones/captura_evidencia.py` best-effort — un fallo del screenshot
    NUNCA tumba la consulta). Flujo: `seccion["_captura"]` (clave volátil,
    descartada por `_limpiar_seccion`) → se guarda EN el doc de caché 24 h
    (`captura_jpg`: un hit de caché muestra la MISMA evidencia) → al cerrar el
    estudio se sube a GCS privado
    `SeguridadEstudios/{empresa}/{año}/{consulta_id}_captura_{fuente}.jpg` y el
    doc persiste `evidencias{fuente: {gcs_ruta, sha256, tamano, capturado_en}}`
    (la REGENERACIÓN del PDF relee los bytes de GCS). El PDF gana una sección
    final "Evidencias de consulta": intro + una página por fuente (título
    legible + fecha/hora Colombia + sha corto + imagen escalada; blob
    irrecuperable → placeholder honesto). Las fuentes de API/dataset (ofac,
    ofac_nit, onu_ue, rues) no generan captura. Costo ~0,5 s por fuente dentro
    del presupuesto; caché +100-300 KB por fuente. **Contraloría (fix
    2026-09-24 tarde, pedido del usuario)**: el certificado SOLO llega como
    descarga y la página del form no cambia — la captura es la **PRIMERA HOJA
    DEL CERTIFICADO PDF rasterizada a JPEG** (`certificado_pdf_a_jpeg` en
    captura_evidencia, pypdfium2 — nueva dependencia en requirements), con
    fallback al viewport si el rasterizado falla. Rama judicial sigue
    evidenciando la CONSULTA (su resultado pagina por fetch sin cambio de UI).
17. **Fuente SISCONMP — Capacitaciones de Mercancías Peligrosa
    (2026-09-25)**: consulta por **cédula** (tipo doc CC, default del portal)
    el sistema de Mintransporte
    `web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/` (requisito
    de la Resolución 1223 de 2014). Portal MVC server-rendered (select2): el
    submit (`#btnConsultarMD`, un `<span>` con onclick — NO un `<button>`)
    ejecuta **reCAPTCHA v3 INVISIBLE** (`grecaptcha.execute`, action
    `consultar_conductor`) que la PROPIA página resuelve → **SIN 2Captcha,
    costo $0** (verificado headless; NO intentar resolver el v3: la primera
    sonda gastó un solve inútil — el token del solver no participa del flujo).
    La consulta es un AJAX POST a `ConsultarConductor` {TDI, NDI,
    recaptchaToken} que devuelve el ARRAY JSON de capacitaciones. ⚠️ **TRAMPA
    (el bot lee la RESPUESTA AJAX, jamás el DOM)**: el handler `error:` del
    portal muestra el MISMO "No se encontrarón registros" cuando el server
    falla (403/500) que cuando la respuesta es vacía legítima — fiarse del
    div envenenaría la caché con falsos "no registra" (misma disciplina
    anti-envenenamiento de RNDC/Policía, aquí en el bot directamente).
    Resultado: `capacitaciones[]` (tipo CURSO BASICO/titulación NCL, entidad
    certificadora MEN/SENA/…, institución educativa, fechas ISO expedición/
    vencimiento/registro — el JSON trae `/Date(ms)/` UTC convertidas a hora
    Colombia — clase de mercancía, tipo de vehículo) + nombres/apellidos del
    ciudadano según el portal. **Semáforo de VIGENCIA (decisión de negocio
    2026-09-25, análogo SOAT/RTM)**: registra capacitaciones, ≥1 legiblemente
    VENCIDA y ninguna vigente → fuente **ADVERTENCIA**; alguna vigente o
    vacío determinante → EXITO (el vacío es un hecho del registro, no un
    hallazgo adverso — lo interpreta el cliente según el cargo). `vigente` se
    RECALCULA en cada hit de caché contra la fecha de vencimiento. Va en
    defaults (portal público de consulta). Caché 24 h por (tipo="sisconmp",
    cédula). Sonda: `scripts/probar_sisconmp.py`. E2E real 2026-09-25:
    1010213062 = vacío legítimo en 10,8 s + hit de caché.
18. **Fuente Situación Militar — Libreta Militar (2026-09-25)**: consulta por
    **cédula** (CC fijo) el certificado público de estado de situación militar
    del Comando de Reclutamiento y Control Reservas del Ejército Nacional
    (`definicion.libretamilitar.mil.co`). El portal (SPA React) hace un **GET
    directo SIN CAPTCHA** a su generador
    `/generator/v1/api/documento/consulta/estado-situacion-militar?tipoDocumento=cc&numeroIdentificacion=…`
    que responde a requests PURO (molde `bot_rues`, SIN navegador): ~1-2 s,
    **costo $0**. 200 = **certificado PDF** procesado EN MEMORIA con
    pdfplumber (nombre completo, tipo documento, **Estado Tarjeta Militar**
    — visto: RESERVISTA - 1RA/2DA CLASE — y fecha de expedición; el PDF NO se
    persiste: minimización); 400 "El tipoDocumento no coincide con el
    registrado para el ciudadano" = cédula no encontrada con CC →
    **no_registra determinante**. **Evidencia sin navegador**: la 1ª hoja del
    certificado rasterizada (`certificado_pdf_a_jpeg`, patrón CGR) viaja como
    captura — la fuente se registró en `FUENTES_NAVEGADOR` solo por el gate
    de evidencias. **Legalidad**: el propio portal declara la consulta "de
    carácter público y no requiere autorización del titular" (art. 10, Ley
    1581/2012) → va en defaults (Ley 1861/2017, Decreto 977/2018, Ley
    1184/2008). **Semáforo (decisión de negocio 2026-09-25)**: estado
    PENDIENTE/NO DEFINIDO/REMISO/APLAZADO → **ADVERTENCIA** (obligación
    militar vigente = riesgo operativo para conducción); definido
    (RESERVISTA/EXCLUIDO/…) o sin registro → EXITO informativo. Caché 24 h
    por (tipo="situacion_militar", cédula). Sonda:
    `scripts/probar_libreta.py` (descubrió el form SPA, el SVG-data:image que
    NO es captcha y el endpoint). E2E real 2026-09-25: 1010213062 =
    RESERVISTA - 2DA CLASE (EDWIN MISAEL ZARATE PEÑA) + hit de caché +
    99999999 = sin registro.
19. **Memoria de personas consultadas (2026-09-25, transversal — no es fuente)**:
    cada estudio deja registro de la PERSONA evaluada en la colección
    `personas_seguridad` (cédula unique, nombres/apellidos informados,
    `nombre_consultado` VERIFICADO por la cascada PGN→Policía — siempre gana —,
    `fecha_expedicion` si alguna vez se envió, `empresas[]`, `total_consultas`,
    primera/última consulta). Propósito: optimizar la próxima consulta —
    (a) el PORTAL autollena nombres/apellidos/fecha SOLO los campos vacíos con
    el onBlur de la cédula (`buscarPersona` → `GET /personas`, aviso "Persona
    consultada antes por su empresa (N consultas)"); (b) el POST del estudio
    (`_completar_desde_memoria` en rutas) completa lo que el body NO trajo —
    salva el 422 de `delitos_sexuales` (fecha_expedicion), el de `rama_judicial`
    (nombres/apellidos) y alimenta el captcha determinista de la PGN sin caer
    a Gemini; lo del body SIEMPRE gana; (c) las integraciones API se benefician
    igual. **Aislamiento por tenant (decisión del usuario 2026-09-25)**: una
    empresa solo ve/usa la memoria de las personas que ELLA consultó antes
    (filtro `empresas`); otra empresa → `encontrada:false` (no revela que un
    tercero la consultó). Best-effort total: `registrar_consulta_persona`
    traga sus propios errores — la memoria JAMÁS tumba un estudio. Módulo
    `Funciones/personas_seguridad.py` (molde cobro: índices idempotentes al
    importar — unique cedula + (cedula, empresas)). Upsert NO destructivo:
    una consulta sin nombres no borra los que había; ⚠️ `total_consultas` solo
    por `$inc` ($setOnInsert+$inc sobre el mismo campo = operadores en
    conflicto para Mongo). E2E real 2026-09-25: POST de 79882073 SIN
    nombres/apellidos con plan AVANZADO (rama_judicial los exige) → la memoria
    los completó, 201 sin 422 y `total_consultas` 1→2.
    **El nombre de la memoria es el VERIFICADO, no el digitado (fix mismo
    día, reporte del usuario)**: `_nombres_verificados_del_estudio` (rutas)
    prefiere los `nombres`/`apellidos` SEPARADOS que entregan
    situacion_militar y sisconmp (certificados oficiales del Ejército y
    Mintransporte) y los escribe SOBRE lo que el consultante digitó — escribir
    el nombre mal la primera vez NO envenena la memoria. La cascada PGN→
    Policía (un solo string apellidos+nombres, sin split confiable) queda
    solo como `nombre_consultado`. Sin fuente verificada en el plan: la
    memoria conserva lo digitado (y una re-consulta con el nombre corregido
    lo sobreescribe).

## 1. Fuentes y estado

| Fuente | Endpoint | Legalidad del canal | Estado |
|---|---|---|---|
| **RNDC — Historial de Viajes** (Mintransporte) | `GET /seguridad/manifiestos` + dentro del estudio | Portal público de consulta por placa **o cédula** (lo usa cualquier ciudadano). El web service SOAP rechazó el proceso 12 (Vehículo) para las credenciales actuales: RNDC07 sistemático tras ~30 variantes → sondeo en `scripts/probar_rndc_placa.py`. | ✅ Productivo |
| **Procuraduría — Antecedentes disciplinarios** | `GET /seguridad/procuraduria` + dentro del estudio | La **Ley 1238 de 2008** habilita expresamente a entidades públicas **y privadas** a consultar los antecedentes de aspirantes a cargos/contratos. El sistema conserva únicamente el veredicto; no guarda ni publica el certificado del portal. Gratuito. | ✅ Productivo |
| **Contraloría — Antecedentes fiscales (persona natural)** | `GET /seguridad/contraloria` + dentro del estudio | Certificado de Antecedentes Fiscales de la CGR (SIBOR): **público y gratuito**, exigible en contratación (Decreto 2150 de 1995) — consulta de terceros por diseño. reCAPTCHA v2 resuelto por 2Captcha (~US$0.003). El certificado PDF se procesa EN MEMORIA para leer el veredicto + código de verificación; no se guarda ni expone. Veredicto: "no se encuentra reportado como responsable fiscal" (EXITO) / reportado (ADVERTENCIA). Caché por (tipo, cédula). | ✅ Productivo |
| **Policía — Antecedentes judiciales** | `GET /seguridad/policia` + dentro del estudio | ⚠️ Portal de **autoconsulta** (art. 94 Decreto 019/2012); los términos **prohíben la consulta por terceros**. Se consulta con autorización previa del titular (Ley 1581/2012, obligación del solicitante). reCAPTCHA v2 resuelto por 2Captcha (~US$0.003/consulta). **Opt-in por empresa** (NO está en el default). Resultado: leyendas oficiales sin PDF. | ✅ Productivo (opt-in) |
| **RUNT — Vehículo** (placa + cédula del propietario) | `GET /seguridad/runt` + dentro del estudio | Portal **público de consulta ciudadana** (diseñado para consulta por placa con verificación de la cédula del propietario ACTIVO): sin restricción de terceros → va en el default. Captcha de **imagen propio** (PNG embebido data:image) resuelto por 2Captcha `method=base64` (~US$0.001). Devuelve datos del vehículo + SOAT (semáforo de vigencia) + **RTM revisión técnico-mecánica con semáforo (2026-09-14: vencida → ADVERTENCIA, recálculo en cada hit de caché)** + historial de pólizas y revisiones. **Placa obligatoria** en el POST si runt corre; caché por (tipo, cédula, placa). | ✅ Productivo |
| **SIMIT — Comparendos** (placa) | `GET /seguridad/simit` + dentro del estudio | Portal **público del estado de cuenta** de la FCM: consulta ciudadana abierta por placa (un campo acepta cédula O placa) → va en el default. **Sin captcha** (hallazgo de la sonda 2026-09-01: submit directo, ~10 s, costo $0). Devuelve resumen (comparendos/multas/acuerdos/deuda) + **saldo exigible** (`total_a_pagar`: >0 → ADVERTENCIA) + filas de la primera página. La consulta es sobre la PLACA: no es antecedente personal. **Placa obligatoria** en el POST si simit corre; caché por (tipo, placa) con `cedula: None`. | ✅ Productivo |
| **SENA — Certificados de formación** (cédula) | `GET /seguridad/sena` + dentro del estudio | Portal **público Certificado Digital** del SENA (GeneXUS, sin restricción de terceros) → va en el default. Captcha de **imagen propio** (GIF embebido → 2Captcha `method=base64`, ~US$0.001). Devuelve el listado de certificados disponibles (programa, título, tipo Acta/Título/Certificado, fechas ISO). Es FORMACIÓN, no antecedente: **siempre EXITO informativo** — jamás credencial verificada. Solo listado (sin PDFs anexos). Caché por (tipo, cédula). | ✅ Productivo |
| **OFAC — Lista SDN (Lista Clinton)** (cédula) | `GET /seguridad/ofac` + dentro del estudio | Dataset XML oficial de OFAC, sin captcha ni API key. Comparación **exacta** del número de identificación; no usa el fuzzy search por nombre. Coincidencia → ADVERTENCIA + revisión humana obligatoria; ausencia → EXITO informativo. Dataset indexado en memoria (TTL 6 h), caché de resultado por (tipo, cédula) 24 h. | ✅ Productivo |
| **OFAC — Empresas por NIT** | `GET /seguridad/ofac-nit` + dentro del estudio | Fuente `ofac_nit`, totalmente separada de la consulta personal. Coincidencia exacta del NIT completo contra identificadores tributarios del dataset SDN; excluye números de registro genéricos. Coincidencia → ADVERTENCIA + revisión humana. Comparte el dataset en memoria, no duplica los ~29 MB. | ✅ Productivo |
| **RUES — Registro Mercantil (Confecámaras)** | `GET /seguridad/rues` + dentro del estudio | Portal **público de consulta ciudadana** por NIT sin DV → va en el default. **Sin navegador**: la SPA consulta un API Elasticsearch con payloads AES (passphrase embebida "qwerty") que el bot replica con requests (~1-2 s, $0, sin captcha). Devuelve estado de la matrícula (ACTIVA/CANCELADA/…), razón social, cámara, matrícula, renovaciones, tipo societario, CIIU y representante legal. Matrícula ≠ ACTIVA → **ADVERTENCIA**; NIT sin match exacto → EXITO determinante (trampa: NIT inexistente lista TODO, total 10000). Caché 24 h por (tipo, NIT). | ✅ Productivo |
| **ONU/UE — Listas internacionales de sanciones** | `GET /seguridad/sanciones` + dentro del estudio | Fuente `onu_ue` (2026-09-14): agrega la Lista Consolidada ONU (Consejo de Seguridad) + el Financial Sanctions File 1.1 de la UE — datasets XML oficiales indexados en memoria (TTL 6 h, molde bot_ofac, $0, sin captcha). Comparación **exacta** de la cédula contra documentos de identidad (pasaportes/tributarios excluidos). Coincidencia → ADVERTENCIA + revisión humana. Degradación honesta: una lista caída se declara en `listas_no_disponibles` y la consulta sale con la otra. Caché por (tipo, cédula). | ✅ Productivo |
| **SISCONMP — Capacitaciones Mercancías Peligrosas** (cédula) | dentro del estudio | Fuente `sisconmp` (2026-09-25): portal público de Mintransporte (Resolución 1223/2014). **reCAPTCHA v3 invisible que la propia página ejecuta** → sin 2Captcha, $0, ~11 s. El bot lee la **RESPUESTA AJAX** (jamás el DOM: el handler error: muestra el mismo "no registra" del vacío legítimo cuando el server falla). Devuelve capacitaciones (curso básico/titulación NCL, entidad MEN/SENA, institución, fechas ISO, clase MP, tipo vehículo) + nombre del ciudadano. **Semáforo de vigencia** (análogo SOAT/RTM, recalculado por fecha en cada hit de caché): todas vencidas → ADVERTENCIA; alguna vigente o vacío determinante → EXITO. Caché 24 h por (tipo, cédula). | ✅ Productivo |
| **Ejército — Situación militar (libreta militar)** (cédula) | dentro del estudio | Fuente `situacion_militar` (2026-09-25): certificado público del Comando de Reclutamiento (el portal declara la consulta pública sin autorización del titular, art. 10 Ley 1581/2012). **GET directo SIN captcha, SIN navegador** (API del generador de certificados, ~1-2 s, $0): 200 = PDF procesado en memoria (nombre + **Estado Tarjeta Militar**; el PDF no se persiste); 400 "tipoDocumento no coincide" = no_registra determinante. **Semáforo**: PENDIENTE/NO DEFINIDO/REMISO/APLAZADO → ADVERTENCIA (obligación militar vigente = riesgo operativo); definido o sin registro → EXITO. Evidencia = 1ª hoja del certificado rasterizada (patrón CGR). Caché 24 h por (tipo, cédula). | ✅ Productivo |
| Rama Judicial — Procesos por radicado | — | Consulta pública de terceros; sin captcha fuerte. Candidata siguiente (agregar como `bot_rama_judicial.py` + fuente en el orquestador). | 🔜 Pendiente |
| **Policía — Inhabilidades por delitos sexuales contra menores (Ley 1918/2018)** | dentro del estudio | Fuente `delitos_sexuales` (2026-09-14, sprint 2A): consulta de terceros **pública por diseño** (la Ley 1918 la creó para verificación de aspirantes; Ley 2375/2024 la extendió) → va en defaults. Portal `inhabilidades.policia.gov.co:8080` (server-rendered, POST + JSESSIONID): cédula + **fecha de expedición** (campo nuevo `fecha_expedicion` del POST, 422 si falta) + empresa consultante (nombre/NIT con DV, calculado por módulo 11) + reCAPTCHA v2 (~US$0.003; submit NATIVO con stub del getResponse — main.js decodificado). Registra inhabilidad → **ADVERTENCIA + revisión humana**; no registra → EXITO. Sin fórmula legible (fecha equivocada) → NO_DISPONIBLE sin cachear. Caché 24 h por cédula. E2E real 1010213062: NO REGISTRA INHABILIDAD (19:32 DIJIN, GLAMPEROS como consultante). | ✅ Productivo |
| **INPEC — Población privada de la libertad** | — | Portal `inpec.gov.co/registro-de-la-poblacion-privada-de-la-libertad` (buscador oficial de internos, gratuito). **INALCANZABLE desde la red local del usuario** (goto con Chromium real → timeout 90 s; HTTP plano también; WebFetch falla el certificado — posible WAF/geo-bloqueo a IPs de Colombia residenciales). Factibilidad SIN confirmar: reintentar la sonda desde el Shell de Render (IP US) antes de descartar. | 🔜 Bloqueado |
| **Registraduría — Defunciones** | — | **Sin canal público de consulta por terceros**: el web service `ws.registraduria.gov.co` es solo para entidades conveniadas (DIAN, banca) y el portal ciudadano SCCRC (`consultasrc.registraduria.gov.co`) consulta registros de nacimiento/matrimonio del TITULAR con sus propios datos. Que TusDatos lo muestre no lo replica honestamente desde un canal público → **fuera de alcance** del módulo (no fabricar). | ❌ Fuera de alcance |
| **ADRES — Afiliación EPS (BDUA)** | — | **DESCARTADA por decisión del usuario (2026-09-25)** — no reintentar. Sondeada a fondo 2026-09-24: reCAPTCHA **Enterprise invisible** que valida el token contra la API propia de ADRES (`recaptcha.adres.gov.co/check/token`; tokens de 2Captcha enterprise = `valid:false` en 4 variantes) y el execute nativo solo resuelve en Chrome real externo por subprocess; el resultado abre en popup `RespuestaConsulta.aspx?tok=`. Todo el artefacto (PoC, dumps `descargas_adres/`, perfiles de Chrome) fue ELIMINADO a pedido del usuario. | ❌ Descartada |

## 2. Arquitectura de Estudios de Seguridad (v2)

```text
Usuario (portal cliente) — cédula + fuentes elegidas
      |
      v
POST /seguridad/estudios/login  (correo + clave → Bearer JWT 8 h)
      |
      v
POST /seguridad/estudios {cedula, forzar?, empresa_id?, plan_id, placa?,
                          cedula_propietario?, nombres?, apellidos?,
                          fecha_expedicion?, fuentes?}
      |        plan_id manda (fuentes del plan ∩ habilitadas); placa OBLIGATORIA
      |        si runt o simit corren; nombres/apellidos para el captcha PGN;
      |        fecha_expedicion (DD/MM/AAAA) para la Ley 1918;
      |        422 accionables por campo
      +-- 1. Rate limit por empresa (ventana 60 s, 429)
      +-- 2. Fuentes efectivas = fuentes_habilitadas_efectivas (plan es el
      |        gate: default de facto; solo policia opt-in; config.fuentes_
      |        excluidas apaga) ∩ fuentes con plan VIGENTE (por fuente) ∩
      |        fuentes elegidas  [503 si vacías]
      +-- 3. RESERVA POR PLAN (v3.5): 1 CONSUMO por PLAN involucrado al
      |        PRECIO del plan (las fuentes NO multiplican: AVANZADO $3.000
      |        con 4 fuentes = $3.000 la consulta); cupo en LOCKSTEP: 1 unidad
      |        de TODAS las entradas del plan; fallo → COMPENSA las previas
      |        (reversión de cupos + borrado de movimientos) y 402.
      +-- 4. Doc del estudio EN_PROGRESO (trazabilidad aunque todo falle)
      +-- 5. asyncio.gather: fuentes efectivas (demás → DESHABILITADA)
      |        cada fuente: caché 24 h → asyncio.to_thread(bot_sync)
      |        → wait_for(150 s) → 1 reintento (backoff 3 s) → NUNCA aborta
      +-- 6. Estado global honesto (ver §4; DESHABILITADA no cuenta)
      +-- 7. PDF reportlab desde el doc persistido (reproducible)
      +-- 8. GCS PRIVADO: SeguridadEstudios/{empresa}/{año}/{consulta_id}.pdf
      |        (ruta SIN cédula; servido solo por endpoint autenticado)
      +-- 9. Actualiza estudio + eventos de auditoría (consumo por fuente)
      |      └─ >51% de las fuentes corridas fallidas (o ninguna corrió)
      |         → REEMBOLSO automático de TODOS los consumos
      v
201 {consulta_id, estado, fuentes, pdf:{endpoint, sha256}}
```

### Archivos

| Archivo | Responsabilidad |
|---|---|
| `rutas/seguridad_estudios.py` | Endpoints del estudio + admin (empresas/usuarios), rate limit, eventos, aislamiento (`_filtro_empresa`). |
| `Funciones/auth_seguridad.py` | Bearer JWT (reusa secreto/bcrypt de baseusuarios), `actor_actual` que RECARGA de BD, roles. |
| `Funciones/orquestador_estudios.py` | Ejecución de fuentes (parámetro `fuentes` autoritativo), reintentos, estados, minimización de datos, **RNDC vacío sin confirmación → NO_DISPONIBLE sin cachear**. `FUENTES = (manifiestos_rndc, procuraduria, contraloria, delitos_sexuales, policia, runt, simit, sena, sisconmp, ofac, ofac_nit, onu_ue, bdme, bdme_nit, rama_judicial, rues, situacion_militar)` — catálogo canónico (policia NO va en defaults de empresa: opt-in; las demás SÍ). `placa` para runt/simit y `nit` para ofac_nit/bdme_nit/rues. ⚠️ **Toda fuente nueva de Playwright debe registrarse en `_SYNC_ORIGINALES`** (dict literal que compara los wrappers sync: sin registro, `_invocar_playwright` lanza KeyError — visto con contraloria 2026-09-02). **Windows dev (2026-09-02)**: `_invocar_playwright` corre la corutina del bot en un HILO con ProactorEventLoop propio cuando `sys.platform == "win32"` — uvicorn `--reload` pisa la política Proactor con Selector (sin subprocess → Playwright NotImplementedError); el `lifespan` de `main.py` re-fuerza Proactor para los `asyncio.run` de los hilos de los endpoints v1. En Linux (Render) es no-op. |
| `Funciones/pdf_estudio_seguridad.py` | PDF reportlab platypus: portada+QR, tablas, veredicto, trazabilidad, marca de agua, "Página X de Y", reproducible (`rl_config.invariant=1`). Sección Policía semaforizada + sección Vehículo RUNT (semáforo SOAT) + disposiciones legales honestas. |
| `Funciones/storage_seguridad.py` | Subida/descarga/URL firmada de blobs GCS privados. |
| `rutas/seguridad.py` | API v1 por fuente (manifiestos/procuraduría/policia/runt/historico) — auth Bearer; `_normalizar_placa`; caché con clave placa para runt. |
| `Funciones/bot_policia.py` | **2026-08-30** Bot Playwright del portal de antecedentes judiciales (JSF/PrimeFaces 7.0): aceptar términos → cc+cédula → **reCAPTCHA v2 resuelto por 2Captcha** (in.php/res.php polling 5 s) → inyectar token → leer `#form:mensajeCiudadano` (leyendas SU-458 + línea "Apellidos y Nombres" = nombre_consultado). Excepciones: SinCaptchaKey→NO_DISPONIBLE(config), SinResultado→NO_DISPONIBLE(anti-envenenamiento), CaptchaFallido→ERROR(captcha). `_LOCK` serializa (1 solve en vuelo). Dumps en `descargas_policia/`. |
| `Funciones/bot_runt.py` | **2026-08-30** Bot Playwright del portal público del RUNT (SPA **Angular 11 + Material, hash routing** — jamás `networkidle`). Form por `formcontrolname`: `placa` + `documento` (procedencia default NACIONAL=CC, no se toca) + captcha de **imagen propia** (única `data:image/png` del DOM → 2Captcha `method=base64` ~US$0.001) → botón "Consultar Información". Resultado en `#/consulta-vehiculo/consulta/info-vehiculo`: acordeón de mat-expansion-panel con carga perezosa (hay que ABRIR "Póliza SOAT" para que cargue la tabla). Parseo: campos del vehículo por **pares de labels consecutivos** (`_CAMPOS_VEHICULO` + terminadores extra) y pólizas por regex de fila (`_RE_POLIZA`: nro, 3 fechas, aseguradora, tarifa, estado). Requiere **cédula del propietario ACTIVO** de la placa (si no: "Los datos registrados no corresponden con los propietarios activos" → `no_registra=False`, resultado legítimo). `_soat_vigente` compara fecha fin vs hoy Colombia. Excepciones espejo de policía. `_LOCK` serializa. Dumps en `descargas_runt/`. |
| `Funciones/cobro_seguridad.py` | Motor de cobro puro (sin HTTP): `reservar_consumos` (multi-fuente atómico posicional + compensación en cascada), `fuentes_con_plan`, `reembolsar_consumos_consulta` (ERROR global), reembolso idempotente, pagos/ajustes, cierres de período en hora Colombia. **v3.4**: `sincronizar_fuentes_planes` (propagación read-time de fuentes del catálogo, append con guard) y `_entrada_consumible` (vigencia + membresía de fuente). La interfaz vieja `reservar_consumo` (subdoc único) queda como fallback pre-backfill. |
| `Funciones/pdf_cuenta_cobro.py` | Cuenta de cobro mensual PDF (mismo patrón del estudio): tabla de estudios con **cédula completa** (visible desde 2026-08-30), pagos, totales congelados, condiciones, marca de agua. |
| `rutas/seguridad_cobro.py` | API admin `/seguridad/admin/cobro`: planes CRUD, `POST /empresas` (alta de empresas), **`PUT /empresas/{id}/plan` + `DELETE /empresas/{id}/plan/{plan_id}`** (asignación/retiro POR PLAN, acumulable), pagos/ajustes/reembolsos, movimientos, períodos y PDF de cuenta. |
| `Funciones/bot_simit.py` | **2026-09-01** Bot Playwright del estado de cuenta SIMIT de la FCM (SPA hash-routing, `domcontentloaded`, jamás `networkidle`). Form: `#txtBusqueda` (cédula O placa) + submit `#btnNumDocPlaca`; errores de campo en `#messageErrorTxtBusqueda`. **Sin captcha**. Resultado en la MISMA vista: `#resumenEstadoCuenta` (conteos + deuda total), tabla `tr.page-row` con `td[data-label]` (número/tipo/fecha en Tipo, infracción COMPLETA en el `data-content` del popover) y `#valorTotal` (total EXIGIBLE — semáforo). Lee SOLO contenido visible (modales ocultos traen plantilla con la placa). Excepciones: `BotSimitSinResultado`→NO_DISPONIBLE (anti-envenenamiento; `BotSimitError`→ERROR). `_LOCK` serializa. Dumps en `descargas_simit/`. |
| `Funciones/bot_sena.py` | **2026-09-01** Bot Playwright del Certificado Digital del SENA (GeneXUS server-rendered: carga rápida, ids estables). Form: `#vNUMERO_DOCUMENTO` (selects Documento+CC ya default, no se tocan) + captcha de imagen `#vCAPTCHAIMAGE` (GIF data:image → 2Captcha base64) + `#vCAPTCHATEXT` + botón `#CONSULTAR`. Resultado MISMA página: filas `tr#GridceContainerRow_NNNN` con spans `vVAR1`/`vTITULO_OBTENIDO`/`vTIPO`/`vPROGRAMA`/`vFECHA_CERTIFICACION`/`vFECHA_CARGA_PDF`; fechas "09 Febrero, 2013"→ISO (`_fecha_iso_es`). ⚠️ Evidencia de postback procesado = imagen del captcha CAMBIÓ ("No hay resultados" es el estado inicial y también lo del captcha fallido — ErrorViewer "no corresponde con la imagen" → `BotSenaCaptchaFallido`; sin filas ni error ni imagen nueva → `BotSenaSinResultado`). Excepciones espejo de runt. `_LOCK` serializa. Dumps en `descargas_sena/`. |
| `Funciones/bot_ofac.py` | **2026-09-01** Descarga el XML SDN oficial, valida metadatos, construye un índice en memoria por identificación normalizada y devuelve coincidencias exactas con UID, nombre, tipo de documento y programas. Refresco configurable (6 h default), SHA-256 del dataset para auditoría y sin automatización de navegador. |
| `Funciones/bot_rues.py` | **2026-09-03** Bot del RUES **SIN navegador** (requests puro): replica el cifrado CryptoJS AES-256-CBC (OpenSSL Salted__, passphrase `SEGURIDAD_RUES_SECRET_KEY` default "qwerty") del API `elasticprd.rues.org.co` que consume la SPA. `POST /query` con `Status:["ACTIVA","CANCELADA"]` + coincidencia EXACTA de `numero_identificacion` (padding de ceros; un NIT inexistente lista total 10000) → `POST /api/Expediente/DetalleRM` (estado, fechas, societario, CIIU) → `ConsultFacultadesXCamYMatricula` (representante legal en HTML con `&nbsp;`; best-effort). Estado con letras espaciadas ("A C T I V A") normalizado. Excepciones espejo simit (`BotRuesError`/`BotRuesSinResultado`). ~1-2 s, $0. CLI: `python Funciones/bot_rues.py 901923029`. |
| `Funciones/bot_sanciones.py` | **2026-09-14** Bot de las listas ONU/UE (molde `bot_ofac`, sin navegador): descarga y parsea la Lista Consolidada ONU (`INDIVIDUAL_DOCUMENT` tipo `National Identification Number`) y el FSF 1.1 de la UE (`identification` con `identificationTypeCode="id"` y `subjectType` persona), indexa el documento NORMALIZADO → coincidencias con `lista`/`programas`/`referencia`. Metadatos POR LISTA (fecha, total personas, SHA-256). Degradación honesta: una lista caída → `listas_no_disponibles`; ambas → `BotSancionesError`. TTL 6 h compartido. CLI: `python scripts/probar_sanciones.py <cedula>`. |
| `Funciones/bot_sisconmp.py` | **2026-09-25** Bot Playwright del SISCONMP (capacitaciones MP por cédula, Mintransporte): MVC server-rendered; llena `#txtNDI` (el `#cboTDI` NO se toca — CC default), clic en `#btnConsultarMD` y **captura la RESPUESTA AJAX** (`expect_response` de `ConsultarConductor`: status 200 + JSON array — el DOM miente). El reCAPTCHA **v3 invisible lo ejecuta la propia página** (gratis; JAMÁS resolverlo con 2Captcha — el token del solver no participa). Fechas `/Date(ms)/` UTC → ISO hora Colombia (`_fecha_iso`); `_capacitacion_vigente` calcula el semáforo (recalculado por el orquestador en cada hit de caché). 200 no-lista o non-200 → excepciones espejo simit (`BotSisconmpError`/`BotSisconmpSinResultado`). `_LOCK` serializa. Dumps en `descargas_sisconmp/`. CLI: `python Funciones/bot_sisconmp.py CEDULA`. |
| `scripts/probar_sisconmp.py` | **2026-09-25** Sonda del portal SISCONMP (CÉDULA): `--solo-formulario` (estructura), `--headless`; inventario de inputs/botones/captcha + dumps paso a paso en `descargas_sisconmp/`. Con ella se descubrió: el "botón" es un `<span>` con onclick (no un `<button>`), el reCAPTCHA v3 nativo (la sonda gastó 1 solve inútil antes de darse cuenta), el AJAX POST con JSON array y la trampa del handler error: (mismo mensaje para fallo y vacío). |
| `Funciones/bot_situacion_militar.py` | **2026-09-25** Bot de la situación militar (libreta militar, Ejército) **SIN navegador** (molde `bot_rues`): GET al generador de certificados con requests puro (~1-2 s, $0, sin captcha). 200=PDF → pdfplumber en memoria (nombres, `Estado Tarjeta Militar`, fecha de expedición "a los N días del mes de X de YYYY"→ISO); 400 "no coincide"=no_registra determinante; sin estado legible → `BotSituacionMilitarSinResultado` (anti-envenenamiento). La evidencia es la 1ª hoja del certificado rasterizada (`certificado_pdf_a_jpeg`, patrón CGR). El PDF NO se persiste (minimización). `_LOCK` serializa la sesión HTTP. CLI: `python Funciones/bot_situacion_militar.py CEDULA`. |
| `scripts/probar_libreta.py` | **2026-09-25** Sonda del portal de situación militar (CÉDULA): SPA React (form `#tipoDocumento` cc/ti + `#numeroDocumento`; el botón Consultar queda disabled sin tipo doc). Con ella se descubrió el endpoint GET del generador (respondiendo a requests puro) y que el `data:image` del DOM es un SVG decorativo, NO un captcha. Dumps en `descargas_libreta/`. |
| `scripts/probar_sanciones.py` | **2026-09-14** Sonda standalone del bot ONU/UE (descarga viva de ambos datasets e imprime el resultado). Con ella se verificó: XML ONU con `dateGenerated`, la trampa del token UE (`webgate` anónimo → EU Login) y el censo de tipos de documento indexables (ONU 151 IDs · UE 221 IDs de personas). |
| `Funciones/bot_delitos_sexuales.py` | **2026-09-14 (sprint 2A)** Bot de la consulta de inhabilidades Ley 1918 (DIJIN): server-rendered, POST al action con JSESSIONID; campos CC+nuip+`fechaExpNuip`+`nombreEmpresa`+`nitEmpresa` (DV calculado módulo 11 si falta); reCAPTCHA v2 2Captcha; **submit NATIVO** con stub de `grecaptcha.getResponse` + campo oculto `#captcha` (main.js decodificado: jquery.validate no intercepta el submit nativo). Veredicto "NO REGISTRA/REGISTRA INHABILIDAD" (el NO primero), hora DIJIN y empresa consultante del resultado. La página debe traer la cédula consultada (garantía de correspondencia). Excepciones espejo contraloría (`_LOCK`, dumps `descargas_delitos/`). CLI: `python Funciones/bot_delitos_sexuales.py CEDULA FECHA EMPRESA NIT`. |
| `scripts/probar_delitos_sexuales.py` | **2026-09-14** Sonda del portal Ley 1918: `--solo-formulario` (estructura) + consulta viva (1 solve). Con ella se descubrió: el form POST con JSESSIONID, la exigencia de fecha de expedición + empresa consultante + NIT con DV, el reCAPTCHA y la trampa del botón deshabilitado (jquery.validate/grecaptcha). |
| `scripts/probar_inpec.py` | **2026-09-14** Sonda del buscador de internos del INPEC (`--solo-formulario`). Bloqueada desde la red local (goto timeout 90 s con Chromium real): reintentar desde el Shell de Render (IP US). |
| `scripts/probar_rues.py` | **2026-09-03** Sonda del API del RUES (NIT): consulta viva + `--estructura` (contrato descifrado) + `--descubrir-endpoints` (abre el portal con Playwright UA real — el CDN bloquea HeadlessChrome — recorre las pestañas del detalle y descifra TODOS los payloads `elasticprd`; dumps en `descargas_rues/`). |
| `scripts/probar_simit.py` | **2026-09-01** Sonda del portal SIMIT (PLACA): `--solo-formulario` (estructura), `--token`, `--headless`; inventario de inputs/botones/captchas + dumps paso a paso en `descargas_simit/`. Con ella se descubrió: campo único cédula-o-placa, botón `#btnNumDocPlaca` sin texto accesible, ausencia de captcha, resumen/tabla/totales del resultado y la placa ZZZ999 (105 comparendos históricos 1999-2000 con $0 exigible — calibró el semáforo). |
| `scripts/habilitar_fuente_simit.py` | **OBSOLETO (2026-09-01 tarde)**: agregaba `simit` a `config.fuentes_habilitadas` de las empresas existentes (`$addToSet`). Desde `fuentes_habilitadas_efectivas` NO hace falta — el plan es el gate. Inofensivo si se corre. |
| `scripts/probar_sena.py` | **2026-09-01** Sonda del portal SENA (CÉDULA): `--solo-formulario` (estructura + guarda el captcha GIF), `--token` (captcha a mano), `--headless`; inventario de inputs/botones + dumps paso a paso en `descargas_sena/`. Con ella se descubrió: GeneXUS con ids `v*`, captcha GIF embebido, grid `GridceContainerRow_NNNN`, "No hay resultados" como estado inicial y el ErrorViewer del captcha fallido. |
| `scripts/habilitar_fuente_sena.py` | **OBSOLETO (2026-09-01 tarde)**: idem simit/runt — el plan es el gate. |
| `Funciones/bot_rndc2.py`, `Funciones/bot_procuraduria.py` | Bots Playwright. **PGN captcha en TRES vías (2026-09-01)**: aritmética (regex) → **derivada del DOCUMENTO o del NOMBRE** (`_resolver_captcha_documento`: "dos últimos dígitos del documento a consultar" con la cédula del formulario; "¿cuál es el primer nombre/apellido de la persona que está consultando?" con los `nombres`/`apellidos` que el PORTAL ahora pide cuando el plan incluye procuraduria — deterministas, sin red; antes iban a Gemini sin contexto y fallaban) → conocimiento general (Gemini con cédula+nombres como contexto; `maxOutputTokens` 8192 — en Gemini 3.x el thinking cuenta en el presupuesto). Sin pista y pregunta de nombre → reintento del orquestador (pregunta nueva por intento). **PGN rediseñó su resultado (2026-09-02)**: el veredicto llega INLINE en la misma página (sección "Datos del ciudadano": "El ciudadano no presenta antecedentes" / "presenta antecedentes") — ya NO hay `#btnDescargar` ni PDF que descargar; el bot espera `.datosConsultado, #btnDescargar` (spinner "Consultando por favor espere..." tarda >45 s) y parsea las fórmulas nuevas ANTES de caer a las viejas del PDF (chequeando el "no" primero). Los regex viejos quedan para compatibilidad. **Sin PDF ya no hay `nombre_certificado`**: el bot extrae **`nombre_consultado`** de los `<span>` de `.datosConsultado` (el texto plano los CONCATENA — "JHOAMORLANDOAMAYATOVAR"; por spans sale con espacios) y el PDF hace fallback `nombre_certificado || nombre_consultado` (orquestador lo cachea y lo sirve en hit de caché). **`?nocache=1`** (hallazgo del usuario 2026-09-02): el goto usa `PORTAL_URL + "?nocache=1"` — evita la copia cacheada del iframe del formulario que a veces venía sin sesión y el postback no respondía. **Nueva variante de captcha** (2026-09-02): "las dos/tres primeras letras del primer nombre/apellido" — determinista en `_resolver_captcha_documento` con los `nombres` del POST. Sonda: `scripts/sondar_procuraduria_hoy.py`. |
| `Funciones/bot_contraloria.py` | **2026-09-02** Bot Playwright del Certificado de Antecedentes Fiscales — Persona Natural (CGR). Form en IFRAME WebForms `cfiscal.contraloria.gov.co/Certificados/CertificadoPersonaNatural.aspx`: `#ddlTipoDocumento` (value "CC") + `#txtNumeroDocumento` + submit `#btnBuscar`. **reCAPTCHA v2** (sitekey leído del iframe de Google, fallback por rotación) resuelto por 2Captcha `userrecaptcha`; token INYECTADO en el textarea `g-recaptcha-response` del iframe. Marca por JS los TRES radios de la encuesta oculta OBLIGATORIA del validationGroup (sin ellos el postback se bloquea en silencio). El postback es PARCIAL async y el certificado llega como DESCARGA: `expect_download` → pdfplumber en memoria → veredicto ("NO SE ENCUENTRA REPORTADO COMO RESPONSABLE FISCAL" → no_registra True; "SE ENCUENTRA REPORTADO…" → False) + `codigo_verificacion`. Sin veredicto y sin PDF → `BotContraloriaSinResultado` (anti-envenenamiento). Excepciones espejo de policía. `_LOCK` serializa. Dumps en `descargas_contraloria/`. |
| `scripts/probar_contraloria.py` | **2026-09-02** Sonda del portal CGR (CÉDULA): `--solo-formulario` (estructura), `--token` (reCAPTCHA resuelto a mano), `--headless`; inventario de inputs/botones/captchas + dumps paso a paso en `descargas_contraloria/`. Con ella se descubrió: el iframe WebForms, el value "CC" del select, el reCAPTCHA v2, la encuesta oculta obligatoria (validadores `reqExpectativa/reqOportunidad/reqUtilidad` ocultos que bloquean el submit), el postback parcial async y la descarga `{cedula}.pdf`. |
| `scripts/inicializar_seguridad_estudios.py` | Seed: empresa INTEGRA, índices (incl. TTL), roles a ADMIN existentes. Dry-run default. `fuentes_habilitadas` SIN policia (opt-in). |
| `scripts/probar_policia.py` | **2026-08-30** Sonda del portal Policía: `--solo-formulario` (valida selectores sin gastar), `--token TOKEN` (token manual), headed default; dumps paso a paso en `descargas_policia/`. Con esto se DESCUBRIÓ la página de resultado (`formAntecedentes.xhtml`). |
| `scripts/probar_runt.py` | **2026-08-30** Sonda del portal RUNT (PLACA CEDULA): `--solo-formulario` (estructura + guarda la imagen del captcha), `--token` (captcha leído a mano), `--headless`; inventario de inputs/botones/captchas + dumps en `descargas_runt/`. Con esto se descubrió: form Angular Material, captcha de imagen embebida, página de resultado `info-vehiculo` con acordeón perezoso y el rechazo "no propietario activo". |
| `scripts/habilitar_fuente_runt.py` | **OBSOLETO (2026-09-01 tarde)**: idem — el plan es el gate (`fuentes_habilitadas_efectivas`). |
| `scripts/migrar_planes_por_fuente.py` | **v3.1** Backfill `plan`→`planes` (una entrada por fuente del plan viejo, cupo clonado por fuente). Dry-run default, `--ejecutar` real. Correr DESPUÉS de desplegar el backend nuevo (el código tolera ambas formas). |
| **Frontend** `src/Paginas/AdminSeguridadP/` + `ApiPedidos/seguridadCobro.tsx` | Panel admin (perfil ADMIN, tarjeta en LoginUsuario; header de app ancho completo con menú usuario/cerrar sesión): 4 pestañas. Empresas: **fila expandible ▸** (resumen = nombres de planes + badge cantidad; despliegue = tarjetas por plan con cupo/barra/botón ✕ FaTimes), botón **"+ Asignar plan"** (plan + cupo o sin tope; acumulable) y **"+ Nueva empresa"** (NIT+nombre, slug derivado). |
| **Frontend** `src/Paginas/PortalSeguridadP/` + `ApiPedidos/seguridadCliente.tsx` | Portal cliente INDEPENDIENTE de la Torre de Control: login propio (perfil CLIENTE_ESTUDIOS), tarjeta **"Sus planes"** (por plan, no por fuente), **ACORDEÓN de planes en "Nueva consulta"** (2026-08-30: el input fijo de cédula DESAPARECIÓ; cada plan se abre con click y su cuerpo trae el formulario con los campos que ESE plan requiere — cédula siempre, **placa SOLO si incluye runt**; sin preselección automática; validación de placa AAA123/AAA12A y re-chequeo de cupo al submit), lottie detective con mensajes rotativos, PDF, historial (columna Placa condicional). **2026-08-30 tarde: campo opcional "Cédula del propietario"** (solo si el plan incluye runt; vacía = el conductor es el dueño) y aviso en el resultado cuando propietario ≠ evaluado. **Botón "Certificado Procuraduría"** (tarjeta de resultado + historial, `abrirAnexoPgn`): descarga el anexo OFICIAL de la PGN del estudio vía `GET /{id}/procuraduria.pdf` (solo visible si `anexo_procuraduria` existe). `crearEstudio(cedula, fuentes?, planId?, placa?, cedulaPropietario?)`. |

## 3. Multi-tenant: empresas, usuarios y roles

- Colección **`empresas_seguridad`**: `{nombre, slug, nit?, logo_url?, activo, config:{retencion_dias(730), aislamiento_usuario(false), consultas_por_minuto(10), fuentes_habilitadas[] (⚠️ 2026-09-01 tarde: SOLO persiste las opt-in — el gate real es el PLAN, ver `fuentes_habilitadas_efectivas` en §4b), fuentes_excluidas?[] (apaga una fuente puntual por empresa)}}`. Alta desde el panel (`POST /seguridad/admin/cobro/empresas`) o seed.
- **`baseusuarios` reutilizada** (sin duplicar usuarios): +2 campos opcionales `empresa_id` y `rol_seguridad`. Obligatorios en runtime para el módulo.
- **Perfiles (separación cliente externo vs interno, 2026-08-29)**:
  - **`CLIENTE_ESTUDIOS`** = cliente de una empresa en el portal de Estudios de Seguridad. REQUIERE `empresa_id_seguridad` + `rol_seguridad` al crearlo (GestionUsuarios muestra el selector solo con ese perfil; el backend lo exige con 422). Entra a `/LoginUsuario` → **aterriza directo en `/PortalSeguridad`** (patrón FINANCIERO): jamás ve la Torre de Control ni la tarjeta Seguridad. Sin clientes FMC.
  - **`SEGURIDAD`** = personal interno de Integra (aprobación de placas en `/revision`). NO darle a clientes externos.
- Roles derivados en `Funciones/auth_seguridad.py`:

| Rol | Origen | Alcance |
|---|---|---|
| `ADMIN_INTEGRA` | perfil baseusuarios `ADMIN` | Todas las empresas; CRUD empresas; asignar usuarios; estadísticas globales. Al crear estudio debe pasar `empresa_id` en el body. |
| `ADMIN_EMPRESA` | `CLIENTE_ESTUDIOS` o `SEGURIDAD` + `rol_seguridad=ADMIN_EMPRESA` | Su empresa: consultar, ver todo, regenerar PDF, estadísticas. |
| `CONSULTADOR` | `CLIENTE_ESTUDIOS` o `SEGURIDAD` (sin rol o CONSULTADOR) | Su empresa: crear estudios, ver/descargar. Con `aislamiento_usuario:true` solo ve SUS estudios. |

- **Aislamiento**: todo `find/count` filtra por `empresa_id` del actor. Acceso
  cross-tenant → **404** (no revela existencia) + evento `acceso_denegado`.
- El token lleva `empresa_id`/`rol` solo como hint; **la autorización siempre
  relee la BD** (desactivar usuario/empresa revoca el token al instante).

## 4. Estados (nunca éxito con fuente fallida)

Por fuente: `EXITO` · `ADVERTENCIA` (Procuraduría con PDF pero veredicto
ilegible; **RUNT con SOAT vencido** — decisión de negocio 2026-08-30;
**SIMIT con saldo exigible** (`total_a_pagar` > 0, el agregado del portal) —
decisión de negocio 2026-09-01) · `NO_DISPONIBLE` (timeout/portal caído) ·
`ERROR` (fallo funcional del bot) · `DESHABILITADA` (excluida del cálculo).

| Global | Regla |
|---|---|
| `COMPLETADA` | TODAS las fuentes EXITO. |
| `COMPLETADA_CON_ADVERTENCIAS` | Todas terminaron, ≥1 ADVERTENCIA, ninguna fallida. |
| `PARCIAL` | ≥1 terminó bien y ≥1 falló. |
| `ERROR` | Todas fallaron. |

**RUNT y el semáforo RTM** (2026-09-14): el panel "Certificado de revisión
técnico mecánica y de emisiones contaminantes" se EXPANDE junto al de SOAT
(carga perezosa del acordeón Angular) y su primera fila es la revisión
vigente — `_RE_RTM` parsea (tipo, expedición, vigencia, CDA, vigente_portal,
nro. certificado, información consistente). La fuente sale `ADVERTENCIA`
cuando la `fecha_vigencia` de la revisión vigente es < hoy (Colombia) — el
vehículo no está al día en revisión; igual que el SOAT, el semáforo se
RECALCULA en cada hit de caché. **El "paz y salvo"/multas de CONDUCCIÓN que
muestra TusDatos NO existe en el portal de vehículo** (es de la consulta del
conductor con login): documentado como fuera de alcance de esta fuente. El
panel "Tarjeta de Operación" existe pero solo trae datos en vehículos de
carga — PENDIENTE calibrar su parseo con una placa de la flota (ver §12).

**RUNT y el semáforo SOAT** (2026-08-30): la fuente sale `ADVERTENCIA` cuando
la póliza más reciente tiene `fecha_fin_vigencia < hoy` (Colombia) — el
vehículo no está asegurado; el estudio global queda `COMPLETADA_CON_ADVERTENCIAS`.
El estado se RECALCULA en cada hit de caché (`_estado_runt` + `_soat_vigente`):
un SOAT vigente ayer puede estar vencido hoy y la caché no debe congelar el
veredicto. "Placa sin información" (`no_registra=True`) y "cédula no es del
propietario activo" (`False`) son respuestas determinantes del portal → EXITO
(el tri-estado es sobre la PLACA, no sobre la persona: jamás presentarlo como
"limpio").

**SIMIT y el semáforo de saldo exigible** (2026-09-01): la fuente sale
`ADVERTENCIA` cuando el **agregado** `total_a_pagar` del portal es > 0
("Total a pagar (N): $X") — hay deuda vigente exigible. La deuda HISTÓRICA
sin saldo exigible NO advierte (verificado con ZZZ999: 105 pendientes de
1999-2000 con agregado $0 — prescritos; las FILAS pueden traer "valor a
pagar" > 0 aunque el agregado diga $0: el semáforo se fía del agregado).
"Sin comparendos ni multas" es EXITO determinante; el portal NO distingue
placa inexistente de placa limpia, y la consulta es sobre la PLACA — jamás
un antecedente personal de la persona evaluada.

**SISCONMP y el semáforo de vigencia (2026-09-25)**: la fuente sale
`ADVERTENCIA` cuando el conductor REGISTRA capacitaciones de Mercancías
Peligrosas, al menos una está legiblemente VENCIDA (`fecha_vencimiento` < hoy
Colombia) y NINGUNA vigente — análogo SOAT/RTM; el estado se RECALCULA en cada
hit de caché. Alguna capacitación vigente → EXITO. Sin capacitaciones
registradas (`no_registra=True`) → EXITO determinante informativo: la ausencia
es un hecho del registro (Resolución 1223/2014), no un hallazgo adverso — el
cliente lo interpreta según el cargo a evaluar. Fechas ilegibles (vigente
None) → EXITO sin inventar advertencia.

**Situación militar y el semáforo de obligación vigente (2026-09-25)**: la
fuente sale `ADVERTENCIA` cuando el Estado Tarjeta Militar certificado por el
Ejército contiene PENDIENTE / NO DEFINIDO / REMISO / APLAZADO — el conductor
tiene una obligación militar sin definir (riesgo operativo: retención en
puestos de control). Situación DEFINIDA (RESERVISTA 1RA/2DA CLASE, EXCLUIDO…)
o ciudadano sin registro con CC → EXITO informativo/determinante.

**SENA — sin semáforo (2026-09-01)**: la fuente es SIEMPRE EXITO
informativo (decisión de negocio). El SENA reporta certificados de FORMACIÓN
disponibles (títulos, actas, certificados de aprobación/notas): registrar N
o ninguno son respuestas determinantes del portal — no existe la "mala"
noticia y el listado JAMÁS se presenta como credencial verificada (el
informe declara que no constituye verificación de títulos).

HTTP 201 = el estudio se creó y auditó; el resultado real va en `estado` del body.

## 4b. Cobro postpago (v3): flujo del dinero

```text
ADMIN_INTEGRA (tú)                      EMPRESA CLIENTE (María)
  │ crea PLAN ($/consulta, fuentes)       │
  │ asigna PLAN(ES) acumulables + cupo ──►│ "AVANZADO (RNDC+Proc) + PRO (RNDC)"
  │   (PUT /empresas/{id}/plan; puede     │ "RNDC: 70 disponibles · Proc: 50"
  │    asignar varios: conviven por fuente)│
  │                                       │ POST /seguridad/estudios {cedula, plan_id}
  │                                       │   └─ el PLAN define las fuentes a correr
  │                                       │     (sus entradas ∩ habilitadas)
  │                                       │   └─ RESERVA ATÓMICA POR FUENTE
  │                                       │     (planes.<i>.cupo_*; FIFO por
  │                                       │      asignado_en, el plan ELEGIDO al
  │                                       │      frente; fallo en la N compensa)
  │                                       │   └─ ejecuta fuentes (paralelo)
  │       ◄── N movimientos CONSUMO ──────│   └─ PDF + GCS (1 por fuente corrida,
  │                                       │      precio CONGELADO de SU entrada)
  │                                       │      └─ si ERROR global:
  │                                       │         REEMBOLSO automático de TODOS
  │ fin de mes: CERRAR período            │
  │   └─ congela totales (precio snapshot)│
  │   └─ genera cuenta de cobro PDF       │
  │ la empresa paga por fuera (transfer.) │
  │ REGISTRAR PAGO ──► si cubre el total ►│ período PAGADA
```

Reglas clave:

- **Planes acumulables (v3.2)**: `empresas_seguridad.planes` admite VARIAS entradas por fuente `{plan_id, plan_nombre, fuente, precio_congelado, cupo_autorizado, cupo_disponible, cupo_consumido, asignado_por/en}`. Asignar AVANZADO y PRO a RNDC = 2 entradas; en la MISMA fuente los cupos suman. `cupo_autorizado: null` = **sin tope** (un plan sin tope hace TODA la fuente sin tope).
- **EL PLAN SE COBRA POR CONSULTA (v3.5, 2026-08-30)**: 1 CONSUMO por PLAN involucrado en la consulta, al `precio_congelado` del plan — las fuentes que incluye NO multiplican el precio (AVANZADO $3.000 con 4 fuentes = consulta de $3.000; v3.1 cobraba por fuente y la misma consulta salía $12.000). El cupo cuenta CONSULTAS del plan y se descuenta en **LOCKSTEP**: 1 unidad de TODAS las entradas del plan (sus fuentes drenan juntas → "quedan N" es coherente en portal, admin y `/cupo`, que muestran el valor de la entrada, NO la suma). El movimiento lleva `fuentes` (corridas) y `fuentes_tocadas` (entradas descontadas, para que el REEMBOLSO devuelva exacto — incluso si la fuente se retiró del catálogo después). Movimientos pre-v3.5 (por fuente) se reembolsan con la lógica vieja.
- **Consumo FIFO con elección (v3.3)**: por fuente se ordenan las entradas por `asignado_en` y se gasta la primera con cupo. Si el POST trae `plan_id`, esa entrada pasa al frente (elección explícita > FIFO; si se agota cae a la siguiente). El 402 nombra los planes agotados.
- **Consumo atómico**: `find_one_and_update({"_id", "planes.<i>.plan_id": pid, "planes.<i>.cupo_disponible": {"$gt": 0}}, $inc)` posicional con condición de plan (protege la carrera) + releer posiciones frescas por plan. **Compensación en cascada**: si la reserva N falla, se revierten los cupos y se BORRAN los movimientos ya insertados.
- **El plan define las fuentes (v3.3)**: con `plan_id`, las fuentes a correr = entradas de ese plan ∩ habilitadas. Elegir PRO corre SOLO manifiestos_rndc aunque la empresa tenga AVANZADO con ambas. Las fuentes no cubiertas quedan `DESHABILITADA` en el doc (presente en el dict pero no corrida ni cobrada — al auditar mirar `estado`, no la presencia de la clave).
- **El catálogo manda: propagación automática de fuentes (v3.4, 2026-08-30)**: editar `fuentes_incluidas` de un plan YA NO requiere reasignar. Las fuentes NUEVAS se materializan solas en cada empresa en su próxima consulta (`sincronizar_fuentes_planes` en `POST /estudios`, `/cupo` y `GET /admin/cobro/empresas`): entrada append-only con `$push` + guard `$not $elemMatch` (concurrencia segura; **nunca cambiar por `$set` del array** — perdería `$inc` posicionales concurrentes), `precio_congelado` = precio ACTUAL del catálogo, cupo clonado de la hermana más antigua del plan, `cupo_consumido 0`, `asignado_en` = ahora (ventana de vigencia completa), `asignado_por: "sync_catalogo"`. Nada existente se toca (nada se resetea). **Fix 2026-09-01**: el skip de "ya existe" es por PAR (plan_id, fuente), igual que el guard del update — por fuente SOLA, la entrada de un plan se saltaba cuando OTRO plan de la empresa ya cubría esa fuente (BASICO·simit impedía materializar AVANZADO·simit y el acordeón del portal no mostraba simit en AVANZADO). Fuente RETIRADA del catálogo: la entrada sobrevive (historial de cobro; el reembolso le sigue devolviendo cupo) pero deja de ser consumible al instante (`_entrada_consumible` = vigencia + membresía; la fuente sintética `"todas"` del subdoc viejo nunca se bloquea) — `/cupo` no la ofrece y `GET /empresas` la marca `retirada: true` (el panel la pinta gris, su cupo fuera de barras). Sin propagación write-time en `PATCH /planes` (escritura masiva de duración incierta): el evento registra el diff `+/-` de fuentes como auditoría.
- **EL PLAN ES EL GATE — `fuentes_habilitadas_efectivas` (2026-09-01 tarde, pedido del usuario)**: agregar una fuente a un plan del catálogo queda disponible para TODA empresa con ese plan **INMEDIATAMENTE**, sin migrar configs ni correr scripts `habilitar_fuente_*` (quedaron OBSOLETOS — el paso se olvidó con simit y con sena: plan BASICO editado con sena salía DESHABILITADA en GLAMPEROS porque su config persistido no la listaba, ES-85DB8C510811). Semántica nueva (`fuentes_habilitadas_efectivas` en el orquestador, usada en `crear_estudio` y el fallback de `ejecutar_estudio`): las fuentes DEFAULT (portales públicos: rndc, procuraduría, runt, simit, sena) corren de facto para toda empresa; `config.fuentes_habilitadas` SOLO persiste las OPT-IN (`FUENTES_OPT_IN = {policia}` — exige presencia explícita, sin cambio); `config.fuentes_excluidas` (opcional) apaga una fuente puntual por empresa (pisa al whitelist). Retro-compatible: lo listado en el whitelist viejo sigue habilitado.
- **Precio congelado**: cada CONSUMO cobra el `precio_congelado` de la ENTRADA (asignación), no el precio actual del catálogo. Reasignar el mismo plan resetea cupo y congela el precio nuevo; cambiar el catálogo NO toca asignaciones vigentes.
- **Criterio de reembolso (2026-09-01): >51% de las fuentes CORRIDAS fallidas** (`mayoria_fuentes_fallidas` en el orquestador). Se devuelve la consulta solo si más de la mitad de las fuentes que corrieron quedaron NO_DISPONIBLE/ERROR (o si no corrió ninguna — falla catastrófica); con la mitad o menos caídas **se cobra** — que un portal esté caído no invalida el resto del informe (decisión de negocio: antes solo reembolsaba el ERROR global = 100% caídas y un PARCIAL 2/3 caídas cobraba). El ERROR global (todas) sigue reembolsando (100% > 51%). `reembolsar_consumos_consulta` devuelve TODOS los consumos (v3.5 reintegra a las `fuentes_tocadas` del plan). Anti-doble: flag `reembolsado` condicionado + índice único parcial `{consulta_id, consumo_id, tipo:REEMBOLSO}`.
- **402/503 accionables**: sin plan para ninguna fuente / vencido / cupo agotado por fuente — cada mensaje dice qué hacer.
- **ADMIN_INTEGRA con empresa sin planes** = consumo EXENTO único (monto 0, trazable). Válvula de despliegue: `SEGURIDAD_COBRO_EXIGIR_PLAN=false` deja consultar sin plan durante la transición.
- **Compatibilidad pre-backfill**: empresa con subdoc `plan` viejo → `reservar_consumos` delega en la lógica de subdoc único. El propio `PUT .../plan` hace el backfill implícito al asignar.
- **Períodos**: `periodo = "YYYY-MM"` en hora **Colombia (UTC−5)** (cruce de medianoche UTC no cambia el mes). Solo meses pasados se cierran → el reembolso automático nunca choca con un cierre. Un **PAGO sí puede aplicarse a un período cerrado** (si cubre el total, el período pasa a PAGADA solo). AJUSTES/reembolsos manuales en cerrado → 409 "reábrela". Con cobro por consulta (v3.5), "unidades" cuenta CONSULTAS (1 por plan involucrado).

## 5. Colecciones (BD `integra`)

| Colección | Uso |
|---|---|
| `empresas_seguridad` | Tenants. Índices unique nombre y slug. **v3.1**: array `planes` con una entrada POR FUENTE `{plan_id, plan_nombre, fuente, precio_congelado, cupo_autorizado (None=sin tope), cupo_disponible (desnorm, $inc atómico posicional), cupo_consumido, asignado_por/en}`. **v3.4**: el array puede CRECER solo por sync read-time (`asignado_por: "sync_catalogo"`) cuando el catálogo agrega fuentes a un plan; las entradas de fuentes retiradas sobreviven no consumibles. El subdoc `plan` viejo sobrevive solo hasta el backfill (`scripts/migrar_planes_por_fuente.py`). |
| `estudios_seguridad` | Doc maestro del estudio: consulta_id único (`ES-`+12 hex), codigo_verificacion, empresa/usuario denormalizados (**ObjectId nativo** — los filtros comparan ObjectId; bug de string corregido 2026-08-29), cédula, **nombres/apellidos del consultante (2026-09-01, captcha PGN — SIN tildes, mayúsculas; NO es el nombre verificado)**, **placa (solo si la consulta incluyó runt o simit; None si no)**, **`vehiculos[]` (2026-08-30): `[{placa, cedula_propietario, propietario_es_evaluado}]` — el RUNT valida contra el DUEÑO de la placa que puede ≠ conductor evaluado; array listo para varios vehículos (hoy 1); `placa` top-level queda como espejo de `vehiculos[0]` (compat docs previos)**, nombre_consultado, estado, timestamps, fuentes (viajes tope 500, mensaje ≤300 chars, SIN texto_pdf/html — minimización; `fuentes.runt` = {datos_vehiculo, soat, polizas ≤10, no_registra, placa}), pdf {gcs_ruta, sha256, version, historial}, anexo_procuraduria, anexo_runt (canal listo: el portal no genera PDF consolidado), `retencion_expira_en` (**índice TTL real**), auditoria {ip, user_agent}. |
| `eventos_seguridad` | Log por evento: login_exitoso/fallido, estudio_creado, fuente_error, pdf_generado/regenerado/descargado, estudio_visto, verificacion_qr, acceso_denegado, empresa_creada/actualizada, usuario_asignado, y de cobro: plan_creado/actualizado/asignado, pago_registrado, reembolso, periodo_cerrado/reabierto/pagado, ajuste_manual, consumo_registrado, sin_cupo. Cédula siempre enmascarada. |
| `consultas_seguridad` | Caché global 24 h por (tipo, cédula) — los datos del tercero son iguales para cualquier empresa; docs nuevos añaden empresa_id/usuario_id como metadato y `mensaje_portal` (confirmación RNDC). **Una caché RNDC con 0 viajes y sin "Consulta realizada" se ignora al leer y jamás se escribe** (fix anti-envenenamiento 2026-08-29). Docs `tipo:"policia"`: `{no_registra, mensaje (leyenda SU-458), nombre_consultado}` — sin leyenda ni nombre jamás se cachea (anti-envenenamiento análogo). **Docs `tipo:"runt"` (2026-08-30): clave (tipo, cédula, PLACA)** — una cédula puede tener varios vehículos; `_buscar_cache` con `tipo=runt` y sin placa NUNCA hace hit (guard anti cross-contaminación; el índice `idx_seg_tipo_cedula` sigue sirviendo: el prefijo (tipo, cédula) acota y la placa filtra entre pocos docs). Shape: `{placa, no_registra, mensaje, datos_vehiculo{}, soat{numero, aseguradora, fechas, estado_portal, vigente}, polizas[]}` — sin datos ni pólizas ni mensaje jamás se cachea. **Docs `tipo:"simit"` (2026-09-01): clave (tipo, PLACA) con `cedula: None`** — la cédula del evaluado jamás participó en la consulta (el portal solo recibe la placa); `_buscar_cache` con `tipo=simit` y sin placa NUNCA hace hit (mismo guard que runt). Shape: `{placa, no_registra, mensaje, total_comparendos, total_multas, total_acuerdos, total_deuda, total_a_pagar, comparendos[] (≤20)}` — sin totales ni filas ni mensaje jamás se cachea. Índice `idx_seg_tipo_placa` (tipo+placa+consultado_en) además del viejo por cédula. **Docs `tipo:"sena"` (2026-09-01): clave (tipo, cédula)** — el default del módulo, sin placa. Shape: `{no_registra, mensaje, total_certificados, certificados[] (≤20: registro, titulo, tipo, programa, fecha_certificacion, fecha_firma ISO)}` — sin certificados, sin no_registra determinante y sin mensaje jamás se cachea (anti-envenenamiento; sirve el índice `idx_seg_tipo_cedula`). **Docs `tipo:"contraloria"` (2026-09-02): clave (tipo, cédula)** — el default del módulo. Shape: `{no_registra, mensaje, codigo_verificacion}` — sin veredicto jamás se cachea (anti-envenenamiento). **Docs `tipo:"delitos_sexuales"` (2026-09-14): clave (tipo, cédula)** — el default del módulo. Shape: `{no_registra, mensaje, fecha_consulta (la que estampa la DIJIN), empresa_consultante}` — sin veredicto jamás se cachea (anti-envenenamiento; cubre la fecha de expedición equivocada). **Docs `tipo:"onu_ue"` (2026-09-14): clave (tipo, cédula)** — el default del módulo. Shape: `{aplica, no_registra, total_coincidencias, coincidencias[{lista, uid, nombre, programas, referencia, tipo_documento, numero_documento, pais_documento}], listas{ONU/UE:{fecha_publicacion, total_registros_lista, sha256_dataset}}, listas_no_disponibles[]}` — sin metadatos de NINGUNA lista jamás se cachea (anti-envenenamiento análogo OFAC). **Docs `tipo:"sisconmp"` (2026-09-25): clave (tipo, cédula)** — el default del módulo. Shape: `{no_registra, mensaje, apellidos, nombres, total_capacitaciones, capacitaciones[] (≤20: tipo_capacitacion, nombre, entidad_certificadora, institucion_educativa, fecha_expedicion, fecha_vencimiento, fecha_registro ISO, clase, descripcion_clase, tipo_vehiculo, vigente)}` — sin capacitaciones, sin no_registra determinante y sin mensaje jamás se cachea (anti-envenenamiento; cubre el caso del portal re-renderizando por token rechazado). **Docs `tipo:"situacion_militar"` (2026-09-25): clave (tipo, cédula)** — el default del módulo. Shape: `{no_registra, mensaje, nombres, apellidos, nombre_completo, estado_tarjeta_militar, fecha_expedicion}` — sin estado legible jamás se cachea (anti-envenenamiento; el PDF del certificado NO se persiste: minimización). |
| `planes_seguridad` | **v3** Catálogo armable: `{nombre unique, descripcion, precio_por_estudio COP, fuentes_incluidas ⊆ FUENTES (ej. "solo procuraduría"), vigencia_dias? (None = sin vencimiento, cuenta desde la ASIGNACIÓN), activo}`. DELETE = soft. Al asignar por fuente se valida que el plan incluya la fuente. |
| `api_keys_seguridad` | **2026-08-30** API keys de integración: `{empresa_id, nombre, prefijo visible (sek_ab12cd…), hash_sha256 (índice UNIQUE — la clave plana solo existe al crearla y se muestra UNA vez), activo, scopes ["estudios:crear","estudios:leer"] (una key NUNCA es admin), creado_por/en, ultimo_uso_en (throttle 1 write/min), revocada_en}`. Índices `idx_apikey_hash` unique + `idx_apikey_empresa_activa` (en `asegurar_indices_cobro`). Lookup por hash en cada request → revocar es instantáneo. |
| `movimientos_cobro_seguridad` | **v3.1** UNA colección con tipo y signo: `CONSUMO (+1u,+cop, precio snapshot, consulta_id, FUENTE) · REEMBOLSO (−1u,−cop, consumo_id, fuente) · PAGO (0,−cop, fecha_pago/metodo/referencia) · AJUSTE (0,±cop, motivo)` — **N CONSUMO por consulta (uno por fuente corrida)**. Campo `periodo "YYYY-MM"` (hora Colombia). Índice único **parcial** `{consulta_id, consumo_id, tipo:REEMBOLSO}` (v2; el viejo `{consulta_id, tipo}` se dropea solo) → doble reembolso imposible. |
| `periodos_cobro_seguridad` | **v3** Cierres: `{empresa_id+periodo unique, totales CONGELADOS {consumos, unidades, subtotal/reembolsos/ajustes/pagos/total_cop}, estado PENDIENTE_COBRO\|PAGADA, pdf {gcs_ruta...}, cerrado_por/en}`. Solo meses PASADOS se cierran. |
| `personas_seguridad` | **2026-09-25** Memoria de personas consultadas (capa 19): `{cedula unique, nombres, apellidos, nombre_consultado (verificado), fecha_expedicion?, empresas[] (gate de visibilidad por tenant), total_consultas, primera/ultima_consulta_en}`. Índices `idx_persseg_cedula` (unique) + `idx_persseg_cedula_empresa`. Write en cada POST del estudio (upsert no destructivo); read en `_completar_desde_memoria` y `GET /personas`. |

## 6. Autenticación

- `POST /seguridad/estudios/login {correo, clave}` → `{access_token, expira_en_min (SEGURIDAD_TOKEN_MINUTES, 8 h), usuario{...rol, empresa}}`. Reusa `_buscar_baseusuario_activo` (bcrypt dual-mode).
- **API keys (2026-08-30)**: `Authorization: Bearer sek_…` en el MISMO header que el
  JWT — `actor_actual` enruta por prefijo (`_actor_de_api_key`: lookup por hash
  SHA-256 → actor CONSULTADOR de la empresa con canal="api", usuario "API: nombre",
  sin usuario_id). Clave inválida/revocada/empresa inactiva = mismo 401 genérico
  (anti-enumeración). El cliente elige plan_id igual que el portal (`POST /estudios`),
  descarga PDFs y ve su historial (`GET /estudios`) con las mismas reglas de cobro.
  Timeout recomendado para integraciones: 120 s (el estudio es síncrono, 3–60 s).
- Perfiles admitidos: SEGURIDAD (con empresa) y ADMIN. Errores: 401 credenciales, 403 inactivo/perfil sin acceso/sin empresa.
- **Los endpoints v1** (`manifiestos`, `procuraduria`, `policia`, `historico`) también requieren Bearer (migrados 2026-08-29; ya NO aceptan `usuario` por query). `/policia` sin key de captcha configurada → **503 accionable**.
- ⚠️ `JWT_SECRET` compartido con baseusuarios: fijar valor fuerte en Render (el default inseguro se reporta CRITICAL al arrancar).

## 7. Los PDFs

**Estudio de seguridad** (`Funciones/pdf_estudio_seguridad.py`):
(1) portada con badge de estado, datos de la persona, resumen por fuente
(cabecera azul con letra **blanca**) y **QR de verificación** →
`{SEGURIDAD_VERIFICACION_URL_PUBLICA}?consulta={consulta_id}&codigo={codigo}`.
La ruta pública responsive `/VerificarEstudio` del frontend presenta al auditor
autenticidad, estado, empresa, fecha, cédula enmascarada, fuentes consultadas y
SHA-256 del PDF; nunca publica hallazgos personales. Sin la variable nueva se
conserva como fallback el endpoint JSON histórico del backend;
(2) manifiestos RNDC (tabla ≤8 columnas con celdas Paragraph-wrap y anchos
ponderados, tope 300 filas); (3) veredicto Procuraduría destacado + datos del
certificado + anexo; (3b) **veredicto Policía** semaforizado (verde "NO
REGISTRA ANTECEDENTES JUDICIALES" / rojo "REGISTRA REQUERIMIENTO JUDICIAL" /
ámbar no concluyente) + leyenda oficial + nombre según el portal + fila en el
resumen y en reintentos (2026-08-30); (3c) **Vehículo — RUNT** (2026-08-30):
banner semaforizado por SOAT (verde VIGENTE con fecha de vencimiento / rojo
VENCIDO / ámbar sin póliza registrada / neutro-ámbar "PLACA SIN INFORMACIÓN" o
"cédula no es del propietario activo" — el tri-estado es sobre la PLACA, nunca
"limpio"), tabla de datos del vehículo (marcados los campos que trajo el
portal), póliza vigente + historial ≤5 pólizas, "Placa consultada (RUNT)" en
los datos de la persona y fila en resumen/reintentos. **Propietario ≠ evaluado
(2026-08-30 tarde)**: fila "Propietario del vehículo" en los datos de la
persona (cédula ENMASCARADA + "es/distinta de la persona evaluada"), badge
ámbar al inicio de la sección RUNT cuando el dueño ≠ evaluado, fila "Cédula
consultada (propietario)" cuando el portal rechaza por propiedad (explica con
qué cédula se validó — no leerlo como antecedente) y fila "Vehículo /
propietario" en trazabilidad. Docs previos (sin `vehiculos[]`): se asume que
el propietario era el evaluado (comportamiento real de entonces); **(3d)
Comparendos — SIMIT (2026-09-01)**: banner semaforizado por saldo EXIGIBLE
(ámbar "COMPARENDOS PENDIENTES — SALDO $X" / neutro "SIN SALDO EXIGIBLE —
registra N antecedentes históricos" / verde "SIN COMPARENDOS NI MULTAS REGISTRADAS"
REGISTRADAS"), tabla de totales + detalle ≤10 filas de la primera página del
portal, "Placa consultada (SIMIT)" en los datos de la persona cuando runt no
corrió; **el informe SOLO muestra las fuentes que CORRIERON (2026-09-01)**:
helper `_corrio(f)` (estado ∉ {None, DESHABILITADA}) gatea resumen, secciones
y reintentos — una fuente fuera del plan (o clave ausente en docs viejos) ya
no aparece como fila fantasma "no consultada"; (4) trazabilidad (usuario, empresa,
IP/UA, tiempos, intentos, sha256); (5) disposiciones legales — **Policía con
base legal honesta: autoconsulta del titular (Decreto 019/2012 art. 94),
consulta bajo autorización previa del titular (Ley 1581), obligación del
solicitante; SIN cita de norma habilitante de terceros** (no existe
equivalente de la 1238) + **leyenda oficial SU-458 COMPLETA al pie de la
sección Policía (2026-09-01, constante `LEYENDA_SU458_POLICIA`)** + **RUNT
honesto: Portal Público de Consulta Ciudadana, datos como reportados en la
fecha, vigencia del SOAT informativa (SIN citar normas)** + **SIMIT honesto:
estado de cuenta público de la FCM, consulta sobre el VEHÍCULO que no
constituye antecedente personal, saldos informativos** + Leyes 1238/1581 para
Procuraduría — **un párrafo por fuente corrida** (las excluidas por el plan
no citan); **(8) Evidencias de consulta (2026-09-24, patrón TusDatos)**:
sección final — solo si el doc tiene `evidencias` — con una página por fuente
de navegador (título legible, fecha/hora Colombia de la captura, sha256 corto
e imagen JPEG del viewport del portal escalada al ancho útil; blob
irrecuperable → placeholder textual honesto). Los bytes se leen de GCS
privado en cada generación → la regeneración los conserva; marca de agua y
"Página X de Y" se aplican igual (docs viejos sin `evidencias`: sección
omitida, PDF idéntico al de siempre); (6) **marca de
agua diagonal en todas las páginas**: `{empresa} | {usuario} | {fecha} | {consulta_id}`;
(7) footer "Página X de Y". Horas en hora Colombia (UTC−5). **TODAS las
fechas del informe van en dd/mm/aaaa (2026-09-04)**: `_fecha_legible`
normaliza lo que trae cada fuente (ISO/slash/US mm-dd-aaaa/datetime ISO) y
deja intacto el texto no fechable. **Todas las
celdas de texto largo usan `Paragraph` con `splitLongWords`** (fix
2026-08-29: SHA-256, nombres de anexos y mensajes del portal se desbordaban
de las tablas — los strings crudos en `Table` no hacen wrap).

**Cuenta de cobro mensual** (`Funciones/pdf_cuenta_cobro.py`): membrete,
empresa+NIT, período, tabla de estudios del mes (fecha, consulta, **cédula
completa** — visible desde 2026-08-30 para que el cliente cruce con sus
registros, estado, plan, precio snapshot), pagos aplicados,
ajustes/reembolsos, resumen financiero con total, condiciones de pago, marca
de agua `CUENTA DE COBRO | empresa | período`. Totales CONGELADOS del cierre
(regenerar no recalcula). GCS: `SeguridadCobro/{empresa_id}/{YYYY-MM}/cuenta_cobro_{período}.pdf`.

Ambos reproducibles byte a byte (`rl_config.invariant=1`).

## 8. Endpoints

Prefijo `/seguridad/estudios` (+`/seguridad/admin`). Bearer salvo `verificar`.

| Método y ruta | Roles | Descripción |
|---|---|---|
| `POST /login` | público | Login del módulo (JSON correo+clave) → Bearer 8 h. |
| `POST /cambiar-clave` | los 3 (humano) | **2026-09-25** Cambio de clave AUTENTICADO (menú del avatar del portal): `{clave_actual, clave_nueva}` (mín. 6) — verifica contra `baseusuarios` con el dual-mode de `Funciones/claves` y guarda la nueva hasheada. API keys → 403. Eventos `clave_cambiada`/`clave_cambio_fallido`. |
| `POST /recuperar/solicitar` | público | **2026-09-25** "Olvidé mi contraseña" paso 1: `{correo}` → código de 6 dígitos de un solo uso (hash + exp 15 min en baseusuarios) enviado por Resend (branding seguriDatia). Respuesta NEUTRA (anti-enumeración). Reusa env `RESEND_API_KEY`/`MAIL_FROM` de aut2 — ojo: los endpoints de aut2 apuntan a la colección `usuarios` (Torre) y NO sirven para los usuarios del portal (viven en `baseusuarios`). |
| `POST /recuperar/confirmar` | público | Paso 2: `{correo, codigo, clave_nueva}` — verifica hash+exp, máx 8 intentos por código (luego se invalida), cambia la clave y deja el código inservible. Evento `clave_restablecida`. |
| `GET /seguridad/policia?cedula=` | SEGURIDAD/ADMIN | **2026-08-30** Antecedentes judiciales (leyendas SU-458, sin PDF; trae `nombre_consultado`). Sin key de captcha → 503. |
| `GET /seguridad/runt?placa=&cedula=` | SEGURIDAD/ADMIN | **2026-08-30** Consulta ciudadana de vehículo del RUNT (portal público; la cédula debe ser del propietario ACTIVO). Devuelve `datos_vehiculo`, `soat` (con `vigente`), `polizas` (historial). Caché 24 h por (cédula, placa). Sin key de captcha → 503. |
| `GET /seguridad/simit?placa=` | SEGURIDAD/ADMIN | **2026-09-01** Estado de cuenta de comparendos SIMIT de la placa (portal público FCM, sin captcha). Devuelve totales (`total_comparendos/multas/acuerdos`, `total_deuda`, `total_a_pagar` exigible) + `comparendos[]` (primera página). Caché 24 h por placa (sin cédula). 502 si el portal no responde. |
| `GET /seguridad/sena?cedula=` | SEGURIDAD/ADMIN | **2026-09-01** Certificados de formación del SENA por cédula (portal público, captcha de imagen). Devuelve `no_registra`, `total_certificados` y `certificados[]` (programa, título, tipo, fechas ISO). Solo listado (sin PDFs anexos). Caché 24 h por cédula. 503 sin key de captcha, 502 si el portal no responde. |
| `GET /seguridad/rues?nit=` | SEGURIDAD/ADMIN | **2026-09-03** Registro Mercantil del RUES por NIT sin DV (API público, sin captcha, ~1-2 s). Devuelve `estado_matricula` (ACTIVA/CANCELADA/…), `razon_social`, `nit_con_dv`, cámara, matrícula, fechas de matrícula/renovación/cancelación, tipo societario, `ciiu{}` y `representantes[]`. Caché 24 h por NIT. 422 NIT inválido (6-10 dígitos), 502 sin resultado legible, 503 API caído/passphrase rotada. |
| `GET /seguridad/sanciones?cedula=` | SEGURIDAD/ADMIN | **2026-09-14** Coincidencia exacta de la cédula en las listas agregadas ONU+UE (datasets oficiales indexados en memoria; sin captcha, $0). Devuelve `aplica`, `total_coincidencias`, `coincidencias[{lista, uid, nombre, programas, referencia}]`, metadatos `listas{ONU,UE}` (fecha, registros, sha256) y `listas_no_disponibles[]` si alguna no pudo descargarse. Caché 24 h por cédula. 503 si ambas listas caen. |
| `POST /token` | público | Variante OAuth2 formulario para el botón Authorize de Swagger. |
| `GET /me` | los 3 | Identidad/rol/empresa del token. |
| `GET /cupo` | los 3 | **v3.3**: `{vigente, fuentes:[consolidado por fuente: plan_nombre "A + B", cupos sumados, un ilimitado → fuente sin tope], planes:[individuales elegibles: {plan_id, nombre, precio, fuentes[], cupos} — solo cupo>0 o sin tope], consumo_mes}`. **Contrato del portal cliente.** **v3.4**: sincroniza fuentes nuevas antes de consolidar y EXCLUYE entradas de fuentes retiradas del catálogo. |
| `POST /` | los 3 | Crear consulta `{cedula, forzar?, empresa_id?, plan_id, placa?, cedula_propietario?}` → 201. **v3.3: `plan_id` OBLIGATORIO desde el portal** — el plan define las fuentes a correr (sus entradas ∩ habilitadas) y el precio del consumo (elegido > FIFO; si el plan se agota cae al siguiente). 422 si el plan no está asignado o no cubre ninguna fuente habilitada. `fuentes` (legacy) sigue soportado: lista explícita sin plan. **`placa` obligatoria (422 accionable, nombra RUNT y/o SIMIT) si entre las fuentes a correr está `runt` o `simit` (2026-09-01); se normaliza (AAA123/AAA12A/AA1234) y se persiste en el doc si runt o simit corrieron**. **`cedula_propietario` opcional (SOLO runt, 2026-08-30 tarde): cédula del DUEÑO de la placa cuando el conductor evaluado NO es el propietario (el portal del RUNT solo responde con la cédula del propietario ACTIVO); vacía → se usa la cédula consultada; la caché y el bot van con la cédula del propietario. Con solo simit el campo se IGNORA (simit no usa cédula)**. **`nombres`/`apellidos` (2026-09-01, procuraduria): el portal los PIDE obligatorios cuando el plan incluye procuraduria (el captcha de la PGN pregunta por el nombre de la persona consultada — "¿cuál es su primer nombre?"); el endpoint los normaliza SIN tildes y en mayúsculas (`_normalizar_nombre`), se persisten en el doc y viajan al bot como pista determinista del captcha. Opcionales en el body para no romper integraciones API: sin ellos esas variantes caen a Gemini/reintento**. **Consume 1 unidad por fuente corrida** (402/503 sin plan/vencido/agotado por fuente; ERROR global → reembolso automático de todos los consumos). |
| `GET /` | scope empresa | Listado paginado (filtros cedula/estado/usuario/fechas). |
| `POST /iniciar` | los 3 | **2026-09-25** Arranca el estudio y devuelve `{consulta_id}` AL INSTANTE (202): la ejecución corre en background (`asyncio.create_task` sobre `_finalizar_estudio`); mismas validaciones/cobro/cascada que el POST síncrono (que queda INTACTO como contrato de las integraciones API). Si el usuario cierra la ventana el estudio igual termina y queda en su historial (más robusto que el POST bloqueante, que abortaba con la conexión). |
| `GET /{consulta_id}/progreso` | los 3 | **2026-09-25** Avance en vivo para el polling del portal: `{total, hechas, pct, fuentes{nombre: estado|null}}` desde el registro en memoria `_PROGRESO` del orquestador (single-instance; caído/reiniciado → 404 y el portal sigue sondeando); al terminar TODO (incluido el PDF) agrega `estudio` con la respuesta completa. Aislamiento por empresa (404 neutro); ADMIN_INTEGRA ve todos. |
| `GET /personas?cedula=` | los 3 | **2026-09-25** Memoria de personas: datos que la EMPRESA del actor ya envió/verificó en consultas previas de esa cédula (`{encontrada, nombres, apellidos, nombre_consultado, fecha_expedicion, total_consultas, ultima_consulta_en}`) para autollenar el formulario del portal. Aislamiento: solo personas que SU empresa consultó; otra → `encontrada:false`. ADMIN_INTEGRA sin empresa pasa `empresa_id`. |
| `GET /{consulta_id}` | scope empresa | Doc completo. |
| `POST /{consulta_id}/completar` | los 3 | **2026-09-25** Vuelve a consultar SOLO las fuentes que quedaron NO_DISPONIBLE/ERROR del estudio (parámetros del doc: cédula, placa, propietario, nombres, NIT, fecha_expedición), mergea sobre lo que ya respondió, recalcula el estado global y **re-emite el PDF (mismo consulta_id, versión +1)**. **Cobro (decisión del usuario 2026-09-25)**: consulta ya cobrada (≤51% caídas) completa GRATIS; consulta REEMBOLSADA (>51%) se vuelve a cobrar (`_consumos_del_completado`: reserva de las fuentes originales con el plan de los CONSUMO originales como preferido; 402 ANTES de tocar portales) y si el completado tampoco entrega (>51% sobre el estado FINAL) se reembolsa otra vez (`cobrado:false` en la respuesta). Tope `completar_intentos` 5 (429). 422 si no hay pendientes, 409 si está EN_PROGRESO. Respuesta incluye `fuentes_completadas`, `fuentes_pendientes` y `cobrado`. |
| `GET /{consulta_id}/pdf` | scope empresa | PDF inline (`?descarga=true` attachment, `?url_firmada=true` 15 min). Auditado. |
| `GET /{consulta_id}/procuraduria.pdf` | scope empresa | Certificado oficial original (solo consultas en vivo). |
| `GET /{consulta_id}/runt.pdf` | scope empresa | **2026-08-30** Anexo del RUNT si el portal llegara a entregarlo (hoy no genera PDF consolidado: canal listo). |
| `POST /{consulta_id}/pdf/regenerar` | ADMIN_EMPRESA/INTEGRA | Reconstruye desde el doc SIN tocar portales; versiona y guarda historial. |
| `GET /verificar/{consulta_id}?codigo=` | público | QR. `{valido, estado, fecha, empresa, cédula enmascarada}`; nunca 404 (anti-enumeración). |
| `GET /estadisticas/estudios?desde&hasta` | ADMIN_* | Totales por estado/usuario, tiempo promedio, hits de caché. |
| `GET /eventos/auditoria?consulta_id&evento` | ADMIN_* | Log de eventos con filtros. |
| `GET/POST /seguridad/admin/empresas` · `PATCH .../{id}` | ADMIN_INTEGRA | CRUD de empresas. |
| `GET /seguridad/admin/usuarios?empresa_id=` · `PATCH .../{usuario_id}/empresa` | ADMIN_INTEGRA | Asignar empresa/rol a usuarios. |

### 8b. Endpoints de cobro (`/seguridad/admin/cobro`, todos ADMIN_INTEGRA)

| Método y ruta | Descripción |
|---|---|
| `GET/POST /planes` · `PATCH/DELETE /planes/{id}` | CRUD de planes (DELETE = soft, avisa `empresas_afectadas`). Cambiar precio NO toca consumos pasados. Cambiar `fuentes_incluidas` se PROPAGA solo (v3.4): nuevas fuentes aparecen en cada empresa en su próxima consulta; retiradas dejan de ser consumibles al instante (el evento audita el diff). |
| `POST /empresas` `{nit, nombre, slug?}` | **Alta de empresa** desde el panel (slug derivado del nombre; config default de INTEGRA; nace sin planes). 409 si nombre/NIT/slug duplicados. |
| `GET /empresas` | Empresas con `planes[]` (entradas por (plan,fuente) con `entrada_id` único: cupo/ilimitado/consumido/vence/**retirada**), consumo del mes, saldo pendiente. **Sincroniza fuentes nuevas (v3.4) antes de listar.** |
| `GET /empresas/{id}/api-keys` · `POST …/api-keys` · `DELETE …/api-keys/{key_id}` | **2026-08-30 API keys de integración**: listar (sin hash; prefijo + ultimo_uso + estado) · crear `{nombre}` → 201 con la clave COMPLETA (única vez; en BD solo hash; máx 10 activas por empresa) · revocar (soft activo=False; el próximo request de esa integración da 401). Eventos `api_key_creada/revocada`. |
| `PUT /empresas/{id}/plan` `{plan_id, cupo_autorizado\|null}` | **v3.2 Asignación POR PLAN (acumulable)**: agrega una entrada por CADA fuente que el plan incluye. Si el mismo plan ya estaba en una fuente, la actualiza (cupo reset, `actualizado:true`). Otro plan distinto en la misma fuente → conviven (consumo FIFO). Null = sin tope. Backfill implícito. |
| `DELETE /empresas/{id}/plan/{plan_id}` | **v3.2** Retirar el plan completo (todas sus fuentes). 422 si ya tiene consumos → cerrar período primero o asignar cupo 0. |
| `GET /dashboard` | KPIs: empresas, cupo global, consumo del mes, cartera. |
| `POST /empresas/{id}/pagos` | Registrar pago (auto-aplica al cierre pendiente más antiguo; si cubre el total → PAGADA). |
| `POST /empresas/{id}/ajustes` | Ajuste ±COP con motivo (409 en período cerrado). |
| `POST /empresas/{id}/reembolsos` | Reembolso manual por consulta_id con motivo (mismo motor idempotente). |
| `GET /movimientos` | Línea de tiempo del dinero con filtros (empresa/tipo/período/fechas/consulta). |
| `GET /periodos` · `POST /periodos/cerrar` | Cierres de mes (solo pasados; congela totales + genera PDF). |
| `POST /periodos/{id}/reabrir` · `PATCH /periodos/{id}` | Reabrir con motivo · marcar PAGADA/pendiente. |
| `GET /periodos/{id}/pdf` · `POST .../pdf/regenerar` | Cuenta de cobro PDF (genera on-demand si faltó). |

### 8c. Frontend

| Ruta | Quién | Qué |
|---|---|---|
| `/AdminSeguridad` (integrapp-next) | perfil ADMIN (tarjeta "Estudios de Seguridad" en LoginUsuario) | Header de app ancho completo (logo+menú usuario+cerrar sesión, patrón /indicadores) + 4 pestañas. **Cada pestaña tiene su propia ruta** (`/AdminSeguridad/{empresas,planes,movimientos,cuentas}`; raíz = empresas; mirror con `history.replaceState` nativo — patrón /revision — para que el F5 aterrice en la pestaña). **Empresas**: fila expandible **▸** (resumen: nombres de planes + badge; despliegue: tarjetas por plan con fuentes+cupo/barra+✕ FaTimes; entradas de fuente retirada del catálogo en gris, cupo fuera de barras), botones **"+ Asignar plan"** (acumulable, cupo o sin tope), **"+ Nueva empresa"**, Pago/Ajuste/Cerrar mes · Planes (CRUD) · Movimientos (filtros, reembolsos, export) · Cuentas de cobro (PDF, pagada, reabrir). |
| `/PortalSeguridad/{panel,nueva-consulta,historial}` (integrapp-next) | cliente de la empresa, perfil **CLIENTE_ESTUDIOS** (login propio, independiente de la Torre de Control) | **REDISEÑO 2026-09-25 (patrón dash-board.tusdatos.co)**: sidebar fija blanca con módulos (Panel / Nueva consulta / Historial; activo = píldora teal + filo), topbar con breadcrumb + pill "Consultas disponibles: N" + avatar-menu (Volver al inicio / **Cambiar contraseña** / Cerrar sesión), tarjetas blancas radius 12 sobre gris #F4F6F8, acento teal #00A5B5, logo seguriDatia; login split teal/blanco con "¿Olvidó su contraseña?" (código 6 dígitos por correo). Módulo Panel: 3 KPIs (disponibles, consumo del mes "sept 2026", total) + planes con barra de progreso. Nueva consulta: **ACORDEÓN por plan** (cédula/NIT; placa si runt/simit; propietario solo runt; fecha expedición solo Ley 1918; **SIN campos de nombres** — el backend los detecta solo, y si falla pide escape hatch con reintento automático) → detective con mensajes rotativos + ding → semáforo con badges SOLO de fuentes corridas + Swal criterio >51% → PDF + Completar pendientes. Historial: tabla con columna Pendientes (badge SOLO número + botón ↻ de completar) y **drill-down expandible** con el listado; colapso a tarjetas ≤640px; móvil ≤900px sidebar → drawer. Rutas por módulo con carpeta estática + replaceState nativo (F5 aterriza en el módulo). |
| `/GestionUsuarios` (integrapp-next) | perfil ADMIN | Con perfil **CLIENTE_ESTUDIOS** aparecen "Empresa de Estudios de Seguridad" (requerida) y "Rol en Seguridad" (CONSULTADOR/ADMIN_EMPRESA); sin checkboxes de clientes FMC para ese perfil. |

## 9. Variables de entorno

| Variable | Default | Uso |
|---|---|---|
| `JWT_SECRET` | ✅ ya fuerte en Render y .env | Compartido con baseusuarios. (El default inseguro se reporta CRITICAL al arrancar.) |
| `SEGURIDAD_TOKEN_MINUTES` | `480` | Vida del token del módulo. |
| `SEGURIDAD_ESTUDIOS_BUCKET` / `_CARPETA` | `integrapp-privado` / `SeguridadEstudios` | GCS **privado** (publicAccessPrevention enforced) de informes. NUNCA usar el bucket público `integrapp` aquí. |
| `SEGURIDAD_ESTUDIOS_URL_PUBLICA` | `https://integrappi-dvmh.onrender.com` | Base del QR de los PDF. |
| `SEGURIDAD_VERIFICACION_URL_PUBLICA` | URL del frontend + `/VerificarEstudio` | Destino visual y responsive de los QR nuevos; si falta, usa el JSON histórico. |
| `SEGURIDAD_INTENTOS_FUENTE` | `2` | Intentos totales por fuente (1 reintento). |
| `SEGURIDAD_BACKOFF_MS` | `3000` | Pausa entre intentos. |
| `SEGURIDAD_TIMEOUT_FUENTE_S` | `150` | Presupuesto de pared por fuente (global). |
| `SEGURIDAD_PROCURADURIA_TIMEOUT_S` | `150` | Presupuesto de pared SOLO para procuraduría (sobrescribe el global vía `TIMEOUTS_FUENTE_S`/`_timeout_fuente`). Historial: 150→300 s (2026-09-04, el postback PGN tarda >45 s)→90 s (2026-09-24)→**150 s (2026-09-24 tarde, pedido del usuario: punto medio)**. Navegación+captcha+60 s de espera caben con margen; el reintento completo NO cabe (solo fallos rápidos reintentan). | |
| `SEGURIDAD_PROCURADURIA_ESPERA_MS` | `60000` | **2026-09-24** Espera del bot PGN por el postback del resultado (`.datosConsultado/#btnDescargar`; era literal 180 s): navegación+captcha+60 s ≈ 85-90 s caben en el presupuesto de la fuente. |
| `SEGURIDAD_CAPTURA_CALIDAD` | `70` | **2026-09-24** Calidad JPEG de las capturas de evidencia de los bots (viewport, ~100-300 KB). |
| `SEGURIDAD_RETENCION_DIAS` | `730` | Default de retención (TTL) si la empresa no define. |
| `SEGURIDAD_MAX_VIAJES_PDF` / `_DOC` | `300` / `500` | Topes de filas en PDF / persistidas. |
| `SEGURIDAD_MAX_COMPARENDOS_DOC` | `20` | **2026-09-01** Tope de comparendos SIMIT persistidos por sección (el total global vive en `total_*`). SIMIT no necesita keys de captcha (el portal no tiene). |
| `SEGURIDAD_CONSULTAS_MIN` | `10` | Rate limit por empresa. |
| `SEGURIDAD_COBRO_CARPETA` | `SeguridadCobro` | Carpeta GCS de las cuentas de cobro. |
| `SEGURIDAD_COBRO_EXIGIR_PLAN` | `true` | `false` = válvula para desplegar sin interrumpir (consulta sin plan). |
| `SEGURIDAD_COBRO_CONDICIONES` | texto legal default | Condiciones de pago impresas en la cuenta de cobro. |
| `SEGURIDAD_POLICIA_CAPTCHA_KEY` | `""` (fallback `API_KEY_CAPTCHA`) | **2026-08-30** Key 2Captcha del reCAPTCHA del portal Policía. Vacía → fuente NO_DISPONIBLE `configuracion_faltante` (v1: 503 accionable). |
| `SEGURIDAD_POLICIA_CAPTCHA_URL` | `https://2captcha.com` | Base del resolvedor (in.php/res.php; clones compatibles). |
| `SEGURIDAD_POLICIA_CAPTCHA_TIMEOUT_S` | `90` | Presupuesto de polling del solve (todo el presupuesto interno ~115 s < 150 s de la fuente). |
| `SEGURIDAD_RUNT_CAPTCHA_KEY` | `""` (fallback `API_KEY_CAPTCHA`) | **2026-08-30** Key 2Captcha del captcha de IMAGEN del portal RUNT (`method=base64`, ~US$0.001). Vacía → fuente NO_DISPONIBLE `configuracion_faltante` (v1: 503 accionable). |
| `SEGURIDAD_RUNT_CAPTCHA_URL` | `https://2captcha.com` | Base del resolvedor para RUNT. |
| `SEGURIDAD_RUNT_CAPTCHA_TIMEOUT_S` | `90` | Presupuesto de polling del solve (navegación ~15 s + solve + lectura ≈ 60-90 s < 150 s de la fuente). |
| `SEGURIDAD_SENA_CAPTCHA_KEY` | `""` (fallback `API_KEY_CAPTCHA`) | **2026-09-01** Key 2Captcha del captcha de IMAGEN del portal SENA (`method=base64`, ~US$0.001). Vacía → fuente NO_DISPONIBLE `configuracion_faltante` (v1: 503 accionable). |
| `SEGURIDAD_SENA_CAPTCHA_URL` | `https://2captcha.com` | Base del resolvedor para SENA. |
| `SEGURIDAD_SENA_CAPTCHA_TIMEOUT_S` | `90` | Presupuesto de polling del solve (portal rápido ~5 s + solve + lectura ≈ 15-70 s < 150 s de la fuente). |
| `SEGURIDAD_MAX_CERTIFICADOS_DOC` | `20` | **2026-09-01** Tope de certificados SENA persistidos por sección (el total global vive en `total_certificados`). |
| `SEGURIDAD_SISCONMP_URL` | URL del portal SISCONMP | **2026-09-25** Base de la fuente sisconmp (el default apunta a `web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/`). Sin key de captcha: el reCAPTCHA v3 lo ejecuta la propia página. |
| `SEGURIDAD_MAX_CAPACITACIONES_DOC` | `20` | **2026-09-25** Tope de capacitaciones SISCONMP persistidas por sección (el total global vive en `total_capacitaciones`). |
| `SEGURIDAD_SITUACION_MILITAR_URL` | URL del generador del Ejército | **2026-09-25** Endpoint de la fuente situacion_militar (GET sin captcha). |
| `SEGURIDAD_SITUACION_MILITAR_TIMEOUT_S` | `30` | **2026-09-25** Timeout HTTP del bot de situación militar. |
| `SEGURIDAD_OFAC_SDN_URL` | `https://www.treasury.gov/ofac/downloads/sdn.xml` | Dataset XML SDN oficial; no contiene secretos. |
| `SEGURIDAD_OFAC_DATASET_TTL_S` | `21600` | Refresco del índice oficial en memoria (6 horas). |
| `SEGURIDAD_OFAC_TIMEOUT_S` | `60` | Timeout de descarga del dataset oficial. |
| `SEGURIDAD_RUES_SECRET_KEY` | `qwerty` | **2026-09-03** Passphrase AES del API del RUES (embebida en la SPA hoy; rotable sin redeploy si Confecámaras la cambia). |
| `SEGURIDAD_RUES_API_URL` | `https://elasticprd.rues.org.co` | Base del API Elasticsearch del RUES. |
| `SEGURIDAD_RUES_TIMEOUT_S` | `30` | Timeout HTTP del bot RUES. |
| `SEGURIDAD_ONU_XML_URL` | `https://scsanctions.un.org/resources/xml/en/consolidated.xml` | **2026-09-14** XML de la Lista Consolidada ONU (fuente `onu_ue`). |
| `SEGURIDAD_UE_XML_URL` | `…webgate.ec.europa.eu/…xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw` | **2026-09-14** FSF 1.1 de la UE. ⚠️ El token público es parte de la URL oficial documentada en data.europa.eu; si la Comisión lo rota, actualizar aquí (sin token responde EU Login). |
| `SEGURIDAD_SANCIONES_DATASET_TTL_S` | `21600` | Refresco de los índices ONU/UE en memoria (6 horas). |
| `SEGURIDAD_SANCIONES_TIMEOUT_S` | `120` | Timeout de descarga de cada dataset (la UE pesa ~25 MB). |
| `SEGURIDAD_DELITOS_CAPTCHA_KEY` | `""` (fallback `API_KEY_CAPTCHA`) | **2026-09-14** Key 2Captcha del reCAPTCHA del portal Ley 1918. Vacía → fuente NO_DISPONIBLE `configuracion_faltante`. |
| `SEGURIDAD_DELITOS_CAPTCHA_URL` | `https://2captcha.com` | Base del resolvedor para Ley 1918. |
| `SEGURIDAD_DELITOS_CAPTCHA_TIMEOUT_S` | `90` | Presupuesto de polling del solve (navegación ~15 s + solve + resultado ≈ 60-110 s < 150 s de la fuente). |
| `GEMINI_API_KEY`, `GEMINI_MODEL`, `RNDC_NIT_EMPRESA`, `MONGO_URI`, `GOOGLE_APPLICATION_CREDENTIALS` | — | Reutilizadas (ver §1 y bots). | |

## 10. Protección de datos personales

- **Minimización**: no se persisten `texto_pdf`/`texto_resultado`/`html`; mensaje ≤300 chars; viajes (operativos, necesarios para reproducir) tope 500.
- **Retención**: `retencion_expira_en` (índice TTL real de Mongo) + regla de lifecycle GCS recomendada para el prefijo.
- **PII en logs**: cédula enmascarada (`10******42`) en eventos y logs; nunca en rutas GCS. **Decisión de negocio 2026-08-30**: los DOCUMENTOS que ve el cliente (PDF del estudio, cuenta de cobro, panel admin de movimientos) muestran la cédula COMPLETA — el cliente la necesita para cruzar con sus registros; el enmascarado se mantiene en el endpoint PÚBLICO del QR (anti-enumeración) y en logs/eventos.
- **Acceso**: PDFs privados servidos solo por endpoint autenticado y auditado; verificación pública expone solo mínimos.

## 11. Verificación rápida

```powershell
# Con el backend corriendo (integrappi/):
# 0) Solo una vez: inicializar empresas/índices/roles
python scripts\inicializar_seguridad_estudios.py            # dry-run
python scripts\inicializar_seguridad_estudios.py --ejecutar # real
# 0b) Tras desplegar el backend v3.1: backfill plan→planes (una vez)
python scripts\migrar_planes_por_fuente.py                  # dry-run
python scripts\migrar_planes_por_fuente.py --ejecutar       # real
# 1) Login → copiar access_token
curl -s -X POST http://127.0.0.1:8000/seguridad/estudios/login -H "Content-Type: application/json" -d '{"correo":"...","clave":"..."}'
# 2) Consulta completa (3-60 s; 201 aunque haya fuentes caídas — mirar "estado")
curl -s -X POST http://127.0.0.1:8000/seguridad/estudios -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"cedula":"1033688842","empresa_id":"<id>"}'
#    Solo una fuente (la que el usuario elija):
curl -s -X POST http://127.0.0.1:8000/seguridad/estudios -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"cedula":"1033688842","fuentes":["procuraduria"]}'
#    Antecedentes judiciales Policía (requiere plan con esa fuente + SEGURIDAD_POLICIA_CAPTCHA_KEY; 15-90 s si no hay caché):
curl -s -X POST http://127.0.0.1:8000/seguridad/estudios -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"cedula":"1033688842","fuentes":["policia"]}'
#    Vehículo RUNT (requiere plan con esa fuente + placa del vehículo; 15-90 s si no hay caché):
curl -s -X POST http://127.0.0.1:8000/seguridad/estudios -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"cedula":"1010213062","fuentes":["runt"],"placa":"MVX48E"}'
# 2b) Sonda del portal Policía (descubrimiento/dumps):
python scripts\probar_policia.py 1033688842 --solo-formulario   # sin gastar captcha
python Funciones\bot_policia.py 1033688842                       # bot standalone (gasta 1 solve)
# 2c) RUNT — vehículo por placa + cédula del PROPIETARIO ACTIVO:
python scripts\probar_runt.py MVX48E 1010213062 --solo-formulario  # estructura + imagen del captcha
python Funciones\bot_runt.py MVX48E 1010213062                     # bot standalone (gasta 1 solve)
curl -s "http://127.0.0.1:8000/seguridad/runt?placa=MVX48E&cedula=1010213062" -H "Authorization: Bearer $TOKEN"
# 2d) SIMIT — estado de cuenta de comparendos por placa (SIN captcha, ~10 s):
python scripts\probar_simit.py MVX48E --solo-formulario   # estructura del form
python Funciones\bot_simit.py MVX48E                       # bot standalone (gratis)
curl -s "http://127.0.0.1:8000/seguridad/simit?placa=MVX48E" -H "Authorization: Bearer $TOKEN"
# 2e) SENA — certificados de formación por cédula (captcha de imagen, ~15-70 s):
python scripts\probar_sena.py 1010213062 --solo-formulario  # estructura + captcha GIF
python Funciones\bot_sena.py 1010213062                     # bot standalone (gasta 1 solve)
curl -s "http://127.0.0.1:8000/seguridad/sena?cedula=1010213062" -H "Authorization: Bearer $TOKEN"
# 2f) RUES — Registro Mercantil por NIT sin DV (API directo, SIN captcha, ~2 s):
python scripts\probar_rues.py 901923029                     # sonda (consulta viva)
python Funciones\bot_rues.py 901923029                      # bot standalone
curl -s "http://127.0.0.1:8000/seguridad/rues?nit=901923029" -H "Authorization: Bearer $TOKEN"
# 2g) SISCONMP — capacitaciones Mercancías Peligrosas por cédula (v3 nativo, $0, ~11 s):
python scripts\probar_sisconmp.py 1010213062 --solo-formulario  # estructura
python Funciones\bot_sisconmp.py 1010213062                     # bot standalone (gratis)
# 2h) Situación militar (libreta) por cédula (API directo, $0, ~2 s):
python Funciones\bot_situacion_militar.py 1010213062
# 3) PDF (abrir como application/pdf)
curl -s http://127.0.0.1:8000/seguridad/estudios/ES-XXXX/pdf -H "Authorization: Bearer $TOKEN" -o informe.pdf
# 4) QR público (el código sale en la respuesta de la consulta o del detalle)
curl -s "http://127.0.0.1:8000/seguridad/estudios/verificar/ES-XXXX?codigo=XXXXXXXXXX"
# 5) Cupo del cliente por fuente (lo que ve el portal)
curl -s http://127.0.0.1:8000/seguridad/estudios/cupo -H "Authorization: Bearer $TOKEN"
# 6) Tests del módulo (estudios + PDFs + cobro)
python -m unittest tests.test_seguridad_estudios tests.test_pdf_estudio_seguridad tests.test_seguridad_cobro -v
```

**Flujo de negocio completo para probar** (Swagger o paneles):
1. ADMIN: `POST /seguridad/admin/cobro/planes` (ej: BASICO $1.200 solo procuraduria; PRO $2.000 solo manifiestos_rndc; AVANZADO $3.000 ambas).
2. ADMIN: `POST /seguridad/admin/cobro/empresas` (NIT+nombre) → `PUT .../empresas/{id}/plan` con AVANZADO cupo 50 → agregar PRO cupo 20 (acumulable: RNDC queda 70).
3. ADMIN: GestionUsuarios → usuario perfil CLIENTE_ESTUDIOS + empresa + correo/clave.
4. Cliente: login en `/PortalSeguridad` → radios de plan (AVANZADO/BASICO) → elegir PRO = consulta SOLO RNDC, 1 CONSUMO $2.000; elegir AVANZADO = 2 CONSUMO ($6.000 con precio congelado 3.000 c/u).
5. Agotar los cupos de una fuente → 402 nombrando los planes agotados.
6. Forzar un ERROR global (ambas fuentes caídas) → verificar REEMBOLSO automático de TODOS los consumos y cupos restaurados.
7. ADMIN: cambiar precios del catálogo → asignaciones vigentes NO cambian (precio congelado); reasignar el mismo plan congela el precio nuevo.
8. ADMIN: `POST /periodos/cerrar` del mes pasado → PDF de cuenta con totales → `POST .../pagos` que cubra el total → período PAGADA.

## 12. Mantenimiento y riesgos

- **Portales frágiles** (WebForms/DNN): si un portal rediseña, la fuente queda NO_DISPONIBLE/ERROR en el estudio (nunca silenciosa). HTML de debug en `descargas_rndc2/` / `descargas_procuraduria/` / `descargas_policia/`.
- **RNDC con respuesta incompleta** (fix 2026-08-29): 0 viajes sin "Consulta realizada" en el portal → `NO_DISPONIBLE` y **NO se cachea** (antes un vacío así envenenaba la caché 24 h); las cachés viejas así se ignoran al leer. Vacío CONFIRMADO sí es EXITO legítimo y se cachea.
- **Ventana RNDC: NUNCA fecha futura** (fix 2026-08-29 noche): la ventana se calculaba con `+5 h` "para Colombia" — pero Colombia es UTC−5: entre las 19:00–24:00 CO la fecha fin caía en MAÑANA y el portal responde con la página **sin módulo de consulta** (0 tablas, sin "Consulta realizada") = NO_DISPONIBLE nocturno. Corregido a `-5 h` en `_ventana_rndc` (orquestador) y `rutas/seguridad.py`. Portal manual de comparación: https://rndc2.mintransporte.gov.co/logistica/ctl/HistorialViajes/mid/394 (tipo C, ventana 365 días AAAA/MM/DD, fecha fin ≤ hoy).
- **Docs EN_PROGRESO huérfanos** si muere el proceso a mitad: visibles en listado; los consumos NO se reembolsan solos — reembolso manual desde el panel (pestaña Movimientos).
- **Chromium concurrente**: semáforo de 2 estudios simultáneos + rate limit por empresa.
- **Filesystem efímero en Render**: los PDF van a GCS (nunca a disco). El bot de Procuraduría sigue dejando su PDF en `descargas_procuraduria/` (local, efímero).
- **Cupo desnormalizado** (`planes.<i>.cupo_disponible` en la empresa): la verdad del DINERO siempre es la agregación de movimientos; el contador solo gobierna el cupo de unidades por fuente. Si se desincroniza (fallo catastrófico), recalcular desde movimientos.
- **Carrera del posicional** (`planes.<i>`): si otro update reordena el array entre lectura y update, el filtro por `planes.<i>.fuente` no matchea → releer + 1 reintento; si persiste, 402 (nunca cobra la fuente equivocada).
- **Deploy v3.1**: orden backend-nuevo → backfill (`scripts/migrar_planes_por_fuente.py`). El código tolera ambas formas (subdoc viejo delega en la lógica antigua). NUECA correr el backfill con el backend viejo desplegado.
- **Deploy con cobro**: las empresas quedan SIN planes → nadie consulta (503) hasta asignar por fuente desde el panel. Para no interrumpir: arrancar con `SEGURIDAD_COBRO_EXIGIR_PLAN=false` y activarlo tras crear los planes.
- **Rama Judicial**: próxima fuente → `bot_rama_judicial.py` + entrada en `FUENTES` del orquestador + sección del PDF + aparece sola en los checkboxes de planes.
- **Sync read-time (v3.4)**: la propagación de fuentes escribe en GETs (`/cupo`, `GET /empresas`) y en el POST del estudio — amortizado (tras el primer sync por empresa es no-op) y con degradación controlada (fallo de Mongo → warning y se continúa con las entradas actuales, jamás 5xx). Concurrencia: `$push` con guard `$not $elemMatch` — dos consultas paralelas no duplican la entrada. ⚠️ El guard vive en el filtro del update, no en un índice: endurecimiento opcional = unique multikey `{planes.plan_id, planes.fuente}` (verificar duplicados previos). El cupo desnormalizado de una fuente retirada no es consumible: `dashboard` lo excluye del cupo global; si se olvida el filtro de membresía en algún agregado futuro, sobrerreportará.
- **Policía — términos del portal (riesgo dominante, 2026-08-30)**: el portal prohíbe EXPRESAMENTE la consulta por terceros ("Se prohíbe a cualquier persona natural o jurídica diferente del titular…"; el "Acepto" declara "únicamente solicitaré información relativa a mi persona"). Mitigaciones: **opt-in por empresa** (NO está en `CONFIG_DEFAULT_EMPRESA` ni en el seed), PDF con base legal honesta (autorización del titular, Ley 1581; obligación del solicitante), docstring del endpoint v1 que lo declara. Activarla en una empresa = decisión de negocio explícita con autorización documentada del titular.
- **Policía — presupuesto y costo**: navegación ~15 s + solve 10–60 s + resultado ~10 s ≈ 115 s < 150 s de `TIMEOUT_FUENTE_S` → el reintento del orquestador solo ocurre en fallos rápidos (sin key, in.php rechazado). Cada consulta con miss de caché cuesta ~US$0.003 de 2Captcha (caché 24 h amortiza). El PrimeFaces Poll de la vista devuelve a index a los 60 s: el bot lee el resultado apenas estabiliza. Sitekey se lee del DOM en runtime (fallback constante). `_LOCK` del bot + semáforo 2 estudios = 1 solve en vuelo por instancia.
- **RUNT — validación de propietario (trampa de UX, 2026-08-30)**: el portal SÓLO responde si la cédula es de un **propietario ACTIVO** de la placa ("Los datos registrados no corresponden con los propietarios activos…" en modal SweetAlert2). Con EYX243/15887928 dio ese rechazo (¿placa vendida?); con MVX48E/1010213062 (moto del dueño) éxito. El portal cliente ya muestra la placa junto a la cédula para que el usuario entienda el par. `no_registra=False` + ese mensaje = EXITO determinante (se cachea 24 h). **Mitigado (2026-08-30 tarde)**: el conductor evaluado muchas veces NO es el dueño → el POST acepta `cedula_propietario` y el RUNT se consulta con la cédula del DUEÑO (caché y bot incluidos); el portal pide la cédula del propietario solo si el plan incluye runt (vacía = conductor es dueño) y el PDF diferencia evaluado vs propietario. El rechazo con cédula correcta del dueño sigue siendo legítimo (placa vendida: `no_registra=False`, EXITO, se cachea).
- **RUNT — paneles perezosos**: la página `info-vehiculo` carga la tabla SOAT solo al ABRIR el mat-expansion-panel (el bot hace click en "Póliza SOAT" y espera 2.5 s). Si el portal cambia el título del panel, el bot queda sin pólizas pero CON datos del vehículo (la sección general está abierta por defecto) → EXITO sin semáforo SOAT, no error.
- **RUNT — presupuesto y costo**: boot SPA ~4 s + solve imagen (10–60 s, `method=base64` ~US$0.001) + lectura ~5 s ≈ 15–70 s, muy por debajo de los 150 s. El captcha de imagen (5 chars mixed-case) es más barato y rápido que el reCAPTCHA de policía. `_LOCK` + semáforo = 1 solve en vuelo.
- **RUNT — deploy**: ~~correr `scripts/habilitar_fuente_runt.py --ejecutar`~~ **OBSOLETO (2026-09-01 tarde)**: desde `fuentes_habilitadas_efectivas` las fuentes default corren para toda empresa sin tocar config — basta editar/crear el plan con runt (la sync v3.4 materializa las entradas al vuelo).
- **SIMIT — presupuesto y costo (2026-09-01)**: boot SPA ~4 s + submit + resultado ~5 s ≈ **10 s** (la fuente más rápida del módulo) y **costo $0** (sin captcha). `_LOCK` del bot serializa; el semáforo global (2 estudios) la comparte con los demás bots.
- **SIMIT — modales ocultos (trampa)**: el DOM del resultado trae ~7 modales Bootstrap OCULTOS cuyo texto plantilla menciona la placa consultada ("Se han encontrado varios resultados para la búsqueda del número X") — parsear el HTML crudo produce falsos mensajes. El bot lee SOLO contenido visible (`is_visible` / elementos del resultado real).
- **SIMIT — filas vs agregado (trampa del semáforo)**: cada fila trae "Valor a pagar" nominal que puede ser > 0 aunque el agregado `#valorTotal` sea $0 (prescrito/condonado — visto en ZZZ999). El semáforo usa el AGREGADO (`total_a_pagar`), nunca la suma de filas.
- **SIMIT — paginación**: la tabla pagina 5/10/15; el bot lee la primera página y los TOTALES vienen del resumen (globales). El PDF declara "primera página del portal" — no promete el detalle completo.
- **SIMIT — deploy**: ~~`habilitar_fuente_simit.py`~~ **OBSOLETO (2026-09-01 tarde, ver RUNT)** — el plan es el gate; sin vars nuevas (no hay captcha).
- **SENA — presupuesto y costo (2026-09-01)**: carga GeneXUS ~3 s + solve del captcha GIF (10–60 s, `method=base64` ~US$0.001) + postback ~3 s ≈ **15–70 s** < 150 s de la fuente. `_LOCK` + semáforo global = 1 solve en vuelo.
- **SENA — "No hay resultados" tri-ambiguo (trampa calibrada)**: ese label es el estado INICIAL de la página, lo que muestra un captcha EQUIVOCADO y la respuesta legítima de una cédula sin certificados. El bot distingue: ErrorViewer visible con "no corresponde con la imagen" → `BotSenaCaptchaFallido` (ERROR captcha, reintento con imagen nueva); filas del grid → certificados; sin filas + sin error + **imagen del captcha CAMBIADA** → vacío legítimo (EXITO, se cachea); sin ninguna señal de postback → `BotSenaSinResultado` (NO_DISPONIBLE, sin cachear). El PDF de cada certificado se descarga por link propio (`com.sena.guardar?var1=<registro>`) — fuera de alcance (solo listado).
- **SENA — deploy**: ~~`habilitar_fuente_sena.py`~~ **OBSOLETO (2026-09-01 tarde, ver RUNT — este fue el caso que lo motivó: plan BASICO con sena quedaba DESHABILITADA en GLAMPEROS porque el config persistido no la listaba)**. Solo falta el env `SEGURIDAD_SENA_CAPTCHA_KEY` en Render (o dejar el fallback `API_KEY_CAPTCHA` ya existente) y crear/editar el plan con sena.
- **RUES — dependencia de la passphrase y del API no documentado (riesgo dominante, 2026-09-03)**: el bot usa el mismo API cifrado que la SPA pública con la passphrase embebida `"qwerty"`; Confecámaras puede rotarla o bloquear IPs de Render sin aviso → la fuente cae a NO_DISPONIBLE accionable (`rues_no_disponible`) sin veneno de caché. Mitigaciones: passphrase en env (`SEGURIDAD_RUES_SECRET_KEY`, rotable sin deploy), sonda `scripts/probar_rues.py --estructura` para redescubrir el contrato, y plan B documentado (bot Playwright con UA real: el CDN de la SPA ya bloquea el UA `HeadlessChrome` — requiere `--disable-blink-features=AutomationControlled` + UA de navegador). Deploy: `pycryptodome` NUEVA dependencia en requirements (Render la instala solo); sin migraciones ni env obligatorios (defaults funcionan).
- **RUES — presupuesto y costo**: la fuente más rápida y barata después de simit: 2 POST cifrados ≈ 1-2 s, **$0**, sin captcha ni navegador. `_LOCK` del bot serializa la sesión HTTP compartida; no comparte el semáforo de Chromium con los demás bots.

### ⏭️ PENDIENTE PRÓXIMA SESIÓN: paginación del portal RNDC

**Descubierto 2026-08-29**: `https://rndc2.mintransporte.gov.co/logistica/ctl/HistorialViajes/mid/394`
devuelve los manifiestos **PAGINADOS** en el TreeView `tvDatos`. El bot
(`Funciones/bot_rndc2.py::_extraer_resultados`) lee UNA sola página: con
cédulas de mucha actividad (ej. >20 manifiestos en la ventana de 365 días)
el estudio reporta solo la primera página — total subestimado frente a la
consulta manual en el navegador.

**Paso a paso para analizar/implantar**:
1. Reproducir con una cédula de alto tráfico (ej. 1033688842 trajo 20 en el
   bot; comparar cuántas páginas muestra el portal manual) — modo `headed=True`
   y guardar el HTML de `descargas_rndc2/`.
2. Identificar en el DOM los controles de paginación del TreeView (DNN/WebForms:
   típicamente un `asp:DataPager`/links `__doPostBack` con números de página o
   "Siguiente") y el contador total de registros si el portal lo muestra.
3. Decidir estrategia en `consultar_historial_viajes`:
   loop `while` que hace click en página siguiente → espera `networkidle` o el
   mensaje "Consulta realizada" renovado → parsea y ACUMULA hasta que el
   control de "siguiente" desaparezca o se repita la página (protección anti-bucle).
4. Presupuesto: cada página extra consume el `TIMEOUT_FUENTE_S` (150 s) del
   orquestador — medir cuánto tarda un giro de página y, si hace falta,
   subir el tope solo para RNDC o paginar dentro del bot con su propio límite
   (ej. máx 10 páginas) + reportar `paginas`/`truncado:true` en el resultado.
5. Recalcular `total` como suma de las páginas (hoy es el count de la primera)
   y propagarlo al doc del estudio y al PDF ("Total de viajes").
6. Caché: el resultado completo se cachea igual (24 h) — el fix beneficia a
   todas las empresas; al desplegar, borrar cachés RNDC previas de cédulas
   afectadas o dejar que expiren.
7. Tests: añadir en `tests/test_seguridad_estudios.py` un fake del bot que
   devuelva 2 páginas y verificar acumulación + total.

**Nota**: la ventana de 365 días ya trae ~20 viajes donde la de pruebas de 6
meses traía 5 — la subestimación por paginación solo se nota en conductores
de alto tráfico; priorizar según el uso real que le den los clientes.

### ⏭️ PENDIENTE: Tarjeta de Operación y Pólizas RC del RUNT (2026-09-14)

El acordeón de `info-vehiculo` tiene 20 paneles; hoy el bot expande SOAT y
RTM. Para vehículos de CARGA interesan además:

1. **Tarjeta de Operación** (habilitación de transporte público/carga): el
   panel existe pero con MVX48E (particular) viene vacío. Calibrar el parseo
   con una placa de flota de carga (camión propio del usuario) — misma técnica
   del RTM: expandir el panel por título, dump en `descargas_runt/`, regex
   calibrada con la fila real. Semáforo candidato: TO vencida/sin registrar →
   ADVERTENCIA (análogo SOAT/RTM) — decidir con el usuario.
2. **Pólizas de Responsabilidad Civil** (soat de carga): mismo procedimiento.

Cada panel extra consume ~2,5 s de espera de carga Ajax dentro del presupuesto
de 150 s de la fuente (hoy SOAT+RTM usan ~5 s de los ~60-75 s totales).

### ⏭️ PLAN: sprint 2 y siguientes (estado 2026-09-14)

**Sprint 2A — fuente `delitos_sexuales` ✅ COMPLETADO (2026-09-14)**: ver capa
15 y bitácora (sonda → bot con submit nativo + stub → campo `fecha_expedicion`
en POST/portal/422 → integración completa → 278 tests OK → E2E real).

**Sprint 2B — INPEC (BLOQUEADO desde red local, reintentar)**

El portal no responde NI con Chromium real desde la red del usuario
(timeout en goto; posible geo-bloqueo/WAF a IPs residenciales colombianas o
caída del sitio). Paso: correr `scripts/probar_inpec.py --solo-formulario`
**desde el Shell de Render** (IP US, `curl` primero para descartar TLS):
si abre → sonda completa → bot molde policía/simit según captcha; si no →
dejar documentado como NO DISPONIBLE estructural y pasar.

**Defunciones Registraduría — CERRADO (ver §1)**: sin canal público por
terceros (webservice solo para conveniados tipo DIAN). No implementar.

**Backlog posterior (decisión del usuario)**
- **Sprint 3**: RUAF (ARL/EPS/pensiones activas — killer feature para En
  Ruta) SOLO con marco opt-in + autorización documentada del titular
  (habeas data, Ley 1581);FOPEP/SISBÉN descartables.
- **RUNT carga**: Tarjeta de Operación + Pólizas RC calibrando con una
  placa de camión de la flota (§12 anterior).
- **Empresas NIT**: Insolvencias Supersociedades + DIAN proveedores
  ficticios (boletines públicos) cerrarían el paquete NIT.
- **UK OFSI**: single list sin bulk download; solo API POST del SPA
  (search-uk-sanctions-list) — evaluar si vale la pena.
- **Deploy pendiente de los sprints 1-2** (`onu_ue` + RTM + `delitos_sexuales`):
  backend + frontend; sin migraciones (el plan es el gate: editar el catálogo
  materializa las fuentes solas).

## Bitácora

| Fecha | Cambio |
|---|---|
| 2026-08-29 | Módulo v1 por fuente (RNDC 1 año + Procuraduría), bots Playwright, caché 24 h. Eliminado CoreSoft. Policía pausada. |
| 2026-08-29 | **Estudios de Seguridad v2 (multi-tenant)**: login Bearer con roles, orquestador paralelo con reintentos y estados honestos, PDF consolidado reproducible con QR/marca de agua, GCS privado, eventos de auditoría, seed INTEGRA, 38 tests. Endpoints v1 migrados a Bearer. E2E verificado con cédula real (ES-7996BE8F2B49, COMPLETADA). |
| 2026-08-29 | Producción blindada: JWT_SECRET fuerte (Render+.env), migración a bucket PRIVADO `integrapp-privado` (publicAccessPrevention enforced; el PDF inicial expuesto fue migrado y borrado del público), QR apuntando a la URL de Render. E2E empresa real GLAMPEROS SAS + usuaria MGOMEZ (CONSULTADOR): estudio COMPLETADA con marca de agua de SU empresa. |
| 2026-08-29 | Fixes post-prueba: `empresa_id`/`usuario_id` como ObjectId nativo en estudios (bug: el propio autor no veía su estudio; filtro tolerante a ambos tipos + test de regresión), serialización ObjectId→string en respuestas (crear estudio y crear plan explotaban 500), tabla de manifiestos con celdas Paragraph-wrap y anchos ponderados (los nombres largos invadían columnas), zona horaria Colombia corregida a UTC−5 en PDF y cobro, endpoint `POST /token` para el Authorize de Swagger. |
| 2026-08-29 | **v3 — Planes y cobro postpago**: motor de consumo atómico con precio snapshot, reembolso automático en ERROR (índice único parcial anti-doble), pagos/ajustes/reembolsos manuales, cierres de mes con totales congelados y cuenta de cobro PDF reproducible, planes armables (precio COP + fuentes incluidas + vigencia), 402 accionables, válvula EXIGIR_PLAN. Router `/seguridad/admin/cobro` (17 endpoints). Panel admin `/AdminSeguridad` (tarjeta solo ADMIN en LoginUsuario) y portal cliente `/PortalSeguridad` (login propio independiente). 24 tests de cobro (198 totales OK). |
| 2026-08-29 | UX portal: plan sin tope (checkbox admin, caja verde "Sin límite" en el portal), renombrado estudio→consulta en todos los textos del portal y mensajes 402, lottie detective a medida (`AnimationDetective.json`: expediente+lupa+check) con mensajes rotativos mientras consulta (fix z-order de capas: el fondo opaco tapaba todo). |
| 2026-08-29 | **v3.1 — Multi-plan por fuente + elección del usuario**: array `empresas_seguridad.planes` (un plan POR fuente, cupos independientes o None=sin tope), `reservar_consumos` con update atómico posicional y compensación en cascada (reversión de cupos + borrado de movimientos), N CONSUMO por consulta con precio de cada plan, `fuentes` en el POST para elegir qué consultar (422 si pide fuente sin plan; no elegidas → DESHABILITADA), `reembolsar_consumos_consulta` en ERROR global, índice único `(consulta_id, consumo_id, REEMBOLSO)`, `/cupo` con desglose por fuente, admin `PUT/DELETE /empresas/{id}/planes/{fuente}` (backfill implícito), selector de fuentes con checkboxes en el portal, tabla admin con desglose por fuente. Backfill `scripts/migrar_planes_por_fuente.py`. Fix bugs: `asyncio.gather` con fuente deshabilitada (TypeError unhashable) y `fuentes.error_global` str rompiendo la serialización. |
| 2026-08-29 | **Fix caché RNDC envenenada**: respuesta Ajax incompleta del portal (0 viajes sin confirmación) se trataba como EXITO y caché 24 h → la cédula quedaba "sin manifiestos" todo el día. Ahora: vacío sin "Consulta realizada" = NO_DISPONIBLE sin cachear; `_buscar_cache` ignora cachés viejas así; vacío confirmado sigue siendo EXITO legítimo. 3 tests de regresión. |
| 2026-08-29 | **Alta de empresas y usuarios desde la UI**: `POST /seguridad/admin/cobro/empresas` (NIT+nombre, slug derivado, config default) + botón "+ Nueva empresa" en el panel. `POST/PUT /baseusuarios/` aceptan `empresa_id_seguridad`/`rol_seguridad` (valida empresa activa y rol; PUT limpia campos si el usuario deja de ser del módulo). **Perfil `CLIENTE_ESTUDIOS`**: cliente externo del portal (requiere empresa; SEGURIDAD queda como personal interno de /revision — un cliente NUNCA debe tenerlo). LoginUsuarios manda CLIENTE_ESTUDIOS directo a /PortalSeguridad. GestionUsuarios: selector empresa+rol solo con ese perfil. Header de app ancho completo en AdminSeguridad con menú usuario y cerrar sesión (patrón /indicadores). Fix serialización: `empresa_id` puede ser ObjectId en BD → `modelo_usuario` normaliza a string (500 en /baseusuarios/). |
| 2026-08-29 | **v3.2 — Planes acumulables + UX por plan**: el array `planes` admite varias entradas por fuente (AVANZADO + PRO conviven; cupos suman; un plan sin tope → fuente sin tope). Asignación POR PLAN `PUT /empresas/{id}/plan` (aditivo; mismo plan → update de cupo; `DELETE .../plan/{plan_id}` retira todo el plan, 422 si tiene consumos). Endpoints por fuente `PUT/DELETE .../planes/{fuente}` ELIMINADOS. `reservar_consumos`: itera candidatos FIFO por `asignado_en` hasta reservar (402 nombra planes agotados); precio = `precio_congelado` de la entrada. `reembolsar_consumo` localiza por (fuente, plan_id). Panel: fila expandible ▸ con tarjetas por plan, botón "+ Asignar plan", ✕ FaTimes por plan. Tests FIFO multi-plan. |
| 2026-08-29 | **v3.3 — El usuario elige el plan al consultar**: `POST /seguridad/estudios` acepta `plan_id` (obligatorio desde el portal): el PLAN define las fuentes a correr (∩ habilitadas) y su entrada pasa al frente del FIFO. `/cupo` agrega `planes[]` (elegibles) además del consolidado `fuentes[]`. Portal: radios SOLO de planes (sin "Automático", sin checkboxes de fuentes — implícitas en el plan), preselección si hay 1, Swal exige elección. |
| 2026-08-29 | **Fix ventana RNDC fecha futura (NO_DISPONIBLE nocturno)**: `+5 h` en lugar de `−5 h` (UTC+5=Estambul) hacía que entre 19:00–24:00 CO la fecha fin cayera en mañana → portal responde página sin módulo → respuesta incompleta. Corregido en `_ventana_rndc` y `rutas/seguridad.py`. Ventana 365d correcta trae ~20 viajes donde la de pruebas de 6 meses traía 5. |
| 2026-08-29 | **Fix PDF desbordes**: celdas con SHA-256/anexos/mensajes largos se salían de las tablas (strings crudos en Table no hacen wrap). Helper `celda()` con Paragraph + `splitLongWords` en tablas persona/resumen/proc/trazabilidad; SHA del anexo completo (antes 32 chars); cabecera "Resumen por fuente" con letra blanca sobre fondo azul. |
| 2026-08-29 | **PENDIENTE (próxima sesión)**: el portal RNDC **pagina** los manifiestos del TreeView `tvDatos`; el bot lee solo la primera página → total subestimado en cédulas de alto tráfico. Plan de análisis/implantación detallado en §12 "PENDIENTE PRÓXIMA SESIÓN". |
| 2026-08-30 | **Fuente "policia" — Antecedentes Judiciales (Policía Nacional)**: tercera fuente del módulo. Descubrimiento con sonda `scripts/probar_policia.py` (dumps paso a paso; la página de resultado real es `formAntecedentes.xhtml` con el mensaje en `#form:mensajeCiudadano`): leyendas oficiales SU-458/2012 (NO TIENE ASUNTOS PENDIENTES → no_registra true / ACTUALMENTE NO ES REQUERIDO → false), **sin PDF** (Decreto 19/2012 art. 93 lo eliminó) y línea "Apellidos y Nombres" → `nombre_consultado`. Bot `Funciones/bot_policia.py` (JSF/PrimeFaces por clicks — nunca POSTs manuales; reCAPTCHA v2 resuelto por 2Captcha con sitekey leído del DOM; token inyectado en `#g-recaptcha-response`; PrimeFaces Poll a 60 s → leer apenas estabiliza). Excepciones → estados: SinCaptchaKey=NO_DISPONIBLE config (no dispara reembolso), SinResultado=NO_DISPONIBLE anti-envenenamiento (doble barrera bot+orquestador, sin cachear), CaptchaFallido=ERROR captcha. Orquestador: FUENTES +3ª, ramas elif, loop de anexos (`anexo_policia` listo aunque el portal no genera PDF), nombre_consultado en cascada PGN→Policía. PDF: sección 3b semaforizada + fila resumen + reintentos + disposición legal HONESTA (autoconsulta Decreto 019/2012 + autorización titular Ley 1581, sin norma habilitante de terceros). **OPT-IN por empresa**: no va en CONFIG_DEFAULT_EMPRESA ni seed. Endpoint v1 `GET /seguridad/policia` (503 accionable sin key). Frontend: etiqueta/tarjeta/mensajes en portal, checkbox con labels legibles en admin (mapa compartido ETIQUETAS_FUENTE). E2E real con 1033688842: GET vivo 28 s (AMAYA TOVAR JHOAM ORLANDO, no_registra true), estudio ES-A548F7216C6F COMPLETADA vía plan POLICIA TEST (CONSUMO $2.500, cupo 20→19), PDF verificado visualmente. 105 tests OK (9 policia estudios + 6 PDF + 2 cobro nuevos). ~US$0.003/consulta sin caché. |
| 2026-08-30 | **v3.4 — El catálogo manda: propagación automática de fuentes + rutas por pestaña**: editar `fuentes_incluidas` de un plan ya NO requiere reasignar por empresa. `sincronizar_fuentes_planes` (cobro_seguridad) materializa fuentes NUEVAS en read-time (POST estudio, `/cupo`, GET /empresas): `$push` append-only con guard `$not $elemMatch` (carrera segura, nunca `$set` del array — perdería `$inc` posicionales), precio congelado = ACTUAL del catálogo, cupo clonado de la hermana más antigua, `asignado_en=ahora`, `asignado_por:"sync_catalogo"`; NADA existente se toca. Fuentes RETIRADAS: entrada sobrevive (reembolsable, historial intacto) pero no consumible (`_entrada_consumible` = vigencia+membresía en `fuentes_con_plan`/`reservar_consumos`; `"todas"` nunca se bloquea); `/cupo` las excluye, `GET /empresas` marca `retirada`, dashboard excluye su cupo, ADMIN_INTEGRA tampoco corre fuente retirada (evitaba consumos gratis), `plan_pedido` filtra membresía. Evento de `PATCH /planes` audita diff +/- fuentes. Frontend: AdminSeguridad con ruta propia por pestaña (`/AdminSeguridad/{empresas,planes,movimientos,cuentas}`, carpetas estáticas + mirror `history.replaceState` patrón /revision) y entradas retiradas en gris con cupo fuera de barras. ColFake extendida ($push/$pull/$not/$elemMatch). 109 tests OK (+18: 10 sync, 5 retiradas, 3 endpoints). |
| 2026-08-30 | **Portal cliente: header de app + responsive**: `/PortalSeguridad` estrena el header de app ancho completo de AdminSeguridad (logo IntegrApp, título+empresa en el header, menú de usuario con dropdown "Volver al inicio"/"Cerrar sesión" y encabezado con nombre+empresa; **hamburguesa en ≤600px** donde el botón de usuario queda solo-icono). Responsive completo: grid a 1 columna ≤800px (ya existía), selector de planes apilado y form de consulta a columna (botón full-width), y el **Historial de consultas ya no desborda** — envoltura con scroll horizontal en rango intermedio y colapso a TARJETAS (una por consulta, campos con `td::before { content: attr(data-label) }`) bajo 640px. |
| 2026-08-30 | **Fuente "runt" — Vehículo (RUNT) + acordeón de planes en el portal**: cuarta fuente y PRIMERA con parámetro extra: consulta por **placa + cédula del propietario ACTIVO**. Descubrimiento con sonda `scripts/probar_runt.py`: SPA Angular 11 + Material (hash routing, jamás networkidle), form por `formcontrolname` (placa/documento/captcha), **captcha de IMAGEN propio** (única data:image/png del DOM → 2Captcha `method=base64` ~US$0.001), resultado en `info-vehiculo` con acordeón perezoso (abrir "Póliza SOAT" carga la tabla) y rechazo "no propietario activo" en modal SweetAlert2. Bot `Funciones/bot_runt.py`: parseo por labels consecutivos + regex de fila de póliza; `_soat_vigente` (hoy Colombia). Integración: `FUENTES` +runt, `placa` kwarg por toda la cadena (POST → ejecutar_estudio → _ejecutar_fuente → bot), caché con clave **(tipo, cédula, placa)** y guard sin-placa-nunca-hit, **SOAT vencido → fuente ADVERTENCIA** (recalculado en cada hit de caché — una caché de ayer puede estar vencida hoy), `no_registra` = sobre la PLACA (jamás "limpio"), anexo_runt listo, endpoint v1 `GET /seguridad/runt` (503 sin key), `GET /{id}/runt.pdf`, `_normalizar_placa` (AAA123/AAA12A/AA1234), 422 accionable si falta placa, stats +runt, CONFIG_DEFAULT/seed +runt (portal público, NO opt-in), script `habilitar_fuente_runt.py` ($addToSet idempotente). PDF: sección 3c "Vehículo — RUNT" semaforizada + historial de pólizas + disposición legal honesta (Portal Público de Consulta Ciudadana, SOAT informativo). **Portal cliente: ACORDEÓN** — el input fijo de cédula DESAPARECE; cada plan se abre con click y trae el formulario con lo que ESE plan pide (cédula siempre, placa solo si incluye runt; validación de placa; sin preselección; columna Placa condicional en historial). Admin: etiqueta+checkbox runt. E2E real MVX48E/1010213062: bot standalone OK (HONDA CB 160F DLX, SOAT AXA 3453028900 vigente→2026-10-22, 5 pólizas históricas). 145 tests OK (+36: 12 TestFuenteRunt/TestCacheRunt, 9 TestSeccionRunt, 1 cobro runt, fixes de mocks con placa=None). |
| 2026-08-30 | **Fix dumps de debug que tumbaban fuentes + costo en historial + headers a ancho completo**: (1) `bot_policia.py` escribía `descargas_policia/resultado_{cedula}.html` SIN crear la carpeta y sin try — con la carpeta ausente el FileNotFoundError dejaba la fuente en ERROR aunque el portal HUBIERA respondido (bug real con 52978733); ahora mkdir al vuelo + try/except en policía y procuraduría: un dump jamás tumba una consulta. (2) `GET /seguridad/estudios` (historial) enriquece cada ítem con **`costo_cop`** (suma CONSUMO−REEMBOLSO por consulta_id, una sola aggregation `$in`, fallo≠listado); portal: columna "Costo" ("Sin costo" en $0). (3) Headers de AdminSeguridad/PortalSeguridad alineados al patrón /Pedidos: fuera `max-width:1280px` del inner (en pantallas grandes quedaba franja encogida al centro distinta a la Torre de Control), altura 64px, título del módulo como TAB central (`.AS-headerTab` "Estudios de Seguridad" con FaShieldAlt; PS-header-info centrado con flex:1), oculto ≤900px como el nav de Pedidos. |
| 2026-08-30 | **Propietario ≠ persona evaluada (diseño de sujetos)**: el estudio deja de asumir un solo sujeto — la persona evaluada (RNDC/Procuraduría/Policía) y el VEHÍCULO con SU propietario (RUNT) quedan diferenciados. POST acepta `cedula_propietario` opcional (solo runt; vacía = el evaluado es el dueño, comportamiento previo); orquestador consulta/cachea runt con la cédula del propietario (`_ejecutar_fuente` kwarg `cedula_propietario`); doc persiste `vehiculos[{placa, cedula_propietario, propietario_es_evaluado}]` (array listo para varios vehículos; `placa` top-level = espejo de `vehiculos[0]` para compatibilidad). PDF: fila "Propietario del vehículo" (cédula ENMASCARADA + relación) en datos de la persona, badge ámbar "PROPIETARIO … DISTINTO DE LA PERSONA EVALUADA" al inicio de la sección RUNT, fila "Cédula consultada (propietario)" en el rechazo por propiedad (explica con qué cédula validó el portal) y fila "Vehículo/propietario" en trazabilidad; docs previos sin `vehiculos` se asumen dueño=evaluado. Portal: campo opcional "Cédula del propietario (vacía si es el conductor)" en el acordeón del plan cuando incluye runt + aviso en el resultado. 155 tests OK (+10: 5 TestFuenteRuntPropietario, 5 de PDF). Compatibilidad total: sin `cedula_propietario` todo se comporta como antes. |
| 2026-08-30 | **Servicio por API — API keys de integración + canal portal/api**: los clientes consumen el servicio máquina a máquina con `Authorization: Bearer sek_…` (mismo header que el JWT; `actor_actual` enruta por prefijo). Colección `api_keys_seguridad` (hash SHA-256 unique — la clave plana se muestra UNA vez al crearla; prefijo visible; scopes solo estudios:crear/leer — NUNCA admin; máx 10 activas por empresa; `ultimo_uso_en` con throttle 1 write/min; revocación soft instantánea porque cada request relee la BD). Actor de key = CONSULTADOR de la empresa, usuario "API: nombre", usuario_id None, canal="api". **Canal**: campo `canal` en doc del estudio (`+ api_key {id,nombre}`), en cada movimiento CONSUMO (extra de `_nuevo_movimiento`) y en eventos → historial del portal cliente con badge "API" (`PS-badge-api`), movimientos del admin con badge: distinción manual vs integración visible en panel y cuentas. API key ignora `aislamiento_usuario` (integración de la empresa); rate limit por empresa le aplica. Endpoints admin `GET/POST/DELETE /empresas/{id}/api-keys` (201 devuelve la clave completa una única vez; eventos api_key_creada/revocada; índices `idx_apikey_hash` unique + `idx_apikey_empresa_activa` en asegurar_indices_cobro). AdminSeguridad: sección "API keys" en la fila expandible de cada empresa (crear con Swal que muestra la clave una vez, tarjetas con prefijo/último uso/revocar). Anti-enumeración: clave inválida/revocada/empresa inactiva → mismo 401. 164 tests OK (+9: 7 TestApiKeyAuth, 1 canal en consumos). Sin migración: colección nueva + índices automáticos. |
| 2026-08-30 | **Fix Procuraduría "no concluyente" + caché envenenada (cédula 1033688842, ES-8F34FEE82AAA)**: el postback de la PGN a `verpdf.aspx` es LENTO e intermitente (visto tardar >45 s o no llegar); cuando fallaba, el bot retornaba silenciosamente sin veredicto y SIN PDF → la fuente salía ADVERTENCIA "Veredicto no concluyente — ver certificado adjunto" apuntando a un anexo INEXISTENTE, y ese vacío quedaba CACHÉ 24 h (la cédula repetía "no concluyente" aunque el portal respondiera bien después) — el mismo bug anti-envenenamiento de RNDC/Policía/RUNT pero en la fuente que nunca recibió la barrera. Fixes: (1) bot espera `#btnDescargar` de forma explícita (45 s) en vez de networkidle+3 s; (2) bot LANZA `BotProcuraduriaSinResultado` cuando no hay veredicto ni PDF (dispara reintento del orquestador; clasificado NO_DISPONIBLE portal_inconsistente); (3) guard en el orquestador: sin veredicto Y sin PDF → NO_DISPONIBLE SIN cachear (PDF ilegible pero presente sigue siendo ADVERTENCIA legítima con anexo); (4) `_buscar_cache` ignora cachés ya envenenadas de procuraduría (no_registra None + pdf_tamano 0) al leer; (5) `_texto_veredicto` del PDF solo promete "ver certificado adjunto" si `pdf_tamano > 0` (si no: "certificado no entregado por el portal"); (6) bot solo dice "Certificado generado; ver PDF" cuando el PDF EXISTE. Borrada la caché envenenada de producción (1 doc). Verificación en vivo post-fix: veredicto limpio + PDF 52 KB. 168 tests OK (+4 TestFuenteProcuraduriaAntiEnvenenamiento). |
| 2026-08-30 | **Cédulas visibles + botón del certificado PGN en el portal**: (1) decisión de negocio — la cédula va COMPLETA en el PDF del estudio (fila Propietario, badge, cédula consultada, trazabilidad — `_enmascarar_cedula` del PDF ahora pasa el valor limpio), en la cuenta de cobro mensual y en los movimientos del panel admin; el enmascarado queda SOLO donde es de seguridad: endpoint público del QR (anti-enumeración) y logs/eventos. (2) Portal cliente: botón "Certificado Procuraduría" en la tarjeta de resultado (variante secundaria `PS-boton-anexo`) y "PGN" en el historial — descargan el anexo OFICIAL vía `GET /{consulta_id}/procuraduria.pdf` (`descargarAnexoProcuraduria`), visible solo si `anexo_procuraduria` existe en el estudio. 168 tests OK (aserciones de cédula completa). |
| 2026-08-30 | **v3.5 — EL PLAN SE COBRA POR CONSULTA (fix del precio × fuentes)**: el usuario reportó que una consulta del plan AVANZADO ($3.000, 4 fuentes) cobraba $12.000 — v3.1 había degradado `precio_por_estudio` a precio-POR-FUENTE (1 CONSUMO por fuente corrida). v3.5: 1 CONSUMO por PLAN involucrado en la consulta al `precio_congelado` del plan; las fuentes NO multiplican. Cupo en LOCKSTEP: 1 unidad de TODAS las entradas del plan por consulta (el cupo autorizado cuenta CONSULTAS; las fuentes del plan drenan juntas). Movimiento lleva `fuentes` (corridas) + `fuentes_tocadas` (entradas descontadas) → reembolso devuelve EXACTO (aunque la fuente se retirara del catálogo después; movs pre-v3.5 siguen la lógica vieja por (fuente, plan_id)). Fase de cobertura SIN escrituras: fuente sin entrada consumible se OMITE (contrato previo), fuente con entradas vigentes pero TODAS sin cupo → 402 nombrando fuentes y planes agotados. Displays alineados al cupo-por-consultas: `/cupo` planes[] usa MÍNIMO de entradas (no suma: 4 fuentes × 50 = 50 consultas, no 200) y `cupo_consumido` = máx; tarjeta de plan del admin igual (min/max). FIFO/planes-acumulables/compensación/402/E2E de mix de planes mono-fuente intactos (2 planes mono-fuente en una consulta = 2 consumos a SUS precios). Histórico pre-v3.5 NO se toca. 169 tests OK (+1 regresión del caso exacto del usuario: plan $3.000 × 4 fuentes = $3.000 y cupo 49/50 en todas las entradas). |
| 2026-09-01 | **Fuente "simit" — Comparendos SIMIT (FCM), QUINTA fuente**: consulta por **PLACA** (sin cédula, sin propietario) del estado de cuenta público `fcm.org.co/simit/#/estado-cuenta`. Descubrimiento con sonda `scripts/probar_simit.py`: SPA hash-routing, campo único `#txtBusqueda` (acepta cédula O placa) + submit `#btnNumDocPlaca` (solo icono), **SIN CAPTCHA** (submit directo: ~10 s, costo $0 — la fuente más rápida y gratis del módulo); resultado en la misma vista: `#resumenEstadoCuenta` (comparendos/multas/acuerdos/deuda total), tabla `tr.page-row` con `td[data-label]` (infracción COMPLETA en `data-content` del popover) y `#valorTotal` (**total EXIGIBLE**). Trampas: modales OCULTOS con plantilla que menciona la placa (el bot lee solo contenido VISIBLE) y filas con "valor a pagar" > 0 aunque el agregado sea $0 (ZZZ999: 105 pendientes 1999-2000 prescritos — el semáforo se fía del AGREGADO). Bot `Funciones/bot_simit.py` (parseo DOM por data-label + `_cop` COP→float; `BotSimitSinResultado`→NO_DISPONIBLE). Integración: `FUENTES` +simit (cobro SIN cambios: el catálogo ES la tupla), caché por **(tipo, placa) con `cedula: None`** + índice `idx_seg_tipo_placa` + guard sin-placa, `_estado_simit` (saldo exigible >0 → ADVERTENCIA → estudio CON_ADVERTENCIAS), anti-envenenamiento doble barrera, `MAX_COMPARENDOS_DOC=20`, placa obligatoria en el POST si runt **o** simit (422 nombra las fuentes), `vehiculos[]` SOLO con runt (solo simit: espejo de placa sin tríada), defaults+seed+simit (portal público, NO opt-in), script `habilitar_fuente_simit.py`, endpoint v1 `GET /seguridad/simit`. PDF: sección 4c "Comparendos — SIMIT" semaforizada (ámbar exigible / neutro histórico / verde limpio), tabla ≤10 filas, fila resumen + trazabilidad + disposición legal honesta (consulta sobre el VEHÍCULO, no antecedente personal; saldos informativos). Frontend: admin (ETIQUETAS_FUENTE + checkbox), portal (`pidePlaca` = runt O simit separado de `pidePropietario` = solo runt, badges SIMIT, aviso de texto). E2E real: MVX48E limpia ("No tienes comparendos…", EXITO) y ZZZ999 con deuda histórica (EXITO sin exigible). 186 tests OK (+17: 9 TestFuenteSimit/SoloPlaca/CacheSimit, 7 TestSeccionSimit, 1 cobro). |
| 2026-09-01 | **Diagnóstico ES-EB849B65D2D4 (plan proc+simit sin resultados)**: simit DESHABILITADA porque la empresa no la tenía en `config.fuentes_habilitadas` (faltó el paso de deploy) — corregido corriendo `habilitar_fuente_simit.py --ejecutar`; procuraduría NO_DISPONIBLE por timeout de 150 s del postback de la PGN (intermitente, reintenta solo). **PDF SOLO fuentes del plan**: helper `_corrio(f)` (estado ∉ {None, DESHABILITADA}) gatea resumen, secciones, reintentos de trazabilidad y PÁRRAFOS LEGALES (uno por fuente corrida) — las DESHABILITADAS/ausentes ya no aparecen como fila fantasma "no consultada" (las fallidas SIGUEN mostrándose: honestidad); **el mismo filtro aplica a la TARJETA DE RESULTADO del portal** (badges por fuente con `corrio()`: estado existente y ≠ DESHABILITADA — antes Policía/RNDC/RUNT mostraban "⚠️ Ver PDF"/"0 viajes" en estudios que no los incluían); fix del badge de propietario-distinto que se disparaba con `cedula_propietario=None` en estudios solo-simit. **Texto de espera dinámico** en el portal (las fuentes van en PARALELO: el total = la más lenta; procuraduría ~120 s — el viejo "3 a 60 segundos" mentía con planes grandes). |
| 2026-09-01 | **Criterio de reembolso >51% (decisión de negocio, pedido del usuario)**: la consulta se reembolsa SOLO si más del 51% de las fuentes CORRIDAS fallaron (`mayoria_fuentes_fallidas` en el orquestador; DESHABILITADAS y claves ausentes no cuentan, ADVERTENCIA cuenta como entregada, nada corrido = reembolso por falla catastrófica). Antes solo el ERROR global (100%) reembolsaba y un PARCIAL 2/3 caídas COBRABA — que un portal esté caído no invalida el resto del informe. El Swal del portal replica el criterio ("Parcial: la mayoría de las fuentes falló (no se cobra)"). 195 tests OK (+8 TestMayoriaFuentesFallidas). |
| 2026-09-01 | **Captcha PGN con preguntas de documento (reporte del usuario)**: el portal además de la aritmética pregunta cosas como "¿Escriba los dos últimos dígitos del documento a consultar?" (respuesta = la propia cédula, que el bot ya diligenció) — antes iba a Gemini SIN la cédula y fallaba. Nueva capa `_resolver_captcha_documento` (determinista: 2/3 últimos dígitos, primer dígito, número de dígitos; sin red) y Gemini ahora recibe la cédula como contexto + `maxOutputTokens` 8192 (bug conocido de Gemini 3.x: el thinking cuenta en el presupuesto y con 512 devuelve vacío). 198 tests OK (+5 TestCaptchaProcuraduriaDocumento). |
| 2026-09-01 | **Estrategia nombres para el captcha PGN (pedido del usuario)**: cuando el plan incluye procuraduria, el PORTAL pide **Nombres y Apellidos** (inputs separados, placeholder "sin tildes", obligatorios — Swal y botón deshabilitado) porque el captcha de la PGN pregunta por ellos ("¿cuál es el primer nombre de la persona que está consultando?"). Cadena: `CrearEstudio.nombres/apellidos` (opcionales en el body: no romper integraciones API) → `_normalizar_nombre` (MAYÚSCULAS, sin tildes, solo letras y espacios, Ñ conservada) → persistidos en el doc (`nombres`/`apellidos`, separados del `nombre_consultado` VERIFICADO de la cascada PGN→Policía) → `ejecutar_estudio`/`_ejecutar_fuente` → `consultar_antecedentes_sync(cedula, nombres=, apellidos=)` → `_resolver_captcha_documento` resuelve primer/segundo nombre y primer/segundo apellido de forma determinista; Gemini también recibe la pista como contexto. Sin pista o "tercer nombre" → None → Gemini/reintento. CLI del bot: `python Funciones/bot_procuraduria.py CEDULA [NOMBRES] [APELLIDOS]`. 201 tests OK. |
| 2026-09-01 | **Leyenda oficial SU-458 COMPLETA en el PDF de Policía (pedido del usuario)**: la sección Policía ahora imprime VERBATIM el disclaimer legal del portal ("En cumplimiento de la Sentencia SU-458 del 21 de junio de 2012… extinción de la condena o la prescripción de la pena… solo aplica para el territorio colombiano…") como constante fija `LEYENDA_SU458_POLICIA` — el `mensaje` que captura el bot es SOLO la línea del veredicto, y este texto es igual para toda consulta. Test `test_leyenda_oficial_su458_completa` (aserciones wrap-tolerantes: pdfplumber mete `\|` en los saltos de línea). 203 tests OK. |
| 2026-09-01 | **Fix sync v3.4 por par (plan, fuente)**: el skip de `sincronizar_fuentes_planes` era por fuente SOLA — una entrada de OTRO plan (BASICO·simit) impedía materializar la del plan editado (AVANZADO·simit) y el acordeón del portal no mostraba simit en AVANZADO aunque el catálogo la incluyera. El skip ahora es por (plan_id, fuente), igual que el guard `$elemMatch` del `$push`. Regresión `test_fuente_de_otro_plan_no_bloquea_la_entrada`. BD curada en vivo (entrada AVANZADO·simit creada por sync_catalogo). OJO en tests: `plan_doc()` sin `plan_id` comparte la constante PLAN_ID — en escenarios multi-plan hay que pasar ids distintos. 202 tests OK. |
| 2026-09-01 | **Fuente "sena" — Certificados de formación SENA, SEXTA fuente**: consulta por **CÉDULA** del portal público Certificado Digital (`certificados.sena.edu.co`, GeneXUS server-rendered — NO JSF/SPA: ids `vNUMERO_DOCUMENTO`/`vCAPTCHATEXT`/`#CONSULTAR` estables; selects Documento+CC default). Descubrimiento con sonda `scripts/probar_sena.py`: captcha de **imagen GIF embebido** (data:image → 2Captcha `method=base64` ~US$0.001), resultado en la misma página (grid `tr#GridceContainerRow_NNNN` con spans vVAR1/vTITULO_OBTENIDO/vTIPO/vPROGRAMA/vFECHA_CERTIFICACION/vFECHA_CARGA_PDF; link `com.sena.guardar?var1=` por certificado — NO se descarga). **Trampa calibrada**: "No hay resultados" es el estado INICIAL y también lo del captcha fallido (ErrorViewer "El texto digitado no corresponde con la imagen") → el bot usa la IMAGEN DEL CAPTCHA CAMBIADA como evidencia de postback procesado; sin señal → `BotSenaSinResultado` (anti-envenenamiento). Bot `Funciones/bot_sena.py` (fechas "09 Febrero, 2013"→ISO, excepciones espejo runt, `_LOCK`, dumps `descargas_sena/`). Integración: `FUENTES` +sena, caché por (tipo, cédula) default, `_estado_sena` SIEMPRE EXITO (formación ≠ antecedente — decisión de negocio; jamás credencial verificada), `MAX_CERTIFICADOS_DOC=20`, defaults+seed+sena, endpoint v1 `GET /seguridad/sena` (503 sin key), script `habilitar_fuente_sena.py`. PDF: sección 4d "Formación SENA — Certificados" (banner REGISTRA N / SIN CERTIFICADOS, tabla ≤10 con columnas ensanchadas para que las fechas no se partan), fila resumen, reintentos, párrafo legal honesto (NO constituye verificación de títulos). Frontend: admin (ETIQUETAS_FUENTE + checkbox), portal (nombreFuente, badge 🎓, SEGUNDOS_FUENTE 70 s), FuenteEstudio.sena. E2E real: 1010213062 = 13 certificados (TECNÓLOGO EN GESTIÓN DE LA PRODUCCIÓN INDUSTRIAL 2013, HIGIENE Y MANIPULACIÓN DE ALIMENTOS 2023…) y 1033688842 vacío legítimo. 215 tests OK (+13: 6 TestFuenteSena, 7 TestSeccionSena). |
| 2026-09-01 | **EL PLAN ES EL GATE — `fuentes_habilitadas_efectivas` (pedido del usuario tras el fallo de sena en BASICO)**: el usuario asignó sena al plan BASICO y salía DESHABILITADA (ES-85DB8C510811) porque `config.fuentes_habilitadas` de la empresa no la listaba — el mismo paso de deploy olvidado de simit. Semántica nueva en el orquestador (`FUENTES_OPT_IN = {policia}` + `fuentes_habilitadas_efectivas(empresa)`, usada en `crear_estudio` y el fallback de `ejecutar_estudio`): fuentes DEFAULT corren de facto para TODA empresa; solo policia exige presencia explícita en `config.fuentes_habilitadas`; `config.fuentes_excluidas` apaga una fuente puntual por empresa. Editar un plan (o crear una fuente del catálogo) se refleja INMEDIATAMENTE en todos los clientes — los scripts `habilitar_fuente_*.py` quedan OBSOLETOS. `datos.placa`/`cedula_propietario` del POST ahora con getattr (SimpleNamespace de integraciones). Fix CSS portal: `.PS-boton-pdf + .PS-boton-pdf { margin-left: 10px }` (informe y anexo PGN pegados). 220 tests OK (+5 TestFuentesHabilitadasEfectivas). |
| 2026-09-01 | **Fuente `ofac` — Lista SDN / Lista Clinton, SÉPTIMA fuente**: comparación exacta por cédula contra el XML oficial de OFAC (`bot_ofac.py`), índice en memoria con refresco 6 h y SHA-256 auditable. Sin captcha, Gemini ni 2Captcha. Integrada al catálogo, defaults/seed, caché 24 h, endpoint v1, planes/admin/portal y PDF. Coincidencia → ADVERTENCIA + revisión humana, nunca rechazo automático. Caso oficial de regresión: cédula `208079`, UID `56062`, programa `ILLICIT-DRUGS-EO14059`. |
| 2026-09-01 | **Fuente `ofac_nit` — OFAC empresarial, OCTAVA fuente**: separada de `ofac`; recibe `nit` completo con dígito de verificación y solo indexa identificadores tributarios inequívocos (`Tax ID No.`/códigos fiscales), evitando `Registration Number` por riesgo de falsos positivos. Comparte el mismo XML/índice en memoria, pero tiene endpoint (`/seguridad/ofac-nit`), caché, plan, formulario, resultado y sección PDF propios. |
| 2026-09-02 | **Fix PGN rediseñado (reporte "Fuente no disponible")**: la PGN cambió su página de resultado — el veredicto llega INLINE ("El ciudadano no presenta antecedentes", sección "Datos del ciudadano") y ya NO hay `#btnDescargar` ni PDF; los regex viejos no matcheaban → `BotProcuraduriaSinResultado` → NO_DISPONIBLE. Bot: espera `.datosConsultado, #btnDescargar` (60 s; el spinner "Consultando por favor espere..." tarda >45 s) + fórmulas nuevas con el "no" chequeado primero; regex del PDF quedan de compatibilidad. Consecuencia: `anexo_procuraduria` queda vacío en estudios nuevos (el botón "Certificado Procuraduría" del portal no aparece — el veredicto sí llega). Sonda: `scripts/sondar_procuraduria_hoy.py`. |
| 2026-09-02 | **Fuente "contraloria" — Antecedentes Fiscales CGR (persona natural), NOVENA fuente**: consulta por **cédula** (tipo doc fijo CC) del Certificado de Antecedentes Fiscales contra el SIBOR. Descubrimiento con sonda `scripts/probar_contraloria.py`: portal Liferay que embebe el form en IFRAME WebForms `cfiscal.contraloria.gov.co` (`#ddlTipoDocumento` value "CC", `#txtNumeroDocumento`, `#btnBuscar`), **reCAPTCHA v2** (2Captcha `userrecaptcha`, token inyectado en el textarea del IFRAME — el servidor valida el campo POST). **Trampas**: (a) el validationGroup exige TRES radios de una ENCUESTA OCULTA (`rbdExpectativa/rbdOportunidad/rbdUtilidad`) — sin marcarlas el postback se bloquea EN SILENCIO; (b) postback PARCIAL async (sin navegación); (c) el certificado llega como DESCARGA `{cedula}.pdf` procesada en memoria (pdfplumber): veredicto "NO SE ENCUENTRA REPORTADO COMO RESPONSABLE FISCAL" (EXITO) / "SE ENCUENTRA REPORTADO…" (ADVERTENCIA) + **codigo_verificacion** (para constatar ante la CGR). Bot `Funciones/bot_contraloria.py` (excepciones espejo policía, `_LOCK`, dumps `descargas_contraloria/`). Integración: `FUENTES` +contraloria (**y en `_SYNC_ORIGINALES`** — trampa: sin registro ahí `_invocar_playwright` lanza KeyError), `_clasificar_error`, caché (tipo, cédula) shape `{no_registra, mensaje, codigo_verificacion}` anti-envenenamiento, endpoint v1 `GET /seguridad/contraloria`, PDF sección 3b semaforizada + código de verificación + párrafo legal (Decreto 2150/1995), `/VerificarEstudio`, admin (checkbox) y portal (badge ✅ Sin responsabilidad fiscal / ⛔ Reportado). Va en defaults (certificado de terceros por diseño). E2E real 1033688842: EXITO con reintento (validó el camino de reintentos) + hit de caché. 165 tests OK (+2 TestSeccionContraloria). ~US$0.003/consulta sin caché. |
| 2026-09-02 | **Fix Windows dev: Playwright NotImplementedError bajo uvicorn**: en Windows `asyncio.create_subprocess_exec` (Playwright) solo funciona con ProactorEventLoop, pero uvicorn `--reload` pisa la política Proactor de `main.py` con `WindowsSelectorEventLoopPolicy` → las fuentes Playwright del estudio morían con NotImplementedError (fuentes en ERROR, estudio PARCIAL). Fixes: `_invocar_playwright` corre la corutina del bot en un HILO con ProactorEventLoop propio cuando `sys.platform == "win32"`, y el `lifespan` de `main.py` re-fuerza la política Proactor (cubre los `asyncio.run` de los hilos de los endpoints v1). En Render/Linux no-op. |
| 2026-09-03 | **Fuente "rues" — Registro Mercantil del RUES (Confecámaras), DÉCIMA fuente por NIT sin DV**: PRIMER bot SIN navegador del módulo — la SPA de rues.org.co consume un API Elasticsearch público (`elasticprd.rues.org.co`) con payloads **AES cifrados** (CryptoJS OpenSSL Salted, passphrase "qwerty" embebida en el chunk services/127; descubierta y replicada con pycryptodome, nueva dependencia en requirements). Bot `Funciones/bot_rues.py` (molde bot_ofac): /query con Status ACTIVA+CANCELADA + match EXACTO de numero_identificacion (trampa: NIT inexistente lista total 10000), DetalleRM (estado/renovaciones/societario/CIIU) y representante legal best-effort (HTML con &nbsp;); estado espaciado "A C T I V A" normalizado; ~1-2 s y $0 por consulta. Semáforo (decisión de negocio, análoga SOAT/SIMIT): matrícula ≠ ACTIVA → ADVERTENCIA; NIT sin registro → EXITO determinante. Integración: `FUENTES` +rues (remapeo nit como ofac_nit, sin `_SYNC_ORIGINALES` ni semáforo de navegadores), `_estado_rues`, `_clasificar_error` (BotRues*→NO_DISPONIBLE), caché 24 h por (tipo, NIT en campo cédula) + anti-envenenamiento doble barrera, endpoint v1 `GET /seguridad/rues` (422/502/503), POST del estudio: rues en los sets de cédula-no-requerida y nit-requerido, PDF sección 4g semaforizada (banner verde ACTIVA+renovación / ámbar no activa / neutro sin registro) + tabla + representantes + párrafo legal honesto (informativo, NO es el Certificado de Existencia) + fila resumen + reintentos. Frontend: ETIQUETAS_FUENTE + checkbox admin, portal `pideNit` incluye rues / `pideCedula` lo excluye (ambos sitios), badge ✅/⛔/🔍, SEGUNDOS_FUENTE 15 s. Sonda `scripts/probar_rues.py` (--estructura / --descubrir-endpoints con Playwright UA real — el CDN bloquea HeadlessChrome). Env: `SEGURIDAD_RUES_SECRET_KEY/_API_URL/_TIMEOUT_S`. E2E real: endpoint v1 vivo 1,5 s + hit de caché 0,2 s; estudio ES-17133CFF11D9 COMPLETADA con PDF verificado (reutilizó la caché del v1). 239 tests OK (+10: 6 TestFuenteRues, 4 TestSeccionRues). Deploy: requirements +pycryptodome; sin migraciones ni scripts (plan es el gate). |
| 2026-09-04 | **Fechas unificadas a dd/mm/aaaa en TODO el PDF del estudio (pedido del usuario)**: cada fuente traía su formato (ISO `aaaa-mm-dd` de runt/simit/sena/rues, `aaaa/mm/dd` del RNDC, US `mm/dd/aaaa` del `Publish_Date` de OFAC, ISO datetime `aaaa-mm-ddThh:mm:ss` de rama judicial, `aaaa-mm-dd` en la marca de agua) y el informe los pintaba crudos. Helper `_fecha_legible` (pdf_estudio_seguridad.py): parsea ISO/slash/US → `dd/mm/aaaa`, conserva la hora cuando viene con espacio (tabla "Fecha Hora Radicación" del RNDC), descarta el sufijo `T…` de ISO, y pasa INTACTO lo que no es fecha parseable (celdas de datos de portales, "VIGENTE", fechas imposibles) — vacío → "—". Ambigüedad dd/mm vs mm/dd: slash se asume US mm/dd salvo que el día no quepa como mes (primero > 12 → dd/mm). Aplicado en: marca de agua, ventana RNDC, columna de fecha de la tabla de manifiestos, SOAT RUNT (banner VENCE, vigencia, historial de pólizas, fila resumen, `fecha_matricula_inicial`), SIMIT `fecha_imposicion`, SENA certificación/firma, OFAC `fecha_publicacion` (secciones cédula y NIT), rama judicial `fechaProceso`, RUES (banner RENOVADA HASTA, matrícula/renovación/cancelación). La cuenta de cobro ya iba en dd/mm/aaaa (`_fecha_colombia` de pdf_cuenta_cobro.py) — sin cambios. 247 tests OK (+8 TestFechaLegible + regresión "ningun ISO sobrevive en el PDF completo"). |
| 2026-09-04 | **Presupuesto de 300 s para procuraduría (pedido del usuario)**: el presupuesto de pared pasa de un valor global único a POR FUENTE — `TIMEOUTS_FUENTE_S` + `_timeout_fuente(nombre)` en el orquestador (`SEGURIDAD_PROCURADURIA_TIMEOUT_S`, default 300; las demás siguen con `SEGURIDAD_TIMEOUT_FUENTE_S` 150). El presupuesto cubre los 2 intentos (wait_for + reintento) igual que antes. `_clasificar_error` recibe `nombre` para que el mensaje de timeout diga el presupuesto real de la fuente (antes "150 s" para todas). Bot PGN: la espera del postback `.datosConsultado/#btnDescargar` sube de 60 s a **180 s** — se rendía antes de agotar el presupuesto y leía la página a medias (SinResultado prematuro que gastaba el intento); navegación+captcha+180 s caben en los 300. 248 tests OK (+1 test_procuraduria_tiene_presupuesto_propio_300s). |
| 2026-09-14 | **Fuente `onu_ue` — Listas internacionales de sanciones ONU/UE (inspirada en el reporte de TusDatos, sprint 1)**: agrega en UNA consulta la Lista Consolidada del Consejo de Seguridad ONU (~2 MB, 736 personas, `dateGenerated` 2026-09-12) y el Consolidated Financial Sanctions File 1.1 de la UE (~25 MB, 4.462 personas, generación 2026-08-05). Bot `Funciones/bot_sanciones.py` (molde `bot_ofac`: sin navegador/captcha/$0, índice en memoria TTL 6 h): solo indexa IDs inequívocos de cédula (ONU `National Identification Number` → 151 docs; UE `identificationTypeCode="id"` persona → 221 docs; pasaportes/taxid/fiscalcode EXCLUIDOS), coincidencia EXACTA → ADVERTENCIA + revisión humana (`_estado_ofac` reutilizado), degradación honesta por lista (`listas_no_disponibles`; ambas caídas → NO_DISPONIBLE `sanciones_no_disponible`), anti-envenenamiento doble barrera (sin metadatos de ninguna lista no se cachea). Integración: `FUENTES` +onu_ue, invocación to_thread, caché (tipo, cédula), endpoint v1 `GET /seguridad/sanciones`, PDF sección 4g semaforizada (banner + tabla con metadatos POR LISTA + coincidencias con lista/referencia/programa + párrafo legal honesto), fila resumen, reintentos, `/VerificarEstudio`, seed/labels admin (ETIQUETAS_FUENTE + checkbox), portal (nombre, mensaje de espera, SEGUNDOS_FUENTE 25 s, badge ✅/⛔ con listas no disponibles). **Hallazgos de la sonda**: (a) la descarga anónima del FSF 1.1 en webgate responde EU Login — la URL oficial tokenizada (`?token=dG9rZW4tMjAxNw`, documentada en data.europa.eu) es la que funciona, env `SEGURIDAD_UE_XML_URL`; (b) UK OFSI migró el 28/01/2026 a una single list SIN descarga masiva (solo API POST del SPA search-uk-sanctions-list) → fuera del sprint, seguimiento; (c) el XML ONU viejo `resources/xml/en.xml` está 404 — el vigente es `resources/xml/en/consolidated.xml`. E2E real: `python scripts/probar_sanciones.py 1033688842` (ONU 736 + UE 4462 indexadas, ~30 s primera descarga, sin coincidencias). 262 tests OK (+14: 7 bot, 5 orquestador, 2 PDF). Sin migraciones ni scripts de deploy (plan es el gate). |
| 2026-09-14 | **RTM del vehículo en runt (sprint 1 parte 2, pedido "paz y salvo")**: sondeo en vivo del portal (MVX48E/1010213062, 3 corridas por fallos de captcha — 1 imagen no renderizó, 1 rechazo) reveló que la página del VEHÍCULO **NO tiene panel de multas/paz y salvo** (el de TusDatos es de la consulta del CONDUCTOR con login) pero sí 20 paneles en el acordeón; el bot ahora expande además "Certificado de revisión técnico mecánica" y parsea `_RE_RTM` (tipo/expedición/vigencia/CDA/vigente/certificado/consistente; primera fila = vigente). `rtm` + `revisiones[]` en bot/caché/sección; **RTM vencida → ADVERTENCIA** (análogo SOAT, recálculo por fecha en cada hit de caché — test con vigencia de ayer). PDF: banner combinado, filas RTM, historial ≤3, resumen actualizado (la sección RUNT pasó a ocupar página propia: test de paginación 4→5). Portal: badge ⛔ RTM vencida / mención en el de SOAT. Pendiente documentado: panel "Tarjeta de Operación" (solo vehículos de carga — calibrar con placa de flota) y Pólizas RC. 267 tests OK (+5: 2 regex/semáforo, 1 caché-recálculo, 2 PDF). |
| 2026-09-14 | **Sprint 2 — sondas (hallazgos, sin implementación aún)**: (1) **Inhabilidades por delitos sexuales contra menores (Ley 1918/2018) VIABLE**: `inhabilidades.policia.gov.co:8080` responde 200 server-rendered; form POST `/consulta` (JSESSIONID en el action) con `tipo` CC/CX/PA, `nuip`, **`fechaExpNuip` DD/MM/AAAA obligatoria** (fecha de expedición de la cédula — campo nuevo del POST del estudio), `nombreEmpresa`/`nitEmpresa` (la empresa CONSULTANTE), reCAPTCHA v2 con sitekey en el DOM y checkbox de términos → molde `bot_contraloria` + `fecha_expedicion` nuevo; consulta pública de terceros POR DISEÑO (Ley 2375/2024 la extendió a entrenadores/parques) → iría en defaults. Plan detallado en §12. (2) **INPEC BLOQUEADO desde la red local**: el buscador de internos ni abre con Chromium real (goto timeout 90 s; HTTP plano también; WebFetch rechaza el certificado) — reintentar la sonda `scripts/probar_inpec.py` desde el Shell de Render (IP US) antes de descartar. (3) **Defunciones Registraduría FUERA DE ALCANCE**: el webservice es solo para entidades conveniadas (DIAN) y el portal ciudadano SCCRC es del titular — lo que muestra TusDatos no es replicable por canal público honesto. |
| 2026-09-15 | **Procuraduría SIEMPRE de última en el PDF (pedido del usuario)**: la sección de detalle de la PGN pasa de 2ª a última fuente del informe (después de RUES, antes de Trazabilidad), y el mismo orden se aplica en la tabla "Resumen por fuente" de la portada (fila + `estados_resumen` movidos al final — el sombreado de estados mapea por índice), en "Reintentos por fuente" de trazabilidad y en los párrafos de Disposiciones legales. Motivo: la PGN es la fuente más lenta (300 s) y el veredicto disciplinario queda como cierre. Test de paginación actualizado (el informe fixture pasa a 4 páginas; la página 2 abre con la continuación de la tabla RTM del RUNT — SIMIT a media página es continuación legítima). 203 tests OK (67 PDF + 136 estudios). |
| 2026-09-15 | **Fix contraloria: WAF en el portal Liferay (fuente caída "portal_inconsistente")**: la CGR puso `www.contraloria.gov.co` detrás de un WAF (Azure Application Gateway v2) que responde **403 Forbidden incluso a Chromium real** → el iframe de `cfiscal` jamás cargaba y la fuente abortaba en ~12 s ("iframe ausente", 2 intentos; visto en ES-F53DBBEF88B0/ES-A1CD3018EA4D con 1010213062). El WebForms sigue respondiendo DIRECTO → el bot ahora entra a `FORM_URL = cfiscal.contraloria.gov.co/Certificados/CertificadoPersonaNatural.aspx` (si la página principal ES el WebForms, "vista" = frame principal; queda el loop de iframe como compat si volviera a venir embebido). Verificado E2E real 1010213062: certificado descargado, NO SE ENCUENTRA REPORTADO, código 1010213062260915205134. Tests Contraloria PDF OK. ⚠️ Riesgo nuevo a vigilar: si la CGR extiende el WAF a `cfiscal`, la fuente cae de nuevo (reverificar con sonda `--solo-formulario`). |
| 2026-09-14 | **Fuente `delitos_sexuales` — Inhabilidades Ley 1918 (sprint 2A COMPLETO)**: décimo-quinta capa, DUODÉCIMA fuente. Calibrada con consultas REALES (2 solves de sonda + 1 E2E del bot, ~US$0.009): 1010213062/14/02/2012 con GLAMPEROS como consultante → "NO REGISTRA INHABILIDAD" + hora DIJIN 19:32:09. **Descubrimientos**: (a) main.js OBFUSCADO decodificado — jquery.validate exige `grecaptcha.getResponse()` y habilita `#btnConsultar` con el checkbox de términos, pero el submit NATIVO (`form.submit()`) NO dispara los handlers → stub del getResponse + campo oculto `#captcha` con el token; (b) el NIT de la consultante exige DV (máscara 000000000-0, patternNit regex) → el bot calcula el DV por módulo 11 si falta (pesos 3,7,13… de derecha a izquierda — el primer intento con pesos invertidos daba DV 1 en vez de 2, atrapado por test); (c) el resultado estampa hora DIJIN + empresa consultante + fundamento legal completo (Ley 1918, Decreto 753/2019, Leyes 1581/1712). Bot `Funciones/bot_delitos_sexuales.py` (molde contraloria, excepciones espejo, la página debe traer la cédula consultada). Integración: `FUENTES` +delitos_sexuales (defaults, NO opt-in: consulta de terceros por diseño), `_SYNC_ORIGINALES`, `_clasificar_error` (SinCaptchaKey→config_faltante, SinResultado→portal_inconsistente, CaptchaFallido→captcha), campo NUEVO `fecha_expedicion` en POST/CrearEstudio (422 accionable si la fuente corre; persiste en el doc), `empresa_consultante` derivada del doc de la empresa (nombre+NIT) en `_ejecutar_fuente`, caché 24 h (tipo, cédula) shape `{no_registra, mensaje, fecha_consulta, empresa_consultante}`, anti-envenenamiento doble barrera (fecha equivocada → sin fórmula → NO cachear). PDF sección 3c semaforizada (banner verde/rojo "REVISIÓN HUMANA OBLIGATORIA", tabla con hora DIJIN + consultante) + fila resumen + reintentos ("Inhabilidades 1918") + párrafo legal honesto. Frontend: admin (ETIQUETAS + checkbox), portal (nombre, espera 110 s, **campo "Fecha de expedición de la cédula (DD/MM/AAAA)"** solo si el plan la incluye con validación dd/mm/aaaa + ayuda, badge ✅/⛔, `crearEstudio(..., fechaExpedicion)`), tipos FuenteEstudio (+fecha_consulta/empresa_consultante). 278 tests OK (+11: 4 bot, 5 orquestador, 2 PDF). **Deploy: env SEGURIDAD_DELITOS_CAPTCHA_KEY (o fallback API_KEY_CAPTCHA ya existente); sin migraciones (plan es el gate).** Sin endpoint v1 individual (el estudio es el canal; añadir después si hace falta — precedent bdme/rama_judicial). |
| 2026-09-24 | **Evidencias de consulta en el PDF (pantallazos por fuente, patrón TusDatos — pedido del usuario) + timeout de procuraduría a 90 s**: (1) **Evidencias**: cada bot Playwright (rndc, procuraduria, contraloria, delitos, policia, runt, simit, sena, bdme/bdme_nit, rama_judicial) fotografía el VIEWPORT de la página de resultado (`captura_jpg` JPEG q~70; helper `Funciones/captura_evidencia.py`, try/except → None — JAMÁS tumba una consulta). Orquestador: `FUENTES_NAVEGADOR` + `seccion["_captura"]` (volátil, fuera del doc) en la rama portal Y restaurada del caché en hit (`_con_captura` adjunta `captura_jpg` al doc de caché 24 h — decisión del usuario: un estudio de caché muestra la MISMA evidencia); `ejecutar_estudio` sube cada captura a GCS privado `...{consulta_id}_captura_{fuente}.jpg` (ruta_blob con `ext` parametrizable, content_type image/jpeg) y persiste `evidencias{fuente: {gcs_ruta, sha256, tamano, capturado_en}}` en el doc — la REGENERACIÓN relee de GCS; fallo de subida → `evidencia_error` sin tumbar el estudio. PDF: sección final "Evidencias de consulta" (intro + 1 página por fuente en orden canónico con título legible, fecha/hora Colombia `_hora_colombia` y sha corto; imagen escalada por ImageReader; blob irrecuperable → placeholder honesto); reproducible (JPEG DCTDecode sin recomprimir + invariant=1 — test de igualdad byte a byte). Docs viejos sin `evidencias`: sección omitida. PGN: `scroll_into_view_if_needed` del `.datosConsultado` antes de capturar (el veredicto vive en iframe). Sin cambios de frontend ni migraciones. 281 tests OK (+12: 7 orquestador/ejecutar_estudio, 2 helper, 1 ruta_blob, 4 PDF — TestSeccionEvidencias). (2) **Timeout PGN 300→90 s** (pedido del usuario; env revertible sin deploy): default de `SEGURIDAD_PROCURADURIA_TIMEOUT_S` a 90 y la espera interna del postback del bot de 180 s a `SEGURIDAD_PROCURADURIA_ESPERA_MS` (default 60 s) — navegación+captcha+60 s caben en los 90; con 90 s el REINTENTO completo ya no cabe (solo fallos rápidos reintentan) y postbacks PGN >60 s en horas pico serán NO_DISPONIBLE con más frecuencia (riesgo aceptado por el usuario). Test renombrado a `test_procuraduria_tiene_presupuesto_propio_90s`. |
| 2026-09-24 | **Tarde — E2E real + 3 fixes en caliente (cédula 79882073, plan AVANZADO GLAMPEROS)**: (1) **Evidencias en el PDF: FIX reportlab** — `Image(lector)` con `ImageReader` revienta en reportlab 4.x ("expected str, bytes…") → TODAS las evidencias caían al placeholder aunque el blob estaba en GCS (los tests no lo atraparon: no incrustan imágenes reales); fix `Image(io.BytesIO(datos_img))`, verificado regenerando ES-28C0DBA1EA8A (5 imágenes, 0 placeholders). (2) **Contraloría: la evidencia es la HOJA del certificado** (pedido del usuario — el pantallazo del form no prueba nada porque el resultado llega como DESCARGA): `certificado_pdf_a_jpeg(pdf_bytes)` en captura_evidencia rasteriza la 1ª página del PDF descargado con **pypdfium2** (nueva dependencia en requirements), fallback a viewport si falla; la evidencia de ES-28C0DBA1EA8A se reparó en vivo (blob sobrescrito en GCS + sha/tamano del doc actualizados + PDF regenerado, 617 KB). (3) **Timeout PGN 90→150 s** (el run real de las 11:55 falló a los 90.01 s exactos y con 150 completó en 101 s — postback PGN en horas pico). E2E: estudio 7 fuentes COMPLETADA en 108,6 s (PGN 101 · rama judicial 33 · SENA 31 · policía 26 · contraloria 16 · ONU/UE 9 · OFAC 5 — paralelo: total = la más lenta + ~7 s de overhead). 288 tests OK (217 estudios+PDF tras renombrar el test de presupuesto a 150 s). |
| 2026-09-24 | **Branding seguriDatia + navegación del informe (pedidos del usuario)**: (1) **MARCA = "seguriDatia"** (constante en pdf_estudio_seguridad; `LOGO_SEGURIDATA=None` queda listo para el logo que el usuario entregará después — dibujarlo en `_encabezado` al llegar; la marca de agua ya no dice "Integra Logística"). (2) **Botón "ver evidencia"**: el nombre de cada fuente en la tabla "Resumen por fuente" es un LINK INTERNO (`<a href="#ev_<fuente>">`, azul subrayado — solo si la fuente tiene evidencia) a la página de su pantallazo (ancla `<a name="ev_<fuente>"/>` en el título de la sección Evidencias). ⚠️ **FIX NumberedCanvas**: el two-pass de "Página X de Y" ligaba TODOS los anchors a la página 1 (bookmarkPage captura thisPageRef() del armado, cuando solo existe una página del doc); `bookmarkPage` ahora registra las anclas de cada página y `save()` las RE-LIGA a la página real del replay — verificado: contraloria→6, policia→7, sena→8, rama_judicial→9, procuraduria→10. (3) **Pie legal en TODAS las páginas**: disclaimer de seguriDatia (datos públicos, calidad atribuible a la fuente, no administra las fuentes) a 5,2 pt + línea "Este reporte fue generado el {dd, Mes aaaa a las h:mmAM/PM}. Reporte generado por: {usuario_nombre}. Correo: {usuario_correo}." (VARIABLES del doc; fecha desde creado_en — reproducible; `_fecha_pie_generado` con meses en español). Reemplaza la línea "Documento confidencial" (queda Página X de Y + consulta_id abajo). 217 tests OK. |
| 2026-09-25 | **Fuente `sisconmp` — Capacitaciones de Mercancías Peligrosa (Mintransporte), DECIMOSÉPTIMA capa / 13ª fuente productiva**: consulta por cédula del portal público SISCONMP (`web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/`, Resolución 1223/2014). Descubrimiento con sonda `scripts/probar_sisconmp.py`: MVC server-rendered; el "botón" es un `<span>` con onclick (no `<button>`); **reCAPTCHA v3 INVISIBLE** (`grecaptcha.execute` action `consultar_conductor`) que la PROPIA página resuelve → **$0, sin 2Captcha** (la primera corrida de la sonda gastó un solve inútil — el token del solver no participa del flujo; verificado headless); la consulta es un AJAX POST `ConsultarConductor` {TDI, NDI, recaptchaToken} → ARRAY JSON (campos documentados desde el JS del portal). ⚠️ **Trampa**: el handler `error:` muestra el MISMO "No se encontrarón registros" para server caído (403/500) y vacío legítimo → el bot (`Funciones/bot_sisconmp.py`) captura la RESPUESTA AJAX (`expect_response`) y JAMÁS lee el DOM; non-200 o JSON no-lista → excepciones espejo simit. Fechas ASP.NET `/Date(ms)/` UTC → ISO hora Colombia. **Semáforo de VIGENCIA (decisión de negocio, análogo SOAT/RTM)**: registra capacitaciones, ≥1 legiblemente vencida y ninguna vigente → ADVERTENCIA (recalculada en cada hit de caché); alguna vigente o vacío determinante → EXITO. Integración: `FUENTES` +sisconmp (defaults, NO opt-in), `FUENTES_NAVEGADOR` (evidencia), `_SYNC_ORIGINALES`, `_clasificar_error`, caché 24 h (tipo, cédula) shape `{no_registra, mensaje, apellidos, nombres, total_capacitaciones, capacitaciones[]}` con anti-envenenamiento doble barrera, PDF sección 4d-bis (banner de vigencia + tabla + párrafo legal honesto Resolución 1223, informativo) + fila resumen + reintentos + evidencias, frontend admin (ETIQUETAS + checkbox) y portal (nombre, mensaje de espera, badge ✅/⛔/🎓), FuenteEstudio.tipos. E2E real: 1010213062 = vacío legítimo, 10,8 s + hit de caché vía `_ejecutar_fuente`; **calibrada con registro vivo 79882073 (2026-09-25)**: DIDIER ALBEIRO PIÑEROS ARIZA, 2 cursos básicos MEN — ACTUALIZACION (INSTITUTO LÍDER DE COLOMBIA, vence 2027-09-21, VIGENTE) + CURSO BASICO (EDUTRANS, venció 2020-02-01) → fuente EXITO por la vigente; fechas `/Date(ms)/`, nombre del ciudadano y semáforo verificados contra el portal. 297 tests OK (+16: 6 TestFuenteSisconmp, 3 TestFechasSisconmp, 7 TestSeccionSisconmp). **Sin migraciones ni env obligatorios (plan es el gate)**. |
| 2026-09-25 | **Fuente `situacion_militar` — Libreta Militar (Ejército Nacional), DECIMOCTAVA capa / 14ª fuente productiva**: consulta por cédula del certificado público de estado de situación militar. Descubrimiento con sonda `scripts/probar_libreta.py`: SPA React (form `#tipoDocumento` cc/ti + `#numeroDocumento`; el botón queda disabled sin tipo doc; el `data:image` del DOM es un SVG decorativo, NO captcha) cuyo submit es un **GET directo al generador de certificados** que responde a requests PURO (molde `bot_rues`: ~1-2 s, $0, sin navegador ni captcha). 200 = **certificado PDF** procesado en memoria con pdfplumber (nombres, **Estado Tarjeta Militar** — RESERVISTA 1RA/2DA CLASE vistos —, fecha "a los N días del mes de X de YYYY"→ISO); el PDF NO se persiste (minimización); 400 "El tipoDocumento no coincide con el registrado para el ciudadano" = **no_registra determinante** (cédula no existe con CC). **Evidencia sin navegador**: 1ª hoja del certificado rasterizada (`certificado_pdf_a_jpeg`, patrón CGR) — la fuente se registró en `FUENTES_NAVEGADOR` solo por el gate de evidencias. **Legalidad**: el portal declara la consulta pública sin autorización del titular (art. 10 Ley 1581/2012; Ley 1861/2017, Decreto 977/2018, Ley 1184/2008) → defaults. **Semáforo (decisión de negocio)**: PENDIENTE/NO DEFINIDO/REMISO/APLAZADO → ADVERTENCIA (obligación militar vigente = riesgo operativo para conducción); definido o sin registro → EXITO. Integración: `FUENTES` +situacion_militar, `_clasificar_error` (SinResultado→portal_inconsistente, Error→situacion_militar_no_disponible NO_DISPONIBLE), caché 24 h (tipo, cédula) shape `{no_registra, mensaje, nombres, apellidos, nombre_completo, estado_tarjeta_militar, fecha_expedicion}` anti-envenenamiento doble barrera, PDF sección 4h (banner verde/ámbar + datos del certificado + párrafo legal honesto) + fila resumen + reintentos + evidencias, frontend admin/portal (badge "Libreta: ✅ RESERVISTA…"), FuenteEstudio.tipos. E2E real: 1010213062 = RESERVISTA - 2DA CLASE + hit de caché + 99999999 sin registro. 310 tests OK (+13: 6 TestFuenteSituacionMilitar, 7 TestSeccionSituacionMilitar). **Sin migraciones ni env obligatorios**. |
| 2026-09-25 | **Memoria de personas consultadas (capa 19, transversal)**: nueva colección `personas_seguridad` (cédula unique + nombres/apellidos informados + `nombre_consultado` VERIFICADO cascada PGN→Policía + `fecha_expedicion` + `empresas[]` + `total_consultas`) — cada POST del estudio hace upsert NO destructivo (`Funciones/personas_seguridad.py::registrar_consulta_persona`; best-effort: jamás tumba un estudio; `total_consultas` SOLO por `$inc` — `$setOnInsert`+`$inc` en el mismo campo es error de operadores en conflicto). Usos: (1) `_completar_desde_memoria` en el POST completa lo que el body NO trajo (salva 422 de delitos_sexuales/rama_judicial y alimenta el captcha PGN; lo del body gana); (2) endpoint `GET /seguridad/estudios/personas?cedula=` (registrado ANTES de `/{consulta_id}`); (3) portal cliente autollena nombres/apellidos/fecha SOLO campos vacíos en el onBlur de la cédula (`buscarPersona` + aviso "Persona consultada antes (N consultas)", patrón OtrosCostos). **Aislamiento por tenant (decisión del usuario)**: solo se ve/usa memoria de personas que la MISMA empresa consultó (filtro `empresas`); otra → `encontrada:false`. Análisis comparativo con el reporte Blindado de TusDatos (cédula 208079, 77 págs) en la sesión: gaps de presentación (resumen ejecutivo jerarquizado Alto/Medio/Bajo — pendiente) y fuentes nuevas priorizadas (PEP/SIGEP elegida como próxima, luego RNMC, SICAAC/RGM). 320 tests OK (+10: 4 TestMemoriaPersonas, 3 TestCompletarDesdeMemoria, 3 TestEndpointPersonas). E2E real: POST 79882073 sin nombres con AVANZADO → memoria completó rama_judicial, 201 y total_consultas 1→2. **Sin migraciones (índices automáticos) ni env**. |
| 2026-09-25 | **Ficha SDN completa en las coincidencias OFAC (patrón TusDatos, pedido del usuario tras comparar su informe con el reporte Blindado)**: el match de `ofac`/`ofac_nit` arrastra ahora título/cargo, fecha y lugar de nacimiento (ítem mainEntry), nacionalidades, ciudadanías, direcciones (≤3), alias (≤5) y observaciones (≤400 chars) directamente del XML oficial que ya descargábamos, más el **link oficial** `sanctionssearch.ofac.treas.gov/Details.aspx?id={uid}` (`_ficha_sdn` en bot_ofac — calibrado con el XML vivo de uid 56062; esquema PublicationPreview: dateOfBirthList/dateOfBirthItem, placeOfBirthList/placeOfBirthItem, nationalityList, citizenshipList, addressList). El orquestador NO cambió (coincidencias viajan verbatim a sección/caché/doc); el PDF reemplaza el párrafo compacto por la tabla FICHA (`_ficha_coincidencia_ofac`: 2 columnas, filas solo si hay valor — cachés de 24 h previas sin los campos siguen pintándose como siempre; link `<link>` de reportlab NO pasa por escape). Análisis de la sesión (comparativa 77 págs TusDatos): PEP/SIGEP elegida como próxima fuente; backlog RNMC/SICAAC/RGM/boletín PGN de destituciones (nuestro certificado en vivo NO mostró la destitución vencida de 2015 que sí lista el boletín). 263 tests OK (+2: ficha bot con fixture enriquecido, ficha en PDF). E2E real: ES-E2BE183E5B59 (208079, solo ofac, 28 s con descarga de dataset) — PDF verificado con extracción filtrada por `char["upright"]` en pdfplumber. **Sin migraciones ni env**. |
| 2026-09-25 | **Resumen del PDF separado por respuesta (pedido del usuario: "solo van a ir las que sí trajeron respuesta")**: el cuadro "Resumen por fuente" de la portada lista SOLO fuentes EXITO/ADVERTENCIA; las NO_DISPONIBLE/ERROR van a un cuadro aparte **"Fuentes sin respuesta en esta consulta"** (columnas Fuente/Estado/Motivo, estado sombreado rojo, intro que aclara que no hicieron parte del resultado). **Mensajes de error limpios**: `_limpiar_mensaje_error` (pdf_estudio_seguridad) poda los stacktraces embebidos de Playwright ("Page.evaluate: TypeError: Failed to fetch at eval (…) at UtilityScript…" → "TypeError: Failed to fetch") y el prefijo técnico `Page.evaluate:` — aplica al cuadro de caídas (`_motivo_no_disponible`), al párrafo de estado de cada sección (`_parrafo_estado_fuente`) y a `_resumen_error` ("Sin respuesta del portal: …", antes "No consultada: …"). 265 tests OK (90 PDF + 175 estudios; +4 TestResumenFuentesSeparado, 2 actualizados). Verificado visualmente página 1. **Sin migraciones ni env**. |
| 2026-09-25 | **Completar fuentes pendientes + detalle discreto (pedido del usuario)**: (1) **PDF**: la sección de detalle de toda fuente caída dice SOLO "La fuente de consulta presenta indisponibilidad." en GRIS itálica (COLOR_NEUTRO) — sin estado con color de alarma ni error técnico del bot (la indisponibilidad y su motivo ya viven en el cuadro aparte del resumen). (2) **Completado del estudio**: `reintentar_fuentes_estudio` (orquestador) re-ejecuta SOLO las fuentes NO_DISPONIBLE/ERROR de un estudio cerrado (`fuentes_pendientes_estudio`), con los parámetros persistidos del doc (cédula, placa + cédula del propietario de `vehiculos[0]`, nombres/apellidos, NIT, fecha_expedición, empresa consultante), MERGEA sobre lo que ya respondió (fuentes, evidencias, anexos; lo intacto NO se re-consulta), recalcula el estado global y la cascada del nombre, `$inc completar_intentos` (tope 5 → 429) y evento `estudio_completado`. Endpoint `POST /{consulta_id}/completar` (CONSULTADOR/ADMIN_EMPRESA/ADMIN_INTEGRA, rate limit, SIN consumo de cobro nuevo) re-emite el PDF vía `_reemitir_pdf` (misma ruta GCS, versión +1 con historial) y devuelve `fuentes_completadas`/`fuentes_pendientes`; memoria de personas actualizada best-effort. (3) **Portal cliente**: botón **"Completar fuentes pendientes"** en la tarjeta de resultado (solo si hay pendientes, Swal con las fuentes a re-consultar, spinner, ding al terminar) y botón **"Completar"** en las filas PARCIAL/ERROR del historial; `completarEstudio` en seguridadCliente.tsx. 333 tests OK (269 estudios+PDF + 64 cobro; +4 TestCompletarFuentesPendientes, 2 actualizados) · tsc --noEmit OK. **Sin migraciones ni env**. |
| 2026-09-25 | **Cuadro "Fuentes sin respuesta" reducido a UNA columna (pedido del usuario: "solo debe mencionarlas sin más")**: eliminadas las columnas Estado y Motivo y el párrafo introductorio — el cuadro lista SOLO el nombre de cada fuente caída (una columna, sin sombreado rojo). Con esto el informe ya NO imprime el estado "NO DISPONIBLE"/"ERROR" por fuente en ninguna parte (antes salía en el resumen, el detalle y el cuadro aparte): la indisponibilidad se declara una sola vez, discreta (nombre en el cuadro + aviso gris en la sección de detalle). Los mensajes técnicos/motivo de los bots NO llegan jamás al PDF (solo a eventos/trazabilidad); `_motivo_no_disponible` eliminado. 90 tests PDF OK (6 de "fuente fallida" actualizados al aviso gris + mención simple; verificado visualmente la portada). **Sin migraciones ni env**. |
| 2026-09-25 | **Historial del portal con drill-down de fuentes pendientes (pedido del usuario)**: `GET /estudios` (historial) enriquece cada ítem con **`fuentes_pendientes[]`** (claves de fuentes NO_DISPONIBLE/ERROR) calculado por aggregation `$objectToArray`/`$filter`/`$map` sobre la clave dinámica `fuentes` (el listado sigue SIN payload de fuentes; fallo del cálculo ≠ fallo del listado, mismo patrón de los costos). Portal: columna **"Pendientes"** (badge gris "N fuente(s)" con chevron) y **fila expandible al hacer clic** (drill-down tipo tabla dinámica) que despliega bajo la consulta el listado legible de fuentes sin respuesta + el botón "Completar fuentes pendientes" (movido ahí desde la celda Informe); filas sin pendientes → "—"; responsive ≤640px integrado al colapso a tarjetas. Verificado: 179 tests estudios OK, tsc --noEmit OK y el aggregation probado contra Mongo real (detectó pendientes de estudios existentes). **Sin migraciones ni env**. |
| 2026-09-25 | **Cobro del completado (decisión del usuario: "cobrar al completar")**: el completado de fuentes pendientes ya no es gratis incondicional — hueco detectado: una consulta REEMBOLSADA (>51% fallidas al crear) se completaba gratis y quedaba el estudio completo sin costo. Regla nueva (`_consumos_del_completado` en rutas): (a) consulta ya cobrada (≤51% caídas) → completa GRATIS (esas fuentes ya se pagaron); (b) consulta reembolsada (existe movimiento REEMBOLSO) → ANTES de tocar portales se vuelve a reservar el consumo de las fuentes originales (excluye DESHABILITADA; plan preferido = el de los CONSUMO originales si fueron de un solo plan, multi-plan → FIFO; 402 accionable si ya no hay cupo) y tras ejecutar aplica el MISMO reembolso automático >51% sobre el estado FINAL (solo toca los consumos nuevos; los originales ya están `reembolsado`). Respuesta del endpoint con `cobrado` true/false; portal: Swal y ayuda del drill-down advierten "si la consulta original quedó sin costo, al completarla se cobra el valor del plan" y el Swal final muestra "Se cobró el valor del plan" cuando aplica. 336 tests OK (+3 TestConsumosDelCompletado) · tsc --noEmit OK. **Sin migraciones ni env**. |
| 2026-09-25 | **Cascada de nombres — adiós a la digitación (decisión del usuario: "consultar en dos fuentes y traer el nombre")**: cuando el plan necesita nombres (captcha de la PGN / búsqueda por nombre de rama_judicial) y ni el body ni la memoria de personas los tienen, el POST los DETECTA solo antes de ejecutar el estudio (`detectar_nombres` en el orquestador): `situacion_militar` (API del Ejército, $0, ~1-2 s, nombres separados VERIFICADOS) → `sisconmp` (Mintransporte, $0, ~11 s, cubre a quien la libreta no registra: mujeres/extranjeros con cursos MP) → si ambas fallan, el 422 de rama_judicial queda como última barrera con mensaje accionable. Ambas consultas pasan por `_ejecutar_fuente` (llenan la caché 24 h: si la fuente está en el plan, el gather principal sirve del hit — no se consulta dos veces); wait_for 45 s por fuente; lo del body SIEMPRE gana. E2E real: 1010213062 → EDWIN MISAEL ZARATE PEÑA en ~2 s sin digitación. 339 tests OK (+3 TestDetectarNombres) · tsc --noEmit OK. **Sin migraciones ni env**. |
| 2026-09-25 | **Portal SIN campos de nombres (pedido del usuario: "¿para qué dejas los dos input? quítalos")**: los inputs de nombres/apellidos DESAPARECIERON del acordeón del plan (antes opcionales; ahora el formulario no los muestra) — quedan solo la cédula/NIT, placa, propietario y fecha de expedición. La cascada del backend cubre todo. Limpieza: states `nombres`/`apellidos` y `requiereNombres` eliminados, `personaMemoria` simplificada a un número (la memoria ya solo autolleena la FECHA de expedición — el aviso "persona consultada antes" se conserva), `crearEstudio` ya no los envía. El 422 de rama_judicial (cascada agotada) ya no remite al formulario: "intente más tarde, comuníquese con Integra o envíelos por API". Las integraciones API siguen pudiendo pasarlos en el body (lo del body gana). 185 tests estudios OK · tsc --noEmit OK. **Sin migraciones ni env**. |
| 2026-09-25 | **Fix doble clic en "Consultar" (reporte del usuario: parpadeo y no arranca)**: el `onBlur` de la cédula (autollenado de memoria) pintaba SINCRÓNICAMENTE el párrafo "Buscando en su historial…" al hacer mousedown en el botón → el formulario crecía, el botón se MOVÍA entre el mousedown y el mouseup → el click nunca aterrizaba en el botón y el submit no se disparaba (el segundo clic ya encontraba todo quieto). Fix: el indicador de búsqueda se ELIMINÓ (la búsqueda de memoria es silenciosa; el aviso "✅ persona consultada antes" solo se pinta cuando la respuesta llega, asincrónico, después del click). También se quitó el párrafo "los nombres se detectan automáticamente" (pedido: "eso sobra") y `pideNombres` muere del todo. tsc --noEmit OK. **Sin migraciones ni env**. |
| 2026-09-25 | **REDISEÑO del portal cliente (pedido del usuario, inspirado en dash-board.tusdatos.co — sesión de diseño con Playwright sobre su cuenta: login, panel de control, histórico)**: `index.tsx` + `estilos.css` de PortalSeguridadP reescritos con prefijo PSX-. Shell: **sidebar FIJA blanca a la izquierda** (logo seguriDatia con acento teal, módulos Panel / Nueva consulta / Historial con ícono, activo = píldora teal + filo izquierdo, pie con avatar de iniciales + usuario/empresa + Volver al inicio + Cerrar sesión) · **top bar** blanca con breadcrumb Inicio / {módulo} y pill "Consultas disponibles: N" + avatar · contenido en **tarjetas blancas radius 12 con sombra suave sobre fondo gris #F4F6F8**, acento **teal #00A5B5** (botones primarios full-width, focus rings, barras de progreso) y labels MAYÚSCULAS sobre los inputs (patrón tusdatos). Módulo **Panel** (nuevo): saludo + 3 KPIs (disponibles, consumo del mes, total historial) + planes con barra de progreso cupo_consumido/cupo_autorizado + CTA Nueva consulta. **Login** split: panel teal con propuesta de valor + checklist, form a la derecha. Móvil ≤900px: sidebar → drawer con overlay y ✕; historial colapsa a tarjetas ≤640px (drill-down integrado). Toda la lógica intacta (acordeón de planes, cascada de nombres, completar pendientes, drill-down, paginación, detective, ding). Verificado visualmente (login, panel, consulta con acordeón abierto, historial con drill-down expandido, móvil) con API moqueada vía Playwright + tsc --noEmit OK. **Solo frontend; sin migraciones ni env**. |
| 2026-09-25 | **Logo seguriDatia en el portal (pedido del usuario: "coloca el de seguriDatia que se coloca en los PDF ya")**: el PNG oficial del PDF (`integrappi/imagenes/Logo seguriDatia.png`, 1448×396 con transparencia) se copió a `integrapp-next/src/Imagenes/LogoSeguriDatia.png` y reemplazó al albatros+nombre en la sidebar (solo el logo horizontal, 38 px) y en el panel teal del login (52 px sobre fondo de color, con fondo blanco suave para que respire). Verificado visualmente (login + panel) · tsc OK. **Solo frontend**. |
| 2026-09-25 | **Cerrar sesión arriba a la derecha (pedido del usuario)**: el avatar de iniciales del topbar es ahora un BOTÓN con chevron que despliega un menú blanco flotante (cabecera usuario/empresa + "Volver al inicio" + "Cerrar sesión" con hover rojo, cierra al hacer clic fuera — patrón tusdatos). Las acciones salieron del pie de la sidebar (queda solo la tarjeta de usuario). Verificado visualmente (menú abierto) · tsc OK. **Solo frontend**. |
| 2026-09-25 | **Formato del periodo en el KPI (pedido del usuario: "sept 2026")**: helper `nombrePeriodo` ("2026-09" → "sept 2026", meses cortos deterministas — el Intl según ICU puede dar "sep") en el KPI "Consultas este mes" del Panel. ⚠️ **El logo de seguriDatia vive en DOS copias**: PDF = `integrappi/imagenes/Logo seguriDatia.png` (LOGO_SEGURIDATIA en pdf_estudio_seguridad.py) y portal = `integrapp-next/src/Imagenes/LogoSeguriDatia.png` (import en PortalSeguridadP/index.tsx) — al cambiar la marca hay que sobrescribir AMBAS. tsc OK. |
| 2026-09-25 | **Barra de progreso REAL en la consulta (pedido del usuario: "avance en porcentaje del total, cuántas ya tiene hechas")**: el POST síncrono se parte en `_preparar_estudio` (validaciones + cascada + cobro + doc EN_PROGRESO) y `_finalizar_estudio` (fuentes + reembolso + memoria + PDF) — el POST `` queda IDÉNTICO para las integraciones API. Nuevo `POST /iniciar` (202, devuelve consulta_id al instante y corre _finalizar en background con asyncio.create_task) + `GET /{id}/progreso` (registro `_PROGRESO` en memoria del orquestador, actualizado por fuente vía wrapper `_con_progreso` en ejecutar_estudio Y reintentar_fuentes_estudio; respuesta final con PDF incluido; poda 200 entradas). Portal: `iniciarEstudio`/`obtenerProgreso`, bucle de polling cada 1,5 s con guard de 8 min (tolera 404 mientras prepara), y en la pantalla del detective BARRA de progreso teal con %, "N de M fuentes completadas" y checklist por fuente (✓ verde / ✕ roja / ⏳ corriendo); el escape hatch de nombres reintenta por la misma vía; aviso "si cierra la ventana, la consulta termina igual y queda en el historial" (beneficio real del background). E2E verificado con mock progresivo (iniciar → 43% mid → resultado con PDF) · 345 tests OK · tsc OK. **Sin migraciones ni env**. |
| 2026-09-25 | **Rutas por módulo en el portal (pedido del usuario)**: cada módulo tiene su URL — `/PortalSeguridad/panel`, `/PortalSeguridad/nueva-consulta` y `/PortalSeguridad/historial` (raíz = panel) — con carpeta estática por vista bajo `src/app/PortalSeguridad/` y prop `vistaInicial` (patrón /AdminSeguridad, exactamente igual que sus pestañas). El cambio de módulo espeja la URL con `history.replaceState` NATIVO (no router.replace: la navegación del App Router re-suspende el Suspense y el módulo parpadea); F5 aterriza en el módulo por la carpeta estática. Verificado: aterrizaje directo en /historial, espejo al hacer clic en el nav y persistencia tras F5 · tsc OK. |
| 2026-09-25 | **Pendientes del historial minimalistas (pedido del usuario)**: el badge gris muestra SOLO el número (antes "3 fuentes") con su chevron, y el completado pasa a un **botón circular ↻ de 26 px al lado del badge** en la misma celda (tooltip + spinner al completar; stopPropagation para no plegar el detalle). El botón grande "Completar fuentes pendientes" sale del detalle desplegado — queda solo el listado + nota que remite al ↻. La tarjeta de resultado de la consulta conserva su botón completo. Verificado visualmente · tsc OK. |
| 2026-09-25 | **Escape hatch de nombres (hueco de la cascada con cédulas de MUJER)**: la cascada (memoria → libreta → sisconmp) no cubre a una mujer sin registro de libreta NI cursos MP NI memoria previa — con plan rama_judicial el 422 la dejaba sin salida (los campos se quitaron del portal). Fix en `consultar()`: al recibir ese 422, el portal muestra un Swal pidiendo nombres/apellidos UNA sola vez y REINTENTA la consulta automáticamente con ellos (quedan en la memoria de personas → nunca más se piden); sin ellos informa "consulta cancelada" (info, no error). Para planes con solo procuraduria no cambia nada: los nombres son pista del captcha y la consulta sigue (variantes aritméticas se resuelven solas; las de nombre caen a Gemini/reintento). tsc OK. |
| 2026-09-25 | **Ojito mostrar/ocultar clave (pedido del usuario)**: todos los campos de contraseña llevan un botón 👁 dentro del input que alterna password/text — el del login es React (FaEye/FaEyeSlash + estado `mostrarClave`); los de los Swal (cambiar clave y recuperación) usan `INPUT_CLAVE_SWAL` (HTML plano con onclick inline, porque dentro del Swal no hay React), con CSS `.PSX-input-clave`/`.PSX-ojito`. Verificado en vivo (toggle cambia a text) · tsc OK. |
| 2026-09-25 | **Autogestión de clave del portal (pregunta del usuario: "¿cómo cambia su clave?")**: antes NO existía — y los endpoints de recuperación de aut2 apuntan a la colección `usuarios` (Torre de Control), no a `baseusuarios` donde viven los CLIENTE_ESTUDIOS. Nuevo en `/seguridad/estudios`: (a) `POST /cambiar-clave` autenticado (menú del avatar → "Cambiar contraseña": Swal con clave actual/nueva/confirmación; verifica con el dual-mode de `Funciones/claves`, API keys 403); (b) "¿Olvidó su contraseña?" en el login: `POST /recuperar/solicitar` (correo → código de 6 dígitos de un solo uso, hash + exp 15 min, enviado por Resend con branding seguriDatia; respuesta neutra anti-enumeración) + `POST /recuperar/confirmar` (correo+código+nueva; máx 8 intentos por código, luego se invalida). Frontend: `cambiarClave`/`recuperarSolicitar`/`recuperarConfirmar` en seguridadCliente.tsx. 191 tests estudios OK (+6 TestCambioYRecuperacionClave) · tsc --noEmit OK. Reusa env RESEND_API_KEY/MAIL_FROM (ya configurados para la Torre). **Sin migraciones**. |
| 2026-09-25 | **Logo NUEVO de seguriDatia (entregado por el usuario)**: versión con guardián teal con detalles naranjas + tagline "INVESTIGACIÓN DE SEGURIDAD DIGITAL" (2172×724 RGBA) — se sobrescribió `integrappi/imagenes/Logo seguriDatia.png` (el usuario) y se copió a `integrapp-next/src/Imagenes/LogoSeguriDatia.png` (portal: sidebar 38 px y login 52 px). Verificado visualmente en ambas vistas; los PDF lo toman solos (misma ruta de siempre). |
