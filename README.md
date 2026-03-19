# IntegraPPI — Backend API de Integra Logística

Backend del sistema de gestión logística de **Integra Cadena de Servicios S.A.S.**, construido con FastAPI y MongoDB. Centraliza la operación de transportadores, empleados, clientes y pedidos, con integración a WhatsApp, sistemas de rastreo externos y generación de documentos.

**Frontend:** ver `../integrapp-next/` (Next.js 14 App Router — migrado desde `../integrapp/` en marzo 2025)

---

## Qué hace este sistema

- Registro y gestión de vehículos con carga de documentos y biometría
- Gestión de usuarios por roles (transportador, despachador, seguridad, admin)
- Gestión de clientes, destinos y tarifas de flete
- Manejo de pedidos: creación masiva, asignación de vehículos, división y fusión
- Chatbot de WhatsApp multi-rol para consultas de manifiestos, certificados y rastreo
- Generación de certificados laborales en PDF enviados por correo
- Integración con Vulcano (manifiestos) y Siscore (rastreo SOAP)
- Reportes de uso de WhatsApp descargables en Excel

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Framework web | FastAPI 0.115 + Uvicorn |
| Base de datos | MongoDB Atlas (PyMongo + Motor async) |
| Autenticación | JWT (PyJWT) + OAuth2 + bcrypt |
| Almacenamiento de archivos | Google Cloud Storage |
| Correo electrónico | Resend |
| Mensajería | WhatsApp Cloud API (Meta) |
| Rastreo externo | Vulcano (REST) + Siscore (SOAP/XML) |
| Generación de documentos | ReportLab (PDF) + openpyxl / xlsxwriter (Excel) |
| Procesamiento de datos | Pandas, Pillow |
| Validación | Pydantic v2 |

---

## Estructura del proyecto

```
integrappi/
├── main.py                  # Entrada de la app, registro de routers
├── .env                     # Variables de entorno (no se sube al repo)
├── credenciales.json        # Credenciales de Google Cloud (no se sube al repo)
├── requirements.txt         # Dependencias Python
│
├── bd/                      # Capa de datos
│   ├── bd_cliente.py        # Conexión a MongoDB
│   ├── models/              # Funciones de formato de documentos MongoDB
│   │   ├── usuario.py
│   │   └── saldos.py
│   └── schemas/             # Esquemas Pydantic para validación
│       ├── usuario.py
│       └── saldos.py
│
├── rutas/                   # Endpoints de la API (un archivo por dominio)
│   ├── aut2.py              # Autenticación y usuarios (transportadores)
│   ├── baseusuarios.py      # Usuarios base (despachadores, seguridad, conductores)
│   ├── vehiculos.py         # Registro y documentos de vehículos
│   ├── revision.py          # Revisión y observaciones de vehículos
│   ├── puente_biometrico.py # Almacenamiento de huellas
│   ├── consultar_biometrico.py # Verificación de huellas
│   ├── empleados.py         # Empleados y certificados laborales
│   ├── clientes.py          # Clientes estándar
│   ├── clientes_siscore.py  # Clientes integrados con Siscore
│   ├── clientes_general.py  # Destinos de entrega con geolocalización
│   ├── ciudades_general.py  # Municipios y coordenadas
│   ├── fletes.py            # Tarifas de flete
│   ├── pedidos.py           # Pedidos (CRUD, carga masiva, reportes)
│   ├── pagoSaldos.py        # Manifiestos de pago
│   ├── novedades.py         # Novedades
│   ├── whatsapp_integra.py  # Chatbot WhatsApp (webhook principal)
│   ├── whatsapp_report_integra.py # Reportes de uso de WhatsApp
│   ├── vulcano.py           # Cliente Vulcano (manifiestos)
│   ├── debug.py             # Diagnóstico de red y variables de entorno
│   └── debug_siscore.py     # Diagnóstico de conexión Siscore
│
├── Funciones/               # Utilidades y lógica de integraciones
│   ├── chat_state_integra.py          # Manejo de estado de conversaciones WhatsApp
│   ├── whatsapp_utils_integra.py      # Envío de mensajes, autenticación de transportadores
│   ├── whatsapp_logs_integra.py       # Registro de eventos WhatsApp en MongoDB
│   ├── whatsapp_certificado_integra.py # Generación de certificados por WhatsApp
│   ├── vulcano_whatsapp_format.py     # Formateo de manifiestos Vulcano para WhatsApp
│   ├── siscore_ws_tracking.py         # Cliente SOAP para rastreo Siscore
│   └── siscore_ws_format.py           # Parseo de respuestas XML de Siscore
│
├── scripts/                 # Scripts utilitarios (no son parte de la API)
│   ├── crear_indice_sesiones.py  # Crea índices TTL en MongoDB para sesiones
│   ├── subirEmpleados.py         # Carga masiva de empleados desde Excel a MongoDB
│   ├── subirEmpleados.spec       # Configuración de PyInstaller para el script anterior
│   ├── coor.py                   # Geocodifica municipios desde Excel (Nominatim)
│   └── empleados.xlsx            # Plantilla de empleados
│
└── docs/
    └── MANUAL WS TRACKING INTEGRA.docx  # Manual de uso del rastreo por WhatsApp
```

---

## Endpoints principales

### Autenticación y usuarios (`/usuarios`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/usuarios/token` | Login, retorna JWT |
| POST | `/usuarios/` | Crear usuario |
| GET | `/usuarios/{id}` | Obtener usuario |
| PUT | `/usuarios/{id}` | Actualizar usuario |
| DELETE | `/usuarios/{id}` | Eliminar usuario |
| POST | `/usuarios/recuperar/solicitar` | Solicitar recuperación de clave (envía correo) |
| POST | `/usuarios/recuperar/confirmar` | Confirmar nueva clave con token |
| POST | `/usuarios/cambiar-clave` | Cambiar clave autenticado |

### Usuarios Torre de Control (`/baseusuarios`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/baseusuarios/` | Listar todos los usuarios |
| POST | `/baseusuarios/` | Crear usuario |
| PUT | `/baseusuarios/{id}` | Actualizar usuario |
| DELETE | `/baseusuarios/{id}` | Eliminar usuario |
| POST | `/baseusuarios/login` | Login estándar — retorna datos del usuario incluyendo `clientes` |
| POST | `/baseusuarios/loginseguridad` | Login perfil SEGURIDAD/ADMIN |
| POST | `/baseusuarios/loginConductor` | Login perfil CONDUCTOR |
| PATCH | `/baseusuarios/{id}/clientes` | Actualizar lista de clientes permitidos para un usuario |

> **Campo `clientes`**: cada usuario en `baseusuarios` puede tener un array `clientes: ["KABI", "MEDICAL_CARE"]` que controla a qué portales de cliente tiene acceso. Si el campo no existe en el documento, se toma por defecto `["KABI"]` para compatibilidad con registros anteriores. Valores válidos: `KABI`, `MEDICAL_CARE`.

### Vehículos (`/vehiculos`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/vehiculos/crear` | Registrar vehículo |
| GET | `/vehiculos/obtener-vehiculos` | Listar vehículos del usuario |
| PUT | `/vehiculos/actualizar-estado` | Cambiar estado (notifica por correo) |
| PUT | `/vehiculos/subir-documento` | Subir documento (tarjeta, SOAT, etc.) |
| PUT | `/vehiculos/subir-firma` | Subir imagen de firma |
| GET | `/vehiculos/obtener-firma` | Obtener firma en base64 |
| GET | `/vehiculos/obtener-aprobados-paginados` | Vehículos aprobados (paginado) |

### Pedidos (`/pedidos`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/pedidos/` | Crear pedido |
| GET | `/pedidos/` | Listar pedidos (con filtros) |
| POST | `/pedidos/cargar-masivo` | Carga masiva desde Excel |
| GET | `/pedidos/reporte-excel` | Descargar reporte en Excel |
| POST | `/pedidos/ajustes-vehiculos` | Ajustes por vehículo (kilos, tarifas) |
| POST | `/pedidos/fusion-vehiculos` | Fusionar vehículos |
| POST | `/pedidos/dividir-hasta-tres` | Dividir pedido en hasta 3 vehículos |

### WhatsApp (`/whatsapp-integra`)
| Método | Ruta | Descripción |
|---|---|---|
| POST | `/whatsapp-integra/webhook` | Recibe mensajes entrantes |
| GET | `/whatsapp-integra/webhook` | Verificación del webhook (Meta) |

### WhatsApp - Reportes (`/whatsapp-report`)
| Método | Ruta | Descripción |
|---|---|---|
| GET | `/whatsapp-report/resumen` | Estadísticas generales de uso |
| GET | `/whatsapp-report/numeros-por-estado` | Números únicos por rol por día |
| GET | `/whatsapp-report/excel` | Descargar reporte en Excel |

> Los demás módulos (empleados, clientes, fletes, biometría, ciudades, manifiestos, novedades) siguen el mismo patrón REST estándar. La documentación interactiva completa está disponible en `/docs` cuando el servidor está corriendo.

---

## Integraciones externas

### Vulcano
Sistema de manifiestos y pagos de la operación logística. Se consulta vía REST con autenticación JWT propia. Provee el estado de guías (en tránsito, cumplidas, liquidadas) y el detalle de pagos a transportadores.

### Siscore
Sistema de rastreo de guías vía SOAP/XML. Se consulta el historial de movimientos de una guía y se obtienen imágenes de trazabilidad. Soporta proxy para redes restringidas.

### WhatsApp Cloud API (Meta)
Chatbot con máquina de estados para tres tipos de usuario:
- **Transportador**: consulta de manifiestos, saldos, recuperación de clave
- **Empleado**: solicitud de certificado laboral
- **Cliente**: rastreo de envíos vía Siscore

### Google Cloud Storage
Almacenamiento de documentos de vehículos (bucket `integrapp`) e imágenes de huellas dactilares. Las imágenes se comprimen automáticamente a WebP antes de subir.

### Resend
Envío de correos para: certificados laborales (PDF adjunto), recuperación de clave, notificaciones de revisión de vehículos y códigos de verificación.

---

## Variables de entorno

Crear un archivo `.env` en la raíz con las siguientes variables:

```env
# Base de datos
MONGO_URI=mongodb+srv://<usuario>:<clave>@<cluster>.mongodb.net/...

# Google Cloud (ruta al archivo de credenciales)
GOOGLE_APPLICATION_CREDENTIALS=./credenciales.json

# Correo (Resend)
RESEND_API_KEY=re_...
MAIL_FROM=no-reply@integralogistica.com

# JWT
JWT_SECRET=<clave_secreta_larga>
RESET_TOKEN_EXPIRE_MINUTES=30

# WhatsApp (Meta)
WHATSAPP_API_TOKEN=...
WHATSAPP_PHONE_NUMBER_ID=...
WHATSAPP_VERIFY_TOKEN=...

# Siscore (SOAP)
SISCORE_SOAP_TOKEN=...
SISCORE_SOAP_ENDPOINT=https://integra.appsiscore.com/app/ws/trazabilidad.php
SISCORE_SOAP_ACTION=ConsultarGuiaImagen

# Vulcano
VULCANO_HOST=https://...
VULCANO_BASE_PATH=/vulcano
VULCANO_USERNAME=...
VULCANO_IDNAME=...
VULCANO_AGENCY=001
VULCANO_PROJECT=1
VULCANO_IS_GROUP=0

# Proxy (opcional, para redes restringidas)
VULCANO_PROXY_URL=http://ip:puerto
VULCANO_VERIFY_SSL=true
VULCANO_CONNECT_TIMEOUT=10

# Frontend (para links de recuperación de clave)
FRONTEND_URL_RECUPERAR=https://integralogistica.com/integrapp/recuperar-clave
```

---

## Cómo correr el proyecto

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar variables de entorno
cp .env.example .env   # editar con los valores reales

# 3. Iniciar el servidor
python main.py
```

El servidor queda disponible en `http://localhost:8000`.

- Documentación interactiva (Swagger): `http://localhost:8000/docs`
- Documentación alternativa (ReDoc): `http://localhost:8000/redoc`

### CORS habilitado para:
- `http://localhost:5173` (frontend Vite legacy — `../integrapp/`)
- `http://localhost:3000` (frontend Next.js — `../integrapp-next/`)
- `http://127.0.0.1:3000`
- `https://integralogistica.com` (producción)
- `https://www.integralogistica.com`

---

## Scripts utilitarios

Estos scripts se corren de forma independiente, no son parte de la API:

| Script | Uso |
|---|---|
| `scripts/subirEmpleados.py` | Interfaz gráfica para cargar empleados desde Excel a MongoDB |
| `scripts/crear_indice_sesiones.py` | Crea índice TTL en MongoDB para expirar sesiones automáticamente |
| `scripts/coor.py` | Geocodifica municipios desde un Excel usando OpenStreetMap (Nominatim) |

---

## Seguridad

- Contraseñas almacenadas con bcrypt (nunca en texto plano)
- JWT con expiración de 20 minutos
- Tokens de recuperación de un solo uso, expiran en 30 minutos
- `.env` y `credenciales.json` excluidos del repositorio vía `.gitignore`
- Endpoints protegidos con OAuth2 Bearer Token

---

## Historial de cambios relevantes

### Marzo 2025 — Multi-cliente y panel de administración
- **`baseusuarios.py`**: añadido campo `clientes: Optional[List[str]]` al modelo `BaseUsuario`. El endpoint `/login` ahora devuelve el array `clientes` en la respuesta. Compatibilidad hacia atrás: documentos sin el campo retornan `["KABI"]` por defecto.
- **`baseusuarios.py`**: nuevo endpoint `PATCH /baseusuarios/{id}/clientes` para que administradores actualicen los portales de cliente a los que tiene acceso cada usuario. Solo acepta los valores `KABI` y `MEDICAL_CARE`.
- **`baseusuarios.py`**: `modelo_usuario()` actualizado para incluir `clientes` en todas las respuestas CRUD.
- **`main.py`**: CORS actualizado para incluir explícitamente `http://localhost:3000` y `http://127.0.0.1:3000`.
