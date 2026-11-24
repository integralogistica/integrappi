"""Constantes compartidas del Módulo Financiero."""
import os

# Schema de PostgreSQL propio del módulo. TODO lo del módulo financiero vive
# aquí (tablas, índices, tabla de versiones de Alembic): un `DROP SCHEMA fin
# CASCADE` jamás toca `informe_guias_tms` ni nada existente.
ESQUEMA_FIN = os.getenv("FIN_DB_SCHEMA", "fin")

# Duración del token de sesión del módulo (horas). El JWT de baseusuarios dura
# 20 min sin refresh; el front lo canjea una vez por este token de sesión.
SESION_HORAS = int(os.getenv("FIN_SESION_HORAS", "12"))

# Origen del token aceptado por la dependencia de autenticación del módulo.
AUTH_SOURCE_MODULO = "fin_modulo"
AUTH_SOURCE_PORTAL = "baseusuarios"  # el que emite el login de integrappi

# Adjuntos en Google Cloud Storage (mismo bucket privado que Otros Costos).
FIN_BUCKET = os.getenv("FIN_BUCKET", "integrapp-privado")
FIN_CARPETA_GCS = "Financiero"
FIN_ADJUNTO_MAX_MB = int(os.getenv("FIN_ADJUNTO_MAX_MB", "15"))
FIN_URL_FIRMADA_MIN = int(os.getenv("FIN_URL_FIRMADA_MIN", "15"))

# Correos salientes (Resend): dominios permitidos como destinatarios.
FIN_DOMINIOS_CORREO = [
    d.strip().lower()
    for d in os.getenv("FIN_DOMINIOS_CORREO", "integralogistica.com").split(",")
    if d.strip()
]
