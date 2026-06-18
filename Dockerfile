# Dockerfile para integrappi en Render.
# Usa la imagen oficial de Playwright para Python: ya trae Chromium y todas las
# dependencias del sistema necesarias para headless. El tag debe coincidir con la
# versión de playwright fijada en requirements.txt (1.49.0).
FROM mcr.microsoft.com/playwright/python:v1.49.0

WORKDIR /app

# Dependencias Python (playwright ya está instalado a nivel de imagen/browsers,
# pero lo fijamos vía requirements para que la API de Python coincida).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código de la aplicación
COPY . .

# Render inyecta PORT; main.py ya lo respeta (default 8000).
ENV PORT=8000
EXPOSE 8000

# Nota: Chromium es pesado. Usar al menos el plan "starter" (con RAM suficiente);
# el free tier puede quedarse sin memoria o dormirse entre requests.
CMD ["python", "main.py"]
