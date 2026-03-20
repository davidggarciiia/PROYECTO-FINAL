# Dockerfile — Backend GeoRetail (FastAPI + Python 3.11)

FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema necesarias para:
#   - WeasyPrint (generación de PDFs): libpango, libcairo, libgdk-pixbuf
#   - psycopg2 / asyncpg: libpq
#   - Shapely / PyProj: libgeos, libproj
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libgdk-pixbuf-xlib-2.0-0 \
    libffi-dev \
    libpq-dev \
    libgeos-dev \
    libproj-dev \
    gdal-bin \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
# (se hace antes que el resto del código para aprovechar la caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . .

# Crear directorios de datos
RUN mkdir -p /data/models /data/exports /data/optuna

# Puerto que expone el contenedor
EXPOSE 8000

# Comando por defecto (se sobreescribe en docker-compose para desarrollo)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
