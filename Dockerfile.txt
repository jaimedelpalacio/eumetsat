# Dockerfile — imagen para Render (servicio web)
FROM python:3.11-slim

# Dependencias del sistema para h5py (libhdf5)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libhdf5-dev && \
    rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Instala dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
COPY app.py ingest.py ./

# Variables de entorno (puedes sobreescribir en Render)
ENV HOST=0.0.0.0 \
    PORT=8080

# Exponer puerto
EXPOSE 8080

# Comando de arranque
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
