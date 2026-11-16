# Imagen de la ingesta del proyecto. Kestra corre esta imagen (pullPolicy: NEVER),
# igual que en el laboratorio de la Semana 4: docker build -t pset2-ingesta:1.0 .
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY kestra/scripts/ingest_uci.py /app/ingest_uci.py
ENTRYPOINT []
CMD ["python", "/app/ingest_uci.py", "--help"]
