# API-RUTAS-VENTAS / Monitoreo de flota — Cloud Run
FROM python:3.13-slim

WORKDIR /workspace

COPY requeriments.txt .
RUN pip install --no-cache-dir -r requeriments.txt

COPY main.py .

EXPOSE 8080
ENV PORT=8080

# Gunicorn: escucha en $PORT y soporta WebSockets (flask-sock).
# NO usar functions-framework aquí: no levanta bien /ws con flask-sock.
CMD exec gunicorn --worker-class gthread --threads 100 --timeout 0 --bind 0.0.0.0:$PORT main:app
