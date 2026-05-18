# Mismo patrón que API-RUTAS-VENTAS (Cloud Run / Cloud Functions Gen2)
FROM python:3.13-slim

WORKDIR /workspace

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8080

ENV PORT=8080

# Entrada HTTP: función flota_zeus en main.py
CMD exec functions-framework --target=flota_zeus --port=$PORT --host=0.0.0.0

# Si los WebSockets (/ws/panel, /ws/conductor) no conectan en producción,
# usa Gunicorn en lugar de la línea anterior:
# CMD exec gunicorn --worker-class gthread --threads 100 --bind 0.0.0.0:$PORT main:app
