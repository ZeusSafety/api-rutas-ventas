# Mismo patrón que API-RUTAS-VENTAS (Cloud Run / Cloud Functions Gen2)
FROM python:3.13-slim

WORKDIR /workspace

# En tus otros backends el archivo se llama requeriments.txt (con 'e')
COPY requeriments.txt .

RUN pip install --no-cache-dir -r requeriments.txt

COPY main.py .

EXPOSE 8080

ENV PORT=8080

CMD exec functions-framework --target=flota_zeus --port=$PORT --host=0.0.0.0

# Si los WebSockets (/ws/panel, /ws/conductor) no conectan, usa:
# CMD exec gunicorn --worker-class gthread --threads 100 --bind 0.0.0.0:$PORT main:app
