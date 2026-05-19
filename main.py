"""
Microservicio de Monitoreo de Flota — Flask + WebSockets
Desplegar en Google Cloud Run (carpeta flota-service/, independiente del frontend).
"""
import json
import logging
import os
import threading
from datetime import date, datetime
from decimal import Decimal
import pymysql
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sock import Sock

load_dotenv()

logger = logging.getLogger(__name__)

###########################################################
# CONFIG DB (mismo patrón que ejemplo.py / otros backends ZEUS)
###########################################################
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
# Solo desarrollo local con cloud-sql-proxy:
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
    if o.strip()
]
PORT = int(os.getenv("PORT", "8080"))

###########################################################
# APP
###########################################################

app = Flask(__name__)
sock = Sock(app)
CORS(app, origins=ALLOWED_ORIGINS, supports_credentials=True)

panel_connections = []
panel_lock = threading.Lock()
MAX_GPS_PRECISION_M = 50


def get_connection():
    """Igual que get_connection() en ejemplo.py — Cloud SQL por socket o TCP local."""
    common = dict(
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=15,
        read_timeout=30,
        write_timeout=30,
    )

    if INSTANCE_CONNECTION_NAME:
        return pymysql.connect(
            unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
            **common,
        )

    if DB_HOST:
        return pymysql.connect(host=DB_HOST, port=DB_PORT, **common)

    raise RuntimeError(
        "Defina INSTANCE_CONNECTION_NAME (Cloud Run) o DB_HOST (local + proxy)."
    )


def get_db():
    return get_connection()


@app.route("/")
def root():
    return jsonify({
        "service": "api-rutas-ventas",
        "modulo": "monitoreo-flota",
        "status": "ok",
    })


def db_error_response(exc):
    """Respuesta JSON clara cuando falla MySQL (config o tablas)."""
    logger.exception("Error de base de datos")
    msg = str(exc)
    hint = "Defina DB_USER, DB_PASSWORD, DB_NAME e INSTANCE_CONNECTION_NAME en Cloud Run."
    if "doesn't exist" in msg.lower() or "1146" in msg:
        hint = "Ejecute database/migrations.sql en su MySQL."
    elif "Access denied" in msg or "1045" in msg:
        hint = "Usuario o contraseña MySQL incorrectos (DB_USER / DB_PASSWORD)."
    elif "Can't connect" in msg or "2003" in msg:
        hint = "Cloud Run no alcanza MySQL: use IP pública de Cloud SQL o VPC connector."
    return jsonify({"error": "Error de base de datos", "detail": msg, "hint": hint}), 503


def serialize_row(row):
    if not row:
        return row
    out = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, bytes):
            out[k] = v.decode()
        else:
            out[k] = v
    return out


def broadcast_to_panel(message: dict):
    payload = json.dumps(message, default=str)
    dead = []
    with panel_lock:
        for ws in panel_connections:
            try:
                ws.send(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in panel_connections:
                panel_connections.remove(ws)


def get_conductor_info(conductor_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, nombre, placa_vehiculo AS placa, tipo_vehiculo
                   FROM conductores WHERE id = %s AND activo = 1""",
                (conductor_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def estado_conductor(velocidad, ultimo_update_iso):
    if not ultimo_update_iso:
        return "sin_senal"
    try:
        ts = datetime.fromisoformat(ultimo_update_iso.replace("Z", "+00:00"))
        if isinstance(ts, datetime) and ts.tzinfo:
            ts = ts.replace(tzinfo=None)
        diff = (datetime.now() - ts).total_seconds()
    except Exception:
        diff = 999
    if diff > 120:
        return "sin_senal"
    if float(velocidad or 0) < 3:
        return "detenido"
    return "en_ruta"


# ─── Health ───────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "flota-service"})


@app.route("/api/health/db", methods=["GET"])
def health_db():
    try:
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
        finally:
            conn.close()
        return jsonify({"status": "ok", "database": "connected"})
    except Exception as e:
        body, code = db_error_response(e)
        return body, code


# ─── Conductores ──────────────────────────────────────────────────────────────

@app.route("/api/conductores", methods=["GET"])
def list_conductores():
    try:
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, nombre, telefono, placa_vehiculo AS placa, tipo_vehiculo
                       FROM conductores WHERE activo = 1 ORDER BY nombre"""
                )
                rows = cur.fetchall()
            return jsonify([serialize_row(r) for r in rows])
        finally:
            conn.close()
    except Exception as e:
        body, code = db_error_response(e)
        return body, code


# ─── Sesiones GPS ─────────────────────────────────────────────────────────────

@app.route("/api/sesion/iniciar", methods=["POST"])
def iniciar_sesion():
    data = request.get_json() or {}
    conductor_id = data.get("conductor_id")
    ruta_asignada_id = data.get("ruta_asignada_id")
    if not conductor_id:
        return jsonify({"error": "conductor_id requerido"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sesiones_ruta (conductor_id, ruta_asignada_id, estado)
                   VALUES (%s, %s, 'activa')""",
                (conductor_id, ruta_asignada_id),
            )
            sesion_id = cur.lastrowid
            if ruta_asignada_id:
                cur.execute(
                    "UPDATE rutas_asignadas SET estado = 'en_curso' WHERE id = %s",
                    (ruta_asignada_id,),
                )
        return jsonify({"sesion_id": sesion_id, "estado": "activa"})
    finally:
        conn.close()


@app.route("/api/sesion/finalizar", methods=["POST"])
def finalizar_sesion():
    data = request.get_json() or {}
    sesion_id = data.get("sesion_id")
    if not sesion_id:
        return jsonify({"error": "sesion_id requerido"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE sesiones_ruta SET estado = 'finalizada', fecha_fin = NOW()
                   WHERE id = %s""",
                (sesion_id,),
            )
            cur.execute(
                "SELECT ruta_asignada_id FROM sesiones_ruta WHERE id = %s",
                (sesion_id,),
            )
            row = cur.fetchone()
            if row and row.get("ruta_asignada_id"):
                cur.execute(
                    "UPDATE rutas_asignadas SET estado = 'completada' WHERE id = %s",
                    (row["ruta_asignada_id"],),
                )
        return jsonify({"ok": True, "sesion_id": sesion_id})
    finally:
        conn.close()


# ─── Historial GPS ────────────────────────────────────────────────────────────

@app.route("/api/ruta/historial/<int:sesion_id>", methods=["GET"])
def historial_sesion(sesion_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT latitud, longitud, velocidad, precision_gps, timestamp
                   FROM puntos_gps WHERE sesion_id = %s ORDER BY timestamp""",
                (sesion_id,),
            )
            rows = cur.fetchall()
        return jsonify([serialize_row(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/conductor/<int:conductor_id>/hoy", methods=["GET"])
def ruta_conductor_dia(conductor_id):
    fecha = request.args.get("fecha", date.today().isoformat())
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT s.id AS sesion_id, s.estado, s.fecha_inicio, s.fecha_fin
                   FROM sesiones_ruta s
                   WHERE s.conductor_id = %s AND DATE(s.fecha_inicio) = %s
                   ORDER BY s.fecha_inicio DESC LIMIT 1""",
                (conductor_id, fecha),
            )
            sesion = cur.fetchone()
            puntos = []
            if sesion:
                cur.execute(
                    """SELECT latitud, longitud, velocidad, timestamp
                       FROM puntos_gps WHERE sesion_id = %s ORDER BY timestamp""",
                    (sesion["sesion_id"],),
                )
                puntos = cur.fetchall()
        return jsonify({
            "sesion": serialize_row(sesion) if sesion else None,
            "puntos": [serialize_row(p) for p in puntos],
        })
    finally:
        conn.close()


# ─── Rutas asignadas y ferreterías ────────────────────────────────────────────

@app.route("/api/ruta/asignada/<int:conductor_id>/hoy", methods=["GET"])
def ruta_asignada_hoy(conductor_id):
    fecha = request.args.get("fecha", date.today().isoformat())
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, nombre_ruta, descripcion, estado, color, fecha_asignacion
                   FROM rutas_asignadas
                   WHERE conductor_id = %s AND fecha_asignacion = %s
                   ORDER BY id DESC LIMIT 1""",
                (conductor_id, fecha),
            )
            ruta = cur.fetchone()
            paradas = []
            if ruta:
                cur.execute(
                    """SELECT p.id, p.nombre_ferreteria, p.direccion, p.latitud, p.longitud, p.orden,
                              (SELECT COUNT(*) FROM reportes_visita rv WHERE rv.parada_id = p.id) AS visitado,
                              (SELECT rv.vendio FROM reportes_visita rv
                               WHERE rv.parada_id = p.id ORDER BY rv.timestamp DESC LIMIT 1) AS vendio,
                              (SELECT rv.foto_url FROM reportes_visita rv
                               WHERE rv.parada_id = p.id ORDER BY rv.timestamp DESC LIMIT 1) AS foto_url
                       FROM paradas_ruta p WHERE p.ruta_id = %s ORDER BY p.orden""",
                    (ruta["id"],),
                )
                paradas = cur.fetchall()
        return jsonify({
            "ruta": serialize_row(ruta) if ruta else None,
            "paradas": [serialize_row(p) for p in paradas],
        })
    finally:
        conn.close()


@app.route("/api/ruta/<int:ruta_id>/paradas", methods=["GET"])
def paradas_ruta(ruta_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, nombre_ferreteria, direccion, latitud, longitud, orden
                   FROM paradas_ruta WHERE ruta_id = %s ORDER BY orden""",
                (ruta_id,),
            )
            rows = cur.fetchall()
        return jsonify([serialize_row(r) for r in rows])
    finally:
        conn.close()


# ─── Reportes de visita (foto + venta) ──────────────────────────────────────────

@app.route("/api/reporte/visita", methods=["POST"])
def crear_reporte_visita():
    data = request.get_json() or {}
    required = ["parada_id", "conductor_id"]
    if not all(data.get(k) for k in required):
        return jsonify({"error": "parada_id y conductor_id requeridos"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO reportes_visita
                   (parada_id, conductor_id, sesion_id, vendio, monto_venta, observaciones,
                    foto_url, latitud, longitud)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    data["parada_id"],
                    data["conductor_id"],
                    data.get("sesion_id"),
                    bool(data.get("vendio", False)),
                    data.get("monto_venta"),
                    data.get("observaciones"),
                    data.get("foto_url"),
                    data.get("lat"),
                    data.get("lng"),
                ),
            )
            reporte_id = cur.lastrowid
            cur.execute(
                """SELECT p.nombre_ferreteria, r.conductor_id
                   FROM paradas_ruta p
                   JOIN rutas_asignadas r ON r.id = p.ruta_id
                   WHERE p.id = %s""",
                (data["parada_id"],),
            )
            parada = cur.fetchone()
        broadcast_to_panel({
            "type": "reporte_visita",
            "conductor_id": data["conductor_id"],
            "parada_id": data["parada_id"],
            "nombre_ferreteria": parada["nombre_ferreteria"] if parada else "",
            "vendio": bool(data.get("vendio", False)),
            "foto_url": data.get("foto_url"),
        })
        return jsonify({"ok": True, "reporte_id": reporte_id})
    finally:
        conn.close()


@app.route("/api/conductor/<int:conductor_id>/reportes", methods=["GET"])
def reportes_conductor(conductor_id):
    fecha = request.args.get("fecha", date.today().isoformat())
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT rv.id, rv.vendio, rv.monto_venta, rv.observaciones, rv.foto_url,
                          rv.timestamp, p.nombre_ferreteria, p.direccion
                   FROM reportes_visita rv
                   JOIN paradas_ruta p ON p.id = rv.parada_id
                   WHERE rv.conductor_id = %s AND DATE(rv.timestamp) = %s
                   ORDER BY rv.timestamp DESC""",
                (conductor_id, fecha),
            )
            rows = cur.fetchall()
        return jsonify([serialize_row(r) for r in rows])
    finally:
        conn.close()


# ─── WebSockets ─────────────────────────────────────────────────────────────────

@sock.route("/ws/panel")
def ws_panel(ws):
    with panel_lock:
        panel_connections.append(ws)
    try:
        ws.send(json.dumps({"type": "connected", "message": "Panel conectado"}))
        while True:
            ws.receive()
    except Exception:
        pass
    finally:
        with panel_lock:
            if ws in panel_connections:
                panel_connections.remove(ws)


@sock.route("/ws/conductor/<int:conductor_id>")
def ws_conductor(ws, conductor_id):
    conductor = get_conductor_info(conductor_id)
    if not conductor:
        ws.send(json.dumps({"error": "Conductor no encontrado"}))
        return

    try:
        while True:
            raw = ws.receive()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            precision = float(data.get("precision", 0) or 0)
            if precision > MAX_GPS_PRECISION_M and precision > 0:
                continue

            lat = data.get("lat")
            lng = data.get("lng")
            sesion_id = data.get("sesion_id")
            velocidad = float(data.get("velocidad", 0) or 0)

            if lat is None or lng is None or not sesion_id:
                continue

            conn = get_db()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO puntos_gps
                           (sesion_id, conductor_id, latitud, longitud, velocidad, precision_gps)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (sesion_id, conductor_id, lat, lng, velocidad, precision),
                    )
                    cur.execute("SELECT NOW() AS ts")
                    ts_row = cur.fetchone()
                    ultimo = ts_row["ts"].isoformat() if ts_row else datetime.now().isoformat()
            finally:
                conn.close()

            estado = estado_conductor(velocidad, ultimo)
            update = {
                "type": "ubicacion",
                "conductor_id": conductor_id,
                "nombre": conductor["nombre"],
                "placa": conductor.get("placa") or "",
                "lat": float(lat),
                "lng": float(lng),
                "velocidad": velocidad,
                "ultimo_update": ultimo,
                "estado": estado,
            }
            broadcast_to_panel(update)
            try:
                ws.send(json.dumps({"ok": True}))
            except Exception:
                break
    except Exception:
        pass


# Solo para despliegue con functions-framework (Cloud Functions HTTP simple, sin WebSockets).
# En Cloud Run usar Gunicorn (ver Dockerfile).
try:
    import functions_framework

    @functions_framework.http
    def flota_zeus(request):
        with app.request_context(request.environ):
            return app.full_dispatch_request()
except ImportError:
    pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
