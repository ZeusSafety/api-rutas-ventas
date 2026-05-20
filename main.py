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
# CONFIG DB — mismas variables que ejemplo.py + defaults como dashboard.py
###########################################################
_DEFAULT_INSTANCE = "stable-smithy-435414-m6:us-central1:zeussafety-2024"

DB_USER = os.getenv("DB_USER") or "zeussafety-2024"
DB_PASSWORD = os.getenv("DB_PASSWORD") or "ZeusSafety2025"
DB_NAME = os.getenv("DB_NAME") or "Zeus_Safety_Data_Integration"
# Solo desarrollo local con cloud-sql-proxy:
DB_HOST = (os.getenv("DB_HOST") or "").strip()
DB_PORT = int(os.getenv("DB_PORT", "3306"))


def _instance_connection_name():
    return (os.getenv("INSTANCE_CONNECTION_NAME") or "").strip() or _DEFAULT_INSTANCE

ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:3000,https://zeus-safety.vercel.app",
    ).split(",")
    if o.strip()
]
PORT = int(os.getenv("PORT", "8080"))

###########################################################
# APP
###########################################################

app = Flask(__name__)
sock = Sock(app)

# If deployer explicitly sets '*' allow all origins (no credentials),
# otherwise restrict to configured origins and allow credentials.
if any(o == "*" for o in ALLOWED_ORIGINS):
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)
else:
    CORS(app, origins=ALLOWED_ORIGINS, supports_credentials=True)

panel_connections = []
panel_lock = threading.Lock()
MAX_GPS_PRECISION_M = 50


def get_connection():
    """Cloud SQL por socket (producción) o TCP con DB_HOST (proxy local)."""
    common = dict(
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "20")),
        read_timeout=int(os.getenv("DB_READ_TIMEOUT", "120")),
        write_timeout=int(os.getenv("DB_WRITE_TIMEOUT", "120")),
    )

    if DB_HOST:
        return pymysql.connect(host=DB_HOST, port=DB_PORT, **common)

    return pymysql.connect(
        unix_socket=f"/cloudsql/{_instance_connection_name()}",
        **common,
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
    hint = "Revise conexión Cloud SQL y credenciales MySQL."
    if "doesn't exist" in msg.lower() or "1146" in msg:
        hint = "La tabla no existe: ejecute database/migrations.sql en Zeus_Safety_Data_Integration."
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


def get_vehiculo_asignado(conductor_id, fecha=None):
    """Vehículo asignado al conductor en una fecha."""
    fecha = fecha or date.today().isoformat()
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT v.id AS vehiculo_id, v.nombre AS vehiculo_nombre, v.placa, v.color
                   FROM asignaciones_conductor ac
                   JOIN vehiculos v ON v.id = ac.vehiculo_id
                   WHERE ac.conductor_id = %s AND ac.fecha_asignacion = %s
                   LIMIT 1""",
                (conductor_id, fecha),
            )
            return cur.fetchone()
    except Exception:
        return None
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


RESULTADOS_VISITA = frozenset({"vendio", "no_estaba", "reagendar"})

SQL_PARADAS_BASE = """
    SELECT id, nombre_ferreteria, direccion, latitud, longitud, orden
    FROM paradas_ruta WHERE ruta_id = %s ORDER BY orden
"""


def _resultado_desde_observaciones(observaciones, vendio=None):
    obs = observaciones or ""
    if vendio in (1, True, "1"):
        return "vendio"
    if obs.startswith("[no_estaba]"):
        return "no_estaba"
    if obs.startswith("[reagendar]"):
        return "reagendar"
    if obs.startswith("[vendio]"):
        return "vendio"
    return None


def _resultado_desde_payload(data):
    r = (data.get("resultado") or data.get("resultado_visita") or "").strip().lower()
    if r in RESULTADOS_VISITA:
        return r
    if data.get("vendio") is True:
        return "vendio"
    if data.get("vendio") is False:
        return "no_estaba"
    return None


def _observaciones_con_resultado(resultado, observaciones):
    obs = (observaciones or "").strip()
    for tag in ("[vendio]", "[no_estaba]", "[reagendar]"):
        if obs.startswith(tag):
            obs = obs[len(tag) :].strip()
    prefix = {
        "vendio": "[vendio]",
        "no_estaba": "[no_estaba]",
        "reagendar": "[reagendar]",
    }[resultado]
    return f"{prefix} {obs}".strip() if obs else prefix


def _fetch_paradas_ruta(cur, ruta_id):
    """Paradas de la ruta + estado de visita (evita subconsultas con columna `timestamp`)."""
    cur.execute(SQL_PARADAS_BASE, (ruta_id,))
    out = []
    for p in cur.fetchall():
        row = serialize_row(p)
        row["visitado"] = False
        row["resultado_visita"] = None
        row["vendio"] = None
        row["foto_url"] = None
        out.append(row)
    if not out:
        return out

    ids = [r["id"] for r in out]
    ph = ",".join(["%s"] * len(ids))

    cur.execute(
        f"SELECT parada_id, COUNT(*) AS cnt FROM reportes_visita "
        f"WHERE parada_id IN ({ph}) GROUP BY parada_id",
        ids,
    )
    counts = {r["parada_id"]: int(r["cnt"]) for r in cur.fetchall()}

    latest_sql = (
        f"SELECT rv.parada_id, rv.vendio, rv.observaciones, rv.foto_url, rv.resultado_visita "
        f"FROM reportes_visita rv INNER JOIN ( "
        f"  SELECT parada_id, MAX(`timestamp`) AS max_ts FROM reportes_visita "
        f"  WHERE parada_id IN ({ph}) GROUP BY parada_id "
        f") t ON t.parada_id = rv.parada_id AND rv.`timestamp` = t.max_ts "
        f"WHERE rv.parada_id IN ({ph})"
    )
    latest_sql_fallback = (
        f"SELECT rv.parada_id, rv.vendio, rv.observaciones, rv.foto_url "
        f"FROM reportes_visita rv INNER JOIN ( "
        f"  SELECT parada_id, MAX(`timestamp`) AS max_ts FROM reportes_visita "
        f"  WHERE parada_id IN ({ph}) GROUP BY parada_id "
        f") t ON t.parada_id = rv.parada_id AND rv.`timestamp` = t.max_ts "
        f"WHERE rv.parada_id IN ({ph})"
    )
    params = ids + ids
    try:
        cur.execute(latest_sql, params)
    except pymysql.err.OperationalError:
        cur.execute(latest_sql_fallback, params)

    latest = {r["parada_id"]: r for r in cur.fetchall()}
    for row in out:
        pid = row["id"]
        row["visitado"] = bool(counts.get(pid, 0))
        rep = latest.get(pid)
        if not rep:
            continue
        rep = serialize_row(rep)
        row["vendio"] = rep.get("vendio")
        row["foto_url"] = rep.get("foto_url")
        rv = rep.get("resultado_visita")
        row["resultado_visita"] = (
            rv
            if rv
            else _resultado_desde_observaciones(rep.get("observaciones"), rep.get("vendio"))
        )
    return out


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
        return jsonify({
            "status": "ok",
            "database": "connected",
            "db": DB_NAME,
            "instance": _instance_connection_name(),
        })
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


# ─── Vehículos y flota ────────────────────────────────────────────────────────

@app.route("/api/vehiculos", methods=["GET"])
def list_vehiculos():
    try:
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, nombre, placa, color, activo
                       FROM vehiculos WHERE activo = 1 ORDER BY nombre"""
                )
                rows = cur.fetchall()
            return jsonify([serialize_row(r) for r in rows])
        finally:
            conn.close()
    except Exception as e:
        body, code = db_error_response(e)
        return body, code


@app.route("/api/vehiculo/<int:vehiculo_id>/conductores-hoy", methods=["GET"])
def conductores_vehiculo_hoy(vehiculo_id):
    """Conductores asignados al vehículo en la fecha indicada (app conductor)."""
    fecha = request.args.get("fecha", date.today().isoformat())
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT c.id, c.nombre, c.telefono, c.placa, c.activo,
                          ac.fecha_asignacion, v.nombre AS vehiculo_nombre
                   FROM asignaciones_conductor ac
                   JOIN conductores c ON c.id = ac.conductor_id AND c.activo = 1
                   JOIN vehiculos v ON v.id = ac.vehiculo_id
                   WHERE ac.vehiculo_id = %s AND ac.fecha_asignacion = %s
                   ORDER BY c.nombre""",
                (vehiculo_id, fecha),
            )
            rows = cur.fetchall()
        return jsonify([serialize_row(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/flota/en-vivo", methods=["GET"])
def flota_en_vivo():
    """Vehículos con conductor asignado hoy (panel lateral + mapa)."""
    fecha = request.args.get("fecha", date.today().isoformat())
    try:
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT v.id AS vehiculo_id, v.nombre AS vehiculo_nombre,
                              v.placa, v.color,
                              c.id AS conductor_id, c.nombre AS conductor_nombre,
                              r.id AS ruta_id, r.nombre_ruta, r.estado AS ruta_estado
                       FROM vehiculos v
                       LEFT JOIN asignaciones_conductor ac
                         ON ac.vehiculo_id = v.id AND ac.fecha_asignacion = %s
                       LEFT JOIN conductores c ON c.id = ac.conductor_id AND c.activo = 1
                       LEFT JOIN rutas_asignadas r
                         ON r.vehiculo_id = v.id AND r.fecha_asignacion = %s
                       WHERE v.activo = 1
                       ORDER BY v.nombre""",
                    (fecha, fecha),
                )
                rows = cur.fetchall()
            out = []
            for row in rows:
                item = serialize_row(row)
                item["nombre"] = item.get("vehiculo_nombre") or ""
                item["conductor_id"] = item.get("conductor_id")
                out.append(item)
            return jsonify(out)
        finally:
            conn.close()
    except Exception as e:
        body, code = db_error_response(e)
        return body, code


@app.route("/api/asignacion", methods=["POST"])
def asignar_conductor_vehiculo():
    data = request.get_json() or {}
    vehiculo_id = data.get("vehiculo_id")
    conductor_id = data.get("conductor_id")
    fecha = data.get("fecha", date.today().isoformat())
    if not vehiculo_id or not conductor_id:
        return jsonify({"error": "vehiculo_id y conductor_id requeridos"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO asignaciones_conductor (vehiculo_id, conductor_id, fecha_asignacion)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE conductor_id = VALUES(conductor_id)""",
                (vehiculo_id, conductor_id, fecha),
            )
        return jsonify({"ok": True, "vehiculo_id": vehiculo_id, "conductor_id": conductor_id, "fecha": fecha})
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
    finally:
        conn.close()


@app.route("/api/ruta/planificar", methods=["POST"])
def planificar_ruta():
    """Crea ruta planificada + paradas y asigna conductor al vehículo."""
    data = request.get_json() or {}
    vehiculo_id = data.get("vehiculo_id")
    conductor_id = data.get("conductor_id")
    nombre_ruta = (data.get("nombre_ruta") or "").strip()
    fecha = data.get("fecha", date.today().isoformat())
    paradas = data.get("paradas") or []

    if not vehiculo_id or not conductor_id or not nombre_ruta:
        return jsonify({"error": "vehiculo_id, conductor_id y nombre_ruta son requeridos"}), 400
    if not paradas:
        return jsonify({"error": "Agregue al menos una parada (ferretería)"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO asignaciones_conductor (vehiculo_id, conductor_id, fecha_asignacion)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE conductor_id = VALUES(conductor_id)""",
                (vehiculo_id, conductor_id, fecha),
            )
            cur.execute("SELECT color FROM vehiculos WHERE id = %s", (vehiculo_id,))
            veh = cur.fetchone()
            color = (veh or {}).get("color") or "#2563eb"

            cur.execute(
                """INSERT INTO rutas_asignadas
                   (vehiculo_id, conductor_id, nombre_ruta, descripcion, fecha_asignacion, estado, color)
                   VALUES (%s, %s, %s, %s, %s, 'pendiente', %s)""",
                (
                    vehiculo_id,
                    conductor_id,
                    nombre_ruta,
                    data.get("descripcion"),
                    fecha,
                    color,
                ),
            )
            ruta_id = cur.lastrowid

            for i, p in enumerate(paradas):
                cur.execute(
                    """INSERT INTO paradas_ruta
                       (ruta_id, nombre_ferreteria, direccion, latitud, longitud, orden)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        ruta_id,
                        p.get("nombre_ferreteria") or p.get("nombre") or f"Parada {i + 1}",
                        p.get("direccion"),
                        p.get("latitud") or p.get("lat"),
                        p.get("longitud") or p.get("lng"),
                        p.get("orden", i + 1),
                    ),
                )

        return jsonify({
            "ok": True,
            "ruta_id": ruta_id,
            "vehiculo_id": vehiculo_id,
            "conductor_id": conductor_id,
            "fecha": fecha,
        })
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
    finally:
        conn.close()


@app.route("/api/rutas/historial", methods=["GET"])
def historial_rutas():
    """Listado de todas las rutas planificadas (filtros opcionales)."""
    fecha_desde = request.args.get("fecha_desde")
    fecha_hasta = request.args.get("fecha_hasta")
    vehiculo_id = request.args.get("vehiculo_id")

    conn = get_db()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT r.id, r.nombre_ruta, r.descripcion, r.fecha_asignacion, r.estado, r.color,
                       r.created_at,
                       v.id AS vehiculo_id, v.nombre AS vehiculo_nombre,
                       c.id AS conductor_id, c.nombre AS conductor_nombre,
                       (SELECT COUNT(*) FROM paradas_ruta p WHERE p.ruta_id = r.id) AS total_paradas,
                       (SELECT COUNT(DISTINCT rv.parada_id)
                        FROM paradas_ruta p
                        JOIN reportes_visita rv ON rv.parada_id = p.id
                        WHERE p.ruta_id = r.id) AS paradas_visitadas
                FROM rutas_asignadas r
                LEFT JOIN vehiculos v ON v.id = r.vehiculo_id
                LEFT JOIN conductores c ON c.id = r.conductor_id
                WHERE 1=1
            """
            params = []
            if fecha_desde:
                sql += " AND r.fecha_asignacion >= %s"
                params.append(fecha_desde)
            if fecha_hasta:
                sql += " AND r.fecha_asignacion <= %s"
                params.append(fecha_hasta)
            if vehiculo_id:
                sql += " AND r.vehiculo_id = %s"
                params.append(vehiculo_id)
            sql += " ORDER BY r.fecha_asignacion DESC, r.id DESC LIMIT 500"
            cur.execute(sql, params)
            rows = cur.fetchall()
        return jsonify([serialize_row(r) for r in rows])
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
    finally:
        conn.close()


@app.route("/api/ruta/asignada/vehiculo/<int:vehiculo_id>/hoy", methods=["GET"])
def ruta_asignada_vehiculo_hoy(vehiculo_id):
    fecha = request.args.get("fecha", date.today().isoformat())
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, nombre_ruta, descripcion, estado, color, fecha_asignacion, conductor_id
                   FROM rutas_asignadas
                   WHERE vehiculo_id = %s AND fecha_asignacion = %s
                   ORDER BY id DESC LIMIT 1""",
                (vehiculo_id, fecha),
            )
            ruta = cur.fetchone()
            paradas = _fetch_paradas_ruta(cur, ruta["id"]) if ruta else []
        return jsonify({
            "ruta": serialize_row(ruta) if ruta else None,
            "paradas": paradas,
        })
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
    finally:
        conn.close()


# ─── Sesiones GPS ─────────────────────────────────────────────────────────────

@app.route("/api/sesion/iniciar", methods=["POST"])
def iniciar_sesion():
    data = request.get_json() or {}
    conductor_id = data.get("conductor_id")
    ruta_asignada_id = data.get("ruta_asignada_id")
    vehiculo_id = data.get("vehiculo_id")
    if not conductor_id:
        return jsonify({"error": "conductor_id requerido"}), 400

    if not vehiculo_id:
        veh = get_vehiculo_asignado(conductor_id)
        vehiculo_id = veh["vehiculo_id"] if veh else None

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sesiones_ruta (conductor_id, vehiculo_id, ruta_asignada_id, estado)
                   VALUES (%s, %s, %s, 'activa')""",
                (conductor_id, vehiculo_id, ruta_asignada_id),
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


@app.route("/api/sesion/puntos-batch", methods=["POST"])
def puntos_gps_batch():
    """Sincroniza puntos GPS guardados offline en la app conductor."""
    data = request.get_json() or {}
    sesion_id = data.get("sesion_id")
    conductor_id = data.get("conductor_id")
    puntos = data.get("puntos") or []
    if not sesion_id or not conductor_id:
        return jsonify({"error": "sesion_id y conductor_id requeridos"}), 400
    if not isinstance(puntos, list) or not puntos:
        return jsonify({"ok": True, "insertados": 0})

    insertados = 0
    conn = get_db()
    try:
        with conn.cursor() as cur:
            for pt in puntos[:500]:
                lat = pt.get("lat")
                lng = pt.get("lng")
                if lat is None or lng is None:
                    continue
                precision = float(pt.get("precision", 0) or 0)
                if precision > MAX_GPS_PRECISION_M and precision > 0:
                    continue
                velocidad = float(pt.get("velocidad", 0) or 0)
                cur.execute(
                    """INSERT INTO puntos_gps
                       (sesion_id, conductor_id, latitud, longitud, velocidad, precision_gps)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (sesion_id, conductor_id, lat, lng, velocidad, precision),
                )
                insertados += 1
        return jsonify({"ok": True, "insertados": insertados})
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
            paradas = _fetch_paradas_ruta(cur, ruta["id"]) if ruta else []
        return jsonify({
            "ruta": serialize_row(ruta) if ruta else None,
            "paradas": [serialize_row(p) for p in paradas],
        })
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
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


def _get_ruta_row(cur, ruta_id):
    cur.execute(
        """SELECT r.id, r.nombre_ruta, r.descripcion, r.fecha_asignacion, r.estado, r.color,
                  r.vehiculo_id, r.conductor_id, r.created_at,
                  v.nombre AS vehiculo_nombre, v.placa AS vehiculo_placa,
                  c.nombre AS conductor_nombre,
                  (SELECT COUNT(*) FROM paradas_ruta p WHERE p.ruta_id = r.id) AS total_paradas
           FROM rutas_asignadas r
           LEFT JOIN vehiculos v ON v.id = r.vehiculo_id
           LEFT JOIN conductores c ON c.id = r.conductor_id
           WHERE r.id = %s""",
        (ruta_id,),
    )
    return cur.fetchone()


@app.route("/api/ruta/<int:ruta_id>", methods=["GET"])
def obtener_ruta_detalle(ruta_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            ruta = _get_ruta_row(cur, ruta_id)
            if not ruta:
                return jsonify({"error": "Ruta no encontrada"}), 404
            paradas = _fetch_paradas_ruta(cur, ruta_id)
        out = serialize_row(ruta)
        out["paradas"] = paradas
        return jsonify(out)
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
    finally:
        conn.close()


@app.route("/api/ruta/<int:ruta_id>", methods=["PUT"])
def actualizar_ruta(ruta_id):
    data = request.get_json() or {}
    conn = get_db()
    try:
        with conn.cursor() as cur:
            ruta = _get_ruta_row(cur, ruta_id)
            if not ruta:
                return jsonify({"error": "Ruta no encontrada"}), 404
            estado = (ruta.get("estado") or "pendiente").lower()

            if estado == "completada":
                return jsonify({"error": "Ruta completada: no se puede modificar"}), 403

            if estado == "en_curso":
                descripcion = data.get("descripcion")
                if descripcion is None and not data.get("nombre_ruta"):
                    return jsonify({"error": "Solo puede actualizar descripción u observaciones"}), 400
                cur.execute(
                    """UPDATE rutas_asignadas SET descripcion = %s WHERE id = %s""",
                    (descripcion, ruta_id),
                )
                if data.get("nombre_ruta"):
                    cur.execute(
                        "UPDATE rutas_asignadas SET nombre_ruta = %s WHERE id = %s",
                        ((data.get("nombre_ruta") or "").strip(), ruta_id),
                    )
                return jsonify({"ok": True, "ruta_id": ruta_id, "modo": "parcial"})

            vehiculo_id = data.get("vehiculo_id", ruta["vehiculo_id"])
            conductor_id = data.get("conductor_id", ruta["conductor_id"])
            nombre_ruta = (data.get("nombre_ruta") or ruta["nombre_ruta"] or "").strip()
            fecha = data.get("fecha", ruta["fecha_asignacion"])
            paradas = data.get("paradas")

            if not nombre_ruta:
                return jsonify({"error": "nombre_ruta requerido"}), 400

            cur.execute(
                """UPDATE rutas_asignadas
                   SET vehiculo_id = %s, conductor_id = %s, nombre_ruta = %s,
                       descripcion = %s, fecha_asignacion = %s
                   WHERE id = %s""",
                (
                    vehiculo_id,
                    conductor_id,
                    nombre_ruta,
                    data.get("descripcion"),
                    fecha,
                    ruta_id,
                ),
            )
            if fecha and vehiculo_id and conductor_id:
                cur.execute(
                    """INSERT INTO asignaciones_conductor (vehiculo_id, conductor_id, fecha_asignacion)
                       VALUES (%s, %s, %s)
                       ON DUPLICATE KEY UPDATE conductor_id = VALUES(conductor_id)""",
                    (vehiculo_id, conductor_id, fecha),
                )

            if paradas is not None:
                if not paradas:
                    return jsonify({"error": "Agregue al menos una parada"}), 400
                cur.execute("DELETE FROM paradas_ruta WHERE ruta_id = %s", (ruta_id,))
                for i, p in enumerate(paradas):
                    cur.execute(
                        """INSERT INTO paradas_ruta
                           (ruta_id, nombre_ferreteria, direccion, latitud, longitud, orden)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (
                            ruta_id,
                            p.get("nombre_ferreteria") or p.get("nombre") or f"Parada {i + 1}",
                            p.get("direccion"),
                            p.get("latitud") or p.get("lat"),
                            p.get("longitud") or p.get("lng"),
                            p.get("orden", i + 1),
                        ),
                    )

        return jsonify({"ok": True, "ruta_id": ruta_id})
    except Exception as e:
        body, code = db_error_response(e)
        return body, code
    finally:
        conn.close()


@app.route("/api/ruta/<int:ruta_id>", methods=["DELETE"])
def eliminar_ruta(ruta_id):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            ruta = _get_ruta_row(cur, ruta_id)
            if not ruta:
                return jsonify({"error": "Ruta no encontrada"}), 404
            estado = (ruta.get("estado") or "pendiente").lower()
            if estado != "pendiente":
                return jsonify({
                    "error": "Solo se pueden eliminar rutas en estado pendiente",
                    "estado": estado,
                }), 403
            cur.execute("DELETE FROM paradas_ruta WHERE ruta_id = %s", (ruta_id,))
            cur.execute("DELETE FROM rutas_asignadas WHERE id = %s", (ruta_id,))
        return jsonify({"ok": True, "ruta_id": ruta_id})
    finally:
        conn.close()


# ─── Reportes de visita (foto + venta) ──────────────────────────────────────────

@app.route("/api/reporte/visita", methods=["POST"])
def crear_reporte_visita():
    data = request.get_json() or {}
    required = ["parada_id", "conductor_id"]
    if not all(data.get(k) for k in required):
        return jsonify({"error": "parada_id y conductor_id requeridos"}), 400

    resultado = _resultado_desde_payload(data)
    if not resultado:
        return jsonify({"error": "resultado inválido (vendio, no_estaba, reagendar)"}), 400

    vendio = resultado == "vendio"
    observaciones = _observaciones_con_resultado(resultado, data.get("observaciones"))

    conn = get_db()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """INSERT INTO reportes_visita
                       (parada_id, conductor_id, sesion_id, vendio, resultado_visita,
                        monto_venta, observaciones, foto_url, latitud, longitud)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        data["parada_id"],
                        data["conductor_id"],
                        data.get("sesion_id"),
                        vendio,
                        resultado,
                        data.get("monto_venta"),
                        observaciones,
                        data.get("foto_url"),
                        data.get("lat"),
                        data.get("lng"),
                    ),
                )
            except pymysql.err.OperationalError:
                cur.execute(
                    """INSERT INTO reportes_visita
                       (parada_id, conductor_id, sesion_id, vendio, monto_venta, observaciones,
                        foto_url, latitud, longitud)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        data["parada_id"],
                        data["conductor_id"],
                        data.get("sesion_id"),
                        vendio,
                        data.get("monto_venta"),
                        observaciones,
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
            "vendio": vendio,
            "resultado": resultado,
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
            veh = get_vehiculo_asignado(conductor_id)
            update = {
                "type": "ubicacion",
                "conductor_id": conductor_id,
                "nombre": conductor["nombre"],
                "conductor_nombre": conductor["nombre"],
                "placa": conductor.get("placa") or "",
                "lat": float(lat),
                "lng": float(lng),
                "velocidad": velocidad,
                "ultimo_update": ultimo,
                "estado": estado,
            }
            if veh:
                update["vehiculo_id"] = veh["vehiculo_id"]
                update["vehiculo_nombre"] = veh["vehiculo_nombre"]
                update["nombre"] = veh["vehiculo_nombre"]
                update["placa"] = veh.get("placa") or update["placa"]
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
