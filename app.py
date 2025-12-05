from flask import (
    Flask, render_template, request, redirect,
    url_for, jsonify, session, flash
)
from dotenv import load_dotenv
import os
import json

# ====== IMPORTS DE NUESTROS MÓDULOS ======
from Helpers.mongoDB import MongoDB
from Helpers.elastic import ElasticSearch
from Helpers.funciones import Funciones
from Helpers.webScraping import WebScraping

# ================= CARGAR VARIABLES DE ENTORNO =================
load_dotenv("env.txt")

app = Flask(__name__)

# --------- VARIABLES DE CONFIGURACIÓN ---------
MONGO_URI       = os.getenv("MONGO_URI")
MONGO_DB        = os.getenv("MONGO_DB") or "proyecto_bigdata"
MONGO_COLECCION = os.getenv("MONGO_COLECCION") or "usuario_roles"

ELASTIC_CLOUD_URL     = os.getenv("ELASTIC_CLOUD_URL")
ELASTIC_API_KEY       = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT") or "index-boletin-semanal"

app.secret_key = os.getenv("SECRET_KEY") or "Mpbm1234"

VERSION_APP = "1.2.0"
CREATOR_APP = "María Paula Barrero"

# --------- DEBUG RÁPIDO ---------
print("DEBUG MONGO_URI:", repr(MONGO_URI))
print("DEBUG MONGO_DB:", repr(MONGO_DB))
print("DEBUG MONGO_COLECCION:", repr(MONGO_COLECCION))
print("DEBUG ELASTIC_CLOUD_URL:", repr(ELASTIC_CLOUD_URL))
print("DEBUG ELASTIC_INDEX_DEFAULT:", repr(ELASTIC_INDEX_DEFAULT))

# ================= INICIALIZAR CONEXIONES =================
mongo = MongoDB(MONGO_URI, MONGO_DB)
elastic = ElasticSearch(
    cloud_url=ELASTIC_CLOUD_URL,
    api_key=ELASTIC_API_KEY,
    default_index=ELASTIC_INDEX_DEFAULT,
)

# ==================== RUTAS PÚBLICAS ====================

@app.route("/")
def landing():
    return render_template(
        "landing.html",
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route("/about")
def about():
    return render_template(
        "about.html",
        version=VERSION_APP,
        creador=CREATOR_APP
    )

# ==================== BUSCADOR SIMPLE ====================

@app.route("/buscador")
def buscador():
    return render_template(
        "buscador.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
        ELASTIC_INDEX_DEFAULT=ELASTIC_INDEX_DEFAULT
    )


@app.route("/buscar-elastic", methods=["POST"])
def buscar_elastic():
    """
    Buscador SIMPLE:
    - texto  -> palabra clave
    - anio   -> año (opcional)
    - semana -> semana epidemiológica (opcional)
    """
    try:
        data = request.get_json() or {}

        texto  = (data.get("texto")  or "").strip()
        anio   = (data.get("anio")   or "").strip()
        semana = (data.get("semana") or "").strip()

        if not texto and not anio and not semana:
            return jsonify({
                "success": False,
                "error": "Debes ingresar al menos una palabra clave, un año o una semana."
            }), 400

        must_clauses = []
        filter_clauses = []

        # Palabra clave: busca en los campos principales
        if texto:
            must_clauses.append({
                "multi_match": {
                    "query": texto,
                    "fields": [
                        "tema_central^3",
                        "temas_portada^2",
                        "expertos_tematicos",
                        "publicacion_en_linea",
                        "rango_fechas",
                        "_all"
                    ]
                }
            })

        # Filtro por año
        if anio:
            try:
                anio_int = int(anio)
                filter_clauses.append({"term": {"anio": anio_int}})
            except ValueError:
                filter_clauses.append({"term": {"anio": anio}})

        # Filtro por semana epidemiológica
        if semana:
            filter_clauses.append({"term": {"semana_epidemiologica": str(semana)}})

        query = {
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else [{"match_all": {}}],
                    "filter": filter_clauses
                }
            }
        }

        resultado = elastic.buscar(
            index=ELASTIC_INDEX_DEFAULT,
            query=query,
            size=150
        )

        return jsonify(resultado)

    except Exception as e:
        print("Error en /buscar-elastic:", e)
        return jsonify({"success": False, "error": str(e)}), 500

# ==================== LOGIN ====================

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        password = request.form.get("password", "").strip()

        # PLAN B – Login fijo
        if usuario == "administrador" and password == "Admin1234":
            session["usuario"] = usuario
            session["permisos"] = {
                "admin_usuarios": True,
                "admin_elastic": True,
                "admin_data_elastic": True,
            }
            session["logged_in"] = True
            flash("Inicio exitoso", "success")
            return redirect(url_for("admin"))

        # PLAN A – Login desde Mongo
        user_data = mongo.validar_usuario(usuario, password, MONGO_COLECCION)

        if user_data:
            session["usuario"] = usuario
            session["permisos"] = user_data.get("permisos", {})
            session["logged_in"] = True
            flash("Inicio exitoso", "success")
            return redirect(url_for("admin"))
        else:
            flash("Usuario o contraseña incorrectos", "danger")

    return render_template(
        "login.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
    )


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada", "info")
    return redirect(url_for("landing"))

# ==================== ADMIN ====================

@app.route("/admin")
def admin():
    if not session.get("logged_in"):
        flash("Inicia sesión", "warning")
        return redirect(url_for("login"))

    return render_template(
        "admin.html",
        usuario=session.get("usuario"),
        permisos=session.get("permisos"),
        version=VERSION_APP,
        creador=CREATOR_APP,
    )

# ==================== USUARIOS ====================

@app.route("/listar-usuarios")
def listar_usuarios():
    try:
        usuarios = mongo.listar_usuarios(MONGO_COLECCION)
        for u in usuarios:
            u["_id"] = str(u["_id"])
        return jsonify(usuarios)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/gestor_usuarios")
def gestor_usuarios():
    if not session.get("logged_in"):
        flash("Inicia sesión", "warning")
        return redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get("admin_usuarios"):
        flash("No tiene permisos", "danger")
        return redirect(url_for("admin"))

    return render_template(
        "gestor_usuarios.html",
        usuario=session.get("usuario"),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP,
    )

# ==================== ELASTIC: GESTOR ====================

@app.route("/gestor_elastic")
def gestor_elastic():
    if not session.get("logged_in"):
        flash("Inicia sesión", "warning")
        return redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get("admin_elastic"):
        flash("No tiene permisos", "danger")
        return redirect(url_for("admin"))

    return render_template(
        "gestor_elastic.html",
        usuario=session.get("usuario"),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP,
    )

# ---------- API: LISTAR ÍNDICES DE ELASTIC (para gestor_elastic) ----------

@app.route("/listar-indices-elastic")
def listar_indices_elastic():
    """
    Devuelve una lista de índices de ElasticSearch en el formato que
    espera gestor_elastic.html.
    """
    try:
        client = elastic.client
        indices_raw = client.cat.indices(format="json")

        indices = []
        for idx in indices_raw:
            indices.append({
                "nombre": idx.get("index"),
                "total_documentos": int(idx.get("docs.count") or 0),
                "tamano": idx.get("store.size") or idx.get("pri.store.size") or "0b",
                "salud": idx.get("health"),
                "estado": idx.get("status"),
            })

        return jsonify(indices)

    except Exception as e:
        print("Error al listar índices de Elastic:", e)
        return jsonify({"error": str(e)}), 500

# ---------- API: EJECUTAR QUERY (SEARCH) EN ELASTIC ----------

@app.route("/ejecutar-query-elastic", methods=["POST"])
def ejecutar_query_elastic():
    """
    Recibe un JSON en texto desde el frontend, lo parsea y llama a
    elastic.ejecutar_query(), que ya tienes definido en Helpers/elastic.py
    """
    try:
        data = request.get_json() or {}
        query_text = data.get("query")

        if not query_text:
            return jsonify({
                "success": False,
                "error": "No se recibió el texto de la query"
            }), 400

        try:
            query_json = json.loads(query_text)
        except json.JSONDecodeError as e:
            return jsonify({
                "success": False,
                "error": f"JSON inválido: {str(e)}"
            }), 400

        resultado = elastic.ejecutar_query(query_json)
        return jsonify(resultado)

    except Exception as e:
        print("Error en /ejecutar-query-elastic:", e)
        return jsonify({"success": False, "error": str(e)}), 500

# ---------- API: EJECUTAR DML (index / update / delete) EN ELASTIC ----------

@app.route("/ejecutar-dml-elastic", methods=["POST"])
def ejecutar_dml_elastic():
    """
    Ejecuta operaciones DML sencillas sobre Elastic:
    {
      "operacion": "index" | "update" | "delete",
      "index": "nombre_indice",
      "id": "1",
      "documento": { ... }
    }
    """
    try:
        data = request.get_json() or {}
        comando_text = data.get("comando")

        if not comando_text:
            return jsonify({
                "success": False,
                "error": "No se recibió el comando DML"
            }), 400

        try:
            comando = json.loads(comando_text)
        except json.JSONDecodeError as e:
            return jsonify({
                "success": False,
                "error": f"JSON inválido: {str(e)}"
            }), 400

        operacion = (comando.get("operacion") or "").lower()
        index = comando.get("index") or ELASTIC_INDEX_DEFAULT
        doc_id = comando.get("id")
        documento = comando.get("documento") or comando.get("doc")

        if operacion not in ("index", "update", "delete"):
            return jsonify({
                "success": False,
                "error": "operacion debe ser 'index', 'update' o 'delete'"
            }), 400

        if operacion in ("index", "update") and not documento:
            return jsonify({
                "success": False,
                "error": "Debe enviar 'documento' para index/update"
            }), 400

        client = elastic.client

        if operacion == "index":
            resp = client.index(index=index, id=doc_id, document=documento, refresh=True)
        elif operacion == "update":
            if not doc_id:
                return jsonify({
                    "success": False,
                    "error": "Debe enviar 'id' para update"
                }), 400
            resp = client.update(index=index, id=doc_id, doc=documento, refresh=True)
        else:  # delete
            if not doc_id:
                return jsonify({
                    "success": False,
                    "error": "Debe enviar 'id' para delete"
                }), 400
            resp = client.delete(index=index, id=doc_id, refresh=True)

        return jsonify({"success": True, "data": resp})

    except Exception as e:
        print("Error en /ejecutar-dml-elastic:", e)
        return jsonify({"success": False, "error": str(e)}), 500

# ==================== ELASTIC: CARGA DE DATOS ====================

@app.route("/cargar_doc_elastic")
def cargar_doc_elastic():
    if not session.get("logged_in"):
        flash("Inicia sesión", "warning")
        return redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get("admin_data_elastic"):
        flash("No tiene permisos", "danger")
        return redirect(url_for("admin"))

    return render_template(
        "documentos_elastic.html",
        usuario=session.get("usuario"),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP,
    )

# ==================== MAIN ====================

if __name__ == "__main__":
    Funciones.crear_carpeta("static/uploads")

    print("\n===============================")
    print("VERIFICANDO CONEXIONES")

    if mongo.test_connection():
        print("MongoDB Atlas OK")
    else:
        print("Error MongoDB")

    if elastic.test_connection():
        print("ElasticSearch Cloud OK")
    else:
        print("Error ElasticSearch")

    app.run(debug=True, host="0.0.0.0", port=5000)