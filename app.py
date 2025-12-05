from flask import (
    Flask, render_template, request, redirect,
    url_for, jsonify, session, flash
)
from dotenv import load_dotenv
import os

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

# ==================== BUSCADOR ====================

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
    """Realiza búsqueda en ElasticSearch."""
    try:
        data = request.get_json()
        texto_buscar = (data.get("texto") or "").strip()

        anio_filtro   = data.get("anio")
        semana_filtro = data.get("semana")
        tipo_archivo  = data.get("tipo_archivo")

        if not texto_buscar:
            return jsonify({"success": False, "error": "Texto requerido"}), 400

        # Query principal
        query_base = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": texto_buscar,
                            "fields": [
                                "tema_central^3",
                                "temas_portada^2",
                                "semana_epidemiologica",
                                "rango_fechas",
                                "expertos_tematicos",
                                "publicacion_en_linea",
                                "_all",
                            ],
                        }
                    }
                ],
                "filter": [],
            }
        }

        # Filtros
        if anio_filtro:
            try:
                anio_int = int(anio_filtro)
                query_base["bool"]["filter"].append({"term": {"anio": anio_int}})
            except ValueError:
                pass

        if semana_filtro:
            try:
                semana_int = int(semana_filtro)
                query_base["bool"]["filter"].append(
                    {"term": {"semana_epidemiologica": str(semana_int)}}
                )
            except ValueError:
                pass

        if tipo_archivo:
            query_base["bool"]["filter"].append(
                {"term": {"tipo_archivo.keyword": tipo_archivo}}
            )

        aggs = {
            "boletines_por_anio": {
                "terms": {"field": "anio", "size": 20}
            },
            "boletines_por_tema_portada": {
                "terms": {"field": "temas_portada.keyword", "size": 20}
            },
        }

        resultado = elastic.buscar(
            index=ELASTIC_INDEX_DEFAULT,
            query={"query": query_base},
            aggs=aggs,
            size=100,
        )

        return jsonify(resultado)

    except Exception as e:
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