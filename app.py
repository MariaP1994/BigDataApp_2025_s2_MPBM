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
# Lee el archivo env.txt (local y en Render si el archivo existe en el repo)
load_dotenv("env.txt")

app = Flask(__name__)

# --------- VARIABLES DE CONFIGURACIÓN (con valores por defecto) ---------
MONGO_URI       = os.getenv("MONGO_URI")
MONGO_DB        = os.getenv("MONGO_DB") or "proyecto_bigdata"
MONGO_COLECCION = os.getenv("MONGO_COLECCION") or "usuario_roles"

ELASTIC_CLOUD_URL     = os.getenv("ELASTIC_CLOUD_URL")
ELASTIC_API_KEY       = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT") or "indice-boletin-semanal"

app.secret_key        = os.getenv("SECRET_KEY") or "Mpbm1234"

# Versión de la aplicación
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
elastic = ElasticSearch(ELASTIC_CLOUD_URL, ELASTIC_API_KEY)

# ==================== RUTAS PÚBLICAS ====================

@app.route("/")
def landing():
    """Landing page pública"""
    return render_template(
        "landing.html",
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route("/about")
def about():
    """Página About"""
    return render_template(
        "about.html",
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# ==================== BUSCADOR ELASTIC (PÚBLICO) ====================

@app.route("/buscador")
def buscador():
    """Página de búsqueda pública"""
    return render_template(
        "buscador.html",
        version=VERSION_APP,
        creador=CREATOR_APP,
        ELASTIC_INDEX_DEFAULT=ELASTIC_INDEX_DEFAULT,
    )


@app.route("/buscar-elastic", methods=["POST"])
def buscar_elastic():
    """API para realizar búsqueda en ElasticSearch"""
    try:
        data = request.get_json()
        texto_buscar = (data.get("texto") or "").strip()
        campo = data.get("campo", "_all")

        if not texto_buscar:
            return jsonify({
                "success": False,
                "error": "Texto de búsqueda es requerido"
            }), 400

        # Query básica de texto
        query_base = {
            "query": {
                "multi_match": {
                    "query": texto_buscar,
                    "fields": [
                        "tema_central^3",
                        "semana_epidemiologica",
                        "fechas",
                        "expertos_tematicos",
                        "publicacion_en_linea",
                        "_all"
                    ]
                }
            }
        }

        # Aggregations adaptables (puedes cambiarlas luego)
        aggs = {
            "boletines_por_anio": {
                "terms": {
                    "field": "anio",
                    "size": 20
                }
            },
            "boletines_por_experto": {
                "terms": {
                    "field": "expertos_tematicos.keyword",
                    "size": 20
                }
            }
        }

        resultado = elastic.buscar(
            index=ELASTIC_INDEX_DEFAULT,
            query=query_base,
            aggs=aggs,
            size=100
        )

        return jsonify(resultado)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== AUTENTICACIÓN Y USUARIOS (MONGODB) ====================

@app.route("/login", methods=["GET", "POST"])
def login():
    """Página de login con validación.

    Tiene un 'plan B' hardcodeado:
    - Usuario:  administrador
    - Password: Admin1234
    """
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        password = request.form.get("password", "").strip()

        # ---- PLAN B: usuario hardcodeado (siempre funciona) ----
        if usuario == "administrador" and password == "Admin1234":
            session["usuario"] = usuario
            session["permisos"] = {
                "admin_usuarios": True,
                "admin_elastic": True,
                "admin_data_elastic": True,
            }
            session["logged_in"] = True

            flash("¡Bienvenido! Inicio de sesión exitoso", "success")
            return redirect(url_for("admin"))

        # ---- PLAN A: validar contra MongoDB (si está bien configurado) ----
        user_data = mongo.validar_usuario(usuario, password, MONGO_COLECCION)

        if user_data:
            session["usuario"] = usuario
            session["permisos"] = user_data.get("permisos", {})
            session["logged_in"] = True

            flash("¡Bienvenido! Inicio de sesión exitoso", "success")
            return redirect(url_for("admin"))
        else:
            flash("Usuario o contraseña incorrectos", "danger")

    return render_template(
        "login.html",
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route("/logout")
def logout():
    """Cerrar sesión"""
    session.clear()
    flash("Sesión cerrada correctamente", "info")
    return redirect(url_for("landing"))


@app.route("/listar-usuarios")
def listar_usuarios():
    """Devuelve listado de usuarios desde MongoDB (solo para depurar)."""
    try:
        usuarios = mongo.listar_usuarios(MONGO_COLECCION)
        for usuario in usuarios:
            usuario["_id"] = str(usuario["_id"])
        return jsonify(usuarios)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/gestor_usuarios")
def gestor_usuarios():
    """Gestión de usuarios (requiere login y permiso admin_usuarios)"""
    if not session.get("logged_in"):
        flash("Por favor, inicia sesión para acceder a esta página", "warning")
        return redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get("admin_usuarios"):
        flash("No tiene permisos para gestionar usuarios", "danger")
        return redirect(url_for("admin"))

    return render_template(
        "gestor_usuarios.html",
        usuario=session.get("usuario"),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route("/crear-usuario", methods=["POST"])
def crear_usuario():
    """API para crear un nuevo usuario"""
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_usuarios"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para crear usuarios"
            }), 403

        data = request.get_json()
        usuario = data.get("usuario")
        password = data.get("password")
        permisos_usuario = data.get("permisos", {})

        if not usuario or not password:
            return jsonify({
                "success": False,
                "error": "Usuario y password son requeridos"
            }), 400

        usuario_existente = mongo.obtener_usuario(usuario, MONGO_COLECCION)
        if usuario_existente:
            return jsonify({
                "success": False,
                "error": "El usuario ya existe"
            }), 400

        resultado = mongo.crear_usuario(
            usuario, password, permisos_usuario, MONGO_COLECCION
        )

        if resultado:
            return jsonify({"success": True})
        else:
            return jsonify({
                "success": False,
                "error": "Error al crear usuario"
            }), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/actualizar-usuario", methods=["POST"])
def actualizar_usuario():
    """API para actualizar un usuario existente"""
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_usuarios"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para actualizar usuarios"
            }), 403

        data = request.get_json()
        usuario_original = data.get("usuario_original")
        datos_usuario = data.get("datos", {})

        if not usuario_original:
            return jsonify({
                "success": False,
                "error": "Usuario original es requerido"
            }), 400

        usuario_existente = mongo.obtener_usuario(usuario_original, MONGO_COLECCION)
        if not usuario_existente:
            return jsonify({
                "success": False,
                "error": "Usuario no encontrado"
            }), 404

        nuevo_usuario = datos_usuario.get("usuario")
        if nuevo_usuario and nuevo_usuario != usuario_original:
            usuario_duplicado = mongo.obtener_usuario(nuevo_usuario, MONGO_COLECCION)
            if usuario_duplicado:
                return jsonify({
                    "success": False,
                    "error": "Ya existe otro usuario con ese nombre"
                }), 400

        resultado = mongo.actualizar_usuario(
            usuario_original, datos_usuario, MONGO_COLECCION
        )

        if resultado:
            return jsonify({"success": True})
        else:
            return jsonify({
                "success": False,
                "error": "Error al actualizar usuario"
            }), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/eliminar-usuario", methods=["POST"])
def eliminar_usuario():
    """API para eliminar un usuario"""
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_usuarios"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para eliminar usuarios"
            }), 403

        data = request.get_json()
        usuario = data.get("usuario")

        if not usuario:
            return jsonify({
                "success": False,
                "error": "Usuario es requerido"
            }), 400

        usuario_existente = mongo.obtener_usuario(usuario, MONGO_COLECCION)
        if not usuario_existente:
            return jsonify({
                "success": False,
                "error": "Usuario no encontrado"
            }), 404

        if usuario == session.get("usuario"):
            return jsonify({
                "success": False,
                "error": "No puede eliminarse a sí mismo"
            }), 400

        resultado = mongo.eliminar_usuario(usuario, MONGO_COLECCION)

        if resultado:
            return jsonify({"success": True})
        else:
            return jsonify({
                "success": False,
                "error": "Error al eliminar usuario"
            }), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== GESTOR ELASTIC (ADMIN) ====================

@app.route("/gestor_elastic")
def gestor_elastic():
    """Página de gestión de ElasticSearch (requiere login y permiso admin_elastic)"""
    if not session.get("logged_in"):
        flash("Por favor, inicia sesión para acceder a esta página", "warning")
        return redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get("admin_elastic"):
        flash("No tiene permisos para gestionar ElasticSearch", "danger")
        return redirect(url_for("admin"))

    return render_template(
        "gestor_elastic.html",
        usuario=session.get("usuario"),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP
    )


@app.route("/listar-indices-elastic")
def listar_indices_elastic():
    """API para listar índices de ElasticSearch"""
    try:
        if not session.get("logged_in"):
            return jsonify({"error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_elastic"):
            return jsonify({
                "error": "No tiene permisos para gestionar ElasticSearch"
            }), 403

        indices = elastic.listar_indices()
        return jsonify(indices)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/ejecutar-query-elastic", methods=["POST"])
def ejecutar_query_elastic():
    """API para ejecutar una query en ElasticSearch"""
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_elastic"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para gestionar ElasticSearch"
            }), 403

        data = request.get_json()
        query_json = data.get("query")

        if not query_json:
            return jsonify({
                "success": False,
                "error": "Query es requerida"
            }), 400

        resultado = elastic.ejecutar_query(query_json)
        return jsonify(resultado)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/cargar_doc_elastic")
def cargar_doc_elastic():
    """Página de carga de documentos a ElasticSearch (requiere login y permiso admin_data_elastic)"""
    if not session.get("logged_in"):
        flash("Por favor, inicia sesión para acceder a esta página", "warning")
        return redirect(url_for("login"))

    permisos = session.get("permisos", {})
    if not permisos.get("admin_data_elastic"):
        flash("No tiene permisos para cargar datos a ElasticSearch", "danger")
        return redirect(url_for("admin"))

    return render_template(
        "documentos_elastic.html",
        usuario=session.get("usuario"),
        permisos=permisos,
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# ---------- PROCESAR ZIP CON JSON ----------
@app.route("/procesar-zip-elastic", methods=["POST"])
def procesar_zip_elastic():
    """
    Recibe un ZIP con archivos JSON,
    lo descomprime en static/uploads y devuelve la lista de archivos.
    """
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_data_elastic"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para cargar datos a ElasticSearch"
            }), 403

        if "file" not in request.files:
            return jsonify({
                "success": False,
                "error": "No se recibió archivo ZIP"
            }), 400

        file = request.files["file"]
        index = request.form.get("index")

        if not index:
            return jsonify({
                "success": False,
                "error": "Índice de destino es requerido"
            }), 400

        if file.filename == "":
            return jsonify({
                "success": False,
                "error": "Nombre de archivo vacío"
            }), 400

        carpeta_destino = "static/uploads"
        Funciones.crear_carpeta(carpeta_destino)
        Funciones.borrar_contenido_carpeta(carpeta_destino)

        zip_path = os.path.join(carpeta_destino, "temp.zip")
        file.save(zip_path)

        archivos = Funciones.descomprimir_zip_local(zip_path, carpeta_destino)
        os.remove(zip_path)

        archivos_json = []
        for a in archivos:
            if a["extension"].lower() == ".json":
                ruta = a["ruta"]
                archivos_json.append({
                    "nombre": a["nombre"],
                    "ruta": ruta,
                    "extension": "json",
                    "tamaño": os.path.getsize(ruta) if os.path.exists(ruta) else 0
                })

        return jsonify({
            "success": True,
            "archivos": archivos_json,
            "mensaje": f"Se encontraron {len(archivos_json)} archivos JSON en el ZIP"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ---------- PROCESAR WEB SCRAPING ----------
@app.route("/procesar-webscraping-elastic", methods=["POST"])
def procesar_webscraping_elastic():
    """API para procesar Web Scraping (descargar PDFs)"""
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_data_elastic"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para cargar datos"
            }), 403

        data = request.get_json()
        url = data.get("url")
        extensiones_navegar = data.get("extensiones_navegar", "aspx")
        tipos_archivos = data.get("tipos_archivos", "pdf")
        index = data.get("index")

        if not url or not index:
            return jsonify({
                "success": False,
                "error": "URL e índice son requeridos"
            }), 400

        lista_ext_navegar = [ext.strip() for ext in extensiones_navegar.split(",")]
        lista_tipos_archivos = [ext.strip() for ext in tipos_archivos.split(",")]

        todas_extensiones = lista_ext_navegar + lista_tipos_archivos

        scraper = WebScraping(dominio_base=url.rsplit("/", 1)[0] + "/")

        carpeta_upload = "static/uploads"
        Funciones.crear_carpeta(carpeta_upload)
        Funciones.borrar_contenido_carpeta(carpeta_upload)

        json_path = os.path.join(carpeta_upload, "links.json")
        resultado = scraper.extraer_todos_los_links(
            url_inicial=url,
            json_file_path=json_path,
            listado_extensiones=todas_extensiones,
            max_iteraciones=50
        )

        if not resultado.get("success", True):
            scraper.close()
            return jsonify({
                "success": False,
                "error": "Error al extraer enlaces"
            }), 500

        resultado_descarga = scraper.descargar_pdfs(json_path, carpeta_upload)
        scraper.close()

        archivos = Funciones.listar_archivos_carpeta(
            carpeta_upload,
            lista_tipos_archivos
        )

        return jsonify({
            "success": True,
            "archivos": archivos,
            "mensaje": f"Se descargaron {len(archivos)} archivos",
            "stats": {
                "total_enlaces": resultado.get("total_links", 0),
                "descargados": resultado_descarga.get("descargados", 0),
                "errores": resultado_descarga.get("errores", 0)
            }
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ---------- CARGAR DOCUMENTOS SELECCIONADOS A ELASTIC ----------
@app.route("/cargar-documentos-elastic", methods=["POST"])
def cargar_documentos_elastic():
    """
    Recibe la lista de archivos seleccionados (JSON) y los indexa en ElasticSearch.
    Actualmente implementado solo para método='zip' (archivos JSON).
    """
    try:
        if not session.get("logged_in"):
            return jsonify({"success": False, "error": "No autorizado"}), 401

        permisos = session.get("permisos", {})
        if not permisos.get("admin_data_elastic"):
            return jsonify({
                "success": False,
                "error": "No tiene permisos para cargar datos a ElasticSearch"
            }), 403

        data = request.get_json()
        archivos = data.get("archivos", [])
        index = data.get("index")
        metodo = data.get("metodo", "zip")

        if not index:
            return jsonify({
                "success": False,
                "error": "Índice es requerido"
            }), 400

        if metodo != "zip":
            return jsonify({
                "success": False,
                "error": "Actualmente solo está implementada la carga desde ZIP con JSON."
            }), 400

        docs = []
        for a in archivos:
            ruta = a.get("ruta")
            if not ruta or not os.path.exists(ruta):
                continue

            contenido = Funciones.leer_json(ruta)
            if isinstance(contenido, list):
                docs.extend(contenido)
            elif isinstance(contenido, dict):
                docs.append(contenido)

        if not docs:
            return jsonify({
                "success": False,
                "error": "No se encontraron documentos válidos para indexar"
            }), 400

        resultado = elastic.indexar_bulk(index=index, documentos=docs)

        if not resultado.get("success", False):
            return jsonify(resultado), 500

        return jsonify({
            "success": True,
            "indexados": resultado.get("indexados", 0),
            "errores": resultado.get("fallidos", 0)
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==================== PÁGINA ADMIN ====================

@app.route("/admin")
def admin():
    """Página de administración (protegida requiere login)"""
    if not session.get("logged_in"):
        flash("Por favor, inicia sesión para acceder al área de administración", "warning")
        return redirect(url_for("login"))

    return render_template(
        "admin.html",
        usuario=session.get("usuario"),
        permisos=session.get("permisos"),
        version=VERSION_APP,
        creador=CREATOR_APP
    )


# ==================== MAIN ====================
if __name__ == "__main__":
    # Crear carpetas necesarias
    Funciones.crear_carpeta("static/uploads")

    print("\n" + "=" * 50)
    print("VERIFICANDO CONEXIONES")

    if mongo.test_connection():
        print("✅ MongoDB Atlas: Conectado")
    else:
        print("❌ MongoDB Atlas: Error de conexión")

    if elastic.test_connection():
        print("✅ ElasticSearch Cloud: Conectado")
    else:
        print("❌ ElasticSearch Cloud: Error de conexión")

    app.run(debug=True, host="0.0.0.0", port=5000)
