from flask import Flask, render_template, request, redirect, url_for, jsonify, session
from elasticsearch import Elasticsearch
from datetime import timedelta
import os

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
app.secret_key = "tu_clave_secreta"
app.permanent_session_lifetime = timedelta(hours=5)

# ========================
# CONFIGURACIÓN ELASTIC
# ========================
ELASTIC_ENDPOINT = os.getenv("ELASTIC_ENDPOINT")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX = "index-boletin-semanal"

if not ELASTIC_ENDPOINT:
    raise RuntimeError("ELASTIC_ENDPOINT no está configurada (revisa tu .env)")
if not ELASTIC_API_KEY:
    raise RuntimeError("ELASTIC_API_KEY no está configurada (revisa tu .env)")

# Cliente de Elasticsearch usando ENDPOINT (no cloud_id)
es = Elasticsearch(
    hosts=[ELASTIC_ENDPOINT],
    api_key=ELASTIC_API_KEY,
)


# ========================
# RUTAS BÁSICAS
# ========================

@app.route("/")
def index():
    return render_template("landing.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario")
        password = request.form.get("password")

        # Lógica sencilla solo para pruebas
        if usuario == "admin" and password == "123":
            session["usuario"] = usuario
            return redirect(url_for("buscador"))

        return render_template("login.html", error_message="Credenciales inválidas")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ========================
# BUSCADOR
# ========================

@app.route("/buscador")
def buscador():
    # if "usuario" not in session:
    #     return redirect(url_for("login"))
    return render_template("buscador.html")


@app.route("/buscar-elastic", methods=["POST"])
def buscar_elastic():
    try:
        data = request.json or {}

        texto = (data.get("texto") or "").trim() if hasattr(str, "trim") else (data.get("texto") or "").strip()
        # la línea de arriba es por seguridad, pero realmente .strip() basta:
        texto = (data.get("texto") or "").strip()

        anio_filtro = data.get("anio")
        semana_filtro = data.get("semana")
        tipo_archivo = data.get("tipo_archivo")

        if not texto:
            return jsonify({"success": False, "error": "Texto vacío"})

        # --------------------------
        # QUERY BASE
        # --------------------------
        query_base = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": texto,
                            "fields": [
                                "tema_central",
                                "temas_portada",
                                "rango_fechas",
                                "publicacion_en_linea",
                                "semana_epidemiologica"
                                # OJO: quitamos "anio" porque es numérico
                            ],
                            "type": "best_fields"
                        }
                    }
                ],
                "filter": []
            }
        }

        # Filtro por año (numérico)
        if anio_filtro:
            try:
                anio_int = int(anio_filtro)
                query_base["bool"]["filter"].append(
                    {"term": {"anio": anio_int}}
                )
            except ValueError:
                pass

        # Filtro por semana (dos dígitos)
        if semana_filtro:
            try:
                semana_int = int(semana_filtro)
                semana_str = f"{semana_int:02d}"   # 1 -> "01"
                query_base["bool"]["filter"].append(
                    {"term": {"semana_epidemiologica": semana_str}}
                )
            except ValueError:
                pass

        # Filtro por tipo de archivo
        if tipo_archivo:
            query_base["bool"]["filter"].append(
                {"term": {"tipo_archivo": tipo_archivo}}
            )

        # --------------------------
        # EJECUTAR QUERY EN ELASTIC
        # --------------------------
        resultado = es.search(
            index=ELASTIC_INDEX,
            size=150,
            query=query_base
        )

        total = resultado["hits"]["total"]["value"]
        hits = resultado["hits"]["hits"]

        return jsonify({
            "success": True,
            "total": total,
            "hits": hits
        })

    except Exception as e:
        print("Error en /buscar-elastic:", e)
        return jsonify({"success": False, "error": str(e)})


# ========================
# EJECUCIÓN LOCAL
# ========================
if __name__ == "__main__":
    app.run(debug=True)