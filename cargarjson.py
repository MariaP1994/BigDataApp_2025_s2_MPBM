import os
import json
from dotenv import load_dotenv
from Helpers.elastic import ElasticSearch


def limpiar_documento(doc: dict) -> dict:
    """
    Limpia campos None y asegura formato correcto.
    También genera un _id basado en año + semana si aplica.
    """
    limpio = {k: v for k, v in doc.items() if v not in (None, "", [], {})}

    # Asegurar que "anio" quede como int
    if "anio" in limpio:
        try:
            limpio["anio"] = int(limpio["anio"])
        except:
            pass

    # Asegurar que "semana_epidemiologica" quede como string para Elastic
    if "semana_epidemiologica" in limpio:
        limpio["semana_epidemiologica"] = str(limpio["semana_epidemiologica"])

    # Generar ID único si el PDF viene de boletín
    if "anio" in limpio and "semana_epidemiologica" in limpio:
        limpio["_id"] = f"{limpio['anio']}-SEM-{limpio['semana_epidemiologica']}"

    return limpio


if __name__ == "__main__":
    # ================== CARGAR VARIABLES DE ENTORNO ==================
    load_dotenv("env.txt")

    ELASTIC_CLOUD_URL     = os.getenv("ELASTIC_CLOUD_URL")
    ELASTIC_API_KEY       = os.getenv("ELASTIC_API_KEY")
    ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT") or "index-boletin-semanal"

    print("ELASTIC_CLOUD_URL:", ELASTIC_CLOUD_URL)
    print("ELASTIC_INDEX_DEFAULT:", ELASTIC_INDEX_DEFAULT)

    # ================== CREAR CLIENTE DE ELASTIC ==================
    es = ElasticSearch(
        cloud_url=ELASTIC_CLOUD_URL,
        api_key=ELASTIC_API_KEY,
        default_index=ELASTIC_INDEX_DEFAULT,
    )

    print("\nProbando conexión a ElasticSearch...")
    if not es.test_connection():
        print("No se pudo conectar a ElasticSearch.")
        raise SystemExit(1)
    else:
        print("Conectado a ElasticSearch correctamente.")

    # ================== CARPETA CON LOS JSON ==================
    json_dir = os.path.join("data")

    print("\nUsando carpeta:", os.path.abspath(json_dir))
    print("Índice destino:", ELASTIC_INDEX_DEFAULT)

    if not os.path.isdir(json_dir):
        print("La carpeta de JSON no existe:", json_dir)
        raise SystemExit(1)

    # ================== CARGAR DOCUMENTOS DESDE LOS JSON ==================
    documentos = []

    for filename in sorted(os.listdir(json_dir)):
        if not filename.lower().endswith(".json"):
            continue

        ruta_archivo = os.path.join(json_dir, filename)
        print(f"Leyendo: {ruta_archivo}")

        try:
            with open(ruta_archivo, "r", encoding="utf-8") as f:
                data = json.load(f)

            # DEPENDIENDO DE TU WEBSCRAPING, cada JSON es *un documento*
            if isinstance(data, dict):
                doc = limpiar_documento(data)
                documentos.append(doc)
            else:
                print(f"Formato no reconocido en {filename}. Se omite.")

        except Exception as e:
            print(f"Error leyendo {filename}: {e}")

    print(f"\nDocumentos preparados para indexar: {len(documentos)}")

    if not documentos:
        print("No hay documentos válidos para indexar.")
        raise SystemExit(0)

    # ================== INDEXAR EN ELASTIC (BULK) ==================
    print("\n Indexando documentos en ElasticSearch...")

    resultado = es.indexar_bulk(
        documentos=documentos,
        index=ELASTIC_INDEX_DEFAULT,
    )

    print("\nResultado de indexación:")
    print(resultado)