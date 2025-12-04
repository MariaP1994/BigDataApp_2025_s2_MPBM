import os
import json
from dotenv import load_dotenv

from Helpers.elastic import ElasticSearch


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

    # Probar conexión
    print("\nProbando conexión a ElasticSearch...")
    if not es.test_connection():
        print("No se pudo conectar a ElasticSearch. Revisa URL o API KEY en env.txt")
        raise SystemExit(1)
    else:
        print("Conectado a ElasticSearch")

    # ================== CARPETA CON LOS JSON ==================
    json_dir = os.path.join("data") 

    print("\nUsando carpeta:", os.path.abspath(json_dir))
    print("Índice destino:", ELASTIC_INDEX_DEFAULT)

    if not os.path.isdir(json_dir):
        print("La carpeta de JSON no existe. Revisa la ruta:", json_dir)
        raise SystemExit(1)

    # ================== CARGAR DOCUMENTOS DESDE LOS JSON ==================
    documentos = []

    for filename in os.listdir(json_dir):
        if not filename.lower().endswith(".json"):
            continue

        ruta_archivo = os.path.join(json_dir, filename)
        print(f" Leyendo: {ruta_archivo}")

        try:
            with open(ruta_archivo, "r", encoding="utf-8") as f:
                data = json.load(f)

                # Si el JSON es una lista de docs, los agregamos todos
                if isinstance(data, list):
                    documentos.extend(data)
                # Si es un solo documento, lo agregamos
                elif isinstance(data, dict):
                    documentos.append(data)
                else:
                    print(f"⚠ Formato no reconocido en {filename}: {type(data)}")

        except Exception as e:
            print(f"Error leyendo {filename}: {e}")

    print(f"\nTotal de documentos preparados para indexar: {len(documentos)}")

    if not documentos:
        print("⚠ No hay documentos para indexar. Revisa el contenido de la carpeta de JSON.")
        raise SystemExit(0)

    # ================== INDEXAR EN ELASTIC (BULK) ==================
    print("\nIndexando documentos en ElasticSearch...")
    resultado = es.indexar_bulk(documentos=documentos, index=ELASTIC_INDEX_DEFAULT)

    print("\nResultado indexación:")
    print(resultado)