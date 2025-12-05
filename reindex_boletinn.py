import os
import json
import glob

from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# ==============================
# 1. Cargar variables del env.txt
# ==============================
load_dotenv("env.txt")

ELASTIC_CLOUD_URL       = os.getenv("ELASTIC_CLOUD_URL")
ELASTIC_API_KEY         = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX_DEFAULT   = os.getenv("ELASTIC_INDEX_DEFAULT") or "index-boletin-semanal"

# 🔴 AJUSTA ESTA RUTA A TU CARPETA REAL DE JSON
CARPETA_JSON = r"C:\Users\ARNULFO\Documents\GitHub\BigDataApp_2025_s2_MPBM\data"

# ==============================
# 2. Crear cliente de Elasticsearch
#    (IGUAL que en tu clase Helpers/elastic.py)
# ==============================
if not ELASTIC_CLOUD_URL:
    raise ValueError("ELASTIC_CLOUD_URL no está configurada en env.txt")
if not ELASTIC_API_KEY:
    raise ValueError("ELASTIC_API_KEY no está configurada en env.txt")

es = Elasticsearch(
    hosts=[ELASTIC_CLOUD_URL],
    api_key=ELASTIC_API_KEY,
)

# ==============================
# 3. Función principal de reindexación
# ==============================
def main():
    indice = ELASTIC_INDEX_DEFAULT
    print(f"Usando índice: {indice}")
    print(f"Leyendo JSON desde: {CARPETA_JSON}")

    archivos = glob.glob(os.path.join(CARPETA_JSON, "*.json"))
    print(f"Encontré {len(archivos)} archivos JSON para indexar")

    if not archivos:
        print("⚠ No se encontraron archivos .json en la carpeta indicada.")
        return

    count_ok = 0
    count_fail = 0

    for ruta in archivos:
        try:
            with open(ruta, "r", encoding="utf-8") as f:
                doc = json.load(f)

            # Indexar el documento
            es.index(index=indice, document=doc)
            count_ok += 1
        except Exception as e:
            print(f"Error al indexar {ruta}: {e}")
            count_fail += 1

    print("========== RESUMEN ==========")
    print(f"Documentos indexados OK : {count_ok}")
    print(f"Documentos con error    : {count_fail}")


if __name__ == "__main__":
    main()