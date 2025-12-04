import os
from dotenv import load_dotenv

from Helpers.elastic import ElasticSearch

if __name__ == "__main__":
    load_dotenv("env.txt")

    ELASTIC_CLOUD_URL     = os.getenv("ELASTIC_CLOUD_URL")
    ELASTIC_API_KEY       = os.getenv("ELASTIC_API_KEY")
    ELASTIC_INDEX_DEFAULT = os.getenv("ELASTIC_INDEX_DEFAULT") or "index-boletin-semanal"

    print("ELASTIC_CLOUD_URL:", ELASTIC_CLOUD_URL)
    print("ELASTIC_INDEX_DEFAULT:", ELASTIC_INDEX_DEFAULT)

    es = ElasticSearch(
        cloud_url=ELASTIC_CLOUD_URL,
        api_key=ELASTIC_API_KEY,
        default_index=ELASTIC_INDEX_DEFAULT,
    )

    print("\nProbando ping a Elastic...")
    if not es.test_connection():
        print("No se pudo conectar a ElasticSearch (ping falso).")
    else:
        print("Ping OK")

    print("\nListando índices visibles...")
    indices = es.listar_indices()
    print("Índices que ve este cliente:", indices)

    if ELASTIC_INDEX_DEFAULT in indices:
        print(f"El índice '{ELASTIC_INDEX_DEFAULT}' SÍ existe en el clúster.")
    else:
        print(f"Ojo: el índice '{ELASTIC_INDEX_DEFAULT}' NO aparece en la lista.")