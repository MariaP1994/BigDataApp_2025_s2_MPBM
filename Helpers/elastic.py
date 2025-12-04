from typing import Dict, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, AuthenticationException


class ElasticSearch:
    def __init__(
        self,
        cloud_url: str,
        api_key: str,
        default_index: str = "index-boletin-semanal",
    ):
        """
        Inicializa conexión a ElasticSearch Cloud.

        Args:
            cloud_url: URL del deployment de Elastic Cloud (https://....:443)
            api_key:  API Key del cluster
            default_index: índice por defecto (por ejemplo, index-boletin-semanal)
        """
        if not cloud_url:
            raise ValueError("ELASTIC_CLOUD_URL no está configurada")
        if not api_key:
            raise ValueError("ELASTIC_API_KEY no está configurada")

        self.default_index = default_index

        # Cliente de Elastic
        self.client = Elasticsearch(
            hosts=[cloud_url],
            api_key=api_key,
        )

    # ------------------------------------------------------------------ #
    #   UTILIDADES BÁSICAS
    # ------------------------------------------------------------------ #

    def test_connection(self) -> bool:
        """Verifica conexión a Elastic."""
        try:
            return self.client.ping()
        except AuthenticationException as e:
            print(f"Error de autenticación con Elastic: {e}")
            return False
        except ConnectionError as e:
            print(f"Error de conexión a Elastic: {e}")
            return False
        except Exception as e:
            print(f"Error inesperado al conectar a Elastic: {e}")
            return False

    def listar_indices(self) -> List[str]:
        """Devuelve la lista de índices disponibles en el clúster."""
        try:
            resp = self.client.indices.get(index="*")
            return list(resp.keys())
        except AuthenticationException as e:
            print("Error al listar índices (autenticación):", e)
            return []
        except Exception as e:
            print("Error al listar índices:", e)
            return []

    # ------------------------------------------------------------------ #
    #   OPERACIONES DE BÚSQUEDA
    # ------------------------------------------------------------------ #

    def buscar(
        self,
        index: Optional[str] = None,
        query: Optional[Dict] = None,
        aggs: Optional[Dict] = None,
        size: int = 10,
    ) -> Dict:


        if not index:
            index = self.default_index

        body = query.copy() if query else {}
        if aggs:
            body["aggs"] = aggs

        try:
            resp = self.client.search(index=index, body=body, size=size)
            return {
                "success": True,
                "total": resp["hits"]["total"]["value"],
                "hits": resp["hits"]["hits"],
                "aggs": resp.get("aggregations", {}),
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def ejecutar_query(self, query_json: Dict) -> Dict:
        """Ejecuta una query raw enviada como dict."""
        try:
            index = query_json.get("index", self.default_index)
            body = {k: v for k, v in query_json.items() if k != "index"}

            resp = self.client.search(index=index, body=body)

            return {
                "success": True,
                "total": resp["hits"]["total"]["value"],
                "hits": resp["hits"]["hits"],
                "aggs": resp.get("aggregations", {}),
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------ #
    #   CARGA MASIVA (BULK)
    # ------------------------------------------------------------------ #

    def indexar_bulk(
        self,
        documentos: List[Dict],
        index: Optional[str] = None,
    ) -> Dict:

        try:
            if not index:
                index = self.default_index

            acciones = []
            for doc in documentos:
                # Acción de indexación
                acciones.append({"index": {"_index": index}})
                # Documento
                acciones.append(doc)

            resp = self.client.bulk(body=acciones)

            errores = sum(
                1
                for item in resp.get("items", [])
                if item.get("index", {}).get("error")
            )

            return {
                "success": errores == 0,
                "indexados": len(documentos),
                "fallidos": errores,
            }

        except Exception as e:
            return {"success": False, "error": str(e)}