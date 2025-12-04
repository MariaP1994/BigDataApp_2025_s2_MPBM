from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from typing import Dict, List


class ElasticSearch:
    def __init__(self, cloud_url: str, api_key: str):
        """
        Inicializa conexión a ElasticSearch Cloud.

        Args:
            cloud_url: URL del deployment de Elastic Cloud (https://....:443)
            api_key: API Key del cluster
        """
        if not cloud_url:
            raise ValueError("ELASTIC_CLOUD_URL no está configurada")
        if not api_key:
            raise ValueError("ELASTIC_API_KEY no está configurada")

        # Cliente de Elastic
        self.client = Elasticsearch(
            hosts=[cloud_url],
            api_key=api_key,
        )

    def test_connection(self) -> bool:
        """Verifica conexión a Elastic."""
        try:
            return self.client.ping()
        except ConnectionError as e:
            print(f"Error de conexión a Elastic: {e}")
            return False

    def buscar(self, index: str, query: Dict, aggs=None, size: int = 10):
        """
        Ejecuta una búsqueda en ElasticSearch.

        Args:
            index: nombre del índice
            query: dict con la query (debe incluir 'query')
            aggs: agregaciones (dict opcional)
            size: número de resultados
        """
        body = query.copy() if query else {}

        if aggs:
            body["aggs"] = aggs

        resp = self.client.search(index=index, body=body, size=size)

        return {
            "success": True,
            "total": resp["hits"]["total"]["value"],
            "hits": resp["hits"]["hits"],
            "aggs": resp.get("aggregations", {})
        }

    def listar_indices(self):
        """Lista los índices disponibles en el cluster."""
        try:
            # Devuelve un dict de índices -> info
            indices = self.client.indices.get_alias("*")
            salida = []
            for nombre, data in indices.items():
                salida.append({
                    "nombre": nombre,
                    "total_documentos": data.get("indices", {}).get(nombre, {}).get("total", {}).get("docs", {}).get("count", 0)
                })
            return salida
        except Exception as e:
            print(f"Error al listar índices: {e}")
            return []

    def ejecutar_query(self, query_json: Dict):
        """Ejecuta una query raw enviada como dict."""
        try:
            index = query_json.pop("index", "_all")
            resp = self.client.search(index=index, body=query_json)
            return {
                "success": True,
                "total": resp["hits"]["total"]["value"],
                "hits": resp["hits"]["hits"],
                "aggs": resp.get("aggregations", {})
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def indexar_bulk(self, index: str, documentos: List[Dict]) -> Dict:
        """Carga múltiples documentos a un índice."""
        try:
            acciones = []
            for doc in documentos:
                acciones.append({"index": {"_index": index}})
                acciones.append(doc)

            resp = self.client.bulk(body=acciones)
            errores = sum(1 for item in resp["items"] if item.get("index", {}).get("error"))

            return {
                "success": errores == 0,
                "indexados": len(documentos),
                "fallidos": errores
            }
        except Exception as e:
            return {"success": False, "error": str(e)}