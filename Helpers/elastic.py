from elasticsearch import Elasticsearch
from typing import Dict, List, Optional, Any
import json

class ElasticSearch:
    def _init_(self, cloud_url: str, api_key: str):
        """
        Inicializa conexión a ElasticSearch Cloud

        Args:
            cloud_url: URL del cluster de Elastic Cloud
            api_key: API Key para autenticación
        """
        self.client = Elasticsearch(
            cloud_url,
            api_key=api_key,
            verify_certs=True
        )

    # -------------------------------------------------
    # UTILIDADES BÁSICAS
    # -------------------------------------------------
    def test_connection(self) -> bool:
        """Prueba la conexión a ElasticSearch"""
        try:
            info = self.client.info()
            print(f"✅ Conectado a Elastic: {info['version']['number']}")
            return True
        except Exception as e:
            print(f"❌ Error al conectar con Elastic: {e}")
            return False

    def close(self):
        """Cierra la conexión"""
        self.client.close()

    # -------------------------------------------------
    # ÍNDICES
    # -------------------------------------------------
    def crear_index(self, nombre_index: str, mappings: Dict = None, settings: Dict = None) -> bool:
        """
        Crea un nuevo índice
        """
        try:
            body = {}
            if mappings:
                body['mappings'] = mappings
            if settings:
                body['settings'] = settings

            self.client.indices.create(index=nombre_index, body=body)
            return True
        except Exception as e:
            print(f"Error al crear índice: {e}")
            return False

    def eliminar_index(self, nombre_index: str) -> bool:
        """Elimina un índice"""
        try:
            self.client.indices.delete(index=nombre_index)
            return True
        except Exception as e:
            print(f"Error al eliminar índice: {e}")
            return False

    def listar_indices(self) -> List[Dict]:
        """Lista todos los índices con información detallada"""
        try:
            indices = self.client.cat.indices(
                format='json',
                h='index,docs.count,store.size,health,status'
            )

            indices_formateados = []
            for idx in indices:
                docs = idx.get('docs.count', '0')
                indices_formateados.append({
                    'nombre': idx.get('index', ''),
                    'total_documentos': int(docs) if str(docs).isdigit() else 0,
                    'tamaño': idx.get('store.size', '0b'),
                    'salud': idx.get('health', 'unknown'),
                    'estado': idx.get('status', 'unknown')
                })

            return indices_formateados
        except Exception as e:
            print(f"Error al listar índices: {e}")
            return []

    # -------------------------------------------------
    # INDEXAR DOCUMENTOS
    # -------------------------------------------------
    def indexar_documento(self, index: str, documento: Dict, doc_id: str = None) -> bool:
        """
        Indexa un documento en ElasticSearch
        """
        try:
            if doc_id:
                self.client.index(index=index, id=doc_id, document=documento)
            else:
                self.client.index(index=index, document=documento)
            return True
        except Exception as e:
            print(f"Error al indexar documento: {e}")
            return False

    def indexar_bulk(self, index: str, documentos: List[Dict]) -> Dict:
        """
        Indexa múltiples documentos de forma masiva
        """
        from elasticsearch.helpers import bulk

        try:
            acciones = [
                {
                    '_index': index,
                    '_source': doc
                }
                for doc in documentos
            ]

            success, errors = bulk(self.client, acciones, raise_on_error=False)

            return {
                'success': True,
                'indexados': success,
                'fallidos': len(errors) if errors else 0,
                'errores': errors if errors else []
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    # 👉 NUEVO: indexar todos los JSON de una carpeta (ej. data/)
    def indexar_json_desde_carpeta(self, index: str, carpeta: str) -> Dict:
        """
        Lee todos los archivos .json de una carpeta y los indexa en ElasticSearch.

        Cada archivo puede contener:
        - Un único objeto JSON ({ ... })
        - O una lista de objetos ([{...}, {...}, ...])

        Args:
            index: índice de destino
            carpeta: ruta a la carpeta (por ejemplo 'data')

        Returns:
            Estadísticas de indexación
        """
        import os

        documentos: List[Dict] = []

        for nombre in os.listdir(carpeta):
            if not nombre.lower().endswith('.json'):
                continue

            ruta = os.path.join(carpeta, nombre)
            print(f"📄 Leyendo {ruta} ...")

            try:
                with open(ruta, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if isinstance(data, list):
                    documentos.extend(data)
                elif isinstance(data, dict):
                    documentos.append(data)
                else:
                    print(f"⚠ Formato no reconocido en {nombre}, se omite.")
            except Exception as e:
                print(f"⚠ Error leyendo {nombre}: {e}")

        if not documentos:
            return {
                'success': False,
                'error': 'No se encontraron documentos JSON válidos'
            }

        print(f"➡ Indexando {len(documentos)} documentos en '{index}' ...")
        return self.indexar_bulk(index, documentos)

    # -------------------------------------------------
    # BÚSQUEDAS
    # -------------------------------------------------
    def buscar(self, index: str, query: Dict, aggs: Dict = None, size: int = 10) -> Dict:
        """
        Realiza una búsqueda en ElasticSearch

        IMPORTANTE:
        - Devuelve 'hits' (no 'resultados') para que coincida con buscador.html
        - Devuelve las aggregations reales que responde Elastic
        """
        try:
            body = query.copy() if query else {}

            if aggs:
                body['aggs'] = aggs

            response = self.client.search(index=index, body=body, size=size)

            return {
                'success': True,
                'total': response['hits']['total']['value'],
                'hits': response['hits']['hits'],
                'aggs': response.get('aggregations', {})
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def buscar_texto(self, index: str, texto: str, campos: List[str] = None, size: int = 10) -> Dict:
        """
        Búsqueda simple de texto en campos específicos
        """
        try:
            if campos:
                query = {
                    "query": {
                        "multi_match": {
                            "query": texto,
                            "fields": campos,
                            "type": "best_fields"
                        }
                    }
                }
            else:
                query = {
                    "query": {
                        "query_string": {
                            "query": texto
                        }
                    }
                }

            # 👀 Antes se llamaba mal: self.buscar(index, query, size)
            return self.buscar(index=index, query=query, aggs=None, size=size)
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    # -------------------------------------------------
    # OTRAS UTILIDADES (las dejo casi igual)
    # -------------------------------------------------
    def ejecutar_comando(self, comando_json: str) -> Dict:
        """
        Ejecuta un comando JSON en ElasticSearch (crear/eliminar índice, etc.)
        """
        try:
            comando = json.loads(comando_json)
            operacion = comando.get('operacion')
            index = comando.get('index')

            if operacion == 'crear_index':
                mappings = comando.get('mappings', {})
                settings = comando.get('settings', {})
                response = self.client.indices.create(
                    index=index,
                    mappings=mappings,
                    settings=settings
                )
                return {'success': True, 'data': response}

            elif operacion == 'eliminar_index':
                response = self.client.indices.delete(index=index)
                return {'success': True, 'data': response}

            elif operacion == 'actualizar_mappings':
                mappings = comando.get('mappings', {})
                response = self.client.indices.put_mapping(
                    index=index,
                    body=mappings
                )
                return {'success': True, 'data': response}

            elif operacion == 'info_index':
                response = self.client.indices.get(index=index)
                return {'success': True, 'data': response}

            elif operacion == 'listar_indices':
                response = self.client.cat.indices(format='json')
                return {'success': True, 'data': response}

            else:
                return {'success': False, 'error': f'Operación no soportada: {operacion}'}

        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'JSON inválido: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def ejecutar_query(self, query_json: str) -> Dict:
        """
        Ejecuta una query en ElasticSearch (consulta arbitraria)
        """
        try:
            query = json.loads(query_json)
            index = query.pop('index', '_all')

            response = self.client.search(index=index, body=query)

            return {
                'success': True,
                'total': response['hits']['total']['value'],
                'hits': response['hits']['hits'],
                'aggs': response.get('aggregations', {})
            }
        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'JSON inválido: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def ejecutar_dml(self, comando_json: str) -> Dict:
        """
        Ejecuta un comando DML (index, update, delete, delete_by_query)
        """
        try:
            comando = json.loads(comando_json)
            operacion = comando.get('operacion')

            if operacion in ('index', 'create'):
                index = comando.get('index')
                documento = comando.get('documento', comando.get('body', {}))
                doc_id = comando.get('id')

                if doc_id:
                    response = self.client.index(index=index, id=doc_id, document=documento)
                else:
                    response = self.client.index(index=index, document=documento)

                return {'success': True, 'data': response}

            elif operacion == 'update':
                index = comando.get('index')
                doc_id = comando.get('id')
                doc = comando.get('doc', comando.get('documento', {}))
                response = self.client.update(index=index, id=doc_id, doc=doc)
                return {'success': True, 'data': response}

            elif operacion == 'delete':
                index = comando.get('index')
                doc_id = comando.get('id')
                response = self.client.delete(index=index, id=doc_id)
                return {'success': True, 'data': response}

            elif operacion == 'delete_by_query':
                index = comando.get('index')
                query = comando.get('query', {})
                response = self.client.delete_by_query(index=index, body={'query': query})
                return {'success': True, 'data': response}

            else:
                return {'success': False, 'error': f'Operación DML no soportada: {operacion}'}

        except json.JSONDecodeError as e:
            return {'success': False, 'error': f'JSON inválido: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def obtener_documento(self, index: str, doc_id: str) -> Optional[Dict]:
        """Obtiene un documento por su ID"""
        try:
            response = self.client.get(index=index, id=doc_id)
            return response['_source']
        except Exception as e:
            print(f"Error al obtener documento: {e}")
            return None

    def actualizar_documento(self, index: str, doc_id: str, datos: Dict) -> bool:
        """Actualiza un documento existente"""
        try:
            self.client.update(index=index, id=doc_id, doc=datos)
            return True
        except Exception as e:
            print(f"Error al actualizar documento: {e}")
            return False

    def eliminar_documento(self, index: str, doc_id: str) -> bool:
        """Elimina un documento"""
        try:
            self.client.delete(index=index, id=doc_id)
            return True
        except Exception as e:
            print(f"Error al eliminar documento: {e}")
            return False