from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from typing import Dict, List, Optional


class MongoDB:
    """
    Clase de ayuda para conectarse a MongoDB y manejar usuarios.
    Debe usarse como: mongo = MongoDB(uri, db_name)
    """

    def _init_(self, uri: str, db_name: str):
        """Inicializa conexión a MongoDB"""
        if not uri:
            raise ValueError("La URI de MongoDB (MONGO_URI) está vacía")

        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def test_connection(self) -> bool:
        """Prueba la conexión a MongoDB"""
        try:
            self.client.admin.command("ping")
            return True
        except ConnectionFailure:
            return False

    # ========== OPERACIONES DE USUARIO ==========

    def validar_usuario(self, usuario: str, password: str, coleccion: str) -> Optional[Dict]:
        """Valida usuario y contraseña (texto plano, SIN MD5 para este proyecto)"""
        try:
            user = self.db[coleccion].find_one({
                "usuario": usuario,
                "password": password   # OJO: debe coincidir con lo que guardaste en Atlas
            })
            return user
        except Exception as e:
            print(f"Error al validar usuario: {e}")
            return None

    def obtener_usuario(self, usuario: str, coleccion: str) -> Optional[Dict]:
        """Obtiene información de un usuario"""
        try:
            return self.db[coleccion].find_one({"usuario": usuario})
        except Exception as e:
            print(f"Error al obtener usuario: {e}")
            return None

    def listar_usuarios(self, coleccion: str) -> List[Dict]:
        """Lista todos los usuarios"""
        try:
            return list(self.db[coleccion].find({}))
        except Exception as e:
            print(f"Error al listar usuarios: {e}")
            return []

    def crear_usuario(self, usuario: str, password: str, permisos: Dict, coleccion: str) -> bool:
        """Crea un nuevo usuario"""
        try:
            documento = {
                "usuario": usuario,
                "password": password,   # sin MD5 para que coincida con validar_usuario
                "permisos": permisos
            }
            self.db[coleccion].insert_one(documento)
            return True
        except Exception as e:
            print(f"Error al crear usuario: {e}")
            return False

    def actualizar_usuario(self, usuario: str, nuevos_datos: Dict, coleccion: str) -> bool:
        """Actualiza un usuario existente"""
        try:
            self.db[coleccion].update_one(
                {"usuario": usuario},
                {"$set": nuevos_datos}
            )
            return True
        except Exception as e:
            print(f"Error al actualizar usuario: {e}")
            return False

    def eliminar_usuario(self, usuario: str, coleccion: str) -> bool:
        """Elimina un usuario"""
        try:
            resultado = self.db[coleccion].delete_one({"usuario": usuario})
            return resultado.deleted_count > 0
        except Exception as e:
            print(f"Error al eliminar usuario: {e}")
            return False

    def close(self):
        """Cierra la conexión"""
        self.client.close()