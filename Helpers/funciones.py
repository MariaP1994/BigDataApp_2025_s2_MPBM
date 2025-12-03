import os
import zipfile
import requests
import json
from typing import Dict, List
from datetime import datetime
import shutil


class Funciones:
    # ==========================================================
    # CARPETAS / ARCHIVOS
    # ==========================================================
    @staticmethod
    def crear_carpeta(ruta: str) -> bool:
        """Crea una carpeta si no existe"""
        try:
            if not os.path.exists(ruta):
                os.makedirs(ruta)
            return True
        except Exception as e:
            print(f"Error al crear carpeta: {e}")
            return False

    @staticmethod
    def borrar_contenido_carpeta(ruta: str) -> bool:
        """
        Borra el contenido de una carpeta sin eliminar la carpeta misma
        """
        try:
            if not os.path.exists(ruta):
                return True  # Si no existe, no hay nada que borrar

            if not os.path.isdir(ruta):
                return False  # No es una carpeta

            for item in os.listdir(ruta):
                item_path = os.path.join(ruta, item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as e:
                    print(f"Error al eliminar {item_path}: {e}")
                    return False

            return True
        except Exception as e:
            print(f"Error al borrar contenido de carpeta: {e}")
            return False

    # ==========================================================
    # ZIP
    # ==========================================================
    @staticmethod
    def descomprimir_zip_local(ruta_file_zip: str, ruta_descomprimir: str) -> List[Dict]:
        """Descomprime un archivo ZIP y retorna info de archivos"""
        archivos = []
        try:
            with zipfile.ZipFile(ruta_file_zip, 'r') as zip_ref:
                for file_info in zip_ref.namelist():
                    if not file_info.endswith('/'):
                        carpeta = os.path.dirname(file_info)
                        nombre_archivo = os.path.basename(file_info)
                        extension = os.path.splitext(nombre_archivo)[1].lower()

                        # Solo procesar txt, pdf y json
                        if extension in ['.txt', '.pdf', '.json']:
                            zip_ref.extract(file_info, ruta_descomprimir)
                            archivos.append({
                                'carpeta': carpeta if carpeta else 'raiz',
                                'nombre': nombre_archivo,
                                'ruta': os.path.join(ruta_descomprimir, file_info),
                                'extension': extension
                            })
            return archivos
        except Exception as e:
            print(f"Error al descomprimir ZIP: {e}")
            return []

    @staticmethod
    def descargar_y_descomprimir_zip(url: str, carpeta_destino: str,
                                     tipoArchivo: str = '') -> List[Dict]:
        """Descarga y descomprime un ZIP desde URL"""
        try:
            Funciones.crear_carpeta(carpeta_destino)

            response = requests.get(url, stream=True)
            zip_path = os.path.join(carpeta_destino, 'temp.zip')

            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            archivos = Funciones.descomprimir_zip_local(zip_path, carpeta_destino)
            os.remove(zip_path)

            return archivos
        except Exception as e:
            print(f"Error al descargar y descomprimir: {e}")
            return []

    @staticmethod
    def allowed_file(filename: str, extensions: List[str]) -> bool:
        """Verifica si un archivo tiene extensión permitida"""
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

    # ==========================================================
    # PDF → TEXTO
    # ==========================================================
    @staticmethod
    def extraer_texto_pdf(ruta_pdf: str) -> str:
        """
        Extrae texto de un archivo PDF (no escaneado)
        """
        try:
            import PyPDF2  # import lazy

            texto = ""
            with open(ruta_pdf, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    contenido = page.extract_text() or ""
                    texto += contenido + "\n"
            return texto.strip()
        except Exception as e:
            print(f"Error al extraer texto del PDF {ruta_pdf}: {e}")
            return ""

    @staticmethod
    def extraer_texto_pdf_ocr(ruta_pdf: str) -> str:
        """
        Extrae texto de un PDF usando OCR (útil para PDFs escaneados)
        """
        try:
            from pdf2image import convert_from_path   # lazy import
            import pytesseract                        # lazy import

            images = convert_from_path(ruta_pdf)

            texto = ""
            for image in images:
                texto += pytesseract.image_to_string(image, lang='spa') + "\n"

            return texto.strip()
        except Exception as e:
            print(f"Error al extraer texto con OCR del PDF {ruta_pdf}: {e}")
            return ""

    # ==========================================================
    # JSON – UTILIDADES GENERALES
    # ==========================================================
    @staticmethod
    def listar_archivos_json(ruta_carpeta: str) -> List[Dict]:
        """
        Lista todos los archivos JSON en una carpeta
        """
        archivos_json = []
        try:
            if not os.path.exists(ruta_carpeta):
                return []

            for archivo in os.listdir(ruta_carpeta):
                if archivo.lower().endswith('.json'):
                    ruta_completa = os.path.join(ruta_carpeta, archivo)
                    archivos_json.append({
                        'nombre': archivo,
                        'ruta': ruta_completa,
                        'tamaño': os.path.getsize(ruta_completa)
                    })

            return archivos_json
        except Exception as e:
            print(f"Error al listar archivos JSON: {e}")
            return []

    @staticmethod
    def leer_json(ruta_json: str) -> Dict:
        """Lee un archivo JSON y retorna su contenido"""
        try:
            with open(ruta_json, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error al leer JSON {ruta_json}: {e}")
            return {}

    @staticmethod
    def guardar_json(ruta_json: str, datos: Dict) -> bool:
        """Guarda datos en un archivo JSON"""
        try:
            directorio = os.path.dirname(ruta_json)
            if directorio:
                Funciones.crear_carpeta(directorio)

            with open(ruta_json, 'w', encoding='utf-8') as f:
                json.dump(datos, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error al guardar JSON: {e}")
            return False

    # ==========================================================
    # LISTAR ARCHIVOS GENERALES
    # ==========================================================
    @staticmethod
    def listar_archivos_carpeta(ruta_carpeta: str,
                                extensiones: List[str] = None) -> List[Dict]:
        """
        Lista archivos en una carpeta con extensiones específicas
        """
        archivos = []
        try:
            if not os.path.exists(ruta_carpeta):
                return []

            for archivo in os.listdir(ruta_carpeta):
                ruta_completa = os.path.join(ruta_carpeta, archivo)
                if os.path.isfile(ruta_completa):
                    extension = os.path.splitext(archivo)[1].lower().replace('.', '')

                    if extensiones is None or extension in extensiones:
                        archivos.append({
                            'nombre': archivo,
                            'ruta': ruta_completa,
                            'extension': extension,
                            'tamaño': os.path.getsize(ruta_completa)
                        })

            return archivos
        except Exception as e:
            print(f"Error al listar archivos: {e}")
            return []

    # ==========================================================
    # FUNCIONES ESPECÍFICAS PARA TUS BOLETINES (JSON)
    # ==========================================================
    @staticmethod
    def cargar_boletines_desde_carpeta(ruta_carpeta: str) -> List[Dict]:
        """
        Carga TODOS los boletines a partir de los .json en una carpeta.
        
        Pensado para tus JSON del BES, por ejemplo con estructura:
        {
          "anio": 2025,
          "semana": 47,
          "fecha_inicio": "2025-11-16",
          "fecha_fin": "2025-11-22",
          "tema_central": "...",
          "expertos_tematicos": ["nombre1", "nombre2"],
          "archivo_pdf": "BES_2025_47.pdf"
        }
        o bien:
        { "boletines": [ { ... }, { ... } ] }

        Devuelve una lista de documentos listos para indexar en Elastic.
        """
        boletines: List[Dict] = []

        archivos_json = Funciones.listar_archivos_json(ruta_carpeta)
        if not archivos_json:
            print(f"No se encontraron JSON en {ruta_carpeta}")
            return []

        for info_archivo in archivos_json:
            ruta = info_archivo["ruta"]
            data = Funciones.leer_json(ruta)
            if not data:
                continue

            # Si el JSON trae una lista "boletines"
            if isinstance(data, dict) and "boletines" in data and isinstance(data["boletines"], list):
                for b in data["boletines"]:
                    doc = Funciones._normalizar_boletin(b, ruta)
                    boletines.append(doc)
            else:
                # Asumimos que el JSON ya es un solo boletín
                doc = Funciones._normalizar_boletin(data, ruta)
                boletines.append(doc)

        print(f"Boletines cargados desde JSON: {len(boletines)}")
        return boletines

    @staticmethod
    def _normalizar_boletin(b: Dict, ruta_json: str) -> Dict:
        """
        Normaliza la estructura de un boletín para que tenga siempre
        los mismos campos antes de indexar en Elastic.
        """
        anio = b.get("anio")
        semana = b.get("semana") or b.get("semana_epidemiologica")

        # ID estándar: 2025-47 -> "2025-S47"
        if anio and semana:
            id_boletin = f"{anio}-S{int(semana):02d}"
        else:
            # fallback: nombre del json
            id_boletin = os.path.splitext(os.path.basename(ruta_json))[0]

        doc = {
            "id_boletin": id_boletin,
            "anio": anio,
            "semana_epidemiologica": semana,
            "tema_central": b.get("tema_central"),
            "fecha_inicio": b.get("fecha_inicio"),
            "fecha_fin": b.get("fecha_fin"),
            "expertos_tematicos": b.get("expertos_tematicos", []),
            "archivo_pdf": b.get("archivo_pdf"),
            "fuente_json": os.path.basename(ruta_json)
        }

        return doc