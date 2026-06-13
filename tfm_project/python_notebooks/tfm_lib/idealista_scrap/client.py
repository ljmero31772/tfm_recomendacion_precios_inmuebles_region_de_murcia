"""
tfm_lib/idealista_scrap/client.py
Cliente Scrapfly configurado para el entorno Docker.
"""

import os
from scrapfly import ScrapflyClient
from loguru import logger as log

#Configuración para páginas de listados y municipios (requieren JS para cargar resultados)
BASE_CONFIG = {
    "asp": True,
    "country": "es,pt,fr,it,be,nl,de",
    "render_js": True,
    "proxy_pool": "public_residential_pool",
    # Sin sesión fija: una sesión compartida entre peticiones concurrentes provoca
    # ERR::SESSION::CONCURRENT_ACCESS. Las páginas de listados son stateless.
    "headers": {
        "referer": "https://www.idealista.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
}

#Configuración mínima para páginas de detalle de anuncios.
#El HTML estático incluye todos los datos necesarios: comentario, características,
#certificado energético y la variable adMultimediasInfo con las URLs de fotos.
#
#  · render_js=False : el HTML ya viene completo sin ejecución JS          → -10×
#  · asp=False       : las páginas de detalle no están detrás de Cloudflare → -10×
#  · sin proxy pool  : proxy datacenter estándar es suficiente              → -5×
#
#Si se detectan muchos errores 403, activar asp=True como fallback.
DETAIL_CONFIG = {
    "asp": True,
    "render_js": False,
    "proxy_pool": "public_datacenter_pool",
    "country": "es,pt,fr,it,be,nl,de",
    "headers": {
        "referer": "https://www.idealista.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
}

def get_scrapfly_client() -> ScrapflyClient:
    key = os.getenv("SCRAPFLY_KEY")
    if not key:
        log.error("Falta SCRAPFLY_KEY en las variables de entorno.")
        raise EnvironmentError("SCRAPFLY_KEY no configurada.")
    return ScrapflyClient(key=key)
