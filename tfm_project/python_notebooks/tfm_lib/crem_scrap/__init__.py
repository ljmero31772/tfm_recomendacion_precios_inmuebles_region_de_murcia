import re
from scrapfly import ScrapeConfig


from ..idealista_scrap.client import get_scrapfly_client, BASE_CONFIG
from ..utils import normalize_column_name

def get_html(url: str) -> str:
    """Obtiene el HTML de *url* usando Scrapfly (síncrono, una sola petición).

    Parameters
    ----------
    url : str
        La URL a escrapear.

    Returns
    -------
    str
        El contenido HTML de la página.
    """
    client = get_scrapfly_client()
    result = client.scrape(ScrapeConfig(url, **BASE_CONFIG))
    return result.content



def normalize_name(name: str) -> str:
    """
    Normaliza un nombre:
    - Pasa a minúsculas
    - Elimina acentos
    - Reemplaza espacios y símbolos por guiones bajos
    - Elimina cualquier carácter no alfanumérico (excepto _)
    """
    return normalize_column_name(name)



def clean_and_decode(s):
    """
    Limpia espacios en blanco, saltos de línea y decodifica secuencias
    de bytes UTF-8 que hayan sido escritas de forma literal (ej. \xc3\xb3 -> ó).
    """
    s = re.sub(r'\s+', ' ', s).strip()
    try:
        s = s.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
    except Exception:
        pass
    return s
