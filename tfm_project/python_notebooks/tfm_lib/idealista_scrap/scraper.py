"""
tfm_lib/idealista_scrap/scraper.py
Orquestación de scraping asíncrono para Idealista.
"""

import asyncio
import json
import math
import os
import random
from typing import List, Dict, Optional
from scrapfly import ScrapeConfig, ScrapeApiResponse
from scrapfly.errors import UpstreamHttpClientError, ScrapflyScrapeError
from loguru import logger as log

from .client import get_scrapfly_client, BASE_CONFIG, DETAIL_CONFIG
from .parser import (
    parse_province_urls,
    parse_search_page,
    parse_search_results,
    parse_property_detail,
    _is_deactivated_ad,
)
from parsel import Selector


def _check_deactivated_from_content(content: str) -> bool:
    """
    Detecta si un contenido HTML crudo corresponde a un anuncio desactivado.
    Se usa cuando Scrapfly devuelve un error pero el HTML está disponible.
    
    Busca indicadores en el HTML:
    - Clase "deactivated-detail-without-suggestions" o "deactivated-detail"
    - Mensaje "Lo sentimos, este anuncio ya no está publicado"
    """
    if not content:
        return False
    
    content_lower = content.lower()
    
    # Buscar clases CSS de anuncio desactivado
    if 'deactivated-detail-without-suggestions' in content_lower:
        return True
    if 'deactivated-detail' in content_lower:
        return True
    
    if "lo sentimos, este anuncio ya no est" in content_lower and " publicado" in content_lower:
        return True

    return False


# Concurrencia para Scrapfly.
# REDUCIDO para evitar throttling: el error indica >= 25 requests fallidos/min
CONCURRENCY_LIMIT = 15
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

# Sin reintentos propios: los anuncios desactivados no deben reintentarse
# Scrapfly ya hace 3 reintentos internos para errores retryables
MAX_RETRIES = 0      # Desactivado: no reintentar errores de anuncios desactivados
RETRY_BACKOFF = 0    # No aplica

# Pausa global entre peticiones para evitar throttling (segundos)
GLOBAL_DELAY = 0.1 # 2 segundos entre cada petición individual

# Pool de países para rotar en cada reintento propio
COUNTRY_POOL = ["es", "pt", "fr", "it", "be", "nl", "de"]

# Split por tipo de propiedad para municipios grandes
PAGE_SPLIT_THRESHOLD = 72       # páginas: si se supera, se divide en pisos + chalets
SUBTYPE_MAX_PAGES = 65          # máximo de páginas por subtype (evita errores en las últimas)
PROPERTY_FILTERS = ["con-pisos", "con-chalets"]
DEFAULT_CHECKPOINT_DIR = "/tmp/idealista_splits"  # directorio JSONL de checkpoint


async def _scrape_with_retry(client, url: str, config_kwargs: dict) -> "ScrapeApiResponse | None":
    """
    Reintenta una petición fallida hasta MAX_RETRIES veces con backoff y rotación de país.

    - 404: descarta inmediatamente (la página no existe, no tiene sentido reintentar).
    - Otros errores o excepciones: reintenta forzando un país diferente al anterior.
    ScrapFly ya hace 3 reintentos internos; aquí añadimos 2 rondas extra con geo distinta.
    """
    used_country: Optional[str] = config_kwargs.get("country", None)

    for attempt in range(1, MAX_RETRIES + 1):
        # Elegir un país diferente al último usado
        available = [c for c in COUNTRY_POOL if c != used_country]
        used_country = random.choice(available)
        kwargs = {**config_kwargs, "country": used_country}

        try:
            async with semaphore:
                response = await client.async_scrape(ScrapeConfig(url, **kwargs))
        except UpstreamHttpClientError as exc:
            log.warning(
                f"[Intento {attempt}/{MAX_RETRIES}] Excepción ScrapFly en {url}: {exc} "
                f"— reintentando en {RETRY_BACKOFF * attempt}s con país={used_country}..."
            )
            await asyncio.sleep(RETRY_BACKOFF * attempt)
            continue

        code = response.upstream_status_code

        if code == 404:
            log.warning(f"Página no encontrada (404) en {url} — descartando sin más reintentos.")
            return None  # No existe, no reintentar

        if code and code < 400:
            return response  # Éxito

        log.warning(
            f"[Intento {attempt}/{MAX_RETRIES}] Error {code} en {url} "
            f"— reintentando en {RETRY_BACKOFF * attempt}s con país={used_country}..."
        )
        await asyncio.sleep(RETRY_BACKOFF * attempt)

    log.error(f"Página definitivamente fallida tras {MAX_RETRIES} reintentos propios: {url}")
    return None


# ---------------------------------------------------------------------------
# Helpers de checkpoint JSONL
# ---------------------------------------------------------------------------

def _save_checkpoint_jsonl(listings: List[Dict], path: str) -> None:
    """Persiste una lista de ofertas como fichero JSONL (una oferta por línea)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for item in listings:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    log.info(f"Checkpoint guardado: {len(listings)} ofertas → {path}")


def _load_checkpoint_jsonl(path: str) -> List[Dict]:
    """Carga un checkpoint JSONL. Devuelve lista vacía si el fichero no existe."""
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _build_filtered_url(base_url: str, filter_name: str) -> str:
    """Añade un filtro de tipo de propiedad a la URL base del municipio.

    Ejemplo::

        _build_filtered_url(
            "https://www.idealista.com/venta-viviendas/murcia-murcia/",
            "con-pisos"
        )
        # → "https://www.idealista.com/venta-viviendas/murcia-murcia/con-pisos/"
    """
    return base_url.rstrip("/") + f"/{filter_name}/"


async def _scrape_by_property_type(
    client,
    base_url: str,
    debug: bool,
    checkpoint_dir: str,
    reuse_checkpoint: bool = False,
) -> List[Dict]:
    """
    Divide el scraping en pisos y casas/chalets para municipios que superan
    PAGE_SPLIT_THRESHOLD páginas.

    - Cada subtype se scrapea de forma independiente (hasta SUBTYPE_MAX_PAGES páginas).
    - Los resultados de cada subtype se persisten en un fichero JSONL como checkpoint.
    - ``reuse_checkpoint=True``: si el JSONL ya existe, se carga sin re-scrapear.
    - ``reuse_checkpoint=False`` (default): siempre re-scrapea y sobreescribe el checkpoint.
    """
    slug = next(
        (p for p in reversed(base_url.rstrip("/").split("/")) if p),
        "municipio"
    )

    all_listings: List[Dict] = []

    for filter_name in PROPERTY_FILTERS:
        checkpoint_path = os.path.join(checkpoint_dir, f"{slug}_{filter_name}.jsonl")

        if reuse_checkpoint:
            existing = _load_checkpoint_jsonl(checkpoint_path)
            if existing:
                log.info(
                    f"Checkpoint reutilizado para {slug}/{filter_name}: "
                    f"{len(existing)} ofertas."
                )
                all_listings.extend(existing)
                continue

        filtered_url = _build_filtered_url(base_url, filter_name)
        log.info(f"Scrapeando subtype '{filter_name}' para {slug}: {filtered_url}")

        listings = await scrape_search_listings(
            filtered_url,
            max_pages=SUBTYPE_MAX_PAGES,
            debug=debug,
            checkpoint_dir=None,  # Evitar anidamiento de splits
        )

        _save_checkpoint_jsonl(listings, checkpoint_path)
        all_listings.extend(listings)
        log.info(f"Subtype '{filter_name}' completado: {len(listings)} ofertas")

    log.info(f"Split completado para {slug}: {len(all_listings)} ofertas totales")
    return all_listings


async def scrape_provinces(province_urls: List[str], debug: bool = False) -> List[Dict]:
    """Extrae las URLs y número de ofertas de todos los municipios de las provincias indicadas."""
    if debug:
        log.info("[DEBUG MODE] Limitando a 1 provincia y usando entorno TEST")
        province_urls = province_urls[:1]
    
    client = get_scrapfly_client()
    municipios = []
    
    for url in province_urls:
        async with semaphore:
            log.info(f"Scrapeando provincia: {url}")
            response = await client.async_scrape(ScrapeConfig(url, **BASE_CONFIG, debug=debug))
            log.debug(f"Response status: {response.upstream_status_code}")
            urls = parse_province_urls(response)
            municipios.extend(urls)
    
    return municipios[:1] if debug else municipios


async def scrape_search_listings(
    search_url: str,
    max_pages: Optional[int] = None,
    known_total_pages: Optional[int] = None,
    debug: bool = False,
    checkpoint_dir: Optional[str] = DEFAULT_CHECKPOINT_DIR,
    reuse_checkpoint: bool = False,
) -> List[Dict]:
    """
    Scrapea todos los listados de búsqueda de una URL (municipio), incluyendo paginación.

    Para municipios con más de ``PAGE_SPLIT_THRESHOLD`` páginas, divide automáticamente
    el trabajo en dos subtypes (pisos y casas/chalets) y persiste cada uno como checkpoint
    JSONL en ``checkpoint_dir`` antes de combinar el resultado final.

    Args:
        search_url:         URL de búsqueda del municipio (debe terminar en '/' o '.htm').
        max_pages:          Límite opcional de páginas a scrapear (usado en debug o subtypes).
        known_total_pages:  Total de páginas ya calculado (campo ``total_paginas`` de la
                            tabla de municipios). Si se proporciona, omite la detección HTML.
        debug:              Si True, limita a 1 página y usa entorno TEST de Scrapfly.
        checkpoint_dir:     Directorio donde guardar los checkpoints JSONL por subtype.
                            Si es None, no se guardan checkpoints (evita anidamiento).
        reuse_checkpoint:   Si True, carga el JSONL de checkpoint si ya existe y no re-scrapea.
                            Si False (default), siempre scrapea de nuevo.
    """
    if debug:
        log.info(f"[DEBUG MODE] Limitando a 1 página para: {search_url}")
        max_pages = 1

    client = get_scrapfly_client()
    async with semaphore:
        log.info(f"Scrapeando primera página de: {search_url}")
        first_response = await client.async_scrape(ScrapeConfig(search_url, **BASE_CONFIG, debug=debug))
        data = parse_search_results(first_response)

    all_listings = data["search_data"]

    # --- Resolución del número total de páginas (prioridad: tabla > detección HTML) ---
    if known_total_pages:
        total_pages = known_total_pages
        log.info(f"Páginas obtenidas desde tabla de municipios para {search_url}: {total_pages}")
    else:
        total_pages = data["max_pages"]
        log.info(f"Páginas detectadas desde HTML para {search_url}: {total_pages}")

    if max_pages:
        total_pages = min(total_pages, max_pages)
        log.info(f"Limitando a {total_pages} páginas por parámetro max_pages={max_pages}")

    # --- Split por tipo de propiedad si el municipio es demasiado grande ---
    is_filtered = any(f in search_url for f in PROPERTY_FILTERS)
    if total_pages > PAGE_SPLIT_THRESHOLD and not is_filtered and not debug:
        log.info(
            f"Municipio con {total_pages} páginas > {PAGE_SPLIT_THRESHOLD} — "
            f"dividiendo por tipo de propiedad (pisos / casas-chalets)"
        )
        effective_checkpoint_dir = checkpoint_dir or DEFAULT_CHECKPOINT_DIR
        return await _scrape_by_property_type(
            client, search_url, debug, effective_checkpoint_dir,
            reuse_checkpoint=reuse_checkpoint,
        )

    if total_pages > 1:
        log.info(f"Scrapeando {total_pages - 1} páginas adicionales para {search_url}")
        remaining = [
            ScrapeConfig(f"{search_url}pagina-{i}.htm", **BASE_CONFIG, debug=debug)
            for i in range(2, total_pages + 1)
        ]
        async for response in client.concurrent_scrape(remaining, concurrency=CONCURRENCY_LIMIT):
            if not response.upstream_status_code or response.upstream_status_code >= 400:
                failed_url = response.config["url"]
                log.warning(
                    f"Página fallida ({response.upstream_status_code}) para "
                    f"{failed_url} — iniciando reintentos propios..."
                )
                response = await _scrape_with_retry(
                    client, failed_url, {**BASE_CONFIG, "debug": debug}
                )
                if response is None:
                    continue
            page_data = parse_search_results(response)
            all_listings.extend(page_data["search_data"])
            log.debug(f"Página scrapeada: {len(page_data['search_data'])} ofertas | Total acumulado: {len(all_listings)}")

    log.info(f"Total ofertas recuperadas para {search_url}: {len(all_listings)}")
    return all_listings[:1] if debug else all_listings


async def crawl_search_pages(search_url: str, max_pages: Optional[int] = None, debug: bool = False) -> List[str]:
    if debug:
        log.info("[DEBUG MODE] Limitando a 1 página y usando entorno TEST")
        max_pages = 1
    
    client = get_scrapfly_client()
    first_response = await client.async_scrape(ScrapeConfig(search_url, **BASE_CONFIG, debug=debug))
    property_urls = parse_search_page(first_response)
    result = parse_search_results(first_response)
    total_pages = result["max_pages"]
    
    if max_pages and max_pages < total_pages: total_pages = max_pages
    if total_pages > 1:
        remaining = [ScrapeConfig(search_url + f"pagina-{page}.htm", **BASE_CONFIG, debug=debug) for page in range(2, total_pages + 1)]
        async for response in client.concurrent_scrape(remaining):
            property_urls.extend(parse_search_page(response))
    
    return property_urls[:1] if debug else property_urls

async def scrape_property_detail(
    property_urls: List[str],
    debug: bool = False,
) -> List[Dict]:
    """
    Scrapea el detalle de cada propiedad. Siempre devuelve un registro por URL:
      · _scraped_status = 'ok'           → parseo exitoso
      · _scraped_status = 'no_encontrado' → 404 (puede ser falso positivo de Scrapfly;
                                             se reintentará en ejecuciones futuras)
      · _scraped_status = 'error'         → otro error HTTP o excepción
    Nunca descarta URLs silenciosamente.
    
    Incluye pausas entre peticiones para evitar throttling (GLOBAL_DELAY).
    """
    # El NB3 ya limita las URLs a procesar en modo debug (1 por municipio).
    # Aquí debug solo activa el modo TEST de Scrapfly (sin coste de tokens).
    client = get_scrapfly_client()
    
    async def scrape_one(index: int, url: str) -> Dict:
        # Pequeño retraso progresivo inicial para no lanzar todos los workers al mismo milisegundo exacto
        if index > 0:
            await asyncio.sleep(index * GLOBAL_DELAY)
            
        async with semaphore:
            config = ScrapeConfig(url, **DETAIL_CONFIG, debug=debug, raise_on_upstream_error=False)
            try:
                response = await client.async_scrape(config)
                code = response.upstream_status_code
                
                # Verificar si hay header Retry-After y respetarlo (pausa este worker)
                retry_after = None
                try:
                    if hasattr(response, 'scrape_result') and response.scrape_result:
                        headers = response.scrape_result.get('headers', {})
                        for key, value in headers.items():
                            if key.lower() == 'retry-after':
                                retry_after = value
                                break
                except Exception:
                    pass
                
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                        log.warning(f"Retry-After header detectado: esperando {wait_time}s en worker {index}")
                        await asyncio.sleep(wait_time)
                    except (ValueError, TypeError):
                        pass
                
                # Verificar si es un anuncio desactivado
                try:
                    if hasattr(response, 'scrape_result') and response.scrape_result:
                        content = response.scrape_result.get('content', '')
                        if content and _check_deactivated_from_content(content):
                            log.info(f"Anuncio desactivado detectado en {url}")
                            return {"url": url, "_scraped_status": "no_encontrado"}
                except Exception:
                    pass
                
                if code == 200:
                    try:
                        data = parse_property_detail(response)
                        if data.get("_is_deactivated"):
                            data["_scraped_status"] = "no_encontrado"
                        else:
                            data["_scraped_status"] = "ok"
                        return data
                    except Exception as exc:
                        log.warning(f"Error parseando {url}: {exc}")
                        return {"url": url, "_scraped_status": "error", "_error_msg": str(exc)}
                
                elif code == 404:
                    log.warning(f"404 en {url} — guardando como 'no_encontrado'")
                    return {"url": url, "_scraped_status": "no_encontrado"}
                
                else:
                    msg = f"HTTP {code}" if code else "sin respuesta"
                    log.warning(f"Error en {url}: {msg}")
                    return {"url": url, "_scraped_status": "error", "_error_msg": msg}
                    
            except Exception as exc:
                log.error(f"Excepción en {url}: {exc}")
                return {"url": url, "_scraped_status": "error", "_error_msg": str(exc)}

    # Lanzamos todos los workers en paralelo. El semaphore limitará la ejecución a CONCURRENCY_LIMIT.
    tasks = [scrape_one(i, url) for i, url in enumerate(property_urls)]
    results = await asyncio.gather(*tasks)
    
    return [r for r in results if r is not None]




def run_async(coro):
    """
    Ejecuta una corrutina en un entorno síncrono (como Jupyter o scripts).
    Maneja el bucle de eventos existente usando nest_asyncio si es necesario.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return asyncio.run(coro)

    if loop.is_running():
        try:
            import nest_asyncio
            nest_asyncio.apply()
        except ImportError:
            log.warning("nest_asyncio no está instalado. Podría haber problemas en Jupyter.")
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)
    
