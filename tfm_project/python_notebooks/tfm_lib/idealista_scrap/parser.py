"""
tfm_lib/idealista_scrap/parser.py
Parsers de datos para Idealista.com.
"""

import re
import json
import math
from collections import defaultdict
from urllib.parse import urljoin
from typing import Dict, List, Optional
from scrapfly import ScrapeApiResponse
from loguru import logger as log


def _clean_price(price_str: Optional[str]) -> Optional[int]:
    if not price_str:
        return None
    clean = re.sub(r'[^\d]', '', price_str)
    return int(clean) if clean else None


def parse_province_urls(response: ScrapeApiResponse) -> List[Dict]:
    """
    Extrae las URLs y el número de ofertas de cada municipio.

    Estructura HTML esperada:
        <li class="em_1">
            <span>152</span>
            <a href="/venta-viviendas/abanilla-murcia/">Abanilla</a>
        </li>

    Returns:
        Lista de dicts con 'municipio_url' (str) y 'num_ofertas' (int).
    """
    try:
        selector = response.selector
    except Exception as e:
        if "parsel" in str(e).lower() or "scrapy" in str(e).lower():
            log.error("Falta la librería 'parsel'. Intenta ejecutar: !pip install parsel")
        raise e

    base_url = str(response.context["url"])
    municipios = []

    for li in selector.css("#location_list ul li"):
        href = li.css("a::attr(href)").get()
        num_str = li.css("span::text").get("").strip()
        if not href:
            continue
        try:
            num_ofertas = int(re.sub(r"[^\d]", "", num_str)) if num_str else 0
        except ValueError:
            num_ofertas = 0
            log.warning(f"No se pudo parsear num_ofertas '{num_str}' para {href}")

        municipios.append({
            "municipio_url": urljoin(base_url, href),
            "num_ofertas": num_ofertas,
        })

    log.info(f"Municipios encontrados: {len(municipios)}")
    return municipios


def parse_search_page_count(response: ScrapeApiResponse) -> int:
    """
    Calcula el número total de páginas de resultados de búsqueda.

    Estrategia con tres niveles de fallback:
      1. Extrae el total de inmuebles del <h1> y divide entre 30.
         Cubre todos los tipos (pisos, casas, chalets, apartamentos…).
      2. Si el h1 no tiene match, cuenta los enlaces de paginación
         visibles en la página (último número visible = nº páginas).
      3. Si ninguno funciona, devuelve 1 (solo hay una página).
    """
    selector = response.selector

    # --- Nivel 1: total de inmuebles desde el h1 ---
    # El span con id="h1-container__text" contiene el texto completo:
    # "2.184 pisos y apartamentos en Murcia", "1.369 casas y chalets en Murcia", etc.
    # Se extrae el texto directamente para evitar que las etiquetas HTML
    # interfieran con el regex (el numero va justo tras '>' en el HTML crudo).
    try:
        h1_text = (
            selector.css("span#h1-container__text::text").get("")
            or selector.css("h1#h1-container").xpath("string(.)").get("")
            or ""
        )
        # Buscar el primer numero en el texto (puede tener puntos/comas de millar)
        match = re.search(r"(\d[\d\.,]*)", h1_text)
        if match:
            total = int(re.sub(r"[^\d]", "", match.group(1)))
            pages = math.ceil(total / 30)
            log.debug(f"Paginas calculadas desde h1 ({total} inmuebles): {pages}")
            return pages
    except Exception as e:
        log.debug(f"Nivel 1 fallo al calcular paginas: {e}")

    # --- Nivel 2: último número de paginación visible ---
    try:
        page_numbers = selector.css(
            "nav.pagination a::text, "
            "ul.pagination a::text, "
            "div.pagination a::text"
        ).getall()
        numeric_pages = [int(p.strip()) for p in page_numbers if p.strip().isdigit()]
        if numeric_pages:
            pages = max(numeric_pages)
            log.debug(f"Páginas calculadas desde paginación visible: {pages}")
            return pages
    except Exception as e:
        log.debug(f"Nivel 2 falló al calcular páginas: {e}")

    log.warning("No se pudo calcular el número de páginas. Default: 1.")
    return 1


def parse_search_results(response: ScrapeApiResponse) -> Dict:
    selector = response.selector
    max_pages = parse_search_page_count(response)
    search_data = []

    for box in selector.xpath("//section[contains(@class, 'items-list')]/article[contains(@class, 'item')]"):
        if box.xpath(".//p[@class='adv_txt']"):
            continue
        
        price_raw = box.xpath(".//span[contains(@class, 'item-price')]/text()").get()
        company_url = box.xpath(".//picture[@class='logo-branding']/a/@href").get()
        desc = box.xpath(".//div[contains(@class, 'item-description')]/p/text()").get()

        search_data.append({
            "title": box.xpath(".//div/a/@title").get(),
            "link": "https://www.idealista.com" + box.xpath(".//div/a/@href").get(),
            "picture": box.xpath(".//img/@src").get(),
            "price": _clean_price(price_raw),
            "currency": box.xpath(".//span[contains(@class, 'item-price')]/span/text()").get(),
            "parking_included": bool(box.xpath(".//span[@class='item-parking']").get()),
            "details": box.xpath(".//div[contains(@class, 'item-detail-char')]/span/text()").getall(),
            "description": desc.replace('\n', '') if desc else "",
            "tags": box.xpath(".//div[@class='listing-tags-container']/span/text()").getall(),
            "listing_company": box.xpath(".//picture[@class='logo-branding']/a/@title").get(),
            "listing_company_url": "https://www.idealista.com" + company_url if company_url else None
        })
    return {"max_pages": max_pages, "search_data": search_data}


def parse_search_page(response: ScrapeApiResponse) -> List[str]:
    selector = response.selector
    urls = selector.css("article.item .item-link::attr(href)").getall()
    return [urljoin(str(response.context["url"]), url) for url in urls]


def _is_deactivated_ad(selector) -> bool:
    """
    Detecta si una página es un anuncio desactivado de Idealista.
    
    Estrategias de detección (en orden):
    1. Buscar el mensaje exacto "Lo sentimos, este anuncio ya no está publicado" en h1
    2. Buscar la clase CSS "deactivated-detail-without-suggestions" o "deactivated-detail"
    3. Buscar el mensaje parcial "anuncio ya no está" en cualquier h1
    
    Returns:
        True si es un anuncio desactivado, False en caso contrario.
    """
    # Estrategia 1: Mensaje exacto en h1
    h1_text = selector.css("h1 ::text").get("").strip().lower()
    if "no está publicado" in h1_text or "anuncio ya no está" in h1_text:
        return True
    
    # Estrategia 2: Clases CSS específicas de anuncio desactivado
    if selector.css(".deactivated-detail-without-suggestions").get() is not None:
        return True
    if selector.css(".deactivated-detail").get() is not None:
        return True
    
    # Estrategia 3: Buscar en todo el contenido del h1 (incluyendo HTML)
    h1_html = selector.css("h1").get("").lower()
    if "no está publicado" in h1_html or "anuncio ya no está" in h1_html:
        return True
    
    return False


def parse_property_detail(response: ScrapeApiResponse) -> Dict:
    """
    Extrae todos los campos relevantes de una página de detalle de propiedad de Idealista.

    Campos devueltos (capa RAW — sin parsear texto):
      · url, title, location, updated
      · price (int €), prev_price, has_price_drop (bool), price_drop_pct (int %)
      · price_per_m2 (int €/m²), gastos_comunidad_raw (str)
      · description  → comentario completo del anunciante (para silver)
      · num_fotos (int), photo_urls (List[str]), plan_urls (List[str])
      · caracteristicas_basicas (List[str])  → textos en bruto para silver
      · equipamiento (List[str])
      · cert_energetico (List[str])
    """
    selector = response.selector

    def _css(sel: str) -> str:
        return selector.css(sel).get("").strip()

    def _css_all(sel: str) -> List[str]:
        return selector.css(sel).getall()

    data: Dict = {}

    # ── Detección de anuncio desactivado (200 OK pero sin contenido real) ─────
    # "Lo sentimos, este anuncio ya no está publicado"
    is_deactivated = _is_deactivated_ad(selector)
    data["_is_deactivated"] = is_deactivated

    # ── Metadatos básicos ─────────────────────────────────────────────────────
    data["url"]      = str(response.context["url"])

    if is_deactivated:
        # En la vista desactivada, los datos están en un bloque simplificado
        # <p class="deactivated-detail_data">
        data["title"]    = _css(".deactivated-block-title::text")
        data["location"] = "" # No suele aparecer claro en esta vista
        data["updated"]  = _css(".deactivated-detail_date::text")
        data["price"]    = _clean_price(selector.css(".deactivated-detail_data > span:nth-child(2)::text").get())
        
        # Características en vista desactivada
        features = selector.css(".deactivated-detail_data .feature span::text").getall()
        # [ "112", "m²", "2", "hab." ]
        try:
            data["price_per_m2"] = None
            data["num_fotos"]    = 0
            data["caracteristicas_basicas"] = [f"{features[i]} {features[i+1]}" for i in range(0, len(features)-1, 2)]
        except Exception:
            data["caracteristicas_basicas"] = []

        data["description"] = ""
        data["photo_urls"]  = []
        data["plan_urls"]   = []
        data["equipamiento"] = []
        data["cert_energetico"] = []
        return data

    data["title"]    = _css("h1 .main-info__title-main::text")
    data["location"] = _css(".main-info__title-minor::text")
    data["updated"]  = _css(".time-since-last-modification::text")

    # ── Precio actual ─────────────────────────────────────────────────────────
    # <span class="info-data-price"><span class="txt-bold">146.000</span> €</span>
    data["price"] = _clean_price(_css(".info-data-price .txt-bold::text"))

    # ── Rebaja de precio ──────────────────────────────────────────────────────
    # <span class="pricedown_price">150.000 €</span>
    # <span class="pricedown_icon icon-pricedown">3%</span>
    prev_price_raw = _css(".pricedown_price::text")
    data["prev_price"]    = _clean_price(prev_price_raw)
    data["has_price_drop"] = bool(prev_price_raw)
    pct_text = _css(".pricedown_icon::text")          # e.g. "3%"
    if pct_text:
        m = re.search(r"\d+", pct_text)
        data["price_drop_pct"] = int(m.group()) if m else None
    else:
        data["price_drop_pct"] = None

    # ── Precio por m² ─────────────────────────────────────────────────────────
    # <p class="flex-feature squaredmeterprice">
    #   <span class="flex-feature-details">Precio por m²:</span>
    #   <span class="flex-feature-details">498 €/m²</span>
    # </p>
    m2_spans = selector.css(".squaredmeterprice .flex-feature-details::text").getall()
    data["price_per_m2"] = _clean_price(m2_spans[1]) if len(m2_spans) >= 2 else None

    # ── Gastos de comunidad ───────────────────────────────────────────────────
    gastos = None
    for p in selector.css(".flex-feature"):
        text = " ".join(p.css("::text").getall()).strip()
        if "comunidad" in text.lower():
            gastos = text
            break
    data["gastos_comunidad_raw"] = gastos

    # ── Comentario del anunciante ─────────────────────────────────────────────
    # <div class="comment"><div class="adCommentsLanguage"><p>...</p></div></div>
    comment_parts = _css_all("div.comment .adCommentsLanguage p ::text")
    data["description"] = " ".join(comment_parts).strip()

    # ── Fotos ─────────────────────────────────────────────────────────────────
    # Número de fotos desde el botón multimedia
    # <button aria-label="39 fotos" data-button-type="pics">
    fotos_label = _css("button[data-button-type='pics']::attr(aria-label)")
    try:
        data["num_fotos"] = int(re.search(r"\d+", fotos_label).group()) if fotos_label else 0
    except Exception:
        data["num_fotos"] = 0

    # URLs de fotos — estrategia 1: variable JS fullScreenGalleryPics (Scrapfly full render)
    photo_urls: List[str] = []
    plan_urls:  List[str] = []
    try:
        raw_content = response.scrape_result["content"]
        img_data_str = re.findall(
            r"fullScreenGalleryPics\s*:\s*(\[.+?\]),", raw_content, re.DOTALL
        )[0]
        images = json.loads(re.sub(r"(\w+?):([^/])", r'"\1":\2', img_data_str))
        for img in images:
            img_url = urljoin(data["url"], img["imageUrl"])
            (plan_urls if img.get("isPlan") else photo_urls).append(img_url)
        log.debug(f"JS gallery: {len(photo_urls)} fotos, {len(plan_urls)} planos")
    except Exception:
        # Estrategia 2: imágenes visibles en el HTML (primera foto + webp sources)
        for src_el in selector.css(".main-image_first source[type='image/jpeg']::attr(srcset)").getall():
            if src_el and src_el not in photo_urls:
                photo_urls.append(src_el)
        # Galería de dispositivo (carousel-gallery)
        for img_el in selector.css(".carousel-gallery picture source[type='image/jpeg']::attr(srcset)").getall():
            if img_el and img_el not in photo_urls:
                photo_urls.append(img_el)
        log.debug(f"HTML fallback: {len(photo_urls)} fotos encontradas en img src")

    data["photo_urls"] = photo_urls
    data["plan_urls"]  = plan_urls
    # Si el JS no devolvió nada pero el botón sí indica fotos, al menos guardamos el count
    if not data["num_fotos"] and photo_urls:
        data["num_fotos"] = len(photo_urls)

    # ── Características básicas, Equipamiento y Certificado energético ────────
    # Estructura HTML:
    #   <h2 class="details-property-h2">Características básicas</h2>
    #   <div class="details-property_features"><ul><li>Chalet adosado</li>...</ul></div>
    #   <h2 class="details-property-h2">Equipamiento</h2>   (en el mismo bloque)
    #   <h2 class="details-property-h2">Certificado energético</h2>
    features_raw: Dict[str, List[str]] = {}
    for h2 in selector.css(".details-property-h2"):
        label = h2.xpath("text()").get("").strip()
        items = h2.xpath("following-sibling::div[1]//li")
        features_raw[label] = [
            "".join(item.xpath(".//text()").getall()).strip()
            for item in items
            if "".join(item.xpath(".//text()").getall()).strip()
        ]

    data["caracteristicas_basicas"] = features_raw.get("Características básicas", [])
    data["equipamiento"]            = features_raw.get("Equipamiento", [])
    data["cert_energetico"]         = features_raw.get("Certificado energético", [])

    log.debug(
        f"Detalle parseado → precio={data['price']} € | fotos={data['num_fotos']} | "
        f"caract={len(data['caracteristicas_basicas'])} items"
    )
    return data
