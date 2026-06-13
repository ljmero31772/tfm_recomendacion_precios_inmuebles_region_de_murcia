"""
tfm_lib/idealista_scrap/__init__.py
Módulo dedicado exclusivamente al scrapeo de Idealista.
"""

from .client import get_scrapfly_client, BASE_CONFIG, DETAIL_CONFIG
from .scraper import (
    scrape_provinces,
    scrape_search_listings,
    scrape_property_detail,
    crawl_search_pages,
    run_async,
)
from .parser import (
    parse_province_urls,
    parse_search_page,
    parse_search_results,
    parse_property_detail,
)

__all__ = [
    "get_scrapfly_client",
    "BASE_CONFIG",
    "scrape_provinces",
    "scrape_search_listings",
    "scrape_property_detail",
    "crawl_search_pages",
    "run_async",
    "parse_province_urls",
    "parse_search_page",
    "parse_search_results",
    "parse_property_detail",
]
