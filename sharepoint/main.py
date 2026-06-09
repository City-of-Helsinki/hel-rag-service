"""
Entry point orchestration script for the SharePoint integration PoC.
Loads credentials from .env, site configs from sites_config.yaml, crawls all 
pages and files from the configured SharePoint sites, and saves them as 
Markdown documents under ./output/<site-name>/.

Usage:
    python main.py                    # Crawl all sites
    python main.py --sites service1   # Crawl only service1 sites
    python main.py --sites service2   # Crawl only service2 sites
    python main.py --list             # List configured sites

TODO: Have similar logic in data pipeline and push the md files to vector store instead of local disk.
This script is mainly for testing and demonstration purposes.

"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from config_loader import load_config, list_available_sites, SiteConfig
from sharepoint_client import SharePointClient
from sharepoint_crawler import SharePointCrawler
from document_converter import BaseDocumentConverter, MarkItDownDocumentConverter

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def crawl_site(
    site: SiteConfig,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_dir: Path,
    file_extensions: set[str],
    converter: BaseDocumentConverter | None = None,
) -> dict[str, int]:
    """Crawl a single SharePoint site and return results."""
    logger.info("=" * 60)
    logger.info("Crawling site: %s (%s)", site.name, site.url)
    if site.page_paths:
        logger.info("Page filter: %d path(s)", len(site.page_paths))
    logger.info("=" * 60)

    sp_client = SharePointClient(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        site_url=site.url,
    )

    # Each site gets its own subdirectory: output/<group>/<site>
    site_output_dir = output_dir / site.group_key / site.name
    crawler = SharePointCrawler(
        client=sp_client,
        output_dir=site_output_dir,
        file_extensions=file_extensions,
        page_paths=site.page_paths,
        converter=converter,
    )

    return crawler.crawl_all()


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Crawl SharePoint sites and convert content to Markdown."
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        default=[],
        help="Site groups to crawl. Omit to crawl all sites.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_sites",
        help="List configured sites and exit.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to sites_config.yaml (default: ./sites_config.yaml)",
    )
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        logger.error(str(e))
        sys.exit(1)

    # List sites and exit if requested
    if args.list_sites:
        list_available_sites(config)
        sys.exit(0)

    # Get sites to crawl
    sites = config.get_sites_by_groups(args.sites)
    if not sites:
        logger.error("No sites to crawl. Check your --sites argument or config file.")
        sys.exit(1)

    logger.info("Will crawl %d site(s)", len(sites))

    # Instantiate converter backend
    logger.info("Using MarkItDown converter backend.")
    converter: BaseDocumentConverter = MarkItDownDocumentConverter()

    # Crawl each site
    total_results = {"pages": 0, "files": 0}
    for site in sites:
        try:
            results = crawl_site(
                site=site,
                tenant_id=config.tenant_id,
                client_id=config.client_id,
                client_secret=config.client_secret,
                output_dir=config.output_dir,
                file_extensions=config.file_extensions,
                converter=converter,
            )
            total_results["pages"] += results["pages"]
            total_results["files"] += results["files"]
        except Exception:
            logger.exception("Failed to crawl site: %s", site.url)

    print("\n" + "=" * 60)
    print("Crawl complete:")
    print(f"  Sites crawled : {len(sites)}")
    print(f"  Pages saved   : {total_results['pages']}")
    print(f"  Files saved   : {total_results['files']}")
    print(f"  Output dir    : {config.output_dir.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()


